// Command controller runs Looprig's optional Kubernetes workload controller:
// the durable driver over the direct-Pod adapter, in one namespace.
//
// IT IS GENERIC AND OPENS NO STORAGE BACKEND. The durable plane it reads is a
// SessionStore over a product-chosen storage composite, and this module pins
// no backend. The binary built from this file therefore REFUSES TO START once
// its configuration is valid, naming the missing piece -- exactly as Host's
// generic binary does. A product supplies a Bootstrap and calls Run.
//
// What a started controller does, and does not do, is stated in the driver and
// kubernetes packages: it ensures one direct Pod per configured dedicated
// session whose durable record asks for one and that no live Host owns; it
// never drains and never deletes (task D2.2); and a Ready Pod it created is a
// placement CANDIDATE only -- the epoch-fenced Host registry, not Kubernetes
// readiness, says which Host holds a session.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/looprig/controller/driver"
	"github.com/looprig/controller/kubernetes"
)

// Bootstrap is the product-specific seam: the storage composite the
// controller's SessionStore is opened over, plus any options (for example a
// legacy single tenant) the deployment's Factory and Hosts open it with.
type Bootstrap interface {
	Backend(ctx context.Context) (*storage.Composite, []sessionstore.Option, error)
}

// ClientFactory builds the Kubernetes client.
type ClientFactory func() (k8s.Interface, error)

// onPass is a test hook observing each driver pass.
var onPass func(driver.PassReport, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := Run(ctx, os.LookupEnv, unconfiguredBootstrap{}, inClusterClient); err != nil {
		fmt.Fprintln(os.Stderr, "controller:", err)
		os.Exit(1)
	}
}

// Run reads configuration, opens the durable store through the bootstrap,
// builds the cluster client, and drives passes until ctx ends.
//
// The order is deliberate. EVERY configuration check -- parsing here, and the
// value checks kubernetes.CheckConfig and driver.CheckConfig own -- runs
// before the backend is asked for, and the backend is opened before any
// cluster client exists, so a controller that cannot run never touches the
// storage backend or the API server, and a bad value is reported by variable.
func Run(ctx context.Context, lookup Environment, bootstrap Bootstrap, newClient ClientFactory) (err error) {
	cfg, err := LoadConfig(lookup)
	if err != nil {
		return err
	}
	source, err := driver.NewFixedSource(cfg.Sessions, driver.MaxKeysPerPassCeiling)
	if err != nil {
		return &ConfigError{Variable: "CONTROLLER_SESSIONS", Reason: "must name 1..256 distinct valid tenant/session pairs"}
	}
	holder, err := holderID(cfg.ReplicaID, rand.Reader)
	if err != nil {
		return err
	}
	adapterCfg := kubernetes.Config{
		Namespace:     cfg.Namespace,
		ControllerID:  cfg.ControllerID,
		HostSubdomain: cfg.HostSubdomain,
		HostPort:      cfg.HostPort,
		Credentials:   cfg.Credentials,
		Clock:         kubernetes.SystemClock{},
	}
	driverCfg := driver.Config{
		Source:         source,
		Clock:          kubernetes.SystemClock{},
		HolderID:       holder,
		ClaimTTL:       cfg.ClaimTTL,
		ItemTimeout:    cfg.ItemTimeout,
		MaxKeysPerPass: len(cfg.Sessions),
		Interval:       cfg.Interval,
		Logger:         slog.New(slog.NewJSONHandler(os.Stderr, nil)),
		OnPass:         onPass,
	}
	if err := variableError(kubernetes.CheckConfig(adapterCfg)); err != nil {
		return err
	}
	if err := variableError(driver.CheckConfig(driverCfg)); err != nil {
		return err
	}

	backend, options, err := bootstrap.Backend(ctx)
	if err != nil {
		return fmt.Errorf("open storage backend: %w", err)
	}
	store, err := sessionstore.Open(ctx, backend, options...)
	if err != nil {
		return fmt.Errorf("open session store: %w", err)
	}
	defer func() {
		closeErr := store.Close(context.WithoutCancel(ctx))
		if err == nil && closeErr != nil {
			err = fmt.Errorf("close session store: %w", closeErr)
		}
	}()
	client, err := newClient()
	if err != nil {
		return fmt.Errorf("build kubernetes client: %w", err)
	}
	adapterCfg.Client, adapterCfg.Registry = client, store
	adapter, err := kubernetes.New(adapterCfg)
	if err != nil {
		return err
	}
	driverCfg.Catalog, driverCfg.Registry, driverCfg.Claims, driverCfg.Workloads = store, store, store, adapter
	d, err := driver.New(driverCfg)
	if err != nil {
		return err
	}
	// driver.Run returns only ctx's error, so ending the context is a clean
	// stop; any other error is returned.
	if runErr := d.Run(ctx); !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
		return runErr
	}
	return nil
}

// fieldVariables maps a value a package check refused to the variable an
// operator sets.
var fieldVariables = map[string]string{
	"Namespace":      "CONTROLLER_NAMESPACE",
	"ControllerID":   "CONTROLLER_ID",
	"HostSubdomain":  "CONTROLLER_HOST_SUBDOMAIN",
	"HostPort":       "CONTROLLER_HOST_PORT",
	"Credentials":    "CONTROLLER_CREDENTIALS",
	"HolderID":       "CONTROLLER_REPLICA_ID",
	"ClaimTTL":       "CONTROLLER_CLAIM_TTL",
	"ItemTimeout":    "CONTROLLER_ITEM_TIMEOUT",
	"MaxKeysPerPass": "CONTROLLER_SESSIONS",
	"Interval":       "CONTROLLER_INTERVAL",
}

func variableError(err error) error {
	if err == nil {
		return nil
	}
	var k *kubernetes.FieldError
	if errors.As(err, &k) {
		if v, ok := fieldVariables[k.Field]; ok {
			return &ConfigError{Variable: v, Reason: k.Reason}
		}
	}
	var d *driver.FieldError
	if errors.As(err, &d) {
		if v, ok := fieldVariables[d.Field]; ok {
			return &ConfigError{Variable: v, Reason: d.Reason}
		}
	}
	return err
}

// holderID is this PROCESS's reconciliation-claim holder: the configured
// replica id, a dot, and 16 random hex characters.
//
// SessionStore treats a second acquire by the same holder as an EXTENSION,
// not a refusal, so two replicas configured with the same CONTROLLER_REPLICA_ID
// (a Deployment template with one static value) would both "hold" every claim
// and suppress nothing. The per-process suffix makes holders distinct among
// live processes whatever the configuration says. The cost is that a
// restarted process does not recognise a claim its predecessor left: it waits
// out that claim's TTL (at most CONTROLLER_CLAIM_TTL), which is the delay a
// crashed replica's claim costs anyway. CONTROLLER_REPLICA_ID should still be
// distinct per replica (for example the Pod name via the downward API),
// because it is what an operator reads.
func holderID(replica string, entropy io.Reader) (string, error) {
	var suffix [8]byte
	if _, err := io.ReadFull(entropy, suffix[:]); err != nil {
		return "", fmt.Errorf("derive claim holder: %w", err)
	}
	return replica + "." + hex.EncodeToString(suffix[:]), nil
}

func inClusterClient() (k8s.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return k8s.NewForConfig(config)
}

// unconfiguredBootstrap is what the generic binary runs with, and it refuses.
// It is not a development default: an in-memory store would start a
// controller that reconciles nothing any Factory wrote, and look healthy.
type unconfiguredBootstrap struct{}

var errNoBootstrap = errors.New("this binary is generic and opens no storage backend; a product supplies a Bootstrap and calls Run")

func (unconfiguredBootstrap) Backend(context.Context) (*storage.Composite, []sessionstore.Option, error) {
	return nil, nil, errNoBootstrap
}
