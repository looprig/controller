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
	"errors"
	"fmt"
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
// The order is deliberate: configuration is refused before the backend is
// asked for, and the backend before any cluster client exists, so a controller
// that cannot run never touches the API server.
func Run(ctx context.Context, lookup Environment, bootstrap Bootstrap, newClient ClientFactory) (err error) {
	cfg, err := LoadConfig(lookup)
	if err != nil {
		return err
	}
	source, err := driver.NewFixedSource(cfg.Sessions, driver.MaxKeysPerPassCeiling)
	if err != nil {
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
	adapter, err := kubernetes.New(kubernetes.Config{
		Client:        client,
		Namespace:     cfg.Namespace,
		ControllerID:  cfg.ControllerID,
		HostSubdomain: cfg.HostSubdomain,
		HostPort:      cfg.HostPort,
		Credentials:   cfg.Credentials,
		Registry:      store,
		Clock:         kubernetes.SystemClock{},
	})
	if err != nil {
		return err
	}
	d, err := driver.New(driver.Config{
		Source:         source,
		Catalog:        store,
		Registry:       store,
		Claims:         store,
		Workloads:      adapter,
		Clock:          kubernetes.SystemClock{},
		HolderID:       cfg.ReplicaID,
		ClaimTTL:       cfg.ClaimTTL,
		ItemTimeout:    cfg.ItemTimeout,
		MaxKeysPerPass: len(cfg.Sessions),
		Interval:       cfg.Interval,
		Logger:         slog.New(slog.NewJSONHandler(os.Stderr, nil)),
		OnPass:         onPass,
	})
	if err != nil {
		return err
	}
	if runErr := d.Run(ctx); !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
		return runErr
	}
	return nil
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
