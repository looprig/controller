package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/looprig/controller/driver"
	"github.com/looprig/controller/kubernetes"
)

func completeEnv() map[string]string {
	return map[string]string{
		"CONTROLLER_NAMESPACE":      "looprig-dedicated",
		"CONTROLLER_ID":             "controller-a",
		"CONTROLLER_REPLICA_ID":     "replica-1",
		"CONTROLLER_HOST_SUBDOMAIN": "looprig-hosts",
		"CONTROLLER_HOST_PORT":      "7443",
		"CONTROLLER_CREDENTIALS":    "session-store=shared-session-store,hostlink-auth=hostlink-identity",
		"CONTROLLER_SESSIONS":       `[{"tenant_id":"tenant-acme","session_id":"session-0001"}]`,
		"CONTROLLER_INTERVAL":       "10ms",
		"CONTROLLER_CLAIM_TTL":      "30s",
		"CONTROLLER_ITEM_TIMEOUT":   "10s",

		"CONTROLLER_DRAIN_CEILING":       "30s",
		"CONTROLLER_COMMIT_MARGIN":       "10s",
		"CONTROLLER_HOSTLINK_TOKEN_FILE": tokenFile,
	}
}

// tokenFile is a readable, non-empty HostLink token file TestMain writes.
var tokenFile string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "controller-token-")
	if err != nil {
		panic(err)
	}
	tokenFile = filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("controller-service-token\n"), 0o600); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func lookup(env map[string]string) Environment {
	return func(name string) (string, bool) {
		v, ok := env[name]
		return v, ok
	}
}

func TestLoadConfigRequiresEveryVariable(t *testing.T) {
	for name := range completeEnv() {
		t.Run(name, func(t *testing.T) {
			env := completeEnv()
			delete(env, name)
			_, err := LoadConfig(lookup(env))
			var configErr *ConfigError
			if !errors.As(err, &configErr) || configErr.Variable != name {
				t.Fatalf("err = %v, want a ConfigError naming %s", err, name)
			}
			env[name] = ""
			if _, err := LoadConfig(lookup(env)); !errors.As(err, &configErr) || configErr.Variable != name {
				t.Fatalf("empty %s: err = %v, want a ConfigError naming it", name, err)
			}
		})
	}
	if _, err := LoadConfig(lookup(completeEnv())); err != nil {
		t.Fatalf("control: complete environment refused: %v", err)
	}
}

func TestLoadConfigRefusesMalformedValues(t *testing.T) {
	for _, tc := range []struct{ variable, value string }{
		{"CONTROLLER_HOST_PORT", "http"},
		{"CONTROLLER_HOST_PORT", "70000"},
		{"CONTROLLER_INTERVAL", "soon"},
		{"CONTROLLER_CLAIM_TTL", "-1s"},
		{"CONTROLLER_ITEM_TIMEOUT", "0s"},
		{"CONTROLLER_DRAIN_CEILING", "never"},
		{"CONTROLLER_COMMIT_MARGIN", "-5s"},
		{"CONTROLLER_CREDENTIALS", "no-equals-sign"},
		{"CONTROLLER_CREDENTIALS", "a=x,a=y"},
		{"CONTROLLER_CREDENTIALS", "=secret"},
		{"CONTROLLER_SESSIONS", `{"tenant_id":"t"}`},
		{"CONTROLLER_SESSIONS", `[{"tenant_id":"t","session_id":"s","extra":1}]`},
		{"CONTROLLER_SESSIONS", `[]`},
		{"CONTROLLER_SESSIONS", `[{"tenant_id":"t","session_id":"s"}] trailing`},
		{"CONTROLLER_SESSIONS", `[{"Tenant_ID":"t","session_id":"s"}]`},
		{"CONTROLLER_SESSIONS", `[{"tenant_id":"t","tenant_id":"u","session_id":"s"}]`},
	} {
		t.Run(tc.variable+"="+tc.value, func(t *testing.T) {
			env := completeEnv()
			env[tc.variable] = tc.value
			_, err := LoadConfig(lookup(env))
			var configErr *ConfigError
			if !errors.As(err, &configErr) || configErr.Variable != tc.variable {
				t.Fatalf("err = %v, want a ConfigError naming %s", err, tc.variable)
			}
			if strings.Contains(err.Error(), "shared-session-store") {
				t.Fatalf("error echoes configuration values: %v", err)
			}
		})
	}
}

// runBounded runs Run with a deadline and fails the test -- by assertion,
// not by the package test timeout -- if Run does not return in time.
func runBounded(t *testing.T, env map[string]string, bootstrap Bootstrap, newClient ClientFactory) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, lookup(env), bootstrap, newClient) }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return: a controller that must refuse to start is running")
		return nil
	}
}

// The generic binary refuses to start: configuration first, then the missing
// storage bootstrap -- and no cluster client is ever built.
func TestGenericBinaryRefusesToStart(t *testing.T) {
	clientBuilt := false
	newClient := func() (k8s.Interface, error) { clientBuilt = true; return fake.NewClientset(), nil }

	err := runBounded(t, map[string]string{}, unconfiguredBootstrap{}, newClient)
	var configErr *ConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("empty environment: err = %v, want a ConfigError", err)
	}

	err = runBounded(t, completeEnv(), unconfiguredBootstrap{}, newClient)
	if !errors.Is(err, errNoBootstrap) {
		t.Fatalf("complete environment, generic bootstrap: err = %v, want errNoBootstrap", err)
	}
	if clientBuilt {
		t.Fatal("a cluster client was built for a controller that cannot start")
	}
}

func TestConfigErrorTextHasOnePrefix(t *testing.T) {
	_, err := LoadConfig(lookup(map[string]string{}))
	if err == nil || err.Error() != "CONTROLLER_NAMESPACE: must be set" {
		t.Fatalf("err = %q, want exactly %q (main adds the one \"controller:\" prefix)", err, "CONTROLLER_NAMESPACE: must be set")
	}
}

type recordingBootstrap struct{ called *bool }

func (b recordingBootstrap) Backend(context.Context) (*storage.Composite, []sessionstore.Option, error) {
	*b.called = true
	return memstore.New(), nil, nil
}

// Spec G8: a value that PARSES but is out of range is refused, by variable,
// before the backend is opened or a client is built.
func TestOutOfRangeValuesAreRefusedBeforeTheBackend(t *testing.T) {
	for _, tc := range []struct{ variable, value string }{
		{"CONTROLLER_NAMESPACE", "Not_A_Label"},
		{"CONTROLLER_HOST_SUBDOMAIN", "UPPER"},
		{"CONTROLLER_HOST_PORT", "0"},
		{"CONTROLLER_CREDENTIALS", "Bad_Ref=shared-session-store"},
		{"CONTROLLER_CREDENTIALS", "session-store=Not_A_Secret"},
		{"CONTROLLER_CLAIM_TTL", "6m"},
		{"CONTROLLER_ITEM_TIMEOUT", "31s"},
		{"CONTROLLER_REPLICA_ID", strings.Repeat("r", 250)},
		{"CONTROLLER_SESSIONS", `[{"tenant_id":"t","session_id":"s"},{"tenant_id":"t","session_id":"s"}]`},
		{"CONTROLLER_COMMIT_MARGIN", "4s"},
		{"CONTROLLER_DRAIN_CEILING", "90m"},
		{"CONTROLLER_HOSTLINK_TOKEN_FILE", "/nonexistent/controller-token"},
	} {
		t.Run(tc.variable+"="+tc.value[:min(len(tc.value), 16)], func(t *testing.T) {
			env := completeEnv()
			env[tc.variable] = tc.value
			backendOpened, clientBuilt := false, false
			err := runBounded(t, env, recordingBootstrap{called: &backendOpened},
				func() (k8s.Interface, error) { clientBuilt = true; return fake.NewClientset(), nil })
			var configErr *ConfigError
			if !errors.As(err, &configErr) || configErr.Variable != tc.variable {
				t.Fatalf("err = %v, want a ConfigError naming %s", err, tc.variable)
			}
			if backendOpened || clientBuilt {
				t.Fatalf("backend opened %v, client built %v; want neither", backendOpened, clientBuilt)
			}
			if strings.Contains(err.Error(), tc.value) {
				t.Fatalf("error echoes the value: %v", err)
			}
		})
	}
}

func TestHolderIDIsDistinctPerProcess(t *testing.T) {
	a, err := holderID("replica-1", rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	b, err := holderID("replica-1", rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if a == b || !strings.HasPrefix(a, "replica-1.") || len(a) != len("replica-1.")+16 {
		t.Fatalf("holders %q and %q: want distinct replica-1.<16 hex>", a, b)
	}
	if _, err := holderID("replica-1", strings.NewReader("short")); err == nil {
		t.Fatal("a failed entropy read produced a holder")
	}
}

func TestRunRefusesAClusterClientFailure(t *testing.T) {
	boom := errors.New("not in a cluster")
	err := Run(context.Background(), lookup(completeEnv()), memBootstrap{}, func() (k8s.Interface, error) { return nil, boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the client failure", err)
	}
}

type memBootstrap struct{ backend *storage.Composite }

func (b memBootstrap) Backend(context.Context) (*storage.Composite, []sessionstore.Option, error) {
	if b.backend != nil {
		return b.backend, nil, nil
	}
	return memstore.New(), nil, nil
}

// A product bootstrap composes a working controller: a real SessionStore
// over the reference backend, a dedicated session in its catalog, and one
// pass creating exactly one Pod.
func TestRunWithAProductBootstrapReconciles(t *testing.T) {
	backend := memstore.New()
	store, err := sessionstore.Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(kubernetes.PayloadV1{
		Image:       "registry.example/looprig/host@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Resources:   kubernetes.Resources{CPU: "500m", Memory: "1Gi", EphemeralStorage: "2Gi"},
		Workspace:   kubernetes.Workspace{SizeLimit: "1Gi"},
		Credentials: []string{"session-store"},
		HostSettings: map[string]string{
			"HOST_WARM_TTL": "5m", "HOST_REGISTRY_HEARTBEAT": "10s", "HOST_REGISTRY_EXPIRY": "30s",
			"HOST_CLAIM_TTL": "30s", "HOST_APPLY_DEADLINE": "2m", "HOST_RECONCILE_INTERVAL": "15s",
			"HOST_COMMAND_QUEUE_SIZE": "64", "HOST_RECONCILE_BATCH": "16", "HOST_MAX_BINDINGS_PER_LINK": "4",
			"HOST_MAX_BINDINGS": "4", "HOST_MAX_TENANT_LINKS": "4", "HOST_DRAIN_GRACE": "30s",
			"HOST_DRAIN_IDLE_BOUNDARY": "5s", "HOST_DRAIN_PUBLISH_BOUND": "5s",
			"HOST_COMPATIBILITY_TIMEOUT": "5s", "HOST_WORK_POLL": "1s",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, _, err := store.CreateCatalogEntry(context.Background(), sessionstore.CreateCatalogEntryRequest{
		TenantID: "tenant-acme", SessionID: "session-0001", AgentID: "agent-coder", RuntimeCompatibilityID: "runtime-2026-09",
		CreatedAt: now, LastActiveAt: now, State: sessionwire.SessionStateIdle, Residency: sessionwire.SessionResidencyCold,
		DesiredPlacement: sessionwire.HostPlacementDedicated,
		DesiredWorkload:  sessionstore.DesiredWorkload{PayloadVersion: kubernetes.PayloadVersionV1, Payload: payload},
		IdempotencyKey:   "create-1",
	}); err != nil {
		t.Fatal(err)
	}
	entry, err := store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: "tenant-acme", SessionID: "session-0001"})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := entry.Record.PlacementIntent()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	client := fake.NewClientset()
	ctx, cancel := context.WithCancel(context.Background())
	passed := make(chan driver.PassReport, 1)
	onPass = func(report driver.PassReport, err error) {
		if err == nil {
			select {
			case passed <- report:
			default:
			}
		}
	}
	t.Cleanup(func() { onPass = nil })
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, lookup(completeEnv()), memBootstrap{backend: backend}, func() (k8s.Interface, error) { return client, nil })
	}()
	select {
	case report := <-passed:
		if len(report.Items) != 1 || report.Items[0].Outcome != driver.OutcomeEnsured {
			t.Fatalf("first pass = %+v, want one ensured item", report)
		}
	case err := <-done:
		t.Fatalf("Run returned before a pass: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no pass")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
	pods, err := client.CoreV1().Pods("looprig-dedicated").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 || pods.Items[0].Name != kubernetes.WorkloadName(intent) {
		t.Fatalf("pods = %d, want exactly one named %s", len(pods.Items), kubernetes.WorkloadName(intent))
	}
}

// The controller binary names Factory's seam only in tests: its production
// import graph must not link Factory (and with it Factory's server), and it
// must reach Kubernetes client packages -- this is the adapter's binary.
func TestBinaryImportGraph(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	deps := strings.Fields(string(out))
	var factoryDeps []string
	client := false
	for _, dep := range deps {
		if dep == "github.com/looprig/factory" || strings.HasPrefix(dep, "github.com/looprig/factory/") {
			factoryDeps = append(factoryDeps, dep)
		}
		if dep == "k8s.io/client-go/kubernetes" {
			client = true
		}
	}
	if len(factoryDeps) != 0 {
		t.Fatalf("cmd/controller links Factory packages %v", factoryDeps)
	}
	if !client {
		t.Fatalf("control: cmd/controller does not reach k8s.io/client-go/kubernetes; the derivation is vacuous")
	}
}

// Quality 8 / probe P3: another process configured with the SAME replica id
// holds the session's claim. With a per-process holder this replica defers
// rather than "extending" the other's claim and working alongside it.
func TestSameReplicaIDDoesNotShareAClaim(t *testing.T) {
	backend := dedicatedBackend(t)
	store, err := sessionstore.Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireReconciliationClaim(context.Background(), sessionstore.AcquireReconciliationClaimRequest{
		TenantID: "tenant-acme", SessionID: "session-0001", HolderID: "replica-1", ExpiresAt: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	report := firstPass(t, backend, fake.NewClientset(), nil)
	if len(report.Items) != 1 || report.Items[0].Outcome != driver.OutcomeDeferred {
		t.Fatalf("first pass = %+v, want deferred to the other replica-1 process", report)
	}
}

type failingCloser struct{}

var errProviderClose = errors.New("provider close failed")

func (failingCloser) Close() error { return errProviderClose }

// A failure to close the store after a clean stop is reported, not dropped.
func TestRunReportsAStoreCloseFailure(t *testing.T) {
	backend := dedicatedBackend(t)
	var runErr error
	firstPass(t, backend, fake.NewClientset(), func(err error) { runErr = err }, sessionstore.WithIOProviderOwnership(failingCloser{}))
	if !errors.Is(runErr, errProviderClose) {
		t.Fatalf("Run = %v, want the store close failure", runErr)
	}
}

type optionsBootstrap struct {
	backend *storage.Composite
	options []sessionstore.Option
}

func (b optionsBootstrap) Backend(context.Context) (*storage.Composite, []sessionstore.Option, error) {
	return b.backend, b.options, nil
}

// dedicatedBackend is a memstore backend holding the one dedicated session
// completeEnv names.
func dedicatedBackend(t *testing.T) *storage.Composite {
	t.Helper()
	backend := memstore.New()
	store, err := sessionstore.Open(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(kubernetes.PayloadV1{
		Image:       "registry.example/looprig/host@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Resources:   kubernetes.Resources{CPU: "500m", Memory: "1Gi", EphemeralStorage: "2Gi"},
		Workspace:   kubernetes.Workspace{SizeLimit: "1Gi"},
		Credentials: []string{"session-store"},
		HostSettings: map[string]string{
			"HOST_WARM_TTL": "5m", "HOST_REGISTRY_HEARTBEAT": "10s", "HOST_REGISTRY_EXPIRY": "30s",
			"HOST_CLAIM_TTL": "30s", "HOST_APPLY_DEADLINE": "2m", "HOST_RECONCILE_INTERVAL": "15s",
			"HOST_COMMAND_QUEUE_SIZE": "64", "HOST_RECONCILE_BATCH": "16", "HOST_MAX_BINDINGS_PER_LINK": "4",
			"HOST_MAX_BINDINGS": "4", "HOST_MAX_TENANT_LINKS": "4", "HOST_DRAIN_GRACE": "30s",
			"HOST_DRAIN_IDLE_BOUNDARY": "5s", "HOST_DRAIN_PUBLISH_BOUND": "5s",
			"HOST_COMPATIBILITY_TIMEOUT": "5s", "HOST_WORK_POLL": "1s",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, _, err := store.CreateCatalogEntry(context.Background(), sessionstore.CreateCatalogEntryRequest{
		TenantID: "tenant-acme", SessionID: "session-0001", AgentID: "agent-coder", RuntimeCompatibilityID: "runtime-2026-09",
		CreatedAt: now, LastActiveAt: now, State: sessionwire.SessionStateIdle, Residency: sessionwire.SessionResidencyCold,
		DesiredPlacement: sessionwire.HostPlacementDedicated,
		DesiredWorkload:  sessionstore.DesiredWorkload{PayloadVersion: kubernetes.PayloadVersionV1, Payload: payload},
		IdempotencyKey:   "create-1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	return backend
}

// firstPass runs the controller until its first pass completes, stops it,
// and returns that pass's report. done, if set, receives Run's result.
func firstPass(t *testing.T, backend *storage.Composite, client k8s.Interface, done func(error), options ...sessionstore.Option) driver.PassReport {
	t.Helper()
	passed := make(chan driver.PassReport, 1)
	onPass = func(report driver.PassReport, err error) {
		if err == nil {
			select {
			case passed <- report:
			default:
			}
		}
	}
	t.Cleanup(func() { onPass = nil })
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- Run(ctx, lookup(completeEnv()), optionsBootstrap{backend: backend, options: options}, func() (k8s.Interface, error) { return client, nil })
	}()
	var report driver.PassReport
	select {
	case report = <-passed:
	case err := <-result:
		t.Fatalf("Run returned before a pass: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no pass")
	}
	cancel()
	select {
	case err := <-result:
		if done != nil {
			done(err)
		} else if err != nil {
			t.Fatalf("Run after cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
	return report
}
