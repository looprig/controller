package main

import (
	"context"
	"encoding/json"
	"errors"
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
	}
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
		{"CONTROLLER_CREDENTIALS", "no-equals-sign"},
		{"CONTROLLER_CREDENTIALS", "a=x,a=y"},
		{"CONTROLLER_CREDENTIALS", "=secret"},
		{"CONTROLLER_SESSIONS", `{"tenant_id":"t"}`},
		{"CONTROLLER_SESSIONS", `[{"tenant_id":"t","session_id":"s","extra":1}]`},
		{"CONTROLLER_SESSIONS", `[]`},
		{"CONTROLLER_SESSIONS", `[{"tenant_id":"t","session_id":"s"}] trailing`},
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

// The generic binary refuses to start: configuration first, then the missing
// storage bootstrap -- and no cluster client is ever built.
func TestGenericBinaryRefusesToStart(t *testing.T) {
	clientBuilt := false
	newClient := func() (k8s.Interface, error) { clientBuilt = true; return fake.NewClientset(), nil }

	err := Run(context.Background(), lookup(map[string]string{}), unconfiguredBootstrap{}, newClient)
	var configErr *ConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("empty environment: err = %v, want a ConfigError", err)
	}

	err = Run(context.Background(), lookup(completeEnv()), unconfiguredBootstrap{}, newClient)
	if !errors.Is(err, errNoBootstrap) {
		t.Fatalf("complete environment, generic bootstrap: err = %v, want errNoBootstrap", err)
	}
	if clientBuilt {
		t.Fatal("a cluster client was built for a controller that cannot start")
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
