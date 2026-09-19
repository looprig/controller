package driver_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/looprig/controller/driver"
	"github.com/looprig/controller/internal/fakeapi"
	"github.com/looprig/controller/kubernetes"
)

// This is the whole D2.1 path against a REAL SessionStore (the storage
// module's in-memory reference backend) and the REAL adapter over client-go's
// fake clientset: Factory-authored desire in the catalog -> durable claim ->
// deterministic Pod. Nothing here is a stand-in for SessionStore semantics.

const namespace = "looprig-dedicated"

type storeClock struct{ now time.Time }

func (c *storeClock) Now() time.Time { return c.now }

func openStore(t *testing.T) *sessionstore.Store {
	t.Helper()
	store, err := sessionstore.Open(context.Background(), memstore.New())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	return store
}

func payload(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(kubernetes.PayloadV1{
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
	return raw
}

type rig struct {
	store   *sessionstore.Store
	client  *fakeapi.Server
	adapter *kubernetes.Controller
	clock   *storeClock
	key     driver.Key
}

func newRig(t *testing.T) *rig {
	t.Helper()
	store := openStore(t)
	clock := &storeClock{now: time.Now().UTC()}
	client := fakeapi.New()
	adapter, err := kubernetes.New(kubernetes.Config{
		Client: client.Clientset, Namespace: namespace, ControllerID: "controller-a",
		HostSubdomain: "looprig-hosts", HostPort: 7443,
		Credentials: map[string]string{"session-store": "shared-session-store"},
		Registry:    store, Clock: clock,
		DrainCeiling: 30 * time.Second, CommitMargin: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := driver.Key{TenantID: "tenant-acme", SessionID: "session-0001"}
	if _, _, err := store.CreateCatalogEntry(context.Background(), sessionstore.CreateCatalogEntryRequest{
		TenantID: key.TenantID, SessionID: key.SessionID,
		AgentID: "agent-coder", RuntimeCompatibilityID: "runtime-2026-09",
		CreatedAt: clock.now, LastActiveAt: clock.now,
		State: sessionwire.SessionStateIdle, Residency: sessionwire.SessionResidencyCold,
		DesiredPlacement: sessionwire.HostPlacementDedicated,
		DesiredWorkload:  sessionstore.DesiredWorkload{PayloadVersion: kubernetes.PayloadVersionV1, Payload: payload(t)},
		IdempotencyKey:   "create-1",
	}); err != nil {
		t.Fatalf("create catalog entry: %v", err)
	}
	return &rig{store: store, client: client, adapter: adapter, clock: clock, key: key}
}

func (r *rig) driver(t *testing.T, holder string) *driver.Driver {
	t.Helper()
	source, err := driver.NewFixedSource([]driver.Key{r.key}, 1)
	if err != nil {
		t.Fatal(err)
	}
	d, err := driver.New(driver.Config{
		Source: source, Catalog: r.store, Registry: r.store, Claims: r.store, Workloads: r.adapter,
		Terminations: r.store, Drainer: &fakeDrainer{},
		Clock: r.clock, HolderID: holder, ClaimTTL: 30 * time.Second, ItemTimeout: 10 * time.Second,
		MaxKeysPerPass: 1, Interval: time.Second, DrainTimeout: tdDrainTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func (r *rig) intent(t *testing.T) sessionstore.PlacementIntent {
	t.Helper()
	entry, err := r.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: r.key.TenantID, SessionID: r.key.SessionID})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := entry.Record.PlacementIntent()
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func onlyItem(t *testing.T, d *driver.Driver) driver.ItemResult {
	t.Helper()
	report, err := d.Pass(context.Background())
	if err != nil {
		t.Fatalf("Pass: %v", err)
	}
	if len(report.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(report.Items))
	}
	return report.Items[0]
}

func creates(client *fakeapi.Server) int {
	n := 0
	for _, a := range client.Actions() {
		if a.GetVerb() == "create" && a.GetResource().Resource == "pods" {
			n++
		}
	}
	return n
}

func TestDriverOverARealStoreCreatesOneDeterministicPod(t *testing.T) {
	r := newRig(t)
	intent := r.intent(t)
	if intent.Generation == 0 {
		t.Fatalf("a created dedicated session carries generation 0")
	}

	item := onlyItem(t, r.driver(t, "replica-a"))
	if item.Outcome != driver.OutcomeEnsured || item.Err != nil || item.Generation != intent.Generation {
		t.Fatalf("item = %+v, want ensured at generation %d", item, intent.Generation)
	}
	pods, err := r.client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 || pods.Items[0].Name != kubernetes.WorkloadName(intent) {
		t.Fatalf("pods = %d, want exactly one named %s", len(pods.Items), kubernetes.WorkloadName(intent))
	}

	// A second replica's pass: the claim was released, the Pod is adopted.
	item = onlyItem(t, r.driver(t, "replica-b"))
	if item.Outcome != driver.OutcomeEnsured {
		t.Fatalf("second pass = %+v, want ensured", item)
	}
	if got := creates(r.client); got != 1 {
		t.Fatalf("creates = %d, want exactly 1", got)
	}
}

func TestDriverOverARealStoreDefersToAHeldClaim(t *testing.T) {
	r := newRig(t)
	if _, err := r.store.AcquireReconciliationClaim(context.Background(), sessionstore.AcquireReconciliationClaimRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, HolderID: "factory-replica", ExpiresAt: r.clock.now.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	item := onlyItem(t, r.driver(t, "replica-a"))
	if item.Outcome != driver.OutcomeDeferred || item.Err != nil {
		t.Fatalf("item = %+v, want deferred", item)
	}
	if len(r.client.Actions()) != 0 {
		t.Fatalf("a deferred pass touched the cluster: %v", r.client.Actions())
	}
}

func TestDriverOverARealStoreStandsBackForALiveHost(t *testing.T) {
	r := newRig(t)
	intent := r.intent(t)
	if _, err := r.store.PutHostRegistration(context.Background(), sessionstore.PutHostRegistrationRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, LeaseEpoch: 1,
		ObservedAt: r.clock.now, ExpiresAt: r.clock.now.Add(30 * time.Second),
		Route: sessionstore.HostRoute{
			HostID: kubernetes.HostID(intent), HostGeneration: intent.Generation,
			AgentID: intent.AgentID, RuntimeCompatibilityID: intent.RuntimeCompatibilityID,
			Placement:        sessionwire.HostPlacementDedicated,
			InternalEndpoint: "ws://host.looprig-hosts.looprig-dedicated.svc:7443/hostlink/tenant-acme",
			Residency:        sessionwire.SessionResidencyResident, Accepting: true,
		},
	}); err != nil {
		t.Fatalf("put registration: %v", err)
	}
	item := onlyItem(t, r.driver(t, "replica-a"))
	if item.Outcome != driver.OutcomeOwned {
		t.Fatalf("item = %+v, want owned", item)
	}
	if len(r.client.Actions()) != 0 {
		t.Fatalf("an owned session's pass touched the cluster")
	}
}

// Readiness is only a candidate; the real registry decides. A Ready Pod with
// no registration is not found; the same Pod with a live matching
// registration is reported with the registry's own observation.
func TestAdapterObservationComesFromTheRealRegistry(t *testing.T) {
	r := newRig(t)
	intent := r.intent(t)
	onlyItem(t, r.driver(t, "replica-a"))
	pod, err := r.client.CoreV1().Pods(namespace).Get(context.Background(), kubernetes.WorkloadName(intent), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = append(pod.Status.Conditions, readyCondition())
	if _, err := r.client.CoreV1().Pods(namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	obs, found, err := r.adapter.ObserveWorkload(context.Background(), intent)
	if err != nil || found || !reflect.DeepEqual(obs, sessionwire.HostLinkRegistryObservation{}) {
		t.Fatalf("ready pod without registration = (%+v, %v, %v), want (zero, false, nil)", obs, found, err)
	}

	endpoint := sessionwire.InternalEndpoint("ws://" + kubernetes.WorkloadName(intent) + ".looprig-hosts." + namespace + ".svc:7443/hostlink/tenant-acme")
	entry, err := r.store.PutHostRegistration(context.Background(), sessionstore.PutHostRegistrationRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, LeaseEpoch: 3,
		ObservedAt: r.clock.now, ExpiresAt: r.clock.now.Add(30 * time.Second),
		Route: sessionstore.HostRoute{
			HostID: kubernetes.HostID(intent), HostGeneration: intent.Generation,
			AgentID: intent.AgentID, RuntimeCompatibilityID: intent.RuntimeCompatibilityID,
			Placement: sessionwire.HostPlacementDedicated, InternalEndpoint: endpoint,
			Residency: sessionwire.SessionResidencyResident, Accepting: true,
		},
	})
	if err != nil {
		t.Fatalf("put registration: %v", err)
	}
	want, err := entry.Registration.Observation()
	if err != nil {
		t.Fatal(err)
	}
	obs, found, err = r.adapter.ObserveWorkload(context.Background(), intent)
	if err != nil || !found || !reflect.DeepEqual(obs, want) {
		t.Fatalf("observe = (%+v, %v, %v), want exactly (%+v, true, nil)", obs, found, err, want)
	}
	if obs.LeaseEpoch != 3 {
		t.Fatalf("lease epoch = %d, want the registry's 3", obs.LeaseEpoch)
	}

	// The Pod still stands; the driver stands back for the live Host.
	item := onlyItem(t, r.driver(t, "replica-a"))
	if item.Outcome != driver.OutcomeOwned || item.Err != nil {
		t.Fatalf("item = %+v, want owned", item)
	}
}

func readyCondition() corev1.PodCondition {
	return corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
}

// bumpDesired is "Factory" writing a new desired generation for the session.
func bumpDesired(t *testing.T, r *rig, idempotencyKey string) {
	t.Helper()
	entry, err := r.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: r.key.TenantID, SessionID: r.key.SessionID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.store.UpdateCatalogDesiredState(context.Background(), sessionstore.UpdateCatalogDesiredStateRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, ExpectedRevision: entry.Revision, IdempotencyKey: idempotencyKey,
		DesiredPlacement: entry.Record.DesiredPlacement, RuntimeCompatibilityID: entry.Record.RuntimeCompatibilityID,
		DesiredWorkload: entry.Record.DesiredWorkload,
	}); err != nil {
		t.Fatalf("bump desired: %v", err)
	}
}

// staleFirstRead returns the record it read and then, before the driver can
// take its claim, lets "Factory" write a newer desired generation.
type staleFirstRead struct {
	r    *rig
	t    *testing.T
	done bool
}

func (s *staleFirstRead) GetCatalogEntry(ctx context.Context, req sessionstore.GetCatalogEntryRequest) (sessionstore.CatalogEntry, error) {
	entry, err := s.r.store.GetCatalogEntry(ctx, req)
	if err == nil && !s.done {
		s.done = true
		bumpDesired(s.t, s.r, "factory-bump-2")
	}
	return entry, err
}

// Quality review probe P1, committed: a desired-generation write between the
// driver's first read and its claim must not produce a Pod for the obsolete
// generation. Without the re-read under the claim this creates a gen-1 Pod
// that then blocks gen 2 with GenerationConflictError for good in D2.1.
func TestDriverReReadsDesireUnderTheClaim(t *testing.T) {
	r := newRig(t)
	gen1 := r.intent(t).Generation
	source, err := driver.NewFixedSource([]driver.Key{r.key}, 1)
	if err != nil {
		t.Fatal(err)
	}
	d, err := driver.New(driver.Config{
		Source: source, Catalog: &staleFirstRead{r: r, t: t}, Registry: r.store, Claims: r.store, Workloads: r.adapter,
		Terminations: r.store, Drainer: &fakeDrainer{},
		Clock: r.clock, HolderID: "replica-a", ClaimTTL: 30 * time.Second, ItemTimeout: 10 * time.Second,
		MaxKeysPerPass: 1, Interval: time.Second, DrainTimeout: tdDrainTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	item := onlyItem(t, d)
	current := r.intent(t)
	if current.Generation == gen1 {
		t.Fatal("precondition: the bump did not move the generation")
	}
	if item.Outcome != driver.OutcomeEnsured || item.Generation != current.Generation {
		t.Fatalf("item = %+v, want ensured at the current generation %d", item, current.Generation)
	}
	pods, err := r.client.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 || pods.Items[0].Name != kubernetes.WorkloadName(current) {
		t.Fatalf("pods = %d, want exactly one, for the current generation", len(pods.Items))
	}
	if next := onlyItem(t, r.driver(t, "replica-a")); next.Outcome != driver.OutcomeEnsured || next.Err != nil {
		t.Fatalf("next pass = %+v, want ensured (adopted), not a generation conflict", next)
	}
}
