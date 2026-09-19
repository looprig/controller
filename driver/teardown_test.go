package driver_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage/memstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	"github.com/looprig/controller/driver"
	"github.com/looprig/controller/hostlink"
	"github.com/looprig/controller/internal/fakeapi"
	"github.com/looprig/controller/kubernetes"
)

// The drain-before-delete state machine, end to end over a REAL SessionStore
// (memstore), the REAL adapter over the fake API server (internal/fakeapi adds
// the UID, resourceVersion, precondition and finalizer behaviours client-go's
// fake lacks), and a scripted Host drain client. Every case starts from a
// Factory-authored desire and ends by reading the durable records back.

type runtimeObject = runtime.Object

const (
	tdEpoch        = uint64(3)
	tdDrainTimeout = 40 * time.Second
)

// sharedClock is the ONE clock the store and the driver read, so a case that
// moves time moves it for both.
type sharedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *sharedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *sharedClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// hookedStore is the real store with hooks and an ordered log in front of the
// two writes the state machine makes.
type hookedStore struct {
	*sessionstore.Store
	mu           sync.Mutex
	log          []string
	beforeClear  func(sessionstore.ClearHostRegistrationRequest) error
	beforeRecord func(sessionstore.RecordPlacementTerminationRequest) error
	afterClear   func(sessionstore.ClearHostRegistrationRequest)
	cleared      []sessionstore.ClearHostRegistrationRequest
	recorded     []sessionstore.RecordPlacementTerminationRequest
}

func (h *hookedStore) note(s string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.log = append(h.log, s)
}

func (h *hookedStore) ClearHostRegistration(ctx context.Context, req sessionstore.ClearHostRegistrationRequest) (sessionstore.HostRegistrationEntry, error) {
	h.note(fmt.Sprintf("clear@%d", req.LeaseEpoch))
	h.cleared = append(h.cleared, req)
	if h.beforeClear != nil {
		if err := h.beforeClear(req); err != nil {
			return sessionstore.HostRegistrationEntry{}, err
		}
	}
	entry, err := h.Store.ClearHostRegistration(ctx, req)
	if err == nil && h.afterClear != nil {
		h.afterClear(req)
	}
	return entry, err
}

func (h *hookedStore) RecordPlacementTermination(ctx context.Context, req sessionstore.RecordPlacementTerminationRequest) (sessionstore.PlacementTerminationEntry, bool, error) {
	h.note(fmt.Sprintf("record@%d", req.Generation))
	h.recorded = append(h.recorded, req)
	if h.beforeRecord != nil {
		if err := h.beforeRecord(req); err != nil {
			return sessionstore.PlacementTerminationEntry{}, false, err
		}
	}
	return h.Store.RecordPlacementTermination(ctx, req)
}

// fakeDrainer is the Host end of HostLink, scripted per case. It records
// every call; answer decides the reply.
type fakeDrainer struct {
	mu     sync.Mutex
	calls  []drainCall
	answer func(method string, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error)
}

type drainCall struct {
	Method   string
	Endpoint sessionwire.InternalEndpoint
	Request  sessionwire.HostLinkDrainRequest
}

func (f *fakeDrainer) do(method string, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
	f.mu.Lock()
	f.calls = append(f.calls, drainCall{Method: method, Endpoint: endpoint, Request: req})
	answer := f.answer
	f.mu.Unlock()
	if answer == nil {
		return sessionwire.HostLinkDrainObservation{}, errors.New("fakeDrainer: no answer scripted")
	}
	return answer(method, req)
}

func (f *fakeDrainer) StartDrain(_ context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
	return f.do(sessionwire.HostLinkMethodDrain, endpoint, req)
}

func (f *fakeDrainer) DrainStatus(_ context.Context, endpoint sessionwire.InternalEndpoint, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
	return f.do(sessionwire.HostLinkMethodDrainStatus, endpoint, req)
}

func (f *fakeDrainer) methods() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, c.Method)
	}
	return out
}

func answering(state sessionwire.HostLinkDrainState) func(string, sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
	return func(_ string, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
		return sessionwire.HostLinkDrainObservation{
			HostID: req.HostID, HostGeneration: req.HostGeneration, DrainGeneration: 1,
			State: state, TenantID: req.TenantID, SessionID: req.SessionID,
		}, nil
	}
}

func refusing(code sessionwire.HostLinkErrorCode) func(string, sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
	return func(method string, _ sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
		return sessionwire.HostLinkDrainObservation{}, &hostlink.RefusalError{Method: method, Refusal: sessionwire.HostLinkError{Code: code}}
	}
}

type tdRig struct {
	t       *testing.T
	store   *hookedStore
	api     *fakeapi.Server
	adapter *kubernetes.Controller
	drainer *fakeDrainer
	clock   *sharedClock
	key     driver.Key
}

func newTDRig(t *testing.T) *tdRig {
	t.Helper()
	clock := &sharedClock{now: time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)}
	store, err := sessionstore.Open(context.Background(), memstore.New(), sessionstore.WithClock(clock))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	api := fakeapi.New()
	api.Now = clock.Now
	adapter, err := kubernetes.New(kubernetes.Config{
		Client: api.Clientset, Namespace: namespace, ControllerID: "controller-a",
		HostSubdomain: "looprig-hosts", HostPort: 7443,
		Credentials: map[string]string{"session-store": "shared-session-store"},
		Registry:    store, Clock: clock,
		DrainCeiling: 30 * time.Second, CommitMargin: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	r := &tdRig{
		t: t, store: &hookedStore{Store: store}, api: api, adapter: adapter,
		drainer: &fakeDrainer{}, clock: clock,
		key: driver.Key{TenantID: "tenant-acme", SessionID: "session-0001"},
	}
	if _, _, err := store.CreateCatalogEntry(context.Background(), sessionstore.CreateCatalogEntryRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID,
		AgentID: "agent-coder", RuntimeCompatibilityID: "runtime-2026-09",
		CreatedAt: clock.Now(), LastActiveAt: clock.Now(),
		State: sessionwire.SessionStateIdle, Residency: sessionwire.SessionResidencyCold,
		DesiredPlacement: sessionwire.HostPlacementDedicated,
		DesiredWorkload:  sessionstore.DesiredWorkload{PayloadVersion: kubernetes.PayloadVersionV1, Payload: payload(t)},
		IdempotencyKey:   "create-1",
	}); err != nil {
		t.Fatalf("create catalog entry: %v", err)
	}
	return r
}

func (r *tdRig) driver(holder string) *driver.Driver {
	r.t.Helper()
	source, err := driver.NewFixedSource([]driver.Key{r.key}, 1)
	if err != nil {
		r.t.Fatal(err)
	}
	d, err := driver.New(driver.Config{
		Source: source, Catalog: r.store, Registry: r.store, Claims: r.store, Workloads: r.adapter,
		Terminations: r.store, Drainer: r.drainer,
		Clock: r.clock, HolderID: holder, ClaimTTL: 30 * time.Second, ItemTimeout: 10 * time.Second,
		MaxKeysPerPass: 1, Interval: time.Second, DrainTimeout: tdDrainTimeout,
	})
	if err != nil {
		r.t.Fatal(err)
	}
	return d
}

// pass runs one pass of a fresh replica. Each call uses a new holder, so a
// claim a previous pass kept (P2) is a claim held by ANOTHER replica.
func (r *tdRig) pass(holder string) driver.ItemResult {
	r.t.Helper()
	report, err := r.driver(holder).Pass(context.Background())
	if err != nil {
		r.t.Fatalf("Pass: %v", err)
	}
	if len(report.Items) != 1 {
		r.t.Fatalf("items = %d, want 1", len(report.Items))
	}
	return report.Items[0]
}

func (r *tdRig) intent() sessionstore.PlacementIntent {
	r.t.Helper()
	entry, err := r.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: r.key.TenantID, SessionID: r.key.SessionID})
	if err != nil {
		r.t.Fatal(err)
	}
	intent, err := entry.Record.PlacementIntent()
	if err != nil {
		r.t.Fatal(err)
	}
	return intent
}

// running creates the generation's Pod through a driver pass and marks it
// Running, as a kubelet would.
func (r *tdRig) running() (string, sessionstore.PlacementIntent) {
	r.t.Helper()
	intent := r.intent()
	if item := r.pass("replica-create"); item.Outcome != driver.OutcomeEnsured {
		r.t.Fatalf("create pass = %+v, want ensured", item)
	}
	name := kubernetes.WorkloadName(intent)
	if err := r.api.SetStatus(namespace, name, corev1.PodStatus{Phase: corev1.PodRunning}); err != nil {
		r.t.Fatal(err)
	}
	return name, intent
}

// register is the Host in the named workload publishing its live route.
func (r *tdRig) register(name string, generation, epoch uint64) {
	r.t.Helper()
	if _, err := r.store.PutHostRegistration(context.Background(), sessionstore.PutHostRegistrationRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, LeaseEpoch: epoch,
		ObservedAt: r.clock.Now(), ExpiresAt: r.clock.Now().Add(50 * time.Minute),
		Route: sessionstore.HostRoute{
			HostID: sessionwire.HostID(name), HostGeneration: generation,
			AgentID: "agent-coder", RuntimeCompatibilityID: "runtime-2026-09",
			Placement:        sessionwire.HostPlacementDedicated,
			InternalEndpoint: sessionwire.InternalEndpoint("ws://" + name + ".looprig-hosts." + namespace + ".svc:7443/hostlink/tenant-acme"),
			Residency:        sessionwire.SessionResidencyResident, Accepting: true,
		},
	}); err != nil {
		r.t.Fatalf("put registration: %v", err)
	}
}

// desire is "Factory" writing a new desired state; a nil workload is deletion.
func (r *tdRig) desire(key string, workload *sessionstore.DesiredWorkload) uint64 {
	r.t.Helper()
	entry, err := r.store.GetCatalogEntry(context.Background(), sessionstore.GetCatalogEntryRequest{TenantID: r.key.TenantID, SessionID: r.key.SessionID})
	if err != nil {
		r.t.Fatal(err)
	}
	var desired sessionstore.DesiredWorkload
	if workload != nil {
		desired = *workload
	}
	updated, err := r.store.UpdateCatalogDesiredState(context.Background(), sessionstore.UpdateCatalogDesiredStateRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, ExpectedRevision: entry.Revision, IdempotencyKey: key,
		DesiredPlacement: sessionwire.HostPlacementDedicated, RuntimeCompatibilityID: entry.Record.RuntimeCompatibilityID,
		DesiredWorkload: desired,
	})
	if err != nil {
		r.t.Fatalf("update desired state: %v", err)
	}
	return updated.Record.DesiredGeneration
}

func (r *tdRig) podOrNil(name string) *corev1.Pod {
	r.t.Helper()
	pod, err := r.api.CoreV1().Pods(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		r.t.Fatal(err)
	}
	return pod
}

func (r *tdRig) termination(generation uint64) (sessionstore.PlacementTerminationEntry, error) {
	return r.store.GetPlacementTermination(context.Background(), sessionstore.GetPlacementTerminationRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, Generation: generation,
	})
}

func (r *tdRig) wantTermination(generation uint64, kind sessionstore.PlacementTerminationKind, reason sessionstore.PlacementForcedReason, epoch uint64) sessionstore.PlacementTerminationEntry {
	r.t.Helper()
	entry, err := r.termination(generation)
	if err != nil {
		r.t.Fatalf("termination for generation %d: %v", generation, err)
	}
	got := entry.Termination
	if got.Generation != generation || got.Kind != kind || got.ForcedReason != reason || got.LeaseEpoch != epoch {
		r.t.Fatalf("termination = {gen %d %s %q epoch %d}, want {gen %d %s %q epoch %d}",
			got.Generation, got.Kind, got.ForcedReason, got.LeaseEpoch, generation, kind, reason, epoch)
	}
	return entry
}

func (r *tdRig) wantNoTermination() {
	r.t.Helper()
	_, err := r.termination(1)
	var te *sessionstore.TerminationError
	if !errors.As(err, &te) || te.Code != sessionstore.TerminationErrorNotFound || te.Generation != 0 {
		r.t.Fatalf("termination read = %v, want not_found with no row at all", err)
	}
}

func (r *tdRig) registration() (sessionstore.HostRegistrationEntry, error) {
	return r.store.GetHostRegistration(context.Background(), sessionstore.GetHostRegistrationRequest{TenantID: r.key.TenantID, SessionID: r.key.SessionID})
}

// deletes returns every Pod delete sent, with its UID precondition.
func (r *tdRig) deletes() []string {
	var out []string
	for _, a := range r.api.Actions() {
		if a.GetVerb() == "delete" && a.GetResource().Resource == "pods" {
			del := a.(k8stesting.DeleteActionImpl)
			uid := "<none>"
			if p := del.DeleteOptions.Preconditions; p != nil && p.UID != nil {
				uid = string(*p.UID)
			}
			out = append(out, del.Name+"/"+uid)
		}
	}
	return out
}

func wantOutcome(t *testing.T, item driver.ItemResult, want driver.Outcome) {
	t.Helper()
	if item.Outcome != want || item.Err != nil {
		t.Fatalf("item = %+v (err %v), want outcome %q with no error", item, item.Err, want)
	}
}

// ---------------------------------------------------------------------------
// The graceful path.
// ---------------------------------------------------------------------------

func TestGracefulDrainBeforeDelete(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	uid := string(r.podOrNil(name).UID)
	r.register(name, intent.Generation, tdEpoch)
	deletion := r.desire("delete-1", nil)
	if deletion != intent.Generation+1 {
		t.Fatalf("deletion desire generation = %d, want %d", deletion, intent.Generation+1)
	}

	// Pass 1: the drain RPC, and nothing destructive.
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDraining)
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if got := r.drainer.methods(); !slices.Equal(got, []string{sessionwire.HostLinkMethodDrain}) {
		t.Fatalf("drain calls = %v, want exactly one hostlink.drain", got)
	}
	call := r.drainer.calls[0]
	wantEndpoint := sessionwire.InternalEndpoint("ws://" + name + ".looprig-hosts." + namespace + ".svc:7443/hostlink/tenant-acme")
	wantReq := sessionwire.HostLinkDrainRequest{
		Version: sessionwire.CurrentWireVersion, HostID: sessionwire.HostID(name), HostGeneration: intent.Generation,
		IdempotencyKey: "looprig-controller/drain/" + name, TenantID: r.key.TenantID, SessionID: r.key.SessionID,
	}
	if call.Endpoint != wantEndpoint || call.Request != wantReq {
		t.Fatalf("drain call = %+v, want endpoint %s and request %+v", call, wantEndpoint, wantReq)
	}
	pod := r.podOrNil(name)
	if pod == nil || pod.DeletionTimestamp != nil || pod.Annotations[kubernetes.AnnotationTermination] != "" {
		t.Fatalf("after the drain request the pod must stand undecided: %+v", pod)
	}
	if pod.Annotations[kubernetes.AnnotationDrain] == "" {
		t.Fatalf("the drain start was not persisted before the RPC")
	}
	if len(r.deletes()) != 0 || len(r.store.cleared) != 0 || len(r.store.recorded) != 0 {
		t.Fatalf("pass 1 deleted %v, cleared %v, recorded %v; want nothing", r.deletes(), r.store.cleared, r.store.recorded)
	}
	if _, err := r.registration(); err != nil {
		t.Fatalf("the controller touched the live route before the drain completed: %v", err)
	}

	// The Host finishes: it releases its own registration (the tombstone at
	// its epoch) and reports drained to the caller that began the drain.
	if _, err := r.store.Store.ClearHostRegistration(context.Background(), sessionstore.ClearHostRegistrationRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, LeaseEpoch: tdEpoch,
	}); err != nil {
		t.Fatal(err)
	}
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)

	// The Kind must be persisted BEFORE the fence and the pod still standing;
	// the record must come AFTER the delete.
	r.store.beforeClear = func(sessionstore.ClearHostRegistrationRequest) error {
		p := r.podOrNil(name)
		if p == nil || p.DeletionTimestamp != nil || p.Annotations[kubernetes.AnnotationTermination] == "" {
			t.Errorf("fence ran before the decision was persisted or after the delete: %+v", p)
		}
		return nil
	}
	r.store.beforeRecord = func(sessionstore.RecordPlacementTerminationRequest) error {
		if p := r.podOrNil(name); p == nil || p.DeletionTimestamp == nil {
			t.Errorf("termination recorded before the pod was deleted")
		}
		return nil
	}
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	// A drain already begun whose route is gone is OBSERVED, never re-begun.
	if got := r.drainer.methods(); !slices.Equal(got, []string{sessionwire.HostLinkMethodDrain, sessionwire.HostLinkMethodDrainStatus}) {
		t.Fatalf("drain calls = %v, want [drain drain_status]", got)
	}
	if got, want := r.deletes(), []string{name + "/" + uid}; !slices.Equal(got, want) {
		t.Fatalf("deletes = %v, want exactly %v (UID-preconditioned)", got, want)
	}
	if got := r.store.log; !slices.Equal(got, []string{fmt.Sprintf("clear@%d", tdEpoch), fmt.Sprintf("record@%d", intent.Generation)}) {
		t.Fatalf("store writes = %v, want [clear@%d record@%d]", got, tdEpoch, intent.Generation)
	}
	// THE GENERATION NAMED IS THE ONE WHOSE WORKLOAD ENDED, not the desire's.
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
	if r.podOrNil(name) != nil {
		t.Fatalf("pod still exists after its finalizer was released")
	}

	// Pass 3: nothing is left and nothing is desired.
	wantOutcome(t, r.pass("replica-c"), driver.OutcomeDeleted)
	if n := len(r.deletes()); n != 1 {
		t.Fatalf("deletes = %d after the sequence finished, want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Each forced reason.
// ---------------------------------------------------------------------------

func TestForcedDrainTimeout(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDraining)

	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	r.clock.advance(tdDrainTimeout - time.Second)
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	if len(r.deletes()) != 0 || len(r.store.recorded) != 0 {
		t.Fatalf("forced before the drain timeout")
	}
	r.clock.advance(time.Second)
	wantOutcome(t, r.pass("replica-c"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedDrainTimeout, tdEpoch)
	// The fence tombstoned the still-live route at the observed epoch.
	_, err := r.registration()
	var re *sessionstore.RegistryError
	if !errors.As(err, &re) || re.Code != sessionstore.RegistryErrorReleased || re.Epoch != tdEpoch {
		t.Fatalf("registration after the fence = %v, want released at epoch %d", err, tdEpoch)
	}
	if r.podOrNil(name) != nil || len(r.deletes()) != 1 {
		t.Fatalf("pod not deleted exactly once: deletes %v", r.deletes())
	}
}

func TestAHostThatDoesNotAdvertiseDrainIsForcedDrainRefused(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = func(method string, _ sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
		return sessionwire.HostLinkDrainObservation{}, fmt.Errorf("%w: %s", hostlink.ErrNotAdvertised, method)
	}
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedDrainRefused, tdEpoch)
	if r.podOrNil(name) != nil || len(r.store.cleared) != 1 || r.store.cleared[0].LeaseEpoch != tdEpoch {
		t.Fatalf("want the route fenced at %d and the pod gone; cleared %v", tdEpoch, r.store.cleared)
	}
}

func TestRuntimeUnavailableMeansReobserveNeverFailure(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = refusing(sessionwire.HostLinkErrorRuntimeUnavailable)

	// While the route still names this workload, a refusal is a wait.
	for i, holder := range []string{"replica-a", "replica-b"} {
		wantOutcome(t, r.pass(holder), driver.OutcomeTearingDown)
		if len(r.deletes()) != 0 || len(r.store.recorded) != 0 || len(r.store.cleared) != 0 {
			t.Fatalf("pass %d acted destructively on an ambiguous refusal", i+1)
		}
	}
	// The Host went cold without the controller observing a drain: forced
	// drain_refused, at the epoch the drain began at, with no fence needed
	// beyond an idempotent one.
	if _, err := r.store.Store.ClearHostRegistration(context.Background(), sessionstore.ClearHostRegistrationRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, LeaseEpoch: tdEpoch,
	}); err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, r.pass("replica-c"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedDrainRefused, tdEpoch)
	if r.podOrNil(name) != nil {
		t.Fatalf("pod not deleted")
	}
}

func TestARefusalAtTheDeadlineIsDrainRefusedNotTimeout(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = refusing(sessionwire.HostLinkErrorRuntimeUnavailable)
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	r.clock.advance(tdDrainTimeout)
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedDrainRefused, tdEpoch)
}

func TestAnUnreachableHostPastTheDeadlineIsDrainRefused(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = func(string, sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
		return sessionwire.HostLinkDrainObservation{}, fmt.Errorf("%w: connection refused", hostlink.ErrDialFailed)
	}
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if len(r.store.recorded) != 0 {
		t.Fatalf("an unreachable Host with a live route was forced before its deadline")
	}
	r.clock.advance(tdDrainTimeout)
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedDrainRefused, tdEpoch)
}

func TestAHostHoldingNothingIsForcedWithoutADrain(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.desire("delete-1", nil) // registry cold: no Host ever held the session
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if n := len(r.drainer.methods()); n != 0 {
		t.Fatalf("drain calls = %d, want none: the Host holds nothing to drain", n)
	}
	if len(r.store.cleared) != 0 {
		t.Fatalf("fenced with no observed epoch: %v", r.store.cleared)
	}
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedDrainRefused, 0)
	if r.podOrNil(name) != nil {
		t.Fatalf("pod not deleted")
	}
}

func TestAPlatformDeletedPodIsForcedPlatformDeleted(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	// An operator or an eviction deletes the Pod; the finalizer holds it.
	if err := r.api.CoreV1().Pods(namespace).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if n := len(r.drainer.methods()); n != 0 {
		t.Fatalf("drained a Pod the platform is already deleting (%d calls)", n)
	}
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedPlatformDeleted, tdEpoch)
	if r.podOrNil(name) != nil {
		t.Fatalf("finalizer not released")
	}
}

// G10: a crashed Host's terminal Pod, registry cold, CURRENT generation.
func TestATerminalPodWithAColdRegistryIsDeletedWithoutDrainAndReplaced(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	oldUID := r.podOrNil(name).UID
	if err := r.api.SetStatus(namespace, name, corev1.PodStatus{Phase: corev1.PodFailed}); err != nil {
		t.Fatal(err)
	}
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if n := len(r.drainer.methods()); n != 0 {
		t.Fatalf("drained a terminated Host (%d calls)", n)
	}
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedWorkloadTerminated, 0)
	if r.podOrNil(name) != nil {
		t.Fatalf("terminal pod not deleted")
	}
	// The generation is no longer stranded: the next pass recreates it.
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeEnsured)
	if pod := r.podOrNil(name); pod == nil || pod.UID == oldUID {
		t.Fatalf("generation %d was not recreated as a new incarnation", intent.Generation)
	}
}

func TestATerminalPodStillRoutedIsFencedAtItsEpoch(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	if err := r.api.SetStatus(namespace, name, corev1.PodStatus{Phase: corev1.PodFailed}); err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedWorkloadTerminated, tdEpoch)
	if len(r.store.cleared) != 1 || r.store.cleared[0].LeaseEpoch != tdEpoch {
		t.Fatalf("cleared = %v, want one fence at %d", r.store.cleared, tdEpoch)
	}
}

// ---------------------------------------------------------------------------
// Restart and idempotence.
// ---------------------------------------------------------------------------

func TestRestartAfterTheDecisionRecordsWhatWasDecided(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	crash := errors.New("controller crashed")
	r.store.beforeClear = func(sessionstore.ClearHostRegistrationRequest) error { return crash }
	if item := r.pass("replica-a"); item.Outcome != driver.OutcomeFailed || !errors.Is(item.Err, crash) {
		t.Fatalf("item = %+v, want the injected crash", item)
	}
	if pod := r.podOrNil(name); pod == nil || pod.Annotations[kubernetes.AnnotationTermination] == "" || pod.DeletionTimestamp != nil {
		t.Fatalf("the decision must be persisted and the pod standing after a crash at the fence")
	}
	// After the restart the Host would now REFUSE (it released), and the
	// registry is still live -- the new process must not re-derive the Kind.
	r.drainer.answer = refusing(sessionwire.HostLinkErrorRuntimeUnavailable)
	r.store.beforeClear = nil
	calls := len(r.drainer.methods())
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	if len(r.drainer.methods()) != calls {
		t.Fatalf("a restarted controller re-asked the Host after its decision was persisted")
	}
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
}

func TestRestartAfterTheDeleteRecordsFromTheHeldPod(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	crash := errors.New("controller crashed")
	r.store.beforeRecord = func(sessionstore.RecordPlacementTerminationRequest) error { return crash }
	if item := r.pass("replica-a"); item.Outcome != driver.OutcomeFailed || !errors.Is(item.Err, crash) {
		t.Fatalf("item = %+v, want the injected crash", item)
	}
	pod := r.podOrNil(name)
	if pod == nil || pod.DeletionTimestamp == nil || !slices.Contains(pod.Finalizers, kubernetes.Finalizer) {
		t.Fatalf("after a crash between delete and record the pod must be held terminating: %+v", pod)
	}
	r.wantNoTermination()
	r.store.beforeRecord = nil
	r.drainer.answer = nil // any Host call now fails the case
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
	if r.podOrNil(name) != nil || len(r.deletes()) != 1 {
		t.Fatalf("want exactly one delete and the pod released; deletes %v", r.deletes())
	}
}

func TestRepeatedPassesAndReplicasAreIdempotent(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	// A release that fails once leaves a recorded, deleted, still-held pod:
	// the next passes must replay the record without a second delete.
	failRelease := true
	r.api.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtimeObject, error) {
		pod := action.(k8stesting.UpdateAction).GetObject().(*corev1.Pod)
		if failRelease && pod.DeletionTimestamp != nil && !slices.Contains(pod.Finalizers, kubernetes.Finalizer) {
			failRelease = false
			return true, nil, apierrors.NewServiceUnavailable("injected")
		}
		return false, nil, nil
	})
	if item := r.pass("replica-a"); item.Outcome != driver.OutcomeFailed {
		t.Fatalf("item = %+v, want the injected release failure", item)
	}
	first := r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
	for _, holder := range []string{"replica-b", "replica-c", "replica-b"} {
		item := r.pass(holder)
		if item.Outcome != driver.OutcomeTearingDown && item.Outcome != driver.OutcomeDeleted {
			t.Fatalf("item = %+v", item)
		}
	}
	again := r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
	if again.Revision != first.Revision || !again.Termination.RecordedAt.Equal(first.Termination.RecordedAt) {
		t.Fatalf("an identical replay rewrote the row: %+v -> %+v", first, again)
	}
	if n := len(r.deletes()); n != 1 {
		t.Fatalf("deletes = %d, want exactly 1 across restarts and replicas", n)
	}
	if r.podOrNil(name) != nil {
		t.Fatalf("pod not released")
	}
}

// A later epoch prevents an old controller's observation from deleting a new
// owner's workload.
func TestALaterEpochStopsAnOldDecisionFromDeleting(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	crash := errors.New("controller stalled")
	r.store.beforeClear = func(sessionstore.ClearHostRegistrationRequest) error { return crash }
	if item := r.pass("replica-a"); !errors.Is(item.Err, crash) {
		t.Fatalf("item = %+v, want the stall", item)
	}
	// While the old decision (epoch 3) sits on the Pod, the Host in it takes
	// the session again under a later lease.
	r.register(name, intent.Generation, tdEpoch+2)
	r.store.beforeClear = nil
	item := r.pass("replica-b")
	if item.Outcome != driver.OutcomeTearingDown {
		t.Fatalf("item = %+v, want tearing down (re-deciding), not a delete", item)
	}
	if len(r.deletes()) != 0 {
		t.Fatalf("the old decision deleted the new owner's workload: %v", r.deletes())
	}
	pod := r.podOrNil(name)
	if pod == nil || pod.Annotations[kubernetes.AnnotationTermination] != "" || pod.Annotations[kubernetes.AnnotationDrain] != "" {
		t.Fatalf("the refused decision must be dropped so the next pass decides afresh: %+v", pod)
	}
	if reg, err := r.registration(); err != nil || reg.Registration.LeaseEpoch != tdEpoch+2 {
		t.Fatalf("the new owner's route must stand at %d: %+v %v", tdEpoch+2, reg, err)
	}
	r.wantNoTermination()
	// The next pass drains the new owner at ITS epoch.
	wantOutcome(t, r.pass("replica-c"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch+2)
}

// The delete is preconditioned on the UID the decision was made about.
func TestAReplacedPodIsNeverDeleted(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	var replacement types.UID
	r.store.beforeClear = func(sessionstore.ClearHostRegistrationRequest) error {
		uid, err := r.api.Replace(namespace, name)
		replacement = uid
		return err
	}
	item := r.pass("replica-a")
	if item.Outcome != driver.OutcomeFailed || !errors.Is(item.Err, kubernetes.ErrWorkloadReplaced) {
		t.Fatalf("item = %+v, want ErrWorkloadReplaced", item)
	}
	if pod := r.podOrNil(name); pod == nil || pod.UID != replacement || pod.DeletionTimestamp != nil {
		t.Fatalf("the replacement was touched: %+v", pod)
	}
	r.wantNoTermination()
}

// ---------------------------------------------------------------------------
// Generation order.
// ---------------------------------------------------------------------------

func TestReplacementDrainsTheOldGenerationBeforeCreatingTheNew(t *testing.T) {
	r := newTDRig(t)
	oldName, oldIntent := r.running()
	r.register(oldName, oldIntent.Generation, tdEpoch)
	workload := oldIntent.Workload
	next := r.desire("replace-1", &workload)
	newName := kubernetes.WorkloadName(r.intent())
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDraining)
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if r.podOrNil(newName) != nil {
		t.Fatalf("generation %d created while generation %d still drains", next, oldIntent.Generation)
	}
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	r.wantTermination(oldIntent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
	wantOutcome(t, r.pass("replica-c"), driver.OutcomeEnsured)
	if r.podOrNil(newName) == nil || r.podOrNil(oldName) != nil {
		t.Fatalf("want exactly the new generation's pod")
	}
}

func TestOlderGenerationsAreRecordedInOrder(t *testing.T) {
	r := newTDRig(t)
	firstName, first := r.running()
	// A second generation's Pod exists alongside (the P2 race's residue).
	workload := first.Workload
	r.desire("replace-1", &workload)
	second := r.intent()
	if err := r.adapter.EnsureWorkload(context.Background(), second); err == nil {
		t.Fatalf("precondition: the adapter must refuse a second generation beside the first")
	}
	secondPod := r.podOrNil(firstName).DeepCopy()
	secondPod.Name = kubernetes.WorkloadName(second)
	secondPod.UID, secondPod.ResourceVersion = "", ""
	secondPod.Labels[kubernetes.LabelWorkload] = secondPod.Name
	secondPod.Labels[kubernetes.LabelGeneration] = fmt.Sprint(second.Generation)
	if _, err := r.api.CoreV1().Pods(namespace).Create(context.Background(), secondPod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	final := r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	for i := 0; i < 4 && r.pass(fmt.Sprintf("replica-%d", i)).Outcome != driver.OutcomeDeleted; i++ {
	}
	var generations []uint64
	for _, req := range r.store.recorded {
		generations = append(generations, req.Generation)
	}
	if !slices.Equal(generations, []uint64{first.Generation, second.Generation}) {
		t.Fatalf("recorded generations in order %v, want [%d %d] (and never the desire's %d)", generations, first.Generation, second.Generation, final)
	}
	r.wantTermination(second.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedDrainRefused, 0)
}

func TestSupersededIsTerminalNotRetried(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	workload := intent.Workload
	r.desire("replace-1", &workload)
	r.desire("delete-1", nil)
	// Another writer already recorded a HIGHER generation's outcome.
	if _, _, err := r.store.Store.RecordPlacementTermination(context.Background(), sessionstore.RecordPlacementTerminationRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, Generation: intent.Generation + 1,
		Kind: sessionstore.PlacementTerminationForced, ForcedReason: sessionstore.PlacementForcedPlatformDeleted,
	}); err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if r.podOrNil(name) != nil {
		t.Fatalf("a superseded outcome must still release the ended workload")
	}
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeDeleted)
	if n := len(r.store.recorded); n != 1 {
		t.Fatalf("record attempts = %d, want exactly 1 (superseded is terminal, not retried)", n)
	}
	r.wantTermination(intent.Generation+1, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedPlatformDeleted, 0)
}

// P2: a claim is not released while an Ensure's outcome is unknown.
func TestAnUnknownOutcomeEnsureKeepsTheClaim(t *testing.T) {
	r := newTDRig(t)
	r.api.PrependReactor("create", "pods", func(k8stesting.Action) (bool, runtimeObject, error) {
		return true, nil, context.DeadlineExceeded
	})
	if item := r.pass("replica-a"); item.Outcome != driver.OutcomeFailed {
		t.Fatalf("item = %+v, want failed", item)
	}
	if item := r.pass("replica-b"); item.Outcome != driver.OutcomeDeferred {
		t.Fatalf("second replica = %+v, want deferred behind the kept claim", item)
	}
}

// A later epoch held ELSEWHERE does not undo an honest decision: this
// workload holds nothing, so it is deleted without a fence and the decision
// recorded as made.
func TestALaterEpochHeldElsewhereLeavesTheDecisionStanding(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	crash := errors.New("controller stalled")
	r.store.beforeClear = func(sessionstore.ClearHostRegistrationRequest) error { return crash }
	if item := r.pass("replica-a"); !errors.Is(item.Err, crash) {
		t.Fatalf("item = %+v, want the stall", item)
	}
	// A pooled Host takes the session under a later lease.
	if _, err := r.store.PutHostRegistration(context.Background(), sessionstore.PutHostRegistrationRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, LeaseEpoch: tdEpoch + 4,
		ObservedAt: r.clock.Now(), ExpiresAt: r.clock.Now().Add(50 * time.Minute),
		Route: sessionstore.HostRoute{
			HostID: "pooled-host-1", HostGeneration: 1, AgentID: "agent-coder", RuntimeCompatibilityID: "runtime-2026-09",
			Placement: sessionwire.HostPlacementPooled, InternalEndpoint: "ws://pooled-host-1.looprig-hosts.svc:7443/hostlink/tenant-acme",
			Residency: sessionwire.SessionResidencyResident, Accepting: true,
		},
	}); err != nil {
		t.Fatal(err)
	}
	r.store.beforeClear = nil
	// The pooled route owns the session, so the fast path stands back; the
	// stale decision is finished once the session is next reconciled with no
	// owner. Model that by letting the pooled route lapse.
	if item := r.pass("replica-b"); item.Outcome != driver.OutcomeOwned {
		t.Fatalf("item = %+v, want owned by the pooled Host", item)
	}
	r.clock.advance(51 * time.Minute)
	wantOutcome(t, r.pass("replica-c"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
	if reg := r.store.cleared; len(reg) != 2 || reg[1].LeaseEpoch != tdEpoch {
		t.Fatalf("cleared = %v, want the retried fence at %d", reg, tdEpoch)
	}
}

// An observation that is not about this workload never makes it graceful.
func TestAForeignDrainedObservationIsNotGraceful(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = func(_ string, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
		return sessionwire.HostLinkDrainObservation{
			HostID: req.HostID, HostGeneration: req.HostGeneration + 1, DrainGeneration: 1,
			State: sessionwire.HostLinkDrainStateDrained, TenantID: req.TenantID, SessionID: req.SessionID,
		}, nil
	}
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if len(r.store.recorded) != 0 || len(r.deletes()) != 0 {
		t.Fatalf("a foreign observation advanced the teardown")
	}
	r.clock.advance(tdDrainTimeout)
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedDrainRefused, tdEpoch)
}

// G10's recreated incarnation shares its generation with the one that
// crashed; the store keeps one outcome per generation, so the second end is
// unrecordable (mismatch) and must still release the workload.
func TestARecreatedGenerationsSecondEndIsUnrecordableButReleased(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	if err := r.api.SetStatus(namespace, name, corev1.PodStatus{Phase: corev1.PodFailed}); err != nil {
		t.Fatal(err)
	}
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeEnsured)
	first := r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedWorkloadTerminated, 0)
	r.desire("delete-1", nil)
	wantOutcome(t, r.pass("replica-c"), driver.OutcomeTearingDown)
	if r.podOrNil(name) != nil {
		t.Fatalf("an unrecordable outcome held the workload")
	}
	wantOutcome(t, r.pass("replica-d"), driver.OutcomeDeleted)
	again := r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedWorkloadTerminated, 0)
	if again.Revision != first.Revision {
		t.Fatalf("the stored outcome was rewritten")
	}
}

// A higher generation is never recorded while a lower one is still draining:
// that would supersede the lower one's outcome for good.
func TestAHigherGenerationWaitsForALowerOneStillDraining(t *testing.T) {
	r := newTDRig(t)
	firstName, first := r.running()
	r.register(firstName, first.Generation, tdEpoch)
	workload := first.Workload
	r.desire("replace-1", &workload)
	second := r.intent()
	secondPod := r.podOrNil(firstName).DeepCopy()
	secondPod.Name = kubernetes.WorkloadName(second)
	secondPod.UID, secondPod.ResourceVersion = "", ""
	secondPod.Labels[kubernetes.LabelWorkload] = secondPod.Name
	secondPod.Labels[kubernetes.LabelGeneration] = fmt.Sprint(second.Generation)
	if _, err := r.api.CoreV1().Pods(namespace).Create(context.Background(), secondPod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDraining)
	for _, holder := range []string{"replica-a", "replica-b"} {
		wantOutcome(t, r.pass(holder), driver.OutcomeTearingDown)
	}
	if len(r.store.recorded) != 0 || len(r.deletes()) != 0 {
		t.Fatalf("recorded %v and deleted %v while generation %d still drains", r.store.recorded, r.deletes(), first.Generation)
	}
	if pod := r.podOrNil(secondPod.Name); pod == nil || pod.Annotations[kubernetes.AnnotationTermination] != "" {
		t.Fatalf("generation %d was decided before generation %d finished", second.Generation, first.Generation)
	}
}

// A workload for a generation the desire has not issued fails the item closed:
// nothing is drained, deleted or created.
func TestAWorkloadAboveTheDesiredGenerationFailsClosed(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	pod := r.podOrNil(name).DeepCopy()
	pod.Name = "lrh-" + fmt.Sprintf("%056d", 9)
	pod.UID, pod.ResourceVersion = "", ""
	pod.Labels[kubernetes.LabelWorkload] = pod.Name
	pod.Labels[kubernetes.LabelGeneration] = fmt.Sprint(intent.Generation + 5)
	if _, err := r.api.CoreV1().Pods(namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	item := r.pass("replica-a")
	if item.Outcome != driver.OutcomeFailed || !errors.Is(item.Err, driver.ErrNewerWorkload) {
		t.Fatalf("item = %+v, want ErrNewerWorkload", item)
	}
	if len(r.drainer.methods()) != 0 || len(r.deletes()) != 0 || len(r.store.recorded) != 0 {
		t.Fatalf("a fail-closed item acted: drains %v deletes %v records %v", r.drainer.methods(), r.deletes(), r.store.recorded)
	}
}

// A released workload whose object the kubelet has not yet removed is left
// alone: no second fence, record or release, and nothing is created beside it.
func TestAReleasedWorkloadStillStoppingIsLeftAlone(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	workload := intent.Workload
	r.desire("replace-1", &workload)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	// The kubelet has not finished: releasing the finalizer leaves the object.
	r.api.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtimeObject, error) {
		pod := action.(k8stesting.UpdateAction).GetObject().(*corev1.Pod).DeepCopy()
		if pod.DeletionTimestamp == nil || slices.Contains(pod.Finalizers, kubernetes.Finalizer) {
			return false, nil, nil
		}
		stored, err := r.api.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), namespace, pod.Name)
		if err != nil {
			return true, nil, err
		}
		kept := stored.(*corev1.Pod).DeepCopy()
		kept.Finalizers, kept.Annotations = pod.Finalizers, pod.Annotations
		kept.ResourceVersion = pod.ResourceVersion + "0"
		return true, kept, r.api.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), kept, namespace)
	})
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
	clears, records := len(r.store.cleared), len(r.store.recorded)
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	if len(r.store.cleared) != clears || len(r.store.recorded) != records {
		t.Fatalf("a finished teardown was repeated: clears %d->%d records %d->%d", clears, len(r.store.cleared), records, len(r.store.recorded))
	}
	if n := creates(&fakeapi.Server{Clientset: r.api.Clientset}); n != 1 {
		t.Fatalf("creates = %d, want only the first generation's while its object remains", n)
	}
}

// The epoch is bound to THIS workload only by a route naming its HostID AND
// its host generation; a route naming either one alone is someone else's.
func TestARouteNamingOnlyHalfTheWorkloadIsNotItsRoute(t *testing.T) {
	for name, route := range map[string]func(string, uint64) (string, uint64){
		"same host, other generation": func(n string, g uint64) (string, uint64) { return n, g + 1 },
		"other host, same generation": func(_ string, g uint64) (string, uint64) { return "lrh-other-host", g },
	} {
		t.Run(name, func(t *testing.T) {
			r := newTDRig(t)
			podName, intent := r.running()
			workload := intent.Workload
			r.desire("replace-1", &workload)
			r.desire("delete-1", nil) // desire gen 3; a route at gen <= 2 is not "owned"
			host, gen := route(podName, intent.Generation)
			r.register(host, gen, tdEpoch)
			r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
			wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
			if n := len(r.drainer.methods()); n != 0 {
				t.Fatalf("drained through a route that is not this workload's (%d calls)", n)
			}
			if len(r.store.cleared) != 0 {
				t.Fatalf("fenced another workload's route: %v", r.store.cleared)
			}
			r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedDrainRefused, 0)
		})
	}
}

// A fence that finds no registration at all has nothing to fence: the
// teardown proceeds.
func TestAFenceWithNoRegistrationProceeds(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	r.store.beforeClear = func(sessionstore.ClearHostRegistrationRequest) error {
		return &sessionstore.RegistryError{Code: sessionstore.RegistryErrorNotFound, Field: "record"}
	}
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
	if r.podOrNil(name) != nil {
		t.Fatalf("pod not deleted")
	}
}

// ---------------------------------------------------------------------------
// Fix round (gates on 5261cc9).
// ---------------------------------------------------------------------------

func (r *tdRig) hostReleases(epoch uint64) {
	r.t.Helper()
	if _, err := r.store.Store.ClearHostRegistration(context.Background(), sessionstore.ClearHostRegistrationRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, LeaseEpoch: epoch,
	}); err != nil {
		r.t.Fatal(err)
	}
}

// Spec gate F1 (its probe, committed): an epoch-0 decision -- no route named
// the Pod -- is persisted, the delete fails, and the Host in that SAME Pod then
// registers at epoch 5. The old decision must not delete the new owner.
func TestAnEpochZeroDecisionNeverDeletesALaterOwner(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDraining)
	failDelete := true
	r.api.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtimeObject, error) {
		if failDelete {
			failDelete = false
			return true, nil, apierrors.NewServiceUnavailable("injected")
		}
		return false, nil, nil
	})
	if item := r.pass("replica-a"); item.Outcome != driver.OutcomeFailed {
		t.Fatalf("item = %+v, want the injected delete failure", item)
	}
	r.register(name, intent.Generation, 5)
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	if n := len(r.deletes()); n != 1 { // the one injected failure only
		t.Fatalf("deletes = %v: the old epoch-0 decision deleted the owner at epoch 5", r.deletes())
	}
	if pod := r.podOrNil(name); pod == nil || pod.DeletionTimestamp != nil || pod.Annotations[kubernetes.AnnotationTermination] != "" {
		t.Fatalf("the overtaken decision must be dropped and the pod left standing: %+v", pod)
	}
	r.wantNoTermination()
	// The next pass drains the owner at ITS epoch.
	wantOutcome(t, r.pass("replica-c"), driver.OutcomeTearingDown)
	if got := r.drainer.methods(); !slices.Equal(got, []string{sessionwire.HostLinkMethodDrain}) {
		t.Fatalf("drain calls = %v, want the owner drained", got)
	}
}

// Quality gate Q2 (committed): the fence at epoch 3 succeeds, then the same
// Pod's Host registers again at epoch 5 BEFORE the pre-delete read. The fence
// refuses only lower epochs, so only that read can stop the delete.
func TestAHostThatRegistersAgainAfterTheFenceIsNotDeleted(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	r.store.afterClear = func(sessionstore.ClearHostRegistrationRequest) {
		r.store.afterClear = nil
		r.register(name, intent.Generation, tdEpoch+2)
	}
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if len(r.deletes()) != 0 {
		t.Fatalf("deleted the pod while a later lease (epoch %d) named it", tdEpoch+2)
	}
	r.wantNoTermination()
	if reg, err := r.registration(); err != nil || reg.Registration.LeaseEpoch != tdEpoch+2 {
		t.Fatalf("the new owner's route = %+v, %v", reg, err)
	}
	if pod := r.podOrNil(name); pod == nil || pod.Annotations[kubernetes.AnnotationTermination] != "" {
		t.Fatalf("the overtaken decision was not dropped")
	}
}

// The window that remains, pinned so it is not mistaken for covered: a
// registration landing between the controller's last registry read and the
// API server applying the delete is not seen. The Pod is deleted and its
// Host's SIGTERM drain is the backstop. The README states exactly this.
func TestARegistrationInsideTheDeleteWindowIsLeftToTheBackstop(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	once := true
	r.api.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtimeObject, error) {
		if once {
			once = false
			r.register(name, intent.Generation, tdEpoch+2)
		}
		return false, nil, nil
	})
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if len(r.deletes()) != 1 {
		t.Fatalf("deletes = %v", r.deletes())
	}
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
}

// Quality gate Q1 (its probe, committed): the Host finishes and releases
// between the look and hostlink.drain, so drain is refused. The next pass must
// ask drain_status and record graceful, not a permanent forced drain_refused.
func TestAReleaseBetweenTheLookAndTheDrainIsObservedNotForced(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDraining)
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	released := false
	r.drainer.answer = func(method string, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
		if method == sessionwire.HostLinkMethodDrain {
			if !released {
				released = true
				r.hostReleases(tdEpoch)
			}
			return sessionwire.HostLinkDrainObservation{}, &hostlink.RefusalError{Method: method, Refusal: sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorRuntimeUnavailable}}
		}
		return answering(sessionwire.HostLinkDrainStateDrained)(method, req)
	}
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	if len(r.store.recorded) != 0 {
		t.Fatalf("a refusal racing the Host's release was recorded as final: %v", r.store.recorded)
	}
	wantOutcome(t, r.pass("replica-c"), driver.OutcomeTearingDown)
	if got := r.drainer.methods(); !slices.Equal(got, []string{sessionwire.HostLinkMethodDrain, sessionwire.HostLinkMethodDrain, sessionwire.HostLinkMethodDrainStatus}) {
		t.Fatalf("drain calls = %v", got)
	}
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
}

// Spec gate F2: the Host released on its own -- an unclean release writes the
// same tombstone -- before any drain was begun. The Kind is never read from
// the registry: forced drain_refused at epoch 0, not graceful at the
// tombstone's epoch.
func TestAHostThatReleasedOnItsOwnBeforeAnyDrainIsForcedAtEpochZero(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.hostReleases(tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if n := len(r.drainer.methods()); n != 0 {
		t.Fatalf("drain calls = %d, want none", n)
	}
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationForced, sessionstore.PlacementForcedDrainRefused, 0)
}

// Quality gate Q3 (its probe, committed): after the controller's OWN delete
// and a crash before the record, a later epoch naming the terminating Pod must
// not re-decide that delete as platform_deleted.
func TestTheControllersOwnDeleteIsNeverRelabelledPlatformDeleted(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	crash := errors.New("controller crashed")
	r.store.beforeRecord = func(sessionstore.RecordPlacementTerminationRequest) error { return crash }
	if item := r.pass("replica-a"); !errors.Is(item.Err, crash) {
		t.Fatalf("item = %+v", item)
	}
	r.store.beforeRecord = nil
	r.register(name, intent.Generation, tdEpoch+2)
	for i := 0; i < 3; i++ {
		_ = r.pass(fmt.Sprintf("replica-%d", i))
	}
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
}

// Quality gate Q4 (its probe, committed): a Pod NOT held by this controller's
// finalizer (stripped, or created before it had one) lingers after the delete,
// as a real API server keeps a Pod through its grace period. A crash between
// its delete and its record must still record.
func TestAnUnheldPodCrashAfterDeleteStillRecords(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	pod := r.podOrNil(name)
	pod.Finalizers = []string{"example.com/other"}
	if _, err := r.api.CoreV1().Pods(namespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	crash := errors.New("controller crashed")
	r.store.beforeRecord = func(sessionstore.RecordPlacementTerminationRequest) error { return crash }
	if item := r.pass("replica-a"); !errors.Is(item.Err, crash) {
		t.Fatalf("item = %+v", item)
	}
	r.store.beforeRecord = nil
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
}

// Spec gate O7: the drain start is persisted BEFORE the first drain RPC. If
// persisting fails, no drain is asked for.
func TestNoDrainIsAskedForBeforeItsStartIsPersisted(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDraining)
	r.api.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtimeObject, error) {
		if _, ok := action.(k8stesting.UpdateAction).GetObject().(*corev1.Pod).Annotations[kubernetes.AnnotationDrain]; ok {
			return true, nil, apierrors.NewServiceUnavailable("injected")
		}
		return false, nil, nil
	})
	if item := r.pass("replica-a"); item.Outcome != driver.OutcomeFailed {
		t.Fatalf("item = %+v, want the persist failure", item)
	}
	if n := len(r.drainer.methods()); n != 0 {
		t.Fatalf("drain asked for %d times with its start unpersisted", n)
	}
}

// Quality gate Q9: a route binds to this workload only if it is DEDICATED. A
// pooled route that happens to carry this Pod's HostID and generation at a
// later epoch is not this Pod's owner and does not stop the delete.
func TestAPooledRouteIsNeverThisWorkloadsRoute(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	r.store.afterClear = func(sessionstore.ClearHostRegistrationRequest) {
		r.store.afterClear = nil
		if _, err := r.store.PutHostRegistration(context.Background(), sessionstore.PutHostRegistrationRequest{
			TenantID: r.key.TenantID, SessionID: r.key.SessionID, LeaseEpoch: tdEpoch + 2,
			ObservedAt: r.clock.Now(), ExpiresAt: r.clock.Now().Add(50 * time.Minute),
			Route: sessionstore.HostRoute{
				HostID: sessionwire.HostID(name), HostGeneration: intent.Generation,
				AgentID: "agent-coder", RuntimeCompatibilityID: "runtime-2026-09",
				Placement: sessionwire.HostPlacementPooled, InternalEndpoint: "ws://pooled.looprig-hosts.svc:7443/hostlink/tenant-acme",
				Residency: sessionwire.SessionResidencyResident, Accepting: true,
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if len(r.deletes()) != 1 {
		t.Fatalf("deletes = %v, want the delete to proceed past a pooled route", r.deletes())
	}
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
}
