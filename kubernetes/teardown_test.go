package kubernetes

import (
	"context"
	"errors"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	k8stesting "k8s.io/client-go/testing"

	"github.com/looprig/controller/internal/fakeapi"
	"github.com/looprig/controller/workload"
)

// ---------------------------------------------------------------------------
// Step 3: the failure backstop's timing.
// ---------------------------------------------------------------------------

func TestTheDrainCeilingIsStrictlyShorterThanTheGracePeriod(t *testing.T) {
	for _, tc := range []struct {
		ceiling, margin time.Duration
		want            int64
	}{
		{30 * time.Second, 5 * time.Second, 35},
		{30 * time.Second, 5*time.Second + 500*time.Millisecond, 36}, // rounded UP, never down
		{time.Nanosecond, MinCommitMargin, 6},
		{45 * time.Second, 15 * time.Second, 60},
	} {
		got := TerminationGraceSeconds(tc.ceiling, tc.margin)
		if got != tc.want {
			t.Fatalf("TerminationGraceSeconds(%v, %v) = %d, want %d", tc.ceiling, tc.margin, got, tc.want)
		}
		grace := time.Duration(got) * time.Second
		if !(tc.ceiling < grace) || grace-tc.ceiling < tc.margin || grace-tc.ceiling < MinCommitMargin {
			t.Fatalf("grace %v for ceiling %v leaves %v, want strictly longer by at least %v", grace, tc.ceiling, grace-tc.ceiling, tc.margin)
		}
	}
	if MinCommitMargin != 5*time.Second {
		t.Fatalf("MinCommitMargin = %v, want the documented 5s floor", MinCommitMargin)
	}
}

func TestConfigRefusesAnUnsafeBackstop(t *testing.T) {
	server := newAPIServer(t)
	for name, tc := range map[string]struct {
		mutate func(*Config)
		field  string
	}{
		"zero ceiling":             {func(c *Config) { c.DrainCeiling = 0 }, "DrainCeiling"},
		"negative ceiling":         {func(c *Config) { c.DrainCeiling = -time.Second }, "DrainCeiling"},
		"margin below the minimum": {func(c *Config) { c.CommitMargin = MinCommitMargin - time.Nanosecond }, "CommitMargin"},
		"zero margin":              {func(c *Config) { c.CommitMargin = 0 }, "CommitMargin"},
		"grace over an hour": {func(c *Config) {
			c.DrainCeiling = MaxTerminationGrace - MinCommitMargin + time.Nanosecond
			c.CommitMargin = MinCommitMargin
		}, "DrainCeiling"},
	} {
		cfg := testConfig(server, &fakeRegistry{})
		tc.mutate(&cfg)
		_, err := New(cfg)
		var fieldErr *FieldError
		if !errors.As(err, &fieldErr) || fieldErr.Field != tc.field || !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%s: err = %v, want a FieldError on %s", name, err, tc.field)
		}
	}
	for name, mutate := range map[string]func(*Config){
		"minimum margin": func(c *Config) { c.CommitMargin = MinCommitMargin },
		"exactly one hour": func(c *Config) {
			c.DrainCeiling = MaxTerminationGrace - MinCommitMargin
			c.CommitMargin = MinCommitMargin
		},
	} {
		cfg := testConfig(server, &fakeRegistry{})
		mutate(&cfg)
		if _, err := New(cfg); err != nil {
			t.Fatalf("control %s refused: %v", name, err)
		}
	}
}

func TestTheRenderedPodCarriesTheBackstopAndTheFinalizer(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 3)
	ensure(t, c, intent)
	pod := server.pod(t, WorkloadName(intent))
	if pod.Spec.TerminationGracePeriodSeconds == nil || *pod.Spec.TerminationGracePeriodSeconds != 60 {
		t.Fatalf("terminationGracePeriodSeconds = %v, want 60 (45s ceiling + 15s margin)", pod.Spec.TerminationGracePeriodSeconds)
	}
	if !slices.Equal(pod.Finalizers, []string{Finalizer}) {
		t.Fatalf("finalizers = %v, want exactly [%s]", pod.Finalizers, Finalizer)
	}
	// No preStop hook: the Host drains on SIGTERM, and a hook would only
	// spend the grace period before the signal that starts that drain.
	if pod.Spec.Containers[0].Lifecycle != nil {
		t.Fatalf("container lifecycle = %+v, want none", pod.Spec.Containers[0].Lifecycle)
	}
}

func TestAHostDrainGraceAboveTheCeilingIsRefusedBeforeAnyCall(t *testing.T) {
	for _, tc := range []struct {
		grace string
		ok    bool
	}{
		{"45s", true}, // equal to the ceiling: the grace period still exceeds it by the margin
		{"45001ms", false},
		{"1m", false},
	} {
		server := newAPIServer(t, testSecret())
		c := newTestController(t, server, &fakeRegistry{})
		payload := testPayload()
		payload.HostSettings["HOST_DRAIN_GRACE"] = tc.grace
		intent := testIntent(t, 1)
		intent.Workload.Payload = encodePayload(t, payload)
		err := c.EnsureWorkload(context.Background(), intent)
		if tc.ok {
			if err != nil {
				t.Fatalf("HOST_DRAIN_GRACE=%s refused: %v", tc.grace, err)
			}
			continue
		}
		if !errors.Is(err, ErrInvalidPayload) || !strings.Contains(err.Error(), "drain ceiling") {
			t.Fatalf("HOST_DRAIN_GRACE=%s: err = %v, want ErrInvalidPayload naming the ceiling", tc.grace, err)
		}
		if n := len(server.Actions()); n != 0 {
			t.Fatalf("HOST_DRAIN_GRACE=%s: %d cluster actions, want none", tc.grace, n)
		}
	}
}

// ---------------------------------------------------------------------------
// G3: a draining Host must stay resolvable.
// ---------------------------------------------------------------------------

func TestTheHostsServicePublishesNotReadyAddresses(t *testing.T) {
	raw, err := os.ReadFile("../deploy/hosts-service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode(raw, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, ok := obj.(*corev1.Service)
	if !ok {
		t.Fatalf("hosts-service.yaml decodes to %T", obj)
	}
	// A draining Host reports not-ready (/readyz answers accepting only), and
	// the controller must still resolve it to observe drain_status.
	if !service.Spec.PublishNotReadyAddresses {
		t.Fatalf("publishNotReadyAddresses = false; a draining Host's DNS record would vanish mid-drain")
	}
	if service.Spec.ClusterIP != corev1.ClusterIPNone || service.Spec.Type != "" && service.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("service must stay headless ClusterIP: %+v", service.Spec)
	}
	if service.Spec.Selector[LabelManagedBy] != ManagedByValue {
		t.Fatalf("selector = %v", service.Spec.Selector)
	}
}

// ---------------------------------------------------------------------------
// Teardown operations.
// ---------------------------------------------------------------------------

type teardownRig struct {
	api *fakeapi.Server
	c   *Controller
}

func newTeardownRig(t *testing.T) *teardownRig {
	t.Helper()
	api := fakeapi.New(testSecret())
	cfg := testConfig(&apiServer{Clientset: api.Clientset}, &fakeRegistry{})
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &teardownRig{api: api, c: c}
}

func (r *teardownRig) pod(t *testing.T, name string) *corev1.Pod {
	t.Helper()
	pod, err := r.api.CoreV1().Pods(testNamespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	return pod
}

func (r *teardownRig) list(t *testing.T) []workload.Workload {
	t.Helper()
	ws, err := r.c.ListWorkloads(context.Background(), "tenant-acme", "session-0001")
	if err != nil {
		t.Fatalf("ListWorkloads: %v", err)
	}
	return ws
}

func TestListWorkloadsReportsWhatThePlatformHolds(t *testing.T) {
	r := newTeardownRig(t)
	ensure(t, r.c, testIntent(t, 2))
	name := WorkloadName(testIntent(t, 2))
	ws := r.list(t)
	pod := r.pod(t, name)
	want := workload.Workload{
		Name: name, UID: string(pod.UID), Revision: pod.ResourceVersion, Generation: 2,
		Endpoint: sessionwire.InternalEndpoint("ws://" + name + "." + testSubdomain + "." + testNamespace + ".svc:7443/hostlink/tenant-acme"),
		Held:     true,
	}
	if len(ws) != 1 || !reflect.DeepEqual(ws[0], want) {
		t.Fatalf("workloads = %+v, want exactly [%+v]", ws, want)
	}
	if err := r.api.SetStatus(testNamespace, name, corev1.PodStatus{Phase: corev1.PodFailed}); err != nil {
		t.Fatal(err)
	}
	if err := r.api.CoreV1().Pods(testNamespace).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	ws = r.list(t)
	if len(ws) != 1 || !ws[0].Terminal || !ws[0].Terminating || !ws[0].Held {
		t.Fatalf("workload = %+v, want terminal, terminating and still held", ws)
	}
}

func TestListWorkloadsRefusesAPodItCannotVouchFor(t *testing.T) {
	for name, edit := range map[string]func(*corev1.Pod){
		"foreign owner":        func(p *corev1.Pod) { p.Labels[LabelOwner] = "someone-else" },
		"no generation":        func(p *corev1.Pod) { delete(p.Labels, LabelGeneration) },
		"workload label moved": func(p *corev1.Pod) { p.Labels[LabelWorkload] = "other" },
		"owner references":     func(p *corev1.Pod) { p.OwnerReferences = []metav1.OwnerReference{{Name: "rs"}} },
		"malformed drain mark": func(p *corev1.Pod) { p.Annotations[AnnotationDrain] = `{"epoch":3}` },
		"malformed decision":   func(p *corev1.Pod) { p.Annotations[AnnotationTermination] = `{"kind":"graceful","epoch":0}` },
		"case-variant decision": func(p *corev1.Pod) {
			p.Annotations[AnnotationTermination] = `{"Kind":"forced","reason":"drain_refused","epoch":0}`
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := newTeardownRig(t)
			intent := testIntent(t, 1)
			ensure(t, r.c, intent)
			pod := r.pod(t, WorkloadName(intent))
			edit(pod)
			if _, err := r.api.CoreV1().Pods(testNamespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			_, err := r.c.ListWorkloads(context.Background(), "tenant-acme", "session-0001")
			if !errors.Is(err, ErrOwnershipConflict) && !errors.Is(err, ErrMalformedMark) {
				t.Fatalf("err = %v, want an ownership or malformed-mark refusal", err)
			}
		})
	}
}

func TestMarksRoundTripAndAreCompareAndSwapped(t *testing.T) {
	r := newTeardownRig(t)
	ensure(t, r.c, testIntent(t, 1))
	w := r.list(t)[0]
	started := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	marked, err := r.c.MarkDrain(context.Background(), "tenant-acme", "session-0001", w, workload.Drain{Epoch: 7, StartedAt: started})
	if err != nil {
		t.Fatal(err)
	}
	if marked.Drain == nil || *marked.Drain != (workload.Drain{Epoch: 7, StartedAt: started}) || marked.Revision == w.Revision {
		t.Fatalf("marked = %+v, want the drain read back at a new revision", marked)
	}
	if got := r.pod(t, w.Name).Annotations[AnnotationDrain]; got != `{"epoch":7,"started_at":"2026-09-18T12:00:00Z"}` {
		t.Fatalf("drain annotation = %s", got)
	}
	// The stale view (w) is refused, and nothing is written.
	if _, err := r.c.MarkDecision(context.Background(), "tenant-acme", "session-0001", w, workload.Graceful(7)); !errors.Is(err, ErrWorkloadChanged) {
		t.Fatalf("stale mark err = %v, want ErrWorkloadChanged", err)
	}
	if _, ok := r.pod(t, w.Name).Annotations[AnnotationTermination]; ok {
		t.Fatalf("a refused mark was written")
	}
	decided, err := r.c.MarkDecision(context.Background(), "tenant-acme", "session-0001", marked, workload.Forced(sessionstore.PlacementForcedDrainTimeout, 7))
	if err != nil {
		t.Fatal(err)
	}
	if decided.Decision == nil || *decided.Decision != workload.Forced(sessionstore.PlacementForcedDrainTimeout, 7) {
		t.Fatalf("decision = %+v", decided.Decision)
	}
	if got := r.pod(t, w.Name).Annotations[AnnotationTermination]; got != `{"kind":"forced","reason":"drain_timeout","epoch":7}` {
		t.Fatalf("termination annotation = %s", got)
	}
	// An invalid decision is refused before any call.
	before := len(r.api.Actions())
	if _, err := r.c.MarkDecision(context.Background(), "tenant-acme", "session-0001", decided, workload.Graceful(0)); !errors.Is(err, ErrMalformedMark) {
		t.Fatalf("graceful at epoch 0: err = %v, want ErrMalformedMark", err)
	}
	if len(r.api.Actions()) != before {
		t.Fatalf("an invalid decision reached the API server")
	}
	cleared, err := r.c.ClearMarks(context.Background(), "tenant-acme", "session-0001", decided)
	if err != nil {
		t.Fatal(err)
	}
	if cleared.Drain != nil || cleared.Decision != nil {
		t.Fatalf("cleared = %+v, want no marks", cleared)
	}
	// A replaced object is refused even at a matching revision.
	if _, err := r.api.Replace(testNamespace, w.Name); err != nil {
		t.Fatal(err)
	}
	fresh := r.list(t)[0]
	fresh.UID = cleared.UID
	if _, err := r.c.MarkDrain(context.Background(), "tenant-acme", "session-0001", fresh, workload.Drain{Epoch: 1, StartedAt: started}); !errors.Is(err, ErrWorkloadChanged) {
		t.Fatalf("replaced object: err = %v, want ErrWorkloadChanged", err)
	}
}

func TestTerminateIsPreconditionedAndReleaseHoldsTheObjectUntilThen(t *testing.T) {
	r := newTeardownRig(t)
	ensure(t, r.c, testIntent(t, 1))
	w := r.list(t)[0]
	if err := r.c.Terminate(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	var sent *metav1.Preconditions
	for _, a := range r.api.Actions() {
		if a.GetVerb() == "delete" {
			sent = a.(k8stesting.DeleteActionImpl).DeleteOptions.Preconditions
		}
	}
	if sent == nil || sent.UID == nil || string(*sent.UID) != w.UID {
		t.Fatalf("delete preconditions = %+v, want UID %s", sent, w.UID)
	}
	// The finalizer holds the object terminating until Release.
	held := r.list(t)
	if len(held) != 1 || !held[0].Terminating || !held[0].Held {
		t.Fatalf("after delete = %+v, want the object held terminating", held)
	}
	if err := r.c.Release(context.Background(), held[0]); err != nil {
		t.Fatal(err)
	}
	if ws := r.list(t); len(ws) != 0 {
		t.Fatalf("after release = %+v, want the object gone", ws)
	}
	// Idempotent on an absent object.
	if err := r.c.Terminate(context.Background(), w); err != nil {
		t.Fatalf("repeat terminate: %v", err)
	}
	if err := r.c.Release(context.Background(), w); err != nil {
		t.Fatalf("repeat release: %v", err)
	}
}

func TestTerminateAndReleaseRefuseAReplacedObject(t *testing.T) {
	r := newTeardownRig(t)
	ensure(t, r.c, testIntent(t, 1))
	w := r.list(t)[0]
	replacement, err := r.api.Replace(testNamespace, w.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.c.Terminate(context.Background(), w); !errors.Is(err, ErrWorkloadReplaced) {
		t.Fatalf("terminate err = %v, want ErrWorkloadReplaced", err)
	}
	if err := r.c.Release(context.Background(), w); !errors.Is(err, ErrWorkloadReplaced) {
		t.Fatalf("release err = %v, want ErrWorkloadReplaced", err)
	}
	pod := r.pod(t, w.Name)
	if pod.UID != replacement || pod.DeletionTimestamp != nil || !slices.Contains(pod.Finalizers, Finalizer) {
		t.Fatalf("the replacement was touched: %+v", pod.ObjectMeta)
	}
}

func TestReleaseRemovesOnlyTheControllersFinalizer(t *testing.T) {
	r := newTeardownRig(t)
	ensure(t, r.c, testIntent(t, 1))
	w := r.list(t)[0]
	pod := r.pod(t, w.Name)
	pod.Finalizers = append(pod.Finalizers, "example.com/other")
	if _, err := r.api.CoreV1().Pods(testNamespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.c.Release(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if got := r.pod(t, w.Name).Finalizers; !slices.Equal(got, []string{"example.com/other"}) {
		t.Fatalf("finalizers = %v, want only the foreign one left", got)
	}
}

func TestMarkCodecsAreStrict(t *testing.T) {
	for _, raw := range []string{
		`{"epoch":3}`,
		`{"epoch":0,"started_at":"2026-09-18T12:00:00Z"}`,
		`{"epoch":3,"started_at":"2026-09-18T12:00:00Z","extra":1}`,
		`{"epoch":3,"epoch":4,"started_at":"2026-09-18T12:00:00Z"}`,
		`{"EPOCH":3,"started_at":"2026-09-18T12:00:00Z"}`,
		`{"epoch":3,"started_at":"2026-09-18T12:00:00Z"} {}`,
	} {
		if _, err := decodeDrain(raw); !errors.Is(err, ErrMalformedMark) {
			t.Fatalf("drain %s: err = %v, want ErrMalformedMark", raw, err)
		}
	}
	for _, raw := range []string{
		`{"kind":"graceful","epoch":0}`,
		`{"kind":"graceful","reason":"drain_timeout","epoch":3}`,
		`{"kind":"forced","epoch":3}`,
		`{"kind":"forced","reason":"evicted","epoch":3}`,
		`{"kind":"abandoned","epoch":3}`,
		`{"kind":"forced","reason":"drain_timeout","epoch":3,"x":1}`,
	} {
		if _, err := decodeDecision(raw); !errors.Is(err, ErrMalformedMark) {
			t.Fatalf("decision %s: err = %v, want ErrMalformedMark", raw, err)
		}
	}
	for raw, want := range map[string]workload.Decision{
		`{"kind":"graceful","epoch":3}`:                           workload.Graceful(3),
		`{"kind":"forced","reason":"platform_deleted","epoch":0}`: workload.Forced(sessionstore.PlacementForcedPlatformDeleted, 0),
	} {
		got, err := decodeDecision(raw)
		if err != nil || got != want {
			t.Fatalf("decision %s = %+v, %v; want %+v", raw, got, err, want)
		}
	}
}
