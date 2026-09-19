package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory"
	"github.com/looprig/sessionstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8stesting "k8s.io/client-go/testing"
)

// The adapter is Factory's released seam, not a look-alike.
var _ factory.WorkloadController = (*Controller)(nil)

var namePattern = regexp.MustCompile(`^lrh-[0-9a-f]{56}$`)
var hashLabelPattern = regexp.MustCompile(`^[0-9a-f]{56}$`)

func ensure(t *testing.T, c *Controller, intent sessionstore.PlacementIntent) {
	t.Helper()
	if err := c.EnsureWorkload(context.Background(), intent); err != nil {
		t.Fatalf("EnsureWorkload: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Deterministic create.
// ---------------------------------------------------------------------------

func TestEnsureCreatesExactlyOneDeterministicPod(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 3)

	ensure(t, c, intent)

	if got, want := server.verbs(), []string{"list", "get", "create"}; !slices.Equal(got, want) {
		t.Fatalf("pod verbs = %v, want exactly %v", got, want)
	}
	pods := server.pods(t)
	if len(pods) != 1 {
		t.Fatalf("pods = %d, want exactly 1", len(pods))
	}
	pod := pods[0]
	name := WorkloadName(intent)
	if pod.Name != name || !namePattern.MatchString(name) {
		t.Fatalf("pod name = %q, derived %q; want the derived hash name matching %s", pod.Name, name, namePattern)
	}

	// Determinism across processes: an independent controller over an
	// independent API server derives the identical name and identical spec.
	other := newAPIServer(t, testSecret())
	ensure(t, newTestController(t, other, &fakeRegistry{}), testIntent(t, 3))
	otherPod := other.pods(t)[0]
	if otherPod.Name != pod.Name {
		t.Fatalf("second derivation named %q, first %q", otherPod.Name, pod.Name)
	}
	if !reflect.DeepEqual(otherPod.Spec, pod.Spec) || !reflect.DeepEqual(otherPod.Labels, pod.Labels) ||
		!reflect.DeepEqual(otherPod.Annotations, pod.Annotations) {
		t.Fatalf("two derivations of one intent rendered different Pods")
	}

	wantLabels := map[string]string{
		LabelManagedBy:  ManagedByValue,
		LabelOwner:      ownerHash(testControllerID),
		LabelSession:    SessionHash(intent.TenantID, intent.SessionID),
		LabelWorkload:   name,
		LabelGeneration: "3",
	}
	if !reflect.DeepEqual(pod.Labels, wantLabels) {
		t.Fatalf("labels = %v, want exactly %v", pod.Labels, wantLabels)
	}
	if !hashLabelPattern.MatchString(pod.Labels[LabelOwner]) || !hashLabelPattern.MatchString(pod.Labels[LabelSession]) {
		t.Fatalf("owner/session labels are not hashes: %v", pod.Labels)
	}
	spec, err := renderedSpecHash(t, c, intent)
	if err != nil {
		t.Fatal(err)
	}
	wantAnnotations := map[string]string{
		AnnotationGeneration:     "3",
		AnnotationPayloadVersion: PayloadVersionV1,
		AnnotationSpecHash:       spec,
	}
	if !reflect.DeepEqual(pod.Annotations, wantAnnotations) {
		t.Fatalf("annotations = %v, want exactly %v", pod.Annotations, wantAnnotations)
	}

	// Host configuration: dedicated, capacity one, fixed session, no tenant.
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(pod.Spec.Containers))
	}
	container := pod.Spec.Containers[0]
	env := map[string]string{}
	for _, v := range container.Env {
		if v.ValueFrom != nil {
			t.Fatalf("env %s uses ValueFrom; the adapter sets literal non-secret values only", v.Name)
		}
		if _, dup := env[v.Name]; dup {
			t.Fatalf("env %s set twice", v.Name)
		}
		env[v.Name] = v.Value
	}
	wantEnv := testHostSettings()
	for k, v := range map[string]string{
		"HOST_ID":                string(HostID(intent)),
		"HOST_GENERATION":        "3",
		"HOST_INTERNAL_ENDPOINT": "ws://" + name + "." + testSubdomain + "." + testNamespace + ".svc:7443/hostlink/" + string(intent.TenantID),
		"HOST_ISOLATION_CLASS":   string(sessionwire.HostIsolationClassTenantExclusive),
		"HOST_PLACEMENT":         string(sessionwire.HostPlacementDedicated),
		"HOST_CAPACITY":          "1",
		"HOST_FIXED_SESSION_ID":  string(intent.SessionID),
		"HOST_LISTEN_ADDRESS":    ":7443",
	} {
		wantEnv[k] = v
	}
	if !reflect.DeepEqual(env, wantEnv) {
		t.Fatalf("env = %v\nwant exactly %v", env, wantEnv)
	}
	if err := sessionwire.InternalEndpoint(env["HOST_INTERNAL_ENDPOINT"]).Validate(); err != nil {
		t.Fatalf("rendered endpoint is not a valid Core InternalEndpoint: %v", err)
	}
	if container.Image != testImage {
		t.Fatalf("image = %q", container.Image)
	}
	wantProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: "/readyz", Port: intstr.FromString("hostlink"), Scheme: corev1.URISchemeHTTP,
		}},
		PeriodSeconds: 5, FailureThreshold: 3, SuccessThreshold: 1, TimeoutSeconds: 2,
	}
	if !reflect.DeepEqual(container.ReadinessProbe, wantProbe) || container.LivenessProbe != nil {
		t.Fatalf("probes = readiness %+v liveness %+v, want exactly /readyz and no liveness", container.ReadinessProbe, container.LivenessProbe)
	}
	if len(container.Ports) != 1 || container.Ports[0].Name != "hostlink" || container.Ports[0].ContainerPort != testPort {
		t.Fatalf("ports = %+v", container.Ports)
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("restartPolicy = %q, want Never", pod.Spec.RestartPolicy)
	}
	if pod.Spec.Hostname != name || pod.Spec.Subdomain != testSubdomain {
		t.Fatalf("hostname/subdomain = %q/%q", pod.Spec.Hostname, pod.Spec.Subdomain)
	}
	wantQuantity := map[corev1.ResourceName]resource.Quantity{
		corev1.ResourceCPU:              resource.MustParse("500m"),
		corev1.ResourceMemory:           resource.MustParse("1Gi"),
		corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
	}
	for _, list := range []corev1.ResourceList{container.Resources.Requests, container.Resources.Limits} {
		if len(list) != len(wantQuantity) {
			t.Fatalf("resources = %v", list)
		}
		for k, v := range wantQuantity {
			if got := list[k]; got.Cmp(v) != 0 {
				t.Fatalf("resource %s = %s, want %s", k, got.String(), v.String())
			}
		}
	}
}

func renderedSpecHash(t *testing.T, c *Controller, intent sessionstore.PlacementIntent) (string, error) {
	t.Helper()
	desired, err := c.desired(intent)
	if err != nil {
		return "", err
	}
	return desired.Annotations[AnnotationSpecHash], nil
}

// ---------------------------------------------------------------------------
// Adoption and duplicate reconcile.
// ---------------------------------------------------------------------------

func TestEnsureAdoptsAnExistingMatchingPodWithoutCreating(t *testing.T) {
	server := newAPIServer(t, testSecret())
	intent := testIntent(t, 1)
	ensure(t, newTestController(t, server, &fakeRegistry{}), intent)
	before := server.pod(t, WorkloadName(intent))
	server.ClearActions()

	// A restarted controller process: a new Controller, the same cluster.
	ensure(t, newTestController(t, server, &fakeRegistry{}), intent)

	if got, want := server.verbs(), []string{"list", "get"}; !slices.Equal(got, want) {
		t.Fatalf("adoption verbs = %v, want exactly %v", got, want)
	}
	after := server.pod(t, WorkloadName(intent))
	if after.UID != before.UID || after.UID == "" {
		t.Fatalf("adopted pod UID = %q, want the original %q", after.UID, before.UID)
	}
	if len(server.pods(t)) != 1 {
		t.Fatalf("pods = %d, want exactly 1", len(server.pods(t)))
	}
}

func TestDuplicateReconcileCreatesOnce(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 1)
	for range 3 {
		ensure(t, c, intent)
	}
	if got := server.count("create"); got != 1 {
		t.Fatalf("creates = %d, want exactly 1", got)
	}
	if got := server.count("delete") + server.count("update") + server.count("patch"); got != 0 {
		t.Fatalf("mutations other than the one create = %d, want 0", got)
	}
	if len(server.pods(t)) != 1 {
		t.Fatalf("pods = %d, want exactly 1", len(server.pods(t)))
	}
}

// ---------------------------------------------------------------------------
// Stale-generation replacement never silently deletes.
// ---------------------------------------------------------------------------

func TestEnsureNewGenerationRefusesAndDeletesNothing(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	old := testIntent(t, 1)
	ensure(t, c, old)
	oldPod := server.pod(t, WorkloadName(old))
	server.ClearActions()

	err := c.EnsureWorkload(context.Background(), testIntent(t, 2))

	if !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("err = %v, want ErrGenerationConflict", err)
	}
	var conflict *GenerationConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err %T is not a *GenerationConflictError", err)
	}
	if want := (GenerationConflictError{Intent: 2, Older: []uint64{1}}); !reflect.DeepEqual(*conflict, want) {
		t.Fatalf("conflict = %+v, want exactly %+v", *conflict, want)
	}
	if got, want := server.verbs(), []string{"list"}; !slices.Equal(got, want) {
		t.Fatalf("verbs = %v, want exactly %v (no create, no delete)", got, want)
	}
	pods := server.pods(t)
	if len(pods) != 1 || pods[0].Name != oldPod.Name || pods[0].UID != oldPod.UID {
		t.Fatalf("pods after refusal = %v, want exactly the generation-1 pod", podNames(pods))
	}
}

func TestEnsureOlderGenerationRefusesBehindANewerWorkload(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	ensure(t, c, testIntent(t, 5))
	server.ClearActions()

	err := c.EnsureWorkload(context.Background(), testIntent(t, 4))

	var conflict *GenerationConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %v, want *GenerationConflictError", err)
	}
	if want := (GenerationConflictError{Intent: 4, Newer: []uint64{5}}); !reflect.DeepEqual(*conflict, want) {
		t.Fatalf("conflict = %+v, want exactly %+v", *conflict, want)
	}
	if got, want := server.verbs(), []string{"list"}; !slices.Equal(got, want) {
		t.Fatalf("verbs = %v, want exactly %v", got, want)
	}
	if len(server.pods(t)) != 1 {
		t.Fatalf("pods = %d, want exactly 1", len(server.pods(t)))
	}
}

func podNames(pods []corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.Name)
	}
	return names
}

// ---------------------------------------------------------------------------
// Ownership / label mismatch fails closed.
// ---------------------------------------------------------------------------

func TestAdoptionMismatchFailsClosed(t *testing.T) {
	intent := testIntent(t, 1)
	name := WorkloadName(intent)
	cases := []struct {
		name   string
		mutate func(*corev1.Pod)
		want   error
	}{
		{"managed-by label", func(p *corev1.Pod) { p.Labels[LabelManagedBy] = "someone-else" }, ErrOwnershipConflict},
		{"managed-by label missing", func(p *corev1.Pod) { delete(p.Labels, LabelManagedBy) }, ErrOwnershipConflict},
		{"owner label", func(p *corev1.Pod) { p.Labels[LabelOwner] = ownerHash("another-controller") }, ErrOwnershipConflict},
		{"session label", func(p *corev1.Pod) { p.Labels[LabelSession] = SessionHash("tenant-acme", "session-9999") }, ErrOwnershipConflict},
		{"workload label", func(p *corev1.Pod) { p.Labels[LabelWorkload] = "lrh-other" }, ErrOwnershipConflict},
		{"generation label", func(p *corev1.Pod) { p.Labels[LabelGeneration] = "2" }, ErrOwnershipConflict},
		{"owner references", func(p *corev1.Pod) {
			p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs", UID: "rs-uid"}}
		}, ErrOwnershipConflict},
		{"spec hash annotation", func(p *corev1.Pod) { p.Annotations[AnnotationSpecHash] = strings.Repeat("0", 64) }, ErrSpecMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newAPIServer(t, testSecret())
			c := newTestController(t, server, &fakeRegistry{})
			pod, err := c.desired(intent)
			if err != nil {
				t.Fatal(err)
			}
			pod.UID = "foreign-uid"
			tc.mutate(pod)
			if err := server.Tracker().Add(pod); err != nil {
				t.Fatal(err)
			}
			server.ClearActions()

			ensureErr := c.EnsureWorkload(context.Background(), intent)
			if !errors.Is(ensureErr, tc.want) {
				t.Fatalf("Ensure err = %v, want %v", ensureErr, tc.want)
			}
			if server.count("create")+server.count("delete")+server.count("update")+server.count("patch") != 0 {
				t.Fatalf("Ensure mutated the cluster on a mismatch: %v", server.verbs())
			}
			_, found, observeErr := c.ObserveWorkload(context.Background(), intent)
			if found || !errors.Is(observeErr, tc.want) {
				t.Fatalf("Observe = (found %v, err %v), want (false, %v)", found, observeErr, tc.want)
			}
			if tc.want == ErrOwnershipConflict {
				deleteErr := c.DeleteWorkload(context.Background(), intent)
				if !errors.Is(deleteErr, ErrOwnershipConflict) {
					t.Fatalf("Delete err = %v, want ErrOwnershipConflict", deleteErr)
				}
			}
			if got := server.count("delete"); got != 0 {
				t.Fatalf("deletes = %d, want 0", got)
			}
			if got := server.pod(t, name); got.UID != "foreign-uid" {
				t.Fatalf("foreign pod was replaced: uid %q", got.UID)
			}
		})
	}
}

func TestForeignPodInTheSessionFailsClosed(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 1)
	foreign := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "not-ours", Namespace: testNamespace, UID: "foreign",
		Labels: map[string]string{LabelSession: SessionHash(intent.TenantID, intent.SessionID)},
	}}
	if err := server.Tracker().Add(foreign); err != nil {
		t.Fatal(err)
	}
	server.ClearActions()

	err := c.EnsureWorkload(context.Background(), intent)
	if !errors.Is(err, ErrOwnershipConflict) || errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("err = %v, want ErrOwnershipConflict and not ErrGenerationConflict", err)
	}
	if got, want := server.verbs(), []string{"list"}; !slices.Equal(got, want) {
		t.Fatalf("verbs = %v, want exactly %v", got, want)
	}
}

// Another controller's workload for the same session, well-formed in every
// label but the owner, is a foreign Pod -- not an "older generation" this
// adapter could ever drain and replace.
func TestAnotherControllersGenerationIsOwnershipNotGeneration(t *testing.T) {
	server := newAPIServer(t, testSecret())
	intent := testIntent(t, 2)
	otherCfg := testConfig(server, &fakeRegistry{})
	otherCfg.ControllerID = "another-controller"
	other, err := New(otherCfg)
	if err != nil {
		t.Fatal(err)
	}
	ensure(t, other, testIntent(t, 1))
	c := newTestController(t, server, &fakeRegistry{})
	server.ClearActions()

	err = c.EnsureWorkload(context.Background(), intent)
	if !errors.Is(err, ErrOwnershipConflict) || errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("err = %v, want ErrOwnershipConflict and not ErrGenerationConflict", err)
	}
	if got, want := server.verbs(), []string{"list"}; !slices.Equal(got, want) {
		t.Fatalf("verbs = %v, want exactly %v", got, want)
	}
}

func TestEnsureListIsBoundedAndSelectsTheSession(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 1)
	ensure(t, c, intent)
	var lists []k8stesting.ListActionImpl
	for _, a := range server.Actions() {
		if l, ok := a.(k8stesting.ListActionImpl); ok {
			lists = append(lists, l)
		}
	}
	if len(lists) != 1 {
		t.Fatalf("list calls = %d, want 1", len(lists))
	}
	opts := lists[0].ListOptions
	if opts.Limit != MaxSessionWorkloads || opts.LabelSelector != LabelSession+"="+SessionHash(intent.TenantID, intent.SessionID) {
		t.Fatalf("list options = %+v, want Limit %d and the session selector exactly", opts, MaxSessionWorkloads)
	}

	// A continued list means more workloads than one bounded page: refuse.
	server.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{ListMeta: metav1.ListMeta{Continue: "more"}}, nil
	})
	server.ClearActions()
	if err := c.EnsureWorkload(context.Background(), testIntent(t, 1)); !errors.Is(err, ErrTooManyWorkloads) {
		t.Fatalf("err = %v, want ErrTooManyWorkloads", err)
	}
	if got, want := server.verbs(), []string{"list"}; !slices.Equal(got, want) {
		t.Fatalf("verbs = %v, want exactly %v", got, want)
	}
}

func TestEnsureRefusesAPodThatCannotServe(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status corev1.PodStatus
		delete bool
		want   error
	}{
		{name: "failed", status: corev1.PodStatus{Phase: corev1.PodFailed}, want: ErrWorkloadTerminated},
		{name: "succeeded", status: corev1.PodStatus{Phase: corev1.PodSucceeded}, want: ErrWorkloadTerminated},
		{name: "terminating", status: readyStatus(), delete: true, want: ErrWorkloadTerminating},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newAPIServer(t, testSecret())
			c := newTestController(t, server, &fakeRegistry{})
			intent := testIntent(t, 1)
			ensure(t, c, intent)
			pod := server.pod(t, WorkloadName(intent))
			pod.Status = tc.status
			if tc.delete {
				now := metav1.NewTime(testNow)
				pod.DeletionTimestamp = &now
			}
			if err := server.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, testNamespace); err != nil {
				t.Fatal(err)
			}
			server.ClearActions()

			err := c.EnsureWorkload(context.Background(), intent)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if got, want := server.verbs(), []string{"list", "get"}; !slices.Equal(got, want) {
				t.Fatalf("verbs = %v, want exactly %v (no replacement)", got, want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Readiness is a placement candidate only; the registry decides.
// ---------------------------------------------------------------------------

func liveRegistration(intent sessionstore.PlacementIntent) *sessionstore.HostRegistrationEntry {
	name := WorkloadName(intent)
	return &sessionstore.HostRegistrationEntry{
		Revision: 4,
		Registration: sessionstore.HostRegistration{
			TenantID: intent.TenantID, SessionID: intent.SessionID,
			LeaseEpoch: 9,
			ObservedAt: testNow.Add(-5 * time.Second),
			ExpiresAt:  testNow.Add(25 * time.Second),
			Route: &sessionstore.HostRoute{
				HostID:                 HostID(intent),
				HostGeneration:         intent.Generation,
				AgentID:                intent.AgentID,
				RuntimeCompatibilityID: intent.RuntimeCompatibilityID,
				Placement:              sessionwire.HostPlacementDedicated,
				InternalEndpoint:       sessionwire.InternalEndpoint("ws://" + name + "." + testSubdomain + "." + testNamespace + ".svc:7443/hostlink/" + string(intent.TenantID)),
				Residency:              sessionwire.SessionResidencyResident,
				Accepting:              true,
			},
		},
	}
}

func TestObserveWorkload(t *testing.T) {
	intent := testIntent(t, 2)
	type setup struct {
		pod    bool
		status corev1.PodStatus
		entry  func() *sessionstore.HostRegistrationEntry
		err    error
	}
	live := func() *sessionstore.HostRegistrationEntry { return liveRegistration(intent) }
	mutate := func(f func(*sessionstore.HostRegistrationEntry)) func() *sessionstore.HostRegistrationEntry {
		return func() *sessionstore.HostRegistrationEntry { e := liveRegistration(intent); f(e); return e }
	}
	cases := []struct {
		name          string
		setup         setup
		wantFound     bool
		wantErr       bool
		wantRegistry  int
		wantPodChecks int
	}{
		{name: "no pod", setup: setup{}, wantRegistry: 0},
		{name: "pending pod with live registry", setup: setup{pod: true, status: corev1.PodStatus{Phase: corev1.PodPending}, entry: live}, wantRegistry: 0},
		{name: "running but not ready", setup: setup{pod: true, status: corev1.PodStatus{Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}}, entry: live}, wantRegistry: 0},
		{name: "ready pod, no registration", setup: setup{pod: true, status: readyStatus()}, wantRegistry: 1},
		{name: "ready pod, registration expired by store", setup: setup{pod: true, status: readyStatus(),
			err: &sessionstore.RegistryError{Code: sessionstore.RegistryErrorExpired}}, wantRegistry: 1},
		{name: "ready pod, registration released", setup: setup{pod: true, status: readyStatus(),
			err: &sessionstore.RegistryError{Code: sessionstore.RegistryErrorReleased}}, wantRegistry: 1},
		{name: "ready pod, registration past expiry", setup: setup{pod: true, status: readyStatus(),
			entry: mutate(func(e *sessionstore.HostRegistrationEntry) { e.Registration.ExpiresAt = testNow })}, wantRegistry: 1},
		{name: "ready pod, another host", setup: setup{pod: true, status: readyStatus(),
			entry: mutate(func(e *sessionstore.HostRegistrationEntry) { e.Registration.Route.HostID = "pooled-host-1" })}, wantRegistry: 1},
		{name: "ready pod, another host generation", setup: setup{pod: true, status: readyStatus(),
			entry: mutate(func(e *sessionstore.HostRegistrationEntry) { e.Registration.Route.HostGeneration = 1 })}, wantRegistry: 1},
		{name: "ready pod, pooled route", setup: setup{pod: true, status: readyStatus(),
			entry: mutate(func(e *sessionstore.HostRegistrationEntry) {
				e.Registration.Route.Placement = sessionwire.HostPlacementPooled
			})}, wantRegistry: 1},
		{name: "ready pod, another agent", setup: setup{pod: true, status: readyStatus(),
			entry: mutate(func(e *sessionstore.HostRegistrationEntry) { e.Registration.Route.AgentID = "agent-other" })}, wantRegistry: 1},
		{name: "ready pod, another runtime", setup: setup{pod: true, status: readyStatus(),
			entry: mutate(func(e *sessionstore.HostRegistrationEntry) {
				e.Registration.Route.RuntimeCompatibilityID = "runtime-old"
			})}, wantRegistry: 1},
		{name: "ready condition on a pod that is not running", setup: setup{pod: true, status: corev1.PodStatus{Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}, entry: live}, wantRegistry: 0},
		{name: "ready pod, matching route the store cannot project", setup: setup{pod: true, status: readyStatus(),
			entry: mutate(func(e *sessionstore.HostRegistrationEntry) { e.Registration.Route.InternalEndpoint = "" })}, wantErr: true, wantRegistry: 1},
		{name: "ready pod, registry backend failure", setup: setup{pod: true, status: readyStatus(),
			err: &sessionstore.RegistryError{Code: sessionstore.RegistryErrorBackend}}, wantErr: true, wantRegistry: 1},
		{name: "ready pod, live matching registration", setup: setup{pod: true, status: readyStatus(), entry: live}, wantFound: true, wantRegistry: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newAPIServer(t, testSecret())
			registry := &fakeRegistry{err: tc.setup.err}
			if tc.setup.entry != nil {
				registry.entry = tc.setup.entry()
			}
			c := newTestController(t, server, registry)
			if tc.setup.pod {
				ensure(t, c, intent)
				server.setStatus(t, WorkloadName(intent), tc.setup.status)
			}
			server.ClearActions()

			got, found, err := c.ObserveWorkload(context.Background(), intent)

			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if found != tc.wantFound {
				t.Fatalf("found = %v, want %v", found, tc.wantFound)
			}
			if registry.calls != tc.wantRegistry {
				t.Fatalf("registry reads = %d, want exactly %d", registry.calls, tc.wantRegistry)
			}
			if tc.wantFound {
				want, projErr := liveRegistration(intent).Registration.Observation()
				if projErr != nil {
					t.Fatal(projErr)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("observation = %+v\nwant exactly %+v", got, want)
				}
			} else if !reflect.DeepEqual(got, sessionwire.HostLinkRegistryObservation{}) {
				t.Fatalf("observation on not-found = %+v, want the zero value", got)
			}
			if server.count("create")+server.count("delete") != 0 {
				t.Fatalf("Observe mutated the cluster: %v", server.verbs())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Crash / retry.
// ---------------------------------------------------------------------------

// A create whose effect landed but whose response was lost -- the controller
// "crashed" between the API server committing and the caller hearing -- is
// adopted on retry instead of being created twice.
func TestCreateLostResponseIsAdoptedOnRetry(t *testing.T) {
	server := newAPIServer(t, testSecret())
	lost := true
	server.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if !lost {
			return false, nil, nil
		}
		lost = false
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.UID = "committed-before-crash"
		if err := server.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), pod, testNamespace); err != nil {
			t.Fatal(err)
		}
		return true, nil, apierrors.NewServerTimeout(corev1.Resource("pods"), "create", 1)
	})
	intent := testIntent(t, 1)

	first := newTestController(t, server, &fakeRegistry{}).EnsureWorkload(context.Background(), intent)
	if first == nil {
		t.Fatalf("first Ensure reported success for a create whose response was lost")
	}
	ensure(t, newTestController(t, server, &fakeRegistry{}), intent) // the restarted process

	pods := server.pods(t)
	if len(pods) != 1 || pods[0].UID != "committed-before-crash" {
		t.Fatalf("pods = %v, want exactly the one committed before the crash", podNames(pods))
	}
	if got := server.count("create"); got != 1 {
		t.Fatalf("create calls = %d, want exactly 1 (the retry adopts)", got)
	}
}

// Two replicas racing: this one's Get saw nothing, the other's create landed
// first, and this Create answers AlreadyExists. The adapter re-reads and adopts.
func TestCreateRaceAlreadyExistsIsAdopted(t *testing.T) {
	server := newAPIServer(t, testSecret())
	intent := testIntent(t, 1)
	c := newTestController(t, server, &fakeRegistry{})
	racer := newTestController(t, server, &fakeRegistry{})
	raced := false
	server.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		if raced {
			return false, nil, nil
		}
		raced = true
		// The other replica wins the create between this Get and this Create.
		pod, err := racer.desired(intent)
		if err != nil {
			t.Fatal(err)
		}
		pod.UID = "racer"
		if err := server.Tracker().Add(pod); err != nil {
			t.Fatal(err)
		}
		return true, nil, apierrors.NewNotFound(corev1.Resource("pods"), WorkloadName(intent))
	})

	ensure(t, c, intent)

	if got, want := server.verbs(), []string{"list", "get", "create", "get"}; !slices.Equal(got, want) {
		t.Fatalf("verbs = %v, want exactly %v", got, want)
	}
	pods := server.pods(t)
	if len(pods) != 1 || pods[0].UID != "racer" {
		t.Fatalf("pods = %v, want exactly the racer's", podNames(pods))
	}
}

// The same race, but what won the name is NOT ours: the re-read after
// AlreadyExists applies the full adoption check and fails closed.
func TestCreateRaceWithAForeignPodFailsClosed(t *testing.T) {
	server := newAPIServer(t, testSecret())
	intent := testIntent(t, 1)
	c := newTestController(t, server, &fakeRegistry{})
	raced := false
	server.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		if raced {
			return false, nil, nil
		}
		raced = true
		pod, err := c.desired(intent)
		if err != nil {
			t.Fatal(err)
		}
		pod.UID = "foreign"
		pod.Labels[LabelOwner] = ownerHash("another-controller")
		if err := server.Tracker().Add(pod); err != nil {
			t.Fatal(err)
		}
		return true, nil, apierrors.NewNotFound(corev1.Resource("pods"), WorkloadName(intent))
	})

	err := c.EnsureWorkload(context.Background(), intent)
	if !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("err = %v, want ErrOwnershipConflict", err)
	}
	if got, want := server.verbs(), []string{"list", "get", "create", "get"}; !slices.Equal(got, want) {
		t.Fatalf("verbs = %v, want exactly %v", got, want)
	}
	if got := server.pod(t, WorkloadName(intent)); got.UID != "foreign" {
		t.Fatalf("foreign pod replaced: %q", got.UID)
	}
}

// ---------------------------------------------------------------------------
// Delete with a UID precondition.
// ---------------------------------------------------------------------------

func deleteActions(server *apiServer) []k8stesting.DeleteActionImpl {
	var out []k8stesting.DeleteActionImpl
	for _, a := range server.Actions() {
		if d, ok := a.(k8stesting.DeleteActionImpl); ok && d.GetResource().Resource == "pods" {
			out = append(out, d)
		}
	}
	return out
}

func TestDeleteIsPreconditionedOnTheObservedUID(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 1)
	ensure(t, c, intent)
	uid := server.pod(t, WorkloadName(intent)).UID
	server.ClearActions()

	if err := c.DeleteWorkload(context.Background(), intent); err != nil {
		t.Fatalf("DeleteWorkload: %v", err)
	}
	deletes := deleteActions(server)
	if len(deletes) != 1 {
		t.Fatalf("delete calls = %d, want exactly 1", len(deletes))
	}
	pre := deletes[0].DeleteOptions.Preconditions
	if pre == nil || pre.UID == nil || *pre.UID != uid || deletes[0].Name != WorkloadName(intent) {
		t.Fatalf("delete preconditions = %+v on %q, want UID exactly %q on %q", pre, deletes[0].Name, uid, WorkloadName(intent))
	}
	if len(server.pods(t)) != 0 {
		t.Fatalf("pods after delete = %d, want 0", len(server.pods(t)))
	}

	// Repeated delete: nothing to do and nothing sent.
	server.ClearActions()
	if err := c.DeleteWorkload(context.Background(), intent); err != nil {
		t.Fatalf("repeated DeleteWorkload: %v", err)
	}
	if got, want := server.verbs(), []string{"get"}; !slices.Equal(got, want) {
		t.Fatalf("repeated delete verbs = %v, want exactly %v", got, want)
	}
}

// The Pod was replaced between the adapter's read and its delete (same name,
// new UID). The precondition refuses, and the replacement survives.
func TestDeleteRefusesAReplacedPod(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 1)
	ensure(t, c, intent)
	current := server.pod(t, WorkloadName(intent))
	stale := current.DeepCopy()
	stale.UID = "the-pod-observed-before-replacement"
	// Only the adapter's FIRST read sees the pre-replacement object: the
	// precondition must carry the UID of what the adapter judged, not of
	// whatever a later read would return.
	served := false
	server.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		if served {
			return false, nil, nil
		}
		served = true
		return true, stale.DeepCopy(), nil
	})
	server.ClearActions()

	err := c.DeleteWorkload(context.Background(), intent)

	if !errors.Is(err, ErrWorkloadReplaced) {
		t.Fatalf("err = %v, want ErrWorkloadReplaced", err)
	}
	deletes := deleteActions(server)
	if len(deletes) != 1 || *deletes[0].DeleteOptions.Preconditions.UID != types.UID("the-pod-observed-before-replacement") {
		t.Fatalf("delete actions = %+v, want exactly one preconditioned on the observed UID", deletes)
	}
	if got := server.pods(t); len(got) != 1 || got[0].UID != current.UID {
		t.Fatalf("replacement pod was deleted: %v", podNames(got))
	}
}

func TestDeleteOfTerminatingPodSendsNothing(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 1)
	ensure(t, c, intent)
	pod := server.pod(t, WorkloadName(intent))
	now := metav1.NewTime(testNow)
	pod.DeletionTimestamp = &now
	if err := server.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, testNamespace); err != nil {
		t.Fatal(err)
	}
	server.ClearActions()
	if err := c.DeleteWorkload(context.Background(), intent); err != nil {
		t.Fatalf("DeleteWorkload: %v", err)
	}
	if got, want := server.verbs(), []string{"get"}; !slices.Equal(got, want) {
		t.Fatalf("verbs = %v, want exactly %v", got, want)
	}
}

// ---------------------------------------------------------------------------
// Drain goes through the driver's own HostLink client and is refused on
// this seam, never faked.
// ---------------------------------------------------------------------------

func TestRequestDrainIsRefusedNotFaked(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	got, err := c.RequestDrain(context.Background(), testIntent(t, 1))
	if !errors.Is(err, ErrDrainNotImplemented) {
		t.Fatalf("err = %v, want ErrDrainNotImplemented", err)
	}
	if !reflect.DeepEqual(got, sessionwire.HostLinkDrainObservation{}) {
		t.Fatalf("observation = %+v, want the zero value", got)
	}
	if len(server.Actions()) != 0 {
		t.Fatalf("RequestDrain touched the cluster: %v", server.verbs())
	}
}

// ---------------------------------------------------------------------------
// Secret exclusion and payload constraints.
// ---------------------------------------------------------------------------

func TestRenderedPodCarriesNoSecretsTokensOrTenant(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 1)
	intent.TenantID = "tenant-distinctive-7f3a"
	ensure(t, c, intent)
	pod := server.pod(t, WorkloadName(intent))

	// The tenant appears in exactly ONE place: the HostLink path segment of the
	// advertised endpoint, because the released Host serves HostLink only at
	// /hostlink/<tenant> and Factory dials the advertised endpoint verbatim.
	// It is routing, not a Host tenant gate (H8), and it is in no name, label,
	// annotation or other variable.
	endpoint := "ws://" + pod.Name + "." + testSubdomain + "." + testNamespace + ".svc:7443/hostlink/" + string(intent.TenantID)
	scrubbed := pod.DeepCopy()
	found := 0
	for i, v := range scrubbed.Spec.Containers[0].Env {
		if v.Name == "HOST_INTERNAL_ENDPOINT" {
			if v.Value != endpoint {
				t.Fatalf("endpoint = %q, want %q", v.Value, endpoint)
			}
			scrubbed.Spec.Containers[0].Env[i].Value = ""
			found++
		}
	}
	if found != 1 {
		t.Fatalf("HOST_INTERNAL_ENDPOINT set %d times", found)
	}
	raw, err := json.Marshal(scrubbed)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{secretBytes, string(intent.TenantID), string(intent.AgentID), intent.RuntimeCompatibilityID} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("rendered Pod contains %q outside the endpoint path", forbidden)
		}
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatalf("automountServiceAccountToken = %v, want exactly false", pod.Spec.AutomountServiceAccountToken)
	}
	if pod.Spec.EnableServiceLinks == nil || *pod.Spec.EnableServiceLinks {
		t.Fatalf("enableServiceLinks = %v, want exactly false", pod.Spec.EnableServiceLinks)
	}
	if len(pod.Spec.Containers[0].EnvFrom) != 0 {
		t.Fatalf("envFrom = %v, want none", pod.Spec.Containers[0].EnvFrom)
	}
	// Credentials are REFERENCES: secret volumes naming allowlisted Secrets only.
	var secrets []string
	for _, v := range pod.Spec.Volumes {
		if v.Secret != nil {
			secrets = append(secrets, v.Name+"="+v.Secret.SecretName)
		}
	}
	slices.Sort(secrets)
	if want := []string{"cred-hostlink-auth=hostlink-service-identity", "cred-session-store=shared-session-store"}; !slices.Equal(secrets, want) {
		t.Fatalf("secret volumes = %v, want exactly %v", secrets, want)
	}
}

func TestPayloadConstraintsRefuseBeforeAnyCreate(t *testing.T) {
	base := func() map[string]any {
		var m map[string]any
		if err := json.Unmarshal(encodePayload(t, testPayload()), &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	settings := func(m map[string]any) map[string]any { return m["host_settings"].(map[string]any) }
	cases := []struct {
		name    string
		version string
		payload func() []byte
		want    error
	}{
		{name: "unknown field carrying inline env", payload: func() []byte {
			m := base()
			m["env"] = map[string]string{"AUTH_TOKEN": secretBytes}
			return encodePayload(t, m)
		}, want: ErrInvalidPayload},
		{name: "inline secret value field", payload: func() []byte {
			m := base()
			m["secret_values"] = map[string]string{"token": secretBytes}
			return encodePayload(t, m)
		}, want: ErrInvalidPayload},
		{name: "unlisted host setting", payload: func() []byte {
			m := base()
			settings(m)["HOST_AUTH_TOKEN"] = "10s"
			return encodePayload(t, m)
		}, want: ErrSettingNotAllowed},
		{name: "adapter-owned fixed session", payload: func() []byte {
			m := base()
			settings(m)["HOST_FIXED_SESSION_ID"] = "session-other"
			return encodePayload(t, m)
		}, want: ErrSettingNotAllowed},
		{name: "adapter-owned capacity with a well-formed count", payload: func() []byte {
			m := base()
			settings(m)["HOST_CAPACITY"] = "2"
			return encodePayload(t, m)
		}, want: ErrSettingNotAllowed},
		{name: "adapter-owned generation with a well-formed count", payload: func() []byte {
			m := base()
			settings(m)["HOST_GENERATION"] = "9"
			return encodePayload(t, m)
		}, want: ErrSettingNotAllowed},
		{name: "tenant setting reintroduced", payload: func() []byte {
			m := base()
			settings(m)["HOST_TENANT_ID"] = "tenant-acme"
			return encodePayload(t, m)
		}, want: ErrSettingNotAllowed},
		{name: "short token as a duration value", payload: func() []byte {
			m := base()
			settings(m)["HOST_WARM_TTL"] = "Bearer abc"
			return encodePayload(t, m)
		}, want: ErrInvalidPayload},
		{name: "long token as a duration value", payload: func() []byte {
			m := base()
			settings(m)["HOST_WARM_TTL"] = "Bearer " + secretBytes
			return encodePayload(t, m)
		}, want: ErrInvalidPayload},
		{name: "token as a count value", payload: func() []byte {
			m := base()
			settings(m)["HOST_MAX_BINDINGS"] = "tok3n"
			return encodePayload(t, m)
		}, want: ErrInvalidPayload},
		{name: "missing required host setting", payload: func() []byte {
			m := base()
			delete(settings(m), "HOST_WORK_POLL")
			return encodePayload(t, m)
		}, want: ErrInvalidPayload},
		{name: "credential not in allowlist", payload: func() []byte {
			m := base()
			m["credentials"] = []string{"session-store", "cluster-admin"}
			return encodePayload(t, m)
		}, want: ErrCredentialNotAllowed},
		{name: "duplicate credential", payload: func() []byte {
			m := base()
			m["credentials"] = []string{"session-store", "session-store"}
			return encodePayload(t, m)
		}, want: ErrInvalidPayload},
		{name: "image by tag", payload: func() []byte {
			m := base()
			m["image"] = "registry.example/looprig/host:latest"
			return encodePayload(t, m)
		}, want: ErrInvalidPayload},
		{name: "trailing document", payload: func() []byte {
			return append(encodePayload(t, testPayload()), []byte(` {}`)...)
		}, want: ErrInvalidPayload},
		{name: "unsupported version", version: "looprig.controller/kubernetes-pod/v0", payload: func() []byte {
			return encodePayload(t, testPayload())
		}, want: ErrUnsupportedPayload},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newAPIServer(t, testSecret())
			c := newTestController(t, server, &fakeRegistry{})
			intent := testIntent(t, 1)
			intent.Workload.Payload = tc.payload()
			if tc.version != "" {
				intent.Workload.PayloadVersion = tc.version
			}
			err := c.EnsureWorkload(context.Background(), intent)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if tc.want == ErrSettingNotAllowed && !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("err = %v, want it also classified ErrInvalidPayload", err)
			}
			if tc.want != ErrSettingNotAllowed && errors.Is(err, ErrSettingNotAllowed) {
				t.Fatalf("err = %v names the allowlist, want a value refusal", err)
			}
			if strings.Contains(err.Error(), secretBytes) || strings.Contains(err.Error(), "Bearer") {
				t.Fatalf("error text echoes payload bytes: %v", err)
			}
			if len(server.Actions()) != 0 {
				t.Fatalf("a refused payload reached the cluster: %v", server.verbs())
			}
		})
	}
}

func TestIntentValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*sessionstore.PlacementIntent)
		want   error
	}{
		{"pooled placement", func(i *sessionstore.PlacementIntent) { i.Placement = sessionwire.HostPlacementPooled }, ErrInvalidIntent},
		{"zero generation", func(i *sessionstore.PlacementIntent) { i.Generation = 0 }, ErrInvalidIntent},
		{"empty tenant", func(i *sessionstore.PlacementIntent) { i.TenantID = "" }, ErrInvalidIntent},
		{"empty session", func(i *sessionstore.PlacementIntent) { i.SessionID = "" }, ErrInvalidIntent},
		{"empty agent", func(i *sessionstore.PlacementIntent) { i.AgentID = "" }, ErrInvalidIntent},
		{"empty runtime", func(i *sessionstore.PlacementIntent) { i.RuntimeCompatibilityID = "" }, ErrInvalidIntent},
		{"tenant the Host cannot route", func(i *sessionstore.PlacementIntent) { i.TenantID = "tenant/with/slash" }, ErrInvalidIntent},
		{"no workload", func(i *sessionstore.PlacementIntent) { i.Workload = sessionstore.DesiredWorkload{} }, ErrUnsupportedPayload},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newAPIServer(t, testSecret())
			c := newTestController(t, server, &fakeRegistry{})
			intent := testIntent(t, 1)
			tc.mutate(&intent)
			for op, err := range map[string]error{
				"ensure":  c.EnsureWorkload(context.Background(), intent),
				"observe": func() error { _, _, err := c.ObserveWorkload(context.Background(), intent); return err }(),
				"delete":  c.DeleteWorkload(context.Background(), intent),
			} {
				if !errors.Is(err, tc.want) {
					t.Fatalf("%s err = %v, want %v", op, err, tc.want)
				}
			}
			if len(server.Actions()) != 0 {
				t.Fatalf("an invalid intent reached the cluster: %v", server.verbs())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Names and labels are hashes.
// ---------------------------------------------------------------------------

func TestNamesAreHashesOfTheWholeIdentity(t *testing.T) {
	base := testIntent(t, 7)
	name := WorkloadName(base)
	if !namePattern.MatchString(name) || len(name) > 63 {
		t.Fatalf("name %q is not a 63-byte-safe hash name", name)
	}
	for _, raw := range []string{string(base.TenantID), string(base.SessionID), string(base.AgentID), base.RuntimeCompatibilityID} {
		if strings.Contains(name, raw) || strings.Contains(SessionHash(base.TenantID, base.SessionID), raw) {
			t.Fatalf("a raw identifier %q leaked into a platform name", raw)
		}
	}
	variants := map[string]func(*sessionstore.PlacementIntent){
		"tenant":     func(i *sessionstore.PlacementIntent) { i.TenantID = "tenant-other" },
		"session":    func(i *sessionstore.PlacementIntent) { i.SessionID = "session-other" },
		"agent":      func(i *sessionstore.PlacementIntent) { i.AgentID = "agent-other" },
		"runtime":    func(i *sessionstore.PlacementIntent) { i.RuntimeCompatibilityID = "runtime-other" },
		"generation": func(i *sessionstore.PlacementIntent) { i.Generation = 8 },
	}
	seen := map[string]string{name: "base"}
	for field, mutate := range variants {
		intent := base
		mutate(&intent)
		got := WorkloadName(intent)
		if prior, dup := seen[got]; dup {
			t.Fatalf("changing %s produced the same name as %s", field, prior)
		}
		seen[got] = field
	}
	// Framing: moving a byte across the tenant/session boundary is a different
	// identity, not a concatenation collision.
	a, b := base, base
	a.TenantID, a.SessionID = "ab", "c"
	b.TenantID, b.SessionID = "a", "bc"
	if WorkloadName(a) == WorkloadName(b) || SessionHash(a.TenantID, a.SessionID) == SessionHash(b.TenantID, b.SessionID) {
		t.Fatalf("boundary-shifted identities collide")
	}
	// The session label is generation-independent: it is how another
	// generation's workload is found.
	next := base
	next.Generation = 8
	if SessionHash(base.TenantID, base.SessionID) != SessionHash(next.TenantID, next.SessionID) {
		t.Fatalf("session hash depends on generation")
	}
	if HostID(base) != sessionwire.HostID(name) {
		t.Fatalf("HostID = %q, want the workload name %q", HostID(base), name)
	}
}

func TestNewRefusesIncompleteConfiguration(t *testing.T) {
	server := newAPIServer(t)
	valid := testConfig(server, &fakeRegistry{})
	cases := map[string]func(*Config){
		"nil client":          func(c *Config) { c.Client = nil },
		"empty namespace":     func(c *Config) { c.Namespace = "" },
		"invalid namespace":   func(c *Config) { c.Namespace = "Not_A_Label" },
		"empty controller id": func(c *Config) { c.ControllerID = "" },
		"empty subdomain":     func(c *Config) { c.HostSubdomain = "" },
		"invalid subdomain":   func(c *Config) { c.HostSubdomain = "UPPER" },
		"zero port":           func(c *Config) { c.HostPort = 0 },
		"port too high":       func(c *Config) { c.HostPort = 65536 },
		"no credentials":      func(c *Config) { c.Credentials = nil },
		"invalid ref name":    func(c *Config) { c.Credentials = map[string]string{"Bad Ref": "secret"} },
		"invalid secret name": func(c *Config) { c.Credentials = map[string]string{"ok": "Not A Secret"} },
		"nil registry":        func(c *Config) { c.Registry = nil },
		"nil clock":           func(c *Config) { c.Clock = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			cfg.Credentials = map[string]string{"session-store": "shared-session-store"}
			mutate(&cfg)
			if _, err := New(cfg); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New err = %v, want ErrInvalidConfig", err)
			}
		})
	}
	if _, err := New(valid); err != nil {
		t.Fatalf("control: valid config refused: %v", err)
	}
}
