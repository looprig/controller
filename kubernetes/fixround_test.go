package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"github.com/looprig/controller/internal/strictjson"
)

// ---------------------------------------------------------------------------
// Pod hardening, asserted exactly.
// ---------------------------------------------------------------------------

func TestPodHardeningIsExact(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 1)
	ensure(t, c, intent)
	pod := server.pod(t, WorkloadName(intent))

	wantPod := &corev1.PodSecurityContext{
		RunAsNonRoot:   ptrTo(true),
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	if !reflect.DeepEqual(pod.Spec.SecurityContext, wantPod) {
		t.Fatalf("pod securityContext = %+v, want exactly %+v", pod.Spec.SecurityContext, wantPod)
	}
	container := pod.Spec.Containers[0]
	wantContainer := &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptrTo(false),
		ReadOnlyRootFilesystem:   ptrTo(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
	if !reflect.DeepEqual(container.SecurityContext, wantContainer) {
		t.Fatalf("container securityContext = %+v, want exactly %+v", container.SecurityContext, wantContainer)
	}
	wantMounts := []corev1.VolumeMount{
		{Name: "workspace", MountPath: "/workspace"},
		{Name: "cred-hostlink-auth", MountPath: "/var/run/looprig/credentials/hostlink-auth", ReadOnly: true},
		{Name: "cred-session-store", MountPath: "/var/run/looprig/credentials/session-store", ReadOnly: true},
	}
	if !reflect.DeepEqual(container.VolumeMounts, wantMounts) {
		t.Fatalf("volumeMounts = %+v, want exactly %+v", container.VolumeMounts, wantMounts)
	}
	if len(pod.Spec.Volumes) != 3 {
		t.Fatalf("volumes = %d, want 3", len(pod.Spec.Volumes))
	}
	workspace := pod.Spec.Volumes[0]
	if workspace.Name != "workspace" || workspace.EmptyDir == nil || workspace.EmptyDir.SizeLimit == nil ||
		workspace.EmptyDir.SizeLimit.Cmp(resource.MustParse("1Gi")) != 0 || workspace.EmptyDir.Medium != "" {
		t.Fatalf("workspace volume = %+v, want an emptyDir bounded at exactly 1Gi", workspace)
	}
	wantSecrets := []corev1.Volume{
		{Name: "cred-hostlink-auth", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: "hostlink-service-identity", DefaultMode: ptrTo[int32](0o440), Optional: ptrTo(false)}}},
		{Name: "cred-session-store", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: "shared-session-store", DefaultMode: ptrTo[int32](0o440), Optional: ptrTo(false)}}},
	}
	if !reflect.DeepEqual(pod.Spec.Volumes[1:], wantSecrets) {
		t.Fatalf("secret volumes = %+v, want exactly %+v", pod.Spec.Volumes[1:], wantSecrets)
	}
}

// ---------------------------------------------------------------------------
// Golden identities. These values are DURABLE: existing Pods are found and
// adopted by them. A change to any derivation, domain string, field order or
// encoding -- or a k8s.io/api bump that changes PodSpec's JSON -- must fail
// here rather than orphan running Pods and create a second Host for a session.
// ---------------------------------------------------------------------------

func TestGoldenIdentities(t *testing.T) {
	intent := testIntent(t, 3)
	for _, tc := range []struct{ name, got, want string }{
		{"WorkloadName", WorkloadName(intent), "lrh-" + goldenWorkload},
		{"SessionHash", SessionHash(intent.TenantID, intent.SessionID), goldenSession},
		{"ownerHash", ownerHash(testControllerID), goldenOwner},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want the pinned %q", tc.name, tc.got, tc.want)
		}
	}
	server := newAPIServer(t)
	c := newTestController(t, server, &fakeRegistry{})
	spec, err := renderedSpecHash(t, c, intent)
	if err != nil {
		t.Fatal(err)
	}
	if spec != goldenSpec {
		t.Errorf("spec hash = %q, want the pinned %q", spec, goldenSpec)
	}
}

// The three digests were recomputed independently of this package (Python
// hashlib over the documented framing: each field as an 8-byte big-endian
// length then its bytes, domain string first, generation as 8 big-endian
// bytes, first 28 bytes hex) and agree. The spec hash is a regression pin of
// json.Marshal(corev1.PodSpec) at k8s.io/api v0.37.0.
const (
	goldenWorkload = "8128f49d88b23838ea73cdc47443a659ecdcac3e27c14c1537e13b7f"
	goldenSession  = "8b3e4e4d8e14142a719785959ac576e60b270d3ee8dc844a7b7a93fb"
	goldenOwner    = "a1336f9dd4ca8f1b010d6421eecd6c74f70457960116d3a4d01193ff"
	goldenSpec     = "20b89bd3dad914ff2778674dd43efad7e3552d5225d85fc16e5f0dbc4109c3f2"
)

// ---------------------------------------------------------------------------
// Tenant characters in the HostLink endpoint (spec G1).
// ---------------------------------------------------------------------------

func TestTenantEndpointEscapingAndBounds(t *testing.T) {
	for _, tc := range []struct {
		tenant, escaped string
	}{
		{"tenant?q=1", "tenant%3Fq=1"},
		{"tenant#frag", "tenant%23frag"},
		{"ten%ant", "ten%25ant"},
		{"a b", "a%20b"},
		{"tenant.v2", "tenant.v2"},
	} {
		t.Run("accepted "+tc.tenant, func(t *testing.T) {
			server := newAPIServer(t, testSecret())
			c := newTestController(t, server, &fakeRegistry{})
			intent := testIntent(t, 1)
			intent.TenantID = sessionwire.TenantID(tc.tenant)
			ensure(t, c, intent)
			pod := server.pod(t, WorkloadName(intent))
			var endpoint string
			for _, v := range pod.Spec.Containers[0].Env {
				if v.Name == "HOST_INTERNAL_ENDPOINT" {
					endpoint = v.Value
				}
			}
			want := "ws://" + pod.Name + "." + testSubdomain + "." + testNamespace + ".svc:7443/hostlink/" + tc.escaped
			if endpoint != want {
				t.Fatalf("endpoint = %q, want exactly %q", endpoint, want)
			}
			parsed, err := url.Parse(endpoint)
			if err != nil || parsed.Path != "/hostlink/"+tc.tenant || parsed.RawQuery != "" || parsed.Fragment != "" {
				t.Fatalf("endpoint does not route back to the tenant: %+v, %v", parsed, err)
			}
		})
	}
	for _, tc := range []struct{ name, tenant string }{
		{"dot", "."},
		{"dot-dot", ".."},
		{"slash", "a/b"},
		{"long ascii", strings.Repeat("t", 200)},
		{"long multibyte", strings.Repeat("é", 100)},
	} {
		t.Run("refused "+tc.name, func(t *testing.T) {
			if err := sessionwire.TenantID(tc.tenant).Validate(); err != nil {
				t.Fatalf("precondition: %q is not a legal Core tenant: %v", tc.tenant, err)
			}
			server := newAPIServer(t, testSecret())
			c := newTestController(t, server, &fakeRegistry{})
			intent := testIntent(t, 1)
			intent.TenantID = sessionwire.TenantID(tc.tenant)
			if err := c.EnsureWorkload(context.Background(), intent); !errors.Is(err, ErrInvalidIntent) {
				t.Fatalf("Ensure err = %v, want ErrInvalidIntent", err)
			}
			if _, _, err := c.ObserveWorkload(context.Background(), intent); !errors.Is(err, ErrInvalidIntent) {
				t.Fatalf("Observe err = %v, want ErrInvalidIntent", err)
			}
			if len(server.Actions()) != 0 {
				t.Fatalf("a refused tenant reached the cluster: %v", server.verbs())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The session list: errors, conflicts, bounds and order (quality 2).
// ---------------------------------------------------------------------------

func TestEnsureListErrorFailsClosed(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	server.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(corev1.Resource("pods"), "", errors.New("rbac"))
	})
	err := c.EnsureWorkload(context.Background(), testIntent(t, 1))
	if !apierrors.IsForbidden(err) {
		t.Fatalf("err = %v, want the list's Forbidden", err)
	}
	if got, want := server.verbs(), []string{"list"}; !slices.Equal(got, want) {
		t.Fatalf("verbs = %v, want exactly %v (no create without the generation check)", got, want)
	}
}

func TestEnsureGetErrorFailsClosed(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	server.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInternalError(errors.New("etcd"))
	})
	err := c.EnsureWorkload(context.Background(), testIntent(t, 1))
	if !apierrors.IsInternalError(err) {
		t.Fatalf("err = %v, want the Get's InternalError", err)
	}
	if got, want := server.verbs(), []string{"list", "get"}; !slices.Equal(got, want) {
		t.Fatalf("verbs = %v, want exactly %v", got, want)
	}
}

func TestSameSessionAndGenerationUnderAnotherNameIsRefused(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	other := testIntent(t, 1)
	other.AgentID = "agent-other" // same session and generation, another name
	ensure(t, c, other)
	server.ClearActions()

	err := c.EnsureWorkload(context.Background(), testIntent(t, 1))
	if !errors.Is(err, ErrOwnershipConflict) || errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("err = %v, want ErrOwnershipConflict and not ErrGenerationConflict", err)
	}
	if got, want := server.verbs(), []string{"list"}; !slices.Equal(got, want) {
		t.Fatalf("verbs = %v, want exactly %v", got, want)
	}
	if len(server.pods(t)) != 1 {
		t.Fatalf("pods = %d, want exactly 1 (no duplicate Host for one session+generation)", len(server.pods(t)))
	}
}

// ownedSessionPod is a Pod this controller would own for the test session,
// with its generation and workload labels overridden.
func ownedSessionPod(t *testing.T, c *Controller, generation uint64, name, generationLabel, workloadLabel string) *corev1.Pod {
	t.Helper()
	pod, err := c.desired(testIntent(t, generation))
	if err != nil {
		t.Fatal(err)
	}
	pod.Name = name
	pod.Labels[LabelGeneration] = generationLabel
	pod.Labels[LabelWorkload] = workloadLabel
	return pod
}

func TestMalformedSessionWorkloadLabelsAreRefused(t *testing.T) {
	for _, tc := range []struct{ name, generation, workload string }{
		{"non-numeric generation", "abc", "other-pod"},
		{"zero generation", "0", "other-pod"},
		{"missing generation", "", "other-pod"},
		{"workload label is not the pod name", "7", "some-other-name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newAPIServer(t, testSecret())
			c := newTestController(t, server, &fakeRegistry{})
			if err := server.Tracker().Add(ownedSessionPod(t, c, 7, "other-pod", tc.generation, tc.workload)); err != nil {
				t.Fatal(err)
			}
			server.ClearActions()
			err := c.EnsureWorkload(context.Background(), testIntent(t, 1))
			if !errors.Is(err, ErrOwnershipConflict) || errors.Is(err, ErrGenerationConflict) {
				t.Fatalf("err = %v, want ErrOwnershipConflict", err)
			}
			if got, want := server.verbs(), []string{"list"}; !slices.Equal(got, want) {
				t.Fatalf("verbs = %v, want exactly %v", got, want)
			}
		})
	}
}

func listReturning(pods ...*corev1.Pod) k8stesting.ReactionFunc {
	return func(k8stesting.Action) (bool, runtime.Object, error) {
		list := &corev1.PodList{}
		for _, p := range pods {
			list.Items = append(list.Items, *p.DeepCopy())
		}
		return true, list, nil
	}
}

func TestSessionListItemBound(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	var pods []*corev1.Pod
	for i := range MaxSessionWorkloads + 1 {
		name := fmt.Sprintf("older-%d", i)
		pods = append(pods, ownedSessionPod(t, c, 1, name, "1", name))
	}
	server.PrependReactor("list", "pods", listReturning(pods...))

	err := c.EnsureWorkload(context.Background(), testIntent(t, 2))
	if !errors.Is(err, ErrTooManyWorkloads) {
		t.Fatalf("%d items: err = %v, want ErrTooManyWorkloads", len(pods), err)
	}
	if got, want := server.verbs(), []string{"list"}; !slices.Equal(got, want) {
		t.Fatalf("verbs = %v, want exactly %v", got, want)
	}

	// Control: exactly the bound is judged, not refused as too many.
	server = newAPIServer(t, testSecret())
	c = newTestController(t, server, &fakeRegistry{})
	server.PrependReactor("list", "pods", listReturning(pods[:MaxSessionWorkloads]...))
	var conflict *GenerationConflictError
	if err := c.EnsureWorkload(context.Background(), testIntent(t, 2)); !errors.As(err, &conflict) || len(conflict.Older) != MaxSessionWorkloads {
		t.Fatalf("%d items: err = %v, want a GenerationConflictError naming all of them", MaxSessionWorkloads, err)
	}
}

func TestGenerationConflictListsAreSorted(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	mk := func(g uint64) *corev1.Pod {
		name := fmt.Sprintf("gen-%d", g)
		return ownedSessionPod(t, c, g, name, fmt.Sprint(g), name)
	}
	server.PrependReactor("list", "pods", listReturning(mk(3), mk(9), mk(1), mk(7), mk(2), mk(8)))

	err := c.EnsureWorkload(context.Background(), testIntent(t, 5))
	var conflict *GenerationConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %v, want *GenerationConflictError", err)
	}
	if want := (GenerationConflictError{Intent: 5, Older: []uint64{1, 2, 3}, Newer: []uint64{7, 8, 9}}); !reflect.DeepEqual(*conflict, want) {
		t.Fatalf("conflict = %+v, want exactly %+v", *conflict, want)
	}
}

// ---------------------------------------------------------------------------
// Observe and Delete edges (quality 9).
// ---------------------------------------------------------------------------

func TestObserveTerminatedOrTerminatingIsNotFound(t *testing.T) {
	for _, tc := range []struct {
		name        string
		phase       corev1.PodPhase
		terminating bool
	}{
		{"failed", corev1.PodFailed, false},
		{"succeeded", corev1.PodSucceeded, false},
		{"terminating", corev1.PodRunning, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newAPIServer(t, testSecret())
			intent := testIntent(t, 1)
			registry := &fakeRegistry{entry: liveRegistration(intent)}
			c := newTestController(t, server, registry)
			ensure(t, c, intent)
			pod := server.pod(t, WorkloadName(intent))
			pod.Status = readyStatus()
			pod.Status.Phase = tc.phase
			if tc.terminating {
				now := metav1.NewTime(testNow)
				pod.DeletionTimestamp = &now
			}
			if err := server.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, testNamespace); err != nil {
				t.Fatal(err)
			}
			got, found, err := c.ObserveWorkload(context.Background(), intent)
			if err != nil || found || !reflect.DeepEqual(got, sessionwire.HostLinkRegistryObservation{}) {
				t.Fatalf("Observe = (%+v, %v, %v), want exactly (zero, false, nil)", got, found, err)
			}
			if registry.calls != 0 {
				t.Fatalf("registry reads = %d, want 0", registry.calls)
			}
		})
	}
}

func TestDeleteNotFoundRaceIsSuccess(t *testing.T) {
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 1)
	ensure(t, c, intent)
	server.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(corev1.Resource("pods"), WorkloadName(intent))
	})
	if err := c.DeleteWorkload(context.Background(), intent); err != nil {
		t.Fatalf("DeleteWorkload = %v, want nil when the Pod vanished between read and delete", err)
	}
}

// ---------------------------------------------------------------------------
// Payload value constraints and strict decoding (quality 4, quality 6 / G7).
// ---------------------------------------------------------------------------

func TestPayloadValueConstraints(t *testing.T) {
	type mutate func(m map[string]any)
	settings := func(m map[string]any) map[string]any { return m["host_settings"].(map[string]any) }
	resources := func(m map[string]any) map[string]any { return m["resources"].(map[string]any) }
	cases := []struct {
		name string
		edit mutate
		ok   bool
	}{
		{"ping and pong together", func(m map[string]any) {
			settings(m)["HOST_PING_INTERVAL"] = "10s"
			settings(m)["HOST_PONG_TIMEOUT"] = "5s"
		}, true},
		{"ping without pong", func(m map[string]any) { settings(m)["HOST_PING_INTERVAL"] = "10s" }, false},
		{"pong without ping", func(m map[string]any) { settings(m)["HOST_PONG_TIMEOUT"] = "5s" }, false},
		{"zero duration", func(m map[string]any) { settings(m)["HOST_WARM_TTL"] = "0s" }, false},
		{"negative duration", func(m map[string]any) { settings(m)["HOST_WARM_TTL"] = "-5s" }, false},
		{"zero ping", func(m map[string]any) {
			settings(m)["HOST_PING_INTERVAL"] = "0s"
			settings(m)["HOST_PONG_TIMEOUT"] = "5s"
		}, false},
		{"zero count", func(m map[string]any) { settings(m)["HOST_MAX_BINDINGS"] = "0" }, false},
		{"count over 31 bits", func(m map[string]any) { settings(m)["HOST_MAX_BINDINGS"] = "2147483648" }, false},
		{"count at 31 bits", func(m map[string]any) { settings(m)["HOST_MAX_BINDINGS"] = "2147483647" }, true},
		{"setting over 32 bytes", func(m map[string]any) {
			settings(m)["HOST_WARM_TTL"] = strings.Repeat("0", 31) + "1s"
		}, false},
		{"setting at 32 bytes", func(m map[string]any) {
			settings(m)["HOST_WARM_TTL"] = strings.Repeat("0", 30) + "1s"
		}, true},
		{"zero cpu", func(m map[string]any) { resources(m)["cpu"] = "0" }, false},
		{"negative cpu", func(m map[string]any) { resources(m)["cpu"] = "-1" }, false},
		{"zero memory", func(m map[string]any) { resources(m)["memory"] = "0" }, false},
		{"zero ephemeral storage", func(m map[string]any) { resources(m)["ephemeral_storage"] = "0" }, false},
		{"zero workspace", func(m map[string]any) { m["workspace"] = map[string]any{"size_limit": "0"} }, false},
		{"image over 512 bytes", func(m map[string]any) {
			m["image"] = "r/" + strings.Repeat("a", 520) + "@sha256:" + strings.Repeat("0", 64)
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m map[string]any
			if err := json.Unmarshal(encodePayload(t, testPayload()), &m); err != nil {
				t.Fatal(err)
			}
			tc.edit(m)
			assertPayload(t, encodePayload(t, m), tc.ok, ErrInvalidPayload)
		})
	}
}

func TestPayloadSizeAndCredentialBounds(t *testing.T) {
	raw := encodePayload(t, testPayload())
	padded := append([]byte("{"+strings.Repeat(" ", MaxPayloadBytes)), raw[1:]...)
	assertPayload(t, padded, false, ErrInvalidPayload)
	assertPayload(t, append([]byte("{"+strings.Repeat(" ", MaxPayloadBytes-len(raw))), raw[1:]...), true, nil)

	allow := map[string]string{}
	var refs []string
	for i := range maxCredentials + 1 {
		ref := fmt.Sprintf("ref-%d", i)
		allow[ref] = "secret-" + ref
		refs = append(refs, ref)
	}
	for _, tc := range []struct {
		refs []string
		ok   bool
	}{{refs[:maxCredentials], true}, {refs, false}} {
		server := newAPIServer(t)
		cfg := testConfig(server, &fakeRegistry{})
		cfg.Credentials = allow
		c, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		payload := testPayload()
		payload.Credentials = tc.refs
		intent := testIntent(t, 1)
		intent.Workload.Payload = encodePayload(t, payload)
		err = c.EnsureWorkload(context.Background(), intent)
		if tc.ok != (err == nil) || (!tc.ok && !errors.Is(err, ErrInvalidPayload)) {
			t.Fatalf("%d credentials: err = %v, want ok=%v", len(tc.refs), err, tc.ok)
		}
	}
}

func TestPayloadDecodeIsStrictOnMemberNames(t *testing.T) {
	base := string(encodePayload(t, testPayload()))
	must := func(old, new string) []byte {
		if strings.Count(base, old) != 1 {
			t.Fatalf("anchor %q", old)
		}
		return []byte(strings.Replace(base, old, new, 1))
	}
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"case-variant member", must(`"image":`, `"IMAGE":`)},
		{"case-variant nested member", must(`"cpu":`, `"CPU":`)},
		{"case-variant host_settings", must(`"host_settings":`, `"Host_Settings":`)},
		{"duplicate image, tag then digest", must(`{"image":`, `{"image":"registry.example/looprig/host:latest","image":`)},
		{"duplicate host setting, token first", must(`"HOST_WARM_TTL":"5m"`, `"HOST_WARM_TTL":"Bearer-x","HOST_WARM_TTL":"5m"`)},
		{"duplicate nested member", must(`"cpu":"500m"`, `"cpu":"64","cpu":"500m"`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := assertPayload(t, tc.raw, false, ErrInvalidPayload)
			if !errors.Is(err, strictjson.ErrNotStrict) {
				t.Fatalf("err = %v, want it refused by the strict pre-pass", err)
			}
		})
	}
}

// assertPayload runs one Ensure with raw as the payload. A refusal must wrap
// want and reach the cluster not at all; an acceptance must create one Pod.
func assertPayload(t *testing.T, raw []byte, ok bool, want error) error {
	t.Helper()
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 1)
	intent.Workload = sessionstore.DesiredWorkload{PayloadVersion: PayloadVersionV1, Payload: raw}
	err := c.EnsureWorkload(context.Background(), intent)
	if ok {
		if err != nil || server.count("create") != 1 {
			t.Fatalf("err = %v, creates = %d; want accepted with exactly one create", err, server.count("create"))
		}
		return nil
	}
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	if len(server.Actions()) != 0 {
		t.Fatalf("a refused payload reached the cluster: %v", server.verbs())
	}
	return err
}
