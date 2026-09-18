package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// The fake clientset is a plain object tracker: it assigns no UID on create
// and IGNORES delete preconditions. A test that relied on it for either would
// be testing nothing, so apiServer adds exactly those two API-server behaviours
// as reactors in front of the tracker, and every test ALSO asserts the action
// the adapter sent, so the precondition is proved on the wire rather than only
// by the reactor's reaction to it.

const (
	testNamespace    = "looprig-dedicated"
	testControllerID = "controller-under-test"
	testSubdomain    = "looprig-hosts"
	testPort         = 7443
	testImage        = "registry.example/looprig/host@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	// secretBytes is the content of a Secret the adapter is allowed to REFERENCE.
	// It must never appear in a rendered Pod.
	secretBytes = "s3cr3t-token-bytes-never-in-a-pod-spec"
)

type apiServer struct {
	*fake.Clientset
	mu      sync.Mutex
	nextUID int
}

func newAPIServer(t *testing.T, objects ...runtime.Object) *apiServer {
	t.Helper()
	server := &apiServer{Clientset: fake.NewClientset(objects...)}
	server.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
		if pod.UID == "" {
			server.mu.Lock()
			server.nextUID++
			pod.UID = types.UID(fmt.Sprintf("uid-%d", server.nextUID))
			server.mu.Unlock()
		}
		return false, nil, nil
	})
	server.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		del := action.(k8stesting.DeleteActionImpl)
		if del.DeleteOptions.Preconditions == nil || del.DeleteOptions.Preconditions.UID == nil {
			return false, nil, nil
		}
		current, err := server.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), del.Namespace, del.Name)
		if err != nil {
			return true, nil, err
		}
		have := current.(*corev1.Pod).UID
		if want := *del.DeleteOptions.Preconditions.UID; want != have {
			return true, nil, apierrors.NewConflict(corev1.Resource("pods"), del.Name,
				fmt.Errorf("Precondition failed: UID in precondition: %v, UID in object meta: %v", want, have))
		}
		return false, nil, nil
	})
	return server
}

// verbs returns the (verb) sequence of pod actions, in order.
func (s *apiServer) verbs() []string {
	var out []string
	for _, action := range s.Actions() {
		if action.GetResource().Resource == "pods" {
			out = append(out, action.GetVerb())
		}
	}
	return out
}

func (s *apiServer) count(verb string) int {
	n := 0
	for _, v := range s.verbs() {
		if v == verb {
			n++
		}
	}
	return n
}

func (s *apiServer) pods(t *testing.T) []corev1.Pod {
	t.Helper()
	list, err := s.CoreV1().Pods(testNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return list.Items
}

func (s *apiServer) pod(t *testing.T, name string) *corev1.Pod {
	t.Helper()
	pod, err := s.CoreV1().Pods(testNamespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod %s: %v", name, err)
	}
	return pod
}

// setStatus overwrites a Pod's status in the tracker, as a kubelet would.
func (s *apiServer) setStatus(t *testing.T, name string, status corev1.PodStatus) {
	t.Helper()
	pod := s.pod(t, name)
	pod.Status = status
	if err := s.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, testNamespace); err != nil {
		t.Fatalf("update status: %v", err)
	}
}

func readyStatus() corev1.PodStatus {
	return corev1.PodStatus{
		Phase:      corev1.PodRunning,
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
	}
}

// fakeRegistry is the Host registry seam. It answers exactly what it was told.
type fakeRegistry struct {
	mu    sync.Mutex
	entry *sessionstore.HostRegistrationEntry
	err   error
	calls int
}

func (r *fakeRegistry) GetHostRegistration(_ context.Context, req sessionstore.GetHostRegistrationRequest) (sessionstore.HostRegistrationEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.err != nil {
		return sessionstore.HostRegistrationEntry{}, r.err
	}
	if r.entry == nil {
		return sessionstore.HostRegistrationEntry{}, &sessionstore.RegistryError{Code: sessionstore.RegistryErrorNotFound, Field: "registration"}
	}
	if r.entry.Registration.TenantID != req.TenantID || r.entry.Registration.SessionID != req.SessionID {
		return sessionstore.HostRegistrationEntry{}, &sessionstore.RegistryError{Code: sessionstore.RegistryErrorNotFound, Field: "registration"}
	}
	return *r.entry, nil
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

var testNow = time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)

func testHostSettings() map[string]string {
	return map[string]string{
		"HOST_WARM_TTL":              "5m",
		"HOST_REGISTRY_HEARTBEAT":    "10s",
		"HOST_REGISTRY_EXPIRY":       "30s",
		"HOST_CLAIM_TTL":             "30s",
		"HOST_APPLY_DEADLINE":        "2m",
		"HOST_RECONCILE_INTERVAL":    "15s",
		"HOST_COMMAND_QUEUE_SIZE":    "64",
		"HOST_RECONCILE_BATCH":       "16",
		"HOST_MAX_BINDINGS_PER_LINK": "4",
		"HOST_MAX_BINDINGS":          "4",
		"HOST_MAX_TENANT_LINKS":      "4",
		"HOST_DRAIN_GRACE":           "30s",
		"HOST_DRAIN_IDLE_BOUNDARY":   "5s",
		"HOST_DRAIN_PUBLISH_BOUND":   "5s",
		"HOST_COMPATIBILITY_TIMEOUT": "5s",
		"HOST_WORK_POLL":             "1s",
	}
}

func testPayload() PayloadV1 {
	return PayloadV1{
		Image: testImage,
		Resources: Resources{
			CPU:              "500m",
			Memory:           "1Gi",
			EphemeralStorage: "2Gi",
		},
		Workspace:    Workspace{SizeLimit: "1Gi"},
		Credentials:  []string{"session-store", "hostlink-auth"},
		HostSettings: testHostSettings(),
	}
}

func encodePayload(t *testing.T, payload any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return raw
}

func testIntent(t *testing.T, generation uint64) sessionstore.PlacementIntent {
	t.Helper()
	return sessionstore.PlacementIntent{
		TenantID:               "tenant-acme",
		SessionID:              "session-0001",
		AgentID:                "agent-coder",
		RuntimeCompatibilityID: "runtime-2026-09",
		Placement:              sessionwire.HostPlacementDedicated,
		Workload: sessionstore.DesiredWorkload{
			PayloadVersion: PayloadVersionV1,
			Payload:        encodePayload(t, testPayload()),
		},
		Generation: generation,
	}
}

func testSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-session-store", Namespace: testNamespace},
		Data:       map[string][]byte{"token": []byte(secretBytes)},
	}
}

func testConfig(server *apiServer, registry Registry) Config {
	return Config{
		Client:        server.Clientset,
		Namespace:     testNamespace,
		ControllerID:  testControllerID,
		HostSubdomain: testSubdomain,
		HostPort:      testPort,
		Credentials: map[string]string{
			"session-store": "shared-session-store",
			"hostlink-auth": "hostlink-service-identity",
			"object-store":  "shared-object-store",
		},
		Registry: registry,
		Clock:    fixedClock{now: testNow},
	}
}

func newTestController(t *testing.T, server *apiServer, registry Registry) *Controller {
	t.Helper()
	controller, err := New(testConfig(server, registry))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return controller
}
