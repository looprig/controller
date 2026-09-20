package kubernetes

import (
	"context"
	"errors"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkloadEndpointBeforeAttach(t *testing.T) {
	intent := testIntent(t, 2)
	for _, tc := range []struct {
		name   string
		create bool
		ready  bool
		change func(*corev1.Pod)
		want   bool
		fail   error
	}{
		{name: "absent"},
		{name: "not ready", create: true},
		{name: "adopted ready pod without registration", create: true, ready: true, want: true},
		{name: "wrong generation label", create: true, ready: true, change: func(p *corev1.Pod) { p.Labels[LabelGeneration] = "3" }, fail: ErrOwnershipConflict},
		{name: "wrong spec hash", create: true, ready: true, change: func(p *corev1.Pod) { p.Annotations[AnnotationSpecHash] = "other" }, fail: ErrSpecMismatch},
		{name: "wrong host target with original spec hash", create: true, ready: true, change: func(p *corev1.Pod) {
			for i := range p.Spec.Containers[0].Env {
				if p.Spec.Containers[0].Env[i].Name == "HOST_ID" {
					p.Spec.Containers[0].Env[i].Value = "other-host"
				}
			}
		}, fail: ErrSpecMismatch},
		{name: "wrong host DNS target", create: true, ready: true, change: func(p *corev1.Pod) { p.Spec.Hostname = "other-host" }, fail: ErrSpecMismatch},
		{name: "wrong owner", create: true, ready: true, change: func(p *corev1.Pod) { p.Labels[LabelOwner] = "other" }, fail: ErrOwnershipConflict},
		{name: "terminated", create: true, ready: true, change: func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed }},
		{name: "terminating", create: true, ready: true, change: func(p *corev1.Pod) { p.DeletionTimestamp = &metav1.Time{Time: testNow} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newAPIServer(t, testSecret())
			registry := &fakeRegistry{}
			c := newTestController(t, server, registry)
			if tc.create {
				ensure(t, c, intent)
				if tc.ready {
					server.setStatus(t, WorkloadName(intent), readyStatus())
				}
				if tc.change != nil {
					pod := server.pod(t, WorkloadName(intent))
					tc.change(pod)
					if err := server.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, testNamespace); err != nil {
						t.Fatal(err)
					}
				}
			}
			server.ClearActions()
			hostID, generation, base, found, err := c.WorkloadEndpoint(context.Background(), intent)
			if !errors.Is(err, tc.fail) || found != tc.want {
				t.Fatalf("endpoint = %q %d %q, found %v, err %v; want found %v, err %v", hostID, generation, base, found, err, tc.want, tc.fail)
			}
			if tc.want {
				wantBase, err := c.hostLinkBase(WorkloadName(intent), intent.TenantID)
				if err != nil {
					t.Fatal(err)
				}
				if hostID != HostID(intent) || generation != intent.Generation || base != wantBase {
					t.Fatalf("endpoint = %q %d %q, want %q %d %q", hostID, generation, base, HostID(intent), intent.Generation, wantBase)
				}
				if _, err := sessionwire.HostLinkEndpoint(base, intent.TenantID); err != nil {
					t.Fatalf("base is not routable: %v", err)
				}
			} else if hostID != "" || generation != 0 || base != "" {
				t.Fatalf("nonzero absent endpoint = %q %d %q", hostID, generation, base)
			}
			if registry.calls != 0 {
				t.Fatalf("pre-attach endpoint read registry %d times", registry.calls)
			}
			if got := server.verbs(); len(got) != 1 || got[0] != "get" {
				t.Fatalf("discovery pod calls = %v, want one get", got)
			}
		})
	}
}

func TestWorkloadEndpointIsNotARegistryObservation(t *testing.T) {
	server := newAPIServer(t, testSecret())
	registry := &fakeRegistry{}
	c := newTestController(t, server, registry)
	intent := testIntent(t, 2)
	ensure(t, c, intent)
	server.setStatus(t, WorkloadName(intent), readyStatus())
	if _, _, _, ready, err := c.WorkloadEndpoint(context.Background(), intent); err != nil || !ready {
		t.Fatalf("pre-attach endpoint = %v, %v; want ready", ready, err)
	}
	if _, found, err := c.ObserveWorkload(context.Background(), intent); err != nil || found {
		t.Fatalf("residency observation = %v, %v; want absent without registration", found, err)
	}
	if registry.calls != 1 {
		t.Fatalf("Observe registry reads = %d, want one; discovery must read none", registry.calls)
	}
}

func TestWorkloadEndpointRejectsReconfigurationAndOtherGeneration(t *testing.T) {
	server := newAPIServer(t, testSecret())
	registry := &fakeRegistry{}
	c := newTestController(t, server, registry)
	intent := testIntent(t, 2)
	ensure(t, c, intent)
	server.setStatus(t, WorkloadName(intent), readyStatus())

	next := intent
	next.Generation++
	if _, _, _, found, err := c.WorkloadEndpoint(context.Background(), next); err != nil || found {
		t.Fatalf("next generation discovery = %v, %v; want absent", found, err)
	}

	reconfigured := testConfig(server, registry)
	reconfigured.HostPort++
	changed, err := New(reconfigured)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, found, err := changed.WorkloadEndpoint(context.Background(), intent); !errors.Is(err, ErrSpecMismatch) || found {
		t.Fatalf("reconfigured discovery = %v, %v; want spec mismatch", found, err)
	}

	invalid := sessionstore.PlacementIntent{}
	server.ClearActions()
	if _, _, _, found, err := c.WorkloadEndpoint(context.Background(), invalid); !errors.Is(err, ErrInvalidIntent) || found {
		t.Fatalf("invalid discovery = %v, %v", found, err)
	}
	if len(server.Actions()) != 0 {
		t.Fatalf("invalid intent reached cluster: %v", server.verbs())
	}
}
