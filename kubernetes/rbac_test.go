package kubernetes

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

// The shipped RBAC is namespace-only and grants EXACTLY the (resource, verb)
// pairs the adapter's own calls make -- derived from the calls, not listed by
// hand, so an added call with no grant, or a grant no call needs, fails here.
func TestRBACGrantsExactlyWhatTheAdapterCalls(t *testing.T) {
	raw, err := os.ReadFile("../deploy/rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var roles []*rbacv1.Role
	decode := scheme.Codecs.UniversalDeserializer()
	for _, doc := range strings.Split(string(raw), "\n---\n") {
		obj, gvk, err := decode.Decode([]byte(doc), nil, nil)
		if err != nil {
			t.Fatalf("decode rbac document: %v", err)
		}
		switch gvk.Kind {
		case "Role":
			roles = append(roles, obj.(*rbacv1.Role))
		case "RoleBinding":
			binding := obj.(*rbacv1.RoleBinding)
			if binding.RoleRef.Kind != "Role" {
				t.Fatalf("RoleBinding references a %s", binding.RoleRef.Kind)
			}
		case "ServiceAccount":
		default:
			t.Fatalf("rbac.yaml carries a %s; only namespace-scoped ServiceAccount/Role/RoleBinding are allowed", gvk.Kind)
		}
	}
	if len(roles) != 1 {
		t.Fatalf("roles = %d, want exactly 1", len(roles))
	}
	granted := map[string]bool{}
	for _, rule := range roles[0].Rules {
		if !slices.Equal(rule.APIGroups, []string{""}) || len(rule.ResourceNames) != 0 || len(rule.NonResourceURLs) != 0 {
			t.Fatalf("rule %+v is not a plain core-group rule", rule)
		}
		for _, resource := range rule.Resources {
			for _, verb := range rule.Verbs {
				if verb == "*" || resource == "*" {
					t.Fatalf("wildcard grant %s/%s", resource, verb)
				}
				granted[resource+"/"+verb] = true
			}
		}
	}

	// Drive every adapter path that reaches the API server: create (list,
	// get, create), adopt (list, get), observe (get) and delete (get, delete).
	server := newAPIServer(t, testSecret())
	c := newTestController(t, server, &fakeRegistry{})
	intent := testIntent(t, 1)
	ensure(t, c, intent)
	ensure(t, c, intent)
	server.setStatus(t, WorkloadName(intent), readyStatus())
	if _, _, err := c.ObserveWorkload(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteWorkload(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	used := map[string]bool{}
	for _, action := range server.Actions() {
		used[action.GetResource().Resource+"/"+action.GetVerb()] = true
	}
	if !mapsEqual(granted, used) {
		t.Fatalf("granted %v, adapter uses %v; want exactly equal", keys(granted), keys(used))
	}
}

func mapsEqual(a, b map[string]bool) bool {
	return slices.Equal(keys(a), keys(b))
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
