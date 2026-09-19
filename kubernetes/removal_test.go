package kubernetes

import (
	"context"
	"os"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// N1: two controller deployments may share a namespace (LabelOwner exists for
// exactly that), so the selector an operator strips finalizers by must name
// ONE deployment's Pods. A selector on managed-by alone would strip the
// other, still-running deployment's finalizers and race it.
func TestOwnerSelectorSelectsOnlyThisDeploymentsPods(t *testing.T) {
	server := newAPIServer(t, testSecret())
	ours := newTestController(t, server, &fakeRegistry{})
	otherCfg := testConfig(server, &fakeRegistry{})
	otherCfg.ControllerID = "another-controller"
	theirs, err := New(otherCfg)
	if err != nil {
		t.Fatal(err)
	}
	ensure(t, ours, testIntent(t, 1))
	theirIntent := testIntent(t, 1)
	theirIntent.SessionID = "session-theirs"
	ensure(t, theirs, theirIntent)

	selector := OwnerSelector(testControllerID)
	if want := LabelManagedBy + "=" + ManagedByValue + "," + LabelOwner + "=" + goldenOwner; selector != want {
		t.Fatalf("OwnerSelector = %q, want exactly %q", selector, want)
	}
	list, err := server.CoreV1().Pods(testNamespace).List(context.Background(), metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].Name != WorkloadName(testIntent(t, 1)) {
		names := make([]string, 0, len(list.Items))
		for _, p := range list.Items {
			names = append(names, p.Name)
		}
		t.Fatalf("selector %q matched %v, want exactly this deployment's one Pod", selector, names)
	}
}

// The README's removal procedure must use the owner-scoped selector, and a
// patch that does not fail on a Pod that has no finalizers.
func TestTheREADMERemovalProcedureIsOwnerScoped(t *testing.T) {
	raw, err := os.ReadFile("../README.md")
	if err != nil {
		t.Fatal(err)
	}
	readme := string(raw)
	want := "-l " + LabelManagedBy + "=" + ManagedByValue + "," + LabelOwner + "=<owner-digest>"
	if !strings.Contains(readme, want) {
		t.Fatalf("README removal procedure does not select by %q", want)
	}
	if strings.Contains(readme, "-l "+LabelManagedBy+"="+ManagedByValue+" ") {
		t.Fatalf("README still selects by managed-by alone")
	}
	if strings.Contains(readme, `"op":"remove","path":"/metadata/finalizers"`) {
		t.Fatalf("README still uses a JSON-patch remove, which fails on a Pod with no finalizers")
	}
}
