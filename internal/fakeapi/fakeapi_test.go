package fakeapi

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Each behaviour the controller's tests lean on, asserted against the fake
// itself, so a weakened fake fails here rather than silently passing them.
func TestTheFakeBehavesLikeTheAPIServerWhereTheControllerDependsOnIt(t *testing.T) {
	ctx := context.Background()
	s := New()
	pods := s.CoreV1().Pods("ns")
	created, err := pods.Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "a", Finalizers: []string{"x/y"}}}, metav1.CreateOptions{})
	if err != nil || created.UID == "" || created.ResourceVersion != "1" {
		t.Fatalf("create = %+v, %v; want a UID and resourceVersion 1", created.ObjectMeta, err)
	}
	created.Annotations = map[string]string{"k": "v"}
	updated, err := pods.Update(ctx, created, metav1.UpdateOptions{})
	if err != nil || updated.ResourceVersion != "2" {
		t.Fatalf("update = %v, %v; want resourceVersion 2", updated, err)
	}
	if _, err := pods.Update(ctx, created, metav1.UpdateOptions{}); !apierrors.IsConflict(err) {
		t.Fatalf("stale update err = %v, want Conflict", err)
	}
	wrong := types.UID("other")
	if err := pods.Delete(ctx, "a", metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &wrong}}); !apierrors.IsConflict(err) {
		t.Fatalf("precondition delete err = %v, want Conflict", err)
	}
	if err := pods.Delete(ctx, "a", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	held, err := pods.Get(ctx, "a", metav1.GetOptions{})
	if err != nil || held.DeletionTimestamp == nil {
		t.Fatalf("a finalized object must stay terminating: %v, %v", held, err)
	}
	held.Finalizers = append(held.Finalizers, "new/one")
	if _, err := pods.Update(ctx, held, metav1.UpdateOptions{}); !apierrors.IsForbidden(err) {
		t.Fatalf("adding a finalizer while deleting: err = %v, want Forbidden", err)
	}
	held.Finalizers = nil
	if _, err := pods.Update(ctx, held, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := pods.Get(ctx, "a", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("after the last finalizer: err = %v, want NotFound", err)
	}
	if _, err := pods.Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "b"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := pods.Delete(ctx, "b", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := pods.Get(ctx, "b", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("an unfinalized object must go at once: %v", err)
	}
}
