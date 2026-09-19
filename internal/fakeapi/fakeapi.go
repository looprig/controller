// Package fakeapi puts the API-server behaviours the controller relies on in
// front of client-go's fake clientset, for tests.
//
// client-go v0.37.0's fake is a plain object tracker. Measured (2026-09-18):
// it assigns no UID and no resourceVersion, accepts an update carrying a stale
// resourceVersion, ignores delete preconditions, and deletes an object that
// carries finalizers immediately. A controller test relying on it for any of
// those would be testing nothing, so Server adds exactly these behaviours for
// Pods, as reactors in front of the tracker:
//
//   - create assigns a UID (unless one is given) and resourceVersion "1";
//   - update (and status update) refuses a stale resourceVersion with a
//     Conflict, refuses a changed UID, refuses adding a finalizer to an object
//     being deleted, bumps the resourceVersion, and REMOVES an object being
//     deleted once its last finalizer is gone;
//   - delete honours a UID precondition with a Conflict, and on an object with
//     finalizers sets deletionTimestamp and keeps it rather than removing it.
//
// It is not a general API server: it models only what the adapter calls.
package fakeapi

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var podsResource = corev1.SchemeGroupVersion.WithResource("pods")

// Server is a fake clientset with the behaviours above.
type Server struct {
	*fake.Clientset
	mu      sync.Mutex
	nextUID int
	// Now stamps deletionTimestamp. It defaults to a fixed instant.
	Now func() time.Time
}

// New returns a Server holding objects.
func New(objects ...runtime.Object) *Server {
	s := &Server{Clientset: fake.NewClientset(objects...)}
	s.Now = func() time.Time { return time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC) }
	s.PrependReactor("create", "pods", s.create)
	s.PrependReactor("update", "pods", s.update)
	s.PrependReactor("delete", "pods", s.delete)
	return s
}

func (s *Server) create(action k8stesting.Action) (bool, runtime.Object, error) {
	pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
	s.mu.Lock()
	defer s.mu.Unlock()
	if pod.UID == "" {
		s.nextUID++
		pod.UID = types.UID(fmt.Sprintf("uid-%d", s.nextUID))
	}
	pod.ResourceVersion = "1"
	if err := s.Tracker().Create(podsResource, pod, action.GetNamespace()); err != nil {
		return true, nil, err
	}
	return true, pod.DeepCopy(), nil
}

func (s *Server) update(action k8stesting.Action) (bool, runtime.Object, error) {
	pod := action.(k8stesting.UpdateAction).GetObject().(*corev1.Pod).DeepCopy()
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, err := s.get(action.GetNamespace(), pod.Name)
	if err != nil {
		return true, nil, err
	}
	if pod.ResourceVersion != "" && pod.ResourceVersion != stored.ResourceVersion {
		return true, nil, apierrors.NewConflict(corev1.Resource("pods"), pod.Name,
			fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again"))
	}
	if pod.UID != "" && pod.UID != stored.UID {
		return true, nil, apierrors.NewConflict(corev1.Resource("pods"), pod.Name,
			fmt.Errorf("Precondition failed: UID in precondition: %v, UID in object meta: %v", pod.UID, stored.UID))
	}
	pod.UID = stored.UID
	if action.GetSubresource() == "status" {
		// A status write changes status only.
		next := stored.DeepCopy()
		next.Status = pod.Status
		pod = next
	} else {
		pod.Status = stored.Status
		pod.DeletionTimestamp = stored.DeletionTimestamp
		pod.DeletionGracePeriodSeconds = stored.DeletionGracePeriodSeconds
		if stored.DeletionTimestamp != nil {
			for _, f := range pod.Finalizers {
				if !contains(stored.Finalizers, f) {
					return true, nil, apierrors.NewForbidden(corev1.Resource("pods"), pod.Name,
						fmt.Errorf("no new finalizers can be added if the object is being deleted"))
				}
			}
		}
	}
	pod.ResourceVersion = bump(stored.ResourceVersion)
	if pod.DeletionTimestamp != nil && len(pod.Finalizers) == 0 {
		if err := s.Tracker().Delete(podsResource, action.GetNamespace(), pod.Name); err != nil {
			return true, nil, err
		}
		return true, pod, nil
	}
	if err := s.Tracker().Update(podsResource, pod, action.GetNamespace()); err != nil {
		return true, nil, err
	}
	return true, pod.DeepCopy(), nil
}

func (s *Server) delete(action k8stesting.Action) (bool, runtime.Object, error) {
	del := action.(k8stesting.DeleteActionImpl)
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, err := s.get(del.Namespace, del.Name)
	if err != nil {
		return true, nil, err
	}
	if p := del.DeleteOptions.Preconditions; p != nil && p.UID != nil && *p.UID != stored.UID {
		return true, nil, apierrors.NewConflict(corev1.Resource("pods"), del.Name,
			fmt.Errorf("Precondition failed: UID in precondition: %v, UID in object meta: %v", *p.UID, stored.UID))
	}
	if len(stored.Finalizers) == 0 {
		return true, nil, s.Tracker().Delete(podsResource, del.Namespace, del.Name)
	}
	if stored.DeletionTimestamp == nil {
		now := metav1.NewTime(s.Now())
		stored.DeletionTimestamp = &now
		grace := int64(30)
		if stored.Spec.TerminationGracePeriodSeconds != nil {
			grace = *stored.Spec.TerminationGracePeriodSeconds
		}
		stored.DeletionGracePeriodSeconds = &grace
		stored.ResourceVersion = bump(stored.ResourceVersion)
		if err := s.Tracker().Update(podsResource, stored, del.Namespace); err != nil {
			return true, nil, err
		}
	}
	return true, nil, nil
}

func (s *Server) get(namespace, name string) (*corev1.Pod, error) {
	obj, err := s.Tracker().Get(podsResource, namespace, name)
	if err != nil {
		return nil, err
	}
	return obj.(*corev1.Pod).DeepCopy(), nil
}

// SetStatus overwrites a Pod's status as a kubelet would, bumping its
// resourceVersion.
func (s *Server) SetStatus(namespace, name string, status corev1.PodStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	pod, err := s.get(namespace, name)
	if err != nil {
		return err
	}
	pod.Status = status
	pod.ResourceVersion = bump(pod.ResourceVersion)
	return s.Tracker().Update(podsResource, pod, namespace)
}

// Replace swaps the object under name for a new incarnation with a new UID,
// as a delete-and-recreate by someone else would.
func (s *Server) Replace(namespace, name string) (types.UID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pod, err := s.get(namespace, name)
	if err != nil {
		return "", err
	}
	s.nextUID++
	pod.UID = types.UID(fmt.Sprintf("uid-%d", s.nextUID))
	pod.DeletionTimestamp = nil
	pod.ResourceVersion = bump(pod.ResourceVersion)
	return pod.UID, s.Tracker().Update(podsResource, pod, namespace)
}

func bump(rv string) string {
	n, _ := strconv.Atoi(rv)
	return strconv.Itoa(n + 1)
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
