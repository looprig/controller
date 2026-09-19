package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	"github.com/looprig/controller/internal/strictjson"
	"github.com/looprig/controller/workload"
)

// Teardown operations.
//
// These are what the driver's drain-before-delete state machine calls. None
// of them decides anything: the adapter reports what the platform holds, and
// persists, deletes and releases exactly the object the driver names --
// preconditioned on the UID (and, for a mark, the revision) the driver read.
//
// # Why marks live on the Pod, and why the Pod carries a finalizer
//
// The termination KIND is decided from the controller's own observations --
// above all, whether it saw this workload's Host report its drain complete.
// That observation cannot be re-made once the Pod is deleted, and the ruling
// orders the durable record AFTER the delete. So the decision is persisted on
// the Pod before any fence or delete (AnnotationTermination), and the Pod
// carries Finalizer so the object -- and the decision on it -- outlives the
// delete until the termination is recorded and the finalizer released. A
// controller restarted anywhere in the sequence reads the decision back and
// records exactly what was decided, instead of re-deriving it from a registry
// it may itself have tombstoned. The drain's start and the epoch it began at
// are persisted the same way (AnnotationDrain), so the drain's deadline
// survives a restart or a replica failover.
//
// The cost is the standard finalizer cost: a controller that is uninstalled
// leaves its Pods' deletions waiting until an operator removes Finalizer.

const (
	// Finalizer holds a workload's Pod object until the controller has
	// recorded how the workload ended. Its string deliberately differs from
	// every annotation key.
	Finalizer = "controller.looprig.dev/record-termination"
	// AnnotationDrain is the persisted workload.Drain.
	AnnotationDrain = "controller.looprig.dev/drain"
	// AnnotationTermination is the persisted workload.Decision.
	AnnotationTermination = "controller.looprig.dev/termination"
	// AnnotationReleased marks a workload whose termination this controller
	// recorded and whose finalizer it removed, in one update.
	AnnotationReleased = "controller.looprig.dev/released"
)

var (
	// ErrWorkloadChanged reports a mark refused because the object is not the
	// revision (or the UID) the caller read. Re-read and decide again.
	ErrWorkloadChanged = errors.New("kubernetes: workload changed since it was read")
	// ErrMalformedMark reports a drain or termination annotation this adapter
	// cannot read. The workload is not acted on.
	ErrMalformedMark = errors.New("kubernetes: workload carries a malformed controller mark")
)

// ListWorkloads returns every workload labelled for the session, ascending by
// generation, or refuses.
//
// It is ONE bounded list. Every listed Pod must carry this controller's
// ownership (managed-by, owner digest, no owner references), a valid
// generation label and a workload label equal to its name, and readable
// marks; ANY Pod failing that fails the whole call, because a Pod labelled for
// the session that this controller cannot vouch for is one it must neither
// drain nor delete nor ignore while it replaces the session's workload.
func (c *Controller) ListWorkloads(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID) ([]workload.Workload, error) {
	if err := validateKey(tenant, session); err != nil {
		return nil, err
	}
	selector := labels.SelectorFromSet(labels.Set{LabelSession: SessionHash(tenant, session)})
	list, err := c.pods.List(ctx, metav1.ListOptions{LabelSelector: selector.String(), Limit: MaxSessionWorkloads})
	if err != nil {
		return nil, fmt.Errorf("kubernetes: list session workloads: %w", err)
	}
	if list.Continue != "" || len(list.Items) > MaxSessionWorkloads {
		return nil, ErrTooManyWorkloads
	}
	out := make([]workload.Workload, 0, len(list.Items))
	for i := range list.Items {
		w, err := c.view(&list.Items[i], tenant, session)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	slices.SortFunc(out, func(a, b workload.Workload) int {
		switch {
		case a.Generation < b.Generation:
			return -1
		case a.Generation > b.Generation:
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
	return out, nil
}

// view projects one Pod, refusing one this controller cannot vouch for.
func (c *Controller) view(pod *corev1.Pod, tenant sessionwire.TenantID, session sessionwire.SessionID) (workload.Workload, error) {
	if err := c.checkOwned(pod); err != nil {
		return workload.Workload{}, err
	}
	if pod.Labels[LabelSession] != SessionHash(tenant, session) {
		return workload.Workload{}, fmt.Errorf("%w: session label", ErrOwnershipConflict)
	}
	generation, err := strconv.ParseUint(pod.Labels[LabelGeneration], 10, 64)
	if err != nil || generation == 0 || pod.Labels[LabelWorkload] != pod.Name {
		return workload.Workload{}, fmt.Errorf("%w: a session workload carries no valid generation", ErrOwnershipConflict)
	}
	endpoint, err := c.hostLinkBase(pod.Name, tenant)
	if err != nil {
		return workload.Workload{}, err
	}
	w := workload.Workload{
		Name:        pod.Name,
		UID:         string(pod.UID),
		Revision:    pod.ResourceVersion,
		Generation:  generation,
		Endpoint:    endpoint,
		Terminal:    pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded,
		Terminating: pod.DeletionTimestamp != nil,
		Held:        slices.Contains(pod.Finalizers, Finalizer),
		Released:    pod.Annotations[AnnotationReleased] == "true",
	}
	if raw, ok := pod.Annotations[AnnotationDrain]; ok {
		d, err := decodeDrain(raw)
		if err != nil {
			return workload.Workload{}, err
		}
		w.Drain = &d
	}
	if raw, ok := pod.Annotations[AnnotationTermination]; ok {
		d, err := decodeDecision(raw)
		if err != nil {
			return workload.Workload{}, err
		}
		w.Decision = &d
	}
	return w, nil
}

// MarkDrain persists d on w, compare-and-swapped against the revision the
// caller read. It returns the workload as written.
func (c *Controller) MarkDrain(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, w workload.Workload, d workload.Drain) (workload.Workload, error) {
	if err := d.Validate(); err != nil {
		return workload.Workload{}, fmt.Errorf("%w: %w", ErrMalformedMark, err)
	}
	raw, err := json.Marshal(drainWire{Epoch: d.Epoch, StartedAt: d.StartedAt.UTC()})
	if err != nil {
		return workload.Workload{}, err
	}
	return c.mark(ctx, tenant, session, w, func(a map[string]string) { a[AnnotationDrain] = string(raw) })
}

// MarkDecision persists d on w, compare-and-swapped against the revision the
// caller read. It returns the workload as written.
func (c *Controller) MarkDecision(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, w workload.Workload, d workload.Decision) (workload.Workload, error) {
	if err := d.Validate(); err != nil {
		return workload.Workload{}, fmt.Errorf("%w: %w", ErrMalformedMark, err)
	}
	raw, err := json.Marshal(decisionWire{Kind: string(d.Kind), Reason: string(d.Reason), Epoch: d.Epoch})
	if err != nil {
		return workload.Workload{}, err
	}
	return c.mark(ctx, tenant, session, w, func(a map[string]string) { a[AnnotationTermination] = string(raw) })
}

// ClearMarks removes both marks from w, compare-and-swapped against the
// revision the caller read, so the next pass decides from scratch.
func (c *Controller) ClearMarks(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, w workload.Workload) (workload.Workload, error) {
	return c.mark(ctx, tenant, session, w, func(a map[string]string) {
		delete(a, AnnotationDrain)
		delete(a, AnnotationTermination)
	})
}

func (c *Controller) mark(ctx context.Context, tenant sessionwire.TenantID, session sessionwire.SessionID, w workload.Workload, edit func(map[string]string)) (workload.Workload, error) {
	pod, err := c.pods.Get(ctx, w.Name, metav1.GetOptions{})
	if err != nil {
		return workload.Workload{}, fmt.Errorf("kubernetes: read workload: %w", err)
	}
	if string(pod.UID) != w.UID || pod.ResourceVersion != w.Revision {
		return workload.Workload{}, ErrWorkloadChanged
	}
	if _, err := c.view(pod, tenant, session); err != nil {
		return workload.Workload{}, err
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	edit(pod.Annotations)
	updated, err := c.pods.Update(ctx, pod, metav1.UpdateOptions{})
	switch {
	case apierrors.IsConflict(err):
		return workload.Workload{}, fmt.Errorf("%w: %w", ErrWorkloadChanged, err)
	case err != nil:
		return workload.Workload{}, fmt.Errorf("kubernetes: mark workload: %w", err)
	}
	return c.view(updated, tenant, session)
}

// Terminate deletes w, preconditioned on its UID. The Pod's finalizer keeps
// the object (terminating) until Release. An absent Pod is success.
func (c *Controller) Terminate(ctx context.Context, w workload.Workload) error {
	if w.UID == "" {
		return fmt.Errorf("%w: a workload with no UID cannot be deleted by precondition", ErrInvalidIntent)
	}
	uid := types.UID(w.UID)
	err := c.pods.Delete(ctx, w.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	switch {
	case err == nil, apierrors.IsNotFound(err):
		return nil
	case apierrors.IsConflict(err):
		return fmt.Errorf("%w: %w", ErrWorkloadReplaced, err)
	default:
		return fmt.Errorf("kubernetes: delete workload: %w", err)
	}
}

// Release removes Finalizer from w and marks it released (AnnotationReleased),
// in one update, if the object under w's name is still w's UID. The mark is
// written even for a Pod that never carried the finalizer, so "finished" is
// never inferred from a finalizer's absence. An absent Pod, or one already
// released, is success.
func (c *Controller) Release(ctx context.Context, w workload.Workload) error {
	pod, err := c.pods.Get(ctx, w.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return fmt.Errorf("kubernetes: read workload: %w", err)
	}
	if string(pod.UID) != w.UID {
		return fmt.Errorf("%w: release", ErrWorkloadReplaced)
	}
	if !slices.Contains(pod.Finalizers, Finalizer) && pod.Annotations[AnnotationReleased] == "true" {
		return nil
	}
	pod.Finalizers = slices.DeleteFunc(pod.Finalizers, func(f string) bool { return f == Finalizer })
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[AnnotationReleased] = "true"
	_, err = c.pods.Update(ctx, pod, metav1.UpdateOptions{})
	switch {
	case err == nil, apierrors.IsNotFound(err):
		return nil
	case apierrors.IsConflict(err):
		return fmt.Errorf("%w: %w", ErrWorkloadChanged, err)
	default:
		return fmt.Errorf("kubernetes: release workload: %w", err)
	}
}

// validateKey is validateIntent's identity half, for calls that name a
// session rather than an intent.
func validateKey(tenant sessionwire.TenantID, session sessionwire.SessionID) error {
	if err := tenant.Validate(); err != nil {
		return fmt.Errorf("%w: tenant_id", ErrInvalidIntent)
	}
	if t := string(tenant); strings.Contains(t, "/") || t == "." || t == ".." {
		return fmt.Errorf("%w: tenant_id cannot be a HostLink path segment", ErrInvalidIntent)
	}
	if err := session.Validate(); err != nil {
		return fmt.Errorf("%w: session_id", ErrInvalidIntent)
	}
	return nil
}

// The marks' stored shapes. They are decoded strictly (exact member names, no
// repeats, no unknown or trailing data) and re-validated, so a hand-edited or
// corrupted mark is refused rather than read as a decision.

type drainWire struct {
	Epoch     uint64    `json:"epoch"`
	StartedAt time.Time `json:"started_at"`
}

type decisionWire struct {
	Kind   string `json:"kind"`
	Reason string `json:"reason,omitempty"`
	Epoch  uint64 `json:"epoch"`
}

var (
	drainSchema    = strictjson.Object{"epoch": nil, "started_at": nil}
	decisionSchema = strictjson.Object{"kind": nil, "reason": nil, "epoch": nil}
)

func decodeStrict(raw string, schema strictjson.Object, into any) error {
	if err := strictjson.Check([]byte(raw), schema); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing data")
	}
	return nil
}

func decodeDrain(raw string) (workload.Drain, error) {
	var w drainWire
	if err := decodeStrict(raw, drainSchema, &w); err != nil {
		return workload.Drain{}, fmt.Errorf("%w: drain", ErrMalformedMark)
	}
	d := workload.Drain{Epoch: w.Epoch, StartedAt: w.StartedAt}
	if err := d.Validate(); err != nil {
		return workload.Drain{}, fmt.Errorf("%w: drain", ErrMalformedMark)
	}
	return d, nil
}

func decodeDecision(raw string) (workload.Decision, error) {
	var w decisionWire
	if err := decodeStrict(raw, decisionSchema, &w); err != nil {
		return workload.Decision{}, fmt.Errorf("%w: termination", ErrMalformedMark)
	}
	d := workload.Decision{
		Kind:   sessionstore.PlacementTerminationKind(w.Kind),
		Reason: sessionstore.PlacementForcedReason(w.Reason),
		Epoch:  w.Epoch,
	}
	if err := d.Validate(); err != nil {
		return workload.Decision{}, fmt.Errorf("%w: termination", ErrMalformedMark)
	}
	return d, nil
}
