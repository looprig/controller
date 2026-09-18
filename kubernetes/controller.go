// Package kubernetes is the Kubernetes platform adapter for Factory's
// WorkloadController seam: one direct Pod per dedicated session's desired
// revision.
//
// # What this adapter is, and what it is not
//
// It CREATES, ADOPTS, OBSERVES and DELETES Pods. It does not decide that a
// session should have one -- Factory authors that as a durable PlacementIntent
// -- and it does not decide that a Host owns a session.
//
// A READY POD IS ONLY A PLACEMENT CANDIDATE. Kubernetes readiness says a
// container is up; it says nothing about which process holds the session's
// lease. ObserveWorkload therefore answers from the epoch-fenced Host registry
// record in SessionStore, and consults readiness only as a precondition: a Pod
// that is not ready is not looked up at all, and a ready Pod with no live
// matching registration is reported as not found. Readiness never replaces the
// session lease.
//
// It never deletes as a side effect. EnsureWorkload refuses, with a typed
// error, when another generation's workload exists for the session; replacing
// it requires a drain first, and drain-before-delete ordering is task D2.2's.
// RequestDrain is refused (ErrDrainNotImplemented) rather than faked.
package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8s "k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

var (
	// ErrInvalidConfig reports a Config New refuses.
	ErrInvalidConfig = errors.New("kubernetes: invalid controller configuration")
	// ErrInvalidIntent reports an intent this adapter must not act on.
	ErrInvalidIntent = errors.New("kubernetes: invalid placement intent")
	// ErrUnsupportedPayload reports a workload payload version this adapter
	// does not render.
	ErrUnsupportedPayload = errors.New("kubernetes: unsupported workload payload version")
	// ErrInvalidPayload reports a payload that violates PayloadV1's constraints.
	ErrInvalidPayload = errors.New("kubernetes: invalid workload payload")
	// ErrSettingNotAllowed reports a host_settings name outside the adapter's
	// allowlist -- including every adapter-owned variable and any tenant
	// variable. It always arrives wrapped with ErrInvalidPayload.
	ErrSettingNotAllowed = errors.New("kubernetes: host_settings names a variable this adapter does not accept")
	// ErrCredentialNotAllowed reports a credential reference outside the
	// controller's allowlist.
	ErrCredentialNotAllowed = errors.New("kubernetes: credential reference is not allowlisted")
	// ErrOwnershipConflict reports a Pod this adapter did not create for this
	// intent occupying its name or its session. It is never adopted, updated
	// or deleted.
	ErrOwnershipConflict = errors.New("kubernetes: pod ownership or labels do not match")
	// ErrSpecMismatch reports an owned Pod whose recorded spec is not the one
	// this adapter renders for the intent now. It is not adopted.
	ErrSpecMismatch = errors.New("kubernetes: existing pod spec does not match the desired spec")
	// ErrGenerationConflict reports another generation's workload for the
	// same session. Nothing is created and nothing is deleted.
	ErrGenerationConflict = errors.New("kubernetes: another generation's workload exists for this session")
	// ErrWorkloadTerminated reports a Pod whose Host process has exited.
	ErrWorkloadTerminated = errors.New("kubernetes: workload pod has terminated")
	// ErrWorkloadTerminating reports a Pod already being deleted.
	ErrWorkloadTerminating = errors.New("kubernetes: workload pod is terminating")
	// ErrWorkloadReplaced reports a delete refused by its UID precondition:
	// the object under the name is not the one this adapter observed.
	ErrWorkloadReplaced = errors.New("kubernetes: workload pod was replaced; delete refused by uid precondition")
	// ErrTooManyWorkloads reports more Pods labelled for one session than a
	// single bounded list returns.
	ErrTooManyWorkloads = errors.New("kubernetes: too many workloads for one session")
	// ErrDrainNotImplemented is RequestDrain's answer until task D2.2 sends
	// the drain over the authenticated HostLink.
	ErrDrainNotImplemented = errors.New("kubernetes: host drain is not implemented by this adapter (task D2.2)")
)

// GenerationConflictError names the other generations found for the session.
type GenerationConflictError struct {
	Intent uint64
	Older  []uint64
	Newer  []uint64
}

func (e *GenerationConflictError) Error() string {
	return fmt.Sprintf("kubernetes: desired generation %d conflicts with existing workloads (older %v, newer %v)", e.Intent, e.Older, e.Newer)
}

// Unwrap classifies the error as ErrGenerationConflict.
func (e *GenerationConflictError) Unwrap() error { return ErrGenerationConflict }

// Registry is the SessionStore surface ObserveWorkload reads. A
// *sessionstore.Store satisfies it.
type Registry interface {
	GetHostRegistration(ctx context.Context, req sessionstore.GetHostRegistrationRequest) (sessionstore.HostRegistrationEntry, error)
}

// Clock is the time seam.
type Clock interface{ Now() time.Time }

// SystemClock is the standard library's clock.
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }

// Config is one adapter's configuration. Every member is required.
type Config struct {
	// Client reaches the API server. Only Pods in Namespace are touched.
	Client k8s.Interface
	// Namespace is the single namespace this adapter creates Pods in.
	Namespace string
	// ControllerID names this controller DEPLOYMENT (not replica). Its digest
	// is the owner label; a Pod carrying another owner is never adopted.
	ControllerID string
	// HostSubdomain is a deployment-owned headless Service in Namespace whose
	// selector matches LabelManagedBy. It gives every Host Pod a stable DNS
	// name; this adapter does not create it.
	HostSubdomain string
	// HostPort is the port the Host listens for HostLink on.
	HostPort int32
	// Credentials is the credential-reference allowlist: reference name ->
	// Secret name in Namespace. A payload may name only these references.
	Credentials map[string]string
	// Registry is the Host registry ObserveWorkload answers from.
	Registry Registry
	// Clock decides whether a registry route has lapsed.
	Clock Clock
}

// MaxSessionWorkloads bounds the one list EnsureWorkload makes per session.
const MaxSessionWorkloads = 16

// Controller implements factory.WorkloadController over direct Pods.
type Controller struct {
	cfg   Config
	pods  corev1client.PodInterface
	owner string
}

// New validates cfg before anything can reach the API server.
func New(cfg Config) (*Controller, error) {
	switch {
	case cfg.Client == nil:
		return nil, fmt.Errorf("%w: Client is nil", ErrInvalidConfig)
	case !validDNSLabel(cfg.Namespace):
		return nil, fmt.Errorf("%w: Namespace must be a DNS label", ErrInvalidConfig)
	case cfg.ControllerID == "":
		return nil, fmt.Errorf("%w: ControllerID is empty", ErrInvalidConfig)
	case !validDNSLabel(cfg.HostSubdomain):
		return nil, fmt.Errorf("%w: HostSubdomain must be a DNS label", ErrInvalidConfig)
	case cfg.HostPort < 1 || cfg.HostPort > 65535:
		return nil, fmt.Errorf("%w: HostPort must be 1..65535", ErrInvalidConfig)
	case len(cfg.Credentials) == 0:
		return nil, fmt.Errorf("%w: the credential allowlist is empty", ErrInvalidConfig)
	case cfg.Registry == nil:
		return nil, fmt.Errorf("%w: Registry is nil", ErrInvalidConfig)
	case cfg.Clock == nil:
		return nil, fmt.Errorf("%w: Clock is nil", ErrInvalidConfig)
	}
	credentials := make(map[string]string, len(cfg.Credentials))
	for ref, secret := range cfg.Credentials {
		// The reference becomes part of a volume name ("cred-" + ref, a DNS
		// label of at most 63 bytes); the Secret name must be a Secret name.
		if !validDNSLabel(ref) || len(credentialPrefix+ref) > 63 {
			return nil, fmt.Errorf("%w: credential reference names must be DNS labels of at most %d bytes", ErrInvalidConfig, 63-len(credentialPrefix))
		}
		if !validDNSSubdomain(secret) {
			return nil, fmt.Errorf("%w: credential Secret names must be DNS subdomains", ErrInvalidConfig)
		}
		credentials[ref] = secret
	}
	cfg.Credentials = credentials
	return &Controller{
		cfg:   cfg,
		pods:  cfg.Client.CoreV1().Pods(cfg.Namespace),
		owner: ownerHash(cfg.ControllerID),
	}, nil
}

// EnsureWorkload makes the intent's Pod exist, exactly once.
//
// In order: validate and render (no cluster call on a refused intent); list
// the session's Pods (bounded) and refuse -- never delete -- if another
// generation's or a foreign Pod is present; Get the derived name and adopt it
// only if every ownership label, the absence of owner references and the
// recorded spec hash match; otherwise Create, and on AlreadyExists re-read
// and apply the same adoption check. A create whose response was lost is thus
// adopted by the retry rather than created twice.
//
// The list-then-create is not atomic across replicas: two adapters acting on
// DIFFERENT generations at the same instant could each create one. The
// durable reconciliation claim the driver takes per session is what
// serialises them; this method alone does not.
func (c *Controller) EnsureWorkload(ctx context.Context, intent sessionstore.PlacementIntent) error {
	want, err := c.desired(intent)
	if err != nil {
		return err
	}
	if err := c.refuseOtherWorkloads(ctx, intent, want.Name); err != nil {
		return err
	}
	existing, err := c.pods.Get(ctx, want.Name, metav1.GetOptions{})
	switch {
	case err == nil:
		return c.adopt(existing, want)
	case !apierrors.IsNotFound(err):
		return fmt.Errorf("kubernetes: read workload: %w", err)
	}
	_, err = c.pods.Create(ctx, want, metav1.CreateOptions{})
	switch {
	case err == nil:
		return nil
	case !apierrors.IsAlreadyExists(err):
		return fmt.Errorf("kubernetes: create workload: %w", err)
	}
	existing, err = c.pods.Get(ctx, want.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("kubernetes: re-read workload after a create conflict: %w", err)
	}
	return c.adopt(existing, want)
}

// refuseOtherWorkloads lists every Pod labelled for the intent's session and
// refuses if any is not the intent's own.
func (c *Controller) refuseOtherWorkloads(ctx context.Context, intent sessionstore.PlacementIntent, name string) error {
	selector := labels.SelectorFromSet(labels.Set{LabelSession: SessionHash(intent.TenantID, intent.SessionID)})
	list, err := c.pods.List(ctx, metav1.ListOptions{LabelSelector: selector.String(), Limit: MaxSessionWorkloads})
	if err != nil {
		return fmt.Errorf("kubernetes: list session workloads: %w", err)
	}
	if list.Continue != "" || len(list.Items) > MaxSessionWorkloads {
		return ErrTooManyWorkloads
	}
	conflict := &GenerationConflictError{Intent: intent.Generation}
	for i := range list.Items {
		pod := &list.Items[i]
		if pod.Name == name {
			continue // judged by adopt against the full desired Pod
		}
		if err := c.checkOwned(pod); err != nil {
			return err
		}
		generation, err := strconv.ParseUint(pod.Labels[LabelGeneration], 10, 64)
		if err != nil || generation == 0 || pod.Labels[LabelWorkload] != pod.Name {
			return fmt.Errorf("%w: a session workload carries no valid generation", ErrOwnershipConflict)
		}
		switch {
		case generation < intent.Generation:
			conflict.Older = append(conflict.Older, generation)
		case generation > intent.Generation:
			conflict.Newer = append(conflict.Newer, generation)
		default:
			// Same session and generation under a different name: the name
			// derivation disagrees (agent or runtime differ). Not ours to judge.
			return fmt.Errorf("%w: another workload claims this session and generation", ErrOwnershipConflict)
		}
	}
	if len(conflict.Older) == 0 && len(conflict.Newer) == 0 {
		return nil
	}
	slices.Sort(conflict.Older)
	slices.Sort(conflict.Newer)
	return conflict
}

// checkOwned is the ownership half of adoption: this adapter's marker, this
// controller's owner digest, and no owner references (a Pod some other
// controller owns is never ours, whatever its labels say).
func (c *Controller) checkOwned(pod *corev1.Pod) error {
	if pod.Labels[LabelManagedBy] != ManagedByValue {
		return fmt.Errorf("%w: managed-by label", ErrOwnershipConflict)
	}
	if pod.Labels[LabelOwner] != c.owner {
		return fmt.Errorf("%w: owner label", ErrOwnershipConflict)
	}
	if len(pod.OwnerReferences) != 0 {
		return fmt.Errorf("%w: pod has owner references", ErrOwnershipConflict)
	}
	return nil
}

// checkIdentity is checkOwned plus the intent's exact identity labels.
func (c *Controller) checkIdentity(pod, want *corev1.Pod) error {
	if err := c.checkOwned(pod); err != nil {
		return err
	}
	for _, key := range []string{LabelSession, LabelWorkload, LabelGeneration} {
		if pod.Labels[key] != want.Labels[key] {
			return fmt.Errorf("%w: %s label", ErrOwnershipConflict, key)
		}
	}
	return nil
}

// adopt accepts an existing Pod as the intent's workload, or refuses.
func (c *Controller) adopt(pod, want *corev1.Pod) error {
	if err := c.checkIdentity(pod, want); err != nil {
		return err
	}
	if pod.Annotations[AnnotationSpecHash] != want.Annotations[AnnotationSpecHash] {
		return ErrSpecMismatch
	}
	if pod.DeletionTimestamp != nil {
		return ErrWorkloadTerminating
	}
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return ErrWorkloadTerminated
	}
	return nil
}

// ObserveWorkload reports the Host registry observation for the intent's
// workload, or (zero, false, nil) when there is none to report.
//
// The Pod must exist, pass adoption and be Ready before the registry is read;
// the registry route must then be live at Clock.Now, carry dedicated placement
// and name exactly the HostID, HostGeneration, AgentID and runtime
// compatibility this adapter configured. Anything else is "not found": a Pod
// being Ready is never, on its own, a placement.
func (c *Controller) ObserveWorkload(ctx context.Context, intent sessionstore.PlacementIntent) (sessionwire.HostLinkRegistryObservation, bool, error) {
	var none sessionwire.HostLinkRegistryObservation
	want, err := c.desired(intent)
	if err != nil {
		return none, false, err
	}
	pod, err := c.pods.Get(ctx, want.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return none, false, nil
	case err != nil:
		return none, false, fmt.Errorf("kubernetes: read workload: %w", err)
	}
	if err := c.adopt(pod, want); err != nil {
		if errors.Is(err, ErrWorkloadTerminated) || errors.Is(err, ErrWorkloadTerminating) {
			return none, false, nil
		}
		return none, false, err
	}
	if !podReady(pod) {
		return none, false, nil
	}

	entry, err := c.cfg.Registry.GetHostRegistration(ctx, sessionstore.GetHostRegistrationRequest{
		TenantID: intent.TenantID, SessionID: intent.SessionID,
	})
	if err != nil {
		var registryErr *sessionstore.RegistryError
		if errors.As(err, &registryErr) {
			switch registryErr.Code {
			case sessionstore.RegistryErrorNotFound, sessionstore.RegistryErrorExpired, sessionstore.RegistryErrorReleased:
				return none, false, nil
			}
		}
		return none, false, fmt.Errorf("kubernetes: read host registration: %w", err)
	}
	registration := entry.Registration
	route := registration.Route
	if route == nil || !c.cfg.Clock.Now().Before(registration.ExpiresAt) {
		return none, false, nil
	}
	if route.HostID != HostID(intent) || route.HostGeneration != intent.Generation ||
		route.Placement != sessionwire.HostPlacementDedicated ||
		route.AgentID != intent.AgentID || route.RuntimeCompatibilityID != intent.RuntimeCompatibilityID {
		return none, false, nil
	}
	observation, err := registration.Observation()
	if err != nil {
		return none, false, fmt.Errorf("kubernetes: project host registration: %w", err)
	}
	return observation, true, nil
}

func podReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// RequestDrain is refused. Initiating a Host drain is an authenticated HostLink
// RPC (task D2.2), never a Pod deletion or a preStop hook, and reporting a
// drain this adapter did not send would let a caller proceed to delete.
func (c *Controller) RequestDrain(_ context.Context, _ sessionstore.PlacementIntent) (sessionwire.HostLinkDrainObservation, error) {
	return sessionwire.HostLinkDrainObservation{}, ErrDrainNotImplemented
}

// DeleteWorkload deletes the intent's Pod, preconditioned on the UID this call
// observed, so a Pod that replaced it under the same name is never deleted.
//
// It does NOT establish that the Host drained: Factory's contract calls it
// only after observing a completed drain, and the ordering is task D2.2's. The
// driver in this repository never calls it.
//
// An absent Pod is success (repeated delete is idempotent). A Pod already
// terminating is success with no second request. A Pod failing the ownership
// or identity labels is refused.
func (c *Controller) DeleteWorkload(ctx context.Context, intent sessionstore.PlacementIntent) error {
	want, err := c.identity(intent)
	if err != nil {
		return err
	}
	pod, err := c.pods.Get(ctx, want.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return fmt.Errorf("kubernetes: read workload: %w", err)
	}
	if err := c.checkIdentity(pod, want); err != nil {
		return err
	}
	if pod.DeletionTimestamp != nil {
		return nil
	}
	uid := pod.UID
	err = c.pods.Delete(ctx, want.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	switch {
	case err == nil, apierrors.IsNotFound(err):
		return nil
	case apierrors.IsConflict(err):
		return fmt.Errorf("%w: %w", ErrWorkloadReplaced, err)
	default:
		return fmt.Errorf("kubernetes: delete workload: %w", err)
	}
}
