// Package workload holds the platform-neutral view of one session's dedicated
// workloads that the driver's teardown state machine reads and the platform
// adapter produces.
//
// It exists so the driver names no platform type and the adapter names no
// driver type: both import this package and nothing of each other.
package workload

import (
	"errors"
	"fmt"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

// Workload is one platform workload labelled for a session, as the platform
// reports it at one read.
type Workload struct {
	// Name is the platform name. It IS the HostID the adapter configured into
	// the workload's Host, so a registry route naming it can only have been
	// published by a process started from this workload.
	Name string
	// UID is the platform's identity for this incarnation of Name. Every
	// destructive call is preconditioned on it.
	UID string
	// Revision is the platform's optimistic-concurrency token for the object
	// as read. A mark written against a stale revision is refused.
	Revision string
	// Generation is the desired generation the workload was created for. It
	// is the Host's HOST_GENERATION and the generation a termination names.
	Generation uint64
	// Endpoint is where this workload's Host serves HostLink for the
	// session's tenant, rendered by the adapter from its own configuration --
	// never read back from the registry, which any writer may mint.
	Endpoint sessionwire.InternalEndpoint

	// Terminal reports a workload whose Host process has exited (a Pod in
	// Failed or Succeeded).
	Terminal bool
	// Terminating reports a workload whose deletion has been requested.
	Terminating bool
	// Held reports that the controller's termination finalizer is present:
	// the platform object cannot disappear before the controller has recorded
	// how the workload ended.
	Held bool
	// Released reports that this controller recorded the workload's end and
	// released it: the released mark is written in the same update that
	// removes the finalizer.
	Released bool

	// Drain is the controller's persisted drain record, nil if none began.
	Drain *Drain
	// Decision is the controller's persisted termination decision, nil if
	// none was made.
	Decision *Decision
}

// Drain is what the controller persists on a workload before it first asks
// the workload's Host to drain. It is the durable anchor of the drain's
// deadline and of the registry epoch the Host held the session at.
type Drain struct {
	// Epoch is the registry lease epoch of the live route that named this
	// workload when the drain began. Always nonzero.
	Epoch uint64
	// StartedAt is the controller clock reading the drain began at.
	StartedAt time.Time
}

// Validate refuses a drain record no controller writes.
func (d Drain) Validate() error {
	if d.Epoch == 0 {
		return errors.New("workload: a drain record names no lease epoch")
	}
	if d.StartedAt.IsZero() {
		return errors.New("workload: a drain record has no start")
	}
	return nil
}

// Decision is how the controller decided a workload ended. It is decided
// from the controller's own observations and persisted BEFORE any fence
// tombstone is written or the workload is deleted, so a restarted controller
// records exactly what was decided rather than re-deriving it from state the
// controller may itself have changed.
type Decision struct {
	Kind   sessionstore.PlacementTerminationKind
	Reason sessionstore.PlacementForcedReason
	// Epoch is the registry lease epoch the controller observed for THIS
	// workload, and zero when it observed none. It is the epoch the fence
	// clears at and the epoch the termination records.
	Epoch uint64
}

// Validate holds a decision to the pairings SessionStore v0.11.0 accepts:
// graceful carries no reason and a nonzero epoch; forced carries one of the
// four known reasons.
func (d Decision) Validate() error {
	switch d.Kind {
	case sessionstore.PlacementTerminationGraceful:
		if d.Reason != "" {
			return errors.New("workload: a graceful decision carries a forced reason")
		}
		if d.Epoch == 0 {
			return errors.New("workload: a graceful decision names no lease epoch")
		}
		return nil
	case sessionstore.PlacementTerminationForced:
		switch d.Reason {
		case sessionstore.PlacementForcedDrainTimeout, sessionstore.PlacementForcedDrainRefused,
			sessionstore.PlacementForcedPlatformDeleted, sessionstore.PlacementForcedWorkloadTerminated:
			return nil
		}
		return fmt.Errorf("workload: forced reason %q is not one SessionStore records", d.Reason)
	default:
		return fmt.Errorf("workload: termination kind %q is not one SessionStore records", d.Kind)
	}
}

// Graceful is the decision for a drain observed complete at epoch.
func Graceful(epoch uint64) Decision {
	return Decision{Kind: sessionstore.PlacementTerminationGraceful, Epoch: epoch}
}

// Forced is a forced decision for reason at epoch.
func Forced(reason sessionstore.PlacementForcedReason, epoch uint64) Decision {
	return Decision{Kind: sessionstore.PlacementTerminationForced, Reason: reason, Epoch: epoch}
}
