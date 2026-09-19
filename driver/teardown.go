package driver

import (
	"context"
	"errors"
	"fmt"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"

	"github.com/looprig/controller/hostlink"
	"github.com/looprig/controller/workload"
)

// DRAIN BEFORE DELETE.
//
// A workload the desire no longer names ends in this order, and the order is
// the mechanism:
//
//  1. deletion desire -- a dedicated placement naming no workload, or a newer
//     generation -- is read from the durable catalog (item);
//  2. the drain RPC is sent over the controller's own HostLink client, gated on
//     the Host ADVERTISING hostlink.drain (decide);
//  3. `drained` is observed for THIS workload: the observation must name the
//     workload's HostID and host generation, and the epoch comes from the
//     registry route naming the same pair (decide);
//  4. the Kind is DECIDED and PERSISTED on the workload, then the registry is
//     fenced with ClearHostRegistration at that epoch (teardown);
//  5. the workload is deleted, preconditioned on its UID (teardown);
//  6. the termination is recorded under the WORKLOAD'S generation, and the
//     workload's finalizer released (teardown).
//
// Pod deletion (and the kubelet's SIGTERM drain it triggers) is the failure
// backstop only: nothing here deletes a workload whose Host still holds the
// session until that Host has drained, the drain has timed out, or the drain
// could not be asked for.
//
// THE KIND IS DECIDED FROM THIS STATE MACHINE'S OWN OBSERVATIONS, never from
// the registry: by step 4 the controller may itself have tombstoned the route,
// and a Host writes the same tombstone after an UNCLEAN release. The table:
//
//	workload terminating, not by this controller   -> forced platform_deleted
//	workload terminal (Host process exited)         -> forced workload_terminated
//	no live route names it and no drain was begun   -> forced drain_refused (epoch 0)
//	the Host reports drained for this workload      -> graceful
//	the Host does not advertise the drain           -> forced drain_refused
//	the Host reports draining past the deadline     -> forced drain_timeout
//	a refusal or no answer, and no live route names
//	  it any more (runtime_unavailable is ambiguous:
//	  re-observe, never fail)                       -> forced drain_refused
//	a refusal or no answer past the deadline        -> forced drain_refused
//	anything else                                   -> undecided; ask again next pass

// DrainIdempotencyKeyPrefix prefixes the retry-stable key every drain request
// for one workload carries.
const DrainIdempotencyKeyPrefix = "looprig-controller/drain/"

// drainRequest is the Core drain request for one workload's Host.
func drainRequest(key Key, w workload.Workload) sessionwire.HostLinkDrainRequest {
	return sessionwire.HostLinkDrainRequest{
		Version:        sessionwire.CurrentWireVersion,
		HostID:         sessionwire.HostID(w.Name),
		HostGeneration: w.Generation,
		IdempotencyKey: DrainIdempotencyKeyPrefix + w.Name,
		TenantID:       key.TenantID,
		SessionID:      key.SessionID,
	}
}

// routeEpoch reports the epoch of a live route that names exactly this
// workload -- its HostID, its host generation, dedicated -- and whether one
// does. The registry is per SESSION, not per workload, so this match is what
// binds an epoch to the workload being ended.
func routeEpoch(live *sessionstore.HostRegistration, w workload.Workload) (uint64, bool) {
	if live == nil || live.Route == nil {
		return 0, false
	}
	route := live.Route
	if route.HostID != sessionwire.HostID(w.Name) || route.HostGeneration != w.Generation ||
		route.Placement != sessionwire.HostPlacementDedicated {
		return 0, false
	}
	return live.LeaseEpoch, true
}

// teardown advances one workload's drain-before-delete sequence. finished
// reports that the workload's termination is recorded (or unrecordable) and
// its finalizer released.
func (d *Driver) teardown(ctx context.Context, key Key, v view, w workload.Workload) (bool, error) {
	if w.Decision != nil && w.Terminating && !w.Held {
		// Finished on an earlier pass; the object waits on the kubelet.
		return true, nil
	}
	decision := w.Decision
	if decision == nil {
		decided, ok, marked, err := d.decide(ctx, key, v, w)
		if err != nil || !ok {
			return false, err
		}
		// Persisted BEFORE any fence tombstone or delete, so every later
		// step -- in this process or a restarted one -- acts on this decision.
		w, err = d.cfg.Workloads.MarkDecision(ctx, key.TenantID, key.SessionID, marked, decided)
		if err != nil {
			return false, fmt.Errorf("driver: persist termination decision: %w", err)
		}
		decision = &decided
	}

	if decision.Epoch > 0 {
		_, err := d.cfg.Registry.ClearHostRegistration(ctx, sessionstore.ClearHostRegistrationRequest{
			TenantID: key.TenantID, SessionID: key.SessionID, LeaseEpoch: decision.Epoch,
		})
		var registryErr *sessionstore.RegistryError
		switch {
		case err == nil:
		case errors.As(err, &registryErr) && registryErr.Code == sessionstore.RegistryErrorNotFound:
			// No registration at all: nothing to fence.
		case errors.As(err, &registryErr) && registryErr.Code == sessionstore.RegistryErrorEpoch:
			// A LATER epoch holds the session. If its live route names THIS
			// workload, the Host in it took the session again after the
			// decision: the decision is about a lease that no longer exists,
			// and deleting would kill the new owner. Drop it and decide
			// afresh. Otherwise the later holder is elsewhere, this workload
			// holds nothing, and the decision stands without a fence.
			live, err := d.liveRegistration(ctx, key)
			if err != nil {
				return false, err
			}
			if _, names := routeEpoch(live, w); names {
				if _, err := d.cfg.Workloads.ClearMarks(ctx, key.TenantID, key.SessionID, w); err != nil {
					return false, fmt.Errorf("driver: drop a fenced-out decision: %w", err)
				}
				d.cfg.Logger.Warn("controller decision fenced out by a later lease; deciding again",
					"tenant", key.TenantID, "session", key.SessionID, "workload", w.Name, "decided_epoch", decision.Epoch)
				return false, nil
			}
		default:
			return false, fmt.Errorf("driver: fence host registration: %w", err)
		}
	}

	if !w.Terminating {
		if err := d.cfg.Workloads.Terminate(ctx, w); err != nil {
			return false, fmt.Errorf("driver: delete workload: %w", err)
		}
	}

	_, _, err := d.cfg.Terminations.RecordPlacementTermination(ctx, sessionstore.RecordPlacementTerminationRequest{
		TenantID:           key.TenantID,
		SessionID:          key.SessionID,
		Generation:         w.Generation,
		Kind:               decision.Kind,
		ForcedReason:       decision.Reason,
		ObservedLeaseEpoch: decision.Epoch,
	})
	var terminationErr *sessionstore.TerminationError
	switch {
	case err == nil:
	case errors.As(err, &terminationErr) &&
		(terminationErr.Code == sessionstore.TerminationErrorSuperseded || terminationErr.Code == sessionstore.TerminationErrorMismatch):
		// OUTCOME UNRECORDABLE, not retryable. Superseded: a higher
		// generation's outcome is stored and this one can never be. Mismatch:
		// this generation's outcome is already decided differently -- the
		// store keeps one row per generation, so a recreated incarnation of
		// the same generation (G10) cannot record its own. Either way the
		// workload has ended and is released; holding it would block the
		// session's next workload for an audit row that cannot be written.
		d.cfg.Logger.Warn("controller termination unrecordable",
			"tenant", key.TenantID, "session", key.SessionID, "generation", w.Generation,
			"code", terminationErr.Code, "stored_generation", terminationErr.Generation)
	default:
		return false, fmt.Errorf("driver: record placement termination: %w", err)
	}

	if err := d.cfg.Workloads.Release(ctx, w); err != nil {
		return false, fmt.Errorf("driver: release workload: %w", err)
	}
	return true, nil
}

// decide runs the drain and returns the termination decision, or ok=false
// while the drain is still in progress. marked is w as last written (a drain
// start is persisted before the first drain RPC).
func (d *Driver) decide(ctx context.Context, key Key, v view, w workload.Workload) (workload.Decision, bool, workload.Workload, error) {
	epoch, names := routeEpoch(v.live, w)
	if !names {
		epoch = 0
	}
	switch {
	case w.Terminating:
		return workload.Forced(sessionstore.PlacementForcedPlatformDeleted, epoch), true, w, nil
	case w.Terminal:
		return workload.Forced(sessionstore.PlacementForcedWorkloadTerminated, epoch), true, w, nil
	case !names && w.Drain == nil:
		// No live route names this workload and no drain was begun: its Host
		// holds nothing a drain could release, and a released Host refuses a
		// drain from a tenant that holds nothing (R-1).
		return workload.Forced(sessionstore.PlacementForcedDrainRefused, 0), true, w, nil
	}

	now := d.cfg.Clock.Now()
	req := drainRequest(key, w)
	var observation sessionwire.HostLinkDrainObservation
	var err error
	if names {
		if w.Drain == nil {
			// The drain's start and epoch are persisted BEFORE the first RPC,
			// so its deadline survives a restart or a replica failover.
			w, err = d.cfg.Workloads.MarkDrain(ctx, key.TenantID, key.SessionID, w, workload.Drain{Epoch: epoch, StartedAt: now})
			if err != nil {
				return workload.Decision{}, false, w, fmt.Errorf("driver: persist drain start: %w", err)
			}
		}
		// Begin -- or re-report, since the Host's drain is idempotent -- for
		// as long as the Host holds the session.
		observation, err = d.cfg.Drainer.StartDrain(ctx, w.Endpoint, req)
	} else {
		// The route is gone but a drain was begun: the Host released, and
		// only the caller that began its drain may still observe it.
		epoch = w.Drain.Epoch
		observation, err = d.cfg.Drainer.DrainStatus(ctx, w.Endpoint, req)
	}
	if err == nil && (observation.HostID != req.HostID || observation.HostGeneration != req.HostGeneration ||
		observation.TenantID != req.TenantID || observation.SessionID != req.SessionID) {
		// An observation about any other Host incarnation or scope is not
		// about this workload, whatever state it reports.
		err = hostlink.ErrForeignObservation
	}
	pastDeadline := !now.Before(w.Drain.StartedAt.Add(d.cfg.DrainTimeout))

	switch {
	case err == nil && observation.State == sessionwire.HostLinkDrainStateDrained:
		return workload.Graceful(epoch), true, w, nil
	case errors.Is(err, hostlink.ErrNotAdvertised):
		return workload.Forced(sessionstore.PlacementForcedDrainRefused, epoch), true, w, nil
	case err == nil:
		if pastDeadline {
			return workload.Forced(sessionstore.PlacementForcedDrainTimeout, epoch), true, w, nil
		}
		return workload.Decision{}, false, w, nil
	}
	// A refusal, no answer, or an answer that is not about this workload. A
	// refusal is ambiguous by construction, so it is never a failure and
	// never a completion: re-observe the registry.
	d.cfg.Logger.Info("controller drain not acknowledged; re-observing the registry",
		"tenant", key.TenantID, "session", key.SessionID, "workload", w.Name, "err", err)
	live, rerr := d.liveRegistration(ctx, key)
	if rerr != nil {
		return workload.Decision{}, false, w, rerr
	}
	if _, stillNames := routeEpoch(live, w); !stillNames || pastDeadline {
		return workload.Forced(sessionstore.PlacementForcedDrainRefused, epoch), true, w, nil
	}
	return workload.Decision{}, false, w, nil
}
