package driver_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"

	"github.com/looprig/controller/driver"
	"github.com/looprig/controller/hostlink"
	"github.com/looprig/controller/kubernetes"
)

// Pins for the D2.2 final regate's can-follow test gaps (N2, N3). Each was a
// reachable branch a mutant survived the committed tests on.

// putPooledRoute registers a POOLED Host holding the session at epoch -- a
// live lease that names no dedicated workload.
func (r *tdRig) putPooledRoute(hostID string, epoch uint64) {
	r.t.Helper()
	if _, err := r.store.PutHostRegistration(context.Background(), sessionstore.PutHostRegistrationRequest{
		TenantID: r.key.TenantID, SessionID: r.key.SessionID, LeaseEpoch: epoch,
		ObservedAt: r.clock.Now(), ExpiresAt: r.clock.Now().Add(time.Hour),
		Route: sessionstore.HostRoute{
			HostID: sessionwire.HostID(hostID), HostGeneration: 1,
			AgentID: "agent-coder", RuntimeCompatibilityID: "runtime-2026-09",
			Placement:        sessionwire.HostPlacementPooled,
			InternalEndpoint: "ws://" + sessionwire.InternalEndpoint(hostID) + ".looprig-hosts.svc:7443",
			Residency:        sessionwire.SessionResidencyResident, Accepting: true,
		},
	}); err != nil {
		r.t.Fatalf("put pooled registration: %v", err)
	}
}

// N2 (mutant G1g): between the look and the fence, a POOLED Host takes the
// session under a later live lease. The fence is refused (epoch), but the live
// route does not name THIS workload, so the decision stands: the Pod -- which
// holds nothing -- is deleted, and the Kind recorded is the one decided
// (graceful at the drained epoch), not a re-decision after the lapse.
func TestAFenceRefusedByALaterLeaseElsewhereKeepsTheDecision(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDrained)
	r.store.beforeClear = func(sessionstore.ClearHostRegistrationRequest) error {
		r.store.beforeClear = nil
		r.putPooledRoute("pooled-host-1", tdEpoch+4)
		return nil
	}
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	if got := r.deletes(); len(got) != 1 {
		t.Fatalf("deletes = %v, want exactly one: a fence refused by a lease elsewhere does not stop it", got)
	}
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
}

// raceRelease makes the next hostlink.drain release the Host's lease (the Host
// finished and released between the controller's look and its RPC) and then
// fail with fail; drain_status afterwards reports drained.
func raceRelease(r *tdRig, fail func(method string) error) {
	released := false
	r.drainer.answer = func(method string, req sessionwire.HostLinkDrainRequest) (sessionwire.HostLinkDrainObservation, error) {
		if method == sessionwire.HostLinkMethodDrain {
			if !released {
				released = true
				r.hostReleases(tdEpoch)
			}
			return sessionwire.HostLinkDrainObservation{}, fail(method)
		}
		return answering(sessionwire.HostLinkDrainStateDrained)(method, req)
	}
}

// N3 (mutant G3b): a LOST reply -- no answer at all, not a refusal -- racing
// the release is deferred to drain_status exactly as a refusal is. Nothing is
// decided, persisted or deleted on it, and the next pass asks drain_status.
func TestALostDrainReplyRacingTheReleaseIsObserved(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDraining)
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	raceRelease(r, func(string) error { return fmt.Errorf("%w: connection reset", hostlink.ErrRPCFailed) })
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	if len(r.store.recorded) != 0 || len(r.deletes()) != 0 {
		t.Fatalf("decided on a lost reply racing a release: recorded %v, deleted %v", r.store.recorded, r.deletes())
	}
	if p := r.podOrNil(name); p == nil || p.Annotations[kubernetes.AnnotationTermination] != "" {
		t.Fatalf("a decision was persisted on a lost reply: %+v", p)
	}
	wantOutcome(t, r.pass("replica-c"), driver.OutcomeTearingDown)
	if got := r.drainer.methods(); len(got) == 0 || got[len(got)-1] != sessionwire.HostLinkMethodDrainStatus {
		t.Fatalf("the next pass did not ask drain_status: %v", got)
	}
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
}

// N3 (mutant G3c): PAST the drain deadline, a refusal racing the release still
// defers to drain_status -- "the route has just gone" takes precedence over
// "at the drain timeout" (README table, N4) -- because the drain may have
// completed; only drain_status can tell.
func TestPastTheDeadlineARefusalRacingTheReleaseStillAsksDrainStatus(t *testing.T) {
	r := newTDRig(t)
	name, intent := r.running()
	r.register(name, intent.Generation, tdEpoch)
	r.desire("delete-1", nil)
	r.drainer.answer = answering(sessionwire.HostLinkDrainStateDraining)
	wantOutcome(t, r.pass("replica-a"), driver.OutcomeTearingDown)
	r.clock.advance(tdDrainTimeout + time.Second)
	raceRelease(r, func(method string) error {
		return &hostlink.RefusalError{Method: method, Refusal: sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorRuntimeUnavailable}}
	})
	wantOutcome(t, r.pass("replica-b"), driver.OutcomeTearingDown)
	if len(r.store.recorded) != 0 || len(r.deletes()) != 0 {
		t.Fatalf("forced past the deadline on a refusal racing the release: recorded %v, deleted %v", r.store.recorded, r.deletes())
	}
	wantOutcome(t, r.pass("replica-c"), driver.OutcomeTearingDown)
	r.wantTermination(intent.Generation, sessionstore.PlacementTerminationGraceful, "", tdEpoch)
}
