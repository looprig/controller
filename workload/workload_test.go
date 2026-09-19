package workload

import (
	"testing"
	"time"

	"github.com/looprig/sessionstore"
)

func TestDecisionValidateHoldsSessionStoresPairings(t *testing.T) {
	for _, ok := range []Decision{
		Graceful(1),
		Forced(sessionstore.PlacementForcedDrainTimeout, 3),
		Forced(sessionstore.PlacementForcedDrainRefused, 0),
		Forced(sessionstore.PlacementForcedPlatformDeleted, 0),
		Forced(sessionstore.PlacementForcedWorkloadTerminated, 9),
	} {
		if err := ok.Validate(); err != nil {
			t.Fatalf("%+v refused: %v", ok, err)
		}
	}
	for _, bad := range []Decision{
		Graceful(0),
		{Kind: sessionstore.PlacementTerminationGraceful, Reason: sessionstore.PlacementForcedDrainTimeout, Epoch: 1},
		{Kind: sessionstore.PlacementTerminationForced, Epoch: 1},
		{Kind: sessionstore.PlacementTerminationForced, Reason: "evicted"},
		{Kind: "abandoned", Epoch: 1},
	} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("%+v accepted", bad)
		}
	}
	if (Drain{Epoch: 1, StartedAt: time.Unix(1, 0)}).Validate() != nil ||
		(Drain{StartedAt: time.Unix(1, 0)}).Validate() == nil || (Drain{Epoch: 1}).Validate() == nil {
		t.Fatal("drain validation")
	}
}
