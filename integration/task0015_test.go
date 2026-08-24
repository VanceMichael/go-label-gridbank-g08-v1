package integration_test

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestWorkloadSubmissionReplayHasOneLeaseAndOneEvent(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Replay Event Provider", "UTC", "replay-event-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Replay Event Pool", domain.CapabilityGPU, "replay-event-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Replay Event Offer", "edge", domain.CapabilityGPU, "replay-event-offer")
	if err != nil {
		t.Fatal(err)
	}
	input := workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: "replay-event", IdempotencyKey: "replay-event-key", RequestID: "replay-event-first"}
	first, err := environment.workloads.Plan(ctx, environment.operator, input)
	if err != nil {
		t.Fatal(err)
	}
	input.RequestID = "replay-event-second"
	second, err := environment.workloads.Plan(ctx, environment.operator, input)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replay || first.Workload.ID != second.Workload.ID || first.Lease.ID != second.Lease.ID {
		t.Fatalf("replay changed durable result: first=%+v second=%+v", first, second)
	}
	var events int
	if err := environment.database.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_events WHERE topic = 'workload.plan' AND aggregate_id = ?`, first.Workload.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("idempotent replay wrote %d workload plan events, want one", events)
	}
}
