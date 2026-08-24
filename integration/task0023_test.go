package integration_test

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestWorkloadSubmissionReplayHasOneLease(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Task 0023 Provider", "UTC", "task0023-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Task 0023 Pool", domain.CapabilityGPU, "task0023-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Task 0023 Offer", "replay", domain.CapabilityGPU, "task0023-offer")
	if err != nil {
		t.Fatal(err)
	}
	input := workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: "task0023-reservation", IdempotencyKey: "task0023-same-key", RequestID: "task0023-first"}
	first, err := environment.workloads.Plan(ctx, environment.operator, input)
	if err != nil {
		t.Fatal(err)
	}
	input.RequestID = "task0023-replay"
	second, err := environment.workloads.Plan(ctx, environment.operator, input)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replay || first.Workload.ID != second.Workload.ID || first.Lease.ID != second.Lease.ID {
		t.Fatalf("replay changed durable result: first=%+v second=%+v", first, second)
	}
	var workloads, leases, events, audits int
	if err := environment.database.SQL().QueryRow(`SELECT COUNT(*) FROM workloads`).Scan(&workloads); err != nil {
		t.Fatal(err)
	}
	if err := environment.database.SQL().QueryRow(`SELECT COUNT(*) FROM leases`).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if err := environment.database.SQL().QueryRow(`SELECT COUNT(*) FROM outbox_events WHERE topic = 'workload.plan'`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := environment.database.SQL().QueryRow(`SELECT COUNT(*) FROM audit_events WHERE action = 'workload.plan'`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if workloads != 1 || leases != 1 || events != 1 || audits != 1 {
		t.Fatalf("replay duplicated durable effects: workloads=%d leases=%d events=%d audits=%d", workloads, leases, events, audits)
	}
}
