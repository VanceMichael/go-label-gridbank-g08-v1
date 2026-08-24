package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestReplayDoesNotResurrectReleasedLease(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Stale Replay Provider", "UTC", "stale-replay-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Stale Replay Pool", domain.CapabilityGPU, "stale-replay-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Stale Replay Offer", "edge", domain.CapabilityGPU, "stale-replay-offer")
	if err != nil {
		t.Fatal(err)
	}
	input := workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: "stale-replay", IdempotencyKey: "stale-replay-key", RequestID: "stale-replay-first"}
	first, err := environment.workloads.Plan(ctx, environment.operator, input)
	if err != nil {
		t.Fatal(err)
	}
	if err := environment.providers.ReleasePool(ctx, environment.operator, pool.ID, first.Lease.ID, first.Lease.Token, first.Lease.Version, "stale-replay-release"); err != nil {
		t.Fatal(err)
	}
	_, err = environment.workloads.Plan(ctx, environment.operator, input)
	if err == nil || (!errors.Is(err, domain.ErrConflict) && !errors.Is(err, domain.ErrPrecondition)) {
		t.Fatalf("stale replay error = %v, want conflict or precondition", err)
	}
	var active int
	if err := environment.database.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE id = ? AND released_at IS NULL`, first.Lease.ID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatalf("stale replay resurrected %d released leases", active)
	}
}
