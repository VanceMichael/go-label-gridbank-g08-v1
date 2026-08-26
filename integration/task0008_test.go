package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestConcurrentLeaseReleaseAndRecoveryIsConsistent(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Recovery Ownership Provider", "UTC", "ownership-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Recovery Ownership Pool", domain.CapabilityGPU, "ownership-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Recovery Ownership Offer", "edge", domain.CapabilityGPU, "ownership-offer")
	if err != nil {
		t.Fatal(err)
	}
	first, err := environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{
		ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID,
		ReservationRef: "ownership-first", IdempotencyKey: "ownership-first", RequestID: "ownership-first",
	})
	if err != nil {
		t.Fatal(err)
	}
	environment.clock.Advance(3 * time.Minute)
	if _, err := environment.recovery.RecoverExpired(ctx, environment.admin.TenantID, environment.admin.UserID, "ownership-recovery"); err != nil {
		t.Fatal(err)
	}
	second, err := environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{
		ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID,
		ReservationRef: "ownership-second", IdempotencyKey: "ownership-second", RequestID: "ownership-second",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = environment.providers.ReleasePool(ctx, environment.operator, pool.ID, first.Lease.ID, first.Lease.Token, first.Lease.Version, "ownership-stale-release")
	if !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale release error = %v, want lease lost", err)
	}
	var active int
	if err := environment.database.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE id = ? AND released_at IS NULL`, second.Lease.ID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("stale worker changed replacement lease active count to %d", active)
	}
}
