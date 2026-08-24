package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestExpiredLeaseRecoveryRequeuesWorkload(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Recovery Requeue Provider", "UTC", "requeue-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Recovery Requeue Pool", domain.CapabilityGPU, "requeue-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Recovery Requeue Offer", "edge", domain.CapabilityGPU, "requeue-offer")
	if err != nil {
		t.Fatal(err)
	}
	planned, err := environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: "requeue-workload", IdempotencyKey: "requeue-workload", RequestID: "requeue-workload"})
	if err != nil {
		t.Fatal(err)
	}
	allocated, err := environment.workloads.MarkReady(ctx, environment.operator, planned.Workload.ID, "requeue-ready")
	if err != nil {
		t.Fatal(err)
	}
	environment.clock.Advance(3 * time.Minute)
	if _, err := environment.recovery.RecoverExpired(ctx, environment.admin.TenantID, environment.admin.UserID, "requeue-recovery"); err != nil {
		t.Fatal(err)
	}
	current, err := environment.workloads.Get(ctx, environment.operator, planned.Workload.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != domain.WorkloadQueued {
		t.Fatalf("recovered workload status = %s, want queued from %s", current.Status, allocated.Status)
	}
	var active int
	if err := environment.database.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE id = ? AND released_at IS NULL`, planned.Lease.ID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatalf("recovery left %d active expired leases", active)
	}
}
