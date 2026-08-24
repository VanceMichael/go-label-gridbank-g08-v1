package integration_test

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestCancelWorkloadReleasesPoolLeaseAtomically(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Cancel Atomic Provider", "UTC", "cancel-atomic-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Cancel Atomic Pool", domain.CapabilityGPU, "cancel-atomic-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Cancel Atomic Offer", "edge", domain.CapabilityGPU, "cancel-atomic-offer")
	if err != nil {
		t.Fatal(err)
	}
	planned, err := environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{
		ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID,
		ReservationRef: "cancel-atomic-workload", IdempotencyKey: "cancel-atomic-key", RequestID: "cancel-atomic-plan",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.database.SQL().ExecContext(ctx, `
		CREATE TRIGGER fail_cancel_audit
		BEFORE INSERT ON audit_events
		WHEN NEW.action = 'workload.cancel'
		BEGIN SELECT RAISE(ABORT, 'forced cancel audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.workloads.Cancel(ctx, environment.operator, planned.Workload.ID, "cancel-atomic-first"); err == nil {
		t.Fatal("cancel succeeded while its audit write was rejected")
	}
	var activeLeases int
	if err := environment.database.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE id = ? AND released_at IS NULL`, planned.Lease.ID).Scan(&activeLeases); err != nil {
		t.Fatal(err)
	}
	if activeLeases != 1 {
		t.Fatalf("failed cancel changed active lease count to %d", activeLeases)
	}
	current, err := environment.workloads.Get(ctx, environment.operator, planned.Workload.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != domain.WorkloadQueued {
		t.Fatalf("failed cancel changed workload status to %s", current.Status)
	}
	if _, err := environment.database.SQL().ExecContext(ctx, `DROP TRIGGER fail_cancel_audit`); err != nil {
		t.Fatal(err)
	}
	canceled, err := environment.workloads.Cancel(ctx, environment.operator, planned.Workload.ID, "cancel-atomic-retry")
	if err != nil {
		t.Fatal(err)
	}
	if canceled.Status != domain.WorkloadCanceled {
		t.Fatalf("successful retry status = %s", canceled.Status)
	}
	if err := environment.database.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE id = ? AND released_at IS NULL`, planned.Lease.ID).Scan(&activeLeases); err != nil {
		t.Fatal(err)
	}
	if activeLeases != 0 {
		t.Fatalf("successful cancel left %d active leases", activeLeases)
	}
}
