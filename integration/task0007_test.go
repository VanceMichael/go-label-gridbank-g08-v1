package integration_test

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestWorkloadPlanRequiresCompatiblePool(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Compatibility Compute", "Asia/Shanghai", "compat-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "CPU Only Pool", domain.CapabilityCPU, "compat-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "GPU Offer", "retail", domain.CapabilityGPU, "compat-offer")
	if err != nil {
		t.Fatal(err)
	}

	_, err = environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: "compatibility", IdempotencyKey: "compatibility", RequestID: "compatibility"})
	if err == nil {
		t.Fatal("incompatible pool was accepted")
	}
	var workloads, leases int
	if err := environment.database.SQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM workloads WHERE tenant_id = ?", environment.admin.TenantID).Scan(&workloads); err != nil {
		t.Fatal(err)
	}
	if err := environment.database.SQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM leases WHERE tenant_id = ?", environment.admin.TenantID).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if workloads != 0 || leases != 0 {
		t.Fatalf("rejected incompatible plan persisted workloads=%d leases=%d", workloads, leases)
	}
}
