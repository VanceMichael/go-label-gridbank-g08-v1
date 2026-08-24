package integration_test

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestCapacityOfferLookupEnforcesTenantBoundary(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Tenant A Compute", "Asia/Shanghai", "tenant-a-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Tenant A Pool", domain.CapabilityGPU|domain.CapabilityCPU, "tenant-a-pool")
	if err != nil {
		t.Fatal(err)
	}
	tenantB, adminB, err := environment.auth.Bootstrap(ctx, auth.BootstrapInput{TenantName: "Tenant B", Email: "admin@tenant-b.test", DisplayName: "Tenant B Admin", Password: "test-password"})
	if err != nil {
		t.Fatal(err)
	}
	foreignOffer, err := environment.providers.CreateCapacityOffer(ctx, auth.Principal{TenantID: tenantB.ID, UserID: adminB.ID, Role: adminB.Role}, "Foreign Offer", "retail", domain.CapabilityGPU, "tenant-b-offer")
	if err != nil {
		t.Fatal(err)
	}

	_, err = environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: foreignOffer.ID, PoolID: pool.ID, ReservationRef: "tenant-boundary", IdempotencyKey: "tenant-boundary", RequestID: "tenant-boundary"})
	if err == nil {
		t.Fatal("cross-tenant capacity offer was accepted")
	}
	var workloads int
	if err := environment.database.SQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM workloads WHERE tenant_id = ?", environment.admin.TenantID).Scan(&workloads); err != nil {
		t.Fatal(err)
	}
	if workloads != 0 {
		t.Fatalf("cross-tenant plan persisted %d workload(s)", workloads)
	}
}
