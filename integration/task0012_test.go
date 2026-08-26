package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestStalePoolHeartbeatCannotRenewReplacementOwner(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Heartbeat Ownership Provider", "UTC", "heartbeat-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Heartbeat Ownership Pool", domain.CapabilityGPU, "heartbeat-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Heartbeat Ownership Offer", "edge", domain.CapabilityGPU, "heartbeat-offer")
	if err != nil {
		t.Fatal(err)
	}
	first, err := environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: "heartbeat-first", IdempotencyKey: "heartbeat-first", RequestID: "heartbeat-first"})
	if err != nil {
		t.Fatal(err)
	}
	environment.clock.Advance(3 * time.Minute)
	if _, err := environment.recovery.RecoverExpired(ctx, environment.admin.TenantID, environment.admin.UserID, "heartbeat-recovery"); err != nil {
		t.Fatal(err)
	}
	second, err := environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: "heartbeat-second", IdempotencyKey: "heartbeat-second", RequestID: "heartbeat-second"})
	if err != nil {
		t.Fatal(err)
	}
	originalExpiry := second.Lease.ExpiresAt
	environment.clock.Advance(30 * time.Second)
	_, err = environment.providers.RenewPool(ctx, environment.operator, first.Lease.ID, pool.ID, first.Lease.Token, first.Lease.Version, "heartbeat-stale")
	if !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale heartbeat error = %v, want lease lost", err)
	}
	var expiry string
	if err := environment.database.SQL().QueryRowContext(ctx, `SELECT expires_at FROM leases WHERE id = ?`, second.Lease.ID).Scan(&expiry); err != nil {
		t.Fatal(err)
	}
	if expiry != storage.FormatTime(originalExpiry) {
		t.Fatalf("stale heartbeat changed replacement expiry from %s to %s", storage.FormatTime(originalExpiry), expiry)
	}
}
