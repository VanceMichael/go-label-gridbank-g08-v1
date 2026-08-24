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

func TestCanceledHeartbeatDoesNotExtendLease(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Canceled Heartbeat Provider", "UTC", "canceled-heartbeat-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Canceled Heartbeat Pool", domain.CapabilityGPU, "canceled-heartbeat-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Canceled Heartbeat Offer", "edge", domain.CapabilityGPU, "canceled-heartbeat-offer")
	if err != nil {
		t.Fatal(err)
	}
	planned, err := environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: "canceled-heartbeat", IdempotencyKey: "canceled-heartbeat", RequestID: "canceled-heartbeat"})
	if err != nil {
		t.Fatal(err)
	}
	environment.clock.Advance(30 * time.Second)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = environment.providers.RenewPool(canceled, environment.operator, planned.Lease.ID, pool.ID, planned.Lease.Token, planned.Lease.Version, "heartbeat-canceled")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled heartbeat error = %v, want context canceled", err)
	}
	var expiry string
	if err := environment.database.SQL().QueryRowContext(ctx, `SELECT expires_at FROM leases WHERE id = ?`, planned.Lease.ID).Scan(&expiry); err != nil {
		t.Fatal(err)
	}
	if expiry != storage.FormatTime(planned.Lease.ExpiresAt) {
		t.Fatalf("canceled heartbeat changed expiry from %s to %s", storage.FormatTime(planned.Lease.ExpiresAt), expiry)
	}
}
