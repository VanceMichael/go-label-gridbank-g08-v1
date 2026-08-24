package ledger

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/provider"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

// TestCreditReservationRollsBackWhenAuditUnavailable reproduces the reported
// reservation flow: when the audit store cannot accept the credit.reserve
// event, the API must return failure while leaving both the account balance
// and the credit ledger untouched, so an idempotent retry can still complete.
func TestCreditReservationRollsBackWhenAuditUnavailable(t *testing.T) {
	db, err := storage.Open(context.Background(), storage.Options{Path: filepath.Join(t.TempDir(), "credits.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := clock.NewManual(time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC))
	authService := auth.NewService(db, c, time.Hour)
	tenant, user, err := authService.Bootstrap(context.Background(), auth.BootstrapInput{TenantName: "Compute Cooperative", Email: "admin@gridbank.test", DisplayName: "Admin", Password: "test-password-admin"})
	if err != nil {
		t.Fatal(err)
	}
	admin := auth.Principal{TenantID: tenant.ID, UserID: user.ID, Role: user.Role}
	service := NewService(db, c)
	account, err := service.OpenCreditAccount(context.Background(), admin, "gpu", "open-account")
	if err != nil {
		t.Fatal(err)
	}
	deposited, err := service.Deposit(context.Background(), admin, account.ID, 100, "deposit-1", "deposit-request")
	if err != nil {
		t.Fatal(err)
	}
	operator := admin
	operator.Role = domain.RoleOperator
	providers := provider.NewService(db, c, 2*time.Minute)
	providerValue, err := providers.CreateProvider(context.Background(), admin, "North Compute Exchange", "Asia/Shanghai", "provider-request")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := providers.CreatePool(context.Background(), admin, providerValue.ID, "GPU Pool", domain.CapabilityGPU, "pool-request")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := providers.CreateCapacityOffer(context.Background(), admin, "GPU Hour", "edge", domain.CapabilityGPU, "offer-request")
	if err != nil {
		t.Fatal(err)
	}
	workloads := workload.NewService(db, c, 2*time.Minute)
	job, err := workloads.Plan(context.Background(), operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID, ReservationRef: "resource-a", IdempotencyKey: "plan-a", RequestID: "plan-a"})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the audit store being unavailable by dropping the audit_events
	// table so the credit.reserve audit insert fails mid-transaction.
	if _, err := db.SQL().ExecContext(context.Background(), `DROP TABLE audit_events`); err != nil {
		t.Fatalf("drop audit table: %v", err)
	}

	// The reservation must fail because its audit effect cannot be written.
	if _, err := service.Reserve(context.Background(), operator, account.ID, job.Workload.ID, 75, "hold-a", "reserve-a"); err == nil {
		t.Fatal("reserve unexpectedly succeeded while audit store is unavailable")
	}

	// Account and ledger must be untouched: no orphaned hold blocking retries.
	current, err := service.Account(context.Background(), admin, account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Balance != 100 || current.Held != 0 {
		t.Fatalf("account leaked hold when audit failed: balance/held = %d/%d", current.Balance, current.Held)
	}
	if current.Version != deposited.Version {
		t.Fatalf("account version advanced despite failed reservation: %d", current.Version)
	}

	// After the audit store recovers, the same idempotent reservation completes.
	if _, err := db.SQL().ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS audit_events (
			id TEXT PRIMARY KEY,
			tenant_id TEXT NOT NULL REFERENCES tenants(id),
			actor_id TEXT NOT NULL,
			action TEXT NOT NULL,
			object_type TEXT NOT NULL,
			object_id TEXT NOT NULL,
			outcome TEXT NOT NULL,
			request_id TEXT NOT NULL,
			detail TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL
		)`); err != nil {
		t.Fatalf("restore audit table: %v", err)
	}
	c.Advance(time.Second)
	reserved, err := service.Reserve(context.Background(), operator, account.ID, job.Workload.ID, 75, "hold-a", "reserve-a")
	if err != nil {
		t.Fatalf("reserve after audit recovery failed: %v", err)
	}
	if reserved.Held != 75 || reserved.Balance != 100 {
		t.Fatalf("post-recovery account = balance %d / held %d", reserved.Balance, reserved.Held)
	}
	// Idempotent replay of the same key returns the same reservation.
	replayed, err := service.Reserve(context.Background(), operator, account.ID, job.Workload.ID, 75, "hold-a", "reserve-a")
	if err != nil {
		t.Fatalf("idempotent replay failed: %v", err)
	}
	if replayed.Version != reserved.Version {
		t.Fatalf("idempotent replay bumped version: %d vs %d", replayed.Version, reserved.Version)
	}
}
