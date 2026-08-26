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

func TestCreditReserveAuditFailureRollsBackHold(t *testing.T) {
	ctx := context.Background()
	db, err := storage.Open(ctx, storage.Options{Path: filepath.Join(t.TempDir(), "reserve-audit.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	clockValue := clock.NewManual(time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC))
	authService := auth.NewService(db, clockValue, time.Hour)
	tenant, adminUser, err := authService.Bootstrap(ctx, auth.BootstrapInput{
		TenantName: "Reserve Audit Cooperative",
		Email: "reserve-admin@gridbank.test",
		DisplayName: "Reserve Admin",
		Password: "reserve-password",
	})
	if err != nil {
		t.Fatal(err)
	}
	admin := auth.Principal{TenantID: tenant.ID, UserID: adminUser.ID, Role: adminUser.Role}
	service := NewService(db, clockValue)
	account, err := service.OpenCreditAccount(ctx, admin, "GPU", "reserve-account")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Deposit(ctx, admin, account.ID, 100, "reserve-deposit", "reserve-deposit-request"); err != nil {
		t.Fatal(err)
	}
	operator := admin
	operator.Role = domain.RoleOperator
	providers := provider.NewService(db, clockValue, 2*time.Minute)
	providerValue, err := providers.CreateProvider(ctx, admin, "Reserve Compute Provider", "Asia/Shanghai", "reserve-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := providers.CreatePool(ctx, admin, providerValue.ID, "Reserve GPU Pool", domain.CapabilityGPU, "reserve-pool")
	if err != nil {
		t.Fatal(err)
	}
	offer, err := providers.CreateCapacityOffer(ctx, admin, "Reserve GPU Offer", "edge", domain.CapabilityGPU, "reserve-offer")
	if err != nil {
		t.Fatal(err)
	}
	workloads := workload.NewService(db, clockValue, 2*time.Minute)
	planned, err := workloads.Plan(ctx, operator, workload.PlanInput{
		ProviderID: providerValue.ID, CapacityOfferID: offer.ID, PoolID: pool.ID,
		ReservationRef: "reserve-workload", IdempotencyKey: "reserve-workload-key", RequestID: "reserve-workload-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
		CREATE TRIGGER fail_credit_reserve_audit
		BEFORE INSERT ON audit_events
		WHEN NEW.action = 'credit.reserve'
		BEGIN SELECT RAISE(ABORT, 'forced reserve audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Reserve(ctx, operator, account.ID, planned.Workload.ID, 40, "reserve-a", "reserve-request-a"); err == nil {
		t.Fatal("reserve succeeded while its audit write was rejected")
	}
	current, err := service.Account(ctx, admin, account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Balance != 100 || current.Held != 0 {
		t.Fatalf("failed reserve changed account balance/held to %d/%d", current.Balance, current.Held)
	}
	var entries int
	if err := db.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM credit_entries WHERE account_id = ? AND idempotency_key = ?`, account.ID, "reserve-a").Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if entries != 0 {
		t.Fatalf("failed reserve left %d credit entries", entries)
	}
	if _, err := db.SQL().ExecContext(ctx, `DROP TRIGGER fail_credit_reserve_audit`); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Reserve(ctx, operator, account.ID, planned.Workload.ID, 40, "reserve-a", "reserve-request-a-retry"); err != nil {
		t.Fatalf("reserve did not recover after audit became writable: %v", err)
	}
	current, err = service.Account(ctx, admin, account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Balance != 100 || current.Held != 40 {
		t.Fatalf("successful reserve account balance/held = %d/%d", current.Balance, current.Held)
	}
	var auditCount int
	if err := db.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_events WHERE action = 'credit.reserve' AND object_id = ?`, planned.Workload.ID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 {
		t.Fatalf("successful retry wrote %d reserve audit events", auditCount)
	}
}
