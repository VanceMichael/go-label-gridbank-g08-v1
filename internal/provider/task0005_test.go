package provider

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

func TestPoolCreationAuditFailureRollsBack(t *testing.T) {
	database, err := storage.Open(context.Background(), storage.Options{Path: filepath.Join(t.TempDir(), "pool-rollback.db"), MaxOpenConns: 4, BusyTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	clockValue := clock.NewManual(time.Date(2026, time.August, 24, 2, 0, 0, 0, time.UTC))
	authService := auth.NewService(database, clockValue, time.Hour)
	tenant, user, err := authService.Bootstrap(context.Background(), auth.BootstrapInput{TenantName: "Pool Rollback Tenant", Email: "admin@pool-rollback.test", DisplayName: "Pool Admin", Password: "test-password"})
	if err != nil {
		t.Fatal(err)
	}
	principal := auth.Principal{TenantID: tenant.ID, UserID: user.ID, Role: domain.RoleTenantAdmin}
	service := NewService(database, clockValue, time.Minute)
	providerValue, err := service.CreateProvider(context.Background(), principal, "North Compute", "Asia/Shanghai", "provider-request")
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.SQL().ExecContext(context.Background(), `
		CREATE TRIGGER fail_pool_audit
		BEFORE INSERT ON audit_events
		WHEN NEW.action = 'pool.create'
		BEGIN SELECT RAISE(ABORT, 'forced pool audit failure'); END`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = service.CreatePool(context.Background(), principal, providerValue.ID, "Pool Alpha", domain.CapabilityGPU|domain.CapabilityCPU, "pool-request")
	if err == nil {
		t.Fatal("pool creation unexpectedly succeeded")
	}
	var pools int
	if err := database.SQL().QueryRowContext(context.Background(), "SELECT COUNT(*) FROM compute_pools WHERE tenant_id = ?", tenant.ID).Scan(&pools); err != nil {
		t.Fatal(err)
	}
	if pools != 0 {
		t.Fatalf("failed pool creation persisted %d pool(s)", pools)
	}
}
