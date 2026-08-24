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

func TestCreateProviderAuditFailureRollsBack(t *testing.T) {
	database, err := storage.Open(context.Background(), storage.Options{Path: filepath.Join(t.TempDir(), "provider-rollback.db"), MaxOpenConns: 4, BusyTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	clockValue := clock.NewManual(time.Date(2026, time.August, 24, 2, 0, 0, 0, time.UTC))
	authService := auth.NewService(database, clockValue, time.Hour)
	tenant, user, err := authService.Bootstrap(context.Background(), auth.BootstrapInput{TenantName: "Provider Rollback Tenant", Email: "admin@provider-rollback.test", DisplayName: "Provider Admin", Password: "test-password"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.SQL().ExecContext(context.Background(), `
		CREATE TRIGGER fail_provider_audit
		BEFORE INSERT ON audit_events
		WHEN NEW.action = 'provider.create'
		BEGIN SELECT RAISE(ABORT, 'forced provider audit failure'); END`)
	if err != nil {
		t.Fatal(err)
	}

	service := NewService(database, clockValue, time.Minute)
	_, err = service.CreateProvider(context.Background(), auth.Principal{TenantID: tenant.ID, UserID: user.ID, Role: domain.RoleTenantAdmin}, "North Compute", "Asia/Shanghai", "provider-request")
	if err == nil {
		t.Fatal("provider creation unexpectedly succeeded")
	}
	var providers int
	if err := database.SQL().QueryRowContext(context.Background(), "SELECT COUNT(*) FROM providers WHERE tenant_id = ?", tenant.ID).Scan(&providers); err != nil {
		t.Fatal(err)
	}
	if providers != 0 {
		t.Fatalf("failed provider creation persisted %d provider(s)", providers)
	}
}
