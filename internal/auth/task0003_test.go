package auth

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

func TestLogoutAuditFailureRollsBackRevocation(t *testing.T) {
	database, err := storage.Open(context.Background(), storage.Options{
		Path:         filepath.Join(t.TempDir(), "logout-rollback.db"),
		MaxOpenConns: 4,
		BusyTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	service := NewService(database, clock.NewManual(time.Date(2026, time.August, 24, 2, 0, 0, 0, time.UTC)), time.Hour)
	tenant, user, err := service.Bootstrap(context.Background(), BootstrapInput{
		TenantName:  "Logout Rollback Tenant",
		Email:       "admin@logout-rollback.test",
		DisplayName: "Logout Admin",
		Password:    "test-password",
	})
	if err != nil {
		t.Fatal(err)
	}
	login, err := service.Login(context.Background(), LoginInput{
		TenantID: tenant.ID, Email: "admin@logout-rollback.test", Password: "test-password", RequestID: "login",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = database.SQL().ExecContext(context.Background(), `
		CREATE TRIGGER fail_logout_audit
		BEFORE INSERT ON audit_events
		WHEN NEW.action = 'auth.logout'
		BEGIN SELECT RAISE(ABORT, 'forced logout audit failure'); END`)
	if err != nil {
		t.Fatal(err)
	}

	err = service.Logout(context.Background(), Principal{
		TenantID: tenant.ID, UserID: user.ID, Role: domain.RoleTenantAdmin, SessionID: login.SessionID,
	}, "logout")
	if err == nil {
		t.Fatal("logout unexpectedly succeeded")
	}

	var active int
	if err := database.SQL().QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM auth_sessions WHERE id = ? AND revoked_at IS NULL", login.SessionID,
	).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("failed logout revoked %d session(s)", 1-active)
	}
}
