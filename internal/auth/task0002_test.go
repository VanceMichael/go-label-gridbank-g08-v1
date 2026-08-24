package auth

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

func TestLoginCancellationDoesNotPersistSession(t *testing.T) {
	database, err := storage.Open(context.Background(), storage.Options{
		Path:         filepath.Join(t.TempDir(), "login-cancel.db"),
		MaxOpenConns: 4,
		BusyTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	service := NewService(database, clock.NewManual(time.Date(2026, time.August, 24, 2, 0, 0, 0, time.UTC)), time.Hour)
	tenant, _, err := service.Bootstrap(context.Background(), BootstrapInput{
		TenantName:  "Canceled Login Tenant",
		Email:       "admin@login-cancel.test",
		DisplayName: "Login Admin",
		Password:    "test-password",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Login(ctx, LoginInput{
		TenantID:  tenant.ID,
		Email:     "admin@login-cancel.test",
		Password:  "test-password",
		RequestID: "cancel-login",
	}); err == nil {
		t.Fatal("canceled login unexpectedly succeeded")
	}

	var sessions int
	if err := database.SQL().QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM auth_sessions WHERE tenant_id = ?", tenant.ID,
	).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if sessions != 0 {
		t.Fatalf("canceled login persisted %d session(s)", sessions)
	}
}
