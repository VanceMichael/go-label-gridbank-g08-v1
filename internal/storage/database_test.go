package storage

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openTestDatabase(t *testing.T) (*Database, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gridbank.db")
	database, err := Open(context.Background(), Options{Path: path, MaxOpenConns: 2, BusyTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})
	return database, path
}

func TestMigrationsCreateRelationalSchema(t *testing.T) {
	database, _ := openTestDatabase(t)
	want := []string{
		"tenants", "users", "auth_sessions", "providers", "compute_pools",
		"capacity_offers", "workloads", "leases", "capacity_streams",
		"usage_records", "usage_batches", "usage_items",
		"quality_reviews", "credit_plans", "credit_plan_items", "credit_releases",
		"credit_release_items", "compute_jobs", "compute_attempts", "outbox_events",
		"idempotency_records", "audit_events", "schema_migrations",
	}
	for _, table := range want {
		var count int
		if err := database.SQL().QueryRowContext(context.Background(), `
			SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&count); err != nil {
			t.Fatalf("look up table %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("table %s count = %d, want 1", table, count)
		}
	}
	var versionCount int
	if err := database.SQL().QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&versionCount); err != nil {
		t.Fatal(err)
	}
	if versionCount != 1 {
		t.Fatalf("migration count = %d, want 1", versionCount)
	}
}

func TestMigrationsAreIdempotent(t *testing.T) {
	database, _ := openTestDatabase(t)
	if err := database.Migrate(context.Background()); err != nil {
		t.Fatalf("second migration run failed: %v", err)
	}
	if err := database.Migrate(context.Background()); err != nil {
		t.Fatalf("third migration run failed: %v", err)
	}
	var count int
	if err := database.SQL().QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE version = 1`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("migration version duplicated: %d", count)
	}
}

func TestCommittedStateSurvivesRestart(t *testing.T) {
	database, path := openTestDatabase(t)
	now := FormatTime(time.Date(2026, 8, 24, 1, 2, 3, 0, time.UTC))
	err := database.Write(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO tenants(id, name, active, created_at, updated_at, version) VALUES('tenant-restart', 'Restart', 1, ?, ?, 1)`, now, now)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), Options{Path: path, MaxOpenConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var name string
	if err := reopened.SQL().QueryRow(`SELECT name FROM tenants WHERE id = 'tenant-restart'`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Restart" {
		t.Fatalf("persisted tenant name = %q", name)
	}
}

func TestWriteRollsBackCallbackFailure(t *testing.T) {
	database, _ := openTestDatabase(t)
	wantErr := errors.New("forced audit failure")
	err := database.Write(context.Background(), func(tx *sql.Tx) error {
		now := FormatTime(time.Now())
		if _, err := tx.Exec(`INSERT INTO tenants(id, name, active, created_at, updated_at, version) VALUES('tenant-rollback', 'Rollback', 1, ?, ?, 1)`, now, now); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Write error = %v, want %v", err, wantErr)
	}
	var count int
	if err := database.SQL().QueryRow(`SELECT COUNT(*) FROM tenants WHERE id = 'tenant-rollback'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("rollback left %d tenants", count)
	}
}

func TestReadRollbackReleasesSingleConnection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "one.db")
	database, err := Open(context.Background(), Options{Path: path, MaxOpenConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	wantErr := errors.New("read failed")
	if err := database.Read(context.Background(), func(tx *sql.Tx) error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("Read error = %v, want %v", err, wantErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := database.Ping(ctx); err != nil {
		t.Fatalf("connection remained held after read failure: %v", err)
	}
}

func TestCanceledContextPreventsTransactions(t *testing.T) {
	database, _ := openTestDatabase(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := database.Write(ctx, func(*sql.Tx) error {
		called = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Write error = %v, want context canceled", err)
	}
	if called {
		t.Fatal("write callback ran after cancellation")
	}
	err = database.Read(ctx, func(*sql.Tx) error {
		called = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Read error = %v, want context canceled", err)
	}
}

func TestForeignKeysAreEnforced(t *testing.T) {
	database, _ := openTestDatabase(t)
	now := FormatTime(time.Now())
	_, err := database.SQL().Exec(`
		INSERT INTO users(id, tenant_id, email, display_name, password_hash, role, active, created_at, updated_at, version)
		VALUES('user-orphan', 'missing-tenant', 'orphan@example.test', 'Orphan', 'hash', 'operator', 1, ?, ?, 1)`, now, now)
	if err == nil {
		t.Fatal("foreign-key violation was accepted")
	}
}

func TestTimeEncodingRoundTrip(t *testing.T) {
	want := time.Date(2026, time.August, 24, 7, 6, 5, 432100000, time.FixedZone("offset", 8*60*60))
	encoded := FormatTime(want)
	got, err := ParseTime(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("round trip = %v (%v), want instant %v in UTC", got, got.Location(), want)
	}
	if NullableTime(nil) != nil {
		t.Fatal("nil time should remain SQL NULL")
	}
	value := want
	if NullableTime(&value) != encoded {
		t.Fatalf("nullable time = %v, want %s", NullableTime(&value), encoded)
	}
}
