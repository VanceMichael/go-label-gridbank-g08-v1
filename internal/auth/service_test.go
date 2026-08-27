package auth

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/audit"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

// failingAuditer simulates an audit backend that is temporarily unwritable,
// as described in the report: the append fails while the rest of the database
// is fine, so a naive implementation would have already committed the tenant.
type failingAuditer struct{ err error }

func (f failingAuditer) Append(context.Context, storage.Queryer, audit.Record) error {
	return f.err
}

func newAuthTestService(t *testing.T, audits audit.Auditer) (*Service, *storage.Database) {
	t.Helper()
	database, err := storage.Open(context.Background(), storage.Options{Path: filepath.Join(t.TempDir(), "auth.db"), MaxOpenConns: 2, BusyTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})
	c := clock.NewManual(time.Date(2026, time.August, 24, 1, 0, 0, 0, time.UTC))
	return NewServiceWithAuditer(database, c, time.Hour, audits), database
}

func bootstrapInput(name string) BootstrapInput {
	return BootstrapInput{TenantName: name, Email: slug(name) + "@motion.test", DisplayName: "Admin", Password: "test-password-admin"}
}

func slug(name string) string {
	out := make([]byte, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out = append(out, byte(r))
		case r >= 'A' && r <= 'Z':
			out = append(out, byte(r+('a'-'A')))
		default:
			if len(out) > 0 && out[len(out)-1] != '-' {
				out = append(out, '-')
			}
		}
	}
	for len(out) > 0 && out[len(out)-1] == '-' {
		out = out[:len(out)-1]
	}
	if len(out) == 0 {
		return "tenant"
	}
	return string(out)
}

func tenantCount(t *testing.T, database *storage.Database, name string) int {
	t.Helper()
	var count int
	if err := database.SQL().QueryRow(`SELECT COUNT(*) FROM tenants WHERE name = ?`, name).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// TestBootstrapRollsBackOnAuditFailure reproduces the reported regression: when
// the audit backend is temporarily unwritable the first registration reports a
// failure but would leave a tenant and admin behind, so a retry collides with
// the residue. With tenant/user and audit coupled in one transaction, the
// failed attempt leaves no residue.
func TestBootstrapRollsBackOnAuditFailure(t *testing.T) {
	auditErr := errors.New("audit backend temporarily unwritable")
	service, database := newAuthTestService(t, failingAuditer{err: auditErr})

	if _, _, err := service.Bootstrap(context.Background(), bootstrapInput("Compute Cooperative")); err == nil {
		t.Fatal("bootstrap with failing audit should report an error")
	}
	if got := tenantCount(t, database, "Compute Cooperative"); got != 0 {
		t.Fatalf("failed bootstrap left %d tenant rows behind, want 0 for safe retry", got)
	}
	var users int
	if err := database.SQL().QueryRow(`SELECT COUNT(*) FROM users WHERE email = ?`, slug("Compute Cooperative")+"@motion.test").Scan(&users); err != nil {
		t.Fatal(err)
	}
	if users != 0 {
		t.Fatalf("failed bootstrap left %d admin rows behind, want 0 for safe retry", users)
	}
}

// TestBootstrapIsRetryableAfterAuditFailure confirms a clean retry once the
// audit backend is writable again produces exactly one tenant and one admin.
func TestBootstrapIsRetryableAfterAuditFailure(t *testing.T) {
	auditErr := errors.New("audit backend temporarily unwritable")
	service, database := newAuthTestService(t, failingAuditer{err: auditErr})

	if _, _, err := service.Bootstrap(context.Background(), bootstrapInput("Retry Lab")); err == nil {
		t.Fatal("bootstrap with failing audit should report an error")
	}

	// Switch to the real audit store and retry with the same tenant name.
	service.audits = audit.Store{}
	tenant, user, err := service.Bootstrap(context.Background(), bootstrapInput("Retry Lab"))
	if err != nil {
		t.Fatalf("retry bootstrap after audit failure: %v", err)
	}
	if tenant.ID == "" || user.ID == "" {
		t.Fatalf("retry returned empty tenant/user: %+v/%+v", tenant, user)
	}
	if got := tenantCount(t, database, "Retry Lab"); got != 1 {
		t.Fatalf("tenant count after retry = %d, want 1", got)
	}
	var admins int
	if err := database.SQL().QueryRow(`SELECT COUNT(*) FROM users WHERE tenant_id = ? AND role = 'tenant_admin'`, tenant.ID).Scan(&admins); err != nil {
		t.Fatal(err)
	}
	if admins != 1 {
		t.Fatalf("admin count after retry = %d, want 1", admins)
	}
}

// TestBootstrapRecordsAuditOnSuccess confirms the happy path is unaffected:
// the durable audit event is written alongside the tenant and admin.
func TestBootstrapRecordsAuditOnSuccess(t *testing.T) {
	service, database := newAuthTestService(t, audit.Store{})
	tenant, _, err := service.Bootstrap(context.Background(), bootstrapInput("Audit Lab"))
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	var events int
	if err := database.SQL().QueryRow(`SELECT COUNT(*) FROM audit_events WHERE tenant_id = ? AND action = 'tenant.bootstrap'`, tenant.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("audit_events for bootstrap = %d, want 1", events)
	}
}
