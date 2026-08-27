package integration_test

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
)

func TestBootstrapAuditFailureRollsBackIdentity(t *testing.T) {
	environment := newEnvironment(t)
	_, err := environment.database.SQL().ExecContext(context.Background(), `
		CREATE TRIGGER fail_gridbank_bootstrap_audit
		BEFORE INSERT ON audit_events
		WHEN NEW.action = 'tenant.bootstrap'
		BEGIN SELECT RAISE(ABORT, 'forced bootstrap audit failure'); END`)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = environment.auth.Bootstrap(context.Background(), auth.BootstrapInput{
		TenantName: "Rollback Tenant", Email: "rollback@gridbank.test", DisplayName: "Rollback Admin", Password: "test-password",
	})
	if err == nil {
		t.Fatal("bootstrap unexpectedly succeeded")
	}
	var tenants, users int
	if err := environment.database.SQL().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM tenants WHERE name = ?`, "Rollback Tenant").Scan(&tenants); err != nil {
		t.Fatal(err)
	}
	if err := environment.database.SQL().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM users WHERE email = ?`, "rollback@gridbank.test").Scan(&users); err != nil {
		t.Fatal(err)
	}
	if tenants != 0 || users != 0 {
		t.Fatalf("failed bootstrap leaked tenant=%d users=%d", tenants, users)
	}
}
