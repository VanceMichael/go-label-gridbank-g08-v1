package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
)

func TestStaleOutboxAckCannotAcknowledgeNewClaim(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	if _, err := environment.ledgers.Create(ctx, environment.steward, "Task 0029 ledger", "task0029-ledger-create"); err != nil {
		t.Fatal(err)
	}
	oldClaim, err := environment.outbox.Claim(ctx, environment.admin.TenantID, environment.worker.UserID)
	if err != nil {
		t.Fatal(err)
	}
	environment.clock.Advance(3 * time.Minute)
	if _, err := environment.recovery.RecoverExpired(ctx, environment.admin.TenantID, environment.admin.UserID, "task0029-recovery"); err != nil {
		t.Fatal(err)
	}
	replacementWorker := environment.createPrincipal(t, "replacement-worker@motion.test", "Replacement Worker", domain.RoleWorker)
	replacement, err := environment.outbox.Claim(ctx, environment.admin.TenantID, replacementWorker.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.ID != oldClaim.ID || replacement.LeaseToken == oldClaim.LeaseToken {
		t.Fatalf("replacement claim does not represent a new ownership epoch: old=%+v replacement=%+v", oldClaim, replacement)
	}
	if _, err := environment.outbox.Acknowledge(ctx, environment.admin.TenantID, oldClaim.ID, environment.worker.UserID, oldClaim.LeaseToken, oldClaim.Version); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale acknowledgement error = %v, want lease lost", err)
	}
	current, err := environment.outbox.Get(ctx, environment.admin.TenantID, oldClaim.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != domain.OutboxDelivering || current.Owner != replacementWorker.UserID || current.LeaseToken != replacement.LeaseToken || current.Version != replacement.Version {
		t.Fatalf("stale worker changed replacement delivery claim: %+v", current)
	}
}
