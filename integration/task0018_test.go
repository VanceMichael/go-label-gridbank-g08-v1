package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
)

func TestLedgerPublishRejectsMembershipDrift(t *testing.T) {
	environment := newEnvironment(t)
	fixture := environment.settledWorkload(t)
	ctx := context.Background()
	batch, _, err := environment.meterings.Create(ctx, environment.steward, fixture.plan.Workload.ID, "membership-drift-batch")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := environment.meterings.Claim(ctx, environment.reviewer, batch.ID, "membership-drift-claim")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range claim.Items {
		if _, err := environment.meterings.Record(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, item.ID, "accepted", `{"ok":true}`, "membership-drift-record", item.Version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := environment.meterings.Submit(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, "membership-drift-submit"); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.meterings.Review(ctx, environment.steward, batch.ID, "membership-drift-review-metering", true, ""); err != nil {
		t.Fatal(err)
	}
	draft, err := environment.ledgers.Create(ctx, environment.steward, "Membership Drift Ledger", "membership-drift-create")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := environment.ledgers.AddWorkloads(ctx, environment.steward, draft.ID, "membership-drift-add", []string{fixture.plan.Workload.ID}); err != nil {
		t.Fatal(err)
	}
	frozen, err := environment.ledgers.Freeze(ctx, environment.steward, draft.ID, "membership-drift-freeze")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.database.SQL().ExecContext(ctx, `UPDATE credit_plan_items SET revision = revision + 1 WHERE ledger_id = ?`, draft.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.ledgers.Review(ctx, environment.reviewer, draft.ID, "membership-drift-review", true, ""); err != nil {
		t.Fatal(err)
	}
	_, err = environment.ledgers.Publish(ctx, environment.steward, draft.ID, "membership-drift-publish")
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("publish after membership drift error = %v, want conflict", err)
	}
	var releases int
	if err := environment.database.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM credit_releases WHERE ledger_id = ?`, draft.ID).Scan(&releases); err != nil {
		t.Fatal(err)
	}
	if releases != 0 {
		t.Fatalf("membership drift created %d release(s) from frozen digest %s", releases, frozen.Digest)
	}
}
