package integration_test

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
)

func TestLedgerFreezeFailurePreservesDraft(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	fixture := environment.settledWorkload(t)

	batch, _, err := environment.meterings.Create(ctx, environment.steward, fixture.plan.Workload.ID, "task0026-metering-create")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := environment.meterings.Claim(ctx, environment.reviewer, batch.ID, "task0026-metering-claim")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range claim.Items {
		if _, err := environment.meterings.Record(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, item.ID, "accepted", `{"quality":"accepted"}`, "task0026-metering-record", item.Version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := environment.meterings.Submit(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, "task0026-metering-submit"); err != nil {
		t.Fatal(err)
	}
	accepted, err := environment.meterings.Review(ctx, environment.steward, batch.ID, "task0026-metering-review", true, "")
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Status != domain.MeteringAccepted {
		t.Fatalf("metering status = %s, want accepted", accepted.Status)
	}

	draft, err := environment.ledgers.Create(ctx, environment.steward, "Task 0026 ledger", "task0026-ledger-create")
	if err != nil {
		t.Fatal(err)
	}
	draft, _, err = environment.ledgers.AddWorkloads(ctx, environment.steward, draft.ID, "task0026-ledger-add", []string{fixture.plan.Workload.ID})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := environment.database.SQL().Exec(`
		CREATE TRIGGER fail_task0026_freeze_audit
		BEFORE INSERT ON audit_events
		WHEN NEW.action = 'ledger.freeze'
		BEGIN SELECT RAISE(ABORT, 'forced ledger freeze audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.ledgers.Freeze(ctx, environment.steward, draft.ID, "task0026-freeze-failing"); err == nil {
		t.Fatal("freeze unexpectedly succeeded while audit insert failed")
	}

	current, items, err := environment.ledgers.Get(ctx, environment.steward, draft.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != domain.LedgerStatusDraft || current.Digest != "" || current.FrozenAt != nil || current.Version != draft.Version {
		t.Fatalf("failed freeze leaked ledger state: %+v", current)
	}
	if len(items) != 1 || current.ItemCount != 1 {
		t.Fatalf("ledger membership changed after failed freeze: items=%d draft=%+v", len(items), current)
	}
	var freezeAudits, freezeEvents int
	if err := environment.database.SQL().QueryRow(`
		SELECT COUNT(*) FROM audit_events
		WHERE object_type = 'ledger' AND object_id = ? AND action = 'ledger.freeze'`, draft.ID).Scan(&freezeAudits); err != nil {
		t.Fatal(err)
	}
	if err := environment.database.SQL().QueryRow(`
		SELECT COUNT(*) FROM outbox_events
		WHERE aggregate_type = 'ledger' AND aggregate_id = ? AND topic = 'ledger.freeze'`, draft.ID).Scan(&freezeEvents); err != nil {
		t.Fatal(err)
	}
	if freezeAudits != 0 || freezeEvents != 0 {
		t.Fatalf("failed freeze leaked effects: audits=%d events=%d", freezeAudits, freezeEvents)
	}

	if _, err := environment.database.SQL().Exec(`DROP TRIGGER fail_task0026_freeze_audit`); err != nil {
		t.Fatal(err)
	}
	frozen, err := environment.ledgers.Freeze(ctx, environment.steward, draft.ID, "task0026-freeze-retry")
	if err != nil {
		t.Fatal(err)
	}
	if frozen.Status != domain.LedgerStatusFrozen || frozen.Digest == "" || frozen.FrozenAt == nil {
		t.Fatalf("retry did not freeze ledger: %+v", frozen)
	}
}
