package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
)

func TestCanceledSchedulerEnqueueLeavesNoJob(t *testing.T) {
	environment := newEnvironment(t)
	release := publishedReleaseForTask0019(t, environment)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := environment.scheduler.Enqueue(canceled, environment.steward, release.ID, "task0019-canceled-enqueue"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled scheduler enqueue error = %v, want context canceled", err)
	}
	var jobs, events int
	if err := environment.database.SQL().QueryRow(`SELECT COUNT(*) FROM compute_jobs WHERE release_id = ?`, release.ID).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := environment.database.SQL().QueryRow(`
		SELECT COUNT(*) FROM outbox_events
		WHERE topic = 'scheduler.enqueue' AND aggregate_type = 'scheduler_job'`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if jobs != 0 || events != 0 {
		t.Fatalf("canceled enqueue left durable effects: jobs=%d events=%d", jobs, events)
	}
}

func publishedReleaseForTask0019(t *testing.T, environment *testEnvironment) domain.LedgerRelease {
	t.Helper()
	ctx := context.Background()
	fixture := environment.settledWorkload(t)
	batch, _, err := environment.meterings.Create(ctx, environment.steward, fixture.plan.Workload.ID, "task0019-metering-create")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := environment.meterings.Claim(ctx, environment.reviewer, batch.ID, "task0019-metering-claim")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range claim.Items {
		if _, err := environment.meterings.Record(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, item.ID, "accepted", `{"quality":"accepted"}`, "task0019-metering-record", item.Version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := environment.meterings.Submit(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, "task0019-metering-submit"); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.meterings.Review(ctx, environment.steward, batch.ID, "task0019-metering-review", true, ""); err != nil {
		t.Fatal(err)
	}
	draft, err := environment.ledgers.Create(ctx, environment.steward, "Task 0019 release", "task0019-ledger-create")
	if err != nil {
		t.Fatal(err)
	}
	draft, _, err = environment.ledgers.AddWorkloads(ctx, environment.steward, draft.ID, "task0019-ledger-add", []string{fixture.plan.Workload.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.ledgers.Freeze(ctx, environment.steward, draft.ID, "task0019-ledger-freeze"); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.ledgers.Review(ctx, environment.reviewer, draft.ID, "task0019-ledger-review", true, ""); err != nil {
		t.Fatal(err)
	}
	release, err := environment.ledgers.Publish(ctx, environment.steward, draft.ID, "task0019-ledger-publish")
	if err != nil {
		t.Fatal(err)
	}
	return release
}
