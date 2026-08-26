package integration_test

import (
	"context"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
)

func TestSchedulerRetryPreservesCheckpoint(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	release := publishedReleaseForTask0021(t, environment)
	job, err := environment.scheduler.Enqueue(ctx, environment.steward, release.ID, "task0021-enqueue")
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := environment.scheduler.Claim(ctx, environment.worker)
	if err != nil {
		t.Fatal(err)
	}
	checkpointed, err := environment.scheduler.Checkpoint(ctx, environment.worker, job.ID, claimed.Job.LeaseToken, "artifact-uploaded", claimed.Job.Version)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := environment.scheduler.Fail(ctx, environment.worker, job.ID, checkpointed.LeaseToken, "transient provider timeout", "task0021-retry", checkpointed.Version, false)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != domain.JobRetrying || failed.Checkpoint != "artifact-uploaded" {
		t.Fatalf("retry lost durable checkpoint: %+v", failed)
	}
	var outcome, errorText string
	if err := environment.database.SQL().QueryRow(`
		SELECT outcome, error_text FROM compute_attempts
		WHERE job_id = ? AND attempt = 1`, job.ID).Scan(&outcome, &errorText); err != nil {
		t.Fatal(err)
	}
	if outcome != "retrying" || errorText != "transient provider timeout" {
		t.Fatalf("attempt history after retry = %q/%q", outcome, errorText)
	}
}

func publishedReleaseForTask0021(t *testing.T, environment *testEnvironment) domain.LedgerRelease {
	t.Helper()
	ctx := context.Background()
	fixture := environment.settledWorkload(t)
	batch, _, err := environment.meterings.Create(ctx, environment.steward, fixture.plan.Workload.ID, "task0021-metering-create")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := environment.meterings.Claim(ctx, environment.reviewer, batch.ID, "task0021-metering-claim")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range claim.Items {
		if _, err := environment.meterings.Record(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, item.ID, "accepted", `{"quality":"accepted"}`, "task0021-metering-record", item.Version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := environment.meterings.Submit(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, "task0021-metering-submit"); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.meterings.Review(ctx, environment.steward, batch.ID, "task0021-metering-review", true, ""); err != nil {
		t.Fatal(err)
	}
	draft, err := environment.ledgers.Create(ctx, environment.steward, "Task 0021 release", "task0021-ledger-create")
	if err != nil {
		t.Fatal(err)
	}
	draft, _, err = environment.ledgers.AddWorkloads(ctx, environment.steward, draft.ID, "task0021-ledger-add", []string{fixture.plan.Workload.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.ledgers.Freeze(ctx, environment.steward, draft.ID, "task0021-ledger-freeze"); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.ledgers.Review(ctx, environment.reviewer, draft.ID, "task0021-ledger-review", true, ""); err != nil {
		t.Fatal(err)
	}
	release, err := environment.ledgers.Publish(ctx, environment.steward, draft.ID, "task0021-ledger-publish")
	if err != nil {
		t.Fatal(err)
	}
	return release
}
