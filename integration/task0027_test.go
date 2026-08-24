package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
)

func TestSettlementReversalRequiresSafeReleaseState(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	release := publishedReleaseForTask0027(t, environment)
	job, err := environment.scheduler.Enqueue(ctx, environment.steward, release.ID, "task0027-enqueue")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.scheduler.Claim(ctx, environment.worker); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.ledgers.Revoke(ctx, environment.steward, release.ID, "task0027-revoke", "customer correction"); !errors.Is(err, domain.ErrPrecondition) {
		t.Fatalf("revoke with active scheduler job error = %v, want precondition", err)
	}
	var status string
	if err := environment.database.SQL().QueryRow(`SELECT status FROM credit_releases WHERE id = ?`, release.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.LedgerStatusPublished) {
		t.Fatalf("active release status after rejected revoke = %s", status)
	}
	var activeJobs int
	if err := environment.database.SQL().QueryRow(`SELECT COUNT(*) FROM compute_jobs WHERE id = ? AND status = 'running'`, job.ID).Scan(&activeJobs); err != nil {
		t.Fatal(err)
	}
	if activeJobs != 1 {
		t.Fatalf("scheduler job no longer active after rejected revoke: %d", activeJobs)
	}
}

func publishedReleaseForTask0027(t *testing.T, environment *testEnvironment) domain.LedgerRelease {
	t.Helper()
	ctx := context.Background()
	fixture := environment.settledWorkload(t)
	batch, _, err := environment.meterings.Create(ctx, environment.steward, fixture.plan.Workload.ID, "task0027-metering-create")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := environment.meterings.Claim(ctx, environment.reviewer, batch.ID, "task0027-metering-claim")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range claim.Items {
		if _, err := environment.meterings.Record(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, item.ID, "accepted", `{"quality":"accepted"}`, "task0027-metering-record", item.Version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := environment.meterings.Submit(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, "task0027-metering-submit"); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.meterings.Review(ctx, environment.steward, batch.ID, "task0027-metering-review", true, ""); err != nil {
		t.Fatal(err)
	}
	draft, err := environment.ledgers.Create(ctx, environment.steward, "Task 0027 release", "task0027-ledger-create")
	if err != nil {
		t.Fatal(err)
	}
	draft, _, err = environment.ledgers.AddWorkloads(ctx, environment.steward, draft.ID, "task0027-ledger-add", []string{fixture.plan.Workload.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.ledgers.Freeze(ctx, environment.steward, draft.ID, "task0027-ledger-freeze"); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.ledgers.Review(ctx, environment.reviewer, draft.ID, "task0027-ledger-review", true, ""); err != nil {
		t.Fatal(err)
	}
	release, err := environment.ledgers.Publish(ctx, environment.steward, draft.ID, "task0027-ledger-publish")
	if err != nil {
		t.Fatal(err)
	}
	return release
}
