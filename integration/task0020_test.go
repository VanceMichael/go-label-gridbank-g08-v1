package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/scheduler"
)

func TestConcurrentSchedulerClaimCreatesOneAttempt(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	releaseID := publishSchedulerRelease(t, environment)
	job, err := environment.scheduler.Enqueue(ctx, environment.steward, releaseID, "task0020-enqueue")
	if err != nil {
		t.Fatal(err)
	}
	replacement := environment.createPrincipal(t, "replacement-scheduler@motion.test", "Replacement Scheduler", domain.RoleWorker)
	repository := scheduler.Repository{}
	staleJob, err := repository.FindDue(ctx, environment.database.SQL(), environment.admin.TenantID, environment.clock.Now())
	if err != nil {
		t.Fatal(err)
	}

	claimSnapshot := func(workerID, token string) error {
		return environment.database.Write(ctx, func(tx *sql.Tx) error {
			now := environment.clock.Now()
			if err := repository.Claim(ctx, tx, staleJob, workerID, token, now, now.Add(2*time.Minute)); err != nil {
				return err
			}
			claimed, err := repository.FindJob(ctx, tx, staleJob.TenantID, staleJob.ID)
			if err != nil {
				return err
			}
			attemptID, err := domain.NewID("attempt")
			if err != nil {
				return err
			}
			return repository.InsertAttempt(ctx, tx, domain.JobAttempt{
				ID:        attemptID,
				TenantID:  staleJob.TenantID,
				JobID:     staleJob.ID,
				Attempt:   claimed.AttemptCount,
				WorkerID:  workerID,
				StartedAt: now,
				Outcome:   "running",
			})
		})
	}

	if err := claimSnapshot(environment.worker.UserID, "task0020-owner-token"); err != nil {
		t.Fatalf("first scheduler claim failed: %v", err)
	}
	if err := claimSnapshot(replacement.UserID, "task0020-stale-token"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale scheduler claim error = %v, want conflict", err)
	}

	current, err := environment.scheduler.Get(ctx, environment.steward, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.AttemptCount != 1 || current.Owner != environment.worker.UserID || current.LeaseToken != "task0020-owner-token" || current.Version != staleJob.Version+1 {
		t.Fatalf("stale claim replaced the first ownership epoch: %+v", current)
	}
	var attempts int
	var attemptWorker string
	if err := environment.database.SQL().QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MAX(worker_id), '')
		FROM compute_attempts WHERE tenant_id = ? AND job_id = ?`,
		environment.admin.TenantID, job.ID,
	).Scan(&attempts, &attemptWorker); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || attemptWorker != environment.worker.UserID {
		t.Fatalf("scheduler attempts = %d for worker %q, want one attempt for %q", attempts, attemptWorker, environment.worker.UserID)
	}
}

func publishSchedulerRelease(t *testing.T, environment *testEnvironment) string {
	t.Helper()
	ctx := context.Background()
	fixture := environment.settledWorkload(t)
	batch, items, err := environment.meterings.Create(ctx, environment.steward, fixture.plan.Workload.ID, "task0020-batch")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := environment.meterings.Claim(ctx, environment.reviewer, batch.ID, "task0020-metering-claim")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if _, err := environment.meterings.Record(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, item.ID, "accepted", `{"quality":"accepted"}`, "task0020-record", item.Version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := environment.meterings.Submit(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, "task0020-submit"); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.meterings.Review(ctx, environment.steward, batch.ID, "task0020-review", true, ""); err != nil {
		t.Fatal(err)
	}
	ledgerValue, err := environment.ledgers.Create(ctx, environment.steward, "Task 0020 ledger", "task0020-ledger")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := environment.ledgers.AddWorkloads(ctx, environment.steward, ledgerValue.ID, "task0020-add", []string{fixture.plan.Workload.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.ledgers.Freeze(ctx, environment.steward, ledgerValue.ID, "task0020-freeze"); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.ledgers.Review(ctx, environment.reviewer, ledgerValue.ID, "task0020-ledger-review", true, ""); err != nil {
		t.Fatal(err)
	}
	release, err := environment.ledgers.Publish(ctx, environment.steward, ledgerValue.ID, "task0020-publish")
	if err != nil {
		t.Fatal(err)
	}
	return release.ID
}
