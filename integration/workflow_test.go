package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/metering"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

func TestEndToEndWorkloadMeteringLedgerScheduler(t *testing.T) {
	environment := newEnvironment(t)
	fixture := environment.settledWorkload(t)
	ctx := context.Background()

	batch, items, err := environment.meterings.Create(ctx, environment.steward, fixture.plan.Workload.ID, "workflow-batch")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 4 {
		t.Fatalf("metering item count = %d, want 4", len(items))
	}
	claim, err := environment.meterings.Claim(ctx, environment.reviewer, batch.ID, "workflow-claim")
	if err != nil {
		t.Fatal(err)
	}
	if claim.Batch.Owner != environment.reviewer.UserID || claim.Batch.LeaseToken == "" {
		t.Fatalf("claim does not identify current reviewer: %+v", claim.Batch)
	}
	for index, item := range claim.Items {
		updated, err := environment.meterings.Record(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, item.ID, "grasp", `{"quality":"accepted"}`, "workflow-item", item.Version)
		if err != nil {
			t.Fatalf("annotate item %d: %v", index, err)
		}
		if !updated.Complete || updated.Label != "grasp" {
			t.Fatalf("metering item %d was not completed: %+v", index, updated)
		}
	}
	currentBatch, _, err := environment.meterings.Get(ctx, environment.reviewer, batch.ID)
	if err != nil {
		t.Fatal(err)
	}
	submitted, err := environment.meterings.Submit(ctx, environment.reviewer, batch.ID, claim.Batch.LeaseToken, "workflow-submit")
	if err != nil {
		t.Fatal(err)
	}
	if submitted.Status != domain.MeteringSubmitted || submitted.Owner != "" || submitted.LeaseToken != "" {
		t.Fatalf("unexpected submitted batch: %+v", submitted)
	}
	if submitted.Version != currentBatch.Version+1 {
		t.Fatalf("submit version = %d, want %d", submitted.Version, currentBatch.Version+1)
	}
	accepted, err := environment.meterings.Review(ctx, environment.steward, batch.ID, "workflow-review", true, "")
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Status != domain.MeteringAccepted {
		t.Fatalf("review status = %s", accepted.Status)
	}

	draft, err := environment.ledgers.Create(ctx, environment.steward, "Retail grasp set", "workflow-ledger")
	if err != nil {
		t.Fatal(err)
	}
	draft, ledgerItems, err := environment.ledgers.AddWorkloads(ctx, environment.steward, draft.ID, "workflow-ledger-items", []string{fixture.plan.Workload.ID})
	if err != nil {
		t.Fatal(err)
	}
	if draft.ItemCount != 1 || len(ledgerItems) != 1 {
		t.Fatalf("ledger membership = %d/%d, want 1/1", draft.ItemCount, len(ledgerItems))
	}
	frozen, err := environment.ledgers.Freeze(ctx, environment.steward, draft.ID, "workflow-freeze")
	if err != nil {
		t.Fatal(err)
	}
	if frozen.Status != domain.LedgerStatusFrozen || len(frozen.Digest) != 64 || frozen.FrozenAt == nil {
		t.Fatalf("unexpected frozen ledger: %+v", frozen)
	}
	approved, err := environment.ledgers.Review(ctx, environment.reviewer, draft.ID, "workflow-quality", true, "")
	if err != nil {
		t.Fatal(err)
	}
	if approved.Status != domain.LedgerStatusApproved {
		t.Fatalf("quality status = %s", approved.Status)
	}
	release, err := environment.ledgers.Publish(ctx, environment.steward, draft.ID, "workflow-publish")
	if err != nil {
		t.Fatal(err)
	}
	if release.Status != domain.LedgerStatusPublished || release.Digest != frozen.Digest {
		t.Fatalf("published release does not preserve frozen digest: %+v", release)
	}

	job, err := environment.scheduler.Enqueue(ctx, environment.steward, release.ID, "workflow-enqueue")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != domain.JobQueued || job.MaxAttempts != 3 {
		t.Fatalf("unexpected queued job: %+v", job)
	}
	claimed, err := environment.scheduler.Claim(ctx, environment.worker)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Job.ID != job.ID || claimed.Job.Status != domain.JobRunning || claimed.Attempt.Attempt != 1 {
		t.Fatalf("unexpected claimed job: %+v", claimed)
	}
	checkpointed, err := environment.scheduler.Checkpoint(ctx, environment.worker, job.ID, claimed.Job.LeaseToken, "artifact-uploaded", claimed.Job.Version)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := environment.scheduler.Complete(ctx, environment.worker, job.ID, checkpointed.LeaseToken, "s3://models/model-1", "workflow-complete", checkpointed.Version)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != domain.JobSucceeded || completed.OutputURI == "" || completed.Owner != "" {
		t.Fatalf("unexpected completed job: %+v", completed)
	}
	var attemptOutcome string
	if err := environment.database.SQL().QueryRow(`SELECT outcome FROM compute_attempts WHERE job_id = ? AND attempt = 1`, job.ID).Scan(&attemptOutcome); err != nil {
		t.Fatal(err)
	}
	if attemptOutcome != "succeeded" {
		t.Fatalf("attempt outcome = %q", attemptOutcome)
	}
}

func TestWorkloadPlanReplayIsScopedAndExact(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Replay Lab", "UTC", "replay-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Replay Pool", domain.CapabilityGPU, "replay-pool")
	if err != nil {
		t.Fatal(err)
	}
	capacity_offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Replay Pose", "office", domain.CapabilityGPU, "replay-capacity_offer")
	if err != nil {
		t.Fatal(err)
	}
	input := workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: capacity_offer.ID, PoolID: pool.ID, ReservationRef: "consent-replay", IdempotencyKey: "same-key", RequestID: "replay-first"}
	first, err := environment.workloads.Plan(ctx, environment.operator, input)
	if err != nil {
		t.Fatal(err)
	}
	input.RequestID = "replay-second"
	second, err := environment.workloads.Plan(ctx, environment.operator, input)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replay || first.Workload.ID != second.Workload.ID || first.Lease.ID != second.Lease.ID || first.Lease.Token != second.Lease.Token {
		t.Fatalf("replay changed durable result: first=%+v second=%+v", first, second)
	}
	var workloadCount, leaseCount int
	if err := environment.database.SQL().QueryRow(`SELECT COUNT(*) FROM workloads`).Scan(&workloadCount); err != nil {
		t.Fatal(err)
	}
	if err := environment.database.SQL().QueryRow(`SELECT COUNT(*) FROM leases`).Scan(&leaseCount); err != nil {
		t.Fatal(err)
	}
	if workloadCount != 1 || leaseCount != 1 {
		t.Fatalf("replay duplicated side effects: workloads=%d leases=%d", workloadCount, leaseCount)
	}
	input.ReservationRef = "changed-consent"
	if _, err := environment.workloads.Plan(ctx, environment.operator, input); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("changed replay error = %v, want idempotency conflict", err)
	}
}

func TestAuditFailureRollsBackWorkloadTransition(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Rollback Lab", "UTC", "rollback-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Rollback Pool", domain.CapabilityGPU, "rollback-pool")
	if err != nil {
		t.Fatal(err)
	}
	capacity_offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Rollback Pose", "home", domain.CapabilityGPU, "rollback-capacity_offer")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: capacity_offer.ID, PoolID: pool.ID, ReservationRef: "consent", IdempotencyKey: "rollback-plan", RequestID: "rollback-plan"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := environment.database.SQL().Exec(`
		CREATE TRIGGER fail_ready_audit BEFORE INSERT ON audit_events
		WHEN NEW.action = 'workload.ready'
		BEGIN SELECT RAISE(ABORT, 'forced audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.workloads.MarkReady(ctx, environment.operator, plan.Workload.ID, "rollback-ready"); err == nil {
		t.Fatal("transition unexpectedly succeeded while audit insert failed")
	}
	current, err := environment.workloads.Get(ctx, environment.operator, plan.Workload.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != domain.WorkloadQueued || current.Version != plan.Workload.Version {
		t.Fatalf("failed transition leaked state: %+v", current)
	}
	var readyEvents int
	if err := environment.database.SQL().QueryRow(`SELECT COUNT(*) FROM outbox_events WHERE topic = 'workload.ready'`).Scan(&readyEvents); err != nil {
		t.Fatal(err)
	}
	if readyEvents != 0 {
		t.Fatalf("failed transition leaked %d outbox events", readyEvents)
	}
}

func TestCanceledContextCannotStartWorkload(t *testing.T) {
	environment := newEnvironment(t)
	ctx := context.Background()
	providerValue, err := environment.providers.CreateProvider(ctx, environment.admin, "Cancel Lab", "UTC", "cancel-provider")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := environment.providers.CreatePool(ctx, environment.admin, providerValue.ID, "Cancel Pool", domain.CapabilityGPU, "cancel-pool")
	if err != nil {
		t.Fatal(err)
	}
	capacity_offer, err := environment.providers.CreateCapacityOffer(ctx, environment.admin, "Cancel Pose", "factory", domain.CapabilityGPU, "cancel-capacity_offer")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := environment.workloads.Plan(ctx, environment.operator, workload.PlanInput{ProviderID: providerValue.ID, CapacityOfferID: capacity_offer.ID, PoolID: pool.ID, ReservationRef: "consent", IdempotencyKey: "cancel-plan", RequestID: "cancel-plan"})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := environment.workloads.MarkReady(ctx, environment.operator, plan.Workload.ID, "cancel-ready")
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := environment.workloads.Start(canceled, environment.operator, plan.Workload.ID, "cancel-start"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start error = %v, want context canceled", err)
	}
	current, err := environment.workloads.Get(ctx, environment.operator, plan.Workload.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != domain.WorkloadAllocated || current.Version != ready.Version || current.StartedAt != nil {
		t.Fatalf("canceled start changed workload: %+v", current)
	}
}

func TestConcurrentMeteringClaimHasOneOwner(t *testing.T) {
	environment := newEnvironment(t)
	fixture := environment.settledWorkload(t)
	batch, _, err := environment.meterings.Create(context.Background(), environment.steward, fixture.plan.Workload.ID, "claim-batch")
	if err != nil {
		t.Fatal(err)
	}
	secondReviewer := environment.createPrincipal(t, "reviewer2@motion.test", "Second Reviewer", domain.RoleReviewer)
	start := make(chan struct{})
	results := make(chan metering.ClaimResult, 2)
	errorsCh := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	claim := func(principal auth.Principal) {
		ready.Done()
		<-start
		result, err := environment.meterings.Claim(context.Background(), principal, batch.ID, "concurrent-claim")
		if err != nil {
			errorsCh <- err
			return
		}
		results <- result
	}
	go claim(environment.reviewer)
	go claim(secondReviewer)
	ready.Wait()
	close(start)
	var successCount, conflictCount int
	for i := 0; i < 2; i++ {
		select {
		case result := <-results:
			successCount++
			if result.Batch.Owner == "" || result.Batch.LeaseToken == "" {
				t.Errorf("successful claim lacks ownership: %+v", result.Batch)
			}
		case err := <-errorsCh:
			if errors.Is(err, domain.ErrConflict) {
				conflictCount++
			} else {
				t.Errorf("unexpected claim error: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent claim did not finish")
		}
	}
	if successCount != 1 || conflictCount != 1 {
		t.Fatalf("claim outcomes success=%d conflict=%d, want 1/1", successCount, conflictCount)
	}
}

func TestRecoveryIsAtomicAcrossExpiredResources(t *testing.T) {
	environment := newEnvironment(t)
	fixture := environment.settledWorkload(t)
	batch, _, err := environment.meterings.Create(context.Background(), environment.steward, fixture.plan.Workload.ID, "recovery-batch")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := environment.meterings.Claim(context.Background(), environment.reviewer, batch.ID, "recovery-claim")
	if err != nil {
		t.Fatal(err)
	}
	environment.clock.Advance(3 * time.Minute)
	if _, err := environment.database.SQL().Exec(`
		CREATE TRIGGER fail_outbox_recovery BEFORE UPDATE ON outbox_events
		WHEN OLD.status = 'delivering'
		BEGIN SELECT RAISE(ABORT, 'forced recovery failure'); END`); err != nil {
		t.Fatal(err)
	}
	now := storage.FormatTime(environment.clock.Now().Add(-time.Minute))
	if _, err := environment.database.SQL().Exec(`
		UPDATE outbox_events SET status = 'delivering', owner = 'old-worker', lease_token = 'old-token', lease_expires_at = ?
		WHERE id = (SELECT id FROM outbox_events LIMIT 1)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := environment.recovery.RecoverExpired(context.Background(), environment.admin.TenantID, environment.admin.UserID, "recovery-run"); err == nil {
		t.Fatal("recovery unexpectedly succeeded through forced outbox failure")
	}
	current, _, err := environment.meterings.Get(context.Background(), environment.reviewer, batch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != domain.MeteringClaimed || current.Owner != claim.Batch.Owner {
		t.Fatalf("failed recovery partially reset metering claim: %+v", current)
	}
	if _, err := environment.database.SQL().Exec(`DROP TRIGGER fail_outbox_recovery`); err != nil {
		t.Fatal(err)
	}
	result, err := environment.recovery.RecoverExpired(context.Background(), environment.admin.TenantID, environment.admin.UserID, "recovery-retry")
	if err != nil {
		t.Fatal(err)
	}
	if result.MeteringBatches != 1 || result.OutboxEvents == 0 {
		t.Fatalf("unexpected recovery result: %+v", result)
	}
	current, _, err = environment.meterings.Get(context.Background(), environment.reviewer, batch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != domain.MeteringOpen || current.Owner != "" || current.LeaseToken != "" {
		t.Fatalf("successful recovery did not reopen batch: %+v", current)
	}
}

func TestRestartRetainsWorkflowState(t *testing.T) {
	environment := newEnvironment(t)
	fixture := environment.settledWorkload(t)
	if err := environment.database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.Open(context.Background(), storage.Options{Path: environment.databasePath, MaxOpenConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var status domain.JobWorkloadStatus
	var manifestCount int
	if err := reopened.SQL().QueryRow(`SELECT status FROM workloads WHERE id = ?`, fixture.plan.Workload.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := reopened.SQL().QueryRow(`SELECT COUNT(*) FROM capacity_streams WHERE workload_id = ? AND status = 'aligned'`, fixture.plan.Workload.ID).Scan(&manifestCount); err != nil {
		t.Fatal(err)
	}
	if status != domain.WorkloadSettled || manifestCount != 2 {
		t.Fatalf("restart state status=%s aligned=%d, want settled/2", status, manifestCount)
	}
}

var _ *sql.DB
