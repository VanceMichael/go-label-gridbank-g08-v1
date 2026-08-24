package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/audit"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/ledger"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/outbox"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

type Service struct {
	db          *storage.Database
	repo        Repository
	ledgers     ledger.Repository
	audits      audit.Store
	outbox      outbox.Repository
	clock       clock.Clock
	leaseTTL    time.Duration
	retryBase   time.Duration
	maxAttempts int
}

type ClaimResult struct {
	Job     domain.SchedulerJob
	Attempt domain.JobAttempt
}

func NewService(db *storage.Database, c clock.Clock, leaseTTL, retryBase time.Duration, maxAttempts int) *Service {
	return &Service{db: db, repo: Repository{}, ledgers: ledger.Repository{}, audits: audit.Store{}, outbox: outbox.Repository{}, clock: c, leaseTTL: leaseTTL, retryBase: retryBase, maxAttempts: maxAttempts}
}

func (s *Service) Enqueue(ctx context.Context, principal auth.Principal, releaseID, requestID string) (domain.SchedulerJob, error) {
	if err := auth.RequireRole(principal, domain.RoleDataSteward); err != nil {
		return domain.SchedulerJob{}, err
	}
	if releaseID == "" || requestID == "" {
		return domain.SchedulerJob{}, domain.Validation("scheduler.enqueue", "release and request id are required")
	}
	id, err := domain.NewID("job")
	if err != nil {
		return domain.SchedulerJob{}, err
	}
	now := s.clock.Now()
	job := domain.SchedulerJob{ID: id, TenantID: principal.TenantID, ReleaseID: releaseID, Status: domain.JobQueued, MaxAttempts: s.maxAttempts, NextAttemptAt: now, CreatedAt: now, UpdatedAt: now, Version: 1}
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		release, err := s.ledgers.FindRelease(ctx, tx, principal.TenantID, releaseID)
		if err != nil {
			return err
		}
		if release.Status != domain.LedgerStatusPublished {
			return domain.Precondition("scheduler.enqueue", "ledger_release", releaseID, "release must be published")
		}
		if err := s.repo.InsertJob(ctx, tx, job); err != nil {
			return err
		}
		return s.appendEffects(ctx, tx, principal.TenantID, principal.UserID, requestID, "scheduler.enqueue", job.ID, "queued", now)
	})
	if err != nil {
		return domain.SchedulerJob{}, fmt.Errorf("enqueue scheduler job: %w", err)
	}
	return job, nil
}

func (s *Service) Claim(ctx context.Context, principal auth.Principal) (ClaimResult, error) {
	if err := auth.RequireRole(principal, domain.RoleWorker); err != nil {
		return ClaimResult{}, err
	}
	token, err := newLeaseToken()
	if err != nil {
		return ClaimResult{}, err
	}
	now := s.clock.Now()
	var result ClaimResult
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		job, err := s.repo.FindDue(ctx, tx, principal.TenantID, now)
		if err != nil {
			return err
		}
		if job.AttemptCount >= job.MaxAttempts {
			return domain.Precondition("scheduler.claim", "scheduler_job", job.ID, "job exhausted its attempt budget")
		}
		expires := now.Add(s.leaseTTL)
		if err := s.repo.Claim(ctx, tx, job, principal.UserID, token, now, expires); err != nil {
			return err
		}
		attemptID, err := domain.NewID("attempt")
		if err != nil {
			return err
		}
		attempt := domain.JobAttempt{ID: attemptID, TenantID: principal.TenantID, JobID: job.ID, Attempt: job.AttemptCount + 1, WorkerID: principal.UserID, StartedAt: now, Outcome: "running"}
		if err := s.repo.InsertAttempt(ctx, tx, attempt); err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal.TenantID, principal.UserID, "worker-claim", "scheduler.claim", job.ID, "running", now); err != nil {
			return err
		}
		job.Status, job.Owner, job.LeaseToken, job.LeaseExpiresAt = domain.JobRunning, principal.UserID, token, &expires
		job.AttemptCount, job.UpdatedAt, job.Version = job.AttemptCount+1, now, job.Version+1
		result = ClaimResult{Job: job, Attempt: attempt}
		return nil
	})
	if err != nil {
		return ClaimResult{}, fmt.Errorf("claim scheduler job: %w", err)
	}
	return result, nil
}

func (s *Service) Renew(ctx context.Context, principal auth.Principal, jobID, token string, version int64) (domain.SchedulerJob, error) {
	if err := auth.RequireRole(principal, domain.RoleWorker); err != nil {
		return domain.SchedulerJob{}, err
	}
	now := s.clock.Now()
	expires := now.Add(s.leaseTTL)
	var renewed domain.SchedulerJob
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		job, err := s.repo.FindJob(ctx, tx, principal.TenantID, jobID)
		if err != nil {
			return err
		}
		if job.Owner != principal.UserID || job.LeaseToken != token || job.Version != version {
			return domain.Wrap(domain.ErrLeaseLost, "scheduler.renew", "scheduler_job", jobID, "lease identity or ownership changed", nil)
		}
		if err := s.repo.Renew(ctx, tx, principal.TenantID, jobID, principal.UserID, token, version, now, expires); err != nil {
			return err
		}
		job.LeaseExpiresAt, job.UpdatedAt, job.Version = &expires, now, job.Version+1
		renewed = job
		return nil
	})
	if err != nil {
		return domain.SchedulerJob{}, fmt.Errorf("renew scheduler job: %w", err)
	}
	return renewed, nil
}

func (s *Service) Checkpoint(ctx context.Context, principal auth.Principal, jobID, token, checkpoint string, version int64) (domain.SchedulerJob, error) {
	if err := auth.RequireRole(principal, domain.RoleWorker); err != nil {
		return domain.SchedulerJob{}, err
	}
	checkpoint = strings.TrimSpace(checkpoint)
	if checkpoint == "" || len(checkpoint) > 2048 {
		return domain.SchedulerJob{}, domain.Validation("scheduler.checkpoint", "bounded checkpoint is required")
	}
	now := s.clock.Now()
	var updated domain.SchedulerJob
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		job, err := s.repo.FindJob(ctx, tx, principal.TenantID, jobID)
		if err != nil {
			return err
		}
		if job.Owner != principal.UserID || job.LeaseToken != token || job.Version != version {
			return domain.Wrap(domain.ErrLeaseLost, "scheduler.checkpoint", "scheduler_job", jobID, "lease identity or ownership changed", nil)
		}
		if err := s.repo.SaveCheckpoint(ctx, tx, principal.TenantID, jobID, principal.UserID, token, checkpoint, version, now); err != nil {
			return err
		}
		job.Checkpoint, job.UpdatedAt, job.Version = checkpoint, now, job.Version+1
		updated = job
		return nil
	})
	if err != nil {
		return domain.SchedulerJob{}, fmt.Errorf("save scheduler checkpoint: %w", err)
	}
	return updated, nil
}

func (s *Service) Complete(ctx context.Context, principal auth.Principal, jobID, token, outputURI, requestID string, version int64) (domain.SchedulerJob, error) {
	if err := auth.RequireRole(principal, domain.RoleWorker); err != nil {
		return domain.SchedulerJob{}, err
	}
	outputURI = strings.TrimSpace(outputURI)
	if outputURI == "" || requestID == "" {
		return domain.SchedulerJob{}, domain.Validation("scheduler.complete", "output URI and request id are required")
	}
	now := s.clock.Now()
	var completed domain.SchedulerJob
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		job, err := s.repo.FindJob(ctx, tx, principal.TenantID, jobID)
		if err != nil {
			return err
		}
		if job.Owner != principal.UserID || job.LeaseToken != token || job.Version != version {
			return domain.Wrap(domain.ErrLeaseLost, "scheduler.complete", "scheduler_job", jobID, "lease identity or ownership changed", nil)
		}
		if err := job.Status.Transition(domain.JobSucceeded); err != nil {
			return err
		}
		if strings.TrimSpace(job.Checkpoint) == "" {
			return domain.Precondition("scheduler.complete", "scheduler_job", jobID, "durable checkpoint is required before completion")
		}
		if err := s.repo.Complete(ctx, tx, principal.TenantID, jobID, principal.UserID, token, outputURI, version, now); err != nil {
			return err
		}
		if err := s.repo.FinishAttempt(ctx, tx, principal.TenantID, jobID, job.AttemptCount, "succeeded", "", now); err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal.TenantID, principal.UserID, requestID, "scheduler.complete", jobID, "succeeded", now); err != nil {
			return err
		}
		job.Status, job.Owner, job.LeaseToken, job.LeaseExpiresAt = domain.JobSucceeded, "", "", nil
		job.OutputURI, job.UpdatedAt, job.Version = outputURI, now, job.Version+1
		completed = job
		return nil
	})
	if err != nil {
		return domain.SchedulerJob{}, fmt.Errorf("complete scheduler job: %w", err)
	}
	return completed, nil
}

func (s *Service) Fail(ctx context.Context, principal auth.Principal, jobID, token, message, requestID string, version int64, permanent bool) (domain.SchedulerJob, error) {
	if err := auth.RequireRole(principal, domain.RoleWorker); err != nil {
		return domain.SchedulerJob{}, err
	}
	message = strings.TrimSpace(message)
	if message == "" || requestID == "" {
		return domain.SchedulerJob{}, domain.Validation("scheduler.fail", "error message and request id are required")
	}
	now := s.clock.Now()
	var failed domain.SchedulerJob
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		job, err := s.repo.FindJob(ctx, tx, principal.TenantID, jobID)
		if err != nil {
			return err
		}
		if job.Owner != principal.UserID || job.LeaseToken != token || job.Version != version {
			return domain.Wrap(domain.ErrLeaseLost, "scheduler.fail", "scheduler_job", jobID, "lease identity or ownership changed", nil)
		}
		retry := !permanent && job.AttemptCount < job.MaxAttempts
		to := domain.JobFailed
		outcome := "failed"
		nextAttempt := now
		if retry {
			to, outcome = domain.JobRetrying, "retrying"
			nextAttempt = now.Add(s.backoff(job.AttemptCount))
		}
		if err := job.Status.Transition(to); err != nil {
			return err
		}
		if err := s.repo.Fail(ctx, tx, job, principal.UserID, token, message, retry, nextAttempt, now); err != nil {
			return err
		}
		if err := s.repo.FinishAttempt(ctx, tx, principal.TenantID, jobID, job.AttemptCount, outcome, message, now); err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal.TenantID, principal.UserID, requestID, "scheduler.fail", jobID, outcome, now); err != nil {
			return err
		}
		job.Status, job.Owner, job.LeaseToken, job.LeaseExpiresAt = to, "", "", nil
		job.LastError, job.NextAttemptAt, job.UpdatedAt, job.Version = message, nextAttempt, now, job.Version+1
		failed = job
		return nil
	})
	if err != nil {
		return domain.SchedulerJob{}, fmt.Errorf("fail scheduler job: %w", err)
	}
	return failed, nil
}

func (s *Service) Get(ctx context.Context, principal auth.Principal, jobID string) (domain.SchedulerJob, error) {
	return s.repo.FindJob(ctx, s.db.SQL(), principal.TenantID, jobID)
}

func (s *Service) backoff(attempt int) time.Duration {
	exponent := math.Min(float64(attempt-1), 8)
	return time.Duration(float64(s.retryBase) * math.Pow(2, exponent))
}

func (s *Service) appendEffects(ctx context.Context, tx *sql.Tx, tenantID, actorID, requestID, action, jobID, outcome string, now time.Time) error {
	auditID, err := domain.NewID("audit")
	if err != nil {
		return err
	}
	eventID, err := domain.NewID("event")
	if err != nil {
		return err
	}
	if err := s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: tenantID, ActorID: actorID, Action: action, ObjectType: "scheduler_job", ObjectID: jobID, Outcome: outcome, RequestID: requestID, CreatedAt: now}); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]string{"job_id": jobID, "event": action, "outcome": outcome})
	return s.outbox.Enqueue(ctx, tx, domain.OutboxEvent{ID: eventID, TenantID: tenantID, Topic: action, AggregateType: "scheduler_job", AggregateID: jobID, Payload: string(payload), Status: domain.OutboxPending, MaxAttempts: 5, NextAttemptAt: now, CreatedAt: now, UpdatedAt: now, Version: 1})
}
