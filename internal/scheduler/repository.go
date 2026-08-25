package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

type Repository struct{}

func (Repository) InsertJob(ctx context.Context, q storage.Queryer, job domain.SchedulerJob) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO compute_jobs(
			id, tenant_id, release_id, status, owner, lease_token,
			lease_expires_at, attempt_count, max_attempts, checkpoint,
			output_uri, last_error, next_attempt_at, created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		job.ID, job.TenantID, job.ReleaseID, job.Status, job.Owner, job.LeaseToken,
		storage.NullableTime(job.LeaseExpiresAt), job.AttemptCount, job.MaxAttempts,
		job.Checkpoint, job.OutputURI, job.LastError, storage.FormatTime(job.NextAttemptAt),
		storage.FormatTime(job.CreatedAt), storage.FormatTime(job.UpdatedAt), job.Version)
	if err != nil {
		return fmt.Errorf("insert scheduler job: %w", err)
	}
	return nil
}

func (Repository) FindJob(ctx context.Context, q storage.Queryer, tenantID, id string) (domain.SchedulerJob, error) {
	return scanJob(q.QueryRowContext(ctx, `
		SELECT id, tenant_id, release_id, status, owner, lease_token,
		       lease_expires_at, attempt_count, max_attempts, checkpoint,
		       output_uri, last_error, next_attempt_at, created_at, updated_at, version
		FROM compute_jobs WHERE tenant_id = ? AND id = ?`, tenantID, id), id)
}

func (Repository) FindDue(ctx context.Context, q storage.Queryer, tenantID string, now time.Time) (domain.SchedulerJob, error) {
	return scanJob(q.QueryRowContext(ctx, `
		SELECT id, tenant_id, release_id, status, owner, lease_token,
		       lease_expires_at, attempt_count, max_attempts, checkpoint,
		       output_uri, last_error, next_attempt_at, created_at, updated_at, version
		FROM compute_jobs
		WHERE tenant_id = ? AND next_attempt_at <= ?
		  AND (status IN ('queued','retrying') OR (status = 'running' AND lease_expires_at <= ?))
		ORDER BY next_attempt_at ASC, created_at ASC, id ASC LIMIT 1`,
		tenantID, storage.FormatTime(now), storage.FormatTime(now)), "due")
}

func scanJob(row scanner, id string) (domain.SchedulerJob, error) {
	var job domain.SchedulerJob
	var leaseExpires sql.NullString
	var nextAttempt, created, updated string
	err := row.Scan(&job.ID, &job.TenantID, &job.ReleaseID, &job.Status,
		&job.Owner, &job.LeaseToken, &leaseExpires, &job.AttemptCount,
		&job.MaxAttempts, &job.Checkpoint, &job.OutputURI, &job.LastError,
		&nextAttempt, &created, &updated, &job.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.SchedulerJob{}, domain.NotFound("scheduler.find_job", "scheduler_job", id)
	}
	if err != nil {
		return domain.SchedulerJob{}, fmt.Errorf("scan scheduler job: %w", err)
	}
	var parseErr error
	if job.LeaseExpiresAt, parseErr = storage.ScanNullableTime(leaseExpires); parseErr != nil {
		return domain.SchedulerJob{}, fmt.Errorf("parse scheduler lease expiry: %w", parseErr)
	}
	if job.NextAttemptAt, parseErr = storage.ParseTime(nextAttempt); parseErr != nil {
		return domain.SchedulerJob{}, fmt.Errorf("parse scheduler next attempt: %w", parseErr)
	}
	if job.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
		return domain.SchedulerJob{}, fmt.Errorf("parse scheduler created_at: %w", parseErr)
	}
	if job.UpdatedAt, parseErr = storage.ParseTime(updated); parseErr != nil {
		return domain.SchedulerJob{}, fmt.Errorf("parse scheduler updated_at: %w", parseErr)
	}
	return job, nil
}

func (Repository) Claim(ctx context.Context, q storage.Queryer, job domain.SchedulerJob, workerID, token string, now, expiresAt time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE compute_jobs
		SET status = 'running', owner = ?, lease_token = ?, lease_expires_at = ?,
		    attempt_count = attempt_count + 1, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND version = ?
		  AND (status IN ('queued','retrying') OR (status = 'running' AND lease_expires_at <= ?))`,
		workerID, token, storage.FormatTime(expiresAt), storage.FormatTime(now),
		job.TenantID, job.ID, job.Version, storage.FormatTime(now))
	if err != nil {
		if storage.IsBusy(err) {
			return domain.Conflict("scheduler.claim", "scheduler_job", job.ID, "another worker is claiming the job")
		}
		return fmt.Errorf("claim scheduler job: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read scheduler claim result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("scheduler.claim", "scheduler_job", job.ID, "job is no longer available")
	}
	return nil
}

func (Repository) InsertAttempt(ctx context.Context, q storage.Queryer, attempt domain.JobAttempt) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO compute_attempts(
			id, tenant_id, job_id, attempt, worker_id, started_at,
			finished_at, outcome, error_text
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`, attempt.ID, attempt.TenantID,
		attempt.JobID, attempt.Attempt, attempt.WorkerID, storage.FormatTime(attempt.StartedAt),
		storage.NullableTime(attempt.FinishedAt), attempt.Outcome, attempt.ErrorText)
	if err != nil {
		return fmt.Errorf("insert scheduler attempt: %w", err)
	}
	return nil
}

func (Repository) Renew(ctx context.Context, q storage.Queryer, tenantID, jobID, owner, token string, version int64, now, expiresAt time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE compute_jobs
		SET lease_expires_at = ?, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'running'
		  AND owner = ? AND lease_token = ? AND version = ? AND lease_expires_at > ?`,
		storage.FormatTime(expiresAt), storage.FormatTime(now), tenantID, jobID,
		owner, token, version, storage.FormatTime(now))
	if err != nil {
		return fmt.Errorf("renew scheduler job: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read scheduler renewal result: %w", err)
	}
	if changed != 1 {
		return domain.Wrap(domain.ErrLeaseLost, "scheduler.renew", "scheduler_job", jobID, "lease expired, changed, or belongs to another worker", nil)
	}
	return nil
}

func (Repository) SaveCheckpoint(ctx context.Context, q storage.Queryer, tenantID, jobID, owner, token, checkpoint string, version int64, now time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE compute_jobs
		SET checkpoint = ?, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'running'
		  AND owner = ? AND lease_token = ? AND version = ? AND lease_expires_at > ?`,
		checkpoint, storage.FormatTime(now), tenantID, jobID, owner, token, version, storage.FormatTime(now))
	if err != nil {
		return fmt.Errorf("save scheduler checkpoint: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read scheduler checkpoint result: %w", err)
	}
	if changed != 1 {
		return domain.Wrap(domain.ErrLeaseLost, "scheduler.checkpoint", "scheduler_job", jobID, "active lease is required", nil)
	}
	return nil
}

func (Repository) Complete(ctx context.Context, q storage.Queryer, tenantID, jobID, owner, token, outputURI string, version int64, now time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE compute_jobs
		SET status = 'succeeded', owner = '', lease_token = '', lease_expires_at = NULL,
		    output_uri = ?, last_error = '', updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'running'
		  AND owner = ? AND lease_token = ? AND version = ? AND lease_expires_at > ?`,
		outputURI, storage.FormatTime(now), tenantID, jobID, owner, token, version, storage.FormatTime(now))
	if err != nil {
		return fmt.Errorf("complete scheduler job: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read scheduler completion result: %w", err)
	}
	if changed != 1 {
		return domain.Wrap(domain.ErrLeaseLost, "scheduler.complete", "scheduler_job", jobID, "active lease is required", nil)
	}
	return nil
}

func (Repository) Fail(ctx context.Context, q storage.Queryer, job domain.SchedulerJob, owner, token, message string, retry bool, nextAttempt, now time.Time) error {
	status := domain.JobFailed
	if retry {
		status = domain.JobRetrying
	}
	result, err := q.ExecContext(ctx, `
		UPDATE compute_jobs
		SET status = ?, owner = '', lease_token = '', lease_expires_at = NULL,
		    last_error = ?, next_attempt_at = ?, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'running'
		  AND owner = ? AND lease_token = ? AND version = ?`,
		status, message, storage.FormatTime(nextAttempt), storage.FormatTime(now),
		job.TenantID, job.ID, owner, token, job.Version)
	if err != nil {
		return fmt.Errorf("fail scheduler job: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read scheduler failure result: %w", err)
	}
	if changed != 1 {
		return domain.Wrap(domain.ErrLeaseLost, "scheduler.fail", "scheduler_job", job.ID, "active lease is required", nil)
	}
	return nil
}

func (Repository) FinishAttempt(ctx context.Context, q storage.Queryer, tenantID, jobID string, attempt int, outcome, errorText string, now time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE compute_attempts SET finished_at = ?, outcome = ?, error_text = ?
		WHERE tenant_id = ? AND job_id = ? AND attempt = ? AND finished_at IS NULL`,
		storage.FormatTime(now), outcome, errorText, tenantID, jobID, attempt)
	if err != nil {
		return fmt.Errorf("finish scheduler attempt: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read attempt finish result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("scheduler.finish_attempt", "job_attempt", jobID, "attempt was already finished or does not exist")
	}
	return nil
}

type scanner interface {
	Scan(...any) error
}
