package recovery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/audit"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

type Service struct {
	db     *storage.Database
	audits audit.Store
	clock  clock.Clock
}

type Result struct {
	PoolLeases      int64 `json:"leases"`
	MeteringBatches int64 `json:"usage_batches"`
	SchedulerJobs   int64 `json:"compute_jobs"`
	OutboxEvents    int64 `json:"outbox_events"`
}

func NewService(db *storage.Database, c clock.Clock) *Service {
	return &Service{db: db, audits: audit.Store{}, clock: c}
}

func (s *Service) RecoverExpired(ctx context.Context, tenantID, actorID, requestID string) (Result, error) {
	if tenantID == "" || actorID == "" || requestID == "" {
		return Result{}, domain.Validation("recovery.expired", "tenant, actor, and request id are required")
	}
	now := s.clock.Now()
	var result Result
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		var err error
		if result.PoolLeases, err = releasePoolLeases(ctx, tx, tenantID, now); err != nil {
			return err
		}
		if result.MeteringBatches, err = reopenMeteringBatches(ctx, tx, tenantID, now); err != nil {
			return err
		}
		if result.SchedulerJobs, err = recoverSchedulerJobs(ctx, tx, tenantID, now); err != nil {
			return err
		}
		if result.OutboxEvents, err = recoverOutboxEvents(ctx, tx, tenantID, now); err != nil {
			return err
		}
		auditID, err := domain.NewID("audit")
		if err != nil {
			return err
		}
		detail, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("encode recovery result: %w", err)
		}
		return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: tenantID, ActorID: actorID, Action: "recovery.expired", ObjectType: "tenant", ObjectID: tenantID, Outcome: "recovered", RequestID: requestID, Detail: string(detail), CreatedAt: now})
	})
	if err != nil {
		return Result{}, fmt.Errorf("recover expired resources: %w", err)
	}
	return result, nil
}

func releasePoolLeases(ctx context.Context, tx *sql.Tx, tenantID string, now time.Time) (int64, error) {
	result, err := tx.ExecContext(ctx, `
		UPDATE leases
		SET released_at = ?, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND released_at IS NULL AND expires_at <= ?`,
		storage.FormatTime(now), storage.FormatTime(now), tenantID, storage.FormatTime(now))
	if err != nil {
		return 0, fmt.Errorf("recover pool leases: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read pool recovery result: %w", err)
	}
	return changed, nil
}

func reopenMeteringBatches(ctx context.Context, tx *sql.Tx, tenantID string, now time.Time) (int64, error) {
	result, err := tx.ExecContext(ctx, `
		UPDATE usage_batches
		SET status = 'open', owner = '', lease_token = '', lease_expires_at = NULL,
		    updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND status = 'claimed' AND lease_expires_at <= ?`,
		storage.FormatTime(now), tenantID, storage.FormatTime(now))
	if err != nil {
		return 0, fmt.Errorf("recover metering batches: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read metering recovery result: %w", err)
	}
	return changed, nil
}

func recoverSchedulerJobs(ctx context.Context, tx *sql.Tx, tenantID string, now time.Time) (int64, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, attempt_count, max_attempts
		FROM compute_jobs
		WHERE tenant_id = ? AND status = 'running' AND lease_expires_at <= ?
		ORDER BY id ASC`, tenantID, storage.FormatTime(now))
	if err != nil {
		return 0, fmt.Errorf("list expired scheduler jobs: %w", err)
	}
	type expiredJob struct {
		id          string
		attempts    int
		maxAttempts int
	}
	jobs := make([]expiredJob, 0)
	for rows.Next() {
		var job expiredJob
		if err := rows.Scan(&job.id, &job.attempts, &job.maxAttempts); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan expired scheduler job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close expired scheduler jobs: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate expired scheduler jobs: %w", err)
	}
	var changed int64
	for _, job := range jobs {
		status := domain.JobRetrying
		if job.attempts >= job.maxAttempts {
			status = domain.JobFailed
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE compute_jobs
			SET status = ?, owner = '', lease_token = '', lease_expires_at = NULL,
			    last_error = CASE WHEN ? = 'failed' THEN 'worker lease expired after final attempt' ELSE 'worker lease expired' END,
			    next_attempt_at = ?, updated_at = ?, version = version + 1
			WHERE tenant_id = ? AND id = ? AND status = 'running' AND lease_expires_at <= ?`,
			status, status, storage.FormatTime(now), storage.FormatTime(now), tenantID, job.id, storage.FormatTime(now))
		if err != nil {
			return 0, fmt.Errorf("recover scheduler job %s: %w", job.id, err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("read scheduler job recovery %s: %w", job.id, err)
		}
		changed += count
	}
	return changed, nil
}

func recoverOutboxEvents(ctx context.Context, tx *sql.Tx, tenantID string, now time.Time) (int64, error) {
	result, err := tx.ExecContext(ctx, `
		UPDATE outbox_events
		SET status = CASE WHEN attempt_count >= max_attempts THEN 'dead' ELSE 'pending' END,
		    owner = '', lease_token = '', lease_expires_at = NULL,
		    last_error = CASE WHEN attempt_count >= max_attempts
		        THEN 'delivery lease expired after final attempt'
		        ELSE 'delivery lease expired' END,
		    next_attempt_at = ?, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND status = 'delivering' AND lease_expires_at <= ?`,
		storage.FormatTime(now), storage.FormatTime(now), tenantID, storage.FormatTime(now))
	if err != nil {
		return 0, fmt.Errorf("recover outbox events: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read outbox recovery result: %w", err)
	}
	return changed, nil
}
