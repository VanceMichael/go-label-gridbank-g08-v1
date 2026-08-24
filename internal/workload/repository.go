package workload

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

func (Repository) Insert(ctx context.Context, q storage.Queryer, value domain.WorkloadSession) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO workloads(
			id, tenant_id, provider_id, capacity_offer_id, pool_id, operator_id,
			status, revision, reservation_ref, started_at, submitted_at,
			settled_at, canceled_at, created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		value.ID, value.TenantID, value.ProviderID, value.CapacityOfferID, value.PoolID,
		value.OperatorID, value.Status, value.Revision, value.ReservationRef,
		storage.NullableTime(value.StartedAt), storage.NullableTime(value.SubmittedAt),
		storage.NullableTime(value.SettledAt), storage.NullableTime(value.CanceledAt),
		storage.FormatTime(value.CreatedAt), storage.FormatTime(value.UpdatedAt), value.Version)
	if err != nil {
		return fmt.Errorf("insert workload session: %w", err)
	}
	return nil
}

func (Repository) Find(ctx context.Context, q storage.Queryer, tenantID, id string) (domain.WorkloadSession, error) {
	var value domain.WorkloadSession
	var started, submitted, settled, canceled sql.NullString
	var created, updated string
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, provider_id, capacity_offer_id, pool_id, operator_id,
		       status, revision, reservation_ref, started_at, submitted_at,
		       settled_at, canceled_at, created_at, updated_at, version
		FROM workloads WHERE tenant_id = ? AND id = ?`, tenantID, id,
	).Scan(&value.ID, &value.TenantID, &value.ProviderID, &value.CapacityOfferID,
		&value.PoolID, &value.OperatorID, &value.Status, &value.Revision,
		&value.ReservationRef, &started, &submitted, &settled, &canceled,
		&created, &updated, &value.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.WorkloadSession{}, domain.NotFound("workload.find", "workload_session", id)
	}
	if err != nil {
		return domain.WorkloadSession{}, fmt.Errorf("find workload session: %w", err)
	}
	var parseErr error
	if value.StartedAt, parseErr = storage.ScanNullableTime(started); parseErr != nil {
		return domain.WorkloadSession{}, fmt.Errorf("parse workload started_at: %w", parseErr)
	}
	if value.SubmittedAt, parseErr = storage.ScanNullableTime(submitted); parseErr != nil {
		return domain.WorkloadSession{}, fmt.Errorf("parse workload submitted_at: %w", parseErr)
	}
	if value.SettledAt, parseErr = storage.ScanNullableTime(settled); parseErr != nil {
		return domain.WorkloadSession{}, fmt.Errorf("parse workload settled_at: %w", parseErr)
	}
	if value.CanceledAt, parseErr = storage.ScanNullableTime(canceled); parseErr != nil {
		return domain.WorkloadSession{}, fmt.Errorf("parse workload canceled_at: %w", parseErr)
	}
	if value.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
		return domain.WorkloadSession{}, fmt.Errorf("parse workload created_at: %w", parseErr)
	}
	if value.UpdatedAt, parseErr = storage.ParseTime(updated); parseErr != nil {
		return domain.WorkloadSession{}, fmt.Errorf("parse workload updated_at: %w", parseErr)
	}
	return value, nil
}

type TransitionTimes struct {
	StartedAt   *time.Time
	SubmittedAt *time.Time
	SettledAt   *time.Time
	CanceledAt  *time.Time
}

func (Repository) Transition(ctx context.Context, q storage.Queryer, tenantID, id string, from, to domain.JobWorkloadStatus, version int64, now time.Time, times TransitionTimes) error {
	result, err := q.ExecContext(ctx, `
		UPDATE workloads
		SET status = ?, started_at = COALESCE(?, started_at),
		    submitted_at = COALESCE(?, submitted_at),
		    settled_at = COALESCE(?, settled_at),
		    canceled_at = COALESCE(?, canceled_at),
		    updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = ? AND version = ?`,
		to, storage.NullableTime(times.StartedAt), storage.NullableTime(times.SubmittedAt),
		storage.NullableTime(times.SettledAt), storage.NullableTime(times.CanceledAt),
		storage.FormatTime(now), tenantID, id, from, version)
	if err != nil {
		return fmt.Errorf("transition workload session: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read workload transition result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("job_workload.transition", "workload_session", id, "state or version changed")
	}
	return nil
}

func (Repository) Reopen(ctx context.Context, q storage.Queryer, tenantID, id string, version int64, now time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE workloads
		SET status = 'ready', revision = revision + 1,
		    started_at = NULL, submitted_at = NULL, settled_at = NULL,
		    updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'failed' AND version = ?`,
		storage.FormatTime(now), tenantID, id, version)
	if err != nil {
		return fmt.Errorf("reopen workload session: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read reopen result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("workload.reopen", "workload_session", id, "state or version changed")
	}
	return nil
}

func (Repository) CountUnalignedStreams(ctx context.Context, q storage.Queryer, tenantID, workloadID string) (int, error) {
	var count int
	err := q.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM capacity_streams
		WHERE tenant_id = ? AND workload_id = ? AND status <> 'aligned'`, tenantID, workloadID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count unaligned manifests: %w", err)
	}
	return count, nil
}

func (Repository) CountStreams(ctx context.Context, q storage.Queryer, tenantID, workloadID string) (int, error) {
	var count int
	err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM capacity_streams WHERE tenant_id = ? AND workload_id = ?`, tenantID, workloadID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count workload manifests: %w", err)
	}
	return count, nil
}
