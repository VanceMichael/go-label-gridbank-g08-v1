package metering

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

func (Repository) InsertBatch(ctx context.Context, q storage.Queryer, batch domain.MeteringBatch) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO usage_batches(
			id, tenant_id, workload_id, status, owner, lease_token,
			lease_expires_at, submitted_at, reviewed_at,
			created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		batch.ID, batch.TenantID, batch.WorkloadID, batch.Status, batch.Owner,
		batch.LeaseToken, storage.NullableTime(batch.LeaseExpiresAt),
		storage.NullableTime(batch.SubmittedAt), storage.NullableTime(batch.ReviewedAt),
		storage.FormatTime(batch.CreatedAt), storage.FormatTime(batch.UpdatedAt), batch.Version)
	if err != nil {
		return fmt.Errorf("insert metering batch: %w", err)
	}
	return nil
}

func (Repository) InsertItems(ctx context.Context, q storage.Queryer, items []domain.MeteringItem) error {
	stmt, err := prepareContext(ctx, q, `
		INSERT INTO usage_items(
			id, tenant_id, batch_id, segment_id, label, payload,
			complete, created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare metering item insert: %w", err)
	}
	defer stmt.Close()
	for _, item := range items {
		if _, err := stmt.ExecContext(ctx, item.ID, item.TenantID, item.BatchID,
			item.SegmentID, item.Label, item.Payload, item.Complete,
			storage.FormatTime(item.CreatedAt), storage.FormatTime(item.UpdatedAt), item.Version); err != nil {
			return fmt.Errorf("insert metering item %s: %w", item.ID, err)
		}
	}
	return nil
}

type preparer interface {
	PrepareContext(context.Context, string) (*sql.Stmt, error)
}

func prepareContext(ctx context.Context, q storage.Queryer, statement string) (*sql.Stmt, error) {
	value, ok := q.(preparer)
	if !ok {
		return nil, errors.New("query boundary does not support prepared statements")
	}
	return value.PrepareContext(ctx, statement)
}

func (Repository) FindBatch(ctx context.Context, q storage.Queryer, tenantID, id string) (domain.MeteringBatch, error) {
	var batch domain.MeteringBatch
	var leaseExpiry, submitted, reviewed sql.NullString
	var created, updated string
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, workload_id, status, owner, lease_token,
		       lease_expires_at, submitted_at, reviewed_at,
		       created_at, updated_at, version
		FROM usage_batches WHERE tenant_id = ? AND id = ?`, tenantID, id,
	).Scan(&batch.ID, &batch.TenantID, &batch.WorkloadID, &batch.Status,
		&batch.Owner, &batch.LeaseToken, &leaseExpiry, &submitted, &reviewed,
		&created, &updated, &batch.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MeteringBatch{}, domain.NotFound("metering.find_batch", "metering_batch", id)
	}
	if err != nil {
		return domain.MeteringBatch{}, fmt.Errorf("find metering batch: %w", err)
	}
	var parseErr error
	if batch.LeaseExpiresAt, parseErr = storage.ScanNullableTime(leaseExpiry); parseErr != nil {
		return domain.MeteringBatch{}, fmt.Errorf("parse metering lease expiry: %w", parseErr)
	}
	if batch.SubmittedAt, parseErr = storage.ScanNullableTime(submitted); parseErr != nil {
		return domain.MeteringBatch{}, fmt.Errorf("parse metering submitted_at: %w", parseErr)
	}
	if batch.ReviewedAt, parseErr = storage.ScanNullableTime(reviewed); parseErr != nil {
		return domain.MeteringBatch{}, fmt.Errorf("parse metering reviewed_at: %w", parseErr)
	}
	if batch.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
		return domain.MeteringBatch{}, fmt.Errorf("parse metering created_at: %w", parseErr)
	}
	if batch.UpdatedAt, parseErr = storage.ParseTime(updated); parseErr != nil {
		return domain.MeteringBatch{}, fmt.Errorf("parse metering updated_at: %w", parseErr)
	}
	return batch, nil
}

func (Repository) ListItems(ctx context.Context, q storage.Queryer, tenantID, batchID string) ([]domain.MeteringItem, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT id, tenant_id, batch_id, segment_id, label, payload,
		       complete, created_at, updated_at, version
		FROM usage_items
		WHERE tenant_id = ? AND batch_id = ?
		ORDER BY created_at ASC, id ASC`, tenantID, batchID)
	if err != nil {
		return nil, fmt.Errorf("list metering items: %w", err)
	}
	defer rows.Close()
	items := make([]domain.MeteringItem, 0)
	for rows.Next() {
		var item domain.MeteringItem
		var created, updated string
		if err := rows.Scan(&item.ID, &item.TenantID, &item.BatchID, &item.SegmentID,
			&item.Label, &item.Payload, &item.Complete, &created, &updated, &item.Version); err != nil {
			return nil, fmt.Errorf("scan metering item: %w", err)
		}
		var parseErr error
		if item.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
			return nil, fmt.Errorf("parse metering item created_at: %w", parseErr)
		}
		if item.UpdatedAt, parseErr = storage.ParseTime(updated); parseErr != nil {
			return nil, fmt.Errorf("parse metering item updated_at: %w", parseErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate metering items: %w", err)
	}
	return items, nil
}

func (Repository) Claim(ctx context.Context, q storage.Queryer, tenantID, batchID, owner, token string, version int64, now, expiresAt time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE usage_batches
		SET status = 'claimed', owner = ?, lease_token = ?, lease_expires_at = ?,
		    updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND version = ?
		  AND (status IN ('open','rework') OR (status = 'claimed' AND lease_expires_at <= ?))`,
		owner, token, storage.FormatTime(expiresAt), storage.FormatTime(now),
		tenantID, batchID, version, storage.FormatTime(now))
	if err != nil {
		if storage.IsBusy(err) {
			return domain.Conflict("metering.claim", "metering_batch", batchID, "another reviewer is claiming the batch")
		}
		return fmt.Errorf("claim metering batch: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read metering claim result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("metering.claim", "metering_batch", batchID, "batch changed or has an active owner")
	}
	return nil
}

func (Repository) Renew(ctx context.Context, q storage.Queryer, tenantID, batchID, owner, token string, version int64, now, expiresAt time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE usage_batches
		SET lease_expires_at = ?, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'claimed'
		  AND owner = ? AND lease_token = ? AND version = ? AND lease_expires_at > ?`,
		storage.FormatTime(expiresAt), storage.FormatTime(now), tenantID, batchID,
		owner, token, version, storage.FormatTime(now))
	if err != nil {
		return fmt.Errorf("renew metering batch: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read metering renewal result: %w", err)
	}
	if changed != 1 {
		return domain.Wrap(domain.ErrLeaseLost, "metering.renew", "metering_batch", batchID, "claim expired, changed, or belongs to another reviewer", nil)
	}
	return nil
}

func (Repository) UpdateItem(ctx context.Context, q storage.Queryer, tenantID, batchID, itemID, label, payload string, version int64, now time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE usage_items
		SET label = ?, payload = ?, complete = 1, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND batch_id = ? AND id = ? AND version = ?`,
		label, payload, storage.FormatTime(now), tenantID, batchID, itemID, version)
	if err != nil {
		return fmt.Errorf("update metering item: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read metering item update result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("metering.update_item", "metering_item", itemID, "item changed or belongs to another batch")
	}
	return nil
}

func (Repository) CountIncomplete(ctx context.Context, q storage.Queryer, tenantID, batchID string) (int, error) {
	var count int
	err := q.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM usage_items
		WHERE tenant_id = ? AND batch_id = ? AND complete = 0`, tenantID, batchID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count incomplete metering items: %w", err)
	}
	return count, nil
}

func (Repository) Submit(ctx context.Context, q storage.Queryer, tenantID, batchID, owner, token string, version int64, now time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE usage_batches
		SET status = 'submitted', owner = '', lease_token = '', lease_expires_at = NULL,
		    submitted_at = ?, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'claimed'
		  AND owner = ? AND lease_token = ? AND version = ? AND lease_expires_at > ?`,
		storage.FormatTime(now), storage.FormatTime(now), tenantID, batchID,
		owner, token, version, storage.FormatTime(now))
	if err != nil {
		return fmt.Errorf("submit metering batch: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read metering submission result: %w", err)
	}
	if changed != 1 {
		return domain.Wrap(domain.ErrLeaseLost, "metering.submit", "metering_batch", batchID, "claim expired, changed, or belongs to another reviewer", nil)
	}
	return nil
}

func (Repository) Review(ctx context.Context, q storage.Queryer, tenantID, batchID string, version int64, accept bool, now time.Time) error {
	status := domain.MeteringRework
	if accept {
		status = domain.MeteringAccepted
	}
	result, err := q.ExecContext(ctx, `
		UPDATE usage_batches
		SET status = ?, reviewed_at = ?, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'submitted' AND version = ?`,
		status, storage.FormatTime(now), storage.FormatTime(now), tenantID, batchID, version)
	if err != nil {
		return fmt.Errorf("review metering batch: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read metering review result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("metering.review", "metering_batch", batchID, "state or version changed")
	}
	if !accept {
		if _, err := q.ExecContext(ctx, `
			UPDATE usage_items SET complete = 0, updated_at = ?, version = version + 1
			WHERE tenant_id = ? AND batch_id = ?`, storage.FormatTime(now), tenantID, batchID); err != nil {
			return fmt.Errorf("reset metering items for rework: %w", err)
		}
	}
	return nil
}
