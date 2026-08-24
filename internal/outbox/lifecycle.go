package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

func (Repository) Claim(ctx context.Context, q storage.Queryer, event domain.OutboxEvent, owner, token string, now, expiresAt time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE outbox_events
		SET status = 'delivering', owner = ?, lease_token = ?, lease_expires_at = ?,
		    attempt_count = attempt_count + 1, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND version = ? AND next_attempt_at <= ?
		  AND (status = 'pending' OR (status = 'delivering' AND lease_expires_at <= ?))`,
		owner, token, storage.FormatTime(expiresAt), storage.FormatTime(now),
		event.TenantID, event.ID, event.Version, storage.FormatTime(now), storage.FormatTime(now))
	if err != nil {
		return fmt.Errorf("claim outbox event: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox claim result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("outbox.claim", "outbox_event", event.ID, "event changed or has an active owner")
	}
	return nil
}

func (Repository) Renew(ctx context.Context, q storage.Queryer, tenantID, eventID, owner, token string, version int64, now, expiresAt time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE outbox_events
		SET lease_expires_at = ?, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'delivering'
		  AND owner = ? AND lease_token = ? AND version = ? AND lease_expires_at > ?`,
		storage.FormatTime(expiresAt), storage.FormatTime(now), tenantID, eventID,
		owner, token, version, storage.FormatTime(now))
	if err != nil {
		return fmt.Errorf("renew outbox event: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox renewal result: %w", err)
	}
	if changed != 1 {
		return domain.Wrap(domain.ErrLeaseLost, "outbox.renew", "outbox_event", eventID, "lease expired, changed, or belongs to another worker", nil)
	}
	return nil
}

func (Repository) Acknowledge(ctx context.Context, q storage.Queryer, event domain.OutboxEvent, owner, token string, now time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE outbox_events
		SET status = 'delivered', owner = '', lease_token = '', lease_expires_at = NULL,
		    last_error = '', updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'delivering'`,
		storage.FormatTime(now), event.TenantID, event.ID)
	if err != nil {
		return fmt.Errorf("acknowledge outbox event: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox acknowledgement result: %w", err)
	}
	if changed != 1 {
		return domain.Wrap(domain.ErrLeaseLost, "outbox.acknowledge", "outbox_event", event.ID, "active lease is required", nil)
	}
	return nil
}

func (Repository) Fail(ctx context.Context, q storage.Queryer, event domain.OutboxEvent, owner, token, message string, retry bool, nextAttempt, now time.Time) error {
	status := domain.OutboxDead
	if retry {
		status = domain.OutboxPending
	}
	result, err := q.ExecContext(ctx, `
		UPDATE outbox_events
		SET status = ?, owner = '', lease_token = '', lease_expires_at = NULL,
		    last_error = ?, next_attempt_at = ?, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'delivering'
		  AND owner = ? AND lease_token = ? AND version = ?`,
		status, message, storage.FormatTime(nextAttempt), storage.FormatTime(now),
		event.TenantID, event.ID, owner, token, event.Version)
	if err != nil {
		return fmt.Errorf("fail outbox event: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox failure result: %w", err)
	}
	if changed != 1 {
		return domain.Wrap(domain.ErrLeaseLost, "outbox.fail", "outbox_event", event.ID, "active lease is required", nil)
	}
	return nil
}

func (Repository) CountBlocking(ctx context.Context, q storage.Queryer, tenantID, aggregateType, aggregateID string) (int, error) {
	var count int
	err := q.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM outbox_events
		WHERE tenant_id = ? AND aggregate_type = ? AND aggregate_id = ?
		  AND status IN ('pending','delivering')`, tenantID, aggregateType, aggregateID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count blocking outbox events: %w", err)
	}
	return count, nil
}
