package outbox

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

func (Repository) Enqueue(ctx context.Context, q storage.Queryer, event domain.OutboxEvent) error {
	if event.ID == "" || event.TenantID == "" || event.Topic == "" || event.AggregateID == "" || event.Payload == "" {
		return domain.Validation("outbox.enqueue", "event identity, topic, aggregate, and payload are required")
	}
	_, err := q.ExecContext(ctx, `
		INSERT INTO outbox_events(
			id, tenant_id, topic, aggregate_type, aggregate_id, payload,
			status, owner, lease_token, lease_expires_at, attempt_count,
			max_attempts, next_attempt_at, last_error, created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.ID, event.TenantID, event.Topic, event.AggregateType, event.AggregateID,
		event.Payload, event.Status, event.Owner, event.LeaseToken,
		storage.NullableTime(event.LeaseExpiresAt), event.AttemptCount, event.MaxAttempts,
		storage.FormatTime(event.NextAttemptAt), event.LastError,
		storage.FormatTime(event.CreatedAt), storage.FormatTime(event.UpdatedAt), event.Version)
	if err != nil {
		return fmt.Errorf("enqueue outbox event: %w", err)
	}
	return nil
}

func (Repository) Find(ctx context.Context, q storage.Queryer, tenantID, id string) (domain.OutboxEvent, error) {
	return scanEvent(q.QueryRowContext(ctx, `
		SELECT id, tenant_id, topic, aggregate_type, aggregate_id, payload,
		       status, owner, lease_token, lease_expires_at, attempt_count,
		       max_attempts, next_attempt_at, last_error, created_at, updated_at, version
		FROM outbox_events WHERE tenant_id = ? AND id = ?`, tenantID, id), id)
}

func (Repository) ListDue(ctx context.Context, q storage.Queryer, tenantID string, now time.Time, limit int) ([]domain.OutboxEvent, error) {
	if limit < 1 || limit > 500 {
		return nil, domain.Validation("outbox.list_due", "limit must be between 1 and 500")
	}
	rows, err := q.QueryContext(ctx, `
		SELECT id, tenant_id, topic, aggregate_type, aggregate_id, payload,
		       status, owner, lease_token, lease_expires_at, attempt_count,
		       max_attempts, next_attempt_at, last_error, created_at, updated_at, version
		FROM outbox_events
		WHERE tenant_id = ?
		  AND status IN ('pending','delivering')
		  AND next_attempt_at <= ?
		  AND (status = 'pending' OR lease_expires_at <= ?)
		ORDER BY next_attempt_at ASC, created_at ASC, id ASC
		LIMIT ?`, tenantID, storage.FormatTime(now), storage.FormatTime(now), limit)
	if err != nil {
		return nil, fmt.Errorf("list due outbox events: %w", err)
	}
	defer rows.Close()
	values := make([]domain.OutboxEvent, 0)
	for rows.Next() {
		value, err := scanEvent(rows, "")
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due outbox events: %w", err)
	}
	return values, nil
}

type scanner interface {
	Scan(...any) error
}

func scanEvent(row scanner, id string) (domain.OutboxEvent, error) {
	var value domain.OutboxEvent
	var leaseExpires sql.NullString
	var nextAttempt, created, updated string
	err := row.Scan(
		&value.ID, &value.TenantID, &value.Topic, &value.AggregateType,
		&value.AggregateID, &value.Payload, &value.Status, &value.Owner,
		&value.LeaseToken, &leaseExpires, &value.AttemptCount, &value.MaxAttempts,
		&nextAttempt, &value.LastError, &created, &updated, &value.Version,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.OutboxEvent{}, domain.NotFound("outbox.find", "outbox_event", id)
	}
	if err != nil {
		return domain.OutboxEvent{}, fmt.Errorf("scan outbox event: %w", err)
	}
	var parseErr error
	if value.LeaseExpiresAt, parseErr = storage.ScanNullableTime(leaseExpires); parseErr != nil {
		return domain.OutboxEvent{}, fmt.Errorf("parse outbox lease expiry: %w", parseErr)
	}
	if value.NextAttemptAt, parseErr = storage.ParseTime(nextAttempt); parseErr != nil {
		return domain.OutboxEvent{}, fmt.Errorf("parse outbox next attempt: %w", parseErr)
	}
	if value.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
		return domain.OutboxEvent{}, fmt.Errorf("parse outbox created_at: %w", parseErr)
	}
	if value.UpdatedAt, parseErr = storage.ParseTime(updated); parseErr != nil {
		return domain.OutboxEvent{}, fmt.Errorf("parse outbox updated_at: %w", parseErr)
	}
	return value, nil
}
