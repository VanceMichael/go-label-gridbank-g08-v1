package idempotency

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

type Store struct{}

func (Store) Lookup(ctx context.Context, q storage.Queryer, tenantID, method, path, key string, now time.Time) (domain.IdempotencyRecord, bool, error) {
	var record domain.IdempotencyRecord
	var created, expires string
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, method, path, key, fingerprint, status_code,
		       response, created_at, expires_at
		FROM idempotency_records
		WHERE tenant_id = ? AND method = ? AND path = ? AND key = ?`,
		tenantID, strings.ToUpper(method), path, key,
	).Scan(
		&record.ID, &record.TenantID, &record.Method, &record.Path, &record.Key,
		&record.Fingerprint, &record.StatusCode, &record.Response, &created, &expires,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.IdempotencyRecord{}, false, nil
	}
	if err != nil {
		return domain.IdempotencyRecord{}, false, fmt.Errorf("lookup idempotency record: %w", err)
	}
	var parseErr error
	if record.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
		return domain.IdempotencyRecord{}, false, fmt.Errorf("parse idempotency created_at: %w", parseErr)
	}
	if record.ExpiresAt, parseErr = storage.ParseTime(expires); parseErr != nil {
		return domain.IdempotencyRecord{}, false, fmt.Errorf("parse idempotency expires_at: %w", parseErr)
	}
	if !now.Before(record.ExpiresAt) {
		if _, err := q.ExecContext(ctx, `
			DELETE FROM idempotency_records
			WHERE id = ? AND tenant_id = ? AND expires_at <= ?`,
			record.ID, tenantID, storage.FormatTime(now),
		); err != nil {
			return domain.IdempotencyRecord{}, false, fmt.Errorf("delete expired idempotency record: %w", err)
		}
		return domain.IdempotencyRecord{}, false, nil
	}
	return record, true, nil
}

func (Store) Save(ctx context.Context, q storage.Queryer, record domain.IdempotencyRecord) error {
	if err := validate(record); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, `
		INSERT INTO idempotency_records(
			id, tenant_id, method, path, key, fingerprint, status_code,
			response, created_at, expires_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID, record.TenantID, strings.ToUpper(record.Method), record.Path,
		record.Key, record.Fingerprint, record.StatusCode, record.Response,
		storage.FormatTime(record.CreatedAt), storage.FormatTime(record.ExpiresAt),
	)
	if err != nil {
		return fmt.Errorf("save idempotency record: %w", err)
	}
	return nil
}

func EnsureFingerprint(record domain.IdempotencyRecord, fingerprint string) error {
	if record.Fingerprint != fingerprint {
		return domain.Wrap(
			domain.ErrIdempotencyConflict,
			"idempotency.replay", "idempotency_key", record.Key,
			"key was already used with a different request", nil,
		)
	}
	return nil
}

func validate(record domain.IdempotencyRecord) error {
	if record.ID == "" || record.TenantID == "" || record.Method == "" || record.Path == "" || record.Key == "" {
		return domain.Validation("idempotency.save", "identity, scope, and key are required")
	}
	if record.Fingerprint == "" {
		return domain.Validation("idempotency.save", "fingerprint is required")
	}
	if record.StatusCode < 100 || record.StatusCode > 599 {
		return domain.Validation("idempotency.save", "status code is invalid")
	}
	if len(record.Response) == 0 {
		return domain.Validation("idempotency.save", "response is required")
	}
	if record.CreatedAt.IsZero() || !record.ExpiresAt.After(record.CreatedAt) {
		return domain.Validation("idempotency.save", "expiry must follow creation")
	}
	return nil
}
