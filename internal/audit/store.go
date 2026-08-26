package audit

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

type Store struct{}

type Record struct {
	ID         string
	TenantID   string
	ActorID    string
	Action     string
	ObjectType string
	ObjectID   string
	Outcome    string
	RequestID  string
	Detail     string
	CreatedAt  time.Time
}

func (Store) Append(ctx context.Context, q storage.Queryer, record Record) error {
	if err := validate(record); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, `
		INSERT INTO audit_events(
			id, tenant_id, actor_id, action, object_type, object_id,
			outcome, request_id, detail, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID, record.TenantID, record.ActorID, record.Action,
		record.ObjectType, record.ObjectID, record.Outcome, record.RequestID,
		record.Detail, storage.FormatTime(record.CreatedAt),
	)
	if err != nil {
		return fmt.Errorf("append audit event: %w", err)
	}
	return nil
}

func (Store) ListObject(ctx context.Context, q storage.Queryer, tenantID, objectType, objectID string) ([]domain.AuditEvent, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT id, tenant_id, actor_id, action, object_type, object_id,
		       outcome, request_id, detail, created_at
		FROM audit_events
		WHERE tenant_id = ? AND object_type = ? AND object_id = ?
		ORDER BY created_at ASC, id ASC`, tenantID, objectType, objectID)
	if err != nil {
		return nil, fmt.Errorf("list audit events: %w", err)
	}
	defer rows.Close()
	events := make([]domain.AuditEvent, 0)
	for rows.Next() {
		var event domain.AuditEvent
		var created string
		if err := rows.Scan(
			&event.ID, &event.TenantID, &event.ActorID, &event.Action,
			&event.ObjectType, &event.ObjectID, &event.Outcome,
			&event.RequestID, &event.Detail, &created,
		); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		parsed, err := storage.ParseTime(created)
		if err != nil {
			return nil, fmt.Errorf("parse audit timestamp: %w", err)
		}
		event.CreatedAt = parsed
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit events: %w", err)
	}
	return events, nil
}

func validate(record Record) error {
	required := map[string]string{
		"id": record.ID, "tenant_id": record.TenantID, "actor_id": record.ActorID,
		"action": record.Action, "object_type": record.ObjectType,
		"object_id": record.ObjectID, "outcome": record.Outcome,
		"request_id": record.RequestID,
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			return domain.Validation("audit.append", field+" is required")
		}
	}
	if record.CreatedAt.IsZero() {
		return domain.Validation("audit.append", "created_at is required")
	}
	return nil
}
