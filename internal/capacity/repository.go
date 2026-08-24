package capacity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

type Repository struct{}

func (Repository) InsertStream(ctx context.Context, q storage.Queryer, value domain.CapacityStream) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO capacity_streams(
			id, tenant_id, workload_id, kind, status, segment_count,
			first_nanos, last_nanos, digest, created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		value.ID, value.TenantID, value.WorkloadID, value.Kind, value.Status,
		value.SegmentCount, value.FirstNanos, value.LastNanos, value.Digest,
		storage.FormatTime(value.CreatedAt), storage.FormatTime(value.UpdatedAt), value.Version)
	if err != nil {
		return fmt.Errorf("insert capacity manifest: %w", err)
	}
	return nil
}

func (Repository) FindStream(ctx context.Context, q storage.Queryer, tenantID, id string) (domain.CapacityStream, error) {
	return scanStream(q.QueryRowContext(ctx, `
		SELECT id, tenant_id, workload_id, kind, status, segment_count,
		       first_nanos, last_nanos, digest, created_at, updated_at, version
		FROM capacity_streams WHERE tenant_id = ? AND id = ?`, tenantID, id), id)
}

func (Repository) FindStreamByKind(ctx context.Context, q storage.Queryer, tenantID, workloadID string, kind domain.CapacityKind) (domain.CapacityStream, error) {
	return scanStream(q.QueryRowContext(ctx, `
		SELECT id, tenant_id, workload_id, kind, status, segment_count,
		       first_nanos, last_nanos, digest, created_at, updated_at, version
		FROM capacity_streams WHERE tenant_id = ? AND workload_id = ? AND kind = ?`, tenantID, workloadID, kind), workloadID+":"+string(kind))
}

func (Repository) ListStreams(ctx context.Context, q storage.Queryer, tenantID, workloadID string) ([]domain.CapacityStream, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT id, tenant_id, workload_id, kind, status, segment_count,
		       first_nanos, last_nanos, digest, created_at, updated_at, version
		FROM capacity_streams WHERE tenant_id = ? AND workload_id = ?
		ORDER BY kind ASC`, tenantID, workloadID)
	if err != nil {
		return nil, fmt.Errorf("list capacity manifests: %w", err)
	}
	defer rows.Close()
	values := make([]domain.CapacityStream, 0)
	for rows.Next() {
		value, err := scanStream(rows, "")
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate capacity manifests: %w", err)
	}
	return values, nil
}

func scanStream(row scanner, id string) (domain.CapacityStream, error) {
	var value domain.CapacityStream
	var created, updated string
	err := row.Scan(&value.ID, &value.TenantID, &value.WorkloadID, &value.Kind,
		&value.Status, &value.SegmentCount, &value.FirstNanos, &value.LastNanos,
		&value.Digest, &created, &updated, &value.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.CapacityStream{}, domain.NotFound("capacity.find_manifest", "capacity_stream", id)
	}
	if err != nil {
		return domain.CapacityStream{}, fmt.Errorf("scan capacity manifest: %w", err)
	}
	var parseErr error
	if value.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
		return domain.CapacityStream{}, fmt.Errorf("parse manifest created_at: %w", parseErr)
	}
	if value.UpdatedAt, parseErr = storage.ParseTime(updated); parseErr != nil {
		return domain.CapacityStream{}, fmt.Errorf("parse manifest updated_at: %w", parseErr)
	}
	return value, nil
}

func (Repository) FindSegmentByKey(ctx context.Context, q storage.Queryer, tenantID, streamID, key string) (domain.CapacitySegment, bool, error) {
	var value domain.CapacitySegment
	var created string
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, stream_id, sequence, start_nanos, end_nanos,
		       object_uri, checksum, idempotency_key, created_at
		FROM usage_records
		WHERE tenant_id = ? AND stream_id = ? AND idempotency_key = ?`, tenantID, streamID, key,
	).Scan(&value.ID, &value.TenantID, &value.StreamID, &value.Sequence,
		&value.StartNanos, &value.EndNanos, &value.ObjectURI, &value.Checksum,
		&value.IdempotencyKey, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.CapacitySegment{}, false, nil
	}
	if err != nil {
		return domain.CapacitySegment{}, false, fmt.Errorf("find capacity segment by key: %w", err)
	}
	value.CreatedAt, err = storage.ParseTime(created)
	if err != nil {
		return domain.CapacitySegment{}, false, fmt.Errorf("parse segment created_at: %w", err)
	}
	return value, true, nil
}

func (Repository) InsertSegment(ctx context.Context, q storage.Queryer, value domain.CapacitySegment) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO usage_records(
			id, tenant_id, stream_id, sequence, start_nanos, end_nanos,
			object_uri, checksum, idempotency_key, created_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, value.ID, value.TenantID,
		value.StreamID, value.Sequence, value.StartNanos, value.EndNanos,
		value.ObjectURI, value.Checksum, value.IdempotencyKey, storage.FormatTime(value.CreatedAt))
	if err != nil {
		return fmt.Errorf("insert capacity segment: %w", err)
	}
	return nil
}

func (Repository) ListSegments(ctx context.Context, q storage.Queryer, tenantID, streamID string) ([]domain.CapacitySegment, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT id, tenant_id, stream_id, sequence, start_nanos, end_nanos,
		       object_uri, checksum, idempotency_key, created_at
		FROM usage_records WHERE tenant_id = ? AND stream_id = ?
		ORDER BY sequence ASC, id ASC`, tenantID, streamID)
	if err != nil {
		return nil, fmt.Errorf("list capacity segments: %w", err)
	}
	defer rows.Close()
	values := make([]domain.CapacitySegment, 0)
	for rows.Next() {
		var value domain.CapacitySegment
		var created string
		if err := rows.Scan(&value.ID, &value.TenantID, &value.StreamID,
			&value.Sequence, &value.StartNanos, &value.EndNanos, &value.ObjectURI,
			&value.Checksum, &value.IdempotencyKey, &created); err != nil {
			return nil, fmt.Errorf("scan capacity segment: %w", err)
		}
		parsed, err := storage.ParseTime(created)
		if err != nil {
			return nil, fmt.Errorf("parse segment created_at: %w", err)
		}
		value.CreatedAt = parsed
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate capacity segments: %w", err)
	}
	sort.SliceStable(values, func(i, j int) bool {
		if values[i].Sequence == values[j].Sequence {
			return values[i].ID < values[j].ID
		}
		return values[i].Sequence < values[j].Sequence
	})
	return values, nil
}

func (Repository) RefreshAggregate(ctx context.Context, q storage.Queryer, tenantID, streamID string, expectedVersion int64, now string) error {
	result, err := q.ExecContext(ctx, `
		UPDATE capacity_streams
		SET segment_count = (
		        SELECT COUNT(*) FROM usage_records
		        WHERE tenant_id = ? AND stream_id = ?
		    ),
		    first_nanos = COALESCE((
		        SELECT MIN(start_nanos) FROM usage_records
		        WHERE tenant_id = ? AND stream_id = ?
		    ), 0),
		    last_nanos = COALESCE((
		        SELECT MAX(end_nanos) FROM usage_records
		        WHERE tenant_id = ? AND stream_id = ?
		    ), 0),
		    updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'open' AND version = ?`,
		tenantID, streamID, tenantID, streamID, tenantID, streamID,
		now, tenantID, streamID, expectedVersion)
	if err != nil {
		return fmt.Errorf("refresh capacity manifest aggregate: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read manifest aggregate result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("capacity.refresh_manifest", "capacity_stream", streamID, "manifest changed or was sealed")
	}
	return nil
}

func (Repository) TransitionStream(ctx context.Context, q storage.Queryer, tenantID, id string, from, to domain.StreamStatus, version int64, digest, now string) error {
	result, err := q.ExecContext(ctx, `
		UPDATE capacity_streams
		SET status = ?, digest = CASE WHEN ? = '' THEN digest ELSE ? END,
		    updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = ? AND version = ?`,
		to, digest, digest, now, tenantID, id, from, version)
	if err != nil {
		return fmt.Errorf("transition capacity manifest: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read manifest transition result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("capacity.transition_manifest", "capacity_stream", id, "state or version changed")
	}
	return nil
}

type scanner interface {
	Scan(...any) error
}
