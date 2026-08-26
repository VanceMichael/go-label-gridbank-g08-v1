package ledger

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

type Repository struct{}

type ListFilter struct {
	Statuses []domain.LedgerStatus
	Search   string
	Limit    int
	Offset   int
}

func (Repository) InsertDraft(ctx context.Context, q storage.Queryer, value domain.LedgerDraft) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO credit_plans(
			id, tenant_id, name, status, revision, digest, item_count,
			frozen_at, created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, value.ID, value.TenantID,
		value.Name, value.Status, value.Revision, value.Digest, value.ItemCount,
		storage.NullableTime(value.FrozenAt), storage.FormatTime(value.CreatedAt),
		storage.FormatTime(value.UpdatedAt), value.Version)
	if err != nil {
		return fmt.Errorf("insert ledger draft: %w", err)
	}
	return nil
}

func (Repository) FindDraft(ctx context.Context, q storage.Queryer, tenantID, id string) (domain.LedgerDraft, error) {
	var value domain.LedgerDraft
	var frozen sql.NullString
	var created, updated string
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, name, status, revision, digest, item_count,
		       frozen_at, created_at, updated_at, version
		FROM credit_plans WHERE tenant_id = ? AND id = ?`, tenantID, id,
	).Scan(&value.ID, &value.TenantID, &value.Name, &value.Status, &value.Revision,
		&value.Digest, &value.ItemCount, &frozen, &created, &updated, &value.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.LedgerDraft{}, domain.NotFound("ledger.find", "ledger_draft", id)
	}
	if err != nil {
		return domain.LedgerDraft{}, fmt.Errorf("find ledger draft: %w", err)
	}
	var parseErr error
	if value.FrozenAt, parseErr = storage.ScanNullableTime(frozen); parseErr != nil {
		return domain.LedgerDraft{}, fmt.Errorf("parse ledger frozen_at: %w", parseErr)
	}
	if value.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
		return domain.LedgerDraft{}, fmt.Errorf("parse ledger created_at: %w", parseErr)
	}
	if value.UpdatedAt, parseErr = storage.ParseTime(updated); parseErr != nil {
		return domain.LedgerDraft{}, fmt.Errorf("parse ledger updated_at: %w", parseErr)
	}
	return value, nil
}

func (Repository) ListDrafts(ctx context.Context, q storage.Queryer, tenantID string, filter ListFilter) ([]domain.LedgerDraft, int, error) {
	if filter.Limit < 1 || filter.Limit > 200 || filter.Offset < 0 {
		return nil, 0, domain.Validation("ledger.list", "limit must be 1-200 and offset non-negative")
	}
	where := []string{"tenant_id = ?"}
	args := []any{tenantID}
	if filter.Search != "" {
		where = append(where, "LOWER(name) LIKE ? ESCAPE '\\'")
		args = append(args, "%"+escapeLike(strings.ToLower(filter.Search))+"%")
	}
	if len(filter.Statuses) > 0 {
		placeholders := make([]string, len(filter.Statuses))
		for i, status := range filter.Statuses {
			placeholders[i] = "?"
			args = append(args, status)
		}
		where = append(where, "status IN ("+strings.Join(placeholders, ",")+")")
	}
	predicate := strings.Join(where, " AND ")
	var total int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM credit_plans WHERE `+predicate, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count ledger drafts: %w", err)
	}
	queryArgs := append(append([]any(nil), args...), filter.Limit, filter.Offset)
	rows, err := q.QueryContext(ctx, `
		SELECT id, tenant_id, name, status, revision, digest, item_count,
		       frozen_at, created_at, updated_at, version
		FROM credit_plans WHERE `+predicate+`
		ORDER BY updated_at DESC, id ASC LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list ledger drafts: %w", err)
	}
	defer rows.Close()
	values := make([]domain.LedgerDraft, 0)
	for rows.Next() {
		value, err := scanDraft(rows)
		if err != nil {
			return nil, 0, err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate ledger drafts: %w", err)
	}
	return values, total, nil
}

func scanDraft(row scanner) (domain.LedgerDraft, error) {
	var value domain.LedgerDraft
	var frozen sql.NullString
	var created, updated string
	if err := row.Scan(&value.ID, &value.TenantID, &value.Name, &value.Status,
		&value.Revision, &value.Digest, &value.ItemCount, &frozen,
		&created, &updated, &value.Version); err != nil {
		return domain.LedgerDraft{}, fmt.Errorf("scan ledger draft: %w", err)
	}
	var err error
	if value.FrozenAt, err = storage.ScanNullableTime(frozen); err != nil {
		return domain.LedgerDraft{}, fmt.Errorf("parse ledger frozen_at: %w", err)
	}
	if value.CreatedAt, err = storage.ParseTime(created); err != nil {
		return domain.LedgerDraft{}, fmt.Errorf("parse ledger created_at: %w", err)
	}
	if value.UpdatedAt, err = storage.ParseTime(updated); err != nil {
		return domain.LedgerDraft{}, fmt.Errorf("parse ledger updated_at: %w", err)
	}
	return value, nil
}

func (Repository) EligibleWorkload(ctx context.Context, q storage.Queryer, tenantID, workloadID string) error {
	var status domain.JobWorkloadStatus
	var reservation string
	err := q.QueryRowContext(ctx, `
		SELECT status, reservation_ref FROM workloads
		WHERE tenant_id = ? AND id = ?`, tenantID, workloadID).Scan(&status, &reservation)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.NotFound("ledger.eligible_workload", "workload_session", workloadID)
	}
	if err != nil {
		return fmt.Errorf("load ledger workload eligibility: %w", err)
	}
	if status != domain.WorkloadSettled || strings.TrimSpace(reservation) == "" {
		return domain.Precondition("ledger.eligible_workload", "workload_session", workloadID, "workload must be settled and have a reservation reference")
	}
	var accepted int
	err = q.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM usage_batches
		WHERE tenant_id = ? AND workload_id = ? AND status = 'accepted'`, tenantID, workloadID).Scan(&accepted)
	if err != nil {
		return fmt.Errorf("count accepted metering batches: %w", err)
	}
	if accepted == 0 {
		return domain.Precondition("ledger.eligible_workload", "workload_session", workloadID, "accepted metering is required")
	}
	return nil
}

func (Repository) InsertItems(ctx context.Context, q storage.Queryer, items []domain.LedgerItem) error {
	for _, item := range items {
		if _, err := q.ExecContext(ctx, `
			INSERT INTO credit_plan_items(id, tenant_id, ledger_id, workload_id, revision, created_at)
			VALUES(?, ?, ?, ?, ?, ?)`, item.ID, item.TenantID, item.LedgerID,
			item.WorkloadID, item.Revision, storage.FormatTime(item.CreatedAt)); err != nil {
			return fmt.Errorf("insert ledger item %s: %w", item.ID, err)
		}
	}
	return nil
}

func (Repository) ListItems(ctx context.Context, q storage.Queryer, tenantID, ledgerID string) ([]domain.LedgerItem, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT id, tenant_id, ledger_id, workload_id, revision, created_at
		FROM credit_plan_items WHERE tenant_id = ? AND ledger_id = ?
		ORDER BY workload_id ASC, id ASC`, tenantID, ledgerID)
	if err != nil {
		return nil, fmt.Errorf("list ledger items: %w", err)
	}
	defer rows.Close()
	values := make([]domain.LedgerItem, 0)
	for rows.Next() {
		var value domain.LedgerItem
		var created string
		if err := rows.Scan(&value.ID, &value.TenantID, &value.LedgerID,
			&value.WorkloadID, &value.Revision, &created); err != nil {
			return nil, fmt.Errorf("scan ledger item: %w", err)
		}
		parsed, err := storage.ParseTime(created)
		if err != nil {
			return nil, fmt.Errorf("parse ledger item created_at: %w", err)
		}
		value.CreatedAt = parsed
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ledger items: %w", err)
	}
	return values, nil
}

func (Repository) RefreshItemCount(ctx context.Context, q storage.Queryer, tenantID, ledgerID string, expectedVersion int64, now time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE credit_plans
		SET item_count = (SELECT COUNT(*) FROM credit_plan_items WHERE tenant_id = ? AND ledger_id = ?),
		    updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = 'draft' AND version = ?`,
		tenantID, ledgerID, storage.FormatTime(now), tenantID, ledgerID, expectedVersion)
	if err != nil {
		return fmt.Errorf("refresh ledger item count: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read ledger item count result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("ledger.add_items", "ledger_draft", ledgerID, "ledger changed or is no longer editable")
	}
	return nil
}

func (Repository) Transition(ctx context.Context, q storage.Queryer, tenantID, ledgerID string, from, to domain.LedgerStatus, expectedVersion int64, digest string, now time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE credit_plans
		SET status = ?, digest = CASE WHEN ? = '' THEN digest ELSE ? END,
		    frozen_at = CASE WHEN ? = 'frozen' THEN ? ELSE frozen_at END,
		    updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = ? AND version = ?`,
		to, digest, digest, to, storage.FormatTime(now), storage.FormatTime(now),
		tenantID, ledgerID, from, expectedVersion)
	if err != nil {
		return fmt.Errorf("transition ledger draft: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read ledger transition result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("ledger.transition", "ledger_draft", ledgerID, "state or version changed")
	}
	return nil
}

func (Repository) InsertReview(ctx context.Context, q storage.Queryer, review domain.QualityReview) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO quality_reviews(
			id, tenant_id, object_type, object_id, reviewer_id,
			outcome, reason, created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, review.ID, review.TenantID,
		review.ObjectType, review.ObjectID, review.ReviewerID, review.Outcome,
		review.Reason, storage.FormatTime(review.CreatedAt), storage.FormatTime(review.UpdatedAt), review.Version)
	if err != nil {
		return fmt.Errorf("insert quality review: %w", err)
	}
	return nil
}

func (Repository) InsertRelease(ctx context.Context, q storage.Queryer, release domain.LedgerRelease, items []domain.LedgerItem) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO credit_releases(
			id, tenant_id, ledger_id, revision, digest, status,
			published_at, revoked_at, created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, release.ID, release.TenantID,
		release.LedgerID, release.Revision, release.Digest, release.Status,
		storage.NullableTime(release.PublishedAt), storage.NullableTime(release.RevokedAt),
		storage.FormatTime(release.CreatedAt), storage.FormatTime(release.UpdatedAt), release.Version)
	if err != nil {
		return fmt.Errorf("insert ledger release: %w", err)
	}
	for _, item := range items {
		if _, err := q.ExecContext(ctx, `
			INSERT INTO credit_release_items(release_id, ledger_item_id, tenant_id, created_at)
			VALUES(?, ?, ?, ?)`, release.ID, item.ID, release.TenantID, storage.FormatTime(release.CreatedAt)); err != nil {
			return fmt.Errorf("insert release item %s: %w", item.ID, err)
		}
	}
	return nil
}

func (Repository) FindRelease(ctx context.Context, q storage.Queryer, tenantID, id string) (domain.LedgerRelease, error) {
	var value domain.LedgerRelease
	var published, revoked sql.NullString
	var created, updated string
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, ledger_id, revision, digest, status,
		       published_at, revoked_at, created_at, updated_at, version
		FROM credit_releases WHERE tenant_id = ? AND id = ?`, tenantID, id,
	).Scan(&value.ID, &value.TenantID, &value.LedgerID, &value.Revision,
		&value.Digest, &value.Status, &published, &revoked, &created, &updated, &value.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.LedgerRelease{}, domain.NotFound("ledger.find_release", "ledger_release", id)
	}
	if err != nil {
		return domain.LedgerRelease{}, fmt.Errorf("find ledger release: %w", err)
	}
	var parseErr error
	if value.PublishedAt, parseErr = storage.ScanNullableTime(published); parseErr != nil {
		return domain.LedgerRelease{}, fmt.Errorf("parse release published_at: %w", parseErr)
	}
	if value.RevokedAt, parseErr = storage.ScanNullableTime(revoked); parseErr != nil {
		return domain.LedgerRelease{}, fmt.Errorf("parse release revoked_at: %w", parseErr)
	}
	if value.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
		return domain.LedgerRelease{}, fmt.Errorf("parse release created_at: %w", parseErr)
	}
	if value.UpdatedAt, parseErr = storage.ParseTime(updated); parseErr != nil {
		return domain.LedgerRelease{}, fmt.Errorf("parse release updated_at: %w", parseErr)
	}
	return value, nil
}

func (Repository) CountActiveJobs(ctx context.Context, q storage.Queryer, tenantID, releaseID string) (int, error) {
	var count int
	err := q.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM compute_jobs
		WHERE tenant_id = ? AND release_id = ? AND status IN ('queued','running','retrying')`, tenantID, releaseID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count active release jobs: %w", err)
	}
	return count, nil
}

func (Repository) TransitionRelease(ctx context.Context, q storage.Queryer, tenantID, releaseID string, from, to domain.LedgerStatus, version int64, now time.Time) error {
	published, revoked := any(nil), any(nil)
	if to == domain.LedgerStatusPublished {
		published = storage.FormatTime(now)
	}
	if to == domain.LedgerStatusRevoked {
		revoked = storage.FormatTime(now)
	}
	result, err := q.ExecContext(ctx, `
		UPDATE credit_releases
		SET status = ?, published_at = COALESCE(?, published_at),
		    revoked_at = COALESCE(?, revoked_at), updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND status = ? AND version = ?`,
		to, published, revoked, storage.FormatTime(now), tenantID, releaseID, from, version)
	if err != nil {
		return fmt.Errorf("transition ledger release: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read release transition result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("ledger.transition_release", "ledger_release", releaseID, "state or version changed")
	}
	return nil
}

func escapeLike(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `%`, `\%`)
	return strings.ReplaceAll(value, `_`, `\_`)
}

type scanner interface {
	Scan(...any) error
}
