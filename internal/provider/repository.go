package provider

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

func (Repository) InsertProvider(ctx context.Context, q storage.Queryer, value domain.Provider) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO providers(id, tenant_id, name, timezone, active, created_at, updated_at, version)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, value.ID, value.TenantID, value.Name,
		value.Timezone, value.Active, storage.FormatTime(value.CreatedAt), storage.FormatTime(value.UpdatedAt), value.Version)
	if err != nil {
		return fmt.Errorf("insert provider: %w", err)
	}
	return nil
}

func (Repository) InsertPool(ctx context.Context, q storage.Queryer, value domain.ComputePool) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO compute_pools(
			id, tenant_id, provider_id, name, capabilities, active, created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`, value.ID, value.TenantID, value.ProviderID,
		value.Name, value.Capabilities, value.Active, storage.FormatTime(value.CreatedAt),
		storage.FormatTime(value.UpdatedAt), value.Version)
	if err != nil {
		return fmt.Errorf("insert workload pool: %w", err)
	}
	return nil
}

func (Repository) InsertCapacityOffer(ctx context.Context, q storage.Queryer, value domain.CapacityOffer) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO capacity_offers(
			id, tenant_id, name, environment, required_capabilities,
			active, created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`, value.ID, value.TenantID, value.Name,
		value.Environment, value.RequiredCapabilities, value.Active,
		storage.FormatTime(value.CreatedAt), storage.FormatTime(value.UpdatedAt), value.Version)
	if err != nil {
		return fmt.Errorf("insert capacity_offer: %w", err)
	}
	return nil
}

func (Repository) FindProvider(ctx context.Context, q storage.Queryer, tenantID, id string) (domain.Provider, error) {
	var value domain.Provider
	var created, updated string
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, name, timezone, active, created_at, updated_at, version
		FROM providers WHERE tenant_id = ? AND id = ?`, tenantID, id,
	).Scan(&value.ID, &value.TenantID, &value.Name, &value.Timezone, &value.Active, &created, &updated, &value.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Provider{}, domain.NotFound("provider.find", "provider", id)
	}
	if err != nil {
		return domain.Provider{}, fmt.Errorf("find provider: %w", err)
	}
	value.CreatedAt, err = storage.ParseTime(created)
	if err != nil {
		return domain.Provider{}, fmt.Errorf("parse provider created_at: %w", err)
	}
	value.UpdatedAt, err = storage.ParseTime(updated)
	if err != nil {
		return domain.Provider{}, fmt.Errorf("parse provider updated_at: %w", err)
	}
	return value, nil
}

func (Repository) FindPool(ctx context.Context, q storage.Queryer, tenantID, id string) (domain.ComputePool, error) {
	var value domain.ComputePool
	var created, updated string
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, provider_id, name, capabilities, active, created_at, updated_at, version
		FROM compute_pools WHERE tenant_id = ? AND id = ?`, tenantID, id,
	).Scan(&value.ID, &value.TenantID, &value.ProviderID, &value.Name,
		&value.Capabilities, &value.Active, &created, &updated, &value.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ComputePool{}, domain.NotFound("provider.find_pool", "compute_pool", id)
	}
	if err != nil {
		return domain.ComputePool{}, fmt.Errorf("find workload pool: %w", err)
	}
	value.CreatedAt, err = storage.ParseTime(created)
	if err != nil {
		return domain.ComputePool{}, fmt.Errorf("parse pool created_at: %w", err)
	}
	value.UpdatedAt, err = storage.ParseTime(updated)
	if err != nil {
		return domain.ComputePool{}, fmt.Errorf("parse pool updated_at: %w", err)
	}
	return value, nil
}

func (Repository) FindCapacityOffer(ctx context.Context, q storage.Queryer, tenantID, id string) (domain.CapacityOffer, error) {
	var value domain.CapacityOffer
	var created, updated string
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, name, environment, required_capabilities,
		       active, created_at, updated_at, version
		FROM capacity_offers WHERE tenant_id = ? AND id = ?`, tenantID, id,
	).Scan(&value.ID, &value.TenantID, &value.Name, &value.Environment,
		&value.RequiredCapabilities, &value.Active, &created, &updated, &value.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.CapacityOffer{}, domain.NotFound("provider.find_capacity_offer", "capacity_offer", id)
	}
	if err != nil {
		return domain.CapacityOffer{}, fmt.Errorf("find capacity_offer: %w", err)
	}
	value.CreatedAt, err = storage.ParseTime(created)
	if err != nil {
		return domain.CapacityOffer{}, fmt.Errorf("parse capacity_offer created_at: %w", err)
	}
	value.UpdatedAt, err = storage.ParseTime(updated)
	if err != nil {
		return domain.CapacityOffer{}, fmt.Errorf("parse capacity_offer updated_at: %w", err)
	}
	return value, nil
}

func (Repository) InsertLease(ctx context.Context, q storage.Queryer, value domain.PoolLease) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO leases(
			id, tenant_id, pool_id, workload_id, owner, token,
			expires_at, released_at, created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, value.ID, value.TenantID,
		value.PoolID, value.WorkloadID, value.Owner, value.Token,
		storage.FormatTime(value.ExpiresAt), storage.NullableTime(value.ReleasedAt),
		storage.FormatTime(value.CreatedAt), storage.FormatTime(value.UpdatedAt), value.Version)
	if err != nil {
		return fmt.Errorf("insert pool lease: %w", err)
	}
	return nil
}

func (Repository) FindActiveLease(ctx context.Context, q storage.Queryer, tenantID, poolID string) (domain.PoolLease, error) {
	var value domain.PoolLease
	var expires, created, updated string
	var released sql.NullString
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, pool_id, workload_id, owner, token,
		       expires_at, released_at, created_at, updated_at, version
		FROM leases
		WHERE tenant_id = ? AND pool_id = ? AND released_at IS NULL`, tenantID, poolID,
	).Scan(&value.ID, &value.TenantID, &value.PoolID, &value.WorkloadID,
		&value.Owner, &value.Token, &expires, &released, &created, &updated, &value.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.PoolLease{}, domain.NotFound("provider.find_active_lease", "pool_lease", poolID)
	}
	if err != nil {
		return domain.PoolLease{}, fmt.Errorf("find active pool lease: %w", err)
	}
	var parseErr error
	if value.ExpiresAt, parseErr = storage.ParseTime(expires); parseErr != nil {
		return domain.PoolLease{}, fmt.Errorf("parse lease expires_at: %w", parseErr)
	}
	if value.ReleasedAt, parseErr = storage.ScanNullableTime(released); parseErr != nil {
		return domain.PoolLease{}, fmt.Errorf("parse lease released_at: %w", parseErr)
	}
	if value.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
		return domain.PoolLease{}, fmt.Errorf("parse lease created_at: %w", parseErr)
	}
	if value.UpdatedAt, parseErr = storage.ParseTime(updated); parseErr != nil {
		return domain.PoolLease{}, fmt.Errorf("parse lease updated_at: %w", parseErr)
	}
	return value, nil
}

func (Repository) RenewLease(ctx context.Context, q storage.Queryer, tenantID, leaseID, owner, token string, version int64, now, expiresAt time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE leases
		SET expires_at = ?, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND owner = ? AND token = ?
		  AND version = ? AND released_at IS NULL AND expires_at > ?`,
		storage.FormatTime(expiresAt), storage.FormatTime(now), tenantID, leaseID,
		owner, token, version, storage.FormatTime(now))
	if err != nil {
		return fmt.Errorf("renew pool lease: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read pool renewal result: %w", err)
	}
	if changed != 1 {
		return domain.Wrap(domain.ErrLeaseLost, "provider.renew_lease", "pool_lease", leaseID, "lease expired, changed, or belongs to another owner", nil)
	}
	return nil
}

func (Repository) ReleaseLease(ctx context.Context, q storage.Queryer, tenantID, leaseID, owner, token string, version int64, now time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE leases
		SET released_at = ?, updated_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND owner = ? AND token = ?
		  AND version = ? AND released_at IS NULL`, storage.FormatTime(now),
		storage.FormatTime(now), tenantID, leaseID, owner, token, version)
	if err != nil {
		return fmt.Errorf("release pool lease: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read pool release result: %w", err)
	}
	if changed != 1 {
		return domain.Wrap(domain.ErrLeaseLost, "provider.release_lease", "pool_lease", leaseID, "lease changed or belongs to another owner", nil)
	}
	return nil
}
