package provider

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/audit"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

type Service struct {
	db       *storage.Database
	repo     Repository
	audits   audit.Store
	clock    clock.Clock
	leaseTTL time.Duration
}

func NewService(db *storage.Database, c clock.Clock, leaseTTL time.Duration) *Service {
	return &Service{db: db, repo: Repository{}, audits: audit.Store{}, clock: c, leaseTTL: leaseTTL}
}

func (s *Service) CreateProvider(ctx context.Context, principal auth.Principal, name, timezone, requestID string) (domain.Provider, error) {
	if err := auth.RequireRole(principal, domain.RoleTenantAdmin); err != nil {
		return domain.Provider{}, err
	}
	name, timezone = strings.TrimSpace(name), strings.TrimSpace(timezone)
	if name == "" || timezone == "" || requestID == "" {
		return domain.Provider{}, domain.Validation("provider.create", "name, timezone, and request id are required")
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return domain.Provider{}, domain.Validation("provider.create", "timezone is invalid")
	}
	id, err := domain.NewID("provider")
	if err != nil {
		return domain.Provider{}, err
	}
	auditID, err := domain.NewID("audit")
	if err != nil {
		return domain.Provider{}, err
	}
	now := s.clock.Now()
	value := domain.Provider{ID: id, TenantID: principal.TenantID, Name: name, Timezone: timezone, Active: true, CreatedAt: now, UpdatedAt: now, Version: 1}
	if err := s.db.Write(ctx, func(tx *sql.Tx) error {
		if err := s.repo.InsertProvider(ctx, tx, value); err != nil {
			return err
		}
		return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: "provider.create", ObjectType: "provider", ObjectID: value.ID, Outcome: "created", RequestID: requestID, CreatedAt: now})
	}); err != nil {
		return domain.Provider{}, fmt.Errorf("create provider: %w", err)
	}
	return value, nil
}

func (s *Service) CreatePool(ctx context.Context, principal auth.Principal, providerID, name string, capabilities domain.PoolCapability, requestID string) (domain.ComputePool, error) {
	if err := auth.RequireRole(principal, domain.RoleTenantAdmin, domain.RoleOperator); err != nil {
		return domain.ComputePool{}, err
	}
	name = strings.TrimSpace(name)
	if providerID == "" || name == "" || capabilities == 0 || requestID == "" {
		return domain.ComputePool{}, domain.Validation("provider.create_pool", "provider, name, capabilities, and request id are required")
	}
	provider, err := s.repo.FindProvider(ctx, s.db.SQL(), principal.TenantID, providerID)
	if err != nil {
		return domain.ComputePool{}, err
	}
	if !provider.Active {
		return domain.ComputePool{}, domain.Precondition("provider.create_pool", "provider", providerID, "provider is inactive")
	}
	id, err := domain.NewID("pool")
	if err != nil {
		return domain.ComputePool{}, err
	}
	auditID, err := domain.NewID("audit")
	if err != nil {
		return domain.ComputePool{}, err
	}
	now := s.clock.Now()
	value := domain.ComputePool{ID: id, TenantID: principal.TenantID, ProviderID: providerID, Name: name, Capabilities: capabilities, Active: true, CreatedAt: now, UpdatedAt: now, Version: 1}
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		if err := s.repo.InsertPool(ctx, tx, value); err != nil {
			return err
		}
		return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: "pool.create", ObjectType: "compute_pool", ObjectID: value.ID, Outcome: "created", RequestID: requestID, CreatedAt: now})
	})
	if err != nil {
		return domain.ComputePool{}, fmt.Errorf("create pool: %w", err)
	}
	return value, nil
}

func (s *Service) CreateCapacityOffer(ctx context.Context, principal auth.Principal, name, environment string, required domain.PoolCapability, requestID string) (domain.CapacityOffer, error) {
	if err := auth.RequireRole(principal, domain.RoleTenantAdmin, domain.RoleDataSteward); err != nil {
		return domain.CapacityOffer{}, err
	}
	name, environment = strings.TrimSpace(name), strings.TrimSpace(environment)
	if name == "" || environment == "" || required == 0 || requestID == "" {
		return domain.CapacityOffer{}, domain.Validation("provider.create_capacity_offer", "name, environment, capabilities, and request id are required")
	}
	id, err := domain.NewID("capacity_offer")
	if err != nil {
		return domain.CapacityOffer{}, err
	}
	auditID, err := domain.NewID("audit")
	if err != nil {
		return domain.CapacityOffer{}, err
	}
	now := s.clock.Now()
	value := domain.CapacityOffer{ID: id, TenantID: principal.TenantID, Name: name, Environment: environment, RequiredCapabilities: required, Active: true, CreatedAt: now, UpdatedAt: now, Version: 1}
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		if err := s.repo.InsertCapacityOffer(ctx, tx, value); err != nil {
			return err
		}
		return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: "capacity_offer.create", ObjectType: "capacity_offer", ObjectID: value.ID, Outcome: "created", RequestID: requestID, CreatedAt: now})
	})
	if err != nil {
		return domain.CapacityOffer{}, fmt.Errorf("create capacity_offer: %w", err)
	}
	return value, nil
}

func (s *Service) ReservePool(ctx context.Context, principal auth.Principal, poolID, workloadID, requestID string) (domain.PoolLease, error) {
	if err := auth.RequireRole(principal, domain.RoleOperator); err != nil {
		return domain.PoolLease{}, err
	}
	if poolID == "" || workloadID == "" || requestID == "" {
		return domain.PoolLease{}, domain.Validation("provider.reserve_pool", "pool, workload, and request id are required")
	}
	pool, err := s.repo.FindPool(ctx, s.db.SQL(), principal.TenantID, poolID)
	if err != nil {
		return domain.PoolLease{}, err
	}
	if !pool.Active {
		return domain.PoolLease{}, domain.Precondition("provider.reserve_pool", "compute_pool", poolID, "pool is inactive")
	}
	leaseID, err := domain.NewID("lease")
	if err != nil {
		return domain.PoolLease{}, err
	}
	token, err := leaseToken()
	if err != nil {
		return domain.PoolLease{}, err
	}
	auditID, err := domain.NewID("audit")
	if err != nil {
		return domain.PoolLease{}, err
	}
	now := s.clock.Now()
	lease := domain.PoolLease{ID: leaseID, TenantID: principal.TenantID, PoolID: poolID, WorkloadID: workloadID, Owner: principal.UserID, Token: token, ExpiresAt: now.Add(s.leaseTTL), CreatedAt: now, UpdatedAt: now, Version: 1}
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		if err := s.repo.InsertLease(ctx, tx, lease); err != nil {
			return domain.Conflict("provider.reserve_pool", "compute_pool", poolID, "pool already has a live lease")
		}
		return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: "pool.reserve", ObjectType: "pool_lease", ObjectID: lease.ID, Outcome: "reserved", RequestID: requestID, CreatedAt: now})
	})
	if err != nil {
		return domain.PoolLease{}, fmt.Errorf("reserve pool: %w", err)
	}
	return lease, nil
}

func (s *Service) RenewPool(ctx context.Context, principal auth.Principal, leaseID, poolID, token string, version int64, requestID string) (domain.PoolLease, error) {
	if err := auth.RequireRole(principal, domain.RoleOperator); err != nil {
		return domain.PoolLease{}, err
	}
	lease, err := s.repo.FindActiveLease(ctx, s.db.SQL(), principal.TenantID, poolID)
	if err != nil {
		return domain.PoolLease{}, err
	}
	if lease.ID != leaseID || lease.Owner != principal.UserID || lease.Token != token || lease.Version != version {
		return domain.PoolLease{}, domain.Wrap(domain.ErrLeaseLost, "provider.renew_pool", "pool_lease", leaseID, "lease identity or ownership changed", nil)
	}
	auditID, err := domain.NewID("audit")
	if err != nil {
		return domain.PoolLease{}, err
	}
	now := s.clock.Now()
	expires := now.Add(s.leaseTTL)
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		if err := s.repo.RenewLease(ctx, tx, principal.TenantID, lease.ID, principal.UserID, token, version, now, expires); err != nil {
			return err
		}
		return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: "pool.renew", ObjectType: "pool_lease", ObjectID: lease.ID, Outcome: "renewed", RequestID: requestID, CreatedAt: now})
	})
	if err != nil {
		return domain.PoolLease{}, err
	}
	lease.ExpiresAt, lease.UpdatedAt, lease.Version = expires, now, lease.Version+1
	return lease, nil
}

func (s *Service) ReleasePool(ctx context.Context, principal auth.Principal, poolID, leaseID, token string, version int64, requestID string) error {
	if err := auth.RequireRole(principal, domain.RoleOperator, domain.RoleDataSteward); err != nil {
		return err
	}
	lease, err := s.repo.FindActiveLease(ctx, s.db.SQL(), principal.TenantID, poolID)
	if err != nil {
		return err
	}
	if lease.ID != leaseID || lease.Owner != principal.UserID || lease.Token != token || lease.Version != version {
		return domain.Wrap(domain.ErrLeaseLost, "provider.release_pool", "pool_lease", leaseID, "lease identity or ownership changed", nil)
	}
	auditID, err := domain.NewID("audit")
	if err != nil {
		return err
	}
	now := s.clock.Now()
	return s.db.Write(ctx, func(tx *sql.Tx) error {
		if err := s.repo.ReleaseLease(ctx, tx, principal.TenantID, lease.ID, principal.UserID, token, version, now); err != nil {
			return err
		}
		return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: "pool.release", ObjectType: "pool_lease", ObjectID: lease.ID, Outcome: "released", RequestID: requestID, CreatedAt: now})
	})
}

func leaseToken() (string, error) {
	value := make([]byte, 24)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate lease token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
