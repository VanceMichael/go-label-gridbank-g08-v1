package workload

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/audit"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/idempotency"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/outbox"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/provider"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

const planPath = "/api/v1/workloads"

type Service struct {
	db             *storage.Database
	repo           Repository
	providers      provider.Repository
	audits         audit.Store
	outbox         outbox.Repository
	idempotency    idempotency.Store
	clock          clock.Clock
	leaseTTL       time.Duration
	idempotencyTTL time.Duration
}

type PlanInput struct {
	ProviderID      string
	CapacityOfferID string
	PoolID          string
	ReservationRef  string
	IdempotencyKey  string
	RequestID       string
}

type PlanResult struct {
	Workload domain.WorkloadSession `json:"workload"`
	Lease    domain.PoolLease       `json:"lease"`
	Replay   bool                   `json:"replay"`
}

func NewService(db *storage.Database, c clock.Clock, leaseTTL time.Duration) *Service {
	return &Service{
		db: db, repo: Repository{}, providers: provider.Repository{}, audits: audit.Store{},
		outbox: outbox.Repository{}, idempotency: idempotency.Store{}, clock: c,
		leaseTTL: leaseTTL, idempotencyTTL: 24 * time.Hour,
	}
}

func (s *Service) Plan(ctx context.Context, principal auth.Principal, input PlanInput) (PlanResult, error) {
	if err := auth.RequireRole(principal, domain.RoleOperator); err != nil {
		return PlanResult{}, err
	}
	if err := validatePlan(input); err != nil {
		return PlanResult{}, err
	}
	fingerprint := domain.Fingerprint(input.ProviderID, input.CapacityOfferID, input.PoolID, input.ReservationRef)
	now := s.clock.Now()
	var result PlanResult
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		record, found, err := s.idempotency.Lookup(ctx, tx, principal.TenantID, http.MethodPost, planPath, input.IdempotencyKey, now)
		if err != nil {
			return err
		}
		if found {
			if err := idempotency.EnsureFingerprint(record, fingerprint); err != nil {
				return err
			}
			if err := json.Unmarshal(record.Response, &result); err != nil {
				return fmt.Errorf("decode plan replay: %w", err)
			}
			result.Replay = true
			return nil
		}
		providerValue, err := s.providers.FindProvider(ctx, tx, principal.TenantID, input.ProviderID)
		if err != nil {
			return err
		}
		pool, err := s.providers.FindPool(ctx, tx, principal.TenantID, input.PoolID)
		if err != nil {
			return err
		}
		capacity_offer, err := s.providers.FindCapacityOffer(ctx, tx, principal.TenantID, input.CapacityOfferID)
		if err != nil {
			return err
		}
		if !providerValue.Active || !pool.Active || !capacity_offer.Active {
			return domain.Precondition("workload.plan", "workload_session", "", "provider, pool, and capacity_offer must be active")
		}
		if pool.ProviderID != providerValue.ID {
			return domain.Precondition("workload.plan", "compute_pool", pool.ID, "pool belongs to another provider")
		}
		if !pool.Supports(capacity_offer.RequiredCapabilities) {
			return domain.Precondition("workload.plan", "compute_pool", pool.ID, "pool does not satisfy capacity_offer capabilities")
		}
		workloadID, err := domain.NewID("workload")
		if err != nil {
			return err
		}
		leaseID, err := domain.NewID("lease")
		if err != nil {
			return err
		}
		token, err := randomLeaseToken()
		if err != nil {
			return err
		}
		workloadValue := domain.WorkloadSession{
			ID: workloadID, TenantID: principal.TenantID, ProviderID: providerValue.ID,
			CapacityOfferID: capacity_offer.ID, PoolID: pool.ID, OperatorID: principal.UserID,
			Status: domain.WorkloadQueued, Revision: 1, ReservationRef: input.ReservationRef,
			CreatedAt: now, UpdatedAt: now, Version: 1,
		}
		lease := domain.PoolLease{
			ID: leaseID, TenantID: principal.TenantID, PoolID: pool.ID,
			WorkloadID: workloadID, Owner: principal.UserID, Token: token,
			ExpiresAt: now.Add(s.leaseTTL), CreatedAt: now, UpdatedAt: now, Version: 1,
		}
		if err := s.repo.Insert(ctx, tx, workloadValue); err != nil {
			return err
		}
		if err := s.providers.InsertLease(ctx, tx, lease); err != nil {
			if releaseErr := s.providers.ReleaseActiveLeaseForRetry(ctx, tx, principal.TenantID, pool.ID, now); releaseErr != nil {
				return releaseErr
			}
			if retryErr := s.providers.InsertLease(ctx, tx, lease); retryErr != nil {
				return domain.Conflict("workload.plan", "compute_pool", pool.ID, "pool already has a live lease")
			}
		}
		if err := s.appendEffects(ctx, tx, principal, input.RequestID, "workload.plan", workloadValue.ID, "planned", now); err != nil {
			return err
		}
		result = PlanResult{Workload: workloadValue, Lease: lease}
		response, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("encode plan response: %w", err)
		}
		recordID, err := domain.NewID("idem")
		if err != nil {
			return err
		}
		return s.idempotency.Save(ctx, tx, domain.IdempotencyRecord{
			ID: recordID, TenantID: principal.TenantID, Method: http.MethodPost,
			Path: planPath, Key: input.IdempotencyKey, Fingerprint: fingerprint,
			StatusCode: http.StatusCreated, Response: response, CreatedAt: now,
			ExpiresAt: now.Add(s.idempotencyTTL),
		})
	})
	if err != nil {
		return PlanResult{}, fmt.Errorf("plan workload: %w", err)
	}
	return result, nil
}

func (s *Service) MarkReady(ctx context.Context, principal auth.Principal, workloadID, requestID string) (domain.WorkloadSession, error) {
	return s.transition(ctx, principal, workloadID, requestID, domain.WorkloadQueued, domain.WorkloadAllocated, TransitionTimes{}, "workload.ready", domain.RoleOperator)
}

func (s *Service) Start(ctx context.Context, principal auth.Principal, workloadID, requestID string) (domain.WorkloadSession, error) {
	now := s.clock.Now()
	return s.transition(ctx, principal, workloadID, requestID, domain.WorkloadAllocated, domain.WorkloadRunning, TransitionTimes{StartedAt: &now}, "workload.start", domain.RoleOperator)
}

func (s *Service) Submit(ctx context.Context, principal auth.Principal, workloadID, requestID string) (domain.WorkloadSession, error) {
	if err := auth.RequireRole(principal, domain.RoleOperator); err != nil {
		return domain.WorkloadSession{}, err
	}
	now := s.clock.Now()
	var updated domain.WorkloadSession
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		current, err := s.repo.Find(ctx, tx, principal.TenantID, workloadID)
		if err != nil {
			return err
		}
		if current.OperatorID != principal.UserID {
			return domain.Wrap(domain.ErrForbidden, "workload.submit", "workload_session", workloadID, "only the workload operator may submit", nil)
		}
		if err := current.Status.Transition(domain.WorkloadMetering); err != nil {
			return err
		}
		manifestCount, err := s.repo.CountStreams(ctx, tx, principal.TenantID, workloadID)
		if err != nil {
			return err
		}
		unaligned, err := s.repo.CountUnalignedStreams(ctx, tx, principal.TenantID, workloadID)
		if err != nil {
			return err
		}
		if manifestCount < 2 || unaligned != 0 {
			return domain.Precondition("workload.submit", "workload_session", workloadID, "at least two aligned manifests are required")
		}
		if err := s.repo.Transition(ctx, tx, principal.TenantID, workloadID, current.Status, domain.WorkloadMetering, current.Version, now, TransitionTimes{SubmittedAt: &now}); err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal, requestID, "workload.submit", workloadID, "metering", now); err != nil {
			return err
		}
		current.Status, current.SubmittedAt, current.UpdatedAt, current.Version = domain.WorkloadMetering, &now, now, current.Version+1
		updated = current
		return nil
	})
	if err != nil {
		return domain.WorkloadSession{}, fmt.Errorf("submit workload: %w", err)
	}
	return updated, nil
}

func (s *Service) Settle(ctx context.Context, principal auth.Principal, workloadID, requestID string) (domain.WorkloadSession, error) {
	now := s.clock.Now()
	return s.transition(ctx, principal, workloadID, requestID, domain.WorkloadMetering, domain.WorkloadSettled, TransitionTimes{SettledAt: &now}, "workload.settle", domain.RoleDataSteward, domain.RoleReviewer)
}

func (s *Service) Fail(ctx context.Context, principal auth.Principal, workloadID, requestID string) (domain.WorkloadSession, error) {
	return s.transition(ctx, principal, workloadID, requestID, domain.WorkloadMetering, domain.WorkloadFailed, TransitionTimes{}, "workload.fail", domain.RoleDataSteward, domain.RoleReviewer)
}

func (s *Service) Cancel(ctx context.Context, principal auth.Principal, workloadID, requestID string) (domain.WorkloadSession, error) {
	if err := auth.RequireRole(principal, domain.RoleOperator, domain.RoleDataSteward); err != nil {
		return domain.WorkloadSession{}, err
	}
	now := s.clock.Now()
	var updated domain.WorkloadSession
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		current, err := s.repo.Find(ctx, tx, principal.TenantID, workloadID)
		if err != nil {
			return err
		}
		if err := current.Status.Transition(domain.WorkloadCanceled); err != nil {
			return err
		}
		if principal.Role == domain.RoleOperator && current.OperatorID != principal.UserID {
			return domain.Wrap(domain.ErrForbidden, "workload.cancel", "workload_session", workloadID, "operator does not own workload", nil)
		}
		lease, err := s.providers.FindActiveLease(ctx, tx, principal.TenantID, current.PoolID)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		if err == nil {
			if lease.WorkloadID != workloadID {
				return domain.Conflict("workload.cancel", "workload_session", workloadID, "pool lease belongs to another workload")
			}
			if err := s.providers.ReleaseLease(ctx, tx, principal.TenantID, lease.ID, lease.Owner, lease.Token, lease.Version, now); err != nil {
				return err
			}
		}
		if err := s.repo.Transition(ctx, tx, principal.TenantID, workloadID, current.Status, domain.WorkloadCanceled, current.Version, now, TransitionTimes{CanceledAt: &now}); err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal, requestID, "workload.cancel", workloadID, "canceled", now); err != nil {
			return err
		}
		current.Status, current.CanceledAt, current.UpdatedAt, current.Version = domain.WorkloadCanceled, &now, now, current.Version+1
		updated = current
		return nil
	})
	if err != nil {
		return domain.WorkloadSession{}, fmt.Errorf("cancel workload: %w", err)
	}
	return updated, nil
}

func (s *Service) Reopen(ctx context.Context, principal auth.Principal, workloadID, requestID string) (domain.WorkloadSession, error) {
	if err := auth.RequireRole(principal, domain.RoleOperator); err != nil {
		return domain.WorkloadSession{}, err
	}
	now := s.clock.Now()
	var updated domain.WorkloadSession
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		current, err := s.repo.Find(ctx, tx, principal.TenantID, workloadID)
		if err != nil {
			return err
		}
		if current.OperatorID != principal.UserID {
			return domain.Wrap(domain.ErrForbidden, "workload.reopen", "workload_session", workloadID, "operator does not own workload", nil)
		}
		if err := current.Status.Transition(domain.WorkloadAllocated); err != nil {
			return err
		}
		leaseID, err := domain.NewID("lease")
		if err != nil {
			return err
		}
		token, err := randomLeaseToken()
		if err != nil {
			return err
		}
		lease := domain.PoolLease{ID: leaseID, TenantID: principal.TenantID, PoolID: current.PoolID, WorkloadID: workloadID, Owner: principal.UserID, Token: token, ExpiresAt: now.Add(s.leaseTTL), CreatedAt: now, UpdatedAt: now, Version: 1}
		if err := s.providers.InsertLease(ctx, tx, lease); err != nil {
			return domain.Conflict("workload.reopen", "compute_pool", current.PoolID, "pool is not available for rework")
		}
		if err := s.repo.Reopen(ctx, tx, principal.TenantID, workloadID, current.Version, now); err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal, requestID, "workload.reopen", workloadID, "ready", now); err != nil {
			return err
		}
		current.Status, current.Revision, current.UpdatedAt, current.Version = domain.WorkloadAllocated, current.Revision+1, now, current.Version+1
		current.StartedAt, current.SubmittedAt, current.SettledAt = nil, nil, nil
		updated = current
		return nil
	})
	if err != nil {
		return domain.WorkloadSession{}, fmt.Errorf("reopen workload: %w", err)
	}
	return updated, nil
}

func (s *Service) Get(ctx context.Context, principal auth.Principal, workloadID string) (domain.WorkloadSession, error) {
	return s.repo.Find(ctx, s.db.SQL(), principal.TenantID, workloadID)
}

func (s *Service) transition(ctx context.Context, principal auth.Principal, workloadID, requestID string, from, to domain.JobWorkloadStatus, times TransitionTimes, action string, roles ...domain.Role) (domain.WorkloadSession, error) {
	if err := auth.RequireRole(principal, roles...); err != nil {
		return domain.WorkloadSession{}, err
	}
	if workloadID == "" || requestID == "" {
		return domain.WorkloadSession{}, domain.Validation(action, "workload and request id are required")
	}
	now := s.clock.Now()
	var updated domain.WorkloadSession
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		current, err := s.repo.Find(ctx, tx, principal.TenantID, workloadID)
		if err != nil {
			return err
		}
		if current.Status != from {
			return domain.Precondition(action, "workload_session", workloadID, fmt.Sprintf("workload must be %s", from))
		}
		if err := current.Status.Transition(to); err != nil {
			return err
		}
		if principal.Role == domain.RoleOperator && current.OperatorID != principal.UserID {
			return domain.Wrap(domain.ErrForbidden, action, "workload_session", workloadID, "operator does not own workload", nil)
		}
		if err := s.repo.Transition(ctx, tx, principal.TenantID, workloadID, from, to, current.Version, now, times); err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal, requestID, action, workloadID, string(to), now); err != nil {
			return err
		}
		current.Status, current.UpdatedAt, current.Version = to, now, current.Version+1
		if times.StartedAt != nil {
			current.StartedAt = times.StartedAt
		}
		if times.SubmittedAt != nil {
			current.SubmittedAt = times.SubmittedAt
		}
		if times.SettledAt != nil {
			current.SettledAt = times.SettledAt
		}
		if times.CanceledAt != nil {
			current.CanceledAt = times.CanceledAt
		}
		updated = current
		return nil
	})
	if err != nil {
		return domain.WorkloadSession{}, fmt.Errorf("%s: %w", action, err)
	}
	return updated, nil
}

func (s *Service) appendEffects(ctx context.Context, tx *sql.Tx, principal auth.Principal, requestID, action, workloadID, outcome string, now time.Time) error {
	auditID, err := domain.NewID("audit")
	if err != nil {
		return err
	}
	outboxID, err := domain.NewID("event")
	if err != nil {
		return err
	}
	if err := s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: action, ObjectType: "workload_session", ObjectID: workloadID, Outcome: outcome, RequestID: requestID, CreatedAt: now}); err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]string{"workload_id": workloadID, "event": action, "status": outcome})
	if err != nil {
		return fmt.Errorf("encode workload event: %w", err)
	}
	return s.outbox.Enqueue(ctx, tx, domain.OutboxEvent{ID: outboxID, TenantID: principal.TenantID, Topic: action, AggregateType: "workload_session", AggregateID: workloadID, Payload: string(payload), Status: domain.OutboxPending, MaxAttempts: 5, NextAttemptAt: now, CreatedAt: now, UpdatedAt: now, Version: 1})
}

func validatePlan(input PlanInput) error {
	if input.ProviderID == "" || input.CapacityOfferID == "" || input.PoolID == "" || strings.TrimSpace(input.ReservationRef) == "" {
		return domain.Validation("workload.plan", "provider, capacity_offer, pool, and reservation reference are required")
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" || len(input.IdempotencyKey) > 128 {
		return domain.Validation("workload.plan", "idempotency key is required and must be at most 128 bytes")
	}
	if strings.TrimSpace(input.RequestID) == "" {
		return domain.Validation("workload.plan", "request id is required")
	}
	return nil
}

func randomLeaseToken() (string, error) {
	value := make([]byte, 24)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate workload lease token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
