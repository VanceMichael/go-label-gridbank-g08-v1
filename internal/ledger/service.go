package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/audit"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/outbox"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

type Service struct {
	db     *storage.Database
	repo   Repository
	audits audit.Store
	outbox outbox.Repository
	clock  clock.Clock
}

func NewService(db *storage.Database, c clock.Clock) *Service {
	return &Service{db: db, repo: Repository{}, audits: audit.Store{}, outbox: outbox.Repository{}, clock: c}
}

func (s *Service) Create(ctx context.Context, principal auth.Principal, name, requestID string) (domain.LedgerDraft, error) {
	if err := auth.RequireRole(principal, domain.RoleDataSteward); err != nil {
		return domain.LedgerDraft{}, err
	}
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 160 || requestID == "" {
		return domain.LedgerDraft{}, domain.Validation("ledger.create", "bounded name and request id are required")
	}
	id, err := domain.NewID("ledger")
	if err != nil {
		return domain.LedgerDraft{}, err
	}
	now := s.clock.Now()
	value := domain.LedgerDraft{ID: id, TenantID: principal.TenantID, Name: name, Status: domain.LedgerStatusDraft, Revision: 1, CreatedAt: now, UpdatedAt: now, Version: 1}
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		if err := s.repo.InsertDraft(ctx, tx, value); err != nil {
			return err
		}
		return s.appendEffects(ctx, tx, principal, requestID, "ledger.create", value.ID, "draft", now)
	})
	if err != nil {
		return domain.LedgerDraft{}, fmt.Errorf("create ledger: %w", err)
	}
	return value, nil
}

func (s *Service) AddWorkloads(ctx context.Context, principal auth.Principal, ledgerID, requestID string, workloadIDs []string) (domain.LedgerDraft, []domain.LedgerItem, error) {
	if err := auth.RequireRole(principal, domain.RoleDataSteward); err != nil {
		return domain.LedgerDraft{}, nil, err
	}
	if ledgerID == "" || requestID == "" || len(workloadIDs) == 0 || len(workloadIDs) > 250 {
		return domain.LedgerDraft{}, nil, domain.Validation("ledger.add_workloads", "ledger, request id, and 1-250 workloads are required")
	}
	seen := make(map[string]struct{}, len(workloadIDs))
	for _, workloadID := range workloadIDs {
		if workloadID == "" {
			return domain.LedgerDraft{}, nil, domain.Validation("ledger.add_workloads", "workload id cannot be empty")
		}
		if _, exists := seen[workloadID]; exists {
			return domain.LedgerDraft{}, nil, domain.Validation("ledger.add_workloads", "workload ids must be unique within the request")
		}
		seen[workloadID] = struct{}{}
	}
	now := s.clock.Now()
	var updated domain.LedgerDraft
	var items []domain.LedgerItem
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		draft, err := s.repo.FindDraft(ctx, tx, principal.TenantID, ledgerID)
		if err != nil {
			return err
		}
		if draft.Status != domain.LedgerStatusDraft {
			return domain.Precondition("ledger.add_workloads", "ledger_draft", ledgerID, "only draft ledgers accept new workloads")
		}
		for _, workloadID := range workloadIDs {
			if err := s.repo.EligibleWorkload(ctx, tx, principal.TenantID, workloadID); err != nil {
				return err
			}
			id, err := domain.NewID("ledger_item")
			if err != nil {
				return err
			}
			items = append(items, domain.LedgerItem{ID: id, TenantID: principal.TenantID, LedgerID: ledgerID, WorkloadID: workloadID, Revision: draft.Revision, CreatedAt: now})
		}
		if err := s.repo.InsertItems(ctx, tx, items); err != nil {
			return err
		}
		if err := s.repo.RefreshItemCount(ctx, tx, principal.TenantID, ledgerID, draft.Version, now); err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal, requestID, "ledger.members.add", ledgerID, "added", now); err != nil {
			return err
		}
		draft.ItemCount += len(items)
		draft.UpdatedAt, draft.Version = now, draft.Version+1
		updated = draft
		return nil
	})
	if err != nil {
		return domain.LedgerDraft{}, nil, fmt.Errorf("add ledger workloads: %w", err)
	}
	return updated, items, nil
}

func (s *Service) Freeze(ctx context.Context, principal auth.Principal, ledgerID, requestID string) (domain.LedgerDraft, error) {
	if err := auth.RequireRole(principal, domain.RoleDataSteward); err != nil {
		return domain.LedgerDraft{}, err
	}
	now := s.clock.Now()
	var frozen domain.LedgerDraft
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		draft, err := s.repo.FindDraft(ctx, tx, principal.TenantID, ledgerID)
		if err != nil {
			return err
		}
		if err := draft.Status.Transition(domain.LedgerStatusFrozen); err != nil {
			return err
		}
		items, err := s.repo.ListItems(ctx, tx, principal.TenantID, ledgerID)
		if err != nil {
			return err
		}
		if len(items) == 0 || len(items) != draft.ItemCount {
			return domain.Precondition("ledger.freeze", "ledger_draft", ledgerID, "ledger membership must be non-empty and internally consistent")
		}
		for _, item := range items {
			if err := s.repo.EligibleWorkload(ctx, tx, principal.TenantID, item.WorkloadID); err != nil {
				return err
			}
		}
		digest := membershipDigest(items)
		if err := s.repo.Transition(ctx, tx, principal.TenantID, ledgerID, draft.Status, domain.LedgerStatusFrozen, draft.Version, digest, now); err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal, requestID, "ledger.freeze", ledgerID, "frozen", now); err != nil {
			return err
		}
		draft.Status, draft.Digest, draft.FrozenAt, draft.UpdatedAt, draft.Version = domain.LedgerStatusFrozen, digest, &now, now, draft.Version+1
		frozen = draft
		return nil
	})
	if err != nil {
		return domain.LedgerDraft{}, fmt.Errorf("freeze ledger: %w", err)
	}
	return frozen, nil
}

func (s *Service) Review(ctx context.Context, principal auth.Principal, ledgerID, requestID string, approve bool, reason string) (domain.LedgerDraft, error) {
	if err := auth.RequireRole(principal, domain.RoleReviewer); err != nil {
		return domain.LedgerDraft{}, err
	}
	if !approve && strings.TrimSpace(reason) == "" {
		return domain.LedgerDraft{}, domain.Validation("ledger.review", "rejection reason is required")
	}
	now := s.clock.Now()
	var reviewed domain.LedgerDraft
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		draft, err := s.repo.FindDraft(ctx, tx, principal.TenantID, ledgerID)
		if err != nil {
			return err
		}
		if draft.Status != domain.LedgerStatusFrozen {
			return domain.Precondition("ledger.review", "ledger_draft", ledgerID, "ledger must be frozen")
		}
		to := domain.LedgerStatusDraft
		outcome := "rejected"
		if approve {
			to, outcome = domain.LedgerStatusApproved, "approved"
		}
		if err := draft.Status.Transition(to); err != nil {
			return err
		}
		reviewID, err := domain.NewID("review")
		if err != nil {
			return err
		}
		review := domain.QualityReview{ID: reviewID, TenantID: principal.TenantID, ObjectType: "ledger_draft", ObjectID: ledgerID, ReviewerID: principal.UserID, Outcome: outcome, Reason: reason, CreatedAt: now, UpdatedAt: now, Version: 1}
		if err := s.repo.InsertReview(ctx, tx, review); err != nil {
			return err
		}
		if err := s.repo.Transition(ctx, tx, principal.TenantID, ledgerID, draft.Status, to, draft.Version, "", now); err != nil {
			return err
		}
		if err := s.appendEffectsWithDetail(ctx, tx, principal, requestID, "ledger.review", ledgerID, outcome, reason, now); err != nil {
			return err
		}
		draft.Status, draft.UpdatedAt, draft.Version = to, now, draft.Version+1
		if !approve {
			draft.FrozenAt, draft.Digest = nil, ""
		}
		reviewed = draft
		return nil
	})
	if err != nil {
		return domain.LedgerDraft{}, fmt.Errorf("review ledger: %w", err)
	}
	return reviewed, nil
}

func (s *Service) Publish(ctx context.Context, principal auth.Principal, ledgerID, requestID string) (domain.LedgerRelease, error) {
	if err := auth.RequireRole(principal, domain.RoleDataSteward); err != nil {
		return domain.LedgerRelease{}, err
	}
	now := s.clock.Now()
	var release domain.LedgerRelease
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		draft, err := s.repo.FindDraft(ctx, tx, principal.TenantID, ledgerID)
		if err != nil {
			return err
		}
		if err := draft.Status.Transition(domain.LedgerStatusPublished); err != nil {
			return err
		}
		items, err := s.repo.ListItems(ctx, tx, principal.TenantID, ledgerID)
		if err != nil {
			return err
		}
		actualCount := len(items)
		actualDigest := membershipDigest(items)
		if actualCount != draft.ItemCount || actualDigest == "" {
			return domain.Conflict("ledger.publish", "ledger_draft", ledgerID, fmt.Sprintf("frozen membership count changed from %d to %d", draft.ItemCount, actualCount))
		}
		releaseID, err := domain.NewID("release")
		if err != nil {
			return err
		}
		release = domain.LedgerRelease{ID: releaseID, TenantID: principal.TenantID, LedgerID: ledgerID, Revision: draft.Revision, Digest: draft.Digest, Status: domain.LedgerStatusPublished, PublishedAt: &now, CreatedAt: now, UpdatedAt: now, Version: 1}
		if err := s.repo.InsertRelease(ctx, tx, release, items); err != nil {
			return err
		}
		if err := s.repo.Transition(ctx, tx, principal.TenantID, ledgerID, draft.Status, domain.LedgerStatusPublished, draft.Version, "", now); err != nil {
			return err
		}
		return s.appendEffects(ctx, tx, principal, requestID, "ledger.publish", releaseID, "published", now)
	})
	if err != nil {
		return domain.LedgerRelease{}, fmt.Errorf("publish ledger: %w", err)
	}
	return release, nil
}

func (s *Service) Revoke(ctx context.Context, principal auth.Principal, releaseID, requestID, reason string) (domain.LedgerRelease, error) {
	if err := auth.RequireRole(principal, domain.RoleDataSteward); err != nil {
		return domain.LedgerRelease{}, err
	}
	if strings.TrimSpace(reason) == "" {
		return domain.LedgerRelease{}, domain.Validation("ledger.revoke", "reason is required")
	}
	now := s.clock.Now()
	var revoked domain.LedgerRelease
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		release, err := s.repo.FindRelease(ctx, tx, principal.TenantID, releaseID)
		if err != nil {
			return err
		}
		if err := release.Status.Transition(domain.LedgerStatusRevoked); err != nil {
			return err
		}
		activeJobs, err := s.repo.CountActiveJobs(ctx, tx, principal.TenantID, releaseID)
		if err != nil {
			return err
		}
		if activeJobs != 0 {
			return domain.Precondition("ledger.revoke", "ledger_release", releaseID, "active scheduler jobs still own the release")
		}
		if err := s.repo.TransitionRelease(ctx, tx, principal.TenantID, releaseID, release.Status, domain.LedgerStatusRevoked, release.Version, now); err != nil {
			return err
		}
		if err := s.appendEffectsWithDetail(ctx, tx, principal, requestID, "ledger.revoke", releaseID, "revoked", reason, now); err != nil {
			return err
		}
		release.Status, release.RevokedAt, release.UpdatedAt, release.Version = domain.LedgerStatusRevoked, &now, now, release.Version+1
		revoked = release
		return nil
	})
	if err != nil {
		return domain.LedgerRelease{}, fmt.Errorf("revoke ledger release: %w", err)
	}
	return revoked, nil
}

func (s *Service) Get(ctx context.Context, principal auth.Principal, ledgerID string) (domain.LedgerDraft, []domain.LedgerItem, error) {
	draft, err := s.repo.FindDraft(ctx, s.db.SQL(), principal.TenantID, ledgerID)
	if err != nil {
		return domain.LedgerDraft{}, nil, err
	}
	items, err := s.repo.ListItems(ctx, s.db.SQL(), principal.TenantID, ledgerID)
	if err != nil {
		return domain.LedgerDraft{}, nil, err
	}
	return draft, items, nil
}

func (s *Service) List(ctx context.Context, principal auth.Principal, filter ListFilter) ([]domain.LedgerDraft, int, error) {
	return s.repo.ListDrafts(ctx, s.db.SQL(), principal.TenantID, filter)
}

func (s *Service) appendEffects(ctx context.Context, tx *sql.Tx, principal auth.Principal, requestID, action, objectID, outcome string, now time.Time) error {
	return s.appendEffectsWithDetail(ctx, tx, principal, requestID, action, objectID, outcome, "", now)
}

func (s *Service) appendEffectsWithDetail(ctx context.Context, tx *sql.Tx, principal auth.Principal, requestID, action, objectID, outcome, detail string, now time.Time) error {
	auditID, err := domain.NewID("audit")
	if err != nil {
		return err
	}
	eventID, err := domain.NewID("event")
	if err != nil {
		return err
	}
	if err := s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: action, ObjectType: "ledger", ObjectID: objectID, Outcome: outcome, RequestID: requestID, Detail: detail, CreatedAt: now}); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]string{"object_id": objectID, "event": action, "outcome": outcome})
	return s.outbox.Enqueue(ctx, tx, domain.OutboxEvent{ID: eventID, TenantID: principal.TenantID, Topic: action, AggregateType: "ledger", AggregateID: objectID, Payload: string(payload), Status: domain.OutboxPending, MaxAttempts: 5, NextAttemptAt: now, CreatedAt: now, UpdatedAt: now, Version: 1})
}
