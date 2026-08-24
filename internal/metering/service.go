package metering

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/audit"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/capacity"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/outbox"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

type Service struct {
	db        *storage.Database
	repo      Repository
	workloads workload.Repository
	capacitys capacity.Repository
	audits    audit.Store
	outbox    outbox.Repository
	clock     clock.Clock
	leaseTTL  time.Duration
}

type ClaimResult struct {
	Batch domain.MeteringBatch
	Items []domain.MeteringItem
}

func NewService(db *storage.Database, c clock.Clock, leaseTTL time.Duration) *Service {
	return &Service{db: db, repo: Repository{}, workloads: workload.Repository{}, capacitys: capacity.Repository{}, audits: audit.Store{}, outbox: outbox.Repository{}, clock: c, leaseTTL: leaseTTL}
}

func (s *Service) Create(ctx context.Context, principal auth.Principal, workloadID, requestID string) (domain.MeteringBatch, []domain.MeteringItem, error) {
	if err := auth.RequireRole(principal, domain.RoleDataSteward, domain.RoleReviewer); err != nil {
		return domain.MeteringBatch{}, nil, err
	}
	if workloadID == "" || requestID == "" {
		return domain.MeteringBatch{}, nil, domain.Validation("metering.create", "workload and request id are required")
	}
	now := s.clock.Now()
	var batch domain.MeteringBatch
	var items []domain.MeteringItem
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		workloadValue, err := s.workloads.Find(ctx, tx, principal.TenantID, workloadID)
		if err != nil {
			return err
		}
		if workloadValue.Status != domain.WorkloadSettled {
			return domain.Precondition("metering.create", "workload_session", workloadID, "workload must be settled")
		}
		manifests, err := s.capacitys.ListStreams(ctx, tx, principal.TenantID, workloadID)
		if err != nil {
			return err
		}
		if len(manifests) == 0 {
			return domain.Precondition("metering.create", "workload_session", workloadID, "workload has no aligned manifests")
		}
		batchID, err := domain.NewID("batch")
		if err != nil {
			return err
		}
		batch = domain.MeteringBatch{ID: batchID, TenantID: principal.TenantID, WorkloadID: workloadID, Status: domain.MeteringOpen, CreatedAt: now, UpdatedAt: now, Version: 1}
		if err := s.repo.InsertBatch(ctx, tx, batch); err != nil {
			return err
		}
		for _, manifest := range manifests {
			if manifest.Status != domain.StreamAligned {
				return domain.Precondition("metering.create", "capacity_stream", manifest.ID, "all manifests must be aligned")
			}
			segments, err := s.capacitys.ListSegments(ctx, tx, principal.TenantID, manifest.ID)
			if err != nil {
				return err
			}
			for _, segment := range segments {
				itemID, err := domain.NewID("metering")
				if err != nil {
					return err
				}
				items = append(items, domain.MeteringItem{ID: itemID, TenantID: principal.TenantID, BatchID: batchID, SegmentID: segment.ID, CreatedAt: now, UpdatedAt: now, Version: 1})
			}
		}
		if len(items) == 0 {
			return domain.Precondition("metering.create", "workload_session", workloadID, "workload has no capacity segments")
		}
		if err := s.repo.InsertItems(ctx, tx, items); err != nil {
			return err
		}
		return s.appendEffects(ctx, tx, principal, requestID, "metering.create", batchID, "open", now)
	})
	if err != nil {
		return domain.MeteringBatch{}, nil, fmt.Errorf("create metering batch: %w", err)
	}
	return batch, items, nil
}

func (s *Service) Claim(ctx context.Context, principal auth.Principal, batchID, requestID string) (ClaimResult, error) {
	if err := auth.RequireRole(principal, domain.RoleReviewer); err != nil {
		return ClaimResult{}, err
	}
	if batchID == "" || requestID == "" {
		return ClaimResult{}, domain.Validation("metering.claim", "batch and request id are required")
	}
	token, err := newLeaseToken()
	if err != nil {
		return ClaimResult{}, err
	}
	now := s.clock.Now()
	var result ClaimResult
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		batch, err := s.repo.FindBatch(ctx, tx, principal.TenantID, batchID)
		if err != nil {
			return err
		}
		if batch.Status != domain.MeteringOpen && batch.Status != domain.MeteringRework && !(batch.Status == domain.MeteringClaimed && batch.LeaseExpiresAt != nil && !now.Before(*batch.LeaseExpiresAt)) {
			return domain.Conflict("metering.claim", "metering_batch", batchID, "batch has an active owner or is not claimable")
		}
		expires := now.Add(s.leaseTTL)
		if err := s.repo.Claim(ctx, tx, principal.TenantID, batchID, principal.UserID, token, batch.Version, now, expires); err != nil {
			return err
		}
		items, err := s.repo.ListItems(ctx, tx, principal.TenantID, batchID)
		if err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal, requestID, "metering.claim", batchID, "claimed", now); err != nil {
			return err
		}
		batch.Status, batch.Owner, batch.LeaseToken, batch.LeaseExpiresAt = domain.MeteringClaimed, principal.UserID, token, &expires
		batch.UpdatedAt, batch.Version = now, batch.Version+1
		result = ClaimResult{Batch: batch, Items: items}
		return nil
	})
	if err != nil {
		return ClaimResult{}, fmt.Errorf("claim metering batch: %w", err)
	}
	return result, nil
}

func (s *Service) Renew(ctx context.Context, principal auth.Principal, batchID, token, requestID string, version int64) (domain.MeteringBatch, error) {
	if err := auth.RequireRole(principal, domain.RoleReviewer); err != nil {
		return domain.MeteringBatch{}, err
	}
	now := s.clock.Now()
	expires := now.Add(s.leaseTTL)
	var updated domain.MeteringBatch
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		batch, err := s.repo.FindBatch(ctx, tx, principal.TenantID, batchID)
		if err != nil {
			return err
		}
		if batch.Owner != principal.UserID || batch.LeaseToken != token || batch.Version != version {
			return domain.Wrap(domain.ErrLeaseLost, "metering.renew", "metering_batch", batchID, "claim identity or ownership changed", nil)
		}
		if err := s.repo.Renew(ctx, tx, principal.TenantID, batchID, principal.UserID, token, version, now, expires); err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal, requestID, "metering.renew", batchID, "renewed", now); err != nil {
			return err
		}
		batch.LeaseExpiresAt, batch.UpdatedAt, batch.Version = &expires, now, batch.Version+1
		updated = batch
		return nil
	})
	if err != nil {
		return domain.MeteringBatch{}, fmt.Errorf("renew metering batch: %w", err)
	}
	return updated, nil
}

func (s *Service) Record(ctx context.Context, principal auth.Principal, batchID, token, itemID, label, payload, requestID string, itemVersion int64) (domain.MeteringItem, error) {
	if err := auth.RequireRole(principal, domain.RoleReviewer); err != nil {
		return domain.MeteringItem{}, err
	}
	label, payload = strings.TrimSpace(label), strings.TrimSpace(payload)
	if label == "" || payload == "" || len(payload) > 64*1024 {
		return domain.MeteringItem{}, domain.Validation("metering.record", "label and bounded payload are required")
	}
	if !json.Valid([]byte(payload)) {
		return domain.MeteringItem{}, domain.Validation("metering.record", "payload must be valid JSON")
	}
	now := s.clock.Now()
	var updated domain.MeteringItem
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		batch, err := s.repo.FindBatch(ctx, tx, principal.TenantID, batchID)
		if err != nil {
			return err
		}
		if batch.Status != domain.MeteringClaimed || batch.Owner != principal.UserID || batch.LeaseToken != token || batch.LeaseExpiresAt == nil || !now.Before(*batch.LeaseExpiresAt) {
			return domain.Wrap(domain.ErrLeaseLost, "metering.record", "metering_batch", batchID, "active claim is required", nil)
		}
		items, err := s.repo.ListItems(ctx, tx, principal.TenantID, batchID)
		if err != nil {
			return err
		}
		found := false
		for _, item := range items {
			if item.ID == itemID {
				if item.Version != itemVersion {
					return domain.Conflict("metering.record", "metering_item", itemID, "item version changed")
				}
				updated = item
				found = true
				break
			}
		}
		if !found {
			return domain.NotFound("metering.record", "metering_item", itemID)
		}
		if err := s.repo.UpdateItem(ctx, tx, principal.TenantID, batchID, itemID, label, payload, itemVersion, now); err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal, requestID, "metering.item.complete", itemID, "completed", now); err != nil {
			return err
		}
		updated.Label, updated.Payload, updated.Complete, updated.UpdatedAt, updated.Version = label, payload, true, now, updated.Version+1
		return nil
	})
	if err != nil {
		return domain.MeteringItem{}, fmt.Errorf("annotate item: %w", err)
	}
	return updated, nil
}

func (s *Service) Submit(ctx context.Context, principal auth.Principal, batchID, token, requestID string) (domain.MeteringBatch, error) {
	if err := auth.RequireRole(principal, domain.RoleReviewer); err != nil {
		return domain.MeteringBatch{}, err
	}
	now := s.clock.Now()
	var submitted domain.MeteringBatch
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		batch, err := s.repo.FindBatch(ctx, tx, principal.TenantID, batchID)
		if err != nil {
			return err
		}
		if batch.Status != domain.MeteringClaimed || batch.Owner != principal.UserID || batch.LeaseToken != token || batch.LeaseExpiresAt == nil || !now.Before(*batch.LeaseExpiresAt) {
			return domain.Wrap(domain.ErrLeaseLost, "metering.submit", "metering_batch", batchID, "active claim is required", nil)
		}
		incomplete, err := s.repo.CountIncomplete(ctx, tx, principal.TenantID, batchID)
		if err != nil {
			return err
		}
		if incomplete != 0 {
			return domain.Precondition("metering.submit", "metering_batch", batchID, "all items must be completed")
		}
		if err := s.repo.Submit(ctx, tx, principal.TenantID, batchID, principal.UserID, token, batch.Version, now); err != nil {
			return err
		}
		if err := s.appendEffects(ctx, tx, principal, requestID, "metering.submit", batchID, "submitted", now); err != nil {
			return err
		}
		batch.Status, batch.Owner, batch.LeaseToken, batch.LeaseExpiresAt = domain.MeteringSubmitted, "", "", nil
		batch.SubmittedAt, batch.UpdatedAt, batch.Version = &now, now, batch.Version+1
		submitted = batch
		return nil
	})
	if err != nil {
		return domain.MeteringBatch{}, fmt.Errorf("submit metering batch: %w", err)
	}
	return submitted, nil
}

func (s *Service) Review(ctx context.Context, principal auth.Principal, batchID, requestID string, accept bool, reason string) (domain.MeteringBatch, error) {
	if err := auth.RequireRole(principal, domain.RoleDataSteward); err != nil {
		return domain.MeteringBatch{}, err
	}
	if !accept && strings.TrimSpace(reason) == "" {
		return domain.MeteringBatch{}, domain.Validation("metering.review", "rework reason is required")
	}
	now := s.clock.Now()
	var reviewed domain.MeteringBatch
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		batch, err := s.repo.FindBatch(ctx, tx, principal.TenantID, batchID)
		if err != nil {
			return err
		}
		if batch.Status != domain.MeteringSubmitted {
			return domain.Precondition("metering.review", "metering_batch", batchID, "batch must be submitted")
		}
		if err := s.repo.Review(ctx, tx, principal.TenantID, batchID, batch.Version, accept, now); err != nil {
			return err
		}
		outcome := "rework"
		if accept {
			outcome = "accepted"
		}
		if err := s.appendEffectsWithDetail(ctx, tx, principal, requestID, "metering.review", batchID, outcome, reason, now); err != nil {
			return err
		}
		if accept {
			batch.Status = domain.MeteringAccepted
		} else {
			batch.Status = domain.MeteringRework
		}
		batch.ReviewedAt, batch.UpdatedAt, batch.Version = &now, now, batch.Version+1
		reviewed = batch
		return nil
	})
	if err != nil {
		return domain.MeteringBatch{}, fmt.Errorf("review metering batch: %w", err)
	}
	return reviewed, nil
}

func (s *Service) Get(ctx context.Context, principal auth.Principal, batchID string) (domain.MeteringBatch, []domain.MeteringItem, error) {
	batch, err := s.repo.FindBatch(ctx, s.db.SQL(), principal.TenantID, batchID)
	if err != nil {
		return domain.MeteringBatch{}, nil, err
	}
	items, err := s.repo.ListItems(ctx, s.db.SQL(), principal.TenantID, batchID)
	if err != nil {
		return domain.MeteringBatch{}, nil, err
	}
	return batch, items, nil
}

func (s *Service) appendEffects(ctx context.Context, tx *sql.Tx, principal auth.Principal, requestID, action, batchID, outcome string, now time.Time) error {
	return s.appendEffectsWithDetail(ctx, tx, principal, requestID, action, batchID, outcome, "", now)
}

func (s *Service) appendEffectsWithDetail(ctx context.Context, tx *sql.Tx, principal auth.Principal, requestID, action, batchID, outcome, detail string, now time.Time) error {
	auditID, err := domain.NewID("audit")
	if err != nil {
		return err
	}
	eventID, err := domain.NewID("event")
	if err != nil {
		return err
	}
	if err := s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: action, ObjectType: "metering_batch", ObjectID: batchID, Outcome: outcome, RequestID: requestID, Detail: detail, CreatedAt: now}); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]string{"batch_id": batchID, "event": action, "outcome": outcome})
	return s.outbox.Enqueue(ctx, tx, domain.OutboxEvent{ID: eventID, TenantID: principal.TenantID, Topic: action, AggregateType: "metering_batch", AggregateID: batchID, Payload: string(payload), Status: domain.OutboxPending, MaxAttempts: 5, NextAttemptAt: now, CreatedAt: now, UpdatedAt: now, Version: 1})
}
