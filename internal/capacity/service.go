package capacity

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/audit"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
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
	audits    audit.Store
	outbox    outbox.Repository
	clock     clock.Clock
}

type SegmentInput struct {
	Sequence       int
	StartNanos     int64
	EndNanos       int64
	ObjectURI      string
	Checksum       string
	IdempotencyKey string
}

type AppendResult struct {
	Segments []domain.CapacitySegment
	Inserted int
	Replayed int
}

func NewService(db *storage.Database, c clock.Clock) *Service {
	return &Service{db: db, repo: Repository{}, workloads: workload.Repository{}, audits: audit.Store{}, outbox: outbox.Repository{}, clock: c}
}

func (s *Service) OpenStream(ctx context.Context, principal auth.Principal, workloadID string, kind domain.CapacityKind, requestID string) (domain.CapacityStream, error) {
	if err := auth.RequireRole(principal, domain.RoleOperator); err != nil {
		return domain.CapacityStream{}, err
	}
	if workloadID == "" || !kind.Valid() || requestID == "" {
		return domain.CapacityStream{}, domain.Validation("capacity.open_manifest", "workload, capacity kind, and request id are required")
	}
	id, err := domain.NewID("manifest")
	if err != nil {
		return domain.CapacityStream{}, err
	}
	auditID, err := domain.NewID("audit")
	if err != nil {
		return domain.CapacityStream{}, err
	}
	now := s.clock.Now()
	manifest := domain.CapacityStream{ID: id, TenantID: principal.TenantID, WorkloadID: workloadID, Kind: kind, Status: domain.StreamOpen, CreatedAt: now, UpdatedAt: now, Version: 1}
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		workloadValue, err := s.workloads.Find(ctx, tx, principal.TenantID, workloadID)
		if err != nil {
			return err
		}
		if workloadValue.OperatorID != principal.UserID {
			return domain.Wrap(domain.ErrForbidden, "capacity.open_manifest", "workload_session", workloadID, "operator does not own workload", nil)
		}
		if workloadValue.Status != domain.WorkloadAllocated && workloadValue.Status != domain.WorkloadRunning {
			return domain.Precondition("capacity.open_manifest", "workload_session", workloadID, "workload must be ready or executing")
		}
		if _, err := s.repo.FindStreamByKind(ctx, tx, principal.TenantID, workloadID, kind); err == nil {
			return domain.Conflict("capacity.open_manifest", "capacity_stream", workloadID+":"+string(kind), "capacity kind already exists")
		} else if !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		if err := s.repo.InsertStream(ctx, tx, manifest); err != nil {
			return err
		}
		return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: "capacity.open", ObjectType: "capacity_stream", ObjectID: manifest.ID, Outcome: "open", RequestID: requestID, CreatedAt: now})
	})
	if err != nil {
		return domain.CapacityStream{}, fmt.Errorf("open capacity manifest: %w", err)
	}
	return manifest, nil
}

func (s *Service) Append(ctx context.Context, principal auth.Principal, streamID, requestID string, inputs []SegmentInput) (AppendResult, error) {
	if err := auth.RequireRole(principal, domain.RoleOperator); err != nil {
		return AppendResult{}, err
	}
	if streamID == "" || requestID == "" || len(inputs) == 0 || len(inputs) > 250 {
		return AppendResult{}, domain.Validation("capacity.append", "manifest, request id, and 1-250 segments are required")
	}
	if err := validateInputs(inputs); err != nil {
		return AppendResult{}, err
	}
	now := s.clock.Now()
	appendCtx := context.Background()
	result := AppendResult{Segments: make([]domain.CapacitySegment, 0, len(inputs))}
	err := s.db.Write(appendCtx, func(tx *sql.Tx) error {
		manifest, err := s.repo.FindStream(appendCtx, tx, principal.TenantID, streamID)
		if err != nil {
			return err
		}
		workloadValue, err := s.workloads.Find(appendCtx, tx, principal.TenantID, manifest.WorkloadID)
		if err != nil {
			return err
		}
		if workloadValue.OperatorID != principal.UserID {
			return domain.Wrap(domain.ErrForbidden, "capacity.append", "workload_session", workloadValue.ID, "operator does not own workload", nil)
		}
		if workloadValue.Status != domain.WorkloadRunning || manifest.Status != domain.StreamOpen {
			return domain.Precondition("capacity.append", "capacity_stream", streamID, "workload must be executing and manifest open")
		}
		for _, input := range inputs {
			existing, found, err := s.repo.FindSegmentByKey(appendCtx, tx, principal.TenantID, streamID, input.IdempotencyKey)
			if err != nil {
				return err
			}
			if found {
				if !sameSegment(existing, input) {
					return domain.Wrap(domain.ErrIdempotencyConflict, "capacity.append", "capacity_segment", existing.ID, "idempotency key represents different segment data", nil)
				}
				result.Segments = append(result.Segments, existing)
				result.Replayed++
				continue
			}
			id, err := domain.NewID("segment")
			if err != nil {
				return err
			}
			segment := domain.CapacitySegment{ID: id, TenantID: principal.TenantID, StreamID: streamID, Sequence: input.Sequence, StartNanos: input.StartNanos, EndNanos: input.EndNanos, ObjectURI: input.ObjectURI, Checksum: strings.ToLower(input.Checksum), IdempotencyKey: input.IdempotencyKey, CreatedAt: now}
			if err := s.repo.InsertSegment(appendCtx, tx, segment); err != nil {
				return err
			}
			result.Segments = append(result.Segments, segment)
			result.Inserted++
		}
		if result.Inserted > 0 {
			if err := s.repo.RefreshAggregate(appendCtx, tx, principal.TenantID, streamID, manifest.Version, storage.FormatTime(now)); err != nil {
				return err
			}
		}
		auditID, err := domain.NewID("audit")
		if err != nil {
			return err
		}
		detail, _ := json.Marshal(map[string]int{"inserted": result.Inserted, "replayed": result.Replayed})
		return s.audits.Append(appendCtx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: "capacity.append", ObjectType: "capacity_stream", ObjectID: streamID, Outcome: "accepted", RequestID: requestID, Detail: string(detail), CreatedAt: now})
	})
	if err != nil {
		return AppendResult{}, fmt.Errorf("append capacity segments: %w", err)
	}
	return result, nil
}

func (s *Service) Seal(ctx context.Context, principal auth.Principal, streamID, requestID string) (domain.CapacityStream, error) {
	if err := auth.RequireRole(principal, domain.RoleOperator); err != nil {
		return domain.CapacityStream{}, err
	}
	now := s.clock.Now()
	var sealed domain.CapacityStream
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		manifest, err := s.repo.FindStream(ctx, tx, principal.TenantID, streamID)
		if err != nil {
			return err
		}
		workloadValue, err := s.workloads.Find(ctx, tx, principal.TenantID, manifest.WorkloadID)
		if err != nil {
			return err
		}
		if workloadValue.Status != domain.WorkloadRunning || workloadValue.OperatorID != principal.UserID {
			return domain.Precondition("capacity.seal", "workload_session", workloadValue.ID, "owned workload must be executing")
		}
		if err := manifest.Status.Transition(domain.StreamSealed); err != nil {
			return err
		}
		segments, err := s.repo.ListSegments(ctx, tx, principal.TenantID, streamID)
		if err != nil {
			return err
		}
		if len(segments) == 0 {
			return domain.Precondition("capacity.seal", "capacity_stream", streamID, "manifest has no segments")
		}
		if err := validateContinuity(segments); err != nil {
			return err
		}
		digest := segmentDigest(segments)
		if err := s.repo.TransitionStream(ctx, tx, principal.TenantID, streamID, manifest.Status, domain.StreamSealed, manifest.Version, digest, storage.FormatTime(now)); err != nil {
			return err
		}
		auditID, err := domain.NewID("audit")
		if err != nil {
			return err
		}
		if err := s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: "capacity.seal", ObjectType: "capacity_stream", ObjectID: streamID, Outcome: "sealed", RequestID: requestID, Detail: digest, CreatedAt: now}); err != nil {
			return err
		}
		manifest.Status, manifest.Digest, manifest.UpdatedAt, manifest.Version = domain.StreamSealed, digest, now, manifest.Version+1
		sealed = manifest
		return nil
	})
	if err != nil {
		return domain.CapacityStream{}, fmt.Errorf("seal capacity manifest: %w", err)
	}
	return sealed, nil
}

func (s *Service) AlignWorkload(ctx context.Context, principal auth.Principal, workloadID, requestID string, tolerance time.Duration) ([]domain.CapacityStream, error) {
	if err := auth.RequireRole(principal, domain.RoleOperator, domain.RoleReviewer); err != nil {
		return nil, err
	}
	if tolerance < 0 || tolerance > 5*time.Second {
		return nil, domain.Validation("capacity.align", "tolerance must be between zero and five seconds")
	}
	now := s.clock.Now()
	var aligned []domain.CapacityStream
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		workloadValue, err := s.workloads.Find(ctx, tx, principal.TenantID, workloadID)
		if err != nil {
			return err
		}
		if workloadValue.Status != domain.WorkloadRunning {
			return domain.Precondition("capacity.align", "workload_session", workloadID, "workload must be executing")
		}
		if principal.Role == domain.RoleOperator && workloadValue.OperatorID != principal.UserID {
			return domain.Wrap(domain.ErrForbidden, "capacity.align", "workload_session", workloadID, "operator does not own workload", nil)
		}
		manifests, err := s.repo.ListStreams(ctx, tx, principal.TenantID, workloadID)
		if err != nil {
			return err
		}
		if len(manifests) < 2 {
			return domain.Precondition("capacity.align", "workload_session", workloadID, "at least two capacitys are required")
		}
		if err := validateAlignment(manifests, tolerance); err != nil {
			return err
		}
		for i := range manifests {
			manifest := &manifests[i]
			if err := manifest.Status.Transition(domain.StreamAligned); err != nil {
				return err
			}
			if err := s.repo.TransitionStream(ctx, tx, principal.TenantID, manifest.ID, domain.StreamSealed, domain.StreamAligned, manifest.Version, "", storage.FormatTime(now)); err != nil {
				return err
			}
			manifest.Status, manifest.UpdatedAt, manifest.Version = domain.StreamAligned, now, manifest.Version+1
		}
		auditID, err := domain.NewID("audit")
		if err != nil {
			return err
		}
		outboxID, err := domain.NewID("event")
		if err != nil {
			return err
		}
		if err := s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: "capacity.align", ObjectType: "workload_session", ObjectID: workloadID, Outcome: "aligned", RequestID: requestID, CreatedAt: now}); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"workload_id": workloadID, "manifest_count": len(manifests)})
		if err := s.outbox.Enqueue(ctx, tx, domain.OutboxEvent{ID: outboxID, TenantID: principal.TenantID, Topic: "capacity.aligned", AggregateType: "workload_session", AggregateID: workloadID, Payload: string(payload), Status: domain.OutboxPending, MaxAttempts: 5, NextAttemptAt: now, CreatedAt: now, UpdatedAt: now, Version: 1}); err != nil {
			return err
		}
		aligned = manifests
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("align workload streams: %w", err)
	}
	return aligned, nil
}

func (s *Service) List(ctx context.Context, principal auth.Principal, workloadID string) ([]domain.CapacityStream, error) {
	if _, err := s.workloads.Find(ctx, s.db.SQL(), principal.TenantID, workloadID); err != nil {
		return nil, err
	}
	return s.repo.ListStreams(ctx, s.db.SQL(), principal.TenantID, workloadID)
}

func validateInputs(inputs []SegmentInput) error {
	sequences := make(map[int]struct{}, len(inputs))
	keys := make(map[string]struct{}, len(inputs))
	for i, input := range inputs {
		if input.Sequence < 0 || input.StartNanos < 0 || input.EndNanos <= input.StartNanos {
			return domain.Validation("capacity.append", fmt.Sprintf("segment %d has an invalid sequence or time range", i))
		}
		if strings.TrimSpace(input.ObjectURI) == "" || strings.TrimSpace(input.Checksum) == "" || strings.TrimSpace(input.IdempotencyKey) == "" {
			return domain.Validation("capacity.append", fmt.Sprintf("segment %d requires object URI, checksum, and idempotency key", i))
		}
		if len(input.Checksum) != 64 {
			return domain.Validation("capacity.append", fmt.Sprintf("segment %d checksum must be a SHA-256 hex digest", i))
		}
		if _, err := hex.DecodeString(input.Checksum); err != nil {
			return domain.Validation("capacity.append", fmt.Sprintf("segment %d checksum is not hexadecimal", i))
		}
		if _, exists := sequences[input.Sequence]; exists {
			return domain.Validation("capacity.append", fmt.Sprintf("segment %d repeats a sequence within the batch", i))
		}
		if _, exists := keys[input.IdempotencyKey]; exists {
			return domain.Validation("capacity.append", fmt.Sprintf("segment %d repeats an idempotency key within the batch", i))
		}
		sequences[input.Sequence], keys[input.IdempotencyKey] = struct{}{}, struct{}{}
	}
	return nil
}

func sameSegment(existing domain.CapacitySegment, input SegmentInput) bool {
	return existing.Sequence == input.Sequence && existing.StartNanos == input.StartNanos &&
		existing.EndNanos == input.EndNanos && existing.ObjectURI == input.ObjectURI &&
		existing.Checksum == strings.ToLower(input.Checksum)
}

func validateContinuity(segments []domain.CapacitySegment) error {
	sorted := append([]domain.CapacitySegment(nil), segments...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Sequence < sorted[j].Sequence })
	for index, segment := range sorted {
		if segment.Sequence != index {
			return domain.Precondition("capacity.seal", "capacity_stream", segment.StreamID, "segment sequence must be contiguous from zero")
		}
		if index > 0 && segment.StartNanos < sorted[index-1].EndNanos {
			return domain.Precondition("capacity.seal", "capacity_stream", segment.StreamID, "segment time ranges overlap")
		}
	}
	return nil
}

func segmentDigest(segments []domain.CapacitySegment) string {
	h := sha256.New()
	for _, segment := range segments {
		fmt.Fprintf(h, "%d\x00%d\x00%d\x00%s\x00%s\n", segment.Sequence, segment.StartNanos, segment.EndNanos, segment.ObjectURI, segment.Checksum)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func validateAlignment(manifests []domain.CapacityStream, tolerance time.Duration) error {
	maxStart := int64(0)
	minEnd := int64(^uint64(0) >> 1)
	for _, manifest := range manifests {
		if manifest.Status != domain.StreamSealed || manifest.SegmentCount == 0 {
			return domain.Precondition("capacity.align", "capacity_stream", manifest.ID, "all manifests must be non-empty and sealed")
		}
		if manifest.FirstNanos > maxStart {
			maxStart = manifest.FirstNanos
		}
		if manifest.LastNanos < minEnd {
			minEnd = manifest.LastNanos
		}
	}
	toleranceNanos := tolerance.Nanoseconds()
	if maxStart-minEnd > toleranceNanos {
		return domain.Precondition("capacity.align", "workload_session", manifests[0].WorkloadID, "capacity time ranges do not overlap within tolerance")
	}
	return nil
}
