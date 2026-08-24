package outbox

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/audit"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

type Service struct {
	db        *storage.Database
	repo      Repository
	audits    audit.Store
	clock     clock.Clock
	leaseTTL  time.Duration
	retryBase time.Duration
}

func NewService(db *storage.Database, c clock.Clock, leaseTTL, retryBase time.Duration) *Service {
	return &Service{db: db, repo: Repository{}, audits: audit.Store{}, clock: c, leaseTTL: leaseTTL, retryBase: retryBase}
}

func (s *Service) Claim(ctx context.Context, tenantID, workerID string) (domain.OutboxEvent, error) {
	if tenantID == "" || workerID == "" {
		return domain.OutboxEvent{}, domain.Validation("outbox.claim", "tenant and worker are required")
	}
	token, err := randomLeaseToken()
	if err != nil {
		return domain.OutboxEvent{}, err
	}
	now := s.clock.Now()
	var claimed domain.OutboxEvent
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		due, err := s.repo.ListDue(ctx, tx, tenantID, now, 1)
		if err != nil {
			return err
		}
		if len(due) == 0 {
			return domain.NotFound("outbox.claim", "outbox_event", "due")
		}
		event := due[0]
		if event.AttemptCount >= event.MaxAttempts {
			return domain.Precondition("outbox.claim", "outbox_event", event.ID, "event exhausted its attempt budget")
		}
		expires := now.Add(s.leaseTTL)
		if err := s.repo.Claim(ctx, tx, event, workerID, token, now, expires); err != nil {
			return err
		}
		event.Status, event.Owner, event.LeaseToken, event.LeaseExpiresAt = domain.OutboxDelivering, workerID, token, &expires
		event.AttemptCount, event.UpdatedAt, event.Version = event.AttemptCount+1, now, event.Version+1
		claimed = event
		return nil
	})
	if err != nil {
		return domain.OutboxEvent{}, fmt.Errorf("claim outbox event: %w", err)
	}
	return claimed, nil
}

func (s *Service) Renew(ctx context.Context, tenantID, eventID, workerID, token string, version int64) (domain.OutboxEvent, error) {
	now := s.clock.Now()
	expires := now.Add(s.leaseTTL)
	var renewed domain.OutboxEvent
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		event, err := s.repo.Find(ctx, tx, tenantID, eventID)
		if err != nil {
			return err
		}
		if event.Owner != workerID || event.LeaseToken != token || event.Version != version {
			return domain.Wrap(domain.ErrLeaseLost, "outbox.renew", "outbox_event", eventID, "lease identity or ownership changed", nil)
		}
		if err := s.repo.Renew(ctx, tx, tenantID, eventID, workerID, token, version, now, expires); err != nil {
			return err
		}
		event.LeaseExpiresAt, event.UpdatedAt, event.Version = &expires, now, event.Version+1
		renewed = event
		return nil
	})
	if err != nil {
		return domain.OutboxEvent{}, fmt.Errorf("renew outbox event: %w", err)
	}
	return renewed, nil
}

func (s *Service) Acknowledge(ctx context.Context, tenantID, eventID, workerID, token string, version int64) (domain.OutboxEvent, error) {
	now := s.clock.Now()
	var delivered domain.OutboxEvent
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		event, err := s.repo.Find(ctx, tx, tenantID, eventID)
		if err != nil {
			return err
		}
		if err := s.repo.Acknowledge(ctx, tx, event, workerID, token, now); err != nil {
			return err
		}
		if err := s.auditLifecycle(ctx, tx, event, workerID, "outbox.delivered", "delivered", now); err != nil {
			return err
		}
		event.Status, event.Owner, event.LeaseToken, event.LeaseExpiresAt = domain.OutboxDelivered, "", "", nil
		event.LastError, event.UpdatedAt, event.Version = "", now, event.Version+1
		delivered = event
		return nil
	})
	if err != nil {
		return domain.OutboxEvent{}, fmt.Errorf("acknowledge outbox event: %w", err)
	}
	return delivered, nil
}

func (s *Service) Fail(ctx context.Context, tenantID, eventID, workerID, token, message string, version int64, permanent bool) (domain.OutboxEvent, error) {
	message = strings.TrimSpace(message)
	if message == "" {
		return domain.OutboxEvent{}, domain.Validation("outbox.fail", "error message is required")
	}
	now := s.clock.Now()
	var failed domain.OutboxEvent
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		event, err := s.repo.Find(ctx, tx, tenantID, eventID)
		if err != nil {
			return err
		}
		if event.Owner != workerID || event.LeaseToken != token || event.Version != version {
			return domain.Wrap(domain.ErrLeaseLost, "outbox.fail", "outbox_event", eventID, "lease identity or ownership changed", nil)
		}
		retry := !permanent && event.AttemptCount < event.MaxAttempts
		nextAttempt := now
		status, outcome := domain.OutboxDead, "dead"
		if retry {
			status, outcome = domain.OutboxPending, "retrying"
			nextAttempt = now.Add(s.backoff(event.AttemptCount))
		}
		if err := s.repo.Fail(ctx, tx, event, workerID, token, message, retry, nextAttempt, now); err != nil {
			return err
		}
		if err := s.auditLifecycle(ctx, tx, event, workerID, "outbox.failed", outcome, now); err != nil {
			return err
		}
		event.Status, event.Owner, event.LeaseToken, event.LeaseExpiresAt = status, "", "", nil
		event.LastError, event.NextAttemptAt, event.UpdatedAt, event.Version = message, nextAttempt, now, event.Version+1
		failed = event
		return nil
	})
	if err != nil {
		return domain.OutboxEvent{}, fmt.Errorf("fail outbox event: %w", err)
	}
	return failed, nil
}

func (s *Service) Get(ctx context.Context, tenantID, eventID string) (domain.OutboxEvent, error) {
	return s.repo.Find(ctx, s.db.SQL(), tenantID, eventID)
}

func (s *Service) backoff(attempt int) time.Duration {
	exponent := math.Min(float64(attempt-1), 8)
	return time.Duration(float64(s.retryBase) * math.Pow(2, exponent))
}

func (s *Service) auditLifecycle(ctx context.Context, tx *sql.Tx, event domain.OutboxEvent, workerID, action, outcome string, now time.Time) error {
	id, err := domain.NewID("audit")
	if err != nil {
		return err
	}
	return s.audits.Append(ctx, tx, audit.Record{ID: id, TenantID: event.TenantID, ActorID: workerID, Action: action, ObjectType: "outbox_event", ObjectID: event.ID, Outcome: outcome, RequestID: "worker:" + workerID, CreatedAt: now})
}

func randomLeaseToken() (string, error) {
	value := make([]byte, 24)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate outbox lease token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
