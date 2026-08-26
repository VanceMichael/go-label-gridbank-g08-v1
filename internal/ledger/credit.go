package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/audit"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

// Credit operations live beside release bookkeeping because both are tenant
// scoped ledgers. Every balance mutation and its immutable entry share one tx.
func (s *Service) OpenCreditAccount(ctx context.Context, principal auth.Principal, currency, requestID string) (domain.CreditAccount, error) {
	if err := auth.RequireRole(principal, domain.RoleTenantAdmin, domain.RoleDataSteward); err != nil {
		return domain.CreditAccount{}, err
	}
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if len(currency) != 3 || requestID == "" {
		return domain.CreditAccount{}, domain.Validation("credit.open_account", "three-letter currency and request id are required")
	}
	now := s.clock.Now()
	account := domain.CreditAccount{}
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		id, err := domain.NewID("credit")
		if err != nil {
			return err
		}
		account = domain.CreditAccount{ID: id, TenantID: principal.TenantID, Currency: currency, CreatedAt: now, UpdatedAt: now, Version: 1}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO credit_accounts(id, tenant_id, currency, balance, held, created_at, updated_at, version)
			VALUES(?, ?, ?, 0, 0, ?, ?, 1)`, id, principal.TenantID, currency, storage.FormatTime(now), storage.FormatTime(now)); err != nil {
			return fmt.Errorf("insert credit account: %w", err)
		}
		return s.creditEffects(ctx, tx, principal, requestID, "credit.account.open", id, "opened", now)
	})
	if err != nil {
		return domain.CreditAccount{}, fmt.Errorf("open credit account: %w", err)
	}
	return account, nil
}

func (s *Service) Deposit(ctx context.Context, principal auth.Principal, accountID string, amount int64, key, requestID string) (domain.CreditAccount, error) {
	if err := auth.RequireRole(principal, domain.RoleTenantAdmin, domain.RoleDataSteward); err != nil {
		return domain.CreditAccount{}, err
	}
	if accountID == "" || amount <= 0 || strings.TrimSpace(key) == "" || requestID == "" {
		return domain.CreditAccount{}, domain.Validation("credit.deposit", "account, positive amount, idempotency key, and request id are required")
	}
	now := s.clock.Now()
	var account domain.CreditAccount
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		if err := s.lockAccount(ctx, tx, principal.TenantID, accountID, &account); err != nil {
			return err
		}
		var existing string
		err := tx.QueryRowContext(ctx, `SELECT id FROM credit_entries WHERE tenant_id = ? AND account_id = ? AND idempotency_key = ?`, principal.TenantID, accountID, key).Scan(&existing)
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("check deposit idempotency: %w", err)
		}
		entryID, err := domain.NewID("credit_entry")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE credit_accounts SET balance = balance + ?, updated_at = ?, version = version + 1 WHERE tenant_id = ? AND id = ? AND version = ?`, amount, storage.FormatTime(now), principal.TenantID, accountID, account.Version); err != nil {
			return fmt.Errorf("deposit balance: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO credit_entries(id, tenant_id, account_id, kind, amount, idempotency_key, created_at) VALUES(?, ?, ?, 'deposit', ?, ?, ?)`, entryID, principal.TenantID, accountID, amount, key, storage.FormatTime(now)); err != nil {
			return fmt.Errorf("record deposit: %w", err)
		}
		account.Balance += amount
		account.UpdatedAt, account.Version = now, account.Version+1
		return s.creditEffects(ctx, tx, principal, requestID, "credit.deposit", accountID, "deposited", now)
	})
	if err != nil {
		return domain.CreditAccount{}, fmt.Errorf("deposit credits: %w", err)
	}
	return account, nil
}

func (s *Service) Reserve(ctx context.Context, principal auth.Principal, accountID, workloadID string, amount int64, key, requestID string) (domain.CreditAccount, error) {
	if err := auth.RequireRole(principal, domain.RoleOperator, domain.RoleDataSteward); err != nil {
		return domain.CreditAccount{}, err
	}
	if accountID == "" || workloadID == "" || amount <= 0 || strings.TrimSpace(key) == "" || requestID == "" {
		return domain.CreditAccount{}, domain.Validation("credit.reserve", "account, workload, positive amount, idempotency key, and request id are required")
	}
	now := s.clock.Now()
	var account domain.CreditAccount
	err := s.db.Write(ctx, func(tx *sql.Tx) error {
		if err := s.lockAccount(ctx, tx, principal.TenantID, accountID, &account); err != nil {
			return err
		}
		var existingKind string
		err := tx.QueryRowContext(ctx, `SELECT kind FROM credit_entries WHERE tenant_id = ? AND account_id = ? AND idempotency_key = ?`, principal.TenantID, accountID, key).Scan(&existingKind)
		if err == nil {
			if existingKind != string(domain.CreditHold) {
				return domain.Conflict("credit.reserve", "credit_entry", key, "idempotency key belongs to another operation")
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("check reserve idempotency: %w", err)
		}
		if account.Balance-account.Held < amount {
			return domain.Conflict("credit.reserve", "credit_account", accountID, "available credits are insufficient")
		}
		entryID, err := domain.NewID("credit_entry")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE credit_accounts SET held = held + ?, updated_at = ?, version = version + 1 WHERE tenant_id = ? AND id = ? AND version = ? AND balance - held >= ?`, amount, storage.FormatTime(now), principal.TenantID, accountID, account.Version, amount); err != nil {
			if storage.IsBusy(err) {
				return domain.Conflict("credit.reserve", "credit_account", accountID, "another reservation is updating the account")
			}
			return fmt.Errorf("reserve credit balance: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO credit_entries(id, tenant_id, account_id, workload_id, kind, amount, idempotency_key, created_at) VALUES(?, ?, ?, ?, 'hold', ?, ?, ?)`, entryID, principal.TenantID, accountID, workloadID, amount, key, storage.FormatTime(now)); err != nil {
			return fmt.Errorf("record credit hold: %w", err)
		}
		account.Held += amount
		account.UpdatedAt, account.Version = now, account.Version+1
		return s.creditEffects(ctx, tx, principal, requestID, "credit.reserve", workloadID, "held", now)
	})
	if err != nil {
		return domain.CreditAccount{}, fmt.Errorf("reserve credits: %w", err)
	}
	return account, nil
}

func (s *Service) Account(ctx context.Context, principal auth.Principal, accountID string) (domain.CreditAccount, error) {
	var account domain.CreditAccount
	err := s.lockAccount(ctx, s.db.SQL(), principal.TenantID, accountID, &account)
	return account, err
}

func (s *Service) lockAccount(ctx context.Context, q storage.Queryer, tenantID, accountID string, target *domain.CreditAccount) error {
	var created, updated string
	err := q.QueryRowContext(ctx, `SELECT id, tenant_id, currency, balance, held, created_at, updated_at, version FROM credit_accounts WHERE tenant_id = ? AND id = ?`, tenantID, accountID).Scan(&target.ID, &target.TenantID, &target.Currency, &target.Balance, &target.Held, &created, &updated, &target.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.NotFound("credit.account", "credit_account", accountID)
	}
	if err != nil {
		return fmt.Errorf("read credit account: %w", err)
	}
	var parseErr error
	if target.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
		return fmt.Errorf("parse credit account created_at: %w", parseErr)
	}
	if target.UpdatedAt, parseErr = storage.ParseTime(updated); parseErr != nil {
		return fmt.Errorf("parse credit account updated_at: %w", parseErr)
	}
	return nil
}

func (s *Service) creditEffects(ctx context.Context, tx *sql.Tx, principal auth.Principal, requestID, action, objectID, outcome string, now time.Time) error {
	auditID, err := domain.NewID("audit")
	if err != nil {
		return err
	}
	return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: action, ObjectType: "credit", ObjectID: objectID, Outcome: outcome, RequestID: requestID, CreatedAt: now})
}
