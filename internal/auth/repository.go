package auth

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

func (Repository) InsertTenant(ctx context.Context, q storage.Queryer, tenant domain.Tenant) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO tenants(id, name, active, created_at, updated_at, version)
		VALUES(?, ?, ?, ?, ?, ?)`, tenant.ID, tenant.Name, tenant.Active,
		storage.FormatTime(tenant.CreatedAt), storage.FormatTime(tenant.UpdatedAt), tenant.Version)
	if err != nil {
		return fmt.Errorf("insert tenant: %w", err)
	}
	return nil
}

func (Repository) InsertUser(ctx context.Context, q storage.Queryer, user domain.User) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO users(
			id, tenant_id, email, display_name, password_hash, role,
			active, created_at, updated_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		user.ID, user.TenantID, strings.ToLower(user.Email), user.DisplayName,
		user.PasswordHash, user.Role, user.Active, storage.FormatTime(user.CreatedAt),
		storage.FormatTime(user.UpdatedAt), user.Version)
	if err != nil {
		return fmt.Errorf("insert user: %w", err)
	}
	return nil
}

func (Repository) FindUserByEmail(ctx context.Context, q storage.Queryer, tenantID, email string) (domain.User, error) {
	var user domain.User
	var created, updated string
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, email, display_name, password_hash, role,
		       active, created_at, updated_at, version
		FROM users WHERE tenant_id = ? AND email = ?`, tenantID, strings.ToLower(email),
	).Scan(&user.ID, &user.TenantID, &user.Email, &user.DisplayName,
		&user.PasswordHash, &user.Role, &user.Active, &created, &updated, &user.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.User{}, domain.NotFound("auth.find_user", "user", email)
	}
	if err != nil {
		return domain.User{}, fmt.Errorf("find user by email: %w", err)
	}
	var parseErr error
	if user.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
		return domain.User{}, fmt.Errorf("parse user created_at: %w", parseErr)
	}
	if user.UpdatedAt, parseErr = storage.ParseTime(updated); parseErr != nil {
		return domain.User{}, fmt.Errorf("parse user updated_at: %w", parseErr)
	}
	return user, nil
}

func (Repository) FindUser(ctx context.Context, q storage.Queryer, tenantID, userID string) (domain.User, error) {
	var user domain.User
	var created, updated string
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, email, display_name, password_hash, role,
		       active, created_at, updated_at, version
		FROM users WHERE tenant_id = ? AND id = ?`, tenantID, userID,
	).Scan(&user.ID, &user.TenantID, &user.Email, &user.DisplayName,
		&user.PasswordHash, &user.Role, &user.Active, &created, &updated, &user.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.User{}, domain.NotFound("auth.find_user", "user", userID)
	}
	if err != nil {
		return domain.User{}, fmt.Errorf("find user: %w", err)
	}
	user.CreatedAt, err = storage.ParseTime(created)
	if err != nil {
		return domain.User{}, fmt.Errorf("parse user created_at: %w", err)
	}
	user.UpdatedAt, err = storage.ParseTime(updated)
	if err != nil {
		return domain.User{}, fmt.Errorf("parse user updated_at: %w", err)
	}
	return user, nil
}

func (Repository) InsertSession(ctx context.Context, q storage.Queryer, session domain.AuthSession) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO auth_sessions(
			id, tenant_id, user_id, token_hash, expires_at, revoked_at, created_at, version
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		session.ID, session.TenantID, session.UserID, session.TokenHash,
		storage.FormatTime(session.ExpiresAt), storage.NullableTime(session.RevokedAt),
		storage.FormatTime(session.CreatedAt), session.Version)
	if err != nil {
		return fmt.Errorf("insert auth session: %w", err)
	}
	return nil
}

func (Repository) FindSessionByToken(ctx context.Context, q storage.Queryer, tokenHash string) (domain.AuthSession, error) {
	var session domain.AuthSession
	var expires, created string
	var revoked sql.NullString
	err := q.QueryRowContext(ctx, `
		SELECT id, tenant_id, user_id, token_hash, expires_at, revoked_at, created_at, version
		FROM auth_sessions WHERE token_hash = ?`, tokenHash,
	).Scan(&session.ID, &session.TenantID, &session.UserID, &session.TokenHash,
		&expires, &revoked, &created, &session.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.AuthSession{}, domain.Wrap(domain.ErrUnauthorized, "auth.find_session", "", "", "invalid session", nil)
	}
	if err != nil {
		return domain.AuthSession{}, fmt.Errorf("find auth session: %w", err)
	}
	var parseErr error
	if session.ExpiresAt, parseErr = storage.ParseTime(expires); parseErr != nil {
		return domain.AuthSession{}, fmt.Errorf("parse session expires_at: %w", parseErr)
	}
	if session.CreatedAt, parseErr = storage.ParseTime(created); parseErr != nil {
		return domain.AuthSession{}, fmt.Errorf("parse session created_at: %w", parseErr)
	}
	if session.RevokedAt, parseErr = storage.ScanNullableTime(revoked); parseErr != nil {
		return domain.AuthSession{}, fmt.Errorf("parse session revoked_at: %w", parseErr)
	}
	return session, nil
}

func (Repository) RevokeSession(ctx context.Context, q storage.Queryer, tenantID, sessionID string, version int64, now time.Time) error {
	result, err := q.ExecContext(ctx, `
		UPDATE auth_sessions
		SET revoked_at = ?, version = version + 1
		WHERE tenant_id = ? AND id = ? AND version = ? AND revoked_at IS NULL`,
		storage.FormatTime(now), tenantID, sessionID, version)
	if err != nil {
		return fmt.Errorf("revoke auth session: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read revoke result: %w", err)
	}
	if changed != 1 {
		return domain.Conflict("auth.revoke_session", "auth_session", sessionID, "session changed or was already revoked")
	}
	return nil
}
