package auth

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/audit"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/clock"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/storage"
)

type Service struct {
	db         *storage.Database
	repo       Repository
	audits     audit.Store
	clock      clock.Clock
	sessionTTL time.Duration
}

type BootstrapInput struct {
	TenantName  string `json:"tenant_name"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	Password    string `json:"password"`
}

type LoginInput struct {
	TenantID  string
	Email     string
	Password  string
	RequestID string
}

type LoginResult struct {
	Token     string      `json:"token"`
	SessionID string      `json:"session_id"`
	TenantID  string      `json:"tenant_id"`
	UserID    string      `json:"user_id"`
	Role      domain.Role `json:"role"`
	ExpiresAt time.Time   `json:"expires_at"`
}

type Principal struct {
	TenantID  string
	UserID    string
	Role      domain.Role
	SessionID string
}

func NewService(db *storage.Database, c clock.Clock, sessionTTL time.Duration) *Service {
	return &Service{db: db, repo: Repository{}, audits: audit.Store{}, clock: c, sessionTTL: sessionTTL}
}

func (s *Service) Bootstrap(ctx context.Context, input BootstrapInput) (domain.Tenant, domain.User, error) {
	input.TenantName = strings.TrimSpace(input.TenantName)
	input.Email = strings.ToLower(strings.TrimSpace(input.Email))
	input.DisplayName = strings.TrimSpace(input.DisplayName)
	if input.TenantName == "" || input.DisplayName == "" {
		return domain.Tenant{}, domain.User{}, domain.Validation("auth.bootstrap", "tenant and display name are required")
	}
	if _, err := mail.ParseAddress(input.Email); err != nil {
		return domain.Tenant{}, domain.User{}, domain.Validation("auth.bootstrap", "email is invalid")
	}
	passwordHash, err := HashPassword(input.Password)
	if err != nil {
		return domain.Tenant{}, domain.User{}, domain.Validation("auth.bootstrap", err.Error())
	}
	tenantID, err := domain.NewID("tenant")
	if err != nil {
		return domain.Tenant{}, domain.User{}, err
	}
	userID, err := domain.NewID("user")
	if err != nil {
		return domain.Tenant{}, domain.User{}, err
	}
	auditID, err := domain.NewID("audit")
	if err != nil {
		return domain.Tenant{}, domain.User{}, err
	}
	now := s.clock.Now()
	tenant := domain.Tenant{ID: tenantID, Name: input.TenantName, Active: true, CreatedAt: now, UpdatedAt: now, Version: 1}
	user := domain.User{ID: userID, TenantID: tenantID, Email: input.Email, DisplayName: input.DisplayName, PasswordHash: passwordHash, Role: domain.RoleTenantAdmin, Active: true, CreatedAt: now, UpdatedAt: now, Version: 1}
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		if err := s.repo.InsertTenant(ctx, tx, tenant); err != nil {
			return err
		}
		if err := s.repo.InsertUser(ctx, tx, user); err != nil {
			return err
		}
		return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: tenantID, ActorID: userID, Action: "tenant.bootstrap", ObjectType: "tenant", ObjectID: tenantID, Outcome: "created", RequestID: "bootstrap", CreatedAt: now})
	})
	if err != nil {
		return domain.Tenant{}, domain.User{}, fmt.Errorf("bootstrap tenant: %w", err)
	}
	return tenant, user, nil
}

func (s *Service) CreateUser(ctx context.Context, principal Principal, email, displayName, password string, role domain.Role, requestID string) (domain.User, error) {
	if principal.Role != domain.RoleTenantAdmin {
		return domain.User{}, domain.Wrap(domain.ErrForbidden, "auth.create_user", "", "", "tenant administrator role required", nil)
	}
	email = strings.ToLower(strings.TrimSpace(email))
	displayName = strings.TrimSpace(displayName)
	if _, err := mail.ParseAddress(email); err != nil || displayName == "" || !role.Valid() {
		return domain.User{}, domain.Validation("auth.create_user", "email, display name, or role is invalid")
	}
	hash, err := HashPassword(password)
	if err != nil {
		return domain.User{}, domain.Validation("auth.create_user", err.Error())
	}
	userID, err := domain.NewID("user")
	if err != nil {
		return domain.User{}, err
	}
	auditID, err := domain.NewID("audit")
	if err != nil {
		return domain.User{}, err
	}
	now := s.clock.Now()
	user := domain.User{ID: userID, TenantID: principal.TenantID, Email: email, DisplayName: displayName, PasswordHash: hash, Role: role, Active: true, CreatedAt: now, UpdatedAt: now, Version: 1}
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		if err := s.repo.InsertUser(ctx, tx, user); err != nil {
			return err
		}
		return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: "user.create", ObjectType: "user", ObjectID: user.ID, Outcome: "created", RequestID: requestID, CreatedAt: now})
	})
	if err != nil {
		return domain.User{}, fmt.Errorf("create user: %w", err)
	}
	return user, nil
}

func (s *Service) Login(ctx context.Context, input LoginInput) (LoginResult, error) {
	if strings.TrimSpace(input.TenantID) == "" || strings.TrimSpace(input.Email) == "" || input.Password == "" || strings.TrimSpace(input.RequestID) == "" {
		return LoginResult{}, domain.Validation("auth.login", "tenant, email, password, and request id are required")
	}
	user, err := s.repo.FindUserByEmail(ctx, s.db.SQL(), input.TenantID, input.Email)
	if err != nil || !user.Active || !VerifyPassword(user.PasswordHash, input.Password) {
		return LoginResult{}, domain.Wrap(domain.ErrUnauthorized, "auth.login", "", "", "invalid credentials", nil)
	}
	token, err := randomToken()
	if err != nil {
		return LoginResult{}, err
	}
	sessionID, err := domain.NewID("session")
	if err != nil {
		return LoginResult{}, err
	}
	auditID, err := domain.NewID("audit")
	if err != nil {
		return LoginResult{}, err
	}
	now := s.clock.Now()
	session := domain.AuthSession{ID: sessionID, TenantID: user.TenantID, UserID: user.ID, TokenHash: domain.HashToken(token), ExpiresAt: now.Add(s.sessionTTL), CreatedAt: now, Version: 1}
	err = s.db.Write(ctx, func(tx *sql.Tx) error {
		if err := s.repo.InsertSession(ctx, tx, session); err != nil {
			return err
		}
		return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: user.TenantID, ActorID: user.ID, Action: "auth.login", ObjectType: "auth_session", ObjectID: session.ID, Outcome: "created", RequestID: input.RequestID, CreatedAt: now})
	})
	if err != nil {
		return LoginResult{}, fmt.Errorf("login: %w", err)
	}
	return LoginResult{Token: token, SessionID: session.ID, TenantID: user.TenantID, UserID: user.ID, Role: user.Role, ExpiresAt: session.ExpiresAt}, nil
}

func (s *Service) Authenticate(ctx context.Context, token string) (Principal, error) {
	if token == "" {
		return Principal{}, domain.Wrap(domain.ErrUnauthorized, "auth.authenticate", "", "", "missing bearer token", nil)
	}
	session, err := s.repo.FindSessionByToken(ctx, s.db.SQL(), domain.HashToken(token))
	if err != nil {
		return Principal{}, err
	}
	if !session.UsableAt(s.clock.Now()) {
		return Principal{}, domain.Wrap(domain.ErrUnauthorized, "auth.authenticate", "auth_session", session.ID, "session expired or revoked", nil)
	}
	user, err := s.repo.FindUser(ctx, s.db.SQL(), session.TenantID, session.UserID)
	if err != nil {
		return Principal{}, domain.Wrap(domain.ErrUnauthorized, "auth.authenticate", "", "", "session user is unavailable", err)
	}
	if !user.Active {
		return Principal{}, domain.Wrap(domain.ErrUnauthorized, "auth.authenticate", "user", user.ID, "user is inactive", nil)
	}
	return Principal{TenantID: session.TenantID, UserID: session.UserID, Role: user.Role, SessionID: session.ID}, nil
}

func (s *Service) Logout(ctx context.Context, principal Principal, requestID string) error {
	if principal.TenantID == "" || principal.UserID == "" || principal.SessionID == "" || requestID == "" {
		return domain.Validation("auth.logout", "principal and request id are required")
	}
	auditID, err := domain.NewID("audit")
	if err != nil {
		return err
	}
	now := s.clock.Now()
	return s.db.Write(ctx, func(tx *sql.Tx) error {
		var version int64
		var revoked sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT version, revoked_at FROM auth_sessions WHERE tenant_id = ? AND id = ?`, principal.TenantID, principal.SessionID).Scan(&version, &revoked)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Wrap(domain.ErrUnauthorized, "auth.logout", "", "", "session not found", nil)
		}
		if err != nil {
			return fmt.Errorf("load session for logout: %w", err)
		}
		if revoked.Valid {
			return nil
		}
		if err := s.repo.RevokeSession(ctx, tx, principal.TenantID, principal.SessionID, version, now); err != nil {
			return err
		}
		return s.audits.Append(ctx, tx, audit.Record{ID: auditID, TenantID: principal.TenantID, ActorID: principal.UserID, Action: "auth.logout", ObjectType: "auth_session", ObjectID: principal.SessionID, Outcome: "revoked", RequestID: requestID, CreatedAt: now})
	})
}

func RequireRole(principal Principal, roles ...domain.Role) error {
	for _, role := range roles {
		if principal.Role == role {
			return nil
		}
	}
	return domain.Wrap(domain.ErrForbidden, "auth.require_role", "", "", "role is not permitted for this operation", nil)
}

func randomToken() (string, error) {
	bytes := make([]byte, 36)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate session token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}
