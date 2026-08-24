package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/capacity"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/httpapi"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/ledger"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/metering"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/provider"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/scheduler"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/workload"
)

type httpFixture struct {
	environment *testEnvironment
	server      *httptest.Server
	client      *http.Client
	tenantID    string
	adminToken  string
}

func newHTTPFixture(t *testing.T) *httpFixture {
	t.Helper()
	environment := newEnvironment(t)
	// The integration fixture is intentionally rebuilt through HTTP in this suite;
	// use a fresh database-backed service graph so the API is the only public boundary.
	if err := environment.database.Close(); err != nil {
		t.Fatal(err)
	}
	return newHTTPFixtureFromFreshDatabase(t, environment.databasePath)
}

func newHTTPFixtureFromFreshDatabase(t *testing.T, path string) *httpFixture {
	t.Helper()
	environment := newEnvironment(t)
	_ = path
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	api, err := httpapi.New(httpapi.Dependencies{
		Database:  environment.database,
		Auth:      environment.auth,
		Providers: environment.providers,
		Workloads: environment.workloads,
		Capacitys: environment.capacitys,
		Meterings: environment.meterings,
		Ledgers:   environment.ledgers,
		Scheduler: environment.scheduler,
		Logger:    logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler())
	t.Cleanup(server.Close)
	return &httpFixture{environment: environment, server: server, client: server.Client()}
}

func requestJSON(t *testing.T, fixture *httpFixture, method, path, token string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, fixture.server.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := fixture.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode %s %s response: %v", method, path, err)
	}
	return response, decoded
}

func TestHTTPHealthReadinessAndRequestID(t *testing.T) {
	environment := newEnvironment(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	api, err := httpapi.New(httpapi.Dependencies{
		Database:  environment.database,
		Auth:      environment.auth,
		Providers: environment.providers,
		Workloads: environment.workloads,
		Capacitys: environment.capacitys,
		Meterings: environment.meterings,
		Ledgers:   environment.ledgers,
		Scheduler: environment.scheduler,
		Logger:    logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler())
	defer server.Close()
	for _, path := range []string{"/healthz", "/readyz"} {
		request, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Request-ID", "request-health")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("%s status=%d body=%s err=%v", path, response.StatusCode, body, readErr)
		}
		if response.Header.Get("X-Request-ID") != "request-health" {
			t.Fatalf("%s request id = %q", path, response.Header.Get("X-Request-ID"))
		}
	}
}

func TestHTTPBootstrapLoginLogoutAndAuthorization(t *testing.T) {
	fixture := newHTTPFixtureFromFreshDatabase(t, "ignored")
	response, bootstrap := requestJSON(t, fixture, http.MethodPost, "/api/v1/bootstrap", "", map[string]any{
		"tenant_name":  "HTTP Lab",
		"email":        "http-admin@motion.test",
		"display_name": "HTTP Admin",
		"password":     "http-admin-password",
	})
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("bootstrap status = %d, body=%v", response.StatusCode, bootstrap)
	}
	tenant, ok := bootstrap["tenant"].(map[string]any)
	if !ok || tenant["id"] == "" {
		t.Fatalf("bootstrap omitted tenant: %v", bootstrap)
	}
	tenantID := tenant["id"].(string)
	response, login := requestJSON(t, fixture, http.MethodPost, "/api/v1/auth/login", "", map[string]any{
		"tenant_id": tenantID,
		"email":     "http-admin@motion.test",
		"password":  "http-admin-password",
	})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("login status = %d, body=%v", response.StatusCode, login)
	}
	token, _ := login["token"].(string)
	if token == "" {
		t.Fatalf("login omitted bearer token: %v", login)
	}
	response, unauthorized := requestJSON(t, fixture, http.MethodPost, "/api/v1/auth/logout", "bad-token", nil)
	if response.StatusCode != http.StatusUnauthorized || unauthorized["error"] == nil {
		t.Fatalf("bad logout status=%d body=%v", response.StatusCode, unauthorized)
	}
	response, logout := requestJSON(t, fixture, http.MethodPost, "/api/v1/auth/logout", token, nil)
	if response.StatusCode != http.StatusOK || logout["status"] != "revoked" {
		t.Fatalf("logout status=%d body=%v", response.StatusCode, logout)
	}
	response, revoked := requestJSON(t, fixture, http.MethodPost, "/api/v1/auth/logout", token, nil)
	if response.StatusCode != http.StatusUnauthorized || revoked["error"] == nil {
		t.Fatalf("revoked token status=%d body=%v", response.StatusCode, revoked)
	}
}

// TestHTTPLogoutStaysRetryableWhenAuditFails reproduces the failure path where the
// audit backend cannot be written while a logout is in flight. The revocation and
// its audit record are committed together, so a failed audit must roll the whole
// logout back: the session stays usable until the audit backend recovers and the
// operator can retry the logout to completion.
func TestHTTPLogoutStaysRetryableWhenAuditFails(t *testing.T) {
	fixture := newHTTPFixtureFromFreshDatabase(t, "ignored")
	response, bootstrap := requestJSON(t, fixture, http.MethodPost, "/api/v1/bootstrap", "", map[string]any{
		"tenant_name":  "Audit Lab",
		"email":        "audit-admin@motion.test",
		"display_name": "Audit Admin",
		"password":     "audit-admin-password",
	})
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("bootstrap status = %d, body=%v", response.StatusCode, bootstrap)
	}
	tenantID := bootstrap["tenant"].(map[string]any)["id"].(string)
	response, login := requestJSON(t, fixture, http.MethodPost, "/api/v1/auth/login", "", map[string]any{
		"tenant_id": tenantID,
		"email":     "audit-admin@motion.test",
		"password":  "audit-admin-password",
	})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("login status = %d, body=%v", response.StatusCode, login)
	}
	token, _ := login["token"].(string)
	if token == "" {
		t.Fatalf("login omitted bearer token: %v", login)
	}

	if _, err := fixture.environment.database.SQL().Exec(`
		CREATE TRIGGER fail_logout_audit BEFORE INSERT ON audit_events
		WHEN NEW.action = 'auth.logout'
		BEGIN SELECT RAISE(ABORT, 'forced audit failure'); END`); err != nil {
		t.Fatal(err)
	}

	failed, logout := requestJSON(t, fixture, http.MethodPost, "/api/v1/auth/logout", token, nil)
	if failed.StatusCode != http.StatusInternalServerError {
		t.Fatalf("logout while audit is down status = %d, body=%v", failed.StatusCode, logout)
	}

	var revokedCount int
	if err := fixture.environment.database.SQL().QueryRow(
		`SELECT COUNT(*) FROM auth_sessions WHERE tenant_id = ? AND id = ? AND revoked_at IS NULL`,
		tenantID, login["session_id"],
	).Scan(&revokedCount); err != nil {
		t.Fatal(err)
	}
	if revokedCount != 1 {
		t.Fatalf("failed logout revoked the session anyway; unrevoked count = %d", revokedCount)
	}

	var logoutEvents int
	if err := fixture.environment.database.SQL().QueryRow(
		`SELECT COUNT(*) FROM audit_events WHERE action = 'auth.logout'`).Scan(&logoutEvents); err != nil {
		t.Fatal(err)
	}
	if logoutEvents != 0 {
		t.Fatalf("failed logout leaked %d audit events", logoutEvents)
	}

	if _, err := fixture.environment.database.SQL().Exec(`DROP TRIGGER fail_logout_audit`); err != nil {
		t.Fatal(err)
	}

	response, retry := requestJSON(t, fixture, http.MethodPost, "/api/v1/auth/logout", token, nil)
	if response.StatusCode != http.StatusOK || retry["status"] != "revoked" {
		t.Fatalf("retry logout status=%d body=%v", response.StatusCode, retry)
	}

	response, revoked := requestJSON(t, fixture, http.MethodPost, "/api/v1/auth/logout", token, nil)
	if response.StatusCode != http.StatusUnauthorized || revoked["error"] == nil {
		t.Fatalf("revoked token status=%d body=%v", response.StatusCode, revoked)
	}
}

func TestHTTPRejectsUnknownFieldsAndMissingAuthentication(t *testing.T) {
	fixture := newHTTPFixtureFromFreshDatabase(t, "ignored")
	response, body := requestJSON(t, fixture, http.MethodPost, "/api/v1/providers", "", map[string]any{"name": "Lab", "timezone": "UTC", "unexpected": true})
	if response.StatusCode != http.StatusUnauthorized || body["error"] == nil {
		t.Fatalf("missing auth should be unauthorized: status=%d body=%v", response.StatusCode, body)
	}
	response, body = requestJSON(t, fixture, http.MethodPost, "/api/v1/bootstrap", "", map[string]any{"tenant_name": "Lab", "email": "valid@motion.test", "display_name": "Valid", "password": "valid-password", "unexpected": true})
	if response.StatusCode != http.StatusUnprocessableEntity || body["error"] == nil {
		t.Fatalf("unknown field should be validation error: status=%d body=%v", response.StatusCode, body)
	}
	oversized := strings.Repeat("x", 1<<20)
	request, err := http.NewRequest(http.MethodPost, fixture.server.URL+"/api/v1/bootstrap", strings.NewReader(`{"tenant_name":"`+oversized+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err = fixture.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("oversized request status = %d", response.StatusCode)
	}
}

func TestHTTPCrossTenantResourceIsolation(t *testing.T) {
	fixture := newHTTPFixtureFromFreshDatabase(t, "ignored")
	response, first := requestJSON(t, fixture, http.MethodPost, "/api/v1/bootstrap", "", map[string]any{"tenant_name": "Tenant One", "email": "one@motion.test", "display_name": "One", "password": "one-password"})
	if response.StatusCode != http.StatusCreated {
		t.Fatal(first)
	}
	firstTenantID := first["tenant"].(map[string]any)["id"].(string)
	response, firstLogin := requestJSON(t, fixture, http.MethodPost, "/api/v1/auth/login", "", map[string]any{"tenant_id": firstTenantID, "email": "one@motion.test", "password": "one-password"})
	if response.StatusCode != http.StatusOK {
		t.Fatal(firstLogin)
	}
	firstToken, ok := firstLogin["token"].(string)
	if !ok || firstToken == "" {
		t.Fatalf("first login omitted token: %v", firstLogin)
	}
	response, second := requestJSON(t, fixture, http.MethodPost, "/api/v1/bootstrap", "", map[string]any{"tenant_name": "Tenant Two", "email": "two@motion.test", "display_name": "Two", "password": "two-password"})
	if response.StatusCode != http.StatusCreated {
		t.Fatal(second)
	}
	secondTenantID := second["tenant"].(map[string]any)["id"].(string)
	if firstTenantID == secondTenantID {
		t.Fatal("bootstrap created duplicate tenant identity")
	}
	response, _ = requestJSON(t, fixture, http.MethodPost, "/api/v1/providers", firstToken, map[string]any{"name": "Private Lab", "timezone": "UTC"})
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("first tenant provider status = %d", response.StatusCode)
	}
	// A token from tenant one cannot authenticate as tenant two because the tenant
	// is taken from the durable session rather than a request-provided scope.
	response, body := requestJSON(t, fixture, http.MethodGet, "/api/v1/ledgers?limit=1", firstToken, nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("tenant one list status = %d body=%v", response.StatusCode, body)
	}
	if items, ok := body["items"].([]any); !ok || len(items) != 0 {
		t.Fatalf("tenant one saw unexpected ledger records: %v", body)
	}
}

func TestHTTPContextCancellationStopsHandlerBeforeMutation(t *testing.T) {
	environment := newEnvironment(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	api, err := httpapi.New(httpapi.Dependencies{Database: environment.database, Auth: environment.auth, Providers: provider.NewService(environment.database, environment.clock, time.Minute), Workloads: workload.NewService(environment.database, environment.clock, time.Minute), Capacitys: capacity.NewService(environment.database, environment.clock), Meterings: metering.NewService(environment.database, environment.clock, time.Minute), Ledgers: ledger.NewService(environment.database, environment.clock), Scheduler: scheduler.NewService(environment.database, environment.clock, time.Minute, time.Second, 2), Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/providers", strings.NewReader(`{"name":"Canceled","timezone":"UTC"}`))
	request = request.WithContext(contextCanceled())
	request.Header.Set("Authorization", "Bearer invalid")
	recorder := httptest.NewRecorder()
	api.Handler().ServeHTTP(recorder, request)
	if recorder.Code != 499 {
		t.Fatalf("canceled unauthenticated request code = %d", recorder.Code)
	}
}

func contextCanceled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

var _ = errors.Is
var _ = auth.Principal{}
