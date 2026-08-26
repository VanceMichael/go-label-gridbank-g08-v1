package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/auth"
	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
)

type contextKey string

const (
	requestIDKey contextKey = "request_id"
	principalKey contextKey = "principal"
)

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestID := strings.TrimSpace(request.Header.Get("X-Request-ID"))
		if requestID == "" || len(requestID) > 128 {
			requestID = newRequestID()
		}
		writer.Header().Set("X-Request-ID", requestID)
		ctx := context.WithValue(request.Context(), requestIDKey, requestID)
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

func recoverPanic(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer func() {
			if value := recover(); value != nil {
				logger.ErrorContext(request.Context(), "panic recovered", "request_id", requestIDFromContext(request.Context()), "panic", value, "stack", string(debug.Stack()))
				writeJSON(writer, http.StatusInternalServerError, map[string]any{"error": map[string]string{"code": "internal_error", "message": "an internal error occurred", "request_id": requestIDFromContext(request.Context())}})
			}
		}()
		next.ServeHTTP(writer, request)
	})
}

func requestLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		next.ServeHTTP(writer, request)
		logger.InfoContext(request.Context(), "request completed", "request_id", requestIDFromContext(request.Context()), "method", request.Method, "path", request.URL.Path, "duration_ms", time.Since(started).Milliseconds())
	})
}

func (a *API) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		header := strings.TrimSpace(request.Header.Get("Authorization"))
		if !strings.HasPrefix(header, "Bearer ") {
			writeError(a.logger, writer, request, domain.Wrap(domain.ErrUnauthorized, "http.authenticate", "", "", "bearer token is required", nil))
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
		principal, err := a.auth.Authenticate(request.Context(), token)
		if err != nil {
			writeError(a.logger, writer, request, err)
			return
		}
		ctx := context.WithValue(request.Context(), principalKey, principal)
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

func principalFromContext(ctx context.Context) (auth.Principal, error) {
	principal, ok := ctx.Value(principalKey).(auth.Principal)
	if !ok || principal.TenantID == "" || principal.UserID == "" {
		return auth.Principal{}, domain.Wrap(domain.ErrUnauthorized, "http.principal", "", "", "authenticated principal is unavailable", nil)
	}
	return principal, nil
}

func requestIDFromContext(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey).(string)
	if value == "" {
		return "unknown"
	}
	return value
}

func newRequestID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return "req-" + hex.EncodeToString(value)
}
