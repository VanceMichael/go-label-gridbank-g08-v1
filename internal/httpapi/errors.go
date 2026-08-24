package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/VanceMichael/go-base-gridbank-g08/internal/domain"
)

type errorResponse struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

func writeError(logger *slog.Logger, writer http.ResponseWriter, request *http.Request, err error) {
	status, code, message := classifyError(err)
	requestID := requestIDFromContext(request.Context())
	if status >= http.StatusInternalServerError {
		logger.ErrorContext(request.Context(), "request failed", "request_id", requestID, "method", request.Method, "path", request.URL.Path, "error", err)
	} else {
		logger.WarnContext(request.Context(), "request rejected", "request_id", requestID, "method", request.Method, "path", request.URL.Path, "code", code, "error", err)
	}
	var response errorResponse
	response.Error.Code = code
	response.Error.Message = message
	response.Error.RequestID = requestID
	writeJSON(writer, status, response)
}

func classifyError(err error) (int, string, string) {
	switch {
	case errors.Is(err, context.Canceled):
		return 499, "request_canceled", "request was canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "deadline_exceeded", "request deadline was exceeded"
	case errors.Is(err, domain.ErrValidation):
		return http.StatusUnprocessableEntity, "validation_failed", publicMessage(err, "request validation failed")
	case errors.Is(err, domain.ErrUnauthorized):
		return http.StatusUnauthorized, "unauthorized", "authentication is required"
	case errors.Is(err, domain.ErrForbidden):
		return http.StatusForbidden, "forbidden", "operation is not permitted"
	case errors.Is(err, domain.ErrNotFound):
		return http.StatusNotFound, "not_found", "requested resource was not found"
	case errors.Is(err, domain.ErrIdempotencyConflict):
		return http.StatusConflict, "idempotency_conflict", "idempotency key was used for another request"
	case errors.Is(err, domain.ErrLeaseLost):
		return http.StatusConflict, "lease_lost", "resource ownership changed or expired"
	case errors.Is(err, domain.ErrConflict):
		return http.StatusConflict, "conflict", publicMessage(err, "resource changed concurrently")
	case errors.Is(err, domain.ErrPrecondition):
		return http.StatusPreconditionFailed, "precondition_failed", publicMessage(err, "business precondition was not met")
	case errors.Is(err, domain.ErrUnavailable):
		return http.StatusServiceUnavailable, "unavailable", "required dependency is unavailable"
	default:
		return http.StatusInternalServerError, "internal_error", "an internal error occurred"
	}
}

func publicMessage(err error, fallback string) string {
	var domainErr *domain.Error
	if errors.As(err, &domainErr) && domainErr.Message != "" {
		return domainErr.Message
	}
	return fallback
}

func writeJSON(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body)
}
