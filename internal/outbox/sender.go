package outbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type Sender struct {
	client       HTTPDoer
	maxBodyBytes int64
}

type DeliveryError struct {
	StatusCode int
	Body       string
	Temporary  bool
	Cause      error
}

func (e *DeliveryError) Error() string {
	if e.Cause != nil {
		return "webhook delivery: " + e.Cause.Error()
	}
	return fmt.Sprintf("webhook delivery returned status %d: %s", e.StatusCode, e.Body)
}

func (e *DeliveryError) Unwrap() error { return e.Cause }

func NewSender(client HTTPDoer) *Sender {
	return &Sender{client: client, maxBodyBytes: 32 * 1024}
}

func (s *Sender) Send(ctx context.Context, endpoint string, eventID string, payload []byte) error {
	parsed, err := url.ParseRequestURI(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return &DeliveryError{Cause: fmt.Errorf("invalid webhook endpoint %q", endpoint)}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return &DeliveryError{Cause: fmt.Errorf("create webhook request: %w", err)}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", eventID)
	request.Header.Set("User-Agent", "GridBank-Outbox/1.0")
	response, err := s.client.Do(request)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return &DeliveryError{Temporary: true, Cause: errors.Join(ctxErr, err)}
		}
		return &DeliveryError{Temporary: true, Cause: fmt.Errorf("perform webhook request: %w", err)}
	}
	if response == nil || response.Body == nil {
		return &DeliveryError{Temporary: true, Cause: errors.New("webhook response or body is nil")}
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, s.maxBodyBytes+1))
	if readErr != nil {
		return &DeliveryError{StatusCode: response.StatusCode, Temporary: true, Cause: fmt.Errorf("read webhook response: %w", readErr)}
	}
	if int64(len(body)) > s.maxBodyBytes {
		return &DeliveryError{StatusCode: response.StatusCode, Temporary: true, Cause: errors.New("webhook response exceeds limit")}
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = http.StatusText(response.StatusCode)
	}
	return &DeliveryError{StatusCode: response.StatusCode, Body: message, Temporary: response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500}
}
