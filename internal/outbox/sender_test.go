package outbox

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

type doerFunc func(*http.Request) (*http.Response, error)

func (f doerFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

type trackingBody struct {
	reader io.Reader
	closed atomic.Bool
}

func (b *trackingBody) Read(data []byte) (int, error) { return b.reader.Read(data) }
func (b *trackingBody) Close() error {
	b.closed.Store(true)
	return nil
}

func TestSenderDeliversJSONAndClosesSuccessBody(t *testing.T) {
	body := &trackingBody{reader: strings.NewReader(`{"ok":true}`)}
	sender := NewSender(doerFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", request.Method)
		}
		if request.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content type = %q", request.Header.Get("Content-Type"))
		}
		if request.Header.Get("Idempotency-Key") != "event-1" {
			t.Errorf("idempotency key = %q", request.Header.Get("Idempotency-Key"))
		}
		payload, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(payload) != `{"workload_id":"c1"}` {
			t.Errorf("payload = %s", payload)
		}
		return &http.Response{StatusCode: http.StatusNoContent, Body: body}, nil
	}))
	if err := sender.Send(context.Background(), "https://hooks.example.test/events", "event-1", []byte(`{"workload_id":"c1"}`)); err != nil {
		t.Fatal(err)
	}
	if !body.closed.Load() {
		t.Fatal("successful response body was not closed")
	}
}

func TestSenderClassifiesHTTPFailuresAndClosesBodies(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		temporary bool
	}{
		{name: "bad request", status: http.StatusBadRequest, body: "invalid event", temporary: false},
		{name: "too many requests", status: http.StatusTooManyRequests, body: "slow down", temporary: true},
		{name: "server error", status: http.StatusServiceUnavailable, body: "later", temporary: true},
		{name: "empty response", status: http.StatusConflict, body: "", temporary: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := &trackingBody{reader: strings.NewReader(test.body)}
			sender := NewSender(doerFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: test.status, Body: body}, nil
			}))
			err := sender.Send(context.Background(), "https://hooks.example.test/events", "event", []byte(`{}`))
			var deliveryErr *DeliveryError
			if !errors.As(err, &deliveryErr) {
				t.Fatalf("error = %v, want DeliveryError", err)
			}
			if deliveryErr.StatusCode != test.status || deliveryErr.Temporary != test.temporary {
				t.Fatalf("delivery error = %+v", deliveryErr)
			}
			if !body.closed.Load() {
				t.Fatal("failure response body was not closed")
			}
		})
	}
}

func TestSenderPreservesCancellationAndTransportCause(t *testing.T) {
	transportErr := errors.New("socket interrupted")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sender := NewSender(doerFunc(func(request *http.Request) (*http.Response, error) {
		if !errors.Is(request.Context().Err(), context.Canceled) {
			t.Fatal("request did not retain canceled context")
		}
		return nil, transportErr
	}))
	err := sender.Send(ctx, "https://hooks.example.test/events", "event", []byte(`{}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation missing from error chain: %v", err)
	}
	if !errors.Is(err, transportErr) {
		t.Fatalf("transport cause missing from error chain: %v", err)
	}
	var deliveryErr *DeliveryError
	if !errors.As(err, &deliveryErr) || !deliveryErr.Temporary {
		t.Fatalf("error should be a temporary DeliveryError: %v", err)
	}
}

func TestSenderRejectsInvalidEndpointsBeforeCallingClient(t *testing.T) {
	called := atomic.Bool{}
	sender := NewSender(doerFunc(func(*http.Request) (*http.Response, error) {
		called.Store(true)
		return nil, nil
	}))
	for _, endpoint := range []string{"", "/relative", "ftp://example.test/file", "://bad"} {
		if err := sender.Send(context.Background(), endpoint, "event", nil); err == nil {
			t.Errorf("endpoint %q was accepted", endpoint)
		}
	}
	if called.Load() {
		t.Fatal("HTTP client was called for invalid endpoint")
	}
}

func TestSenderHandlesNilAndOversizedBodies(t *testing.T) {
	t.Run("nil body", func(t *testing.T) {
		sender := NewSender(doerFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK}, nil
		}))
		if err := sender.Send(context.Background(), "https://hooks.example.test", "event", nil); err == nil {
			t.Fatal("nil response body was accepted")
		}
	})
	t.Run("oversized body", func(t *testing.T) {
		body := &trackingBody{reader: strings.NewReader(strings.Repeat("x", 33*1024))}
		sender := NewSender(doerFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusBadGateway, Body: body}, nil
		}))
		err := sender.Send(context.Background(), "https://hooks.example.test", "event", nil)
		if err == nil || !body.closed.Load() {
			t.Fatalf("oversized response result = %v, closed=%v", err, body.closed.Load())
		}
	})
}
