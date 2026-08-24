package outbox

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestSenderClosesResponseBodyOnHTTPFailure(t *testing.T) {
	body := &task0028ReadErrorBody{}
	sender := NewSender(doerFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusBadGateway, Body: body}, nil
	}))
	if err := sender.Send(context.Background(), "https://hooks.example.test/events", "task0028-event", []byte(`{"job_id":"job-1"}`)); err == nil {
		t.Fatal("HTTP failure unexpectedly returned success")
	}
	if !body.closed.Load() {
		t.Fatal("HTTP failure response body was not closed")
	}
}

type task0028ReadErrorBody struct {
	closed atomic.Bool
}

func (b *task0028ReadErrorBody) Read([]byte) (int, error) {
	return 0, errors.New("upstream body read failed")
}
func (b *task0028ReadErrorBody) Close() error {
	b.closed.Store(true)
	return nil
}

var _ io.ReadCloser = (*task0028ReadErrorBody)(nil)
