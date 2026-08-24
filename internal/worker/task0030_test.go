package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeStopFinalizesOnce(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var finalizations atomic.Int64
	runtime, err := NewRuntime("settlement-delivery", time.Hour, func(context.Context) error {
		close(entered)
		<-release
		return nil
	}, func(context.Context) error {
		finalizations.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-entered

	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired()
	if err := runtime.Stop(expired); !errors.Is(err, context.Canceled) {
		t.Fatalf("first Stop error = %v, want context canceled while tick is active", err)
	}
	releaseOnce.Do(func() { close(release) })
	retryCtx, cancelRetry := context.WithTimeout(context.Background(), time.Second)
	defer cancelRetry()
	if err := runtime.Stop(retryCtx); err != nil {
		t.Fatalf("retry Stop after tick exit: %v", err)
	}
	if finalizations.Load() != 1 {
		t.Fatalf("worker finalizer ran %d times across timed-out Stop and retry, want exactly once", finalizations.Load())
	}
}
