package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeTicksAndRunsFinalizerOnce(t *testing.T) {
	var ticks atomic.Int64
	var stops atomic.Int64
	reached := make(chan struct{})
	runtime, err := NewRuntime("test", 2*time.Millisecond, func(context.Context) error {
		if ticks.Add(1) == 3 {
			close(reached)
		}
		return nil
	}, func(context.Context) error {
		stops.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("worker did not tick")
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := runtime.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if stops.Load() != 1 {
		t.Fatalf("finalizer count = %d, want 1", stops.Load())
	}
	if ticks.Load() < 3 {
		t.Fatalf("tick count = %d, want at least 3", ticks.Load())
	}
}

func TestRuntimeWaitsForCanceledTick(t *testing.T) {
	entered := make(chan struct{})
	exited := make(chan struct{})
	runtime, err := NewRuntime("blocking", time.Hour, func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		close(exited)
		return ctx.Err()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-entered
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
	default:
		t.Fatal("Stop returned before active tick observed cancellation")
	}
}

func TestRuntimeAggregatesTickAndFinalizerErrors(t *testing.T) {
	tickErr := errors.New("tick failed")
	stopErr := errors.New("finalizer failed")
	called := make(chan struct{})
	runtime, err := NewRuntime("errors", time.Hour, func(context.Context) error {
		select {
		case <-called:
		default:
			close(called)
		}
		return tickErr
	}, func(context.Context) error { return stopErr })
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-called
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = runtime.Stop(ctx)
	if !errors.Is(err, tickErr) || !errors.Is(err, stopErr) {
		t.Fatalf("Stop error = %v, want both causes", err)
	}
	if !errors.Is(runtime.Err(), tickErr) {
		t.Fatalf("runtime Err = %v", runtime.Err())
	}
}

func TestRuntimeLifecycleValidation(t *testing.T) {
	if _, err := NewRuntime("", time.Second, func(context.Context) error { return nil }, nil); err == nil {
		t.Fatal("empty worker name was accepted")
	}
	if _, err := NewRuntime("worker", 0, func(context.Context) error { return nil }, nil); err == nil {
		t.Fatal("zero interval was accepted")
	}
	if _, err := NewRuntime("worker", time.Second, nil, nil); err == nil {
		t.Fatal("nil tick was accepted")
	}
	runtime, err := NewRuntime("worker", time.Hour, func(context.Context) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Stop(context.Background()); err == nil {
		t.Fatal("stopping before start was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start error = %v, want context canceled", err)
	}
}

func TestGroupStartsAndStopsInReverseOrder(t *testing.T) {
	var mu sync.Mutex
	order := make([]string, 0)
	makeRuntime := func(name string) *Runtime {
		runtime, err := NewRuntime(name, time.Hour, func(context.Context) error { return nil }, func(context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return runtime
	}
	group := &Group{}
	if err := group.Add(makeRuntime("first")); err != nil {
		t.Fatal(err)
	}
	if err := group.Add(makeRuntime("second")); err != nil {
		t.Fatal(err)
	}
	if err := group.Add(makeRuntime("third")); err != nil {
		t.Fatal(err)
	}
	if err := group.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := group.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{"third", "second", "first"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("stop order = %v, want %v", order, want)
		}
	}
}

func TestGroupRejectsInvalidLifecycleChanges(t *testing.T) {
	group := &Group{}
	if err := group.Add(nil); err == nil {
		t.Fatal("nil runtime was accepted")
	}
	runtime, err := NewRuntime("worker", time.Hour, func(context.Context) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := group.Add(runtime); err != nil {
		t.Fatal(err)
	}
	if err := group.Stop(context.Background()); err == nil {
		t.Fatal("group stopped before start")
	}
	if err := group.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := group.Add(runtime); err == nil {
		t.Fatal("runtime added after start")
	}
	if err := group.Start(context.Background()); err == nil {
		t.Fatal("group started twice")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := group.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := group.Stop(ctx); err == nil {
		t.Fatal("group stopped twice")
	}
}
