package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type TickFunc func(context.Context) error
type StopFunc func(context.Context) error

type Runtime struct {
	name     string
	interval time.Duration
	tick     TickFunc
	onStop   StopFunc

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	started bool
	stopped bool
	err     error
}

func NewRuntime(name string, interval time.Duration, tick TickFunc, onStop StopFunc) (*Runtime, error) {
	if name == "" || interval <= 0 || tick == nil {
		return nil, errors.New("worker name, positive interval, and tick function are required")
	}
	return &Runtime{name: name, interval: interval, tick: tick, onStop: onStop, done: make(chan struct{})}, nil
}

func (r *Runtime) Start(parent context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return fmt.Errorf("worker %s already started", r.name)
	}
	if err := parent.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	r.cancel, r.started = cancel, true
	go r.run(ctx)
	return nil
}

func (r *Runtime) run(ctx context.Context) {
	defer close(r.done)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if err := r.tick(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			r.recordError(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Runtime) Stop(ctx context.Context) error {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return fmt.Errorf("worker %s was not started", r.name)
	}
	if !r.stopped {
		r.stopped = true
		r.cancel()
	}
	done := r.done
	r.mu.Unlock()

	var stopErr error
	if r.onStop != nil {
		stopErr = r.onStop(ctx)
	}
	select {
	case <-done:
	case <-ctx.Done():
		waitErr := fmt.Errorf("wait for worker %s: %w", r.name, ctx.Err())
		if stopErr != nil {
			return errors.Join(waitErr, stopErr)
		}
		return waitErr
	}
	r.mu.Lock()
	runErr := r.err
	r.mu.Unlock()
	if runErr != nil || stopErr != nil {
		return errors.Join(runErr, stopErr)
	}
	return nil
}

func (r *Runtime) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *Runtime) recordError(err error) {
	r.mu.Lock()
	r.err = errors.Join(r.err, fmt.Errorf("worker %s tick: %w", r.name, err))
	r.mu.Unlock()
}
