package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Group struct {
	mu       sync.Mutex
	workers  []*Runtime
	started  bool
	stopping bool
}

func (g *Group) Add(runtime *Runtime) error {
	if runtime == nil {
		return errors.New("worker runtime is nil")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.started {
		return errors.New("cannot add worker after group start")
	}
	g.workers = append(g.workers, runtime)
	return nil
}

func (g *Group) Start(ctx context.Context) error {
	g.mu.Lock()
	if g.started {
		g.mu.Unlock()
		return errors.New("worker group already started")
	}
	g.started = true
	workers := append([]*Runtime(nil), g.workers...)
	g.mu.Unlock()

	started := make([]*Runtime, 0, len(workers))
	for _, runtime := range workers {
		if err := runtime.Start(ctx); err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for _, prior := range started {
				_ = prior.Stop(cleanupCtx)
			}
			return fmt.Errorf("start worker group: %w", err)
		}
		started = append(started, runtime)
	}
	return nil
}

func (g *Group) Stop(ctx context.Context) error {
	g.mu.Lock()
	if !g.started {
		g.mu.Unlock()
		return errors.New("worker group was not started")
	}
	if g.stopping {
		g.mu.Unlock()
		return errors.New("worker group is already stopping")
	}
	g.stopping = true
	workers := append([]*Runtime(nil), g.workers...)
	g.mu.Unlock()

	var result error
	for i := len(workers) - 1; i >= 0; i-- {
		if err := workers[i].Stop(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}
