package clock

import (
	"sync"
	"time"
)

// Clock makes deadlines and lease transitions deterministic in services and tests.
type Clock interface {
	Now() time.Time
}

type Real struct{}

func (Real) Now() time.Time { return time.Now().UTC() }

type Manual struct {
	mu  sync.RWMutex
	now time.Time
}

func NewManual(now time.Time) *Manual {
	return &Manual{now: now.UTC()}
}

func (m *Manual) Now() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.now
}

func (m *Manual) Set(now time.Time) {
	m.mu.Lock()
	m.now = now.UTC()
	m.mu.Unlock()
}

func (m *Manual) Advance(d time.Duration) time.Time {
	m.mu.Lock()
	m.now = m.now.Add(d)
	now := m.now
	m.mu.Unlock()
	return now
}
