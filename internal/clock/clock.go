// Package clock provides the time abstraction used across Courierbox so that
// the retry/backoff machinery and the deterministic test control plane can share
// a single source of truth for "now".
package clock

import (
	"sync"
	"time"
)

// Clock reports the current instant. All time-dependent logic in the engine
// (next-attempt scheduling, signature timestamps, backoff math) reads the time
// from a Clock so tests can drive it manually without sleeping.
type Clock interface {
	Now() time.Time
}

// Real is a Clock backed by the wall clock.
type Real struct{}

// Now returns the wall-clock time.
func (Real) Now() time.Time { return time.Now() }

// Manual is a Clock whose value is advanced explicitly. It is safe for
// concurrent use. Production code never constructs a Manual clock; it exists for
// the deterministic test control plane (POST /_test/advance).
type Manual struct {
	mu  sync.Mutex
	now time.Time
}

// NewManual returns a Manual clock initialised to t.
func NewManual(t time.Time) *Manual {
	return &Manual{now: t}
}

// Now returns the clock's current value.
func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// Advance moves the clock forward by d and returns the new value.
func (m *Manual) Advance(d time.Duration) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
	return m.now
}

// Set replaces the clock's value with t.
func (m *Manual) Set(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = t
}
