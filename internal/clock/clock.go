// Package clock provides a small time abstraction so that time-dependent logic
// (queue retry backoff, lock lease expiry, reconciler windows) can be tested
// deterministically instead of depending on the wall clock.
//
// Production code uses System. Tests use Fixed/Manual to make "now" explicit.
// Keeping a single seam for time avoids flaky tests that sleep or race against
// real durations.
package clock

import (
	"sync"
	"time"
)

// Clock is the minimal time source used across the system. Components that
// compute deadlines (queue, symbollock, reconciler) take a Clock so their
// behaviour is reproducible under test.
type Clock interface {
	// Now returns the current time. Implementations must be safe for
	// concurrent use.
	Now() time.Time
}

// System is the production Clock backed by the real wall clock.
type System struct{}

// Now returns time.Now().
func (System) Now() time.Time { return time.Now() }

// NewSystem returns the production clock.
func NewSystem() Clock { return System{} }

// Manual is a Clock whose value is controlled by the test. It is safe for
// concurrent use because deadline checks may run from background goroutines.
type Manual struct {
	mu  sync.RWMutex
	now time.Time
}

// NewManual returns a Manual clock initialised to start.
func NewManual(start time.Time) *Manual { return &Manual{now: start} }

// Now returns the currently configured time.
func (m *Manual) Now() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.now
}

// Set overwrites the current time.
func (m *Manual) Set(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = t
}

// Advance moves the clock forward by d and returns the new time.
func (m *Manual) Advance(d time.Duration) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
	return m.now
}
