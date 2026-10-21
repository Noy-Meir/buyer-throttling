// Package clock abstracts wall-clock time so that window alignment, grace
// periods and staleness checks can be tested without sleeping.
package clock

import (
	"sync"
	"time"
)

// Clock is the source of current time for components that need it.
type Clock interface {
	Now() time.Time
}

// Real reads the system clock.
type Real struct{}

// Now returns the current system time.
func (Real) Now() time.Time { return time.Now() }

// Fake is a manually controlled clock for tests. It is safe for concurrent use.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake returns a Fake clock starting at start.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start}
}

// Now returns the fake clock's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the fake clock forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Set moves the fake clock to t, which may be earlier than the current time.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}

var (
	_ Clock = Real{}
	_ Clock = (*Fake)(nil)
)
