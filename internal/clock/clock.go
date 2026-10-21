// Package clock abstracts wall-clock time so that window alignment, grace
// periods and staleness checks can be tested without sleeping.
package clock

import (
	"sync"
	"time"
)

// Clock is the source of time for components that need it.
type Clock interface {
	Now() time.Time
	// After returns a channel that receives the time once d has elapsed.
	After(d time.Duration) <-chan time.Time
}

// Real reads the system clock.
type Real struct{}

// Now returns the current system time.
func (Real) Now() time.Time { return time.Now() }

// After wraps time.After.
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Fake is a manually controlled clock for tests. It is safe for concurrent use.
// Timers from After fire only when Advance or Set moves time past their deadline.
type Fake struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     time.Time
	waiters []waiter
}

type waiter struct {
	deadline time.Time
	ch       chan time.Time
}

// NewFake returns a Fake clock starting at start.
func NewFake(start time.Time) *Fake {
	f := &Fake{now: start}
	f.cond = sync.NewCond(&f.mu)
	return f
}

// Now returns the fake clock's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// After registers a timer that fires when the clock reaches Now()+d. A timer
// with d <= 0 fires immediately.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan time.Time, 1)
	deadline := f.now.Add(d)
	if !deadline.After(f.now) {
		ch <- f.now
		return ch
	}
	f.waiters = append(f.waiters, waiter{deadline: deadline, ch: ch})
	f.cond.Broadcast()
	return ch
}

// BlockUntil blocks until at least n timers are pending. Tests use it to know
// a goroutine has reached its wait before advancing time, instead of sleeping.
func (f *Fake) BlockUntil(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.waiters) < n {
		f.cond.Wait()
	}
}

// Advance moves the fake clock forward by d and fires due timers.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setLocked(f.now.Add(d))
}

// Set moves the fake clock to t, which may be earlier than the current time,
// and fires due timers.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setLocked(t)
}

func (f *Fake) setLocked(t time.Time) {
	f.now = t
	pending := f.waiters[:0]
	for _, w := range f.waiters {
		if !w.deadline.After(f.now) {
			w.ch <- f.now
			continue
		}
		pending = append(pending, w)
	}
	f.waiters = pending
}

var (
	_ Clock = Real{}
	_ Clock = (*Fake)(nil)
)
