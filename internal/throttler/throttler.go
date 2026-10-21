// Package throttler is the auction-pod side of buyer throttling. It answers
// ShouldCall from a local snapshot of throttle rates and collects Record
// outcomes into local counters. Neither call touches the network or takes a
// global lock. Background loops (see loop.go) move data to and from the store.
package throttler

import (
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/config"
	"github.com/Noy-Meir/buyer-throttling/internal/store"
)

// basisPoints is the scale of a throttle rate: 1.0 == 10000.
const basisPoints = 10000

// Result is the outcome of one buyer call.
type Result string

const (
	ResultOK      Result = "ok"
	ResultFailed  Result = "failed"
	ResultTimeout Result = "timeout"
)

// Random supplies the coin flip for ShouldCall. IntN returns a value in [0, n).
//
// Implementations must be safe for concurrent use, because ShouldCall runs on
// every request goroutine. SystemRandom is. A seeded *rand.Rand is not, so it
// is only for single-goroutine tests and must never be the production value.
type Random interface {
	IntN(n int) int
}

// SystemRandom uses math/rand/v2's global generator, which is safe for
// concurrent use.
type SystemRandom struct{}

// IntN returns a random value in [0, n).
func (SystemRandom) IntN(n int) int { return rand.IntN(n) }

// Stats are the pod's internal counters, for metrics and debugging.
type Stats struct {
	TrackedBuyers   int64
	Invalid         int64 // Record calls with an unknown result or empty buyer ID
	Untracked       int64 // Record calls for buyers beyond MaxBuyers
	FlushedCalls    int64 // calls successfully written to the store
	FlushFailures   int64 // flush batches dropped after a store error
	RefreshFailures int64 // state refreshes that failed; the last snapshot was kept
}

// snapshot is an immutable view of throttle rates. Readers load it through an
// atomic pointer and never lock. Only buyers with a rate above zero are kept.
type snapshot struct {
	rates       map[string]int // buyer -> rate in basis points
	refreshedAt time.Time
}

// counters are one buyer's call outcomes since the last flush.
type counters struct {
	ok      atomic.Int64
	failed  atomic.Int64
	timeout atomic.Int64
}

// Throttler is the per-pod component. Create it with New, then call Start
// to run the background loops and Stop to shut down with a final flush.
type Throttler struct {
	cfg    config.Config
	store  store.Store
	clock  clock.Clock
	random Random
	logger *slog.Logger

	// counters maps buyerID to *counters. Reads are lock-free and entries are
	// never removed, so a *counters a Record holds can never be orphaned.
	counters sync.Map
	tracked  atomic.Int64

	snap atomic.Pointer[snapshot]

	invalid         atomic.Int64
	untracked       atomic.Int64
	flushedCalls    atomic.Int64
	flushFailures   atomic.Int64
	refreshFailures atomic.Int64

	// lifecycle guards Start and Stop only. It is never taken on the request path.
	lifecycle sync.Mutex
	cancel    func()
	loops     sync.WaitGroup
}

// New returns a Throttler. It starts with no snapshot, so every buyer is
// allowed until the first successful refresh.
func New(cfg config.Config, st store.Store, clk clock.Clock, rnd Random, logger *slog.Logger) *Throttler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Throttler{cfg: cfg, store: st, clock: clk, random: rnd, logger: logger}
}

// ShouldCall reports whether to send a request to buyerID. A buyer with rate
// 0, a buyer with no known rate, and any buyer whose snapshot is too stale all
// return true. Otherwise the request is skipped with probability rate.
func (t *Throttler) ShouldCall(buyerID string) bool {
	snap := t.snap.Load()
	if snap == nil || t.stale(snap) {
		return true
	}
	rate := snap.rates[buyerID] // nil-safe; missing means 0
	if rate == 0 {
		return true
	}
	// IntN(10000) < rate happens with probability rate/10000, which is the
	// block fraction, so the skip rate is exact in basis points.
	return t.random.IntN(basisPoints) >= rate
}

// Record notes the outcome of a call that was actually made. Invalid input is
// counted and ignored; Record never panics.
func (t *Throttler) Record(buyerID string, result Result) {
	if buyerID == "" || !validResult(result) {
		t.invalid.Add(1)
		return
	}
	c := t.countersFor(buyerID)
	if c == nil {
		t.untracked.Add(1)
		return
	}
	switch result {
	case ResultOK:
		c.ok.Add(1)
	case ResultFailed:
		c.failed.Add(1)
	case ResultTimeout:
		c.timeout.Add(1)
	}
}

// Stats returns a point-in-time copy of the internal counters.
func (t *Throttler) Stats() Stats {
	return Stats{
		TrackedBuyers:   t.tracked.Load(),
		Invalid:         t.invalid.Load(),
		Untracked:       t.untracked.Load(),
		FlushedCalls:    t.flushedCalls.Load(),
		FlushFailures:   t.flushFailures.Load(),
		RefreshFailures: t.refreshFailures.Load(),
	}
}

// countersFor returns the counters for buyerID, creating them if the buyer is
// within MaxBuyers. It returns nil for a new buyer past the cap.
func (t *Throttler) countersFor(buyerID string) *counters {
	if v, ok := t.counters.Load(buyerID); ok {
		return v.(*counters)
	}
	// Reserve a slot before creating the entry. Reserving first keeps concurrent
	// first-time Records from overshooting the cap.
	if t.tracked.Add(1) > int64(t.cfg.MaxBuyers) {
		t.tracked.Add(-1)
		return nil
	}
	v, loaded := t.counters.LoadOrStore(buyerID, &counters{})
	if loaded {
		// Another goroutine created the entry first, so this slot is not used.
		t.tracked.Add(-1)
	}
	return v.(*counters)
}

func (t *Throttler) stale(snap *snapshot) bool {
	return t.clock.Now().Sub(snap.refreshedAt) > t.cfg.MaxStaleness
}

func validResult(r Result) bool {
	switch r {
	case ResultOK, ResultFailed, ResultTimeout:
		return true
	}
	return false
}
