package throttler

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/Noy-Meir/buyer-throttling/internal/store"
)

// Start loads an initial snapshot, then runs the flush and refresh loops until
// ctx is cancelled or Stop is called. Calling Start on a running Throttler does
// nothing.
func (t *Throttler) Start(ctx context.Context) {
	t.lifecycle.Lock()
	defer t.lifecycle.Unlock()
	if t.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	// Load rates before serving so that a restarted pod does not start open
	// when the store already holds blocks.
	t.refresh(ctx)

	t.loops.Add(2)
	go t.loop(ctx, t.cfg.FlushInterval, t.flush)
	go t.loop(ctx, t.cfg.StateRefreshInterval, t.refresh)
}

// Stop ends the background loops, waits for them to exit, and flushes what is
// left in the counters. A Throttler that was never started does nothing.
func (t *Throttler) Stop() {
	t.lifecycle.Lock()
	cancel := t.cancel
	t.cancel = nil
	t.lifecycle.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	t.loops.Wait()

	// The final flush runs on its own context, because the loops' context is
	// already cancelled. A flush interval bounds how long shutdown can wait on
	// the store.
	ctx, done := context.WithTimeout(context.Background(), t.cfg.FlushInterval)
	defer done()
	t.flush(ctx)
}

// loop calls fn every interval until ctx is done. Timers come from the injected
// clock, so tests can drive it without sleeping.
func (t *Throttler) loop(ctx context.Context, every time.Duration, fn func(context.Context)) {
	defer t.loops.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.clock.After(every):
			fn(ctx)
		}
	}
}

// flush swaps every buyer's counters to zero and writes the non-empty ones to
// the store as one batch, attributed to the current window.
//
// If the store call fails the batch is dropped, not carried into the next
// flush. Carrying it over would move calls into a later window than the one
// they were made in, and the evaluator would see a distorted sample.
func (t *Throttler) flush(ctx context.Context) {
	window := store.WindowIndex(t.clock.Now(), t.cfg.EvaluationWindow)

	var batch []store.Increment
	var calls int64
	t.counters.Range(func(k, v any) bool {
		c := v.(*counters)
		inc := store.Increment{
			BuyerID: k.(string),
			Window:  window,
			OK:      c.ok.Swap(0),
			Failed:  c.failed.Swap(0),
			Timeout: c.timeout.Swap(0),
		}
		if n := inc.OK + inc.Failed + inc.Timeout; n > 0 {
			batch = append(batch, inc)
			calls += n
		}
		return true
	})
	if len(batch) == 0 {
		return
	}

	if err := t.store.IncrStats(ctx, batch); err != nil {
		t.flushFailures.Add(1)
		t.logger.Error("dropping stats batch after failed flush",
			slog.Int("buyers", len(batch)),
			slog.Int64("calls", calls),
			slog.Int64("window", window),
			slog.Any("err", err))
		return
	}
	t.flushedCalls.Add(calls)
}

// refresh replaces the local snapshot with the throttled buyers from the store.
//
// If the read fails the previous snapshot is kept. Its age keeps growing, so
// once it passes MaxStaleness ShouldCall fails open.
//
// If more buyers are throttled than MaxBuyers allows, the highest rates are
// kept. The rest fail open for this pod until the next refresh. Blocks are the
// most important signal, so they are the last to be dropped.
func (t *Throttler) refresh(ctx context.Context) {
	states, err := t.store.AllStates(ctx)
	if err != nil {
		t.refreshFailures.Add(1)
		t.logger.Warn("state refresh failed; keeping last snapshot", slog.Any("err", err))
		return
	}

	type entry struct {
		buyerID string
		rateBP  int
	}
	var throttled []entry
	for _, s := range states {
		if s.State.ThrottleRateBP > 0 {
			throttled = append(throttled, entry{s.BuyerID, s.State.ThrottleRateBP})
		}
	}
	if limit := t.cfg.MaxBuyers; len(throttled) > limit {
		sort.Slice(throttled, func(i, j int) bool { return throttled[i].rateBP > throttled[j].rateBP })
		t.logger.Warn("throttled buyers exceed cap; lowest rates fail open",
			slog.Int("throttled", len(throttled)), slog.Int("cap", limit))
		throttled = throttled[:limit]
	}

	rates := make(map[string]int, len(throttled))
	for _, e := range throttled {
		rates[e.buyerID] = e.rateBP
	}
	t.snap.Store(&snapshot{rates: rates, refreshedAt: t.clock.Now()})
}
