// Package runner drives the pure evaluator once per window. It picks the last
// closed window, reads each buyer's stats and state from the store, evaluates,
// and writes back through the store's idempotent compare-and-set.
//
// The runner lives in its own package because the store refers to
// evaluator.State, so the runner (which uses the store) cannot sit in the
// evaluator package without an import cycle.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
	"github.com/Noy-Meir/buyer-throttling/internal/store"
)

// Runner evaluates every known buyer once per window.
type Runner struct {
	Store  store.Store
	Clock  clock.Clock
	Params evaluator.Params
	Window time.Duration
	Grace  time.Duration
	Logger *slog.Logger
}

// New returns a Runner. A nil logger falls back to slog.Default().
func New(st store.Store, clk clock.Clock, params evaluator.Params, window, grace time.Duration, logger *slog.Logger) *Runner {
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{Store: st, Clock: clk, Params: params, Window: window, Grace: grace, Logger: logger}
}

// Run blocks, evaluating the last closed window at each boundary plus grace,
// until ctx is cancelled. It never backfills: if the process was paused across
// several boundaries, only the most recently closed window is evaluated.
func (r *Runner) Run(ctx context.Context) error {
	for {
		now := r.Clock.Now()
		fire, window := r.nextTick(now)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.Clock.After(fire.Sub(now)):
		}
		// The tick runs on a context that ignores cancellation, so a shutdown
		// that arrives mid-evaluation lets the current window finish cleanly.
		// Store calls are still bounded by the Redis client's own timeouts.
		if err := r.Tick(context.WithoutCancel(ctx), window); err != nil {
			r.Logger.Error("evaluation tick failed", slog.Int64("window", window), slog.Any("err", err))
		}
	}
}

// nextTick returns the next instant at which a window becomes evaluable, which
// is a window boundary plus grace, and the index of the window that closes
// there. It is the first such instant strictly after now.
func (r *Runner) nextTick(now time.Time) (time.Time, int64) {
	w := int64(r.Window)
	g := int64(r.Grace)
	// Shift by grace so a window is only chosen once it has been closed for
	// the full grace period.
	x := now.UnixNano() - g
	boundary := (x/w + 1) * w
	fire := time.Unix(0, boundary+g)
	closed := store.WindowIndex(time.Unix(0, boundary-w), r.Window)
	return fire, closed
}

// Tick evaluates window for every known buyer. A failure for one buyer is
// recorded and does not stop the others. The returned error joins them all.
func (r *Runner) Tick(ctx context.Context, window int64) error {
	buyers, err := r.Store.Buyers(ctx)
	if err != nil {
		return fmt.Errorf("list buyers: %w", err)
	}
	var errs []error
	for _, id := range buyers {
		if err := r.evaluateBuyer(ctx, id, window); err != nil {
			errs = append(errs, fmt.Errorf("buyer %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

func (r *Runner) evaluateBuyer(ctx context.Context, buyerID string, window int64) error {
	prev, err := r.Store.State(ctx, buyerID)
	if err != nil {
		return err
	}
	// Cheap early exit for a window already evaluated. The compare-and-set below
	// still enforces this, so the check here only saves the stats read.
	if prev.LastEvaluatedWindow >= window {
		r.Logger.Debug("window already evaluated",
			slog.String("buyer", buyerID), slog.Int64("window", window))
		return nil
	}

	stats, err := r.Store.WindowStats(ctx, buyerID, window)
	if err != nil {
		return err
	}
	d := evaluator.Evaluate(stats, prev.State, r.Params)

	applied, err := r.Store.CompareAndSetState(ctx, buyerID, window, d.State)
	if err != nil {
		return err
	}
	if !applied {
		// Another evaluator wrote this window between our read and write.
		r.Logger.Debug("window already evaluated by another evaluator",
			slog.String("buyer", buyerID), slog.Int64("window", window))
		return nil
	}

	r.logDecision(buyerID, window, stats, prev.State, d)
	return nil
}

// logDecision logs the outcome. Violations are warnings, rate and reason
// changes are info, carried small windows are info only while the buyer is
// throttled, and everything else is debug. A watcher sees blocks, recovery and
// carry-over during a block without one line per low-volume buyer per window.
func (r *Runner) logDecision(buyerID string, window int64, stats evaluator.WindowStats, prev evaluator.State, d evaluator.Decision) {
	level := slog.LevelDebug
	switch {
	case d.FailureRateViolated || d.TimeoutRateViolated:
		level = slog.LevelWarn
	case d.State.ThrottleRateBP != prev.ThrottleRateBP || d.State.Reason != prev.Reason:
		level = slog.LevelInfo
	case d.State.CarriedWindows != prev.CarriedWindows && (prev.ThrottleRateBP > 0 || d.State.ThrottleRateBP > 0):
		// A small window held back during a block or recovery is info: that is
		// the carry-over the demo wants to show. At rate 0 it is routine for
		// low-volume buyers, so it stays at debug.
		level = slog.LevelInfo
	}
	r.Logger.LogAttrs(context.Background(), level, "buyer evaluated",
		slog.String("buyer", buyerID),
		slog.Int64("window", window),
		slog.Int64("total", stats.Total),
		slog.Int64("failed", stats.Failed),
		slog.Int64("timeout", stats.Timeout),
		// Effective is the window plus carried windows. It is the sample the
		// decision used, so it explains a held window whose own total is small.
		slog.Int64("effective_total", d.Effective.Total),
		slog.Int64("effective_failed", d.Effective.Failed),
		slog.Int64("effective_timeout", d.Effective.Timeout),
		slog.Int("throttle_rate_bp", d.State.ThrottleRateBP),
		slog.Int("prev_throttle_rate_bp", prev.ThrottleRateBP),
		slog.String("reason", string(d.State.Reason)),
		slog.Bool("failure_rate_violated", d.FailureRateViolated),
		slog.Bool("timeout_rate_violated", d.TimeoutRateViolated),
		slog.Int("carried_windows", d.State.CarriedWindows),
	)
}
