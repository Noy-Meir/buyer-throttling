// Package store persists per-window buyer stats and throttle state. The
// interface is the only thing the rest of the service sees; no Redis types
// leak through it.
package store

import (
	"context"

	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
)

// Increment is one batch entry: call outcomes to add to a buyer's window.
// Counts are deltas, not totals.
type Increment struct {
	BuyerID string
	Window  int64
	OK      int64
	Failed  int64
	Timeout int64
}

// Snapshot is a buyer's persisted throttle state plus the last window that was
// evaluated to produce it. Idempotency relies on LastEvaluatedWindow, which is
// why it lives here and not in evaluator.State.
type Snapshot struct {
	BuyerID             string
	State               evaluator.State
	LastEvaluatedWindow int64
}

// Store is the shared persistence used by pods (stats) and the evaluator
// (state). Implementations must be safe for concurrent use.
type Store interface {
	// IncrStats adds a batch of call outcomes. The batch is applied as a unit
	// on the store side where the backend allows it.
	IncrStats(ctx context.Context, batch []Increment) error

	// WindowStats returns the stats for one buyer and window. A window with no
	// data returns the zero value and no error.
	WindowStats(ctx context.Context, buyerID string, window int64) (evaluator.WindowStats, error)

	// Buyers lists every buyer that has ever reported stats.
	Buyers(ctx context.Context) ([]string, error)

	// State returns a buyer's snapshot. A buyer with no state returns the zero
	// Snapshot (rate 0, nothing evaluated) and no error.
	State(ctx context.Context, buyerID string) (Snapshot, error)

	// AllStates returns a snapshot for every known buyer.
	AllStates(ctx context.Context) ([]Snapshot, error)

	// CompareAndSetState writes next as the state for window, but only if
	// window is later than the last evaluated window. It reports whether the
	// write happened; false means that window was already evaluated.
	CompareAndSetState(ctx context.Context, buyerID string, window int64, next evaluator.State) (bool, error)
}
