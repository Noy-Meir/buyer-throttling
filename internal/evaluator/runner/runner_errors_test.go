package runner

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
	"github.com/Noy-Meir/buyer-throttling/internal/store"
)

// casFailStore fails only the compare-and-set, so the write error path can be
// reached after a successful read.
type casFailStore struct {
	*memStore
}

func (s casFailStore) CompareAndSetState(context.Context, string, int64, evaluator.State) (bool, error) {
	return false, errors.New("write failed")
}

func TestTickReportsWriteFailureAndDoesNotRecordAnObservation(t *testing.T) {
	ctx := context.Background()
	mem := newMemStore()
	mem.setStats("b", 4, evaluator.WindowStats{Total: 100, Failed: 80})
	var logs bytes.Buffer
	r := newRunner(casFailStore{mem}, clock.NewFake(time.Unix(0, 0)), &logs)
	r.Observed = NewObservations(10)

	err := r.Tick(ctx, 4)

	require.Error(t, err)
	assert.ErrorContains(t, err, "write failed")
	got, _ := r.Observed.Snapshot()
	assert.Empty(t, got, "an unwritten decision is not an observation")
	assert.NotContains(t, logs.String(), "buyer evaluated", "an unwritten decision is not logged as applied")
}

func TestTickReportsStateReadFailure(t *testing.T) {
	mem := newMemStore()
	mem.setStats("b", 4, evaluator.WindowStats{Total: 100, Failed: 80})
	mem.failState = map[string]error{"b": errors.New("read failed")}
	var logs bytes.Buffer
	r := newRunner(mem, clock.NewFake(time.Unix(0, 0)), &logs)

	err := r.Tick(context.Background(), 4)

	require.Error(t, err)
	assert.ErrorContains(t, err, "read failed")
	assert.Equal(t, 0, mem.casCalls, "nothing is written when the read fails")
}

// racingStore simulates another evaluator. The first time this evaluator reads
// stats, the other one writes the same window first. This evaluator's
// compare-and-set must then be a no-op, and its decision must not be recorded.
type racingStore struct {
	*memStore
	otherWrote bool
	other      evaluator.State
}

func (s *racingStore) WindowStats(ctx context.Context, buyer string, window int64) (evaluator.WindowStats, error) {
	if !s.otherWrote {
		s.otherWrote = true
		if _, err := s.memStore.CompareAndSetState(ctx, buyer, window, s.other); err != nil {
			return evaluator.WindowStats{}, err
		}
	}
	return s.memStore.WindowStats(ctx, buyer, window)
}

func TestTickLosesRaceToAnotherEvaluatorWithoutOverwriting(t *testing.T) {
	ctx := context.Background()
	mem := newMemStore()
	mem.setStats("b", 4, evaluator.WindowStats{Total: 100, Failed: 80}) // our decision would block
	other := evaluator.State{ThrottleRateBP: 7000, Reason: evaluator.ReasonRecovering}
	st := &racingStore{memStore: mem, other: other}
	var logs bytes.Buffer
	r := newRunner(st, clock.NewFake(time.Unix(0, 0)), &logs)
	r.Observed = NewObservations(10)

	require.NoError(t, r.Tick(ctx, 4))

	assert.Equal(t, store.Snapshot{BuyerID: "b", State: other, LastEvaluatedWindow: 4}, mem.snapshot("b"),
		"the other evaluator's write stands; ours is dropped")
	got, _ := r.Observed.Snapshot()
	assert.Empty(t, got, "a lost race is not recorded as our applied decision")
	assert.NotContains(t, logs.String(), "level=WARN", "no violation is logged for a write we did not make")
}

// A tick where every window was already evaluated writes nothing, but it is
// still a completed tick, so the counter advances. Driven through Run with the
// fake clock, not by calling Tick directly.
func TestRunAdvancesHeartbeatOnTickThatSkipsEvaluatedWindows(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 1, 2, 12, 0, 30, 0, time.UTC))
	mem := newMemStore()
	mem.setStats("b", minuteIndex(time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)), evaluator.WindowStats{Total: 100})
	// Mark the buyer as already evaluated past the window Run will pick, so the tick skips.
	mem.states["b"] = store.Snapshot{BuyerID: "b", LastEvaluatedWindow: 1 << 40}
	mem.beatDone = make(chan int64, 4)
	var logs bytes.Buffer
	r := newRunner(mem, clk, &logs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	clk.BlockUntil(1)
	clk.Advance(35 * time.Second) // 12:01:05: the window 12:00 closes and is evaluated
	assert.Equal(t, int64(1), <-mem.beatDone, "a skipped tick still beats")

	assert.Equal(t, 0, mem.casCalls, "nothing was written: every window was already evaluated")
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

// A failed beat is logged and counted. It does not stop the loop.
func TestBeatFailureIsCountedAndLogged(t *testing.T) {
	mem := newMemStore()
	mem.beatErr = errors.New("redis down")
	var logs bytes.Buffer
	r := newRunner(mem, clock.NewFake(time.Unix(0, 0)), &logs)
	r.Observed = NewObservations(10)

	r.beat(context.Background())

	assert.Equal(t, int64(1), r.Observed.HeartbeatFailures())
	assert.Contains(t, logs.String(), "heartbeat failed")
}
