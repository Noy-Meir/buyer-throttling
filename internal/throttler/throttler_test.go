package throttler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/config"
	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
	"github.com/Noy-Meir/buyer-throttling/internal/store"
)

var testStart = time.Date(2026, 1, 2, 12, 0, 30, 0, time.UTC)

// fakeStore is a goroutine-safe store.Store that records flushed batches and
// can be told to fail either operation.
type fakeStore struct {
	mu        sync.Mutex
	batches   [][]store.Increment
	incrErr   error
	states    []store.Snapshot
	statesErr error
	flushed   chan struct{} // optional: signalled after each IncrStats call
}

func (f *fakeStore) IncrStats(_ context.Context, batch []store.Increment) error {
	f.mu.Lock()
	f.batches = append(f.batches, batch)
	err := f.incrErr
	f.mu.Unlock()
	if f.flushed != nil {
		f.flushed <- struct{}{}
	}
	return err
}

func (f *fakeStore) AllStates(_ context.Context) ([]store.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statesErr != nil {
		return nil, f.statesErr
	}
	return append([]store.Snapshot(nil), f.states...), nil
}

func (f *fakeStore) setStates(states ...store.Snapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states = states
}

func (f *fakeStore) setIncrErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.incrErr = err
}

func (f *fakeStore) setStatesErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statesErr = err
}

func (f *fakeStore) allBatches() [][]store.Increment {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]store.Increment(nil), f.batches...)
}

func (f *fakeStore) WindowStats(context.Context, string, int64) (evaluator.WindowStats, error) {
	return evaluator.WindowStats{}, nil
}
func (f *fakeStore) Buyers(context.Context) ([]string, error) { return nil, nil }
func (f *fakeStore) State(context.Context, string) (store.Snapshot, error) {
	return store.Snapshot{}, nil
}
func (f *fakeStore) CompareAndSetState(context.Context, string, int64, evaluator.State) (bool, error) {
	return false, nil
}

// scriptedRandom returns fixed values in order. Calling it more often than
// scripted fails the test, which proves a path did not draw a random number.
type scriptedRandom struct {
	t      *testing.T
	values []int
	calls  int
}

func (r *scriptedRandom) IntN(n int) int {
	r.t.Helper()
	require.Less(r.t, r.calls, len(r.values), "unexpected random draw")
	v := r.values[r.calls]
	r.calls++
	require.Equal(r.t, basisPoints, n)
	return v
}

func snapFor(buyer string, rateBP int) store.Snapshot {
	return store.Snapshot{BuyerID: buyer, State: evaluator.State{ThrottleRateBP: rateBP, Reason: evaluator.ReasonFailureRate}}
}

func testConfig() config.Config {
	cfg := config.Defaults()
	cfg.MaxBuyers = 100
	return cfg
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newThrottler(t *testing.T, cfg config.Config, st store.Store, rnd Random, clk clock.Clock) *Throttler {
	t.Helper()
	return New(cfg, st, clk, rnd, quietLogger())
}

// withSnapshot builds a throttler whose snapshot holds the given states.
func withSnapshot(t *testing.T, cfg config.Config, rnd Random, states ...store.Snapshot) (*Throttler, *fakeStore, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(testStart)
	st := &fakeStore{states: states}
	th := newThrottler(t, cfg, st, rnd, clk)
	th.refresh(context.Background())
	return th, st, clk
}

func TestShouldCallRateZeroAlwaysTrueWithoutDrawing(t *testing.T) {
	rnd := &scriptedRandom{t: t} // no values: any draw fails the test
	th, _, _ := withSnapshot(t, testConfig(), rnd, snapFor("b", 0))
	for i := 0; i < 1000; i++ {
		require.True(t, th.ShouldCall("b"))
	}
	assert.Equal(t, 0, rnd.calls)
}

func TestShouldCallUnknownBuyerIsTrueWithoutDrawing(t *testing.T) {
	rnd := &scriptedRandom{t: t}
	th, _, _ := withSnapshot(t, testConfig(), rnd, snapFor("bad", 9500))
	assert.True(t, th.ShouldCall("never-seen"))
	assert.Equal(t, 0, rnd.calls)
}

func TestShouldCallBeforeAnySnapshotIsTrue(t *testing.T) {
	th := newThrottler(t, testConfig(), &fakeStore{}, &scriptedRandom{t: t}, clock.NewFake(testStart))
	assert.True(t, th.ShouldCall("anyone"))
}

func TestShouldCallSkipsAtBoundaryOfRandomDraw(t *testing.T) {
	// rate 9500: a draw below 9500 skips, a draw at or above 9500 calls.
	rnd := &scriptedRandom{t: t, values: []int{0, 9499, 9500, 9999}}
	th, _, _ := withSnapshot(t, testConfig(), rnd, snapFor("b", 9500))
	assert.False(t, th.ShouldCall("b"), "draw 0 skips")
	assert.False(t, th.ShouldCall("b"), "draw 9499 skips")
	assert.True(t, th.ShouldCall("b"), "draw 9500 calls")
	assert.True(t, th.ShouldCall("b"), "draw 9999 calls")
}

func TestShouldCallRateNinetyFivePercentStatistically(t *testing.T) {
	const n = 100_000
	const want = 5000 // 5% of calls get through at rate 0.95
	rnd := rand.New(rand.NewPCG(1, 2))
	th, _, _ := withSnapshot(t, testConfig(), rnd, snapFor("b", 9500))

	calls := 0
	for i := 0; i < n; i++ {
		if th.ShouldCall("b") {
			calls++
		}
	}
	// Standard deviation is about 69, so +/-500 is more than 7 sigma. The seed
	// is fixed, so this is a deterministic check of the distribution.
	assert.InDelta(t, want, calls, 500, "calls through at rate 0.95")
}

func TestShouldCallSameSeedSameDecisions(t *testing.T) {
	run := func() int {
		th, _, _ := withSnapshot(t, testConfig(), rand.New(rand.NewPCG(7, 7)), snapFor("b", 5000))
		calls := 0
		for i := 0; i < 1000; i++ {
			if th.ShouldCall("b") {
				calls++
			}
		}
		return calls
	}
	assert.Equal(t, run(), run())
}

func TestShouldCallFailsOpenWhenSnapshotIsStale(t *testing.T) {
	cfg := testConfig()
	cfg.MaxStaleness = 5 * time.Minute
	rnd := &scriptedRandom{t: t, values: []int{0}} // draws 0 would always skip
	th, _, clk := withSnapshot(t, cfg, rnd, snapFor("b", 9500))

	clk.Advance(5 * time.Minute) // exactly at the limit: still fresh
	assert.False(t, th.ShouldCall("b"))

	clk.Advance(time.Second) // past the limit: fail open, no draw
	assert.True(t, th.ShouldCall("b"))
	assert.Equal(t, 1, rnd.calls)
}

func TestFreshRefreshEndsStaleness(t *testing.T) {
	cfg := testConfig()
	rnd := &scriptedRandom{t: t, values: []int{0}}
	st := &fakeStore{states: []store.Snapshot{snapFor("b", 9500)}}
	clk := clock.NewFake(testStart)
	th := newThrottler(t, cfg, st, rnd, clk)
	th.refresh(context.Background())

	clk.Advance(cfg.MaxStaleness + time.Second)
	require.True(t, th.ShouldCall("b"), "stale: fail open")

	th.refresh(context.Background())
	assert.False(t, th.ShouldCall("b"), "a fresh snapshot is used again")
}

func TestFlushSendsBatchForCurrentWindowAndResets(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	clk := clock.NewFake(testStart)
	st := &fakeStore{}
	th := newThrottler(t, cfg, st, &scriptedRandom{t: t}, clk)

	th.Record("a", ResultOK)
	th.Record("a", ResultOK)
	th.Record("a", ResultFailed)
	th.Record("b", ResultTimeout)
	th.flush(ctx)

	batches := st.allBatches()
	require.Len(t, batches, 1)
	window := store.WindowIndex(testStart, cfg.EvaluationWindow)
	assert.ElementsMatch(t, []store.Increment{
		{BuyerID: "a", Window: window, OK: 2, Failed: 1},
		{BuyerID: "b", Window: window, Timeout: 1},
	}, batches[0])

	th.flush(ctx) // counters were swapped to zero, so nothing is sent
	assert.Len(t, st.allBatches(), 1)
	assert.Equal(t, int64(4), th.Stats().FlushedCalls)
}

func TestFailedFlushDropsBatchAndDoesNotReaccumulate(t *testing.T) {
	ctx := context.Background()
	st := &fakeStore{incrErr: errors.New("redis down")}
	th := newThrottler(t, testConfig(), st, &scriptedRandom{t: t}, clock.NewFake(testStart))

	for i := 0; i < 5; i++ {
		th.Record("a", ResultOK)
	}
	th.flush(ctx)
	require.Len(t, st.allBatches(), 1)
	assert.Equal(t, int64(1), th.Stats().FlushFailures)
	assert.Equal(t, int64(0), th.Stats().FlushedCalls)

	// Store is back. The failed batch must not reappear. Only new calls are sent.
	st.setIncrErr(nil)
	th.Record("a", ResultOK)
	th.flush(ctx)

	batches := st.allBatches()
	require.Len(t, batches, 2)
	require.Len(t, batches[1], 1)
	assert.Equal(t, int64(1), batches[1][0].OK, "the dropped five are not resent")
	assert.Equal(t, int64(1), th.Stats().FlushedCalls)
}

func TestFailedRefreshKeepsLastSnapshot(t *testing.T) {
	ctx := context.Background()
	rnd := &scriptedRandom{t: t, values: []int{0}}
	th, st, _ := withSnapshot(t, testConfig(), rnd, snapFor("b", 9500))

	st.setStatesErr(errors.New("redis down"))
	th.refresh(ctx)

	assert.Equal(t, int64(1), th.Stats().RefreshFailures)
	assert.False(t, th.ShouldCall("b"), "the rate from the last good snapshot still applies")
}

func TestBuyerCapDropsNewBuyersAndKeepsTracked(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBuyers = 2
	st := &fakeStore{}
	th := newThrottler(t, cfg, st, &scriptedRandom{t: t}, clock.NewFake(testStart))

	th.Record("a", ResultOK)
	th.Record("b", ResultOK)
	th.Record("c", ResultFailed) // beyond the cap
	th.Record("a", ResultOK)     // existing buyers keep working at the cap
	th.flush(context.Background())

	stats := th.Stats()
	assert.Equal(t, int64(2), stats.TrackedBuyers)
	assert.Equal(t, int64(1), stats.Untracked)

	batches := st.allBatches()
	require.Len(t, batches, 1)
	ids := map[string]int64{}
	for _, inc := range batches[0] {
		ids[inc.BuyerID] = inc.OK
	}
	assert.Equal(t, map[string]int64{"a": 2, "b": 1}, ids, "buyer c is never sent")
}

func TestSnapshotCapKeepsHighestRates(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBuyers = 2
	rnd := &scriptedRandom{t: t}
	th, _, _ := withSnapshot(t, cfg, rnd,
		snapFor("low", 1000),
		snapFor("high", 9500),
		snapFor("mid", 5000),
	)
	assert.True(t, th.ShouldCall("low"), "lowest rate is dropped and fails open")
	assert.Equal(t, 0, rnd.calls)

	rnd2 := &scriptedRandom{t: t, values: []int{0, 0}}
	th2, _, _ := withSnapshot(t, cfg, rnd2,
		snapFor("low", 1000),
		snapFor("high", 9500),
		snapFor("mid", 5000),
	)
	assert.False(t, th2.ShouldCall("high"))
	assert.False(t, th2.ShouldCall("mid"))
}

func TestInvalidRecordIsCountedAndIgnored(t *testing.T) {
	ctx := context.Background()
	st := &fakeStore{}
	th := newThrottler(t, testConfig(), st, &scriptedRandom{t: t}, clock.NewFake(testStart))

	require.NotPanics(t, func() {
		th.Record("a", Result("bogus"))
		th.Record("a", Result(""))
		th.Record("", ResultOK)
	})
	assert.Equal(t, int64(3), th.Stats().Invalid)
	assert.Equal(t, int64(0), th.Stats().TrackedBuyers, "invalid input creates no counters")

	th.flush(ctx)
	assert.Empty(t, st.allBatches())
}

func TestConcurrentRecordAndShouldCallUnderRace(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	cfg.MaxBuyers = 1000
	st := &fakeStore{states: []store.Snapshot{snapFor("b0", 9500), snapFor("b1", 3000)}}
	// SystemRandom, not a seeded Rand: this test runs ShouldCall concurrently.
	th := newThrottler(t, cfg, st, SystemRandom{}, clock.NewFake(testStart))
	th.refresh(ctx)

	ids := []string{"b0", "b1", "b2", "b3"}
	const workers = 16
	const perWorker = 2000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				th.Record(ids[i%len(ids)], ResultOK)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				th.ShouldCall("b0")
			}
		}()
	}
	// Flushes and refreshes run alongside the request path.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			th.flush(ctx)
			th.refresh(ctx)
		}
	}()
	wg.Wait()
	th.flush(ctx) // send whatever is left

	var sent int64
	for _, batch := range st.allBatches() {
		for _, inc := range batch {
			sent += inc.OK
		}
	}
	assert.Equal(t, int64(workers*perWorker), sent, "every recorded call is flushed exactly once")
	assert.Equal(t, int64(workers*perWorker), th.Stats().FlushedCalls)
}
