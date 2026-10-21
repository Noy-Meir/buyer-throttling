package runner

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
	"github.com/Noy-Meir/buyer-throttling/internal/store"
)

var testParams = evaluator.Params{
	FailureThresholdBP: 7000,
	TimeoutThresholdBP: 3000,
	MinSample:          100,
	BlockLevelBP:       9500,
	RecoveryStepBP:     1000,
	CarryCapWindows:    5,
}

// memStore is an in-memory store.Store for runner tests. It mirrors the Redis
// semantics that matter here: the compare-and-set only moves forward.
type memStore struct {
	mu        sync.Mutex
	stats     map[string]evaluator.WindowStats // key: buyer|window
	states    map[string]store.Snapshot
	buyers    map[string]bool
	casCalls  int
	statReads chan int64 // optional: receives the window of each WindowStats read
	failState map[string]error
}

func newMemStore() *memStore {
	return &memStore{
		stats:  map[string]evaluator.WindowStats{},
		states: map[string]store.Snapshot{},
		buyers: map[string]bool{},
	}
}

func key(buyer string, window int64) string { return buyer + "|" + strconv.FormatInt(window, 10) }

// setStats seeds a buyer's window and registers the buyer.
func (m *memStore) setStats(buyer string, window int64, s evaluator.WindowStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats[key(buyer, window)] = s
	m.buyers[buyer] = true
}

func (m *memStore) IncrStats(_ context.Context, batch []store.Increment) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, inc := range batch {
		k := key(inc.BuyerID, inc.Window)
		s := m.stats[k]
		s.Total += inc.OK + inc.Failed + inc.Timeout
		s.Failed += inc.Failed
		s.Timeout += inc.Timeout
		m.stats[k] = s
		m.buyers[inc.BuyerID] = true
	}
	return nil
}

func (m *memStore) WindowStats(_ context.Context, buyer string, window int64) (evaluator.WindowStats, error) {
	if m.statReads != nil {
		m.statReads <- window
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.failState[buyer]; err != nil {
		return evaluator.WindowStats{}, err
	}
	return m.stats[key(buyer, window)], nil
}

func (m *memStore) Buyers(_ context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for id := range m.buyers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (m *memStore) State(_ context.Context, buyer string) (store.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.failState[buyer]; err != nil {
		return store.Snapshot{}, err
	}
	return m.states[buyer], nil
}

func (m *memStore) AllStates(_ context.Context) ([]store.Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.Snapshot
	for _, s := range m.states {
		out = append(out, s)
	}
	return out, nil
}

func (m *memStore) CompareAndSetState(_ context.Context, buyer string, window int64, next evaluator.State) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.casCalls++
	cur := m.states[buyer]
	if cur.LastEvaluatedWindow >= window {
		return false, nil
	}
	m.states[buyer] = store.Snapshot{BuyerID: buyer, State: next, LastEvaluatedWindow: window}
	return true, nil
}

func (m *memStore) snapshot(buyer string) store.Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.states[buyer]
}

// window index helpers: a minute is one window, so index == unix minute.
func minuteIndex(t time.Time) int64 { return store.WindowIndex(t, time.Minute) }

func newRunner(st store.Store, clk clock.Clock, logs *bytes.Buffer) *Runner {
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return New(st, clk, testParams, time.Minute, 5*time.Second, logger)
}

func TestNextTickAlignsToBoundaryPlusGrace(t *testing.T) {
	r := &Runner{Window: time.Minute, Grace: 5 * time.Second}
	tests := []struct {
		name       string
		now        time.Time
		wantFire   time.Time
		wantClosed time.Time // start of the window that becomes evaluable at wantFire
	}{
		{
			name:       "mid-window: next tick after the boundary plus grace",
			now:        time.Date(2026, 1, 2, 12, 0, 30, 0, time.UTC),
			wantFire:   time.Date(2026, 1, 2, 12, 1, 5, 0, time.UTC),
			wantClosed: time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC),
		},
		{
			name:       "inside grace after a boundary: previous window is not yet ready",
			now:        time.Date(2026, 1, 2, 12, 0, 4, 0, time.UTC),
			wantFire:   time.Date(2026, 1, 2, 12, 0, 5, 0, time.UTC),
			wantClosed: time.Date(2026, 1, 2, 11, 59, 0, 0, time.UTC),
		},
		{
			name:       "exactly at the fire instant: schedules the next one",
			now:        time.Date(2026, 1, 2, 12, 1, 5, 0, time.UTC),
			wantFire:   time.Date(2026, 1, 2, 12, 2, 5, 0, time.UTC),
			wantClosed: time.Date(2026, 1, 2, 12, 1, 0, 0, time.UTC),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fire, window := r.nextTick(tc.now)
			assert.True(t, tc.wantFire.Equal(fire), "fire: got %v want %v", fire, tc.wantFire)
			assert.Equal(t, minuteIndex(tc.wantClosed), window)
		})
	}
}

func TestNextTickNeverReturnsAnAlreadyClosedWindow(t *testing.T) {
	r := &Runner{Window: time.Minute, Grace: 5 * time.Second}
	now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 500; i++ {
		fire, _ := r.nextTick(now)
		require.True(t, fire.After(now), "fire %v must be after now %v", fire, now)
		require.True(t, fire.Sub(now) <= 65*time.Second, "fire must be within one window")
		now = now.Add(7 * time.Second)
	}
}

func TestTickBlocksBuyerOnFailureRate(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	var logs bytes.Buffer
	st.setStats("bad", 10, evaluator.WindowStats{Total: 100, Failed: 70})

	require.NoError(t, newRunner(st, clock.NewFake(time.Unix(0, 0)), &logs).Tick(ctx, 10))

	assert.Equal(t, store.Snapshot{
		BuyerID:             "bad",
		State:               evaluator.State{ThrottleRateBP: 9500, Reason: evaluator.ReasonFailureRate},
		LastEvaluatedWindow: 10,
	}, st.snapshot("bad"))
}

func TestTickIsIdempotentForSameWindow(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	var logs bytes.Buffer
	r := newRunner(st, clock.NewFake(time.Unix(0, 0)), &logs)
	st.setStats("b", 10, evaluator.WindowStats{Total: 100, Failed: 70})

	require.NoError(t, r.Tick(ctx, 10))
	blocked := st.snapshot("b")

	// Stats change after evaluation. Re-evaluating window 10 must not move the state.
	st.setStats("b", 10, evaluator.WindowStats{Total: 100})
	require.NoError(t, r.Tick(ctx, 10))

	assert.Equal(t, blocked, st.snapshot("b"))
	assert.Equal(t, 1, st.casCalls, "the second evaluation must stop before writing")
}

func TestTickNeverBackfillsOlderWindows(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	var logs bytes.Buffer
	r := newRunner(st, clock.NewFake(time.Unix(0, 0)), &logs)
	st.setStats("b", 9, evaluator.WindowStats{Total: 100, Failed: 70})
	st.setStats("b", 7, evaluator.WindowStats{Total: 100, Failed: 70})

	require.NoError(t, r.Tick(ctx, 9))
	require.NoError(t, r.Tick(ctx, 7))

	assert.Equal(t, int64(9), st.snapshot("b").LastEvaluatedWindow)
	assert.Equal(t, 1, st.casCalls, "window 7 is older and must not be written")
}

func TestTickRecoversOverTenCleanWindows(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	var logs bytes.Buffer
	r := newRunner(st, clock.NewFake(time.Unix(0, 0)), &logs)
	st.setStats("b", 1, evaluator.WindowStats{Total: 100, Failed: 100})
	require.NoError(t, r.Tick(ctx, 1))

	for w := int64(2); w <= 11; w++ {
		st.setStats("b", w, evaluator.WindowStats{Total: 100})
		require.NoError(t, r.Tick(ctx, w))
	}
	assert.Equal(t, evaluator.State{}, st.snapshot("b").State)
}

func TestTickEvaluatesEveryKnownBuyerIndependently(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	var logs bytes.Buffer
	r := newRunner(st, clock.NewFake(time.Unix(0, 0)), &logs)
	st.setStats("bad", 5, evaluator.WindowStats{Total: 100, Timeout: 30})
	st.setStats("good", 5, evaluator.WindowStats{Total: 100})
	st.setStats("quiet", 5, evaluator.WindowStats{})

	require.NoError(t, r.Tick(ctx, 5))

	assert.Equal(t, evaluator.State{ThrottleRateBP: 9500, Reason: evaluator.ReasonTimeoutRate}, st.snapshot("bad").State)
	assert.Equal(t, evaluator.State{}, st.snapshot("good").State)
	assert.Equal(t, evaluator.State{CarriedWindows: 1}, st.snapshot("quiet").State,
		"no traffic is insufficient: it is carried at rate 0, never blocked")
}

func TestTickContinuesPastOneBuyerError(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	var logs bytes.Buffer
	r := newRunner(st, clock.NewFake(time.Unix(0, 0)), &logs)
	st.setStats("a-broken", 5, evaluator.WindowStats{Total: 100, Failed: 70})
	st.setStats("b-fine", 5, evaluator.WindowStats{Total: 100, Failed: 70})
	st.failState = map[string]error{"a-broken": errors.New("boom")}

	err := r.Tick(ctx, 5)

	require.Error(t, err)
	assert.ErrorContains(t, err, "a-broken")
	assert.ErrorContains(t, err, "boom")
	assert.Equal(t, int64(5), st.snapshot("b-fine").LastEvaluatedWindow, "the other buyer is still evaluated")
}

func TestTickLogsBothViolationFlags(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	var logs bytes.Buffer
	r := newRunner(st, clock.NewFake(time.Unix(0, 0)), &logs)
	st.setStats("both", 5, evaluator.WindowStats{Total: 100, Failed: 70, Timeout: 30})

	require.NoError(t, r.Tick(ctx, 5))

	out := logs.String()
	assert.Contains(t, out, "level=WARN")
	assert.Contains(t, out, "buyer=both")
	assert.Contains(t, out, "failure_rate_violated=true")
	assert.Contains(t, out, "timeout_rate_violated=true")
	assert.Contains(t, out, "reason=failure_rate")
}

func TestTickLogsStateChangesAtInfoAndSteadyStateAtDebug(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	var logs bytes.Buffer
	r := newRunner(st, clock.NewFake(time.Unix(0, 0)), &logs)
	st.setStats("b", 1, evaluator.WindowStats{Total: 100})

	require.NoError(t, r.Tick(ctx, 1))
	assert.NotContains(t, logs.String(), "level=INFO", "steady state at rate 0 is debug only")
	assert.Contains(t, logs.String(), "level=DEBUG")
}

func TestRunEvaluatesLastClosedWindowAtBoundaryPlusGrace(t *testing.T) {
	start := time.Date(2026, 1, 2, 12, 0, 30, 0, time.UTC)
	clk := clock.NewFake(start)
	st := newMemStore()
	st.statReads = make(chan int64, 4)
	st.setStats("b", minuteIndex(time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)), evaluator.WindowStats{Total: 100})
	var logs bytes.Buffer
	r := newRunner(st, clk, &logs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	clk.BlockUntil(1)
	clk.Advance(35 * time.Second) // now 12:01:05, which is 12:01 boundary plus grace

	got := <-st.statReads
	assert.Equal(t, minuteIndex(time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)), got, "window 12:00 is the last closed one")

	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestRunDoesNotBackfillAfterAPause(t *testing.T) {
	start := time.Date(2026, 1, 2, 12, 0, 30, 0, time.UTC)
	clk := clock.NewFake(start)
	st := newMemStore()
	st.statReads = make(chan int64, 8)
	st.setStats("b", 0, evaluator.WindowStats{Total: 100}) // registers the buyer
	var logs bytes.Buffer
	r := newRunner(st, clk, &logs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	clk.BlockUntil(1)
	// The process "stalls" for five minutes. Four boundaries pass unevaluated.
	clk.Advance(5 * time.Minute)

	first := <-st.statReads
	assert.Equal(t, minuteIndex(time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)), first,
		"only the most recent closed window is evaluated")

	// Next fire is 12:06:05, which closes window 12:05. Nothing for 12:01-12:04.
	clk.BlockUntil(1)
	clk.Advance(35 * time.Second) // now 12:06:05
	second := <-st.statReads
	assert.Equal(t, minuteIndex(time.Date(2026, 1, 2, 12, 5, 0, 0, time.UTC)), second)
	assert.Empty(t, st.statReads, "no reads for the skipped windows")

	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestTickLogsCarriedSmallWindowAtInfo(t *testing.T) {
	ctx := context.Background()
	st := newMemStore()
	var logs bytes.Buffer
	r := newRunner(st, clock.NewFake(time.Unix(0, 0)), &logs)
	st.setStats("quiet", 3, evaluator.WindowStats{Total: 40})
	_, err := st.CompareAndSetState(ctx, "quiet", 2, evaluator.State{ThrottleRateBP: 9500, Reason: evaluator.ReasonFailureRate})
	require.NoError(t, err)

	require.NoError(t, r.Tick(ctx, 3))

	assert.Contains(t, logs.String(), "level=INFO")
	assert.Contains(t, logs.String(), "carried_windows=1")
	assert.Contains(t, logs.String(), "effective_total=40")
}
