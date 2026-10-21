package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
)

const testStatsTTL = 5 * time.Minute

func newTestStore(t *testing.T) (*RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisStore(client, testStatsTTL), mr
}

func TestIncrStatsAndWindowStatsDeriveTotal(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	err := s.IncrStats(ctx, []Increment{
		{BuyerID: "b1", Window: 100, OK: 80, Failed: 15, Timeout: 5},
		{BuyerID: "b1", Window: 100, OK: 20, Failed: 0, Timeout: 0},
	})
	require.NoError(t, err)

	got, err := s.WindowStats(ctx, "b1", 100)
	require.NoError(t, err)
	assert.Equal(t, evaluator.WindowStats{Total: 120, Failed: 15, Timeout: 5}, got)
}

func TestWindowStatsMissingWindowIsZero(t *testing.T) {
	s, _ := newTestStore(t)
	got, err := s.WindowStats(context.Background(), "nobody", 42)
	require.NoError(t, err)
	assert.Equal(t, evaluator.WindowStats{}, got)
}

func TestIncrStatsBatchAcrossBuyersAndWindows(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	require.NoError(t, s.IncrStats(ctx, []Increment{
		{BuyerID: "a", Window: 1, OK: 10},
		{BuyerID: "a", Window: 2, Failed: 4},
		{BuyerID: "b", Window: 1, Timeout: 3},
	}))

	a1, err := s.WindowStats(ctx, "a", 1)
	require.NoError(t, err)
	assert.Equal(t, evaluator.WindowStats{Total: 10}, a1)

	a2, err := s.WindowStats(ctx, "a", 2)
	require.NoError(t, err)
	assert.Equal(t, evaluator.WindowStats{Total: 4, Failed: 4}, a2)

	b1, err := s.WindowStats(ctx, "b", 1)
	require.NoError(t, err)
	assert.Equal(t, evaluator.WindowStats{Total: 3, Timeout: 3}, b1)
}

func TestIncrStatsEmptyBatchIsNoop(t *testing.T) {
	s, mr := newTestStore(t)
	require.NoError(t, s.IncrStats(context.Background(), nil))
	assert.Empty(t, mr.Keys())
}

func TestBuyersListsEveryReportingBuyerSorted(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	require.NoError(t, s.IncrStats(ctx, []Increment{
		{BuyerID: "zeta", Window: 1, OK: 1},
		{BuyerID: "alpha", Window: 1, OK: 1},
		{BuyerID: "zeta", Window: 2, OK: 1},
	}))

	got, err := s.Buyers(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "zeta"}, got)
}

func TestStatsKeysExpireAfterTTL(t *testing.T) {
	ctx := context.Background()
	s, mr := newTestStore(t)
	require.NoError(t, s.IncrStats(ctx, []Increment{{BuyerID: "b", Window: 7, OK: 5}}))

	key := "stats:b:7"
	assert.Equal(t, testStatsTTL, mr.TTL(key))

	mr.FastForward(testStatsTTL - time.Second)
	got, err := s.WindowStats(ctx, "b", 7)
	require.NoError(t, err)
	assert.Equal(t, int64(5), got.Total, "still alive just before TTL")

	mr.FastForward(time.Second)
	got, err = s.WindowStats(ctx, "b", 7)
	require.NoError(t, err)
	assert.Equal(t, evaluator.WindowStats{}, got, "expired after TTL")
}

func TestStatsTTLIsRefreshedByLaterWrites(t *testing.T) {
	ctx := context.Background()
	s, mr := newTestStore(t)
	require.NoError(t, s.IncrStats(ctx, []Increment{{BuyerID: "b", Window: 7, OK: 1}}))

	mr.FastForward(testStatsTTL - time.Second)
	require.NoError(t, s.IncrStats(ctx, []Increment{{BuyerID: "b", Window: 7, OK: 1}}))
	mr.FastForward(testStatsTTL - time.Second)

	got, err := s.WindowStats(ctx, "b", 7)
	require.NoError(t, err)
	assert.Equal(t, int64(2), got.Total, "a late write extends the window's life")
}

func TestUnknownStateIsZeroSnapshot(t *testing.T) {
	s, _ := newTestStore(t)
	got, err := s.State(context.Background(), "new-buyer")
	require.NoError(t, err)
	assert.Equal(t, Snapshot{BuyerID: "new-buyer"}, got)
}

func TestCompareAndSetRoundTripsStateAndCarry(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	next := evaluator.State{
		ThrottleRateBP: 9500,
		Reason:         evaluator.ReasonFailureRate,
		CarriedStats:   evaluator.WindowStats{Total: 40, Failed: 10, Timeout: 2},
		CarriedWindows: 2,
	}
	applied, err := s.CompareAndSetState(ctx, "b", 100, next)
	require.NoError(t, err)
	require.True(t, applied)

	got, err := s.State(ctx, "b")
	require.NoError(t, err)
	assert.Equal(t, Snapshot{BuyerID: "b", State: next, LastEvaluatedWindow: 100}, got)
}

func TestCompareAndSetIsIdempotentForSameWindow(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	first := evaluator.State{ThrottleRateBP: 9500, Reason: evaluator.ReasonFailureRate}
	second := evaluator.State{ThrottleRateBP: 0, Reason: evaluator.ReasonNone}

	applied, err := s.CompareAndSetState(ctx, "b", 100, first)
	require.NoError(t, err)
	require.True(t, applied)

	// Evaluating window 100 again, even with a different answer, must be a no-op.
	applied, err = s.CompareAndSetState(ctx, "b", 100, second)
	require.NoError(t, err)
	assert.False(t, applied)

	got, err := s.State(ctx, "b")
	require.NoError(t, err)
	assert.Equal(t, first, got.State)
	assert.Equal(t, int64(100), got.LastEvaluatedWindow)
}

func TestCompareAndSetRejectsOlderWindow(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	newer := evaluator.State{ThrottleRateBP: 8500, Reason: evaluator.ReasonRecovering}

	_, err := s.CompareAndSetState(ctx, "b", 101, newer)
	require.NoError(t, err)

	applied, err := s.CompareAndSetState(ctx, "b", 100, evaluator.State{ThrottleRateBP: 9500, Reason: evaluator.ReasonFailureRate})
	require.NoError(t, err)
	assert.False(t, applied, "a late write for an earlier window must not overwrite")

	got, err := s.State(ctx, "b")
	require.NoError(t, err)
	assert.Equal(t, newer, got.State)
}

func TestCompareAndSetAdvancesThroughLaterWindows(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	for w := int64(1); w <= 3; w++ {
		applied, err := s.CompareAndSetState(ctx, "b", w, evaluator.State{ThrottleRateBP: int(w)})
		require.NoError(t, err)
		require.True(t, applied, "window %d", w)
	}
	got, err := s.State(ctx, "b")
	require.NoError(t, err)
	assert.Equal(t, int64(3), got.LastEvaluatedWindow)
}

func TestAllStatesReturnsEveryKnownBuyer(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	require.NoError(t, s.IncrStats(ctx, []Increment{
		{BuyerID: "a", Window: 1, OK: 1},
		{BuyerID: "b", Window: 1, OK: 1},
	}))
	_, err := s.CompareAndSetState(ctx, "a", 1, evaluator.State{ThrottleRateBP: 9500, Reason: evaluator.ReasonFailureRate})
	require.NoError(t, err)

	got, err := s.AllStates(ctx)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "a", got[0].BuyerID)
	assert.Equal(t, 9500, got[0].State.ThrottleRateBP)
	assert.Equal(t, "b", got[1].BuyerID)
	assert.Equal(t, Snapshot{BuyerID: "b"}, got[1], "never-evaluated buyer is zero state")
}

func TestConcurrentIncrementsAreNotLost(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	const workers = 50
	const perWorker = 20
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				err := s.IncrStats(ctx, []Increment{{BuyerID: "hot", Window: 9, OK: 2, Failed: 1}})
				assert.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	got, err := s.WindowStats(ctx, "hot", 9)
	require.NoError(t, err)
	assert.Equal(t, evaluator.WindowStats{Total: workers * perWorker * 3, Failed: workers * perWorker}, got)
}

func TestConcurrentCompareAndSetHasExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	const evaluators = 20
	results := make(chan bool, evaluators)
	var wg sync.WaitGroup
	for i := 0; i < evaluators; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			applied, err := s.CompareAndSetState(ctx, "b", 55, evaluator.State{ThrottleRateBP: i})
			assert.NoError(t, err)
			results <- applied
		}(i)
	}
	wg.Wait()
	close(results)

	winners := 0
	for applied := range results {
		if applied {
			winners++
		}
	}
	assert.Equal(t, 1, winners, "exactly one evaluator may write a given window")
}

// TestCompareAndSetKeysAreIndependentPerBuyer guards against the script
// touching the wrong key when many buyers are evaluated in one window.
func TestCompareAndSetKeysAreIndependentPerBuyer(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("buyer-%d", i)
		applied, err := s.CompareAndSetState(ctx, id, 10, evaluator.State{ThrottleRateBP: i * 100})
		require.NoError(t, err)
		require.True(t, applied)
	}
	for i := 0; i < 5; i++ {
		got, err := s.State(ctx, fmt.Sprintf("buyer-%d", i))
		require.NoError(t, err)
		assert.Equal(t, i*100, got.State.ThrottleRateBP)
	}
}

func TestWindowIndexCountsWholeWindowsSinceEpoch(t *testing.T) {
	minute := time.Minute
	assert.Equal(t, int64(0), WindowIndex(time.Unix(0, 0), minute))
	assert.Equal(t, int64(0), WindowIndex(time.Unix(59, 999_999_999), minute))
	assert.Equal(t, int64(1), WindowIndex(time.Unix(60, 0), minute))
	assert.Equal(t, int64(28_000_000), WindowIndex(time.Unix(1_680_000_000, 0), minute))
}
