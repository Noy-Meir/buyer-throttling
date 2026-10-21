package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
)

// newUnretriedStore returns a store whose client does not retry, so a dead
// server fails at once instead of waiting through backoff.
func newUnretriedStore(t *testing.T) (*RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisStore(client, testStatsTTL, testStateTTL), mr
}

// A pod or evaluator must see Redis errors, not zero values. A zero value
// would read as "no traffic" or "not throttled", and an outage would be hidden.
func TestEveryOperationReturnsErrorWhenRedisIsDown(t *testing.T) {
	ctx := context.Background()
	s, mr := newUnretriedStore(t)
	mr.Close()

	_, err := s.WindowStats(ctx, "b", 1)
	assert.Error(t, err, "WindowStats")
	_, err = s.Buyers(ctx)
	assert.Error(t, err, "Buyers")
	_, err = s.State(ctx, "b")
	assert.Error(t, err, "State")
	_, err = s.AllStates(ctx)
	assert.Error(t, err, "AllStates")
	err = s.IncrStats(ctx, []Increment{{BuyerID: "b", Window: 1, OK: 1}})
	assert.Error(t, err, "IncrStats")
	applied, err := s.CompareAndSetState(ctx, "b", 1, evaluator.State{ThrottleRateBP: 9500})
	assert.Error(t, err, "CompareAndSetState")
	assert.False(t, applied, "a failed write must not report success")
}

// A corrupt state hash is an error. Reading it as zero would open a blocked buyer.
func TestCorruptStateIsAnErrorNotOpen(t *testing.T) {
	ctx := context.Background()
	s, mr := newTestStore(t)
	mr.HSet("state:bad", "rate", "not-a-number")
	mr.HSet("state:bad", "last_window", "5")
	_, err := mr.SetAdd("buyers", "bad") // AllStates lists buyers from this set
	require.NoError(t, err)

	_, err = s.State(ctx, "bad")
	assert.Error(t, err)

	_, err = s.AllStates(ctx)
	assert.Error(t, err, "one corrupt buyer fails the whole listing")
}

func TestCorruptStatsCounterIsAnError(t *testing.T) {
	ctx := context.Background()
	s, mr := newTestStore(t)
	mr.HSet("stats:b:3", "failed", "lots")

	_, err := s.WindowStats(ctx, "b", 3)
	assert.Error(t, err)
}

func TestAllStatesWithNoBuyersIsEmpty(t *testing.T) {
	s, _ := newTestStore(t)
	got, err := s.AllStates(context.Background())
	require.NoError(t, err)
	assert.Empty(t, got)
}
