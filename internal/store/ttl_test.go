package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
)

// A buyer that the evaluator keeps writing keeps its state. The key expires
// only after writes stop for the full TTL.
func TestStateExpiresOnlyAfterWritesStop(t *testing.T) {
	ctx := context.Background()
	s, mr := newTestStore(t)
	state := evaluator.State{ThrottleRateBP: 9500, Reason: evaluator.ReasonFailureRate}

	_, err := s.CompareAndSetState(ctx, "b", 1, state)
	require.NoError(t, err)
	assert.Equal(t, testStateTTL, mr.TTL("state:b"), "the write sets the TTL")

	// Still being evaluated: each window writes again and refreshes the TTL.
	mr.FastForward(testStateTTL - time.Second)
	_, err = s.CompareAndSetState(ctx, "b", 2, state)
	require.NoError(t, err)
	mr.FastForward(testStateTTL - time.Second)

	got, err := s.State(ctx, "b")
	require.NoError(t, err)
	assert.Equal(t, state, got.State, "a refreshed state is not expired")

	// Writes stop. After the full TTL with no write, the state is gone.
	mr.FastForward(time.Second)
	got, err = s.State(ctx, "b")
	require.NoError(t, err)
	assert.Equal(t, Snapshot{BuyerID: "b"}, got, "an unwritten state expires to the zero state")
}

// The buyers set expires only when no batch has arrived for the TTL. Any buyer
// still sending stats keeps the set alive.
func TestBuyersSetExpiresOnlyAfterTrafficStops(t *testing.T) {
	ctx := context.Background()
	s, mr := newTestStore(t)
	require.NoError(t, s.IncrStats(ctx, []Increment{{BuyerID: "b", Window: 1, OK: 1}}))
	assert.Equal(t, testStateTTL, mr.TTL(buyersKey))

	mr.FastForward(testStateTTL - time.Second)
	require.NoError(t, s.IncrStats(ctx, []Increment{{BuyerID: "b", Window: 2, OK: 1}}))
	mr.FastForward(testStateTTL - time.Second)

	buyers, err := s.Buyers(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, buyers, "a batch refreshed the set")

	mr.FastForward(time.Second)
	buyers, err = s.Buyers(ctx)
	require.NoError(t, err)
	assert.Empty(t, buyers, "with no traffic for the full TTL, the set is gone")
}
