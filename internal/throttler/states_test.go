package throttler

import (
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

func snapWithReason(buyer string, rateBP int, reason evaluator.Reason) store.Snapshot {
	return store.Snapshot{BuyerID: buyer, State: evaluator.State{ThrottleRateBP: rateBP, Reason: reason}}
}

func TestStatesUnionOfSeenAndThrottledSortedWithReasons(t *testing.T) {
	th, _, _ := withSnapshot(t, testConfig(), &scriptedRandom{t: t},
		snapWithReason("buyer_456", 3500, evaluator.ReasonRecovering),
		snapWithReason("buyer_123", 9500, evaluator.ReasonFailureRate),
	)
	th.Record("buyer_789", ResultOK) // seen by this pod, open

	assert.Equal(t, []BuyerState{
		{BuyerID: "buyer_123", RateBP: 9500, Reason: evaluator.ReasonFailureRate},
		{BuyerID: "buyer_456", RateBP: 3500, Reason: evaluator.ReasonRecovering},
		{BuyerID: "buyer_789", RateBP: 0, Reason: evaluator.ReasonNone},
	}, th.States())
}

func TestStatesBeforeAnySnapshotIsSeenBuyersOpen(t *testing.T) {
	th := newThrottler(t, testConfig(), &fakeStore{}, &scriptedRandom{t: t}, clock.NewFake(testStart))
	th.Record("b", ResultOK)
	assert.Equal(t, []BuyerState{{BuyerID: "b"}}, th.States())
}

// The pod serves /state from memory. A store that fails every read must not
// change what it reports.
func TestStatesKeepsLastSnapshotWhenStoreIsDown(t *testing.T) {
	st := &fakeStore{states: []store.Snapshot{snapWithReason("b", 9500, evaluator.ReasonFailureRate)}}
	th := newThrottler(t, testConfig(), st, &scriptedRandom{t: t}, clock.NewFake(testStart))
	th.refresh(context.Background())

	st.setStatesErr(errors.New("redis down"))
	th.refresh(context.Background())

	assert.Equal(t, []BuyerState{{BuyerID: "b", RateBP: 9500, Reason: evaluator.ReasonFailureRate}}, th.States())
}

func TestSnapshotAge(t *testing.T) {
	th, _, clk := withSnapshot(t, testConfig(), &scriptedRandom{t: t}, snapFor("b", 9500))
	age, ok := th.SnapshotAge()
	require.True(t, ok)
	assert.Equal(t, time.Duration(0), age)

	clk.Advance(7 * time.Second)
	age, _ = th.SnapshotAge()
	assert.Equal(t, 7*time.Second, age)
}

func TestSnapshotAgeUnknownBeforeFirstRefresh(t *testing.T) {
	th := newThrottler(t, testConfig(), &fakeStore{}, &scriptedRandom{t: t}, clock.NewFake(testStart))
	_, ok := th.SnapshotAge()
	assert.False(t, ok)
}
