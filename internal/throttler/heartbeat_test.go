package throttler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/store"
)

// alwaysSkip makes every draw 0, so a buyer at a positive rate is skipped every
// time it is enforced. ShouldCall returning true therefore means "failing open".
type alwaysSkip struct{}

func (alwaysSkip) IntN(int) int { return 0 }

func newHeartbeatRig(t *testing.T, present bool, value int64) (*Throttler, *fakeStore, *clock.Fake) {
	t.Helper()
	cfg := testConfig()
	st := &fakeStore{states: []store.Snapshot{snapFor("b", 9500)}}
	st.setHeartbeat(value, present)
	clk := clock.NewFake(testStart)
	th := newThrottler(t, cfg, st, alwaysSkip{}, clk)
	th.refresh(context.Background())
	return th, st, clk
}

// step advances the clock by d in refresh-sized steps, refreshing the snapshot
// at each step as a pod does. It returns the ShouldCall result at the end.
func step(th *Throttler, clk *clock.Fake, d time.Duration) bool {
	const refresh = 2 * time.Second
	for d > 0 {
		s := refresh
		if d < s {
			s = d
		}
		clk.Advance(s)
		th.refresh(context.Background())
		d -= s
	}
	return th.ShouldCall("b")
}

// Evaluator dead, Redis up: the counter stops, refreshes keep succeeding, and
// pods still fail open once MaxStaleness has passed without a change.
func TestEvaluatorDeadWithRedisUpFailsOpenAfterMaxStaleness(t *testing.T) {
	th, _, clk := newHeartbeatRig(t, true, 7)
	max := testConfig().MaxStaleness

	assert.False(t, step(th, clk, max-time.Second), "just inside MaxStaleness: still enforcing")
	assert.True(t, step(th, clk, 2*time.Second), "past MaxStaleness with no change: fail open")
	assert.Equal(t, int64(0), th.Stats().RefreshFailures, "refreshes were succeeding the whole time")
}

// Evaluator alive, buyer held in carry: the counter moves every window while the
// buyer's state does not change, so pods keep enforcing for well past MaxStaleness.
func TestEvaluatorAliveKeepsHeldBuyerEnforced(t *testing.T) {
	th, st, clk := newHeartbeatRig(t, true, 1)
	window := 10 * time.Second
	for elapsed := time.Duration(0); elapsed < 2*testConfig().MaxStaleness; elapsed += 2 * time.Second {
		clk.Advance(2 * time.Second)
		if elapsed%window == 0 {
			st.setHeartbeat(int64(elapsed/window)+2, true) // one tick per window
		}
		th.refresh(context.Background())
		require.False(t, th.ShouldCall("b"), "at %v the evaluator is alive: still enforcing", elapsed)
	}
}

// First start, no counter at all: the staleness clock starts at the first
// refresh. The pod enforces until MaxStaleness has passed, then fails open.
func TestFirstStartWithNoCounterFailsOpenOnlyAfterMaxStaleness(t *testing.T) {
	th, _, clk := newHeartbeatRig(t, false, 0)
	max := testConfig().MaxStaleness

	assert.False(t, step(th, clk, max-time.Second), "a missing counter does not fail open at once")
	assert.True(t, step(th, clk, 2*time.Second), "after MaxStaleness from the first refresh it does")
}

// The counter is lost (Redis restored from an old snapshot). That is "not
// changing": the pod keeps enforcing until MaxStaleness after the last change,
// and does not fail open at the moment the key disappears.
func TestCounterLostAfterRestoreWaitsMaxStaleness(t *testing.T) {
	th, st, clk := newHeartbeatRig(t, true, 42)
	max := testConfig().MaxStaleness

	clk.Advance(100 * time.Second)
	th.refresh(context.Background())
	st.setHeartbeat(0, false) // the key disappears

	assert.False(t, step(th, clk, max-101*time.Second), "still inside MaxStaleness of the last change")
	assert.True(t, step(th, clk, 2*time.Second), "past it, with no change, the pod fails open")
}

// A restore can also bring back an older, lower counter. Any change counts as
// the evaluator being alive, so a lower value resets the clock.
func TestRestoredLowerCounterCountsAsChange(t *testing.T) {
	th, st, clk := newHeartbeatRig(t, true, 42)
	max := testConfig().MaxStaleness

	clk.Advance(200 * time.Second)
	st.setHeartbeat(5, true)
	th.refresh(context.Background())

	assert.False(t, step(th, clk, max-time.Second), "the lower value reset the clock")
	assert.True(t, step(th, clk, 2*time.Second), "MaxStaleness after that reset with no change, fail open")
}
