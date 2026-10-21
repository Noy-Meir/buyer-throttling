package throttler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/store"
)

func TestStartFlushesOnIntervalAndStopFlushesRemainder(t *testing.T) {
	cfg := testConfig()
	st := &fakeStore{
		states:  []store.Snapshot{snapFor("b", 9500)},
		flushed: make(chan struct{}, 10),
	}
	clk := clock.NewFake(testStart)
	rnd := &scriptedRandom{t: t, values: []int{0}}
	th := newThrottler(t, cfg, st, rnd, clk)

	th.Start(context.Background())
	require.False(t, th.ShouldCall("b"), "Start loads the snapshot before serving")

	// Wait for the flush and refresh timers to be registered, then move past
	// the first flush interval.
	clk.BlockUntil(2)
	for i := 0; i < 3; i++ {
		th.Record("b", ResultOK)
	}
	clk.Advance(cfg.FlushInterval)
	<-st.flushed

	batches := st.allBatches()
	require.Len(t, batches, 1)
	assert.Equal(t, int64(3), batches[0][0].OK, "the timed flush sends what was recorded")

	// Record a little more and stop without advancing the clock. Stop must still
	// flush what is left.
	th.Record("b", ResultFailed)
	th.Record("b", ResultFailed)
	th.Stop()

	batches = st.allBatches()
	require.Len(t, batches, 2)
	assert.Equal(t, int64(2), batches[1][0].Failed, "shutdown flushes the remainder")
	assert.Equal(t, int64(5), th.Stats().FlushedCalls)

	th.Stop() // a second Stop is a no-op
	assert.Len(t, st.allBatches(), 2)
}

func TestStopBeforeStartIsNoop(t *testing.T) {
	st := &fakeStore{}
	th := newThrottler(t, testConfig(), st, &scriptedRandom{t: t}, clock.NewFake(testStart))
	th.Record("b", ResultOK)
	th.Stop()
	assert.Empty(t, st.allBatches(), "a throttler that never started does not flush on Stop")
}

func TestContextCancelStopsLoopsWithoutFlushing(t *testing.T) {
	st := &fakeStore{}
	clk := clock.NewFake(testStart)
	th := newThrottler(t, testConfig(), st, &scriptedRandom{t: t}, clk)

	ctx, cancel := context.WithCancel(context.Background())
	th.Start(ctx)
	clk.BlockUntil(2)
	cancel()
	th.Stop() // waits for the loops, then performs the final flush
	assert.Empty(t, st.allBatches())
}
