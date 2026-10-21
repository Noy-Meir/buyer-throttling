package throttler

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Noy-Meir/buyer-throttling/internal/clock"
)

// Many goroutines record the same new buyer at once. Only one counter may be
// created, and the cap must not count the buyer twice.
func TestConcurrentFirstRecordCountsEachBuyerOnce(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBuyers = 2
	th := newThrottler(t, cfg, &fakeStore{}, &scriptedRandom{t: t}, clock.NewFake(testStart))

	// A start barrier releases every racer at once, so they overlap in the
	// creation path instead of running one after another.
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			th.Record("same", ResultOK)
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int64(1), th.Stats().TrackedBuyers, "one buyer, however many racers")
	assert.Equal(t, int64(0), th.Stats().Untracked)

	// The cap still has room for one more buyer after the race.
	th.Record("other", ResultOK)
	assert.Equal(t, int64(2), th.Stats().TrackedBuyers)
	th.Record("third", ResultOK)
	assert.Equal(t, int64(1), th.Stats().Untracked, "the third buyer is past the cap")
}

// The cap must never be exceeded, however many goroutines race to create buyers.
func TestCapIsNeverExceededUnderConcurrentCreation(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBuyers = 5
	th := newThrottler(t, cfg, &fakeStore{}, &scriptedRandom{t: t}, clock.NewFake(testStart))

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			th.Record("buyer-"+strconv.Itoa(i), ResultOK)
		}(i)
	}
	wg.Wait()

	stats := th.Stats()
	assert.Equal(t, int64(5), stats.TrackedBuyers, "exactly the cap, never more")
	assert.Equal(t, int64(195), stats.Untracked, "the rest are dropped and counted")
}

// With a free slot, racers for one new buyer must never be told it is untracked.
func TestConcurrentNewBuyerWithFreeSlotIsNeverUntracked(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBuyers = 10
	th := newThrottler(t, cfg, &fakeStore{}, &scriptedRandom{t: t}, clock.NewFake(testStart))

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			th.Record("new", ResultOK)
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int64(0), th.Stats().Untracked, "a free slot means no call for this buyer is untracked")
	assert.Equal(t, int64(1), th.Stats().TrackedBuyers)
}

// Once the cap is full, a record for a new buyer must not take createMu. The
// test holds the lock, so any attempt to take it would block and fail the test.
func TestRecordPastCapDoesNotTakeCreateLock(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBuyers = 1
	th := newThrottler(t, cfg, &fakeStore{}, &scriptedRandom{t: t}, clock.NewFake(testStart))
	th.Record("first", ResultOK) // fills the cap

	th.createMu.Lock()
	done := make(chan struct{})
	go func() {
		th.Record("second", ResultOK)
		th.Record("third", ResultOK)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		th.createMu.Unlock()
		t.Fatal("Record past the cap blocked on createMu")
	}
	th.createMu.Unlock()

	assert.Equal(t, int64(2), th.Stats().Untracked)
	assert.Equal(t, int64(1), th.Stats().TrackedBuyers)
}
