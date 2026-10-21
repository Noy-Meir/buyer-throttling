package auction

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Racers for one new buyer with a free slot must all get its counters. A nil
// result would drop that buyer's per-buyer metrics.
func TestConcurrentCountsForNewBuyerNeverReturnsNil(t *testing.T) {
	svc := NewService(newFakeThrottler(), &fakeCaller{}, 0, 2)

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan *buyerCounts, 256)
	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- svc.countsFor("same")
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	nils := 0
	for c := range results {
		if c == nil {
			nils++
		}
	}
	assert.Equal(t, 0, nils, "a free slot means every racer gets the buyer's counters")
	assert.Equal(t, int64(1), svc.tracked.Load(), "one buyer, one slot")
}

// The cap must hold under concurrent creation.
func TestConcurrentCountsForNeverExceedsCap(t *testing.T) {
	svc := NewService(newFakeThrottler(), &fakeCaller{}, 0, 5)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			svc.countsFor("buyer-" + string(rune('a'+i%26)) + string(rune('a'+i/26)))
		}(i)
	}
	close(start)
	wg.Wait()

	totals, _ := svc.Totals()
	assert.Len(t, totals, 5, "Totals lists every tracked buyer, even with zero calls")
	assert.Equal(t, int64(5), svc.tracked.Load(), "exactly the cap, never more")
}
