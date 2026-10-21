package auction

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Noy-Meir/buyer-throttling/internal/throttler"
)

// fakeThrottler answers ShouldCall from a map (default true) and records every Record.
type fakeThrottler struct {
	mu       sync.Mutex
	skip     map[string]bool
	recorded map[string][]throttler.Result
}

func newFakeThrottler(skip ...string) *fakeThrottler {
	f := &fakeThrottler{skip: map[string]bool{}, recorded: map[string][]throttler.Result{}}
	for _, id := range skip {
		f.skip[id] = true
	}
	return f
}

func (f *fakeThrottler) ShouldCall(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.skip[id]
}

func (f *fakeThrottler) Record(id string, r throttler.Result) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recorded[id] = append(f.recorded[id], r)
}

// fakeCaller answers per buyer. A buyer marked hang blocks until its context
// deadline passes and reports a timeout, the way the HTTP caller does.
type fakeCaller struct {
	results map[string]throttler.Result
	prices  map[string]float64
	hang    map[string]bool
}

func (f *fakeCaller) Call(ctx context.Context, id, _ string) (float64, throttler.Result) {
	if f.hang[id] {
		<-ctx.Done()
		return 0, throttler.ResultTimeout
	}
	return f.prices[id], f.results[id]
}

func TestRunSkippedBuyersAreNotCalledOrRecorded(t *testing.T) {
	th := newFakeThrottler("skipme")
	caller := &fakeCaller{
		results: map[string]throttler.Result{"ok": throttler.ResultOK, "skipme": throttler.ResultOK},
		prices:  map[string]float64{"ok": 2, "skipme": 99},
	}
	svc := NewService(th, caller, time.Second)

	resp := svc.Run(context.Background(), Request{AuctionID: "a1", Invited: []string{"ok", "skipme"}})

	assert.Equal(t, 1, resp.Calls)
	assert.Equal(t, 1, resp.Skipped)
	assert.Equal(t, "ok", resp.Winner.BuyerID, "a skipped buyer cannot win, since it was never asked")
	assert.Empty(t, th.recorded["skipme"], "skipped calls are not recorded")
	assert.Equal(t, []throttler.Result{throttler.ResultOK}, th.recorded["ok"])
}

func TestRunRecordsEachOutcomeOnce(t *testing.T) {
	th := newFakeThrottler()
	caller := &fakeCaller{
		results: map[string]throttler.Result{
			"good":   throttler.ResultOK,
			"broken": throttler.ResultFailed,
			"slow":   throttler.ResultTimeout,
		},
		prices: map[string]float64{"good": 3},
		hang:   map[string]bool{"slow": true},
	}
	svc := NewService(th, caller, 20*time.Millisecond)

	resp := svc.Run(context.Background(), Request{AuctionID: "a", Invited: []string{"good", "broken", "slow"}})

	assert.Equal(t, 3, resp.Calls)
	assert.Equal(t, 1, resp.Bids)
	assert.Equal(t, []throttler.Result{throttler.ResultOK}, th.recorded["good"])
	assert.Equal(t, []throttler.Result{throttler.ResultFailed}, th.recorded["broken"])
	assert.Equal(t, []throttler.Result{throttler.ResultTimeout}, th.recorded["slow"], "a hung call is recorded as a timeout")
}

func TestWinnerIsHighestSuccessfulBid(t *testing.T) {
	tests := []struct {
		name       string
		results    map[string]throttler.Result
		prices     map[string]float64
		wantWinner string
		wantBids   int
	}{
		{
			name:       "highest price wins",
			results:    map[string]throttler.Result{"a": throttler.ResultOK, "b": throttler.ResultOK, "c": throttler.ResultOK},
			prices:     map[string]float64{"a": 1, "b": 5, "c": 3},
			wantWinner: "b",
			wantBids:   3,
		},
		{
			name:       "a failed buyer with a high price cannot win",
			results:    map[string]throttler.Result{"a": throttler.ResultOK, "b": throttler.ResultFailed},
			prices:     map[string]float64{"a": 1, "b": 50},
			wantWinner: "a",
			wantBids:   1,
		},
		{
			name:       "ties go to the lowest buyer ID so the outcome is deterministic",
			results:    map[string]throttler.Result{"z": throttler.ResultOK, "m": throttler.ResultOK},
			prices:     map[string]float64{"z": 4, "m": 4},
			wantWinner: "m",
			wantBids:   2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ids := make([]string, 0, len(tc.results))
			for id := range tc.results {
				ids = append(ids, id)
			}
			svc := NewService(newFakeThrottler(), &fakeCaller{results: tc.results, prices: tc.prices}, time.Second)
			resp := svc.Run(context.Background(), Request{AuctionID: "a", Invited: ids})
			require.NotNil(t, resp.Winner)
			assert.Equal(t, tc.wantWinner, resp.Winner.BuyerID)
			assert.Equal(t, tc.wantBids, resp.Bids)
		})
	}
}

func TestNoSuccessfulBidMeansNoWinner(t *testing.T) {
	caller := &fakeCaller{results: map[string]throttler.Result{"a": throttler.ResultFailed}}
	svc := NewService(newFakeThrottler(), caller, time.Second)
	resp := svc.Run(context.Background(), Request{AuctionID: "a", Invited: []string{"a"}})
	assert.Nil(t, resp.Winner)
	assert.Equal(t, 0, resp.Bids)
}

func TestNoInvitedBuyers(t *testing.T) {
	svc := NewService(newFakeThrottler(), &fakeCaller{}, time.Second)
	resp := svc.Run(context.Background(), Request{AuctionID: "a"})
	assert.Equal(t, Response{AuctionID: "a"}, resp)
}

func TestSummaryReportsAndResets(t *testing.T) {
	th := newFakeThrottler("skipped")
	caller := &fakeCaller{
		results: map[string]throttler.Result{"b1": throttler.ResultOK, "b2": throttler.ResultFailed},
		prices:  map[string]float64{"b1": 1},
	}
	svc := NewService(th, caller, time.Second)
	svc.Run(context.Background(), Request{AuctionID: "1", Invited: []string{"b1", "b2", "skipped"}})
	svc.Run(context.Background(), Request{AuctionID: "2", Invited: []string{"b1"}})

	got := svc.Summary()
	assert.Equal(t, []BuyerSummary{
		{BuyerID: "b1", Calls: 2, OK: 2},
		{BuyerID: "b2", Calls: 1, Failed: 1},
		{BuyerID: "skipped", Skipped: 1},
	}, got)

	assert.Empty(t, svc.Summary(), "the counters are reset after each summary")
}

func TestConcurrentRunsAreSafe(t *testing.T) {
	caller := &fakeCaller{
		results: map[string]throttler.Result{"a": throttler.ResultOK, "b": throttler.ResultFailed},
		prices:  map[string]float64{"a": 2},
	}
	svc := NewService(newFakeThrottler("c"), caller, time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			svc.Run(context.Background(), Request{AuctionID: "x", Invited: []string{"a", "b", "c"}})
		}()
	}
	wg.Wait()
	total := int64(0)
	for _, s := range svc.Summary() {
		total += s.Calls + s.Skipped
	}
	assert.Equal(t, int64(150), total)
}
