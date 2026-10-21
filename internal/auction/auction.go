// Package auction is the mock auction service that runs inside each pod. For
// one auction it asks the throttler about every invited buyer, calls the ones
// it may call in parallel with a per-call timeout, records each outcome, and
// picks the highest bid.
package auction

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Noy-Meir/buyer-throttling/internal/throttler"
)

// Throttler is the part of the pod-side throttler the auction needs.
type Throttler interface {
	ShouldCall(buyerID string) bool
	Record(buyerID string, result throttler.Result)
}

// Caller asks one buyer for a bid. It returns the price and the outcome. The
// price is meaningful only when the result is ResultOK. Implementations must
// return ResultTimeout when ctx's deadline passed.
type Caller interface {
	Call(ctx context.Context, buyerID, auctionID string) (float64, throttler.Result)
}

// Request is one auction from the simulator.
type Request struct {
	AuctionID string   `json:"auction_id"`
	Invited   []string `json:"invited"`
}

// Bid is a successful buyer response.
type Bid struct {
	BuyerID string  `json:"buyer_id"`
	Price   float64 `json:"price"`
}

// Response is the auction outcome. Winner is nil when no buyer bid.
type Response struct {
	AuctionID string `json:"auction_id"`
	Winner    *Bid   `json:"winner"`
	Bids      int    `json:"bids"`
	Calls     int    `json:"calls"`
	Skipped   int    `json:"skipped"`
}

// Service runs auctions. It is safe for concurrent use.
type Service struct {
	throttler Throttler
	caller    Caller
	timeout   time.Duration

	// counts holds cumulative per-buyer totals, read at scrape time by the
	// metrics collector. Only the first maxBuyers buyers seen are tracked, so
	// label cardinality stays bounded. Calls for other buyers still happen.
	maxBuyers int
	tracked   atomic.Int64
	untracked atomic.Int64
	counts    sync.Map // buyerID -> *buyerCounts

	// summaryMu guards last, the totals at the previous log summary. Summary is
	// not on the request path.
	summaryMu sync.Mutex
	last      map[string]BuyerTotals
}

type buyerCounts struct {
	skipped atomic.Int64
	ok      atomic.Int64
	failed  atomic.Int64
	timeout atomic.Int64
}

// NewService returns a Service. timeout is the per-call deadline and maxBuyers
// caps how many distinct buyers get their own counters.
func NewService(th Throttler, caller Caller, timeout time.Duration, maxBuyers int) *Service {
	return &Service{
		throttler: th,
		caller:    caller,
		timeout:   timeout,
		maxBuyers: maxBuyers,
		last:      map[string]BuyerTotals{},
	}
}

// outcome is one invited buyer's result, written by its goroutine into a slot
// owned by that goroutine, so no lock is needed to collect them.
type outcome struct {
	called bool
	price  float64
	result throttler.Result
}

// Run executes one auction. A skipped buyer is not called and not recorded, so
// the throttler's stats describe calls actually made.
func (s *Service) Run(ctx context.Context, req Request) Response {
	outcomes := make([]outcome, len(req.Invited))
	var wg sync.WaitGroup
	for i, buyerID := range req.Invited {
		if !s.throttler.ShouldCall(buyerID) {
			if c := s.countsFor(buyerID); c != nil {
				c.skipped.Add(1)
			}
			continue
		}
		wg.Add(1)
		go func(i int, buyerID string) {
			defer wg.Done()
			callCtx, cancel := context.WithTimeout(ctx, s.timeout)
			defer cancel()
			price, result := s.caller.Call(callCtx, buyerID, req.AuctionID)
			s.throttler.Record(buyerID, result)
			s.countResult(buyerID, result)
			outcomes[i] = outcome{called: true, price: price, result: result}
		}(i, buyerID)
	}
	wg.Wait()

	resp := Response{AuctionID: req.AuctionID}
	for i, o := range outcomes {
		if !o.called {
			resp.Skipped++
			continue
		}
		resp.Calls++
		if o.result != throttler.ResultOK {
			continue
		}
		resp.Bids++
		bid := Bid{BuyerID: req.Invited[i], Price: o.price}
		if resp.Winner == nil || bid.Price > resp.Winner.Price ||
			(bid.Price == resp.Winner.Price && bid.BuyerID < resp.Winner.BuyerID) {
			resp.Winner = &bid
		}
	}
	return resp
}

// countsFor returns the buyer's counters, creating them while under the cap.
// It returns nil for a buyer beyond the cap, which is counted as untracked.
func (s *Service) countsFor(buyerID string) *buyerCounts {
	if v, ok := s.counts.Load(buyerID); ok {
		return v.(*buyerCounts)
	}
	if s.tracked.Add(1) > int64(s.maxBuyers) {
		s.tracked.Add(-1)
		s.untracked.Add(1)
		return nil
	}
	v, loaded := s.counts.LoadOrStore(buyerID, &buyerCounts{})
	if loaded {
		s.tracked.Add(-1)
	}
	return v.(*buyerCounts)
}

func (s *Service) countResult(buyerID string, result throttler.Result) {
	c := s.countsFor(buyerID)
	if c == nil {
		return
	}
	switch result {
	case throttler.ResultOK:
		c.ok.Add(1)
	case throttler.ResultFailed:
		c.failed.Add(1)
	case throttler.ResultTimeout:
		c.timeout.Add(1)
	}
}

// BuyerTotals are one buyer's cumulative call outcomes since the process started.
type BuyerTotals struct {
	BuyerID string
	Calls   int64 // ok + failed + timeout
	Skipped int64
	OK      int64
	Failed  int64
	Timeout int64
}

// Totals returns cumulative per-buyer totals sorted by buyer ID, and the number
// of calls for buyers beyond the cap. Totals never reset, so they are valid
// Prometheus counters.
func (s *Service) Totals() ([]BuyerTotals, int64) {
	var out []BuyerTotals
	s.counts.Range(func(k, v any) bool {
		c := v.(*buyerCounts)
		t := BuyerTotals{
			BuyerID: k.(string),
			Skipped: c.skipped.Load(),
			OK:      c.ok.Load(),
			Failed:  c.failed.Load(),
			Timeout: c.timeout.Load(),
		}
		t.Calls = t.OK + t.Failed + t.Timeout
		out = append(out, t)
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].BuyerID < out[j].BuyerID })
	return out, s.untracked.Load()
}

// Summary returns each buyer's activity since the previous Summary call, sorted
// by buyer ID. Buyers with no new activity are omitted. Totals are unchanged.
func (s *Service) Summary() []BuyerSummary {
	s.summaryMu.Lock()
	defer s.summaryMu.Unlock()

	totals, _ := s.Totals()
	var out []BuyerSummary
	for _, t := range totals {
		prev := s.last[t.BuyerID]
		s.last[t.BuyerID] = t
		d := BuyerSummary{
			BuyerID: t.BuyerID,
			Calls:   t.Calls - prev.Calls,
			Skipped: t.Skipped - prev.Skipped,
			OK:      t.OK - prev.OK,
			Failed:  t.Failed - prev.Failed,
			Timeout: t.Timeout - prev.Timeout,
		}
		if d.Calls+d.Skipped > 0 {
			out = append(out, d)
		}
	}
	return out
}

// BuyerSummary is one buyer's activity since the previous summary.
type BuyerSummary struct {
	BuyerID string
	Calls   int64
	Skipped int64
	OK      int64
	Failed  int64
	Timeout int64
}
