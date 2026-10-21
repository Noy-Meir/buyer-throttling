package runner

import (
	"sort"
	"sync"

	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
)

// Observation is what the evaluator last did for one buyer. The metrics
// collector reads it at scrape time.
type Observation struct {
	BuyerID        string
	LastWindow     int64
	RateBP         int     // throttle rate after the last applied evaluation
	FailureRate    float64 // effective (carry-inclusive) failure rate of that window
	TimeoutRate    float64 // effective timeout rate of that window
	Evaluations    int64   // applied evaluations
	Violations     int64   // evaluations where a rate reached its threshold
	CarriedWindows int64   // evaluations that held a small window
}

// Observations keeps the per-buyer observations for metrics. It holds at most
// max buyers, so label cardinality stays bounded. Buyers beyond that are still
// evaluated but not observed, and the number of skipped observations is counted.
// It is written once per buyer per window, which is not a hot path.
type Observations struct {
	mu                sync.Mutex
	max               int
	buyers            map[string]*Observation
	untracked         int64
	heartbeatFailures int64
}

// RecordHeartbeatFailure counts a heartbeat write that Redis rejected.
func (o *Observations) RecordHeartbeatFailure() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.heartbeatFailures++
}

// HeartbeatFailures returns how many heartbeat writes failed.
func (o *Observations) HeartbeatFailures() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.heartbeatFailures
}

// NewObservations returns an empty set that tracks at most max buyers.
func NewObservations(max int) *Observations {
	return &Observations{max: max, buyers: map[string]*Observation{}}
}

// Record notes one applied decision for a buyer.
func (o *Observations) Record(buyerID string, window int64, d evaluator.Decision) {
	o.mu.Lock()
	defer o.mu.Unlock()

	obs, ok := o.buyers[buyerID]
	if !ok {
		if len(o.buyers) >= o.max {
			o.untracked++
			return
		}
		obs = &Observation{BuyerID: buyerID}
		o.buyers[buyerID] = obs
	}
	obs.LastWindow = window
	obs.RateBP = d.State.ThrottleRateBP
	obs.FailureRate = rate(d.Effective.Failed, d.Effective.Total)
	obs.TimeoutRate = rate(d.Effective.Timeout, d.Effective.Total)
	obs.Evaluations++
	if d.FailureRateViolated || d.TimeoutRateViolated {
		obs.Violations++
	}
	if d.State.CarriedWindows > 0 {
		obs.CarriedWindows++
	}
}

// Snapshot returns a copy of every observation sorted by buyer ID, plus the
// number of evaluations for buyers beyond the cap.
func (o *Observations) Snapshot() ([]Observation, int64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]Observation, 0, len(o.buyers))
	for _, obs := range o.buyers {
		out = append(out, *obs)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BuyerID < out[j].BuyerID })
	return out, o.untracked
}

// rate returns count/total, or 0 for an empty sample.
func rate(count, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(count) / float64(total)
}
