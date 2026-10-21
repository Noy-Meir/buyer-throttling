package runner

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
)

func TestObservationsRecordsLastDecisionAndCounts(t *testing.T) {
	o := NewObservations(10)
	o.Record("b", 5, evaluator.Decision{
		State:               evaluator.State{ThrottleRateBP: 9500, Reason: evaluator.ReasonFailureRate},
		FailureRateViolated: true,
		Effective:           evaluator.WindowStats{Total: 200, Failed: 140, Timeout: 20},
	})
	o.Record("b", 6, evaluator.Decision{
		State:     evaluator.State{CarriedWindows: 1},
		Effective: evaluator.WindowStats{Total: 40},
	})

	got, untracked := o.Snapshot()
	assert.Equal(t, int64(0), untracked)
	assert.Equal(t, []Observation{{
		BuyerID:        "b",
		LastWindow:     6,
		RateBP:         0,
		FailureRate:    0,
		TimeoutRate:    0,
		Evaluations:    2,
		Violations:     1,
		CarriedWindows: 1,
	}}, got)
}

func TestObservationRatesAreEffectiveFractions(t *testing.T) {
	o := NewObservations(10)
	o.Record("b", 1, evaluator.Decision{
		State:     evaluator.State{ThrottleRateBP: 9500, Reason: evaluator.ReasonTimeoutRate},
		Effective: evaluator.WindowStats{Total: 100, Failed: 10, Timeout: 35},
	})
	got, _ := o.Snapshot()
	assert.InDelta(t, 0.10, got[0].FailureRate, 1e-9)
	assert.InDelta(t, 0.35, got[0].TimeoutRate, 1e-9)
	assert.Equal(t, 9500, got[0].RateBP)
}

func TestObservationsCapsBuyersAndCountsTheRest(t *testing.T) {
	o := NewObservations(2)
	for _, id := range []string{"a", "b", "c", "a"} {
		o.Record(id, 1, evaluator.Decision{Effective: evaluator.WindowStats{Total: 100}})
	}
	got, untracked := o.Snapshot()
	assert.Equal(t, []string{"a", "b"}, []string{got[0].BuyerID, got[1].BuyerID})
	assert.Equal(t, int64(1), untracked, "c is evaluated but not observed")
	assert.Equal(t, int64(2), got[0].Evaluations, "a keeps counting after the cap is reached")
}

func TestObservationsSnapshotIsACopy(t *testing.T) {
	o := NewObservations(10)
	o.Record("b", 1, evaluator.Decision{Effective: evaluator.WindowStats{Total: 100}})
	snap, _ := o.Snapshot()
	snap[0].Evaluations = 999
	again, _ := o.Snapshot()
	assert.Equal(t, int64(1), again[0].Evaluations)
}

func TestTickFeedsObservations(t *testing.T) {
	st := newMemStore()
	st.setStats("bad", 4, evaluator.WindowStats{Total: 100, Failed: 80})
	var logs bytes.Buffer
	r := newRunner(st, clock.NewFake(time.Unix(0, 0)), &logs)
	r.Observed = NewObservations(10)

	if err := r.Tick(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	got, _ := r.Observed.Snapshot()
	assert.Equal(t, 9500, got[0].RateBP)
	assert.Equal(t, int64(4), got[0].LastWindow)
	assert.Equal(t, int64(1), got[0].Violations)
}
