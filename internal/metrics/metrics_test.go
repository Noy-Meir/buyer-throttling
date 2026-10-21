package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Noy-Meir/buyer-throttling/internal/auction"
	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
	"github.com/Noy-Meir/buyer-throttling/internal/evaluator/runner"
	"github.com/Noy-Meir/buyer-throttling/internal/throttler"
)

type fakePod struct {
	totals    []auction.BuyerTotals
	untracked int64
	states    []throttler.BuyerState
	age       time.Duration
	hasAge    bool
	stats     throttler.Stats
}

func (f *fakePod) Totals() ([]auction.BuyerTotals, int64) { return f.totals, f.untracked }
func (f *fakePod) States() []throttler.BuyerState         { return f.states }
func (f *fakePod) SnapshotAge() (time.Duration, bool)     { return f.age, f.hasAge }
func (f *fakePod) Stats() throttler.Stats                 { return f.stats }

type fakeEval struct {
	obs       []runner.Observation
	untracked int64
}

func (f *fakeEval) Snapshot() ([]runner.Observation, int64) { return f.obs, f.untracked }
func (f *fakeEval) HeartbeatFailures() int64                { return 3 }

// gather runs the collectors through a fresh registry, like a scrape would.
func gather(t *testing.T, cs ...prometheus.Collector) map[string]*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	for _, c := range cs {
		require.NoError(t, reg.Register(c))
	}
	mfs, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]*dto.MetricFamily{}
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

// value finds the sample of a metric with the given buyer_id label ("" for none).
func value(t *testing.T, fams map[string]*dto.MetricFamily, name, buyer string) float64 {
	t.Helper()
	mf, ok := fams[name]
	require.True(t, ok, "metric %s missing", name)
	for _, m := range mf.GetMetric() {
		if buyer == "" && len(m.GetLabel()) == 0 {
			return sampleValue(m)
		}
		for _, l := range m.GetLabel() {
			if l.GetName() == "buyer_id" && l.GetValue() == buyer {
				return sampleValue(m)
			}
		}
	}
	t.Fatalf("metric %s has no sample for buyer %q", name, buyer)
	return 0
}

func sampleValue(m *dto.Metric) float64 {
	if m.GetCounter() != nil {
		return m.GetCounter().GetValue()
	}
	return m.GetGauge().GetValue()
}

func TestPodCollectorReportsCountersPerBuyer(t *testing.T) {
	pod := &fakePod{
		totals: []auction.BuyerTotals{
			{BuyerID: "bad", Calls: 10, Skipped: 90, OK: 2, Failed: 7, Timeout: 1},
			{BuyerID: "good", Calls: 100, OK: 100},
		},
		untracked: 3,
		states: []throttler.BuyerState{
			{BuyerID: "bad", RateBP: 9500, Reason: evaluator.ReasonFailureRate},
			{BuyerID: "good"},
		},
		age:    4 * time.Second,
		hasAge: true,
		stats:  throttler.Stats{TrackedBuyers: 2, Untracked: 5, Invalid: 6, FlushedCalls: 700, FlushFailures: 8, RefreshFailures: 9},
	}
	fams := gather(t, NewPodCollector(pod))

	assert.Equal(t, 10.0, value(t, fams, "throttle_pod_calls_total", "bad"))
	assert.Equal(t, 90.0, value(t, fams, "throttle_pod_skipped_total", "bad"))
	assert.Equal(t, 2.0, value(t, fams, "throttle_pod_ok_total", "bad"))
	assert.Equal(t, 7.0, value(t, fams, "throttle_pod_failed_total", "bad"))
	assert.Equal(t, 1.0, value(t, fams, "throttle_pod_timeout_total", "bad"))
	assert.Equal(t, 100.0, value(t, fams, "throttle_pod_calls_total", "good"))

	assert.InDelta(t, 0.95, value(t, fams, "throttle_pod_throttle_rate", "bad"), 1e-9)
	assert.Equal(t, 0.0, value(t, fams, "throttle_pod_throttle_rate", "good"))
	assert.Equal(t, 4.0, value(t, fams, "throttle_pod_snapshot_age_seconds", ""))

	assert.Equal(t, 2.0, value(t, fams, "throttle_pod_tracked_buyers", ""))
	assert.Equal(t, 5.0, value(t, fams, "throttle_pod_untracked_records_total", ""))
	assert.Equal(t, 3.0, value(t, fams, "throttle_pod_untracked_buyer_calls_total", ""))
	assert.Equal(t, 6.0, value(t, fams, "throttle_pod_invalid_results_total", ""))
	assert.Equal(t, 700.0, value(t, fams, "throttle_pod_flushed_calls_total", ""))
	assert.Equal(t, 8.0, value(t, fams, "throttle_pod_flush_failures_total", ""))
	assert.Equal(t, 9.0, value(t, fams, "throttle_pod_refresh_failures_total", ""))
}

func TestPodCollectorOmitsSnapshotAgeBeforeFirstRefresh(t *testing.T) {
	fams := gather(t, NewPodCollector(&fakePod{}))
	_, ok := fams["throttle_pod_snapshot_age_seconds"]
	assert.False(t, ok, "no snapshot yet means no age, not a fake zero")
}

func TestEvaluatorCollectorReportsLastDecision(t *testing.T) {
	ev := &fakeEval{
		obs: []runner.Observation{{
			BuyerID:        "quiet",
			LastWindow:     42,
			RateBP:         9500,
			FailureRate:    0.8,
			TimeoutRate:    0.05,
			Evaluations:    7,
			Violations:     3,
			CarriedWindows: 4,
		}},
		untracked: 2,
	}
	fams := gather(t, NewEvaluatorCollector(ev))

	assert.InDelta(t, 0.95, value(t, fams, "throttle_evaluator_throttle_rate", "quiet"), 1e-9)
	assert.InDelta(t, 0.8, value(t, fams, "throttle_evaluator_failure_rate", "quiet"), 1e-9)
	assert.InDelta(t, 0.05, value(t, fams, "throttle_evaluator_timeout_rate", "quiet"), 1e-9)
	assert.Equal(t, 7.0, value(t, fams, "throttle_evaluator_evaluations_total", "quiet"))
	assert.Equal(t, 3.0, value(t, fams, "throttle_evaluator_violations_total", "quiet"))
	assert.Equal(t, 4.0, value(t, fams, "throttle_evaluator_carried_windows_total", "quiet"))
	assert.Equal(t, 2.0, value(t, fams, "throttle_evaluator_unobserved_evaluations_total", ""))
	assert.Equal(t, 3.0, value(t, fams, "throttle_evaluator_heartbeat_failures_total", ""))
}
