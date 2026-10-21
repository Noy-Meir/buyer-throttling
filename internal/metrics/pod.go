// Package metrics exposes the pod's and the evaluator's state as Prometheus
// metrics. Collectors read the existing atomic counters and in-memory state at
// scrape time. Nothing on the request path updates a labelled metric, so
// ShouldCall and Record never pay for metric labels.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Noy-Meir/buyer-throttling/internal/auction"
	"github.com/Noy-Meir/buyer-throttling/internal/throttler"
)

// PodSource is what the pod collector reads. The auction service and the
// throttler implement it.
type PodSource interface {
	Totals() ([]auction.BuyerTotals, int64)
	States() []throttler.BuyerState
	SnapshotAge() (time.Duration, bool)
	Stats() throttler.Stats
}

// PodCollector reports one pod's per-buyer traffic and enforcement, plus its
// internal counters.
type PodCollector struct {
	src PodSource

	calls, skipped, ok, failed, timeout *prometheus.Desc
	rate                                *prometheus.Desc
	snapshotAge                         *prometheus.Desc
	trackedBuyers                       *prometheus.Desc
	untrackedRecords                    *prometheus.Desc
	untrackedCalls                      *prometheus.Desc
	invalidResults                      *prometheus.Desc
	flushedCalls                        *prometheus.Desc
	flushFailures                       *prometheus.Desc
	refreshFailures                     *prometheus.Desc
}

// NewPodCollector returns a collector for src.
func NewPodCollector(src PodSource) *PodCollector {
	buyer := []string{"buyer_id"}
	return &PodCollector{
		src:              src,
		calls:            prometheus.NewDesc("throttle_pod_calls_total", "Buyer calls this pod made (ok, failed or timeout).", buyer, nil),
		skipped:          prometheus.NewDesc("throttle_pod_skipped_total", "Buyer calls this pod skipped because of the throttle.", buyer, nil),
		ok:               prometheus.NewDesc("throttle_pod_ok_total", "Buyer calls that returned a bid.", buyer, nil),
		failed:           prometheus.NewDesc("throttle_pod_failed_total", "Buyer calls that failed.", buyer, nil),
		timeout:          prometheus.NewDesc("throttle_pod_timeout_total", "Buyer calls that exceeded the call timeout.", buyer, nil),
		rate:             prometheus.NewDesc("throttle_pod_throttle_rate", "Throttle rate this pod enforces, 0 to 1.", buyer, nil),
		snapshotAge:      prometheus.NewDesc("throttle_pod_snapshot_age_seconds", "Seconds since the last successful state refresh.", nil, nil),
		trackedBuyers:    prometheus.NewDesc("throttle_pod_tracked_buyers", "Distinct buyers this pod tracks, bounded by MaxBuyers.", nil, nil),
		untrackedRecords: prometheus.NewDesc("throttle_pod_untracked_records_total", "Record calls dropped because the buyer cap was reached.", nil, nil),
		untrackedCalls:   prometheus.NewDesc("throttle_pod_untracked_buyer_calls_total", "Auction calls not broken out per buyer because of the cap.", nil, nil),
		invalidResults:   prometheus.NewDesc("throttle_pod_invalid_results_total", "Record calls with an unknown result or empty buyer ID.", nil, nil),
		flushedCalls:     prometheus.NewDesc("throttle_pod_flushed_calls_total", "Calls written to the store by the flusher.", nil, nil),
		flushFailures:    prometheus.NewDesc("throttle_pod_flush_failures_total", "Stats batches dropped after a store error.", nil, nil),
		refreshFailures:  prometheus.NewDesc("throttle_pod_refresh_failures_total", "State refreshes that failed; the last snapshot was kept.", nil, nil),
	}
}

// Describe sends every descriptor.
func (c *PodCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.calls, c.skipped, c.ok, c.failed, c.timeout, c.rate,
		c.snapshotAge, c.trackedBuyers, c.untrackedRecords, c.untrackedCalls, c.invalidResults,
		c.flushedCalls, c.flushFailures, c.refreshFailures} {
		ch <- d
	}
}

// Collect reads the current values. It runs once per scrape.
func (c *PodCollector) Collect(ch chan<- prometheus.Metric) {
	totals, untrackedCalls := c.src.Totals()
	for _, t := range totals {
		ch <- prometheus.MustNewConstMetric(c.calls, prometheus.CounterValue, float64(t.Calls), t.BuyerID)
		ch <- prometheus.MustNewConstMetric(c.skipped, prometheus.CounterValue, float64(t.Skipped), t.BuyerID)
		ch <- prometheus.MustNewConstMetric(c.ok, prometheus.CounterValue, float64(t.OK), t.BuyerID)
		ch <- prometheus.MustNewConstMetric(c.failed, prometheus.CounterValue, float64(t.Failed), t.BuyerID)
		ch <- prometheus.MustNewConstMetric(c.timeout, prometheus.CounterValue, float64(t.Timeout), t.BuyerID)
	}

	states := c.src.States()
	for _, s := range states {
		ch <- prometheus.MustNewConstMetric(c.rate, prometheus.GaugeValue, float64(s.RateBP)/10000, s.BuyerID)
	}

	if age, ok := c.src.SnapshotAge(); ok {
		ch <- prometheus.MustNewConstMetric(c.snapshotAge, prometheus.GaugeValue, age.Seconds())
	}

	st := c.src.Stats()
	ch <- prometheus.MustNewConstMetric(c.trackedBuyers, prometheus.GaugeValue, float64(st.TrackedBuyers))
	ch <- prometheus.MustNewConstMetric(c.untrackedRecords, prometheus.CounterValue, float64(st.Untracked))
	ch <- prometheus.MustNewConstMetric(c.untrackedCalls, prometheus.CounterValue, float64(untrackedCalls))
	ch <- prometheus.MustNewConstMetric(c.invalidResults, prometheus.CounterValue, float64(st.Invalid))
	ch <- prometheus.MustNewConstMetric(c.flushedCalls, prometheus.CounterValue, float64(st.FlushedCalls))
	ch <- prometheus.MustNewConstMetric(c.flushFailures, prometheus.CounterValue, float64(st.FlushFailures))
	ch <- prometheus.MustNewConstMetric(c.refreshFailures, prometheus.CounterValue, float64(st.RefreshFailures))
}
