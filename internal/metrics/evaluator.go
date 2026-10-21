package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Noy-Meir/buyer-throttling/internal/evaluator/runner"
)

// EvaluatorSource is what the evaluator collector reads.
type EvaluatorSource interface {
	Snapshot() ([]runner.Observation, int64)
}

// EvaluatorCollector reports what the evaluator last decided for each buyer.
type EvaluatorCollector struct {
	src EvaluatorSource

	rate, failureRate, timeoutRate   *prometheus.Desc
	evaluations, violations, carried *prometheus.Desc
	unobserved                       *prometheus.Desc
}

// NewEvaluatorCollector returns a collector for src.
func NewEvaluatorCollector(src EvaluatorSource) *EvaluatorCollector {
	buyer := []string{"buyer_id"}
	return &EvaluatorCollector{
		src:         src,
		rate:        prometheus.NewDesc("throttle_evaluator_throttle_rate", "Throttle rate after the last evaluation, 0 to 1.", buyer, nil),
		failureRate: prometheus.NewDesc("throttle_evaluator_failure_rate", "Failure rate of the last evaluated window, carry-inclusive.", buyer, nil),
		timeoutRate: prometheus.NewDesc("throttle_evaluator_timeout_rate", "Timeout rate of the last evaluated window, carry-inclusive.", buyer, nil),
		evaluations: prometheus.NewDesc("throttle_evaluator_evaluations_total", "Windows evaluated for this buyer.", buyer, nil),
		violations:  prometheus.NewDesc("throttle_evaluator_violations_total", "Evaluations where a rate reached its threshold.", buyer, nil),
		carried:     prometheus.NewDesc("throttle_evaluator_carried_windows_total", "Evaluations that held a window below the minimum sample.", buyer, nil),
		unobserved:  prometheus.NewDesc("throttle_evaluator_unobserved_evaluations_total", "Evaluations for buyers beyond MaxBuyers, not broken out per buyer.", nil, nil),
	}
}

// Describe sends every descriptor.
func (c *EvaluatorCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.rate, c.failureRate, c.timeoutRate, c.evaluations, c.violations, c.carried, c.unobserved} {
		ch <- d
	}
}

// Collect reads the observations. It runs once per scrape.
func (c *EvaluatorCollector) Collect(ch chan<- prometheus.Metric) {
	obs, unobserved := c.src.Snapshot()
	for _, o := range obs {
		ch <- prometheus.MustNewConstMetric(c.rate, prometheus.GaugeValue, float64(o.RateBP)/10000, o.BuyerID)
		ch <- prometheus.MustNewConstMetric(c.failureRate, prometheus.GaugeValue, o.FailureRate, o.BuyerID)
		ch <- prometheus.MustNewConstMetric(c.timeoutRate, prometheus.GaugeValue, o.TimeoutRate, o.BuyerID)
		ch <- prometheus.MustNewConstMetric(c.evaluations, prometheus.CounterValue, float64(o.Evaluations), o.BuyerID)
		ch <- prometheus.MustNewConstMetric(c.violations, prometheus.CounterValue, float64(o.Violations), o.BuyerID)
		ch <- prometheus.MustNewConstMetric(c.carried, prometheus.CounterValue, float64(o.CarriedWindows), o.BuyerID)
	}
	ch <- prometheus.MustNewConstMetric(c.unobserved, prometheus.CounterValue, float64(unobserved))
}
