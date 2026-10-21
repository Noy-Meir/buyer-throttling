package main

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Noy-Meir/buyer-throttling/internal/api"
	"github.com/Noy-Meir/buyer-throttling/internal/auction"
	"github.com/Noy-Meir/buyer-throttling/internal/metrics"
	"github.com/Noy-Meir/buyer-throttling/internal/throttler"
)

// podSource is the pod's view for metrics and /state: the auction's totals and
// the throttler's local state. Neither touches the store.
type podSource struct {
	*auction.Service
	*throttler.Throttler
}

// podHandler serves the auction routes plus /state and /metrics. /state and
// /metrics read only local memory, so they keep working when Redis is down.
func podHandler(svc *auction.Service, th *throttler.Throttler) http.Handler {
	src := podSource{svc, th}
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.NewPodCollector(src))

	list := func(context.Context) ([]api.Entry, error) {
		var out []api.Entry
		for _, s := range th.States() {
			out = append(out, api.Entry{BuyerID: s.BuyerID, RateBP: s.RateBP, Reason: string(s.Reason)})
		}
		return out, nil
	}

	mux := http.NewServeMux()
	mux.Handle("GET /state", api.StateHandler(list, nil))
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.Handle("/", svc.Handler())
	return mux
}
