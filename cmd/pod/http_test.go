package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Noy-Meir/buyer-throttling/internal/auction"
	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/config"
	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
	"github.com/Noy-Meir/buyer-throttling/internal/store"
	"github.com/Noy-Meir/buyer-throttling/internal/throttler"
)

// flakyStore serves one snapshot until down is set, then fails every call. It
// counts calls so the test can prove the HTTP handler never reaches it.
type flakyStore struct {
	down  atomic.Bool
	calls atomic.Int64
	state []store.Snapshot
}

func (f *flakyStore) call() error {
	f.calls.Add(1)
	if f.down.Load() {
		return errors.New("redis down")
	}
	return nil
}

func (f *flakyStore) IncrStats(context.Context, []store.Increment) error { return f.call() }
func (f *flakyStore) AllStates(context.Context) ([]store.Snapshot, error) {
	if err := f.call(); err != nil {
		return nil, err
	}
	return f.state, nil
}
func (f *flakyStore) WindowStats(context.Context, string, int64) (evaluator.WindowStats, error) {
	return evaluator.WindowStats{}, f.call()
}
func (f *flakyStore) Buyers(context.Context) ([]string, error) { return nil, f.call() }
func (f *flakyStore) State(context.Context, string) (store.Snapshot, error) {
	return store.Snapshot{}, f.call()
}
func (f *flakyStore) CompareAndSetState(context.Context, string, int64, evaluator.State) (bool, error) {
	return false, f.call()
}

type noopCaller struct{}

func (noopCaller) Call(context.Context, string, string) (float64, throttler.Result) {
	return 0, throttler.ResultOK
}

// startedPod returns a pod handler whose snapshot already holds one blocked buyer.
func startedPod(t *testing.T) (http.Handler, *flakyStore) {
	t.Helper()
	st := &flakyStore{state: []store.Snapshot{{
		BuyerID: "buyer_bad",
		State:   evaluator.State{ThrottleRateBP: 9500, Reason: evaluator.ReasonFailureRate},
	}}}
	cfg := config.Defaults()
	cfg.MaxBuyers = 100
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	th := throttler.New(cfg, st, clock.Real{}, throttler.SystemRandom{}, logger)
	svc := auction.NewService(th, noopCaller{}, cfg.Grace, cfg.MaxBuyers)

	ctx, cancel := context.WithCancel(context.Background())
	th.Start(ctx) // loads the snapshot synchronously, then the loops start
	cancel()
	th.Stop() // waits for the loops; the final flush also hits the store

	return podHandler(svc, th), st
}

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	body, _ := io.ReadAll(rec.Body)
	return rec.Code, string(body)
}

func TestPodStateServedFromLocalSnapshot(t *testing.T) {
	h, st := startedPod(t)
	st.down.Store(true) // Redis is gone; the pod must not notice on the request path
	before := st.calls.Load()

	code, body := get(t, h, "/state")
	assert.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, `{"buyers":[{"buyer_id":"buyer_bad","throttle_rate":0.95,"reason":"failure_rate"}]}`, body)
	assert.Equal(t, before, st.calls.Load(), "/state must not call the store")
}

func TestPodStateIsSortedAndJSON(t *testing.T) {
	h, _ := startedPod(t)
	code, body := get(t, h, "/state")
	require.Equal(t, http.StatusOK, code)
	var out struct {
		Buyers []map[string]any `json:"buyers"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	require.Len(t, out.Buyers, 1)
	assert.Equal(t, "buyer_bad", out.Buyers[0]["buyer_id"])
}

func TestPodMetricsExposeEnforcedRate(t *testing.T) {
	h, st := startedPod(t)
	st.down.Store(true)

	code, body := get(t, h, "/metrics")
	assert.Equal(t, http.StatusOK, code)
	assert.True(t, strings.Contains(body, `throttle_pod_throttle_rate{buyer_id="buyer_bad"} 0.95`), "rate gauge missing:\n%s", body)
	assert.Contains(t, body, "throttle_pod_snapshot_age_seconds")
}

func TestPodAuctionRoutesStillServed(t *testing.T) {
	h, _ := startedPod(t)
	code, _ := get(t, h, "/healthz")
	assert.Equal(t, http.StatusOK, code)
}
