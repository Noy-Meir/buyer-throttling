package throttler

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/store"
)

const benchBuyers = 1000

// benchThrottler returns a throttler with benchBuyers buyers, every third of
// them throttled, and a loaded snapshot. It uses the production clock and
// random source.
func benchThrottler(b *testing.B) (*Throttler, []string) {
	b.Helper()
	ids := make([]string, benchBuyers)
	states := make([]store.Snapshot, benchBuyers)
	for i := range ids {
		ids[i] = "buyer-" + strconv.Itoa(i)
		rate := 0
		if i%3 == 0 {
			rate = 9500
		}
		states[i] = snapFor(ids[i], rate)
	}
	cfg := testConfig()
	cfg.MaxBuyers = 2 * benchBuyers
	st := &fakeStore{states: states}
	th := New(cfg, st, clock.Real{}, SystemRandom{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	th.refresh(context.Background())
	// Pre-create counters so the benchmark measures the steady state, not first-seen buyers.
	for _, id := range ids {
		th.Record(id, ResultOK)
	}
	return th, ids
}

func BenchmarkShouldCall(b *testing.B) {
	th, ids := benchThrottler(b)
	var idx atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := idx.Add(1) * 7919
		for pb.Next() {
			th.ShouldCall(ids[i%benchBuyers])
			i++
		}
	})
}

func BenchmarkRecord(b *testing.B) {
	th, ids := benchThrottler(b)
	results := []Result{ResultOK, ResultFailed, ResultTimeout}
	var idx atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := idx.Add(1) * 7919
		for pb.Next() {
			th.Record(ids[i%benchBuyers], results[i%3])
			i++
		}
	})
}
