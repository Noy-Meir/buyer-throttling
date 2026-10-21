package simulator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Noy-Meir/buyer-throttling/internal/clock"
)

type scriptedRandom struct {
	values []int
	i      int
}

func (r *scriptedRandom) IntN(n int) int {
	v := r.values[r.i]
	r.i++
	return v
}

func TestParseScenario(t *testing.T) {
	got, err := ParseScenario("3m:buyer_bad:failure_rate=0; 30s:buyer_bad:failure_rate=0.8,timeout_rate=0.1")
	require.NoError(t, err)
	assert.Equal(t, []Event{
		{At: 30 * time.Second, BuyerID: "buyer_bad", Patch: map[string]float64{"failure_rate": 0.8, "timeout_rate": 0.1}},
		{At: 3 * time.Minute, BuyerID: "buyer_bad", Patch: map[string]float64{"failure_rate": 0}},
	}, got, "events are sorted by offset")
}

func TestParseScenarioEmptyIsNoEvents(t *testing.T) {
	got, err := ParseScenario("  ")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestParseScenarioRejectsBadEvents(t *testing.T) {
	tests := map[string]string{
		"no buyer":       "30s:failure_rate=0.8",
		"bad offset":     "soon:b:failure_rate=0.8",
		"negative":       "-1s:b:failure_rate=0.8",
		"unknown key":    "30s:b:latency=3",
		"not a number":   "30s:b:failure_rate=high",
		"missing equals": "30s:b:failure_rate",
		"blank buyer":    "30s: :failure_rate=0.8",
	}
	for name, spec := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ParseScenario(spec)
			assert.Error(t, err)
		})
	}
}

func TestRunScenarioAppliesEventsAtTheirOffsets(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC))
	events, err := ParseScenario("30s:bad:failure_rate=0.8; 3m:bad:failure_rate=0")
	require.NoError(t, err)

	applied := make(chan Event, 2)
	done := make(chan struct{})
	go func() {
		RunScenario(context.Background(), clk, events, func(_ context.Context, ev Event) error {
			applied <- ev
			return nil
		}, quietLogger())
		close(done)
	}()

	clk.BlockUntil(1)
	clk.Advance(29 * time.Second)
	select {
	case ev := <-applied:
		t.Fatalf("event applied early: %+v", ev)
	default:
	}

	clk.Advance(time.Second)
	assert.Equal(t, 0.8, (<-applied).Patch["failure_rate"])

	clk.BlockUntil(1)
	clk.Advance(150 * time.Second)
	assert.Equal(t, 0.0, (<-applied).Patch["failure_rate"])
	<-done
}

func TestRunScenarioContinuesAfterApplyError(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	events, err := ParseScenario("1s:a:failure_rate=0.1; 2s:b:failure_rate=0.2")
	require.NoError(t, err)

	var mu sync.Mutex
	var seen []string
	done := make(chan struct{})
	go func() {
		RunScenario(context.Background(), clk, events, func(_ context.Context, ev Event) error {
			mu.Lock()
			seen = append(seen, ev.BuyerID)
			mu.Unlock()
			if ev.BuyerID == "a" {
				return errors.New("admin down")
			}
			return nil
		}, quietLogger())
		close(done)
	}()
	clk.BlockUntil(1)
	clk.Advance(time.Second)
	clk.BlockUntil(1)
	clk.Advance(time.Second)
	<-done
	assert.Equal(t, []string{"a", "b"}, seen)
}

func TestRunScenarioStopsOnCancel(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	events, err := ParseScenario("1h:a:failure_rate=0.1")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunScenario(ctx, clk, events, func(context.Context, Event) error {
			t.Error("cancelled scenario must not apply events")
			return nil
		}, quietLogger())
		close(done)
	}()
	clk.BlockUntil(1)
	cancel()
	<-done
}

func TestInviteFollowsParticipation(t *testing.T) {
	// Participation 0.05 is threshold 500. Draws below 500 invite.
	// Sorted IDs: a (participation 1), b (0.05), c (0).
	rnd := &scriptedRandom{values: []int{9999, 499, 500, 0}}
	got := Invite(map[string]float64{"a": 1, "b": 0.05, "c": 0}, rnd)
	assert.Equal(t, []string{"a", "b"}, got, "c never draws, so it is never invited")
}

func TestInviteIsSortedAndAllowsNobody(t *testing.T) {
	rnd := &scriptedRandom{values: []int{0, 0, 0}}
	got := Invite(map[string]float64{"z": 1, "m": 1, "a": 1}, rnd)
	assert.Equal(t, []string{"a", "m", "z"}, got)

	assert.Empty(t, Invite(map[string]float64{"x": 0}, &scriptedRandom{values: []int{0}}))
}

func TestRoundRobinCyclesThroughPods(t *testing.T) {
	rr := NewRoundRobin([]string{"pod-1", "pod-2", "pod-3"})
	var got []string
	for i := 0; i < 7; i++ {
		got = append(got, rr.Next())
	}
	assert.Equal(t, []string{"pod-1", "pod-2", "pod-3", "pod-1", "pod-2", "pod-3", "pod-1"}, got)
}

func TestRoundRobinConcurrentUseIsEven(t *testing.T) {
	rr := NewRoundRobin([]string{"a", "b"})
	counts := map[string]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u := rr.Next()
			mu.Lock()
			counts[u]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	assert.Equal(t, 50, counts["a"])
	assert.Equal(t, 50, counts["b"])
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
