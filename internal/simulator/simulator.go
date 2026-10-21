// Package simulator generates auction traffic for the demo: it picks which
// buyers are invited to each auction, spreads auctions across pods, and runs a
// scripted scenario that changes buyer behaviour at fixed times.
package simulator

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Noy-Meir/buyer-throttling/internal/clock"
)

const basisPoints = 10000

// Random draws invitations. It must be safe for concurrent use.
type Random interface {
	IntN(n int) int
}

// Event changes one buyer's behaviour at offset At from the scenario start.
// Patch keys are the buyer admin fields: failure_rate, timeout_rate and
// participation. Only the keys given are changed.
type Event struct {
	At      time.Duration
	BuyerID string
	Patch   map[string]float64
}

var scenarioKeys = map[string]bool{"failure_rate": true, "timeout_rate": true, "participation": true}

// ParseScenario parses events separated by semicolons:
//
//	30s:buyer_bad:failure_rate=0.8,timeout_rate=0; 3m:buyer_bad:failure_rate=0
//
// Events are returned sorted by offset.
func ParseScenario(spec string) ([]Event, error) {
	var events []Event
	for _, raw := range strings.Split(spec, ";") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		parts := strings.SplitN(raw, ":", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("scenario event %q: want offset:buyer:key=value,...", raw)
		}
		at, err := time.ParseDuration(strings.TrimSpace(parts[0]))
		if err != nil || at < 0 {
			return nil, fmt.Errorf("scenario event %q: offset must be a non-negative duration", raw)
		}
		buyer := strings.TrimSpace(parts[1])
		if buyer == "" {
			return nil, fmt.Errorf("scenario event %q: buyer is required", raw)
		}
		patch := map[string]float64{}
		for _, kv := range strings.Split(parts[2], ",") {
			key, value, ok := strings.Cut(strings.TrimSpace(kv), "=")
			if !ok {
				return nil, fmt.Errorf("scenario event %q: %q is not key=value", raw, kv)
			}
			if !scenarioKeys[key] {
				return nil, fmt.Errorf("scenario event %q: unknown key %q", raw, key)
			}
			v, err := strconv.ParseFloat(value, 64)
			if err != nil || math.IsNaN(v) {
				return nil, fmt.Errorf("scenario event %q: %s must be a number", raw, key)
			}
			patch[key] = v
		}
		events = append(events, Event{At: at, BuyerID: buyer, Patch: patch})
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].At < events[j].At })
	return events, nil
}

// RunScenario applies each event when its offset from the start is reached,
// using the injected clock. It returns when all events have been applied or
// ctx is cancelled. An apply error is logged and the scenario continues.
func RunScenario(ctx context.Context, clk clock.Clock, events []Event, apply func(context.Context, Event) error, logger *slog.Logger) {
	start := clk.Now()
	for _, ev := range events {
		wait := ev.At - clk.Now().Sub(start)
		if wait > 0 {
			select {
			case <-ctx.Done():
				return
			case <-clk.After(wait):
			}
		}
		if err := apply(ctx, ev); err != nil {
			logger.Error("scenario event failed", slog.String("buyer", ev.BuyerID), slog.Any("err", err))
			continue
		}
		logger.Info("scenario event applied", slog.Duration("at", ev.At),
			slog.String("buyer", ev.BuyerID), slog.Any("patch", ev.Patch))
	}
}

// Invite returns the buyers invited to one auction. Each buyer is included
// with probability equal to its participation, in sorted order so a fixed
// draw sequence gives a fixed result.
func Invite(participation map[string]float64, rnd Random) []string {
	ids := make([]string, 0, len(participation))
	for id := range participation {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var invited []string
	for _, id := range ids {
		threshold := int(math.Round(participation[id] * basisPoints))
		if rnd.IntN(basisPoints) < threshold {
			invited = append(invited, id)
		}
	}
	return invited
}

// RoundRobin spreads requests evenly across pod URLs. It is safe for concurrent use.
type RoundRobin struct {
	urls []string
	next atomic.Uint64
}

// NewRoundRobin panics on an empty list, which the caller must have rejected at startup.
func NewRoundRobin(urls []string) *RoundRobin {
	if len(urls) == 0 {
		panic("simulator: no pod URLs")
	}
	return &RoundRobin{urls: urls}
}

// Next returns the next URL in turn.
func (r *RoundRobin) Next() string {
	n := r.next.Add(1) - 1
	return r.urls[n%uint64(len(r.urls))]
}
