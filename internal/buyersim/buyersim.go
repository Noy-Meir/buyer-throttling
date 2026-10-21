// Package buyersim simulates external buyers for the demo. Each buyer answers
// bid requests with a configurable failure rate, timeout rate and
// participation, and its behaviour can be changed at runtime through an admin
// API. It is a test fixture, not part of the throttling service.
package buyersim

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// basisPoints is the scale used for drawing outcomes.
const basisPoints = 10000

// Behavior describes how one buyer answers. All fields are fractions in [0, 1].
// FailureRate and TimeoutRate are outcomes of a call the buyer receives.
// Participation is the share of auctions the buyer is invited to.
type Behavior struct {
	FailureRate   float64 `json:"failure_rate"`
	TimeoutRate   float64 `json:"timeout_rate"`
	Participation float64 `json:"participation"`
}

// Validate checks ranges and that failure and timeout do not exceed 1 together.
func (b Behavior) Validate() error {
	for name, v := range map[string]float64{
		"failure_rate":  b.FailureRate,
		"timeout_rate":  b.TimeoutRate,
		"participation": b.Participation,
	} {
		if math.IsNaN(v) || v < 0 || v > 1 {
			return fmt.Errorf("%s must be between 0 and 1, got %v", name, v)
		}
	}
	if b.FailureRate+b.TimeoutRate > 1 {
		return fmt.Errorf("failure_rate + timeout_rate must not exceed 1")
	}
	return nil
}

// failureBP and timeoutBP return the rates in basis points for integer draws.
func (b Behavior) failureBP() int { return int(math.Round(b.FailureRate * basisPoints)) }
func (b Behavior) timeoutBP() int { return int(math.Round(b.TimeoutRate * basisPoints)) }

// Patch is a partial update. Nil fields keep their current value.
type Patch struct {
	FailureRate   *float64 `json:"failure_rate,omitempty"`
	TimeoutRate   *float64 `json:"timeout_rate,omitempty"`
	Participation *float64 `json:"participation,omitempty"`
}

// ErrUnknownBuyer is returned for buyers the registry was not started with.
var ErrUnknownBuyer = errors.New("unknown buyer")

// Registry holds the current behaviour of every simulated buyer. It is read by
// bid handlers and written by the admin API, so it uses a RWMutex. That is fine
// here because it is the buyer side, not the pod's critical path.
type Registry struct {
	mu     sync.RWMutex
	buyers map[string]Behavior
}

// NewRegistry returns a registry with the given buyers.
func NewRegistry(initial map[string]Behavior) (*Registry, error) {
	r := &Registry{buyers: make(map[string]Behavior, len(initial))}
	for id, b := range initial {
		if err := b.Validate(); err != nil {
			return nil, fmt.Errorf("buyer %s: %w", id, err)
		}
		r.buyers[id] = b
	}
	return r, nil
}

// Get returns a buyer's behaviour.
func (r *Registry) Get(id string) (Behavior, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.buyers[id]
	return b, ok
}

// Entry pairs a buyer ID with its behaviour, for listings.
type Entry struct {
	BuyerID string `json:"buyer_id"`
	Behavior
}

// List returns every buyer sorted by ID.
func (r *Registry) List() []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Entry, 0, len(r.buyers))
	for id, b := range r.buyers {
		out = append(out, Entry{BuyerID: id, Behavior: b})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BuyerID < out[j].BuyerID })
	return out
}

// Update applies a partial patch and returns the new behaviour. The update is
// all or nothing: an invalid result leaves the buyer unchanged.
func (r *Registry) Update(id string, p Patch) (Behavior, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur, ok := r.buyers[id]
	if !ok {
		return Behavior{}, fmt.Errorf("%w: %s", ErrUnknownBuyer, id)
	}
	next := cur
	if p.FailureRate != nil {
		next.FailureRate = *p.FailureRate
	}
	if p.TimeoutRate != nil {
		next.TimeoutRate = *p.TimeoutRate
	}
	if p.Participation != nil {
		next.Participation = *p.Participation
	}
	if err := next.Validate(); err != nil {
		return Behavior{}, err
	}
	r.buyers[id] = next
	return next, nil
}

// ParseSpec parses the initial buyer list. The format is
//
//	buyer_good:failure=0,timeout=0,participation=1;buyer_bad:failure=0.8
//
// Missing keys default to 0 for failure and timeout, and to 1 for
// participation.
func ParseSpec(spec string) (map[string]Behavior, error) {
	out := map[string]Behavior{}
	for _, entry := range strings.Split(spec, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		id, fields, ok := strings.Cut(entry, ":")
		id = strings.TrimSpace(id)
		if !ok || id == "" {
			return nil, fmt.Errorf("buyer entry %q: want id:key=value,...", entry)
		}
		if _, dup := out[id]; dup {
			return nil, fmt.Errorf("buyer %s listed twice", id)
		}
		b := Behavior{Participation: 1}
		if strings.TrimSpace(fields) != "" {
			for _, kv := range strings.Split(fields, ",") {
				key, value, ok := strings.Cut(strings.TrimSpace(kv), "=")
				if !ok {
					return nil, fmt.Errorf("buyer %s: %q is not key=value", id, kv)
				}
				v, err := strconv.ParseFloat(value, 64)
				if err != nil {
					return nil, fmt.Errorf("buyer %s: %s: %w", id, key, err)
				}
				switch key {
				case "failure":
					b.FailureRate = v
				case "timeout":
					b.TimeoutRate = v
				case "participation":
					b.Participation = v
				default:
					return nil, fmt.Errorf("buyer %s: unknown key %q", id, key)
				}
			}
		}
		if err := b.Validate(); err != nil {
			return nil, fmt.Errorf("buyer %s: %w", id, err)
		}
		out[id] = b
	}
	if len(out) == 0 {
		return nil, errors.New("no buyers in spec")
	}
	return out, nil
}
