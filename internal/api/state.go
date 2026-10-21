// Package api holds the HTTP handlers shared by the evaluator and the pods.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
)

// basisPoints converts a stored rate to the float the API reports.
const basisPoints = 10000

// Entry is one buyer's throttle state as reported by /state.
type Entry struct {
	BuyerID string
	RateBP  int    // throttle rate in basis points
	Reason  string // failure_rate, timeout_rate, recovering or ""
}

// Lister returns the buyers to report. An error makes /state answer 503.
type Lister func(ctx context.Context) ([]Entry, error)

type stateResponse struct {
	Buyers []stateBuyer `json:"buyers"`
}

type stateBuyer struct {
	BuyerID      string  `json:"buyer_id"`
	ThrottleRate float64 `json:"throttle_rate"`
	Reason       string  `json:"reason"`
}

// StateHandler serves GET /state in the assignment's format. Buyers are sorted
// by ID so the output is stable between calls.
func StateHandler(list Lister, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entries, err := list(r.Context())
		if err != nil {
			logger.Warn("state unavailable", slog.Any("err", err))
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "state unavailable"})
			return
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].BuyerID < entries[j].BuyerID })

		resp := stateResponse{Buyers: make([]stateBuyer, 0, len(entries))}
		for _, e := range entries {
			resp.Buyers = append(resp.Buyers, stateBuyer{
				BuyerID:      e.BuyerID,
				ThrottleRate: float64(e.RateBP) / basisPoints,
				Reason:       e.Reason,
			})
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
