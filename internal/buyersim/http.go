package buyersim

import (
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"
)

// Random draws the outcome of each bid. It must be safe for concurrent use.
type Random interface {
	IntN(n int) int
}

// SystemRandom uses the package-level generator, which is safe for concurrent use.
type SystemRandom struct{}

// IntN returns a value in [0, n).
func (SystemRandom) IntN(n int) int { return rand.IntN(n) }

// Server exposes the bid endpoint and the admin API.
//
//	POST /bid/{id}                 bid request from a pod
//	GET  /admin/buyers             list all buyers and their behaviour
//	POST /admin/buyers/{id}        partial update of a buyer's behaviour
//	GET  /healthz                  liveness
type Server struct {
	registry     *Registry
	random       Random
	timeoutDelay time.Duration // how long a "timeout" outcome sleeps before answering
	logger       *slog.Logger
}

// NewServer returns a Server. A timeout outcome sleeps for timeoutDelay, which
// must be longer than the pod's call timeout so the pod gives up first.
func NewServer(reg *Registry, rnd Random, timeoutDelay time.Duration, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{registry: reg, random: rnd, timeoutDelay: timeoutDelay, logger: logger}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /bid/{id}", s.bid)
	mux.HandleFunc("GET /admin/buyers", s.listBuyers)
	mux.HandleFunc("POST /admin/buyers/{id}", s.updateBuyer)
	return mux
}

type bidResponse struct {
	Price float64 `json:"price"`
}

// bid draws one outcome for this call: timeout, failure, or a bid. The draw is
// independent for each call, so the observed rates converge on the configured ones.
func (s *Server) bid(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	b, ok := s.registry.Get(id)
	if !ok {
		http.Error(w, "unknown buyer", http.StatusNotFound)
		return
	}

	draw := s.random.IntN(basisPoints)
	switch {
	case draw < b.timeoutBP():
		timer := time.NewTimer(s.timeoutDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
			// Answer late, as a slow buyer would. The pod has already given up.
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	case draw < b.timeoutBP()+b.failureBP():
		http.Error(w, "buyer error", http.StatusInternalServerError)
	default:
		// The price is drawn from the same source so it varies per bid.
		price := 1 + float64(s.random.IntN(900))/100
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(bidResponse{Price: price})
	}
}

func (s *Server) listBuyers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"buyers": s.registry.List()})
}

func (s *Server) updateBuyer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var p Patch
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<12))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	b, err := s.registry.Update(id, p)
	switch {
	case errors.Is(err, ErrUnknownBuyer):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case err != nil:
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.logger.Info("buyer behaviour changed", slog.String("buyer", id),
		slog.Float64("failure_rate", b.FailureRate),
		slog.Float64("timeout_rate", b.TimeoutRate),
		slog.Float64("participation", b.Participation))
	writeJSON(w, http.StatusOK, Entry{BuyerID: id, Behavior: b})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
