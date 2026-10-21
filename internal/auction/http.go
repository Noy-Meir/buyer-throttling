package auction

import (
	"encoding/json"
	"net/http"
)

// Handler returns the pod's HTTP routes.
//
//	POST /auction   run one auction
//	GET  /healthz   liveness
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /auction", func(w http.ResponseWriter, r *http.Request) {
		var req Request
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.AuctionID == "" {
			http.Error(w, "auction_id is required", http.StatusBadRequest)
			return
		}
		resp := s.Run(r.Context(), req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	return mux
}
