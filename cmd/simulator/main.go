// Command simulator sends auctions to the pods at a fixed rate and runs an
// optional scripted scenario against the buyers' admin API.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Noy-Meir/buyer-throttling/internal/app"
	"github.com/Noy-Meir/buyer-throttling/internal/auction"
	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/simulator"
)

func main() {
	logger := app.NewLogger()
	if err := run(logger); err != nil {
		logger.Error("simulator failed", slog.Any("err", err))
		os.Exit(1)
	}
}

// participation is the buyers' current invitation rate, refreshed from the
// buyers' admin API so changes made at runtime take effect here too.
type participation struct {
	mu    sync.RWMutex
	rates map[string]float64
}

func (p *participation) get() map[string]float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.rates
}

func run(logger *slog.Logger) error {
	pods := app.SplitList(os.Getenv("AUCTION_URLS"))
	if len(pods) == 0 {
		return fmt.Errorf("AUCTION_URLS is required, e.g. http://pod-1:8080,http://pod-2:8080")
	}
	buyersURL := strings.TrimRight(app.Getenv("BUYERS_URL", "http://localhost:8081"), "/")
	rate, err := app.EnvFloat("SIM_RATE", 100)
	if err != nil {
		return err
	}
	statsEvery, err := app.EnvDuration("STATS_INTERVAL", 10*time.Second)
	if err != nil {
		return err
	}
	scenarioSpec := os.Getenv("SCENARIO")
	events, err := simulator.ParseScenario(scenarioSpec)
	if err != nil {
		return fmt.Errorf("SCENARIO: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	httpClient := &http.Client{Timeout: 2 * time.Second}
	part := &participation{}
	if err := part.refreshUntilOK(ctx, httpClient, buyersURL, logger); err != nil {
		return err
	}
	go part.refreshLoop(ctx, httpClient, buyersURL, logger)

	if len(events) > 0 {
		logger.Info("scenario scheduled", slog.Int("events", len(events)))
		go simulator.RunScenario(ctx, clock.Real{}, events, func(ctx context.Context, ev simulator.Event) error {
			return patchBuyer(ctx, httpClient, buyersURL, ev)
		}, logger)
	}

	var sent, failed atomic.Int64
	go reportLoop(ctx, logger, statsEvery, &sent, &failed)

	rr := simulator.NewRoundRobin(pods)
	rnd := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1)) // single goroutine, so not shared
	interval := time.Duration(float64(time.Second) / rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	logger.Info("simulator started", slog.Float64("auctions_per_sec", rate), slog.Int("pods", len(pods)))

	var seq int64
	for {
		select {
		case <-ctx.Done():
			logger.Info("simulator stopped", slog.Int64("sent", sent.Load()), slog.Int64("errors", failed.Load()))
			return nil
		case <-ticker.C:
		}
		seq++
		req := auction.Request{
			AuctionID: fmt.Sprintf("auction-%d", seq),
			Invited:   simulator.Invite(part.get(), rnd),
		}
		podURL := rr.Next()
		go func() {
			sent.Add(1)
			if err := postAuction(ctx, httpClient, podURL, req); err != nil && ctx.Err() == nil {
				failed.Add(1)
			}
		}()
	}
}

func postAuction(ctx context.Context, c *http.Client, podURL string, req auction.Request) error {
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, podURL+"/auction", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("pod %s returned %d", podURL, resp.StatusCode)
	}
	return nil
}

// patchBuyer sends a scenario event to the buyers' admin API.
func patchBuyer(ctx context.Context, c *http.Client, buyersURL string, ev simulator.Event) error {
	body, _ := json.Marshal(ev.Patch)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, buyersURL+"/admin/buyers/"+ev.BuyerID, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("buyers admin returned %d", resp.StatusCode)
	}
	return nil
}

// fetchParticipation reads each buyer's participation from the admin API.
func fetchParticipation(ctx context.Context, c *http.Client, buyersURL string) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, buyersURL+"/admin/buyers", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("buyers admin returned %d", resp.StatusCode)
	}
	var out struct {
		Buyers []struct {
			BuyerID       string  `json:"buyer_id"`
			Participation float64 `json:"participation"`
		} `json:"buyers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	rates := make(map[string]float64, len(out.Buyers))
	for _, b := range out.Buyers {
		rates[b.BuyerID] = b.Participation
	}
	return rates, nil
}

// refreshUntilOK retries until the buyers are reachable, so the simulator can
// start before them.
func (p *participation) refreshUntilOK(ctx context.Context, c *http.Client, buyersURL string, logger *slog.Logger) error {
	for {
		rates, err := fetchParticipation(ctx, c, buyersURL)
		if err == nil {
			p.mu.Lock()
			p.rates = rates
			p.mu.Unlock()
			return nil
		}
		logger.Warn("waiting for buyers", slog.Any("err", err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (p *participation) refreshLoop(ctx context.Context, c *http.Client, buyersURL string, logger *slog.Logger) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		rates, err := fetchParticipation(ctx, c, buyersURL)
		if err != nil {
			logger.Warn("participation refresh failed; keeping last values", slog.Any("err", err))
			continue
		}
		p.mu.Lock()
		p.rates = rates
		p.mu.Unlock()
	}
}

func reportLoop(ctx context.Context, logger *slog.Logger, every time.Duration, sent, failed *atomic.Int64) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		logger.Info("simulator totals", slog.Int64("auctions_sent", sent.Load()), slog.Int64("auction_errors", failed.Load()))
	}
}
