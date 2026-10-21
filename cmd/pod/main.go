// Command pod runs a mock auction service with the throttler inside it.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Noy-Meir/buyer-throttling/internal/app"
	"github.com/Noy-Meir/buyer-throttling/internal/auction"
	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/config"
	"github.com/Noy-Meir/buyer-throttling/internal/store"
	"github.com/Noy-Meir/buyer-throttling/internal/throttler"
)

func main() {
	logger := app.NewLogger()
	if err := run(logger); err != nil {
		logger.Error("pod failed", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.FromEnv()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	callTimeout, err := app.EnvDuration("CALL_TIMEOUT", 150*time.Millisecond)
	if err != nil {
		return err
	}
	summaryEvery, err := app.EnvDuration("SUMMARY_INTERVAL", 5*time.Second)
	if err != nil {
		return err
	}
	podID := app.Getenv("POD_ID", "pod")
	buyersURL := app.Getenv("BUYERS_URL", "http://localhost:8081")
	logger = logger.With(slog.String("pod", podID))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisClient := app.NewRedis(app.Getenv("REDIS_ADDR", "localhost:6379"))
	defer redisClient.Close()

	th := throttler.New(cfg, store.NewRedisStore(redisClient, cfg.StatsTTL), clock.Real{}, throttler.SystemRandom{}, logger)
	th.Start(ctx) // loads the first snapshot before the server accepts traffic

	// Buyer calls share one transport. Idle connections are kept so each
	// auction does not pay a new TCP handshake per buyer.
	transport := &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 512,
		IdleConnTimeout:     90 * time.Second,
	}
	caller := auction.NewHTTPCaller(buyersURL, &http.Client{Transport: transport})
	svc := auction.NewService(th, caller, callTimeout, cfg.MaxBuyers)

	go summaryLoop(ctx, svc, th, logger, summaryEvery)

	logger.Info("pod started", slog.Duration("call_timeout", callTimeout), slog.String("buyers_url", buyersURL))
	serveErr := app.Serve(ctx, app.Getenv("LISTEN_ADDR", ":8080"), podHandler(svc, th), logger)

	// Stop after the server has drained, so the final flush includes every
	// auction that finished during shutdown.
	th.Stop()
	logger.Info("pod stopped")
	return serveErr
}

// summaryLoop logs per-buyer activity every interval. Counters reset after each
// report, so every line covers one interval.
func summaryLoop(ctx context.Context, svc *auction.Service, th *throttler.Throttler, logger *slog.Logger, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, s := range svc.Summary() {
			logger.Info("buyer summary",
				slog.String("buyer", s.BuyerID),
				slog.Int64("calls", s.Calls),
				slog.Int64("skipped", s.Skipped),
				slog.Int64("ok", s.OK),
				slog.Int64("failed", s.Failed),
				slog.Int64("timeout", s.Timeout))
		}
		st := th.Stats()
		logger.Info("throttler stats",
			slog.Int64("tracked_buyers", st.TrackedBuyers),
			slog.Int64("untracked", st.Untracked),
			slog.Int64("invalid", st.Invalid),
			slog.Int64("flushed_calls", st.FlushedCalls),
			slog.Int64("flush_failures", st.FlushFailures),
			slog.Int64("refresh_failures", st.RefreshFailures))
	}
}
