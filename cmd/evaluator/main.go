// Command evaluator runs the once-per-window throttle evaluation loop.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Noy-Meir/buyer-throttling/internal/api"
	"github.com/Noy-Meir/buyer-throttling/internal/app"
	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/config"
	"github.com/Noy-Meir/buyer-throttling/internal/evaluator/runner"
	"github.com/Noy-Meir/buyer-throttling/internal/metrics"
	"github.com/Noy-Meir/buyer-throttling/internal/store"
)

func main() {
	logger := app.NewLogger()
	if err := run(logger); err != nil {
		logger.Error("evaluator failed", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.FromEnv()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := app.NewRedis(app.Getenv("REDIS_ADDR", "localhost:6379"))
	defer client.Close()

	st := store.NewRedisStore(client, cfg.StatsTTL, cfg.BuyerStateTTL)
	obs := runner.NewObservations(cfg.MaxBuyers)
	r := runner.New(st, clock.Real{}, cfg.EvaluatorParams(), cfg.EvaluationWindow, cfg.Grace, logger)
	r.Observed = obs

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.NewEvaluatorCollector(obs))
	mux := http.NewServeMux()
	mux.Handle("GET /state", api.StateHandler(storeStates(st), logger))
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	// The HTTP server runs beside the evaluation loop. If it cannot start, the
	// evaluator stops rather than running without its API.
	serveCtx, serveCancel := context.WithCancel(ctx)
	defer serveCancel()
	serveErr := make(chan error, 1)
	go func() {
		err := app.Serve(serveCtx, app.Getenv("LISTEN_ADDR", ":8090"), mux, logger)
		serveCancel()
		serveErr <- err
	}()

	logger.Info("evaluator started",
		slog.Duration("window", cfg.EvaluationWindow),
		slog.Duration("grace", cfg.Grace),
		slog.Int("block_bp", cfg.BlockLevelBP),
		slog.Int64("min_sample", cfg.MinSample))

	err = r.Run(serveCtx)
	serveCancel()
	if sErr := <-serveErr; sErr != nil {
		return fmt.Errorf("http: %w", sErr)
	}
	if errors.Is(err, context.Canceled) {
		logger.Info("evaluator stopped")
		return nil
	}
	return err
}

// storeStates reads every buyer's state from the store. /state on the
// evaluator is the source of truth, so it reads Redis, unlike the pods.
func storeStates(st store.Store) api.Lister {
	return func(ctx context.Context) ([]api.Entry, error) {
		snaps, err := st.AllStates(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]api.Entry, 0, len(snaps))
		for _, s := range snaps {
			out = append(out, api.Entry{BuyerID: s.BuyerID, RateBP: s.State.ThrottleRateBP, Reason: string(s.State.Reason)})
		}
		return out, nil
	}
}
