// Command evaluator runs the once-per-window throttle evaluation loop.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Noy-Meir/buyer-throttling/internal/app"
	"github.com/Noy-Meir/buyer-throttling/internal/clock"
	"github.com/Noy-Meir/buyer-throttling/internal/config"
	"github.com/Noy-Meir/buyer-throttling/internal/evaluator/runner"
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

	st := store.NewRedisStore(client, cfg.StatsTTL)
	r := runner.New(st, clock.Real{}, cfg.EvaluatorParams(), cfg.EvaluationWindow, cfg.Grace, logger)

	logger.Info("evaluator started",
		slog.Duration("window", cfg.EvaluationWindow),
		slog.Duration("grace", cfg.Grace),
		slog.Int("block_bp", cfg.BlockLevelBP),
		slog.Int64("min_sample", cfg.MinSample))

	err = r.Run(ctx)
	if errors.Is(err, context.Canceled) {
		logger.Info("evaluator stopped")
		return nil
	}
	return err
}
