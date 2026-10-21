// Command buyers runs the simulated external buyers and their admin API.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Noy-Meir/buyer-throttling/internal/app"
	"github.com/Noy-Meir/buyer-throttling/internal/buyersim"
)

func main() {
	logger := app.NewLogger()
	if err := run(logger); err != nil {
		logger.Error("buyers failed", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	spec := os.Getenv("BUYERS")
	if spec == "" {
		return fmt.Errorf("BUYERS is required, e.g. buyer_good:failure=0;buyer_bad:failure=0")
	}
	initial, err := buyersim.ParseSpec(spec)
	if err != nil {
		return fmt.Errorf("BUYERS: %w", err)
	}
	reg, err := buyersim.NewRegistry(initial)
	if err != nil {
		return err
	}
	timeoutDelay, err := app.EnvDuration("TIMEOUT_DELAY", 300*time.Millisecond)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := buyersim.NewServer(reg, buyersim.SystemRandom{}, timeoutDelay, logger)
	logger.Info("buyers started", slog.Int("buyers", len(initial)), slog.Duration("timeout_delay", timeoutDelay))
	return app.Serve(ctx, app.Getenv("LISTEN_ADDR", ":8081"), srv.Handler(), logger)
}
