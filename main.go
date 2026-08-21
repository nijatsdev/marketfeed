// Package main is the marketfeed price simulation service.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nijatsdev/marketfeed/internal/server"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := configFromEnv()
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	err = server.Run(ctx, cfg)

	stop()

	if err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}

	slog.Info("marketfeed stopped")
}

// configFromEnv parses and validates the environment into a server.Config.
func configFromEnv() (server.Config, error) {
	tickMS, err := strconv.Atoi(envOr("TICK_INTERVAL_MS", "1000"))
	if err != nil || tickMS <= 0 {
		return server.Config{}, fmt.Errorf("TICK_INTERVAL_MS must be a positive integer, got %q", os.Getenv("TICK_INTERVAL_MS"))
	}

	volMult, err := strconv.ParseFloat(envOr("VOLATILITY_MULTIPLIER", "1.0"), 64)
	if err != nil || volMult <= 0 {
		return server.Config{}, fmt.Errorf("VOLATILITY_MULTIPLIER must be a positive number, got %q", os.Getenv("VOLATILITY_MULTIPLIER"))
	}

	return server.Config{
		Port:           envOr("PORT", "8080"),
		TickInterval:   time.Duration(tickMS) * time.Millisecond,
		VolatilityMult: volMult,
		RedisURL:       os.Getenv("REDIS_URL"),
	}, nil
}

// envOr returns the environment value for key, or def if it is unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
