// Package main is the marketfeed price simulation service.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nijatsdev/marketfeed/internal/feed"
	"github.com/nijatsdev/marketfeed/internal/server"
)

const (
	// catalogFileEnv points at a YAML catalog that replaces the built-in symbols.yaml.
	catalogFileEnv = "SYMBOLS_FILE"

	// tickIntervalEnv sets cadences: a bare number applies to every symbol, SYM:ms entries to one.
	tickIntervalEnv = "TICK_INTERVAL_MS"
)

// tickIntervals is the parsed TICK_INTERVAL_MS value; global is 0 when no bare number was given.
type tickIntervals struct {
	global    time.Duration
	perSymbol map[string]time.Duration
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, intervals, err := configFromEnv()
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	// The catalog must be in place before cadence overrides are validated against it.
	if err := applyCatalogFromEnv(); err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	if err := applyTickIntervals(intervals); err != nil {
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

// configFromEnv parses and validates the environment into a server.Config and the tick cadence overrides.
func configFromEnv() (server.Config, tickIntervals, error) {
	intervals, err := parseTickIntervals(os.Getenv(tickIntervalEnv))
	if err != nil {
		return server.Config{}, tickIntervals{}, err
	}

	volMult, err := strconv.ParseFloat(envOr("VOLATILITY_MULTIPLIER", "1.0"), 64)
	if err != nil || volMult <= 0 {
		return server.Config{}, tickIntervals{}, fmt.Errorf("VOLATILITY_MULTIPLIER must be a positive number, got %q", os.Getenv("VOLATILITY_MULTIPLIER"))
	}

	// Symbols with no catalog cadence use the bare value, or the default when none was given.
	tick := intervals.global
	if tick == 0 {
		tick = feed.DefaultTickInterval
	}

	return server.Config{
		Port:           envOr("PORT", "8080"),
		TickInterval:   tick,
		VolatilityMult: volMult,
		RedisURL:       os.Getenv("REDIS_URL"),
	}, intervals, nil
}

// parseTickIntervals parses values like "1000", "NQ:300", or "1000,NQ:300". Order does not matter: a
// bare number sets every symbol, SYM:ms entries override single ones.
func parseTickIntervals(raw string) (tickIntervals, error) {
	ti := tickIntervals{perSymbol: map[string]time.Duration{}}

	for item := range strings.SplitSeq(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		key, val, perSymbol := strings.Cut(item, ":")
		if !perSymbol {
			val = key
		}

		ms, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil || ms <= 0 {
			return tickIntervals{}, fmt.Errorf("%s: %q must be a positive integer of milliseconds", tickIntervalEnv, item)
		}

		if ms < feed.MinTickIntervalMS {
			return tickIntervals{}, fmt.Errorf("%s: %q is below the %dms floor", tickIntervalEnv, item, feed.MinTickIntervalMS)
		}

		d := time.Duration(ms) * time.Millisecond

		if !perSymbol {
			if ti.global != 0 {
				return tickIntervals{}, fmt.Errorf("%s: more than one bare value", tickIntervalEnv)
			}

			ti.global = d

			continue
		}

		sym := strings.ToUpper(strings.TrimSpace(key))
		if sym == "" {
			return tickIntervals{}, fmt.Errorf("%s: %q has no symbol", tickIntervalEnv, item)
		}

		if _, dup := ti.perSymbol[sym]; dup {
			return tickIntervals{}, fmt.Errorf("%s: %s given twice", tickIntervalEnv, sym)
		}

		ti.perSymbol[sym] = d
	}

	return ti, nil
}

// applyCatalogFromEnv replaces the built-in catalog with the file named by SYMBOLS_FILE, if set.
func applyCatalogFromEnv() error {
	path := os.Getenv(catalogFileEnv)
	if path == "" {
		return nil
	}

	if err := feed.LoadCatalogFile(path); err != nil {
		return err
	}

	slog.Info("symbol catalog loaded", "path", path, "symbols", len(feed.Symbols()))

	return nil
}

// applyTickIntervals writes the cadences into the catalog: the bare value to every symbol, then the
// per-symbol entries on top, so a symbol's own entry always wins.
func applyTickIntervals(ti tickIntervals) error {
	overrides := make(map[string]time.Duration, len(ti.perSymbol))

	if ti.global > 0 {
		for _, sym := range feed.Symbols() {
			overrides[sym] = ti.global
		}
	}

	maps.Copy(overrides, ti.perSymbol)

	return feed.SetIntervalOverrides(overrides)
}

// envOr returns the environment value for key, or def if it is unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
