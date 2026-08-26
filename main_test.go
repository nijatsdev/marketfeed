package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nijatsdev/marketfeed/internal/feed"
)

// The configFromEnv tests use t.Setenv, which is incompatible with t.Parallel.

func TestConfigFromEnv_Defaults(t *testing.T) {
	// An empty value is treated as unset by envOr, exercising the defaults.
	t.Setenv("PORT", "")
	t.Setenv("TICK_INTERVAL_MS", "")
	t.Setenv("VOLATILITY_MULTIPLIER", "")
	t.Setenv("REDIS_URL", "")

	cfg, intervals, err := configFromEnv()
	require.NoError(t, err)
	assert.Equal(t, "8080", cfg.Port)
	assert.Equal(t, feed.DefaultTickInterval, cfg.TickInterval)
	assert.InDelta(t, 1.0, cfg.VolatilityMult, 1e-9)
	assert.Empty(t, cfg.RedisURL)
	assert.Zero(t, intervals.global, "unset leaves the catalog cadences in place")
	assert.Empty(t, intervals.perSymbol)
}

func TestConfigFromEnv_Overrides(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("TICK_INTERVAL_MS", "250")
	t.Setenv("VOLATILITY_MULTIPLIER", "2.5")
	t.Setenv("REDIS_URL", "redis://localhost:6379")

	cfg, intervals, err := configFromEnv()
	require.NoError(t, err)
	assert.Equal(t, "9090", cfg.Port)
	assert.Equal(t, 250*time.Millisecond, cfg.TickInterval)
	assert.Equal(t, 250*time.Millisecond, intervals.global)
	assert.InDelta(t, 2.5, cfg.VolatilityMult, 1e-9)
	assert.Equal(t, "redis://localhost:6379", cfg.RedisURL)
}

func TestConfigFromEnv_Invalid(t *testing.T) {
	cases := []struct {
		name, key, val string
	}{
		{"non-numeric tick", "TICK_INTERVAL_MS", "abc"},
		{"zero tick", "TICK_INTERVAL_MS", "0"},
		{"negative tick", "TICK_INTERVAL_MS", "-5"},
		{"non-numeric volatility", "VOLATILITY_MULTIPLIER", "x"},
		{"zero volatility", "VOLATILITY_MULTIPLIER", "0"},
		{"negative volatility", "VOLATILITY_MULTIPLIER", "-1.0"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TICK_INTERVAL_MS", "1000")
			t.Setenv("VOLATILITY_MULTIPLIER", "1.0")
			t.Setenv(tc.key, tc.val)

			_, _, err := configFromEnv()
			assert.Error(t, err)
		})
	}
}

func TestParseTickIntervals(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, raw string
		global    time.Duration
		perSymbol map[string]time.Duration
	}{
		{"bare number", "1000", time.Second, map[string]time.Duration{}},
		{"one symbol", "NQ:300", 0, map[string]time.Duration{"NQ": 300 * time.Millisecond}},
		{
			"bare plus symbols, spaces and lowercase", " 1000 , nq:300 ,ES:250", time.Second,
			map[string]time.Duration{"NQ": 300 * time.Millisecond, "ES": 250 * time.Millisecond},
		},
		{"empty items skipped", "1000,", time.Second, map[string]time.Duration{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseTickIntervals(tc.raw)
			require.NoError(t, err)
			assert.Equal(t, tc.global, got.global)
			assert.Equal(t, tc.perSymbol, got.perSymbol)
		})
	}
}

func TestParseTickIntervals_Invalid(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, raw string
	}{
		{"two bare values", "1000,500"},
		{"duplicate symbol", "NQ:300,nq:400"},
		{"missing symbol", ":300"},
		{"missing value", "NQ:"},
		{"non-numeric value", "NQ:fast"},
		{"below floor", "NQ:10"},
		{"bare below floor", "10"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseTickIntervals(tc.raw)
			assert.Error(t, err)
		})
	}
}

func TestApplyTickIntervals_UnknownSymbol(t *testing.T) {
	t.Parallel()

	err := applyTickIntervals(tickIntervals{perSymbol: map[string]time.Duration{"NOPE": time.Second}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NOPE")
}

// TestApplyTickIntervals_Precedence pins the bug this feature fixes: a bare value must reach symbols
// whose catalog entry already sets tick_interval_ms.
func TestApplyTickIntervals_Precedence(t *testing.T) { //nolint:paralleltest // mutates the catalog
	before := make(map[string]time.Duration)
	for sym, ms := range feed.SymbolIntervalsMS() {
		before[sym] = time.Duration(ms) * time.Millisecond
	}

	t.Cleanup(func() { assert.NoError(t, feed.SetIntervalOverrides(before)) })

	ti, err := parseTickIntervals("5000,NQ:300")
	require.NoError(t, err)
	require.NoError(t, applyTickIntervals(ti))

	got := feed.SymbolIntervalsMS()
	assert.Equal(t, int64(5000), got["ES"], "a bare value must override a catalog cadence")
	assert.Equal(t, int64(300), got["NQ"], "a symbol's own entry must win over the bare value")
}

func TestApplyCatalogFromEnv(t *testing.T) {
	t.Setenv(catalogFileEnv, "")
	require.NoError(t, applyCatalogFromEnv(), "unset means the built-in catalog")

	t.Setenv(catalogFileEnv, filepath.Join(t.TempDir(), "missing.yaml"))
	require.Error(t, applyCatalogFromEnv())
}
