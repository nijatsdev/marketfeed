package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests use t.Setenv, which is incompatible with t.Parallel.

func TestConfigFromEnv_Defaults(t *testing.T) {
	// An empty value is treated as unset by envOr, exercising the defaults.
	t.Setenv("PORT", "")
	t.Setenv("TICK_INTERVAL_MS", "")
	t.Setenv("VOLATILITY_MULTIPLIER", "")
	t.Setenv("REDIS_URL", "")

	cfg, err := configFromEnv()
	require.NoError(t, err)
	assert.Equal(t, "8080", cfg.Port)
	assert.Equal(t, time.Second, cfg.TickInterval)
	assert.InDelta(t, 1.0, cfg.VolatilityMult, 1e-9)
	assert.Empty(t, cfg.RedisURL)
}

func TestConfigFromEnv_Overrides(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("TICK_INTERVAL_MS", "250")
	t.Setenv("VOLATILITY_MULTIPLIER", "2.5")
	t.Setenv("REDIS_URL", "redis://localhost:6379")

	cfg, err := configFromEnv()
	require.NoError(t, err)
	assert.Equal(t, "9090", cfg.Port)
	assert.Equal(t, 250*time.Millisecond, cfg.TickInterval)
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

			_, err := configFromEnv()
			assert.Error(t, err)
		})
	}
}
