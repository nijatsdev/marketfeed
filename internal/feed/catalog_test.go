package feed

import (
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// restoreCatalog snapshots the catalog and restores it after the test, so
// catalog-mutating tests don't leak into the rest of the package. It copies the
// map — a reference save would be mutated in place by symbols[sym] = cfg, so the
// restore would be a no-op.
func restoreCatalog(t *testing.T) {
	t.Helper()

	saved := maps.Clone(symbols)

	t.Cleanup(func() { symbols = saved })
}

func TestParseCatalog_MinimalEntryDefaults(t *testing.T) {
	t.Parallel()

	catalog, err := parseCatalog([]byte(`
BTC:
  description: Bitcoin Future
  exchange: CME
  tick_size: 5
  base_price: 65000
`))
	require.NoError(t, err)
	require.Len(t, catalog, 1)

	cfg := catalog["BTC"]
	assert.Equal(t, "CME", cfg.spec.Exchange)
	assert.InDelta(t, 5.0, cfg.spec.TickSize, 1e-9)
	assert.InDelta(t, 5.0, cfg.spec.TickValue, 1e-9, "tick_value defaults to tick_size × multiplier")
	assert.InDelta(t, defaultVolatility, cfg.volatility, 1e-9)
	assert.Equal(t, defaultSpreadTicks, cfg.spreadTicks)
}

func TestParseCatalog_LowercaseKeyNormalized(t *testing.T) {
	t.Parallel()

	catalog, err := parseCatalog([]byte("btc:\n  tick_size: 5\n  base_price: 65000\n"))
	require.NoError(t, err)
	assert.Contains(t, catalog, "BTC")
}

func TestParseCatalog_TickInterval(t *testing.T) {
	t.Parallel()

	catalog, err := parseCatalog([]byte("X:\n  tick_size: 1\n  base_price: 10\n  tick_interval_ms: 5000\n"))
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, catalog["X"].interval)

	// Omitted means the global interval (zero override).
	catalog, err = parseCatalog([]byte("Y:\n  tick_size: 1\n  base_price: 10\n"))
	require.NoError(t, err)
	assert.Zero(t, catalog["Y"].interval)
}

func TestParseCatalog_Invalid(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, body, wantErr string
	}{
		{"missing tick size", "X:\n  base_price: 10\n", "tick_size must be positive"},
		{"missing base price", "X:\n  tick_size: 1\n", "base_price must be positive"},
		{"bad symbol name", "\"A B\":\n  tick_size: 1\n  base_price: 10\n", "name must match"},
		{"negative tick interval", "X:\n  tick_size: 1\n  base_price: 10\n  tick_interval_ms: -5\n", "tick_interval_ms must be 0"},
		{"tick interval too small", "X:\n  tick_size: 1\n  base_price: 10\n  tick_interval_ms: 1\n", "tick_interval_ms must be 0"},
		{"empty file", "", "no symbols defined"},
		{"not yaml", "{{{{", "yaml"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseCatalog([]byte(tc.body))
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// TestEmbeddedCatalog pins symbols.yaml: it must parse (init would panic
// otherwise) and contain the twelve contracts with their key params.
func TestEmbeddedCatalog(t *testing.T) {
	t.Parallel()

	catalog, err := parseCatalog(catalogYAML)
	require.NoError(t, err)
	assert.Len(t, catalog, 12)

	es := catalog["ES"]
	assert.InDelta(t, 0.25, es.spec.TickSize, 1e-9)
	assert.Positive(t, es.basePrice, "ES carries a base price")
	assert.Equal(t, 250*time.Millisecond, es.interval, "ES sets a per-symbol tick cadence")

	zb := catalog["ZB"]
	assert.InDelta(t, 0.03125, zb.spec.TickSize, 1e-9, "fractional tick sizes survive YAML")
}
