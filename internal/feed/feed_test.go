package feed

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// priceDelta is the tolerance used when comparing float64 prices.
// snapToTick uses math.Round which is exact for well-defined tick sizes,
// but floating-point representation requires a small epsilon for safety.
const priceDelta = 1e-9

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestSnapToTick(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		price    float64
		tickSize float64
		want     float64
	}{
		// Standard quarter-point ticks (ES, NQ)
		{"rounds down", 5250.12, 0.25, 5250.00},
		{"rounds up", 5250.13, 0.25, 5250.25},
		{"already on tick", 5250.25, 0.25, 5250.25},
		// ZB: 1/32 tick — non-power-of-10, exercises the float-noise rounding
		{"fractional tick", 119.48, 0.03125, 119.46875},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := snapToTick(tc.price, tc.tickSize)
			assert.InDelta(t, tc.want, got, priceDelta, "snapToTick(%v, %v)", tc.price, tc.tickSize)
		})
	}
}

func TestTickInvariants(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	f := New(10*time.Millisecond, 10.0) // high volatility to stress the walk
	ch := f.Subscribe(ctx)

	f.Start(ctx)

	for tick := range ch {
		_, ok := symbols[tick.Symbol]
		require.True(t, ok, "unknown symbol in tick: %s", tick.Symbol)

		assert.Positive(t, tick.Price, "symbol %s price must stay positive", tick.Symbol)
		assert.Less(t, tick.Bid, tick.Ask, "symbol %s bid >= ask", tick.Symbol)
	}
}

func TestFeedChannelClosedAfterCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	f := New(10*time.Millisecond, 1.0)
	ch := f.Subscribe(ctx)
	f.Start(ctx)

	// Drain a few ticks to confirm the feed is running.
	timeout := time.After(2 * time.Second)

	for range 3 {
		select {
		case _, ok := <-ch:
			require.True(t, ok, "channel closed before cancel")
		case <-timeout:
			t.Fatal("timed out waiting for ticks")
		}
	}

	cancel()

	closed := time.After(2 * time.Second)

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-closed:
			t.Fatal("feed channel was not closed after context cancellation")
		}
	}
}

func TestSymbols(t *testing.T) {
	t.Parallel()

	syms := Symbols()
	assert.NotEmpty(t, syms)

	for _, sym := range syms {
		_, ok := SpecFor(sym)
		assert.True(t, ok, "Symbols() returned %q but SpecFor returned false", sym)
	}

	known := map[string]bool{}
	for _, s := range syms {
		known[s] = true
	}

	for _, expected := range []string{"ES", "NQ", "CL", "GC"} {
		assert.True(t, known[expected], "expected %s in Symbols()", expected)
	}
}

func TestNewSeeded(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	f := NewSeeded(0, 1.0, map[string]Tick{
		"ES":      {Price: 5400.0, Open: 5300.0, High: 5450.0, Low: 5250.0, Timestamp: now}, // valid: must override the base state
		"GC":      {Price: 2400.0, Timestamp: now},                                          // no session stats: session restarts at the price
		"NQ":      {Price: -5, Timestamp: now},                                              // non-positive: must fall back to base
		"CL":      {Price: 90.0, Timestamp: now.Add(-time.Hour)},                            // stale: continuity is for failover, not old sessions
		"UNKNOWN": {Price: 1, Timestamp: now},                                               // not a configured symbol: must be ignored
	})

	snap := f.Snapshot(context.Background())

	assert.InDelta(t, 5400.0, snap["ES"].Price, priceDelta, "seeded price must be used")
	assert.InDelta(t, 5300.0, snap["ES"].Open, priceDelta, "seeded open must be used")
	assert.InDelta(t, 5450.0, snap["ES"].High, priceDelta, "seeded high must be used")
	assert.InDelta(t, 5250.0, snap["ES"].Low, priceDelta, "seeded low must be used")

	// A seed tick without session stats (published before they existed)
	// restarts the session at the seeded price.
	assert.InDelta(t, 2400.0, snap["GC"].Open, priceDelta, "missing open must restart at the seeded price")
	assert.InDelta(t, 2400.0, snap["GC"].High, priceDelta, "missing high must restart at the seeded price")
	assert.InDelta(t, 2400.0, snap["GC"].Low, priceDelta, "missing low must restart at the seeded price")

	assert.InDelta(t, symbols["NQ"].basePrice, snap["NQ"].Price, priceDelta,
		"non-positive seed must fall back to base price")

	_, ok := snap["UNKNOWN"]
	assert.False(t, ok, "unknown symbol must not appear in the feed")

	// A stale seed must be ignored: the symbol starts fresh at its base price.
	assert.InDelta(t, symbols["CL"].basePrice, snap["CL"].Price, priceDelta,
		"stale seed must fall back to the base price")

	// A symbol omitted from the seed map keeps its base price.
	assert.InDelta(t, symbols["YM"].basePrice, snap["YM"].Price, priceDelta,
		"omitted symbol must keep its base price")
	assert.InDelta(t, symbols["YM"].basePrice, snap["YM"].Open, priceDelta,
		"omitted symbol's session must open at its base price")
}

func TestPriceFor(t *testing.T) {
	t.Parallel()

	f := New(0, 1.0)

	tick, err := f.PriceFor(context.Background(), "ES")
	require.NoError(t, err)
	assert.Equal(t, "ES", tick.Symbol)
	assert.InDelta(t, symbols["ES"].basePrice, tick.Price, priceDelta, "fresh feed serves the base price")
	assert.Less(t, tick.Bid, tick.Ask)
	assert.InDelta(t, tick.Price, tick.Open, priceDelta, "session opens at the starting price")

	_, err = f.PriceFor(context.Background(), "NOPE")
	assert.ErrorIs(t, err, ErrUnknownSymbol)
}

// TestSpreads verifies bid/ask sit symmetrically around mid and that CL, which
// sets a wider spread in the catalog, quotes more ticks wide than ES.
func TestSpreads(t *testing.T) {
	t.Parallel()

	f := New(0, 1.0)

	// widthTicks reports a symbol's full bid/ask spread measured in ticks.
	widthTicks := func(sym string) float64 {
		tick, err := f.PriceFor(context.Background(), sym)
		require.NoError(t, err)
		assert.InDelta(t, tick.Ask-tick.Price, tick.Price-tick.Bid, priceDelta, "%s spread must be symmetric around mid", sym)

		return (tick.Ask - tick.Bid) / tick.Spec.TickSize
	}

	assert.Greater(t, widthTicks("CL"), widthTicks("ES"), "CL must quote more ticks wide than ES")
}

// clearIntervals zeroes every symbol's per-symbol tick interval so tests run
// on the feed's global interval, restoring the catalog on cleanup.
func clearIntervals(t *testing.T) {
	t.Helper()
	restoreCatalog(t)

	for sym, cfg := range symbols {
		cfg.interval = 0
		symbols[sym] = cfg
	}
}

func TestPerSymbolInterval(t *testing.T) { //nolint:paralleltest // mutates the package catalog
	// Start from a uniform (global-interval) catalog, then override only ES so
	// the global interval keeps every other symbol effectively frozen.
	clearIntervals(t)

	cfg := symbols["ES"]
	cfg.interval = 10 * time.Millisecond
	symbols["ES"] = cfg

	ctx := t.Context()

	f := New(time.Hour, 1.0)
	ch := f.Subscribe(ctx)

	f.Start(ctx)

	esTicks := 0
	deadline := time.After(500 * time.Millisecond)

collect:
	for {
		select {
		case tick, ok := <-ch:
			require.True(t, ok, "channel closed unexpectedly")
			require.Equal(t, "ES", tick.Symbol,
				"only ES has a fast override; no other symbol should have ticked")

			esTicks++
		case <-deadline:
			break collect
		}
	}

	assert.Greater(t, esTicks, 5, "the ES override interval must drive its tick cadence")
}

func TestReady(t *testing.T) {
	t.Parallel()

	f := New(10*time.Millisecond, 1.0)
	assert.False(t, f.Ready(), "feed should not be ready before Start")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	f.Start(ctx)

	waitReady(t, f, 5*time.Second)

	// readySymbols counts one per distinct symbol, not one per tick. After
	// Ready() flips, it must equal the number of configured symbols exactly —
	// never more — so further ticks don't inflate the count.
	assert.Equal(t, uint64(len(symbols)), f.readySymbols.Load(),
		"readySymbols must equal symbol count, not total tick count")
}

func TestNew_volatilityMultGuard(t *testing.T) {
	t.Parallel()

	// volatilityMult <= 0 must be silently reset to 1.0, not panic or produce
	// zero-width price changes that pin all symbols at their base price.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	f := New(10*time.Millisecond, -5.0)
	ch := f.Subscribe(ctx)

	f.Start(ctx)

	select {
	case tick, ok := <-ch:
		require.True(t, ok, "channel closed before first tick")
		assert.Positive(t, tick.Price, "non-positive price with guarded volatilityMult")
	case <-ctx.Done():
		t.Fatal("no tick received within deadline")
	}
}

// waitReady polls f.Ready() until true or the deadline is exceeded.
// Accepts testing.TB so it can be used from both tests and benchmarks.
func waitReady(tb testing.TB, f *Feed, timeout time.Duration) {
	tb.Helper()

	deadline := time.After(timeout)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for !f.Ready() {
		select {
		case <-deadline:
			tb.Fatal("feed did not become ready within deadline")
		case <-ticker.C:
		}
	}
}

// BenchmarkFeed_Snapshot measures concurrent snapshot reads under a live feed.
func BenchmarkFeed_Snapshot(b *testing.B) {
	f := New(10*time.Millisecond, 1.0)

	ctx := b.Context()

	f.Start(ctx)
	waitReady(b, f, 5*time.Second)
	b.ResetTimer()

	for b.Loop() {
		_ = f.Snapshot(ctx)
	}
}
