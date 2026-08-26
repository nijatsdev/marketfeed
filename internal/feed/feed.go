// Package feed simulates CME-style price ticks for a set of configured futures symbols.
package feed

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nijatsdev/marketfeed/internal/metrics"
)

// ErrUnknownSymbol reports that a symbol is not configured. Other lookup errors are transient backend failures.
var ErrUnknownSymbol = errors.New("unknown symbol")

// Tick is a single price update for a futures symbol. Open, High, and Low are session stats persisted across failover.
type Tick struct {
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Bid       float64   `json:"bid"`
	Ask       float64   `json:"ask"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Timestamp time.Time `json:"timestamp"`
	Spec      Spec      `json:"spec"`
}

// Spec holds the exchange contract specification for a futures symbol.
type Spec struct {
	Exchange      string  `json:"exchange"`
	Description   string  `json:"description"`
	Multiplier    float64 `json:"multiplier"`
	TickSize      float64 `json:"tick_size"`
	TickValue     float64 `json:"tick_value"`
	InitialMargin float64 `json:"initial_margin"`
}

type symbolConfig struct {
	spec        Spec
	basePrice   float64
	volatility  float64 // fractional standard deviation of the price move per tick
	spreadTicks int     // half-spread in ticks, either side of mid

	// interval overrides the feed's global tick interval for this symbol; 0 means use the global one.
	interval time.Duration
}

// DefaultTickInterval is the cadence for symbols with no catalog value when the Feed is created with interval 0.
const DefaultTickInterval = time.Second

// seedMaxAge is the max age of a seed tick that still continues the series; older starts fresh at base price.
const seedMaxAge = 5 * time.Minute

// symState is the per-symbol price series state: current price, session open/high/low, and when the price was last set.
type symState struct {
	price, open, high, low float64
	ts                     time.Time
}

// Feed simulates CME-style price ticks for all configured symbols.
type Feed struct {
	mu             sync.RWMutex
	state          map[string]symState
	tickInterval   time.Duration // 0 = DefaultTickInterval; per-symbol overrides live in symbolConfig.interval
	volatilityMult float64
	readySymbols   atomic.Uint64 // distinct symbols that have emitted their first tick

	// subMu is separate from mu so fan-out to subscribers doesn't hold the price lock.
	subMu sync.RWMutex
	subs  []chan Tick
}

// New creates a Feed with every symbol at its base price. See NewSeeded for parameter semantics.
func New(tickInterval time.Duration, volatilityMult float64) *Feed {
	return NewSeeded(tickInterval, volatilityMult, nil)
}

// NewSeeded creates a Feed whose starting state comes from seed ticks; symbols absent, unknown, or non-positive
// start fresh at base price. tickInterval is the global cadence; volatilityMult <= 0 is reset to 1.0.
func NewSeeded(tickInterval time.Duration, volatilityMult float64, seed map[string]Tick) *Feed {
	if volatilityMult <= 0 {
		volatilityMult = 1.0
	}

	now := time.Now().UTC()

	state := make(map[string]symState, len(symbols))
	for sym, cfg := range symbols {
		state[sym] = symState{
			price: cfg.basePrice, open: cfg.basePrice, high: cfg.basePrice, low: cfg.basePrice,
			ts: now,
		}
	}

	for sym, t := range seed {
		if s, ok := seedState(sym, t); ok {
			state[sym] = s
		}
	}

	return &Feed{
		state:          state,
		tickInterval:   tickInterval,
		volatilityMult: volatilityMult,
	}
}

// seedState converts one seed tick into symbol state, reporting false when it's unknown, non-positive, or too old.
func seedState(sym string, t Tick) (symState, bool) {
	if _, ok := symbols[sym]; !ok || t.Price <= 0 {
		return symState{}, false
	}

	if t.Timestamp.IsZero() || time.Since(t.Timestamp) > seedMaxAge {
		return symState{}, false
	}

	s := symState{
		price: t.Price, open: t.Open, high: t.High, low: t.Low, ts: t.Timestamp,
	}

	// A seed missing session stats restarts the session at its price.
	if s.open <= 0 || s.high <= 0 || s.low <= 0 {
		s.open, s.high, s.low = t.Price, t.Price, t.Price
	}

	s.high = math.Max(s.high, s.price)
	s.low = math.Min(s.low, s.price)

	return s, true
}

// Subscribe returns a buffered channel of every tick; ticks are dropped for slow consumers, and it closes on shutdown.
func (f *Feed) Subscribe(_ context.Context) <-chan Tick {
	ch := make(chan Tick, len(symbols)*4)

	f.subMu.Lock()
	f.subs = append(f.subs, ch)
	f.subMu.Unlock()

	return ch
}

// Ready reports whether the feed has emitted at least one tick per symbol.
func (f *Feed) Ready() bool {
	return f.readySymbols.Load() >= uint64(len(symbols))
}

// Symbols returns all configured symbol names.
func Symbols() []string {
	out := make([]string, 0, len(symbols))
	for sym := range symbols {
		out = append(out, sym)
	}

	slices.Sort(out)

	return out
}

// SpecFor returns the contract spec for the given symbol, or false if unknown.
func SpecFor(symbol string) (Spec, bool) {
	cfg, ok := symbols[symbol]
	return cfg.spec, ok
}

// SymbolIntervalsMS returns per-symbol tick interval overrides in milliseconds; symbols on the global interval are omitted.
func SymbolIntervalsMS() map[string]int64 {
	out := make(map[string]int64)

	for sym, cfg := range symbols {
		if cfg.interval > 0 {
			out[sym] = cfg.interval.Milliseconds()
		}
	}

	return out
}

// Start begins the simulation. Subscriber channels close once ctx is cancelled and all ticker goroutines exit.
func (f *Feed) Start(ctx context.Context) {
	var wg sync.WaitGroup

	// run one ticker per symbol.
	for sym, cfg := range symbols {
		wg.Go(func() {
			f.runTicker(ctx, sym, cfg)
		})
	}

	// close subscriber channels once all tickers exit.
	go func() {
		wg.Wait()
		f.subMu.Lock()
		for _, sub := range f.subs {
			close(sub)
		}
		f.subMu.Unlock()
	}()
}

// PriceFor returns the current tick for a single symbol, or ErrUnknownSymbol.
func (f *Feed) PriceFor(_ context.Context, symbol string) (Tick, error) {
	cfg, ok := symbols[symbol]
	if !ok {
		return Tick{}, ErrUnknownSymbol
	}

	f.mu.RLock()
	s := f.state[symbol]
	f.mu.RUnlock()

	return makeTick(symbol, s, cfg), nil
}

// Snapshot returns the current price for all symbols at a single point in time.
func (f *Feed) Snapshot(_ context.Context) map[string]Tick {
	f.mu.RLock()
	defer f.mu.RUnlock()

	out := make(map[string]Tick, len(f.state))
	for sym, s := range f.state {
		out[sym] = makeTick(sym, s, symbols[sym])
	}

	return out
}

// effectiveInterval resolves a symbol's tick cadence: per-symbol override, then global interval, then default.
func (f *Feed) effectiveInterval(cfg symbolConfig) time.Duration {
	switch {
	case cfg.interval > 0:
		return cfg.interval
	case f.tickInterval > 0:
		return f.tickInterval
	default:
		return DefaultTickInterval
	}
}

// runTicker runs a single symbol's ticker until ctx is cancelled, sending ticks to all subscribers. The first tick
// increments readySymbols so the feed can report when all symbols have emitted at least once.
func (f *Feed) runTicker(ctx context.Context, sym string, cfg symbolConfig) {
	interval := f.effectiveInterval(cfg)

	// Resolve once to avoid per-tick hash+RLock inside WithLabelValues.
	ticksCounter, _ := metrics.TicksTotal.GetMetricWithLabelValues(sym)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	firstTick := true

	for {
		select {
		case <-ticker.C:
			tick := f.tick(sym, cfg)

			if firstTick {
				firstTick = false

				f.readySymbols.Add(1)
			}

			ticksCounter.Inc()

			f.subMu.RLock()

			for _, sub := range f.subs {
				select {
				case sub <- tick:
				default:
				}
			}

			f.subMu.RUnlock()
		case <-ctx.Done():
			return
		}
	}
}

// meanReversion scales the pull back toward base_price, keeping prices oscillating around it.
const meanReversion = 1.0

// tick moves one symbol's price by a single step: a random wander plus a pull
// back toward base_price, snapped to the exchange tick size.
func (f *Feed) tick(sym string, cfg symbolConfig) Tick {
	f.mu.Lock()
	s := f.state[sym]

	// How far the price has strayed from base, as a fraction: positive below
	// base (pull up), negative above (pull down), zero at base.
	drift := (cfg.basePrice - s.price) / cfg.basePrice
	vol := cfg.volatility * f.volatilityMult

	// The move: a random shock plus the pull home, both scaled by price so a
	// 1% move means the same at any price level.
	change := s.price * vol * (rand.NormFloat64() + drift*meanReversion) //nolint:gosec // math/rand/v2 is intentional for price simulation, not cryptographic use

	s.price = snapToTick(s.price+change, cfg.spec.TickSize) // land on the exchange tick grid
	s.high = math.Max(s.high, s.price)
	s.low = math.Min(s.low, s.price)

	s.ts = time.Now().UTC()
	f.state[sym] = s
	f.mu.Unlock()

	return makeTick(sym, s, cfg)
}

// makeTick builds a Tick from symbol state. The timestamp is when the price was last set, not when the tick was built.
func makeTick(sym string, s symState, cfg symbolConfig) Tick {
	tickSize := cfg.spec.TickSize

	half := float64(max(cfg.spreadTicks, 1)) * tickSize

	return Tick{
		Symbol:    sym,
		Price:     s.price,
		Bid:       snapToTick(s.price-half, tickSize),
		Ask:       snapToTick(s.price+half, tickSize),
		Open:      s.open,
		High:      s.high,
		Low:       s.low,
		Timestamp: s.ts,
		Spec:      cfg.spec,
	}
}

func snapToTick(price, tickSize float64) float64 {
	snapped := math.Round(price/tickSize) * tickSize
	return math.Round(snapped*1e8) / 1e8
}
