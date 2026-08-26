package feed

import (
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// catalogYAML is the symbol catalog; edit symbols.yaml to add or remove instruments.
//
//go:embed symbols.yaml
var catalogYAML []byte

// symbols is the active catalog. Set during startup, before any Feed exists, and immutable afterwards, so reads are lock-free.
var symbols map[string]symbolConfig

func init() {
	catalog, err := parseCatalog(catalogYAML)
	if err != nil {
		panic(fmt.Sprintf("feed: symbols.yaml is invalid: %v", err))
	}

	symbols = catalog
}

// LoadCatalogFile replaces the built-in catalog with the file at path (same schema as symbols.yaml).
// Call before any Feed exists; an invalid file leaves the current catalog in place.
func LoadCatalogFile(path string) error {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("read catalog: %w", err)
	}

	catalog, err := parseCatalog(raw)
	if err != nil {
		return fmt.Errorf("catalog %s: %w", path, err)
	}

	symbols = catalog

	return nil
}

// SetIntervalOverrides replaces the tick cadence of the given symbols, validating the whole batch before
// applying any. Call before any Feed exists; a rejected batch leaves the catalog untouched.
func SetIntervalOverrides(overrides map[string]time.Duration) error {
	for sym, d := range overrides {
		if _, ok := symbols[sym]; !ok {
			return fmt.Errorf("unknown symbol %q (catalog has %v)", sym, Symbols())
		}

		if d < MinTickIntervalMS*time.Millisecond {
			return fmt.Errorf("symbol %s: tick interval must be >= %dms, got %s", sym, MinTickIntervalMS, d)
		}
	}

	for sym, d := range overrides {
		cfg := symbols[sym]
		cfg.interval = d
		symbols[sym] = cfg
	}

	return nil
}

// symbolYAML is the file-facing shape of one catalog entry. Only tick_size and base_price are required.
type symbolYAML struct {
	Description   string  `yaml:"description"`
	Exchange      string  `yaml:"exchange"`
	TickSize      float64 `yaml:"tick_size"`
	Multiplier    float64 `yaml:"multiplier"`
	TickValue     float64 `yaml:"tick_value"`
	InitialMargin float64 `yaml:"initial_margin"`

	BasePrice   float64 `yaml:"base_price"`
	Volatility  float64 `yaml:"volatility"`
	SpreadTicks int     `yaml:"spread_ticks"`

	// TickIntervalMS overrides the global TICK_INTERVAL_MS for this symbol; 0 uses the global interval.
	TickIntervalMS int `yaml:"tick_interval_ms"`
}

// Defaults for optional catalog fields.
const (
	defaultVolatility  = 0.001
	defaultSpreadTicks = 1

	// MinTickIntervalMS floors every cadence, catalog or env; anything faster is a typo, not intent.
	MinTickIntervalMS = 50
)

// symbolNameRe bounds catalog keys, which become URL path segments, Redis channel names, and Prometheus labels.
var symbolNameRe = regexp.MustCompile(`^[A-Z0-9._-]{1,15}$`)

// parseCatalog decodes and validates a YAML catalog.
func parseCatalog(raw []byte) (map[string]symbolConfig, error) {
	var entries map[string]symbolYAML
	if err := yaml.Unmarshal(raw, &entries); err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, errors.New("no symbols defined")
	}

	catalog := make(map[string]symbolConfig, len(entries))

	for name, e := range entries {
		name = strings.ToUpper(strings.TrimSpace(name))

		cfg, err := e.toConfig(name)
		if err != nil {
			return nil, err
		}

		catalog[name] = cfg
	}

	return catalog, nil
}

// toConfig validates one entry and applies defaults.
func (e symbolYAML) toConfig(name string) (symbolConfig, error) {
	if !symbolNameRe.MatchString(name) {
		return symbolConfig{}, fmt.Errorf("symbol %q: name must match %s", name, symbolNameRe)
	}

	if e.TickSize <= 0 {
		return symbolConfig{}, fmt.Errorf("symbol %s: tick_size must be positive", name)
	}

	if e.BasePrice <= 0 {
		return symbolConfig{}, fmt.Errorf("symbol %s: base_price must be positive", name)
	}

	if e.Volatility < 0 || e.SpreadTicks < 0 {
		return symbolConfig{}, fmt.Errorf("symbol %s: volatility and spread_ticks must be >= 0", name)
	}

	if e.TickIntervalMS < 0 || (e.TickIntervalMS > 0 && e.TickIntervalMS < MinTickIntervalMS) {
		return symbolConfig{}, fmt.Errorf("symbol %s: tick_interval_ms must be 0 (global) or >= %dms", name, MinTickIntervalMS)
	}

	e.applyDefaults()

	return symbolConfig{
		spec: Spec{
			Exchange:      e.Exchange,
			Description:   e.Description,
			Multiplier:    e.Multiplier,
			TickSize:      e.TickSize,
			TickValue:     e.TickValue,
			InitialMargin: e.InitialMargin,
		},
		basePrice:   e.BasePrice,
		volatility:  e.Volatility,
		spreadTicks: e.SpreadTicks,
		interval:    time.Duration(e.TickIntervalMS) * time.Millisecond,
	}, nil
}

// applyDefaults fills the optional fields; tick_value derives from tick size and multiplier.
func (e *symbolYAML) applyDefaults() {
	if e.Volatility == 0 {
		e.Volatility = defaultVolatility
	}

	if e.SpreadTicks == 0 {
		e.SpreadTicks = defaultSpreadTicks
	}

	if e.Multiplier == 0 {
		e.Multiplier = 1
	}

	if e.TickValue == 0 {
		e.TickValue = e.TickSize * e.Multiplier
	}
}
