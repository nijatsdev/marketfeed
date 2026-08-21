package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/nijatsdev/marketfeed/internal/feed"
)

// snapshotter is the read side the REST handlers need, satisfied by both
// feed.Feed and redis.Subscriber. dataSource (server.go) adds Subscribe.
type snapshotter interface {
	Snapshot(ctx context.Context) map[string]feed.Tick
	PriceFor(ctx context.Context, symbol string) (feed.Tick, error)
}

type symbolRow struct {
	Symbol string    `json:"symbol"`
	Spec   feed.Spec `json:"spec"`
}

type handler struct {
	feed       snapshotter
	symbolRows []symbolRow // built once; symbol catalog is static
}

func newHandler(f snapshotter) *handler {
	syms := feed.Symbols()
	rows := make([]symbolRow, 0, len(syms))

	for _, sym := range syms {
		spec, _ := feed.SpecFor(sym)
		rows = append(rows, symbolRow{Symbol: sym, Spec: spec})
	}

	return &handler{feed: f, symbolRows: rows}
}

func (h *handler) getPrices(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.feed.Snapshot(r.Context()))
}

func (h *handler) getPrice(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(chi.URLParam(r, "symbol"))

	tick, err := h.feed.PriceFor(r.Context(), symbol)

	switch {
	case errors.Is(err, feed.ErrUnknownSymbol):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown symbol: " + symbol})
		return
	case err != nil:
		// The backend failed; the symbol may well exist. Don't claim otherwise.
		slog.Warn("price lookup failed", "symbol", symbol, "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "price source unavailable"})

		return
	}

	writeJSON(w, http.StatusOK, tick)
}

func (h *handler) getSymbols(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.symbolRows)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		slog.Warn("json marshal failed", "err", err)
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if _, err = w.Write(b); err != nil {
		slog.Warn("json write failed", "err", err)
	}
}
