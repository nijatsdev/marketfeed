// Package hub fans out price ticks to WebSocket clients via melody.
package hub

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olahol/melody"

	"github.com/nijatsdev/marketfeed/internal/feed"
	"github.com/nijatsdev/marketfeed/internal/metrics"
)

// tickSource is the subset of feed.Feed that Hub needs.
type tickSource interface {
	Subscribe(ctx context.Context) <-chan feed.Tick
	Snapshot(ctx context.Context) map[string]feed.Tick
}

var _ tickSource = (*feed.Feed)(nil)

// sessionSymbols is the session key for a client's symbol filter; absent means all.
const sessionSymbols = "symbols"

// Hub fans out ticks from the feed to all connected WebSocket clients.
type Hub struct {
	m     *melody.Melody
	feed  tickSource
	ticks <-chan feed.Tick

	// clients counts sessions with their snapshot fully enqueued.
	clients atomic.Int64
	wg      sync.WaitGroup // tracks in-flight handlers for graceful drain
}

// New returns a Hub wired to the given tick source.
func New(ctx context.Context, f tickSource) *Hub {
	m := melody.New()
	m.Config.MessageBufferSize = 64      // per-client send buffer; overflow drops ticks
	m.Config.WriteWait = 5 * time.Second // write deadline
	m.Config.MaxMessageSize = 512        // clients only send close/ping frames
	m.Upgrader.CheckOrigin = func(*http.Request) bool {
		// Disabled by design: the feed is public, read-only, credential-free.
		return true
	}

	h := &Hub{m: m, feed: f, ticks: f.Subscribe(ctx)}

	m.HandleConnect(h.onConnect)
	m.HandleDisconnect(func(*melody.Session) {
		h.clients.Add(-1)
		metrics.WSClients.Dec()
	})
	m.HandleError(func(_ *melody.Session, err error) {
		if errors.Is(err, melody.ErrMessageBufferFull) {
			metrics.WSTicksDropped.Inc()
		}
	})

	return h
}

// Handler returns an http.Handler that upgrades connections and serves them
// until they disconnect or the hub shuts down.
func (h *Hub) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.wg.Add(1)
		defer h.wg.Done()

		// Blocks for the session lifetime (melody runs the read pump here).
		if err := h.m.HandleRequest(w, r); err != nil {
			slog.Warn("ws upgrade failed", "err", err)
		}
	})
}

// Wait blocks until all in-flight handlers have returned.
func (h *Hub) Wait() {
	h.wg.Wait()
}

// ClientCount returns the number of connected WebSocket clients.
func (h *Hub) ClientCount() int {
	return int(h.clients.Load())
}

// Run reads ticks and broadcasts to all clients. Call in a goroutine; returns
// when ctx is canceled or the feed closes, closing every session on the way out.
func (h *Hub) Run(ctx context.Context) {
	defer h.m.Close() //nolint:errcheck // closes all sessions; error not actionable on shutdown

	for {
		select {
		case tick, ok := <-h.ticks:
			if !ok {
				return
			}

			// No viewers, no work — the common state on followers.
			if h.clients.Load() == 0 {
				continue
			}

			data, err := json.Marshal(tick)
			if err != nil {
				continue
			}

			h.broadcast(tick.Symbol, data)
		case <-ctx.Done():
			return
		}
	}
}

func (h *Hub) broadcast(symbol string, data []byte) {
	// Errors only occur after Close; broadcasts are moot then.
	_ = h.m.BroadcastFilter(data, func(s *melody.Session) bool {
		return wantsSymbol(s, symbol)
	})
}

func wantsSymbol(s *melody.Session, symbol string) bool {
	v, ok := s.Get(sessionSymbols)
	if !ok {
		return true
	}

	filter, ok := v.(map[string]bool)

	return !ok || filter[symbol]
}

// onConnect stores the client's symbol filter and enqueues the snapshot before
// any live tick. Query param: ?symbols=ES,NQ (omit for all symbols).
func (h *Hub) onConnect(s *melody.Session) {
	if raw := s.Request.URL.Query().Get("symbols"); raw != "" {
		filter := make(map[string]bool)

		for sym := range strings.SplitSeq(raw, ",") {
			sym = strings.TrimSpace(strings.ToUpper(sym))
			if sym != "" {
				filter[sym] = true
			}
		}

		s.Set(sessionSymbols, filter)
	}

	for sym, tick := range h.feed.Snapshot(s.Request.Context()) {
		if !wantsSymbol(s, sym) {
			continue
		}

		if data, err := json.Marshal(tick); err == nil {
			_ = s.Write(data) // full buffer drops the tick, as designed
		}
	}

	h.clients.Add(1)
	metrics.WSClients.Inc()
}
