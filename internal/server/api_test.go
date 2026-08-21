package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-chi/chi/v5"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nijatsdev/marketfeed/internal/feed"
	"github.com/nijatsdev/redlease"
)

// newElector returns a redlease elector backed by a client to mr's address.
func newElector(t *testing.T, addr string) *redlease.Elector {
	t.Helper()

	rc := goredis.NewClient(&goredis.Options{Addr: addr})

	t.Cleanup(func() { _ = rc.Close() })

	e, err := redlease.New(rc, redlease.Config{Name: leaderName})
	require.NoError(t, err)

	return e
}

type fakeSnap struct {
	data map[string]feed.Tick
}

func (f *fakeSnap) Snapshot(_ context.Context) map[string]feed.Tick { return f.data }

func (f *fakeSnap) PriceFor(_ context.Context, symbol string) (feed.Tick, error) {
	tick, ok := f.data[symbol]
	if !ok {
		return feed.Tick{}, feed.ErrUnknownSymbol
	}

	return tick, nil
}

func newFakeSnap(symbols ...string) *fakeSnap {
	data := make(map[string]feed.Tick, len(symbols))
	for _, s := range symbols {
		data[s] = feed.Tick{
			Symbol:    s,
			Price:     100.0,
			Bid:       99.75,
			Ask:       100.25,
			Timestamp: time.Now().UTC(),
		}
	}

	return &fakeSnap{data: data}
}

func newAPIMux(f *fakeSnap) *httptest.Server {
	h := newHandler(f)
	r := chi.NewRouter()
	r.Get("/prices", h.getPrices)
	r.Get("/prices/{symbol}", h.getPrice)
	r.Get("/symbols", h.getSymbols)

	return httptest.NewServer(r)
}

func doRequest(t *testing.T, srv *httptest.Server, method, path string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+path, nil)
	require.NoError(t, err)

	resp, err := srv.Client().Do(req)
	require.NoError(t, err)

	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

func TestGetPrices(t *testing.T) {
	t.Parallel()

	srv := newAPIMux(newFakeSnap("ES", "NQ"))
	defer srv.Close()

	resp := doRequest(t, srv, http.MethodGet, "/prices") //nolint:bodyclose // closed via t.Cleanup inside doRequest

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]feed.Tick
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Contains(t, result, "ES")
	assert.Contains(t, result, "NQ")
}

func TestGetPrice_found(t *testing.T) {
	t.Parallel()

	srv := newAPIMux(newFakeSnap("ES"))
	defer srv.Close()

	// Lowercase in the path also proves the handler upper-cases the symbol.
	resp := doRequest(t, srv, http.MethodGet, "/prices/es") //nolint:bodyclose // closed via t.Cleanup inside doRequest

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var tick feed.Tick
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&tick))
	assert.Equal(t, "ES", tick.Symbol)
}

func TestGetPrice_notFound(t *testing.T) {
	t.Parallel()

	srv := newAPIMux(newFakeSnap("ES"))
	defer srv.Close()

	resp := doRequest(t, srv, http.MethodGet, "/prices/ZZ") //nolint:bodyclose // closed via t.Cleanup inside doRequest

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// errSnap simulates a failing backend: the symbol may exist, the lookup failed.
type errSnap struct{ fakeSnap }

func (e *errSnap) PriceFor(_ context.Context, _ string) (feed.Tick, error) {
	return feed.Tick{}, errors.New("redis: connection refused")
}

func TestGetPrice_backendUnavailable(t *testing.T) {
	t.Parallel()

	h := newHandler(&errSnap{})
	r := chi.NewRouter()
	r.Get("/prices/{symbol}", h.getPrice)

	srv := httptest.NewServer(r)
	defer srv.Close()

	resp := doRequest(t, srv, http.MethodGet, "/prices/ES") //nolint:bodyclose // closed via t.Cleanup inside doRequest

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"a backend failure must not report the symbol as unknown")
}

func TestGetSymbols(t *testing.T) {
	t.Parallel()

	srv := newAPIMux(newFakeSnap())
	defer srv.Close()

	resp := doRequest(t, srv, http.MethodGet, "/symbols") //nolint:bodyclose // closed via t.Cleanup inside doRequest

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var rows []symbolRow
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&rows))
	assert.NotEmpty(t, rows)

	for _, r := range rows {
		assert.NotEmpty(t, r.Symbol)
		assert.NotEmpty(t, r.Spec.Exchange, "symbol %s has empty exchange", r.Symbol)
	}
}

// ── pollReady ─────────────────────────────────────────────────────────────────

func TestPollReady_BecomesReadyWhenFull(t *testing.T) {
	t.Parallel()

	// Pre-populate a full snapshot so pollReady flips ready on its first poll.
	syms := feed.Symbols()
	data := make(map[string]feed.Tick, len(syms))

	for _, s := range syms {
		data[s] = feed.Tick{Symbol: s, Price: 100}
	}

	snap := &fakeSnap{data: data}
	ready := &Readiness{}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	want := len(syms)

	go pollReady(ctx, 250*time.Millisecond, func() bool { return len(snap.Snapshot(ctx)) >= want }, ready)

	require.Eventually(t, ready.ready.Load, time.Second, time.Millisecond,
		"pollReady must set ready once snapshot is full")
}

func TestPollReady_StaysNotReadyWithPartialSnapshot(t *testing.T) {
	t.Parallel()

	// Only 2 of 12 symbols present.
	snap := newFakeSnap("ES", "NQ")
	ready := &Readiness{}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	want := len(feed.Symbols())

	go pollReady(ctx, 250*time.Millisecond, func() bool { return len(snap.Snapshot(ctx)) >= want }, ready)

	// A partial snapshot must never flip ready. require.Never is the right
	// tool here: the assertion holds for the whole duration, not just at end.
	require.Never(t, ready.ready.Load, 300*time.Millisecond, 10*time.Millisecond,
		"pollReady must not set ready with a partial snapshot")
}

// ── runGenerator ─────────────────────────────────────────────────────────────

// TestRunGenerator_PublishesTicks is an integration test that wires the price
// simulation through the Redis publisher and verifies ticks reach the hash.
func TestRunGenerator_PublishesTicks(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	cfg := Config{
		RedisURL:       "redis://" + mr.Addr(),
		TickInterval:   20 * time.Millisecond,
		VolatilityMult: 1.0,
	}

	elector := newElector(t, mr.Addr())

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		defer close(done)

		runGenerator(ctx, cfg, redlease.NewFencer(elector, 1))
	}()

	// Wait until the publisher has written at least one symbol into the hash.
	require.Eventually(t, func() bool {
		return mr.HGet("marketfeed:prices", "ES") != ""
	}, 5*time.Second, 20*time.Millisecond, "no tick published to Redis hash")

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runGenerator did not exit after context cancel")
	}
}

// TestRunGenerator_SeedsFromLastPublishedPrice verifies that a newly elected
// leader continues the price series from the last value in the Redis hash rather
// than snapping back to the symbol's base price — the failover continuity fix.
func TestRunGenerator_SeedsFromLastPublishedPrice(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	t.Cleanup(func() { _ = rc.Close() })

	// A previous leader left ES at 5400 — far from its 5250 base but within the
	// drift band. A correct failover continues from here, not from the base.
	const seeded = 5400.0

	data, err := json.Marshal(feed.Tick{Symbol: "ES", Price: seeded, Timestamp: time.Now().UTC()})
	require.NoError(t, err)
	require.NoError(t, rc.HSet(t.Context(), "marketfeed:prices", "ES", data).Err())

	// Subscribe before the generator starts so the first ES tick is not missed.
	sub := rc.Subscribe(t.Context(), "marketfeed:tick:ES")
	t.Cleanup(func() { _ = sub.Close() })
	_, err = sub.Receive(t.Context())
	require.NoError(t, err)

	cfg := Config{
		RedisURL:       "redis://" + mr.Addr(),
		TickInterval:   20 * time.Millisecond,
		VolatilityMult: 1.0,
	}

	elector := newElector(t, mr.Addr())

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		defer close(done)

		runGenerator(ctx, cfg, redlease.NewFencer(elector, 1))
	}()

	select {
	case msg := <-sub.Channel():
		var tick feed.Tick
		require.NoError(t, json.Unmarshal([]byte(msg.Payload), &tick))

		// One GBM step from 5400 stays near 5400; an unseeded leader would emit a
		// value near the 5250 base. 5325 is the midpoint, cleanly separating them.
		assert.Greater(t, tick.Price, 5325.0,
			"new leader must continue from the seeded price, not the base price")
		assert.InDelta(t, seeded, tick.Price, 100.0,
			"first tick must be a small step from the seeded price")
	case <-time.After(2 * time.Second):
		t.Fatal("no ES tick published")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runGenerator did not exit after context cancel")
	}
}

// TestRunGenerator_StaleLeaderIsFencedOut is the end-to-end fencing test: a
// newer leadership term (higher token) writes, and a still-running stale leader
// (lower token) must not be able to overwrite the hash, even though it keeps
// generating ticks. This proves the takeover path, not just FenceWrite in
// isolation.
func TestRunGenerator_StaleLeaderIsFencedOut(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	t.Cleanup(func() { _ = rc.Close() })

	cfg := Config{
		RedisURL:       "redis://" + mr.Addr(),
		TickInterval:   20 * time.Millisecond,
		VolatilityMult: 1.0,
	}

	elector := newElector(t, mr.Addr())

	// Stale leader: token 1. Let it run and write at least one tick.
	staleCtx, cancelStale := context.WithCancel(t.Context())
	staleDone := make(chan struct{})

	go func() {
		defer close(staleDone)

		runGenerator(staleCtx, cfg, redlease.NewFencer(elector, 1))
	}()

	require.Eventually(t, func() bool {
		return mr.HGet("marketfeed:prices", "ES") != ""
	}, 5*time.Second, 20*time.Millisecond, "stale leader never published")

	// A newer leader (token 2) writes a sentinel value for ES.
	const sentinel = 9999.0

	data, err := json.Marshal(feed.Tick{Symbol: "ES", Price: sentinel})
	require.NoError(t, err)

	applied, err := elector.FenceHSet(t.Context(), 2, "marketfeed:prices", "ES", string(data))
	require.NoError(t, err)
	require.True(t, applied, "newer leader's write must be applied")

	// The stale leader keeps ticking, but every write it attempts now carries
	// token 1 and must be fenced out. Give it time to try several times; the
	// sentinel from token 2 must survive untouched.
	require.Never(t, func() bool {
		raw := mr.HGet("marketfeed:prices", "ES")
		if raw == "" {
			return false
		}

		var tick feed.Tick
		if err := json.Unmarshal([]byte(raw), &tick); err != nil {
			return false
		}

		return tick.Price != sentinel
	}, 500*time.Millisecond, 20*time.Millisecond,
		"stale leader overwrote the newer leader's state — fencing failed")

	cancelStale()

	select {
	case <-staleDone:
	case <-time.After(2 * time.Second):
		t.Fatal("stale generator did not exit after context cancel")
	}
}
