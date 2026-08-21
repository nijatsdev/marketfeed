package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nijatsdev/marketfeed/internal/feed"
	"github.com/nijatsdev/marketfeed/internal/hub"
)

func testConfig() Config {
	return Config{
		Port:           "0",
		TickInterval:   50 * time.Millisecond,
		VolatilityMult: 1.0,
	}
}

func TestReadyz_notReadyBeforeStart(t *testing.T) {
	t.Parallel()

	srv, _, _, _ := build(t.Context(), testConfig())
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestReadyz_readyAfterFeedStarts(t *testing.T) {
	t.Parallel()

	srv, f, _, ready := build(t.Context(), testConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	f.Start(ctx)

	require.Eventually(t, f.Ready, 5*time.Second, 10*time.Millisecond,
		"feed did not become ready within 5s")

	ready.SetReady(true)

	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestStatusEndpoint_symbolIntervals(t *testing.T) {
	t.Parallel()

	// The built-in catalog sets a per-symbol cadence for every symbol, so
	// /status reports them and the decoded map matches SymbolIntervalsMS.
	srv, _, _, _ := build(t.Context(), testConfig())
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status", nil))

	var resp statusResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotEmpty(t, resp.SymbolIntervalsMS, "catalog overrides must surface in /status")
	assert.Equal(t, feed.SymbolIntervalsMS(), resp.SymbolIntervalsMS)
}

func TestStatusRoleTransitions(t *testing.T) {
	t.Parallel()

	f := feed.New(time.Hour, 1.0)
	s := newStatus(testConfig(), hub.New(t.Context(), f), &Readiness{}, true)

	assert.Equal(t, "electing", roleName(s.role.Load()), "redis-enabled instance starts electing")

	s.setLeader(3)
	assert.Equal(t, "leader", roleName(s.role.Load()))
	assert.EqualValues(t, 3, s.fence.Load())

	s.setElecting()
	assert.Equal(t, "electing", roleName(s.role.Load()))
	assert.Zero(t, s.fence.Load(), "stepping down clears the fence")

	s.setFollower()
	assert.Equal(t, "follower", roleName(s.role.Load()))
	assert.Zero(t, s.fence.Load())
}

func TestMethodLabel(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "GET", methodLabel(http.MethodGet), "known methods pass through")
	assert.Equal(t, "OTHER", methodLabel("FOOBAR"),
		"arbitrary methods must not mint new metric label values")
}

func TestStatusEndpoint_standalone(t *testing.T) {
	t.Parallel()

	srv, _, _, _ := build(t.Context(), testConfig())
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status", nil))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var resp statusResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.False(t, resp.Redis)
	assert.Equal(t, "standalone", resp.Role)
	assert.Zero(t, resp.Fence)
	assert.Zero(t, resp.WSClients)
	assert.False(t, resp.Ready)
}

func TestRunGracefulShutdown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- Run(ctx, testConfig())
	}()

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down within 3s")
	}
}
