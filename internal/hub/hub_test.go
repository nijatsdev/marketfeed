package hub_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/nijatsdev/marketfeed/internal/feed"
	"github.com/nijatsdev/marketfeed/internal/hub"
)

// priceDelta is the tolerance used when comparing float64 prices in assertions.
const priceDelta = 1e-9

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.IgnoreCurrent())
}

type fakeFeed struct {
	ticks    chan feed.Tick
	snapshot map[string]feed.Tick
}

func newFakeFeed(snap map[string]feed.Tick) *fakeFeed {
	return &fakeFeed{
		ticks:    make(chan feed.Tick, 64),
		snapshot: snap,
	}
}

func (f *fakeFeed) Subscribe(_ context.Context) <-chan feed.Tick    { return f.ticks }
func (f *fakeFeed) Snapshot(_ context.Context) map[string]feed.Tick { return f.snapshot }
func (f *fakeFeed) send(t feed.Tick)                                { f.ticks <- t }

// newTestServer creates a Hub + httptest.Server backed by a fakeFeed. The hub
// is wired to t.Context() so it shuts down with the test.
func newTestServer(t *testing.T, snap map[string]feed.Tick) (*fakeFeed, *httptest.Server, *hub.Hub) {
	t.Helper()

	f := newFakeFeed(snap)
	h := hub.New(t.Context(), f)
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)

	go h.Run(t.Context())

	return f, srv, h
}

func dialCtx(ctx context.Context, t *testing.T, srv *httptest.Server, path string) *websocket.Conn {
	t.Helper()

	u := "ws" + strings.TrimPrefix(srv.URL, "http") + path
	conn, _, err := websocket.Dial(ctx, u, nil)
	require.NoError(t, err, "dial %s", u)
	t.Cleanup(func() { conn.CloseNow() }) //nolint:errcheck,gosec // best-effort test cleanup

	return conn
}

// dial opens a WebSocket connection bound to the test's context.
func dial(t *testing.T, srv *httptest.Server, path string) *websocket.Conn {
	t.Helper()
	return dialCtx(t.Context(), t, srv, path)
}

// dialBg opens a WebSocket connection using context.Background() so it is NOT
// cancelled when the test's context is cancelled. Use when the test itself
// cancels the server and needs to observe the resulting close frame.
func dialBg(t *testing.T, srv *httptest.Server, path string) *websocket.Conn {
	t.Helper()
	return dialCtx(context.Background(), t, srv, path)
}

func readTick(t *testing.T, conn *websocket.Conn) feed.Tick {
	t.Helper()

	readCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	_, msg, err := conn.Read(readCtx)
	require.NoError(t, err)

	var tick feed.Tick
	require.NoError(t, json.Unmarshal(msg, &tick))

	return tick
}

// waitClients blocks until h.ClientCount() equals want or the deadline passes.
func waitClients(t *testing.T, h *hub.Hub, want int) {
	t.Helper()

	require.Eventually(t, func() bool { return h.ClientCount() == want },
		time.Second, time.Millisecond, "hub client count did not reach %d", want)
}

func TestHub_SnapshotSentOnConnect(t *testing.T) {
	t.Parallel()

	snap := map[string]feed.Tick{
		"ES": {Symbol: "ES", Price: 5000, Bid: 4999.75, Ask: 5000.25},
	}
	_, srv, _ := newTestServer(t, snap)

	conn := dial(t, srv, "/ws/stream")

	tick := readTick(t, conn)
	assert.Equal(t, "ES", tick.Symbol)
	assert.InDelta(t, 5000.0, tick.Price, priceDelta)
}

// TestHub_SnapshotPrecedesLiveTick verifies that snapshot data is always
// enqueued ahead of any broadcast tick the client could receive.
func TestHub_SnapshotPrecedesLiveTick(t *testing.T) {
	t.Parallel()

	snap := map[string]feed.Tick{
		"ES": {Symbol: "ES", Price: 5000},
	}
	f, srv, h := newTestServer(t, snap)

	conn := dial(t, srv, "/ws/stream")

	// Registration happens after snapshot is enqueued, so once we see the
	// client count we know the ES snapshot is already in the send buffer.
	waitClients(t, h, 1)

	// Send a live tick for a different symbol — it lands after the snapshot.
	f.send(feed.Tick{Symbol: "NQ", Price: 18000})

	first := readTick(t, conn)
	assert.Equal(t, "ES", first.Symbol, "snapshot must be the first message")

	second := readTick(t, conn)
	assert.Equal(t, "NQ", second.Symbol, "live tick must follow snapshot")
}

func TestHub_BroadcastReachesClient(t *testing.T) {
	t.Parallel()

	f, srv, h := newTestServer(t, nil)

	conn := dial(t, srv, "/ws/stream")
	waitClients(t, h, 1)

	want := feed.Tick{Symbol: "NQ", Price: 18000, Bid: 17999.75, Ask: 18000.25}
	f.send(want)

	got := readTick(t, conn)
	assert.Equal(t, want.Symbol, got.Symbol)
	assert.InDelta(t, want.Price, got.Price, priceDelta)
}

func TestHub_SymbolFilterRejects(t *testing.T) {
	t.Parallel()

	f, srv, h := newTestServer(t, nil)

	conn := dial(t, srv, "/ws/stream?symbols=NQ")
	waitClients(t, h, 1)

	// Send ES (filtered out) then NQ.
	f.send(feed.Tick{Symbol: "ES", Price: 5100})
	f.send(feed.Tick{Symbol: "NQ", Price: 18500})

	tick := readTick(t, conn)
	assert.Equal(t, "NQ", tick.Symbol, "expected only NQ to pass filter")
}

func TestHub_ClientRemovedAfterDisconnect(t *testing.T) {
	t.Parallel()

	f, srv, h := newTestServer(t, nil)

	conn := dial(t, srv, "/ws/stream")
	waitClients(t, h, 1)

	conn.CloseNow() //nolint:errcheck,gosec // intentional disconnect

	// Wait for ServeWS to unregister the client.
	waitClients(t, h, 0)

	// After unregistration, sending ticks must not block or panic.
	for i := range 5 {
		f.send(feed.Tick{Symbol: "ES", Price: float64(5000 + i)})
	}
}

func TestHub_RunExitsWhenFeedCloses(t *testing.T) {
	t.Parallel()

	f := newFakeFeed(nil)
	h := hub.New(t.Context(), f)

	done := make(chan struct{})

	go func() {
		h.Run(t.Context())
		close(done)
	}()

	close(f.ticks)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("hub.Run did not exit after feed channel closed")
	}
}

// TestHub_GracefulShutdown verifies that cancelling the server-lifetime context
// sends a close frame to connected clients and Hub.Wait returns.
func TestHub_GracefulShutdown(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	f := newFakeFeed(nil)
	h := hub.New(ctx, f)
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	t.Cleanup(cancel)

	go h.Run(ctx)

	// Use dialBg so the client connection lives independently of the server ctx.
	conn := dialBg(t, srv, "/ws/stream")

	waitClients(t, h, 1)

	// Shut down: cancel the hub's root context.
	cancel()

	// The server should send a close frame; the client read must return an error.
	readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer readCancel()

	_, _, err := conn.Read(readCtx)
	require.Error(t, err, "client connection must be closed after server shutdown")

	// Hub.Wait must return once all handlers have drained.
	waitDone := make(chan struct{})

	go func() {
		h.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Hub.Wait did not return after context cancel")
	}
}
