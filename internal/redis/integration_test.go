package redis

// Integration tests exercise the same paths as the miniredis unit tests
// against a real Redis server — the real Lua engine, INCR semantics, and
// pub/sub — catching anything miniredis's re-implementation glosses over.
//
// They run only when REDIS_ADDR points at a reachable server (CI provides one
// as a service container) and are skipped otherwise:
//
//	REDIS_ADDR=localhost:6379 go test ./internal/redis/
//
// They use DB 9 and flush it, staying clear of default-DB data.

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nijatsdev/marketfeed/internal/feed"
	"github.com/nijatsdev/redlease"
)

const integrationDB = 9

// realRedis returns a client for the server behind REDIS_ADDR, skipping the
// test when unset. The integration DB is flushed before and after.
func realRedis(t *testing.T) *goredis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping real-Redis integration test")
	}

	rc := goredis.NewClient(&goredis.Options{Addr: addr, DB: integrationDB})
	require.NoError(t, rc.FlushDB(t.Context()).Err())

	t.Cleanup(func() {
		_ = rc.FlushDB(context.Background()).Err()
		_ = rc.Close()
	})

	return rc
}

// TestIntegration_FencedTickRoundTrip drives a tick through the full pipeline
// on real Redis: fenced hash write via the redlease Lua script, pub/sub
// broadcast, subscriber delivery, and snapshot reads — then verifies a stale
// leadership term is fenced out by the real Lua engine.
//
//nolint:paralleltest // integration tests share and flush DB 9; they must run sequentially
func TestIntegration_FencedTickRoundTrip(t *testing.T) {
	rc := realRedis(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch := NewSubscriber(rc).Subscribe(ctx)
	waitForPatternSub(t, rc)

	elector, err := redlease.New(rc, redlease.Config{Name: "{marketfeed}"})
	require.NoError(t, err)

	// The publisher closes its broadcast client on shutdown; give it its own.
	bcast := goredis.NewClient(&goredis.Options{Addr: os.Getenv("REDIS_ADDR"), DB: integrationDB})
	ticks := make(chan feed.Tick, 1)

	go NewPublisher(bcast, redlease.NewFencer(elector, 1), ticks).Run(ctx)

	ticks <- feed.Tick{Symbol: "ES", Price: 5250, Open: 5200, High: 5260, Low: 5190}

	got, ok := receiveTick(t, ch, 5*time.Second)
	require.True(t, ok, "tick must arrive over real pub/sub")
	assert.Equal(t, "ES", got.Symbol)
	assert.InDelta(t, 5250.0, got.Price, 1e-9)
	assert.InDelta(t, 5200.0, got.Open, 1e-9, "session stats must survive the round trip")

	// The fenced write landed in the durable snapshot.
	snap, err := NewSubscriber(rc).PriceFor(ctx, "ES")
	require.NoError(t, err)
	assert.InDelta(t, 5250.0, snap.Price, 1e-9)
	assert.Equal(t, 1, NewSubscriber(rc).Count(ctx))

	// A newer term (token 2) takes over; the token-1 publisher's next tick must
	// be rejected by the real Lua fence, leaving the sentinel untouched.
	sentinel, err := json.Marshal(feed.Tick{Symbol: "ES", Price: 9999})
	require.NoError(t, err)

	applied, err := elector.FenceHSet(t.Context(), 2, pricesHash, "ES", string(sentinel))
	require.NoError(t, err)
	require.True(t, applied, "the newer term's write must apply")

	ticks <- feed.Tick{Symbol: "ES", Price: 1111}

	require.Never(t, func() bool {
		tick, err := NewSubscriber(rc).PriceFor(ctx, "ES")
		return err != nil || tick.Price != 9999
	}, time.Second, 50*time.Millisecond,
		"stale leader's tick overwrote the newer term's state — real-Lua fencing failed")
}
