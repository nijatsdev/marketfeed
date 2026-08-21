package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nijatsdev/marketfeed/internal/feed"
)

func newSubscriber(t *testing.T) (*miniredis.Miniredis, *goredis.Client, *Subscriber) {
	t.Helper()

	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	t.Cleanup(func() { _ = rc.Close() })

	return mr, rc, NewSubscriber(rc)
}

func publishTick(mr *miniredis.Miniredis, tick feed.Tick) {
	data, _ := json.Marshal(tick)
	mr.Publish(tickPrefix+tick.Symbol, string(data))
}

func receiveTick(t *testing.T, ch <-chan feed.Tick, timeout time.Duration) (feed.Tick, bool) {
	t.Helper()

	select {
	case tick, ok := <-ch:
		return tick, ok
	case <-time.After(timeout):
		t.Error("timed out waiting for tick")
		return feed.Tick{}, false
	}
}

// waitForPatternSub polls until the pattern subscription is acknowledged by the
// in-process Redis server. This replaces time.Sleep and is race-free: the
// PUBSUB NUMPAT command returns 0 until PSubscribe has been processed.
func waitForPatternSub(t *testing.T, rc *goredis.Client) {
	t.Helper()

	require.Eventually(t, func() bool {
		n, err := rc.PubSubNumPat(t.Context()).Result()
		return err == nil && n >= 1
	}, time.Second, time.Millisecond, "PSubscribe not registered")
}

// TestSubscriber_RunServesReadsFromCache deletes the hash after priming: reads that still succeed came from memory.
func TestSubscriber_RunServesReadsFromCache(t *testing.T) {
	t.Parallel()

	mr, rc, sub := newSubscriber(t)

	seeded, err := json.Marshal(feed.Tick{Symbol: "ES", Price: 7500})
	require.NoError(t, err)
	require.NoError(t, rc.HSet(t.Context(), pricesHash, "ES", seeded).Err())

	go sub.Run(t.Context())

	waitForPatternSub(t, rc)

	require.Eventually(t, func() bool {
		return len(sub.Snapshot(t.Context())) == 1
	}, time.Second, time.Millisecond, "cache was not primed from the hash")

	// A live tick must update the cache.
	publishTick(mr, feed.Tick{Symbol: "NQ", Price: 30000})
	require.Eventually(t, func() bool {
		return len(sub.Snapshot(t.Context())) == 2
	}, time.Second, time.Millisecond, "pub/sub tick did not reach the cache")

	// With the hash gone, any read that still hit Redis would fail.
	mr.Del(pricesHash)

	snap := sub.Snapshot(t.Context())
	assert.Len(t, snap, 2, "snapshot must come from memory, not Redis")

	tick, err := sub.PriceFor(t.Context(), "NQ")
	require.NoError(t, err)
	assert.InDelta(t, 30000.0, tick.Price, 0.001)

	_, err = sub.PriceFor(t.Context(), "MISSING")
	assert.ErrorIs(t, err, feed.ErrUnknownSymbol, "unknown symbols still report ErrUnknownSymbol")
}

func TestSubscriber_DeliversTick(t *testing.T) {
	t.Parallel()

	mr, rc, sub := newSubscriber(t)
	ch := sub.Subscribe(t.Context())

	waitForPatternSub(t, rc)

	publishTick(mr, feed.Tick{Symbol: "ES", Price: 5262.25})

	tick, _ := receiveTick(t, ch, time.Second)
	assert.Equal(t, "ES", tick.Symbol)
	assert.InDelta(t, 5262.25, tick.Price, 0.001)
}

// TestSubscriber_ChannelRemainsOpenAfterDisconnect verifies that the tick
// channel stays open when Redis goes down instead of closing permanently.
func TestSubscriber_ChannelRemainsOpenAfterDisconnect(t *testing.T) {
	t.Parallel()

	mr, rc, sub := newSubscriber(t)
	ch := sub.Subscribe(t.Context())

	waitForPatternSub(t, rc)

	publishTick(mr, feed.Tick{Symbol: "ES", Price: 1})
	receiveTick(t, ch, time.Second)

	mr.Close()

	// ch must remain open while go-redis reconnects behind the scenes: closing
	// it would be a bug. require.Never is the right tool for "must NOT happen".
	require.Never(t, func() bool {
		select {
		case _, open := <-ch:
			return !open
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond,
		"subscriber channel closed while ctx is still alive")
}

func TestSubscriber_ChannelClosesOnContextCancel(t *testing.T) {
	t.Parallel()

	_, _, sub := newSubscriber(t)

	ctx, cancel := context.WithCancel(t.Context())
	ch := sub.Subscribe(ctx)

	cancel()

	select {
	case _, open := <-ch:
		assert.False(t, open, "channel must close when ctx is cancelled")
	case <-time.After(time.Second):
		t.Fatal("channel was not closed after ctx cancel")
	}
}

func TestSubscriber_Snapshot(t *testing.T) {
	t.Parallel()

	_, rc, sub := newSubscriber(t)

	tick := feed.Tick{Symbol: "ES", Price: 5262.25}
	data, err := json.Marshal(tick)
	require.NoError(t, err)
	require.NoError(t, rc.HSet(t.Context(), pricesHash, "ES", data).Err())

	snap := sub.Snapshot(t.Context())
	require.Contains(t, snap, "ES")
	assert.InDelta(t, 5262.25, snap["ES"].Price, 0.001)
}

func TestSubscriber_PriceFor(t *testing.T) {
	t.Parallel()

	_, rc, sub := newSubscriber(t)

	tick := feed.Tick{Symbol: "NQ", Price: 18000.5}
	data, err := json.Marshal(tick)
	require.NoError(t, err)
	require.NoError(t, rc.HSet(t.Context(), pricesHash, "NQ", data).Err())

	got, err := sub.PriceFor(t.Context(), "NQ")
	require.NoError(t, err)
	assert.InDelta(t, 18000.5, got.Price, 0.001)

	_, err = sub.PriceFor(t.Context(), "MISSING")
	assert.ErrorIs(t, err, feed.ErrUnknownSymbol,
		"a symbol absent from the hash must map to ErrUnknownSymbol")
}
