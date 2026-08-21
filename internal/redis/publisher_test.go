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
	"go.uber.org/goleak"

	"github.com/nijatsdev/marketfeed/internal/feed"
	"github.com/nijatsdev/redlease"
)

// newElector returns a redlease elector backed by rc, for fencing in tests.
func newElector(t *testing.T, rc *goredis.Client) *redlease.Elector {
	t.Helper()

	e, err := redlease.New(rc, redlease.Config{Name: "marketfeed"})
	require.NoError(t, err)

	return e
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newPublisher(t *testing.T) (*miniredis.Miniredis, *goredis.Client, chan feed.Tick, *Publisher) {
	t.Helper()

	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	t.Cleanup(func() { _ = rc.Close() }) // fallback if p.Run is never called

	ch := make(chan feed.Tick, 8)

	return mr, rc, ch, NewPublisher(rc, redlease.NewFencer(newElector(t, rc), 1), ch)
}

func startPublisher(ctx context.Context, t *testing.T, p *Publisher) func(string) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		p.Run(ctx)
		close(done)
	}()

	return func(failMsg string) {
		t.Helper()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal(failMsg)
		}
	}
}

func TestPublisher_PublishesTickToChannelAndHash(t *testing.T) {
	t.Parallel()

	mr, _, ch, p := newPublisher(t)

	subRC := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	t.Cleanup(func() { _ = subRC.Close() })

	sub := subRC.Subscribe(t.Context(), tickPrefix+"ES")
	t.Cleanup(func() { _ = sub.Close() })
	_, err := sub.Receive(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	wait := startPublisher(ctx, t, p)

	ch <- feed.Tick{Symbol: "ES", Price: 5262.25, Bid: 5262.00, Ask: 5262.50}

	select {
	case msg := <-sub.Channel():
		assert.Equal(t, tickPrefix+"ES", msg.Channel)

		var tick feed.Tick

		require.NoError(t, json.Unmarshal([]byte(msg.Payload), &tick))
		assert.Equal(t, "ES", tick.Symbol)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for published message")
	}

	var val string

	require.Eventually(t, func() bool {
		val = mr.HGet(pricesHash, "ES")
		return val != ""
	}, time.Second, 10*time.Millisecond, "no HSet recorded")

	var tick feed.Tick
	require.NoError(t, json.Unmarshal([]byte(val), &tick))
	assert.Equal(t, "ES", tick.Symbol)

	cancel()
	wait("publisher did not exit")
}

func TestPublisher_FencedOutTickIsNotPublished(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	t.Cleanup(func() { _ = rc.Close() })

	elector := newElector(t, rc)

	// A newer leader (token 9) has already written; this publisher carries an
	// older token and must be fenced out.
	applied, err := elector.FenceHSet(t.Context(), 9, pricesHash, "ES", "newer")
	require.NoError(t, err)
	require.True(t, applied)

	subRC := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	t.Cleanup(func() { _ = subRC.Close() })

	sub := subRC.Subscribe(t.Context(), tickPrefix+"ES")
	t.Cleanup(func() { _ = sub.Close() })
	_, err = sub.Receive(t.Context())
	require.NoError(t, err)

	ch := make(chan feed.Tick, 1)
	p := NewPublisher(rc, redlease.NewFencer(elector, 4), ch) // stale token

	ctx, cancel := context.WithCancel(t.Context())
	wait := startPublisher(ctx, t, p)

	ch <- feed.Tick{Symbol: "ES", Price: 1, Bid: 1, Ask: 1}

	// Nothing must be published, and the newer leader's hash value must stand.
	select {
	case msg := <-sub.Channel():
		t.Fatalf("stale leader published a fenced-out tick: %q", msg.Payload)
	case <-time.After(200 * time.Millisecond):
	}

	assert.Equal(t, "newer", mr.HGet(pricesHash, "ES"), "fenced-out write must not overwrite newer state")

	cancel()
	wait("publisher did not exit")
}

func TestPublisher_ExitsWhenChannelClosed(t *testing.T) {
	t.Parallel()

	_, rc, ch, p := newPublisher(t)
	wait := startPublisher(t.Context(), t, p)

	close(ch)
	wait("publisher did not exit after channel closed")

	assert.ErrorIs(t, rc.Ping(context.Background()).Err(), goredis.ErrClosed)
}
