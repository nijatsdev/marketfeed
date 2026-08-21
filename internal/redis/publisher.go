package redis

import (
	"context"
	"encoding/json"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nijatsdev/marketfeed/internal/feed"
	"github.com/nijatsdev/marketfeed/internal/metrics"
	"github.com/nijatsdev/redlease"
)

// Publisher mirrors ticks into Redis: each tick is written to the prices hash
// under the leadership fencing token, and published only if that write was applied.
type Publisher struct {
	c     *goredis.Client
	fence redlease.Fencer
	ticks <-chan feed.Tick

	// fencedOut records that the previous write was rejected, so step-down logs once, not per tick.
	fencedOut bool
}

// NewPublisher wires the Redis client, fencer, and tick source into a Publisher.
func NewPublisher(c *goredis.Client, fence redlease.Fencer, ticks <-chan feed.Tick) *Publisher {
	return &Publisher{c: c, fence: fence, ticks: ticks}
}

// Run publishes ticks until the channel closes or ctx is cancelled.
func (p *Publisher) Run(ctx context.Context) {
	defer p.c.Close() //nolint:errcheck // best-effort cleanup on shutdown

	for {
		select {
		case tick, ok := <-p.ticks:
			if !ok {
				return
			}

			p.publish(ctx, tick)
		case <-ctx.Done():
			return
		}
	}
}

func (p *Publisher) publish(ctx context.Context, tick feed.Tick) {
	if ctx.Err() != nil {
		return // ctx cancelled; skip to avoid spurious error logs and metrics
	}

	data, err := json.Marshal(tick)
	if err != nil {
		slog.Warn("redis: json marshal failed", "symbol", tick.Symbol, "err", err)
		return
	}

	// Fence first: a rejected write means a newer leader owns the state, so don't broadcast.
	applied, err := p.fence.HSet(ctx, pricesHash, tick.Symbol, string(data))
	if err != nil {
		slog.Warn("redis: fenced write failed", "key", pricesHash, "symbol", tick.Symbol, "err", err)
		metrics.RedisPublishErrors.Inc()

		return
	}

	if !applied {
		metrics.TicksFenced.Inc()

		if !p.fencedOut {
			p.fencedOut = true

			slog.Warn("redis: fenced out by newer leader, dropping ticks until step-down", "symbol", tick.Symbol, "token", p.fence.Token())
		}

		return
	}

	p.fencedOut = false

	channel := tickPrefix + tick.Symbol
	if err := p.c.Publish(ctx, channel, data).Err(); err != nil {
		slog.Warn("redis: publish failed", "channel", channel, "err", err)
		metrics.RedisPublishErrors.Inc()
	}
}
