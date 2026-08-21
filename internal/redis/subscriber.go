package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nijatsdev/marketfeed/internal/feed"
)

// Subscriber reads price ticks from Redis pub/sub for use on follower instances.
type Subscriber struct {
	client *goredis.Client

	// cache holds the latest tick per symbol; reads use it once Run sets primed.
	mu     sync.RWMutex
	cache  map[string]feed.Tick
	primed bool
}

// NewSubscriber returns a Subscriber backed by client.
func NewSubscriber(client *goredis.Client) *Subscriber {
	return &Subscriber{client: client, cache: map[string]feed.Tick{}}
}

// Run primes the tick cache, then keeps it current from pub/sub. Call in a goroutine; returns when ctx is cancelled.
func (s *Subscriber) Run(ctx context.Context) {
	// Subscribe before priming so no tick is missed in the gap between them.
	pubsub := s.client.PSubscribe(ctx, tickPattern)
	defer pubsub.Close() //nolint:errcheck // teardown; error is not actionable

	s.prime(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-pubsub.Channel():
			if !ok {
				return
			}

			tick, err := unmarshalTick(msg.Payload)
			if err != nil {
				continue
			}

			s.mu.Lock()
			s.cache[tick.Symbol] = tick
			s.mu.Unlock()
		}
	}
}

// prime loads the current hash into the cache and marks it authoritative.
func (s *Subscriber) prime(ctx context.Context) {
	seed := s.fetchSnapshot(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Ticks that arrived while priming are newer than the hash read; keep them.
	for sym, tick := range seed {
		if _, ok := s.cache[sym]; !ok {
			s.cache[sym] = tick
		}
	}

	s.primed = true
}

// Subscribe returns a channel of ticks from Redis pub/sub. It survives
// reconnects (go-redis resubscribes) and closes only when ctx is cancelled.
func (s *Subscriber) Subscribe(ctx context.Context) <-chan feed.Tick {
	ch := make(chan feed.Tick, 64)
	pubsub := s.client.PSubscribe(ctx, tickPattern)

	go func() {
		defer close(ch)
		defer pubsub.Close() //nolint:errcheck // teardown; error is not actionable

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-pubsub.Channel():
				if !ok {
					return
				}

				tick, err := unmarshalTick(msg.Payload)
				if err != nil {
					slog.Warn("redis: unmarshal failed", "err", err)
					continue
				}

				select {
				case ch <- tick:
				default:
				}
			}
		}
	}()

	return ch
}

// Count returns the number of symbols in the price hash via a single HLEN.
func (s *Subscriber) Count(ctx context.Context) int {
	n, err := s.client.HLen(ctx, pricesHash).Result()
	if err != nil {
		return 0
	}

	return int(n)
}

// Snapshot returns the current price for all symbols, from the cache once primed.
func (s *Subscriber) Snapshot(ctx context.Context) map[string]feed.Tick {
	if out, primed := s.cachedSnapshot(); primed {
		return out
	}

	return s.fetchSnapshot(ctx)
}

// cachedSnapshot copies the cached ticks, reporting false if the cache is not primed.
func (s *Subscriber) cachedSnapshot() (map[string]feed.Tick, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.primed {
		return nil, false
	}

	out := make(map[string]feed.Tick, len(s.cache))
	maps.Copy(out, s.cache)

	return out, true
}

// cachedTick looks symbol up in the cache; found is meaningful only when primed is true.
func (s *Subscriber) cachedTick(symbol string) (tick feed.Tick, found, primed bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.primed {
		return feed.Tick{}, false, false
	}

	tick, found = s.cache[symbol]

	return tick, found, true
}

// fetchSnapshot reads every tick straight from the price hash.
func (s *Subscriber) fetchSnapshot(ctx context.Context) map[string]feed.Tick {
	data, err := s.client.HGetAll(ctx, pricesHash).Result()
	if err != nil {
		slog.Warn("redis: snapshot failed", "err", err)
		return map[string]feed.Tick{}
	}

	result := make(map[string]feed.Tick, len(data))

	for sym, raw := range data {
		if tick, err := unmarshalTick(raw); err == nil {
			result[sym] = tick
		}
	}

	return result
}

// PriceFor returns a symbol's current tick from the cache once primed, or feed.ErrUnknownSymbol if absent.
func (s *Subscriber) PriceFor(ctx context.Context, symbol string) (feed.Tick, error) {
	if tick, found, primed := s.cachedTick(symbol); primed {
		if !found {
			return feed.Tick{}, feed.ErrUnknownSymbol
		}

		return tick, nil
	}

	raw, err := s.client.HGet(ctx, pricesHash, symbol).Result()

	switch {
	case errors.Is(err, goredis.Nil):
		return feed.Tick{}, feed.ErrUnknownSymbol
	case err != nil:
		return feed.Tick{}, fmt.Errorf("redis: price lookup for %s: %w", symbol, err)
	}

	tick, err := unmarshalTick(raw)
	if err != nil {
		return feed.Tick{}, fmt.Errorf("redis: corrupt tick for %s: %w", symbol, err)
	}

	return tick, nil
}

func unmarshalTick(raw string) (feed.Tick, error) {
	var tick feed.Tick
	return tick, json.Unmarshal([]byte(raw), &tick)
}
