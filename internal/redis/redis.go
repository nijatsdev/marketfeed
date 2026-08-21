// Package redis provides the Redis connection, publisher, and subscriber.
package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

const (
	pricesHash  = "marketfeed:prices"
	tickPrefix  = "marketfeed:tick:"
	tickPattern = tickPrefix + "*"
)

// Connect parses redisURL, pings Redis, and returns a client.
func Connect(ctx context.Context, redisURL string) (*goredis.Client, error) {
	opts, err := goredis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}

	c := goredis.NewClient(opts)

	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, err
	}

	return c, nil
}
