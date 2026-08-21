package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// freePort reserves an ephemeral port and releases it for the server to bind.
// The tiny reuse window is an accepted test-only race.
func freePort(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	_, port, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	require.NoError(t, ln.Close())

	return port
}

// getStatus fetches and decodes /status from the given port, reporting ok=false
// while the server is not yet accepting.
func getStatus(t *testing.T, port string) (statusResponse, bool) {
	t.Helper()

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/status", port)) //nolint:noctx // test helper with Eventually timeout
	if err != nil {
		return statusResponse{}, false
	}

	defer resp.Body.Close() //nolint:errcheck // test teardown

	var s statusResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return statusResponse{}, false
	}

	return s, true
}

// TestRunClustered_LeaderWiring is the leadership wiring test: an instance with
// a free lock must elect itself, report leader + fence over /status, publish
// prices into the hash, and release the lock on shutdown.
func TestRunClustered_LeaderWiring(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	cfg := testConfig()
	cfg.Port = freePort(t)
	cfg.TickInterval = 20 * time.Millisecond
	cfg.RedisURL = "redis://" + mr.Addr()

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)

	go func() { done <- runClustered(ctx, cfg, rc) }()

	require.Eventually(t, func() bool {
		s, ok := getStatus(t, cfg.Port)
		return ok && s.Role == "leader" && s.Fence >= 1 && s.Redis
	}, 5*time.Second, 20*time.Millisecond, "instance never reported itself leader over /status")

	// The elected leader's generator must mirror prices into the hash.
	require.Eventually(t, func() bool {
		return mr.HGet("marketfeed:prices", "ES") != ""
	}, 5*time.Second, 20*time.Millisecond, "leader never published prices")

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err, "graceful shutdown must not error")
	case <-time.After(10 * time.Second):
		t.Fatal("runClustered did not return after context cancel")
	}

	// Shutdown releases the lock so a successor doesn't wait out the TTL.
	assert.False(t, mr.Exists("{marketfeed}:leader"), "lock must be released on shutdown")
}

// TestRunClustered_FollowerWiring verifies that an instance finding the lock
// held reports itself follower and still serves.
func TestRunClustered_FollowerWiring(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)

	// Another instance already leads.
	require.NoError(t, mr.Set("{marketfeed}:leader", "someone-else"))
	mr.SetTTL("{marketfeed}:leader", time.Minute)

	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})

	cfg := testConfig()
	cfg.Port = freePort(t)
	cfg.RedisURL = "redis://" + mr.Addr()

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)

	go func() { done <- runClustered(ctx, cfg, rc) }()

	require.Eventually(t, func() bool {
		s, ok := getStatus(t, cfg.Port)
		return ok && s.Role == "follower" && s.Fence == 0
	}, 5*time.Second, 20*time.Millisecond, "instance never reported itself follower over /status")

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("runClustered did not return after context cancel")
	}
}
