// Package server wires together the HTTP server, feed, hub, and Redis publisher.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	goredis "github.com/redis/go-redis/v9"

	"github.com/nijatsdev/marketfeed/internal/feed"
	"github.com/nijatsdev/marketfeed/internal/hub"
	"github.com/nijatsdev/marketfeed/internal/metrics"
	"github.com/nijatsdev/marketfeed/internal/redis"
	"github.com/nijatsdev/redlease"
)

// Config holds the runtime parameters for the marketfeed server.
type Config struct {
	Port           string
	TickInterval   time.Duration
	VolatilityMult float64
	RedisURL       string
}

// Run starts the server and blocks until ctx is canceled.
func Run(ctx context.Context, cfg Config) error {
	if cfg.RedisURL == "" {
		return runStandalone(ctx, cfg)
	}

	rc, err := redis.Connect(ctx, cfg.RedisURL)
	if err != nil {
		slog.Warn("redis disabled: connection failed, running standalone", "err", err)
		return runStandalone(ctx, cfg)
	}

	return runClustered(ctx, cfg, rc)
}

func runStandalone(ctx context.Context, cfg Config) error {
	srv, f, h, ready := build(ctx, cfg)

	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", srv.Addr)
	if err != nil {
		return err
	}

	f.Start(ctx)

	go h.Run(ctx)
	go pollReady(ctx, 50*time.Millisecond, f.Ready, ready)

	logStarted(cfg)

	return serve(ctx, srv, h, ln, ready)
}

const (
	// lockReleaseGrace caps how long shutdown waits for the Redis lock to release.
	lockReleaseGrace = 6 * time.Second

	// leaderName is the redlease lock name; all instances contend for the same one.
	leaderName = "{marketfeed}"
)

// newLeaderElector wires the redlease elector: observers update status and resolve the first round via roleDecided.
func newLeaderElector(ctx context.Context, rc *goredis.Client, status *Status, roleDecided func()) (*redlease.Elector, error) {
	return redlease.New(rc, redlease.Config{
		Name: leaderName,
		Observer: redlease.Observer{
			OnElected: func(token int64) {
				status.setLeader(token)
				roleDecided()
				slog.Info("leader: elected", "fence", token)
			},
			OnFollower: func() {
				status.setFollower()
				roleDecided()
				slog.Info("redis: running as follower")
			},
			OnSteppedDown: func() {
				if ctx.Err() != nil {
					slog.Info("leader: stepped down (shutting down)")
				} else {
					status.setElecting()
					slog.Info("leader: stepped down, re-contending")
				}
			},
			OnError: func(err error) {
				metrics.ElectionErrors.Inc()
				slog.Warn("redis: election round trip failed", "err", err)
			},
		},
	})
}

// runClustered serves WebSocket and REST from Redis on every replica; the lock holder runs the price generator, and another instance takes over if it exits.
func runClustered(ctx context.Context, cfg Config, rc *goredis.Client) error {
	defer rc.Close() //nolint:errcheck // best-effort cleanup; error not actionable on shutdown

	sub := redis.NewSubscriber(rc)
	srv, h, ready, status := buildComponents(ctx, cfg, sub, true)

	// decided closes once the first election resolves; we don't serve until then.
	decided := make(chan struct{})
	roleDecided := sync.OnceFunc(func() { close(decided) })

	elector, err := newLeaderElector(ctx, rc, status, roleDecided)
	if err != nil {
		return err
	}

	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", srv.Addr)
	if err != nil {
		return err
	}

	leadershipDone := make(chan struct{})

	go func() {
		defer close(leadershipDone)

		elector.Run(ctx, func(leaderCtx context.Context, fence redlease.Fencer) {
			runGenerator(leaderCtx, cfg, fence)
		})
	}()

	want := len(feed.Symbols())

	go h.Run(ctx)
	go sub.Run(ctx)
	go pollReady(ctx, 250*time.Millisecond, func() bool { return sub.Count(ctx) >= want }, ready)

	// Log while waiting so a stuck startup is visible instead of silent.
	waitLog := time.NewTicker(10 * time.Second)
	defer waitLog.Stop()

wait:
	for {
		select {
		case <-decided:
			logStarted(cfg)
			break wait
		case <-waitLog.C:
			slog.Warn("redis: first election round still unresolved, not serving yet")
		case <-ctx.Done():
			_ = ln.Close()
			return ctx.Err()
		}
	}

	err = serve(ctx, srv, h, ln, ready)

	// Let the elector release the lock before rc.Close, bounded so a slow Redis can't hang shutdown.
	select {
	case <-leadershipDone:
	case <-time.After(lockReleaseGrace):
	}

	return err
}

// build wires the standalone components (no Redis); the discarded *Status keeps
// its zero-value "standalone" role.
func build(ctx context.Context, cfg Config) (*http.Server, *feed.Feed, *hub.Hub, *Readiness) {
	f := feed.New(cfg.TickInterval, cfg.VolatilityMult)
	srv, h, ready, _ := buildComponents(ctx, cfg, f, false)

	return srv, f, h, ready
}

// dataSource is satisfied by both standalone (feed.Feed) and clustered
// (redis.Subscriber) sources: the REST read side plus the hub's Subscribe.
type dataSource interface {
	snapshotter
	Subscribe(ctx context.Context) <-chan feed.Tick
}

var (
	_ dataSource = (*feed.Feed)(nil)
	_ dataSource = (*redis.Subscriber)(nil)
)

// buildComponents wires the HTTP server, hub, readiness, and status from any data source; redisEnabled seeds the status role.
func buildComponents(ctx context.Context, cfg Config, src dataSource, redisEnabled bool) (*http.Server, *hub.Hub, *Readiness, *Status) {
	h := hub.New(ctx, src)
	ready := &Readiness{}
	status := newStatus(cfg, h, ready, redisEnabled)

	return newHTTPServer(cfg, buildRouter(newHandler(src), h, ready, status)), h, ready, status
}

// runGenerator runs the price simulation and mirrors every tick into Redis, stamping each write with fence's token so a superseded leader can't overwrite newer state. Returns when ctx is cancelled.
func runGenerator(ctx context.Context, cfg Config, fence redlease.Fencer) {
	pubClient, err := redis.Connect(ctx, cfg.RedisURL)
	if err != nil {
		slog.Warn("leader: publisher unavailable, prices will not be mirrored", "err", err)
		// Run the simulation locally so metrics still update; nothing is mirrored.
		feed.New(cfg.TickInterval, cfg.VolatilityMult).Start(ctx)
		<-ctx.Done()

		return
	}

	// Seed from the last published ticks so a new leader continues the series instead of restarting.
	seed := redis.NewSubscriber(pubClient).Snapshot(ctx)

	f := feed.NewSeeded(cfg.TickInterval, cfg.VolatilityMult, seed)
	f.Start(ctx)

	slog.Info("leader: price generator started", "seeded_symbols", len(seed), "fence", fence.Token())
	redis.NewPublisher(pubClient, fence, f.Subscribe(ctx)).Run(ctx)
}

func serve(ctx context.Context, srv *http.Server, h *hub.Hub, ln net.Listener, ready *Readiness) error {
	serverErr := make(chan error, 1)

	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// wait for either the server to exit (error) or ctx to be cancelled (shutdown).
	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
		ready.SetReady(false)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := srv.Shutdown(shutCtx)

	// Wait for the client pumps to drain, bounded by the shutdown deadline.
	drained := make(chan struct{})

	go func() { h.Wait(); close(drained) }()

	select {
	case <-drained: // all clients disconnected
	case <-shutCtx.Done(): // shutdown deadline hit, stop waiting regardless
	}

	return err
}

func newHTTPServer(cfg Config, r http.Handler) *http.Server {
	return &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           r,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
		WriteTimeout:      0, // unset: WebSocket connections are long-lived
	}
}

func buildRouter(apiHandler *handler, h *hub.Hub, ready *Readiness, status *Status) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.StripSlashes) // normalize paths before requestLogger reads them
	r.Use(requestLogger)
	r.Use(middleware.Recoverer)

	r.Get("/livez", livez)
	r.Get("/readyz", ready.readyz)
	r.Get("/status", status.handler)

	r.Handle("/metrics", promhttp.Handler())

	r.Get("/prices", apiHandler.getPrices)
	r.Get("/prices/{symbol}", apiHandler.getPrice)
	r.Get("/symbols", apiHandler.getSymbols)
	r.Handle("/ws/stream", h.Handler())

	return r
}

func logStarted(cfg Config) {
	slog.Info("marketfeed started",
		"port", cfg.Port,
		"tick_interval_ms", cfg.TickInterval.Milliseconds(),
		"volatility_multiplier", cfg.VolatilityMult,
		"symbols", len(feed.Symbols()),
		"redis", cfg.RedisURL != "",
	)
}

// pollReady polls cond every interval until true, then marks the service ready.
func pollReady(ctx context.Context, interval time.Duration, cond func() bool, ready *Readiness) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if cond() {
				ready.SetReady(true)
				return
			}
		}
	}
}
