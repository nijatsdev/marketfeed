// Package metrics defines the Prometheus metrics exported by the marketfeed service.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const labelSymbol = "symbol"

// Prometheus metrics registered at package init via promauto.
var (
	TicksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "marketfeed_ticks_total",
		Help: "Total price ticks published per symbol.",
	}, []string{labelSymbol})

	WSClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "marketfeed_ws_clients",
		Help: "Currently connected WebSocket clients.",
	})

	WSTicksDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "marketfeed_ws_ticks_dropped_total",
		Help: "Ticks dropped due to slow WebSocket clients.",
	})

	RedisPublishErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "marketfeed_redis_publish_errors_total",
		Help: "Errors publishing ticks to Redis.",
	})

	TicksFenced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "marketfeed_ticks_fenced_total",
		Help: "Ticks rejected at Redis because a newer leadership term owns the state.",
	})

	ElectionErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "marketfeed_election_errors_total",
		Help: "Redis errors during leader election (acquire, renewal, or release).",
	})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "marketfeed_http_request_duration_seconds",
		Help:    "HTTP request latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route", "status"})
)
