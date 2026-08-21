package server

import (
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/nijatsdev/marketfeed/internal/metrics"
)

// skipPaths are excluded from request logs and latency metrics: poll noise,
// operator-facing /status, and long-lived /ws/stream connections.
var skipPaths = []string{"/livez", "/readyz", "/metrics", "/status", "/ws/stream"}

// skipObservability reports whether the path is exempt from logging and metrics.
// StripSlashes runs first, so the path is already normalized.
func skipObservability(path string) bool {
	return slices.Contains(skipPaths, path)
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipObservability(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		duration := time.Since(start)
		status := ww.Status()

		metrics.HTTPRequestDuration.WithLabelValues(methodLabel(r.Method), routePattern(r), strconv.Itoa(status)).Observe(duration.Seconds())

		slog.InfoContext(r.Context(), "request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", status,
			"duration", duration.String(),
		)
	})
}

var knownMethods = []string{
	http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
	http.MethodPatch, http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace,
}

// methodLabel bounds the Prometheus method label so clients can't mint new series.
func methodLabel(m string) string {
	if slices.Contains(knownMethods, m) {
		return m
	}

	return "OTHER"
}

// routePattern returns the matched chi route pattern (e.g. /prices/{symbol}),
// collapsing unmatched requests to "unmatched" to bound label cardinality.
func routePattern(r *http.Request) string {
	if rc := chi.RouteContext(r.Context()); rc != nil {
		if p := rc.RoutePattern(); p != "" {
			return p
		}
	}

	return "unmatched"
}
