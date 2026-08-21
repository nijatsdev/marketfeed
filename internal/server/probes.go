package server

import (
	"net/http"
	"sync/atomic"
)

// Readiness tracks whether the service is ready to serve traffic.
type Readiness struct {
	ready atomic.Bool
}

// SetReady stores v as the readiness state.
func (r *Readiness) SetReady(v bool) {
	r.ready.Store(v)
}

// Ready reports whether the service is ready to serve traffic.
func (r *Readiness) Ready() bool {
	return r.ready.Load()
}

func (r *Readiness) readyz(w http.ResponseWriter, _ *http.Request) {
	if r.ready.Load() {
		w.WriteHeader(http.StatusOK)
		return
	}

	http.Error(w, "feed not ready", http.StatusServiceUnavailable)
}

func livez(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}
