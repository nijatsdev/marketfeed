package server

import (
	"net/http"
	"sync/atomic"

	"github.com/nijatsdev/marketfeed/internal/feed"
	"github.com/nijatsdev/marketfeed/internal/hub"
)

// role values for Status.role, stored atomically; the zero value is standalone.
const (
	roleStandalone int32 = iota
	roleElecting
	roleLeader
	roleFollower
)

func roleName(r int32) string {
	switch r {
	case roleLeader:
		return "leader"
	case roleFollower:
		return "follower"
	case roleElecting:
		return "electing"
	default:
		return "standalone"
	}
}

// Status reports this instance's live state over /status. Values are per-instance: role and ws_clients describe the pod that served the request, not the cluster.
type Status struct {
	redisEnabled   bool
	tickIntervalMS int64            // global tick interval, in ms
	symbolMS       map[string]int64 // per-symbol tick interval overrides, in ms
	hub            *hub.Hub
	ready          *Readiness

	role  atomic.Int32
	fence atomic.Int64
}

// newStatus builds a Status; redisEnabled reflects whether a Redis URL was set.
func newStatus(cfg Config, h *hub.Hub, ready *Readiness, redisEnabled bool) *Status {
	s := &Status{
		redisEnabled:   redisEnabled,
		tickIntervalMS: cfg.TickInterval.Milliseconds(),
		symbolMS:       feed.SymbolIntervalsMS(),
		hub:            h,
		ready:          ready,
	}
	if redisEnabled {
		s.role.Store(roleElecting)
	}

	return s
}

// setLeader records that this instance won the election for the term with the given fence token.
func (s *Status) setLeader(fence int64) {
	s.role.Store(roleLeader)
	s.fence.Store(fence)
}

// setFollower records that this instance is following another leader.
func (s *Status) setFollower() {
	s.role.Store(roleFollower)
	s.fence.Store(0)
}

// setElecting records that this instance stepped down and is contending for the lock again.
func (s *Status) setElecting() {
	s.role.Store(roleElecting)
	s.fence.Store(0)
}

// statusResponse is the JSON returned by /status; the interval fields let clients scale expectations to the configured cadence.
type statusResponse struct {
	Redis             bool             `json:"redis"`
	Role              string           `json:"role"`
	Fence             int64            `json:"fence"`
	WSClients         int              `json:"ws_clients"`
	Ready             bool             `json:"ready"`
	TickIntervalMS    int64            `json:"tick_interval_ms"`
	SymbolIntervalsMS map[string]int64 `json:"symbol_intervals_ms,omitempty"`
}

func (s *Status) handler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{
		Redis:             s.redisEnabled,
		Role:              roleName(s.role.Load()),
		Fence:             s.fence.Load(),
		WSClients:         s.hub.ClientCount(),
		Ready:             s.ready.Ready(),
		TickIntervalMS:    s.tickIntervalMS,
		SymbolIntervalsMS: s.symbolMS,
	})
}
