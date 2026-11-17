// Package health exposes HTTP liveness and readiness endpoints for the worker.
//
// The two endpoints answer different questions and must stay independent:
//
//   - /livez reports whether the worker's connection supervisor is still making
//     progress. It deliberately does not depend on the connection to Oz: a
//     worker that is retrying a dial during a server-side outage is healthy and
//     restarting it would not help. Tying liveness to connectivity would let a
//     brief outage restart every self-hosted worker at once.
//   - /readyz reports whether the worker is currently connected to Oz and able
//     to receive task assignments.
package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

const (
	// LivezPath is the liveness endpoint path.
	LivezPath = "/livez"
	// ReadyzPath is the readiness endpoint path.
	ReadyzPath = "/readyz"

	readHeaderTimeout = 5 * time.Second
)

// State holds the health signals reported by the worker. It is safe for
// concurrent use. A nil *State is valid and all of its methods are no-ops, so
// callers do not need to check whether the health server is enabled.
type State struct {
	staleAfter time.Duration
	now        func() time.Time

	// lastProgress is the Unix nanosecond timestamp of the most recent
	// Progress call. Zero means the supervisor has not started yet.
	lastProgress atomic.Int64
	connected    atomic.Bool
	shuttingDown atomic.Bool
}

// NewState returns a State whose liveness check fails once the supervisor has
// gone longer than staleAfter without reporting progress. staleAfter must
// exceed the longest gap the supervisor can legitimately leave between
// Progress calls.
func NewState(staleAfter time.Duration) *State {
	return &State{staleAfter: staleAfter, now: time.Now}
}

// Progress records that the connection supervisor is still advancing.
func (s *State) Progress() {
	if s == nil {
		return
	}
	s.lastProgress.Store(s.now().UnixNano())
}

// SetConnected records whether the worker holds a live connection to Oz.
func (s *State) SetConnected(connected bool) {
	if s == nil {
		return
	}
	s.connected.Store(connected)
}

// SetShuttingDown records that graceful shutdown has begun. Shutdown may
// legitimately block while tasks drain, so liveness stops tracking progress
// and readiness fails from this point on.
func (s *State) SetShuttingDown() {
	if s == nil {
		return
	}
	s.shuttingDown.Store(true)
}

// Live reports whether the worker should be left running, with a short reason.
func (s *State) Live() (bool, string) {
	if s.shuttingDown.Load() {
		return true, "shutting down"
	}
	last := s.lastProgress.Load()
	if last == 0 {
		// Startup (backend construction, preflight checks) is bounded by its
		// own timeouts; do not restart the worker before it begins running.
		return true, "starting"
	}
	if idle := s.now().Sub(time.Unix(0, last)); idle > s.staleAfter {
		return false, fmt.Sprintf("no progress for %s (limit %s)", idle.Round(time.Second), s.staleAfter)
	}
	return true, "ok"
}

// Ready reports whether the worker can currently receive tasks, with a short
// reason.
func (s *State) Ready() (bool, string) {
	if s.shuttingDown.Load() {
		return false, "shutting down"
	}
	if !s.connected.Load() {
		return false, "not connected to Oz"
	}
	return true, "ok"
}

// Handler returns an http.Handler serving LivezPath and ReadyzPath.
func (s *State) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(LivezPath, func(rw http.ResponseWriter, _ *http.Request) {
		writeStatus(rw, s.Live)
	})
	mux.HandleFunc(ReadyzPath, func(rw http.ResponseWriter, _ *http.Request) {
		writeStatus(rw, s.Ready)
	})
	return mux
}

func writeStatus(rw http.ResponseWriter, check func() (bool, string)) {
	ok, reason := check()
	rw.Header().Set("Content-Type", "text/plain; charset=utf-8")
	rw.Header().Set("Cache-Control", "no-store")
	if !ok {
		rw.WriteHeader(http.StatusServiceUnavailable)
	}
	_, _ = io.WriteString(rw, reason+"\n")
}

// Server is a running health HTTP server.
type Server struct {
	server   *http.Server
	listener net.Listener
	done     chan error
}

// Serve starts an HTTP server for state on addr (for example ":8080"). It
// binds synchronously so that an unusable address is reported to the caller
// instead of surfacing later as failing probes.
func Serve(addr string, state *State) (*Server, error) {
	if state == nil {
		return nil, errors.New("health state is required")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on health address %q: %w", addr, err)
	}
	srv := &Server{
		server: &http.Server{
			Handler:           state.Handler(),
			ReadHeaderTimeout: readHeaderTimeout,
		},
		listener: listener,
		done:     make(chan error, 1),
	}
	go func() {
		err := srv.server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		srv.done <- err
	}()
	return srv, nil
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// Shutdown stops the server, waiting for in-flight requests until ctx ends.
func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.server.Shutdown(ctx); err != nil {
		return err
	}
	return <-s.done
}
