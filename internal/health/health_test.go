package health

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestState(staleAfter time.Duration) (*State, *fakeClock) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	state := NewState(staleAfter)
	state.now = clock.Now
	return state, clock
}

func TestNilStateMethodsAreNoOps(t *testing.T) {
	var state *State
	state.Progress()
	state.SetConnected(true)
	state.SetShuttingDown()
}

func TestLiveBeforeFirstProgress(t *testing.T) {
	state, clock := newTestState(time.Minute)
	clock.Advance(time.Hour)

	if ok, reason := state.Live(); !ok || reason != "starting" {
		t.Fatalf("Live() = %v, %q; want true, %q", ok, reason, "starting")
	}
}

func TestLiveTracksProgressStaleness(t *testing.T) {
	state, clock := newTestState(time.Minute)
	state.Progress()

	clock.Advance(time.Minute)
	if ok, reason := state.Live(); !ok {
		t.Fatalf("Live() at the limit = false (%q); want true", reason)
	}

	clock.Advance(time.Second)
	ok, reason := state.Live()
	if ok {
		t.Fatal("Live() past the limit = true; want false")
	}
	if !strings.Contains(reason, "no progress") {
		t.Errorf("Live() reason = %q; want it to mention missing progress", reason)
	}

	state.Progress()
	if ok, reason := state.Live(); !ok {
		t.Fatalf("Live() after new progress = false (%q); want true", reason)
	}
}

// A worker that cannot reach Oz keeps retrying, which counts as progress.
// Liveness must stay healthy so an Oz outage does not restart the fleet.
func TestLiveDoesNotDependOnConnection(t *testing.T) {
	state, clock := newTestState(time.Minute)
	state.SetConnected(false)

	for range 10 {
		clock.Advance(30 * time.Second)
		state.Progress()
		if ok, reason := state.Live(); !ok {
			t.Fatalf("Live() while disconnected = false (%q); want true", reason)
		}
		if ok, _ := state.Ready(); ok {
			t.Fatal("Ready() while disconnected = true; want false")
		}
	}
}

func TestLiveDuringShutdownIgnoresStaleness(t *testing.T) {
	state, clock := newTestState(time.Minute)
	state.Progress()
	state.SetShuttingDown()
	clock.Advance(time.Hour)

	if ok, reason := state.Live(); !ok || reason != "shutting down" {
		t.Fatalf("Live() = %v, %q; want true, %q", ok, reason, "shutting down")
	}
}

func TestReady(t *testing.T) {
	state, _ := newTestState(time.Minute)

	if ok, reason := state.Ready(); ok || reason != "not connected to Oz" {
		t.Fatalf("Ready() before connecting = %v, %q; want false, %q", ok, reason, "not connected to Oz")
	}

	state.SetConnected(true)
	if ok, reason := state.Ready(); !ok || reason != "ok" {
		t.Fatalf("Ready() while connected = %v, %q; want true, %q", ok, reason, "ok")
	}

	state.SetConnected(false)
	if ok, _ := state.Ready(); ok {
		t.Fatal("Ready() after disconnect = true; want false")
	}

	state.SetConnected(true)
	state.SetShuttingDown()
	if ok, reason := state.Ready(); ok || reason != "shutting down" {
		t.Fatalf("Ready() during shutdown = %v, %q; want false, %q", ok, reason, "shutting down")
	}
}

func TestHandlerStatusCodes(t *testing.T) {
	state, clock := newTestState(time.Minute)
	handler := state.Handler()

	get := func(path string) (int, string) {
		t.Helper()
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		return recorder.Code, strings.TrimSpace(recorder.Body.String())
	}

	if code, body := get(LivezPath); code != http.StatusOK || body != "starting" {
		t.Errorf("GET %s = %d %q; want 200 %q", LivezPath, code, body, "starting")
	}
	if code, body := get(ReadyzPath); code != http.StatusServiceUnavailable || body != "not connected to Oz" {
		t.Errorf("GET %s = %d %q; want 503 %q", ReadyzPath, code, body, "not connected to Oz")
	}

	state.Progress()
	state.SetConnected(true)
	if code, body := get(LivezPath); code != http.StatusOK || body != "ok" {
		t.Errorf("GET %s = %d %q; want 200 %q", LivezPath, code, body, "ok")
	}
	if code, body := get(ReadyzPath); code != http.StatusOK || body != "ok" {
		t.Errorf("GET %s = %d %q; want 200 %q", ReadyzPath, code, body, "ok")
	}

	clock.Advance(2 * time.Minute)
	if code, _ := get(LivezPath); code != http.StatusServiceUnavailable {
		t.Errorf("GET %s after stalled progress = %d; want 503", LivezPath, code)
	}
	// A stalled supervisor does not change the last known connection state;
	// the two endpoints are independent.
	if code, _ := get(ReadyzPath); code != http.StatusOK {
		t.Errorf("GET %s after stalled progress = %d; want 200", ReadyzPath, code)
	}

	if code, _ := get("/unknown"); code != http.StatusNotFound {
		t.Errorf("GET /unknown = %d; want 404", code)
	}
}

func TestServe(t *testing.T) {
	state := NewState(time.Minute)
	state.Progress()

	server, err := Serve("127.0.0.1:0", state)
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}

	resp, err := http.Get("http://" + server.Addr() + LivezPath)
	if err != nil {
		t.Fatalf("GET %s error = %v", LivezPath, err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != "ok" {
		t.Errorf("GET %s = %d %q; want 200 %q", LivezPath, resp.StatusCode, body, "ok")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if _, err := http.Get("http://" + server.Addr() + LivezPath); err == nil {
		t.Error("GET after Shutdown succeeded; want a connection error")
	}
}

func TestServeReportsBindErrors(t *testing.T) {
	state := NewState(time.Minute)
	first, err := Serve("127.0.0.1:0", state)
	if err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	defer func() {
		_ = first.Shutdown(context.Background())
	}()

	if _, err := Serve(first.Addr(), state); err == nil {
		t.Fatal("Serve() on an address already in use succeeded; want an error")
	}
	if _, err := Serve("127.0.0.1:0", nil); err == nil {
		t.Fatal("Serve() with a nil state succeeded; want an error")
	}
}
