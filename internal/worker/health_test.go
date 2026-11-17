package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/warpdotdev/oz-agent-worker/internal/health"
	"github.com/warpdotdev/oz-agent-worker/internal/types"
)

// waitForHealth polls check until it reports want or the timeout elapses.
func waitForHealth(t *testing.T, name string, check func() (bool, string), want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ok, reason := check()
		if ok == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s = %v (%q); want %v", name, ok, reason, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func newHealthTestWorker(ctx context.Context, wsURL string, state *health.State, heartbeatInterval time.Duration) *Worker {
	return &Worker{
		config: Config{
			WorkerID:     "test-worker",
			WebSocketURL: wsURL,
			Health:       state,
		},
		ctx:               ctx,
		outbound:          newOutboundQueue(8),
		activeTasks:       make(map[taskExecution]activeTask),
		oneShot:           newOneShotState(),
		backend:           &recordingBackend{},
		heartbeatInterval: heartbeatInterval,
	}
}

func TestRunReportsHealthAcrossConnectionLifecycle(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(rw, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	state := health.NewState(LivenessStaleAfter)
	if ok, reason := state.Live(); !ok || reason != "starting" {
		t.Fatalf("Live() before Run = %v, %q; want true, %q", ok, reason, "starting")
	}
	if ok, _ := state.Ready(); ok {
		t.Fatal("Ready() before Run = true; want false")
	}

	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	w := newHealthTestWorker(serverCtx, "ws"+strings.TrimPrefix(srv.URL, "http"), state, HeartbeatInterval)

	runResult := make(chan error, 1)
	go func() {
		runResult <- w.Run()
	}()

	waitForHealth(t, "Ready() after connecting", state.Ready, true)
	if ok, reason := state.Live(); !ok || reason != "ok" {
		t.Fatalf("Live() while connected = %v, %q; want true, %q", ok, reason, "ok")
	}

	cancelServer()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after server cancellation")
	}

	// The shutdown signal is delivered by context.AfterFunc on its own
	// goroutine, so allow it a moment to land.
	deadline := time.Now().Add(5 * time.Second)
	for {
		readyOK, readyReason := state.Ready()
		liveOK, liveReason := state.Live()
		if !readyOK && readyReason == "shutting down" && liveOK && liveReason == "shutting down" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after shutdown Ready() = %v, %q and Live() = %v, %q; want false and true with reason %q",
				readyOK, readyReason, liveOK, liveReason, "shutting down")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// An Oz outage must leave workers alive: a worker that keeps retrying the dial
// is healthy, and restarting it would not restore the connection.
func TestRunStaysLiveWhileOzIsUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	srv.Close()

	state := health.NewState(LivenessStaleAfter)
	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	w := newHealthTestWorker(serverCtx, wsURL, state, HeartbeatInterval)

	runResult := make(chan error, 1)
	go func() {
		runResult <- w.Run()
	}()

	// "ok" (rather than "starting") shows the supervisor reported progress.
	deadline := time.Now().Add(5 * time.Second)
	for {
		ok, reason := state.Live()
		if !ok {
			t.Fatalf("Live() while Oz is unreachable = false (%q); want true", reason)
		}
		if reason == "ok" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Live() reason = %q; want %q", reason, "ok")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ok, _ := state.Ready(); ok {
		t.Fatal("Ready() while Oz is unreachable = true; want false")
	}

	cancelServer()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after server cancellation")
	}
}

// If message handling blocks, the reader stops servicing the connection even
// though the process is alive and heartbeat pings still go out. Liveness must
// catch that, and recover once the reader is unblocked.
func TestLivenessFailsWhileReaderIsBlocked(t *testing.T) {
	data, err := json.Marshal(types.TaskCancellationMessage{TaskID: "unknown-task"})
	if err != nil {
		t.Fatalf("failed to marshal cancellation message: %v", err)
	}
	message, err := json.Marshal(types.WebSocketMessage{
		Type: types.MessageTypeTaskCancellation,
		Data: data,
	})
	if err != nil {
		t.Fatalf("failed to marshal websocket message: %v", err)
	}

	upgrader := websocket.Upgrader{}
	sendMessage := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(rw, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
		}()
		go func() {
			<-sendMessage
			_ = conn.WriteMessage(websocket.TextMessage, message)
		}()
		// Reading keeps the default ping handler replying with pongs.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	state := health.NewState(250 * time.Millisecond)
	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	w := newHealthTestWorker(serverCtx, "ws"+strings.TrimPrefix(srv.URL, "http"), state, 10*time.Millisecond)

	runResult := make(chan error, 1)
	go func() {
		runResult <- w.Run()
	}()
	waitForHealth(t, "Ready() after connecting", state.Ready, true)

	// Hold the lock that message handling needs, then deliver a message so the
	// reader blocks inside handleMessage.
	w.tasksMutex.Lock()
	close(sendMessage)
	waitForHealth(t, "Live() while the reader is blocked", state.Live, false)
	if ok, _ := state.Ready(); !ok {
		t.Error("Ready() while the reader is blocked = false; want the last known connection state")
	}

	w.tasksMutex.Unlock()
	waitForHealth(t, "Live() after the reader is unblocked", state.Live, true)

	cancelServer()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after server cancellation")
	}
}
