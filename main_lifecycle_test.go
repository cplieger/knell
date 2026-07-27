package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cplieger/health"
	"github.com/cplieger/knell/internal/metrics"
	"github.com/cplieger/slogx/capture"
)

// stillRunningWarn is the message awaitWatchLoop logs when the watch loop
// outlives the shutdown grace, spelled out here so a reworded production line
// fails the tests that assert on it rather than silently matching nothing.
const stillRunningWarn = "watch loop still running at the end of the shutdown grace"

// waitForMarkerWithin blocks until the health marker reaches the wanted
// presence, or fails the test. A Stat error other than fs.ErrNotExist is a
// hard failure, never evidence of presence: a marker the probe cannot stat
// is not a healthy switch, and swallowing the error turned the oracle into
// "any error means absent".
func waitForMarkerWithin(t *testing.T, present bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := os.Stat(health.DefaultPath)
		switch {
		case err == nil && present:
			return
		case errors.Is(err, fs.ErrNotExist) && !present:
			return
		case err != nil && !errors.Is(err, fs.ErrNotExist):
			t.Fatalf("stat marker %s: %v", health.DefaultPath, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("marker %s presence never became %v within %s", health.DefaultPath, present, timeout)
}

// prepareLifecycleRun installs the one process-global contract every full
// run() test shares and returns the address run() will bind. It reserves and
// releases an ephemeral port (so the test knows the address before run()
// chooses it), installs a complete beat/webhook environment with the _FILE and
// token variables explicitly cleared (a leaked one from another test changes
// what run() reads), swaps in a fresh slog default that is restored at test
// end, and establishes marker ABSENCE before the boot: a marker left by a
// previous failed run or another knell process would satisfy a presence wait
// immediately and let a caller signal itself before run() installed its signal
// context. Callers keep their own done channel/start/wait sequence, so a test
// can still observe state (e.g. a boot floor) between this setup and run().
func prepareLifecycleRun(t *testing.T, beat string) string {
	t.Helper()
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot reserve a port: %v", err)
	}
	addr := free.Addr().String()
	if err := free.Close(); err != nil {
		t.Fatalf("releasing the reserved port: %v", err)
	}

	t.Setenv("BEATS", beat+":1m")
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/api/webhooks/1234567890/verysecrettoken")
	unsetEnv(t, "DISCORD_WEBHOOK_URL_FILE")
	unsetEnv(t, "BEAT_TOKEN")
	unsetEnv(t, "BEAT_TOKEN_FILE")
	t.Setenv("LISTEN_ADDR", addr)
	capture.Default(t)
	if err := os.Remove(health.DefaultPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Skipf("cannot clear health marker at %s: %v", health.DefaultPath, err)
	}
	t.Cleanup(func() { _ = os.Remove(health.DefaultPath) })
	return addr
}

// startHeldBeatRequest opens a beat request against a serving knell and stops
// with the handler already inside the body read, leaving the request in flight
// for the caller to complete or abandon. The Expect: 100-continue exchange is
// the oracle: net/http emits the interim response on the first Read of the
// body, so receiving it proves the request was accepted AND the handler
// entered, without sending any body bytes. The returned reader holds whatever
// the connection has buffered past the interim response, so a caller reading
// the final response must read through it rather than the raw conn. The read
// deadline set here covers only the handshake; callers clear or replace it.
func startHeldBeatRequest(t *testing.T, addr, beat string, contentLength int) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial serving knell: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	headers := "POST /beat/" + beat + " HTTP/1.1\r\nHost: " + addr +
		"\r\nContent-Length: " + strconv.Itoa(contentLength) + "\r\nExpect: 100-continue\r\n\r\n"
	if _, err := io.WriteString(conn, headers); err != nil {
		t.Fatalf("start slow beat request: %v", err)
	}
	reader := bufio.NewReader(conn)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set continue deadline: %v", err)
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read 100-continue response: %v", err)
	}
	if line != "HTTP/1.1 100 Continue\r\n" {
		t.Fatalf("continue status line = %q, want HTTP/1.1 100 Continue", line)
	}
	if line, err = reader.ReadString('\n'); err != nil || line != "\r\n" {
		t.Fatalf("continue terminator = %q, %v, want blank line", line, err)
	}
	return conn, reader
}

// TestRunTracksHealthMarkerAcrossServeAndDrain pins the full lifecycle of the
// marker — the only thing the baked `knell health` probe looks at — including
// the ordering the deferred marker.Cleanup hides: the marker becomes present
// once the listener is serving (a dropped marker.Set(true) would leave the
// container unhealthy forever while serving correctly), it becomes ABSENT
// while run() is still draining a real in-flight /beat request (a deleted or
// post-Shutdown marker.Set(false) would call a draining container healthy for
// the whole stop window), and a SIGTERM returns run() with a nil error so the
// container exits 0 instead of looking like a crash.
func TestRunTracksHealthMarkerAcrossServeAndDrain(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default, a
	// process-wide signal, and the shared health-marker path.
	addr := prepareLifecycleRun(t, "api")

	done := make(chan error, 1)
	go func() { done <- run() }()
	waitForMarkerWithin(t, true, 10*time.Second)

	// Hold one real request open across the shutdown so the interval between
	// pre-drain and the deferred cleanup becomes observable. Expect:
	// 100-continue proves the server accepted the request and is inside the
	// handler before the signal, without sending the body.
	conn, _ := startHeldBeatRequest(t, addr, "api", 1<<20)
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear continue deadline: %v", err)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	waitForMarkerWithin(t, false, time.Second)
	select {
	case err := <-done:
		t.Fatalf("run() returned before its active request drained: %v", err)
	default:
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("release slow beat request: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() = %v, want nil after the active request drained", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after the active request drained")
	}

	if _, err := os.Stat(health.DefaultPath); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("marker %s after shutdown: stat = %v, want it gone; a stopped switch must not report healthy",
			health.DefaultPath, err)
	}
}

// TestRunPublishesTheBootArmedBaselineFromProcessStart pins the one half of
// knell's boot-armed clock that main owns: the processStart baseline run()
// captures before config parsing and hands to watch.New, which publishes it
// as every beat's initial last-seen sample. If that argument degrades -
// dropped, zeroed, or replaced by a far-past value - every configured beat
// boots already overdue and the first sweep fires a false missing notice on
// every container restart, which is the exact failure the boot-armed clock
// exists to prevent. The watch package's own tests supply their own start
// value, so nothing outside main can catch it.
//
// The oracle is a range, so it pins that the published baseline is a real
// boot-window instant, not the exact instruction that captured it: moving the
// capture from run()'s entry to the watch.New call site shifts it by under a
// second and this test still passes. The degradation class it does catch is
// the one that breaks the switch - a dropped, zeroed, or far-past baseline.
func TestRunPublishesTheBootArmedBaselineFromProcessStart(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default, a
	// process-wide signal, and the shared health-marker path.
	addr := prepareLifecycleRun(t, "api")

	// Captured before the boot: the published baseline must sit between this
	// floor and the scrape. 2s of slack absorbs a slow bind on a loaded host
	// without admitting a zeroed or epoch baseline.
	bootFloor := time.Now().Add(-2 * time.Second).Unix()
	done := make(chan error, 1)
	go func() { done <- run() }()
	waitForMarkerWithin(t, true, 10*time.Second)

	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("scrape /metrics: %v", err)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("read /metrics: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close /metrics body: %v", err)
	}

	const series = `knell_beat_last_seen_timestamp_seconds{beat="api"}`
	baseline, found := sampleValue(t, string(body), series)
	if !found {
		t.Fatalf("%s missing from /metrics; a beat with no published baseline has no quorum signal at all:\n%s", series, body)
	}
	if ceiling := time.Now().Unix(); int64(baseline) < bootFloor || int64(baseline) > ceiling {
		t.Errorf("boot-armed baseline = %v, want it inside [%d, %d]; a baseline outside the boot window makes every beat overdue at startup and fires a false missing notice on every restart",
			baseline, bootFloor, ceiling)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() = %v, want nil after a shutdown signal", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after a shutdown signal")
	}
}

// sampleValue extracts the value of one "name{labels}" sample out of a
// Prometheus exposition, reporting whether the series was present at all.
func sampleValue(t *testing.T, exposition, series string) (float64, bool) {
	t.Helper()
	value, found := 0.0, false
	for line := range strings.SplitSeq(exposition, "\n") {
		rest, ok := strings.CutPrefix(line, series)
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			t.Fatalf("parsing %q: %v", line, err)
		}
		value, found = v, true
	}
	return value, found
}

// scrapeCounter reads one sample out of the process metrics registry by its
// full "name{labels}" prefix, in process: the registry is a package-level
// singleton, so it is still readable after run() has returned and the listener
// is gone.
func scrapeCounter(t *testing.T, series string) float64 {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("in-process /metrics = %d, want 200", rec.Code)
	}
	v, ok := sampleValue(t, rec.Body.String(), series)
	if !ok {
		t.Fatalf("%s missing from the exposition:\n%s", series, rec.Body.String())
	}
	return v
}

// TestBeatInFlightAtShutdownIsRefusedAndRecordsNothing pins the acceptance
// window end to end, through a real listener, a real SIGTERM and the real
// drain. It is the window srv.Shutdown keeps open BY DESIGN: an in-flight
// request is waited for, so a ping admitted while the app was still live can
// reach the state machine well after the shared context was cancelled — which
// is after watch.Run already returned, took its undelivered-work snapshot, and
// stopped reading the recovery channel. A ping recorded there is a signal
// accepted with no sender behind it: it moves lastSeen and
// knell_beats_received_total, republishes the beat as fresh, and can queue a
// recovered notice nobody will ever deliver, all of it invisible to the
// shutdown log that just reported zero undelivered work.
//
// The request here is admitted BEFORE the signal (the 100-continue proves the
// handler is already inside the body read) and completes AFTER it, so this can
// only pass if acceptance is closed on the far side of the body read too, not
// merely at handler entry.
func TestBeatInFlightAtShutdownIsRefusedAndRecordsNothing(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default, a
	// process-wide signal, and the shared health-marker path.
	//
	// A beat id no other test in this package pings, since the metrics
	// registry is a package-level singleton shared by the whole test binary.
	const beat = "drain-guard"
	addr := prepareLifecycleRun(t, beat)

	received := `knell_beats_received_total{beat="` + beat + `"}`
	done := make(chan error, 1)
	go func() { done <- run() }()
	waitForMarkerWithin(t, true, 10*time.Second)
	before := scrapeCounter(t, received)

	// Admit the ping while the app is live, then hold it inside the handler's
	// body read. Expect: 100-continue is the proof the handler was entered and
	// is reading: net/http emits it on the first Read of the body.
	conn, reader := startHeldBeatRequest(t, addr, beat, 1)

	// Cancel the shared context. The marker flip happens in the pre-drain hook,
	// which webhttp invokes only after cancellation, so waiting for the marker
	// to go makes the rest of this test deterministically post-cancellation.
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	waitForMarkerWithin(t, false, 5*time.Second)

	// Complete the body: the handler leaves its read and decides, now, whether
	// to record a ping the watch loop can no longer account for.
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set body write deadline: %v", err)
	}
	if _, err := io.WriteString(conn, "x"); err != nil {
		t.Fatalf("finish slow beat body: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set response deadline: %v", err)
	}
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("read beat response: %v", err)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		t.Fatalf("read beat response body: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close beat response body: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("in-flight ping completing during the drain = %d, want 503: a beat accepted after the watch loop stopped is a signal recorded with no sender behind it (body %s)",
			resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "shutting_down") {
		t.Errorf("refusal body = %s, want the shutting_down code", body)
	}
	if strings.Contains(string(body), beat) {
		t.Errorf("refusal body = %s, must not echo the beat id", body)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() = %v, want nil after the shutdown signal", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run() did not return after the drain")
	}

	if after := scrapeCounter(t, received); after != before {
		t.Errorf("%s = %v, want it unchanged at %v: the refused ping must not be counted, or the shutdown tally watch.Run already reported is stale",
			received, after, before)
	}
}

// TestRunGatesTheBeatEndpointWithTheConfiguredToken pins the one half of the
// beat-token gate main owns: the wiring of cfg.BeatToken into webapi.New. The
// webapi tests receive the token as an argument, so they cannot catch a
// composition root that passes "" (or the wrong field): the endpoint would be
// open to anyone who can reach the port while the operator's BEAT_TOKEN
// suggests it is gated, and every existing lifecycle test boots WITHOUT a token
// so an ungated boot looks identical to them.
func TestRunGatesTheBeatEndpointWithTheConfiguredToken(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default, a
	// process-wide signal, and the shared health-marker path.
	//
	// A beat id no other test in this package pings, since the metrics
	// registry is a package-level singleton shared by the whole test binary.
	const beat = "token-gate"
	const token = "unit-test-beat-token"
	addr := prepareLifecycleRun(t, beat)
	// After prepareLifecycleRun, which clears BEAT_TOKEN: this is the variable
	// under test.
	t.Setenv("BEAT_TOKEN", token)

	done := make(chan error, 1)
	go func() { done <- run() }()
	// Stop run() even when a t.Fatalf below skips the explicit SIGTERM: a
	// surviving run goroutine keeps the listener, the signal registration and
	// the shared marker alive into later tests, and would consume a later
	// test's process-wide SIGTERM.
	runStopped := false
	t.Cleanup(func() {
		if runStopped {
			return
		}
		select {
		case <-done:
			return
		default:
		}
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			t.Errorf("cleanup signal: %v", err)
			return
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("run() did not stop during cleanup")
		}
	})
	waitForMarkerWithin(t, true, 10*time.Second)

	url := "http://" + addr + "/beat/" + beat
	unauthorized := postBeat(t, url, "")
	if unauthorized.status != http.StatusUnauthorized {
		t.Errorf("unauthenticated ping = %d, want 401: an unwired token leaves the switch keepable by anyone who can reach the port (body %s)",
			unauthorized.status, unauthorized.body)
	}
	if strings.Contains(unauthorized.body, token) {
		t.Errorf("refusal body = %s, must not echo the configured token", unauthorized.body)
	}

	if wrong := postBeat(t, url, "Bearer not-the-token"); wrong.status != http.StatusUnauthorized {
		t.Errorf("ping with a wrong token = %d, want 401 (body %s)", wrong.status, wrong.body)
	}

	accepted := postBeat(t, url, "Bearer "+token)
	if accepted.status != http.StatusOK {
		t.Errorf("ping with the configured token = %d, want 200: the gate must admit the sender the operator configured (body %s)",
			accepted.status, accepted.body)
	}
	if !strings.Contains(accepted.body, `"ok":true`) {
		t.Errorf("accepted ping body = %s, want the ok acknowledgement", accepted.body)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	select {
	case err := <-done:
		runStopped = true
		if err != nil {
			t.Fatalf("run() = %v, want nil after a shutdown signal", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after a shutdown signal")
	}
}

// beatResponse is one /beat answer: the status and the body a failure message
// should quote.
type beatResponse struct {
	body   string
	status int
}

// postBeat pings url with the given Authorization header, sending none when
// auth is empty.
func postBeat(t *testing.T, url, auth string) beatResponse {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, nil)
	if err != nil {
		t.Fatalf("building beat request: %v", err)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("ping %s: %v", url, err)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		t.Fatalf("read beat response: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close beat response body: %v", err)
	}
	return beatResponse{status: resp.StatusCode, body: string(body)}
}

// TestAwaitWatchLoopStaysSilentWhenTheLoopStoppedInsideAnExpiredGrace pins the
// guard the teardown hook exists for. webhttp.Run derives the teardown context
// from the SAME deadline srv.Shutdown just spent, so a drain that used the whole
// grace hands the hook an already-expired context: with the loop already stopped
// AND the context already done, a single select over both would pick
// pseudo-randomly and log "watch loop still running" on about half of all
// ordinary shutdowns — a WARN whose whole purpose is to mean the sender's
// abandoned deliveries went unlogged, so a false one sends the operator hunting
// for lost notices that were never lost.
//
// The repetition is the oracle: collapsing the two selects into one is
// detected by a single call only half the time, so one call cannot pin the
// guard at all; 200 calls make the collapse practically certain to show up and
// still finish in microseconds.
func TestAwaitWatchLoopStaysSilentWhenTheLoopStoppedInsideAnExpiredGrace(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default.
	rec := capture.Default(t)

	stopped := make(chan struct{})
	close(stopped)
	expired, cancel := context.WithCancel(context.Background())
	cancel()

	for range 200 {
		awaitWatchLoop(expired, stopped)
	}

	if n := rec.CountLevel(slog.LevelWarn, stillRunningWarn); n != 0 {
		t.Errorf("%d spurious %q warnings over 200 teardowns with the loop already stopped, want 0: a stopped watch loop must never be reported as still running",
			n, stillRunningWarn)
	}
}

// TestAwaitWatchLoopWarnsOnceWhenTheLoopOutlivesTheGrace pins the WARN itself:
// when the grace is gone and the loop is still running, the abandoned
// deliveries never got logged, and this line is the operator's only trace that
// the shutdown tally is incomplete. Dropping it makes that loss silent.
func TestAwaitWatchLoopWarnsOnceWhenTheLoopOutlivesTheGrace(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default.
	rec := capture.Default(t)

	running := make(chan struct{}) // never closed: the loop outlives the grace
	expired, cancel := context.WithCancel(context.Background())
	cancel()

	awaitWatchLoop(expired, running)

	if n := rec.CountLevel(slog.LevelWarn, stillRunningWarn); n != 1 {
		t.Errorf("%q at WARN = %d, want exactly 1; messages = %v", stillRunningWarn, n, rec.Messages())
	}
}

// TestAwaitWatchLoopWaitsForALoopThatStopsInsideTheGrace pins that the hook
// actually WAITS: the single sender needs the window to finish abandoning its
// in-flight delivery so its log lines land instead of racing process exit. A
// hook that returned immediately would still pass both cases above, and the
// loss would only ever show up as missing log lines in production.
func TestAwaitWatchLoopWaitsForALoopThatStopsInsideTheGrace(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default.
	rec := capture.Default(t)

	const stopAfter = 50 * time.Millisecond
	stopping := make(chan struct{})
	teardownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	timer := time.AfterFunc(stopAfter, func() { close(stopping) })
	defer timer.Stop()

	awaitWatchLoop(teardownCtx, stopping)
	waited := time.Since(start)

	if waited < stopAfter {
		t.Errorf("awaitWatchLoop returned after %s, want at least %s: it must wait for the watch loop so the sender's abandoned-delivery lines land before exit",
			waited, stopAfter)
	}
	if rec.Contains(stillRunningWarn) {
		t.Errorf("messages = %v, want no still-running warning for a loop that stopped inside the grace", rec.Messages())
	}
}
