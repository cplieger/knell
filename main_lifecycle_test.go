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
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
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
// context. It is deliberately separate from startLifecycleRun, so a test can
// still observe state (e.g. a boot floor) between this setup and the boot.
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

	installRunEnv(t, beat+":1m", addr)
	capture.Default(t)
	if err := os.Remove(health.DefaultPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Skipf("cannot clear health marker at %s: %v", health.DefaultPath, err)
	}
	t.Cleanup(func() { _ = os.Remove(health.DefaultPath) })
	return addr
}

// lifecycleRun is one background run() under test: the channel its return
// value lands on, plus whether the test already collected it.
type lifecycleRun struct {
	done    chan error
	stopped bool
}

// startLifecycleRun boots run() in the background and registers the guard
// every full-lifecycle test needs: a t.Fatalf between the boot and the test's
// own SIGTERM leaves run() serving, and a surviving run reacts to the NEXT
// test's self-signal by flipping the SHARED health marker under it — so one
// real failure makes a later test's marker oracle attributable to the wrong
// process. Both registrations receive a process-wide SIGTERM, so a surviving
// run steals nothing; the damage is the interference.
func startLifecycleRun(t *testing.T) *lifecycleRun {
	t.Helper()
	r := &lifecycleRun{done: make(chan error, 1)}
	go func() { r.done <- run() }()
	t.Cleanup(func() {
		if r.stopped {
			return
		}
		// Arm our own SIGTERM guard BEFORE checking whether run has stopped:
		// run() can have returned (its deferred stop() already unregistering
		// its NotifyContext) without the wrapper goroutine having reached
		// `r.done <- run()` yet. In that window the nonblocking receive below
		// sees nothing, and a SIGTERM with no handler registered takes the
		// process's default termination path — killing the test binary mid-run
		// instead of stopping run(), with no failure report.
		signalGuard := make(chan os.Signal, 1)
		signal.Notify(signalGuard, syscall.SIGTERM)
		defer signal.Stop(signalGuard)

		select {
		case <-r.done:
			return
		default:
		}
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			t.Errorf("cleanup signal: %v", err)
			return
		}
		select {
		case <-r.done:
		case <-time.After(10 * time.Second):
			t.Error("run() did not stop during cleanup")
		}
	})
	return r
}

// waitForReturn waits for run() to return within timeout. after names the
// event the return is expected after, so a stalled path names itself.
func (r *lifecycleRun) waitForReturn(t *testing.T, timeout time.Duration, after string) {
	t.Helper()
	select {
	case err := <-r.done:
		r.stopped = true
		if err != nil {
			t.Fatalf("run() = %v, want nil %s", err, after)
		}
	case <-time.After(timeout):
		t.Fatalf("run() did not return %s", after)
	}
}

// startHeldBeatRequest opens a beat request against a serving knell and stops
// with the handler already inside the body read, leaving the request in flight
// for the caller to complete or abandon. It presents the required credential:
// the gate runs before the body is touched, so an unauthenticated request would
// answer 401 and never reach the handler this helper exists to hold open. The
// Expect: 100-continue exchange is
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
		"\r\nAuthorization: Bearer " + testBeatToken +
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

	r := startLifecycleRun(t)
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
	case err := <-r.done:
		r.stopped = true
		t.Fatalf("run() returned before its active request drained: %v", err)
	default:
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("release slow beat request: %v", err)
	}
	r.waitForReturn(t, 10*time.Second, "after the active request drained")

	if _, err := os.Stat(health.DefaultPath); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("marker %s after shutdown: stat = %v, want it gone; a stopped switch must not report healthy",
			health.DefaultPath, err)
	}
}

// TestRunPublishesTheBootArmedBaselineFromProcessStart pins that every first
// deadline counts from BEFORE configuration, so the time spent reading a
// mounted secret is charged to the deadline rather than to the sender. A FIFO
// holds config.Load across a whole-second boundary because the published
// last-seen metric intentionally has second precision.
//
// The oracle's reach is the config boundary, not the exact instruction: a
// capture moved anywhere earlier than config.Load - to the slogx.Setup or
// marker-probe lines - still lands on the process-start side and still passes.
// Covering the marker probe too would need a second controllable delay at
// health.DefaultPath, which is a fixed path this test cannot gate.
//
// This is the one half of knell's boot-armed clock that main owns: the
// processStart baseline run() captures BEFORE config parsing and hands to
// watch.New, which publishes it as every beat's initial last-seen sample. If
// that argument degrades - dropped, zeroed, replaced by a far-past value, or
// re-captured after configuration - the first deadline no longer covers the
// whole boot, and in the degraded-baseline cases every configured beat boots
// already overdue and the first sweep fires a false missing notice on every
// container restart. The watch package's own tests supply their own start
// value, so nothing outside main can catch it.
func TestRunPublishesTheBootArmedBaselineFromProcessStart(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default, a
	// process-wide signal, and the shared health-marker path.
	addr := prepareLifecycleRun(t, "api")

	fifo := t.TempDir() + "/webhook"
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("cannot create a FIFO to delay configuration loading: %v", err)
	}
	t.Setenv("DISCORD_WEBHOOK_URL_FILE", fifo)
	bootFloor := time.Now().Add(-2 * time.Second)
	r := startLifecycleRun(t)

	var secret *os.File
	openDeadline := time.Now().Add(5 * time.Second)
	for secret == nil {
		f, err := os.OpenFile(fifo, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		switch {
		case err == nil:
			secret = f
		case errors.Is(err, syscall.ENXIO):
			select {
			case runErr := <-r.done:
				r.stopped = true
				t.Fatalf("run() returned before reading the delayed webhook secret: %v", runErr)
			default:
			}
			if time.Now().After(openDeadline) {
				t.Fatal("run() never started reading the delayed webhook secret")
			}
			time.Sleep(5 * time.Millisecond)
		default:
			t.Fatalf("opening delayed webhook secret: %v", err)
		}
	}
	t.Cleanup(func() { _ = secret.Close() })

	// The FIFO writer can open only after config.Load has opened its reader.
	// Hold that read across this boundary so a baseline captured after config
	// cannot fall on the process-start side of it.
	configBoundary := time.Now().Truncate(time.Second).Add(time.Second)
	time.Sleep(time.Until(configBoundary) + 10*time.Millisecond)
	if _, err := secret.WriteString(testWebhookURL + "\n"); err != nil {
		t.Fatalf("writing delayed webhook secret: %v", err)
	}
	if err := secret.Close(); err != nil {
		t.Fatalf("closing delayed webhook secret: %v", err)
	}

	waitForMarkerWithin(t, true, 10*time.Second)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/metrics", nil)
	if err != nil {
		t.Fatalf("building the /metrics request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
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
	floorSeconds := float64(bootFloor.UnixNano()) / float64(time.Second)
	boundarySeconds := float64(configBoundary.UnixNano()) / float64(time.Second)
	if baseline < floorSeconds || baseline >= boundarySeconds {
		t.Errorf("boot-armed baseline = %.9f, want it before the delayed configuration boundary %.9f: capturing it after config shortens every beat's first deadline by the secret-read delay", baseline, boundarySeconds)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	r.waitForReturn(t, 10*time.Second, "after a shutdown signal")
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
	r := startLifecycleRun(t)
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

	r.waitForReturn(t, 15*time.Second, "after the drain")

	if after := scrapeCounter(t, received); after != before {
		t.Errorf("%s = %v, want it unchanged at %v: the refused ping must not be counted, or the shutdown tally watch.Run already reported is stale",
			received, after, before)
	}
}

// TestRunGatesTheBeatEndpointWithTheConfiguredToken pins the one half of the
// beat-token gate main owns: the wiring of cfg.BeatToken into webapi.New. The
// webapi tests receive the token as an argument, so they cannot catch a
// composition root that passes the wrong field — the gate would then refuse the
// operator's real senders while admitting whatever value was wired instead, and
// one deadline later every configured beat would post a false MISSING notice.
// This test pins the whole verdict set against the value the environment
// actually carried: refused unauthenticated, refused wrong, admitted correct.
func TestRunGatesTheBeatEndpointWithTheConfiguredToken(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default, a
	// process-wide signal, and the shared health-marker path.
	//
	// A beat id no other test in this package pings, since the metrics
	// registry is a package-level singleton shared by the whole test binary.
	const beat = "token-gate"
	// A token no other boot in this package uses, so the gate cannot pass by
	// admitting the value installRunEnv installs for every other test.
	const token = "lifecycle-token-gate-token"
	addr := prepareLifecycleRun(t, beat)
	// prepareLifecycleRun installs the shared testBeatToken; this overrides it
	// with the value under test.
	t.Setenv("BEAT_TOKEN", token)

	r := startLifecycleRun(t)
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
	r.waitForReturn(t, 10*time.Second, "after a shutdown signal")
}

// TestRunAppliesConfiguredHostAllowlist pins the other half main owns: the
// wiring of cfg.AllowedHosts into webapi.New. The config suite pins env-to-
// policy parsing and the webapi suite receives the policy as an argument, so
// neither can catch a composition root that drops the field — every endpoint
// would stay reachable through an attacker-controlled Host while startup still
// reports allowlist(1).
func TestRunAppliesConfiguredHostAllowlist(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default, a
	// process-wide signal, and the shared health-marker path.
	addr := prepareLifecycleRun(t, "host-gate")
	// Every other lifecycle boot leaves ALLOWED_HOSTS unset (the policy is
	// then inactive and admits any Host), so this is the variable under test.
	t.Setenv("ALLOWED_HOSTS", "knell.internal")

	r := startLifecycleRun(t)
	waitForMarkerWithin(t, true, 10*time.Second)

	blockedStatus, blockedBody := getHealthzAsHost(t, addr, "attacker.example")
	if blockedStatus != http.StatusForbidden {
		t.Errorf("request with an unlisted Host = %d, want 403: an unwired ALLOWED_HOSTS policy leaves the DNS-rebinding guard disabled (body %s)", blockedStatus, blockedBody)
	}
	if !strings.Contains(blockedBody, "host_not_allowed") {
		t.Errorf("blocked-host body = %s, want the host_not_allowed code", blockedBody)
	}

	if allowedStatus, allowedBody := getHealthzAsHost(t, addr, "knell.internal"); allowedStatus != http.StatusOK {
		t.Errorf("request with the configured Host = %d, want 200: the allowlist must admit the hostname the operator configured (body %s)", allowedStatus, allowedBody)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	r.waitForReturn(t, 10*time.Second, "after a shutdown signal")
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
	// Both figures carry the diagnosis the line exists for: without "grace"
	// nothing names the constant to raise, and without "waited" the line cannot
	// distinguish a drain that ate the whole budget (waited near zero, the loop
	// never got a chance) from a wedged loop (waited near the grace), so it
	// reads as an accusation against the loop either way.
	if !rec.HasAttr(stillRunningWarn, "grace", shutdownGrace.String()) {
		t.Errorf("still-running warning omits grace=%s: nothing then names the constant an operator would raise; records = %v", shutdownGrace, rec.Records())
	}
	if _, reported := rec.AttrValue(stillRunningWarn, "waited"); !reported {
		t.Errorf("still-running warning omits the waited attr: the line cannot then tell a drain that consumed the whole budget from a wedged watch loop; records = %v", rec.Records())
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

	synctest.Test(t, func(t *testing.T) {
		const stopAfter = 50 * time.Millisecond
		stopping := make(chan struct{})
		teardownCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		start := time.Now()
		timer := time.AfterFunc(stopAfter, func() { close(stopping) })
		defer timer.Stop()

		awaitWatchLoop(teardownCtx, stopping)

		if waited := time.Since(start); waited != stopAfter {
			t.Errorf("awaitWatchLoop waited %s, want %s: it must wait for the watch loop so the sender's abandoned-delivery lines land before exit",
				waited, stopAfter)
		}
	})

	if rec.CountLevel(slog.LevelWarn, stillRunningWarn) != 0 {
		t.Errorf("messages = %v, want no still-running warning for a loop that stopped inside the grace", rec.Messages())
	}
}

// TestTeardownAfterServeExitMarksUnhealthyThenCancelsAndWaits pins the
// non-graceful teardown: webhttp skips the drain hooks when Serve returns
// before a signal, so this path is the only thing that stops a dead process
// from reporting itself healthy and the only thing that cancels the watch
// loop. A dropped marker flip leaves `knell health` calling a container with
// no listener healthy until it is killed; a missing stop() leaves the watcher
// alerting behind a server that no longer answers, and makes the teardown burn
// the whole grace on a loop nobody asked to stop.
//
// The stop func closes watcherDone: the wait can only finish if cancellation
// happened FIRST, so a teardown that waits before cancelling fails here
// instead of only being slow in production.
func TestTeardownAfterServeExitMarksUnhealthyThenCancelsAndWaits(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default.
	rec := capture.Default(t)

	marker := health.NewMarker(t.TempDir() + "/healthy")
	marker.Set(true)
	if !marker.Healthy() {
		t.Skip("cannot plant a health marker in the test temp dir")
	}

	exitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	watcherDone := make(chan struct{})
	stopCalls := 0
	stop := func() {
		stopCalls++
		close(watcherDone)
	}

	start := time.Now()
	teardownAfterServeExit(exitCtx, marker, stop, watcherDone)
	waited := time.Since(start)

	if stopCalls != 1 {
		t.Errorf("stop() called %d times, want exactly 1: an uncancelled watcher keeps alerting behind a server that stopped serving", stopCalls)
	}
	if marker.Healthy() {
		t.Error("marker still healthy after the serve loop exited: the baked `knell health` probe would report a container with no listener as healthy")
	}
	if !rec.Contains("serve loop exited, tearing down") {
		t.Errorf("messages = %v, want the non-graceful exit named; without it the watch loop's abandoned-delivery lines are read before anything says why", rec.Messages())
	}
	if n := rec.CountLevel(slog.LevelWarn, stillRunningWarn); n != 0 {
		t.Errorf("%q logged %d times, want 0: the loop stopped inside the grace", stillRunningWarn, n)
	}
	if waited > time.Second {
		t.Errorf("teardown took %s, want it to cancel the watch loop before waiting for it", waited)
	}
}

// TestClassifyAbandonedWatchLoopDeniesACleanExitOverAnAbandonedLoop pins run's
// exit contract for the teardown verdict: an error already in hand names the
// earlier failure and wins, a loop that stopped keeps the stop clean, and a nil
// serve error over an unstopped loop must NOT read as a clean stop -- that exit 0
// would sit on top of the still-running WARN and vouch for notices the loop was
// still holding.
func TestClassifyAbandonedWatchLoopDeniesACleanExitOverAnAbandonedLoop(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("serve failed")
	if got := classifyAbandonedWatchLoop(sentinel, false); !errors.Is(got, sentinel) {
		t.Errorf("error in hand = %v, want the sentinel to win: it names an earlier failure of the same sequence", got)
	}
	if got := classifyAbandonedWatchLoop(nil, true); got != nil {
		t.Errorf("clean stop = %v, want nil: a stopped loop must not fail the exit", got)
	}
	got := classifyAbandonedWatchLoop(nil, false)
	if got == nil {
		t.Fatal("nil over an unstopped loop, want an error: exit 0 would claim a clean stop over abandoned notices")
	}
	if !strings.Contains(got.Error(), shutdownGrace.String()) {
		t.Errorf("abandonment error %q does not name the grace constant an operator would raise", got)
	}
}

// TestNewServerBoundsWholeRequestsAndRoutesConnectionErrorsThroughSlog pins the
// two server wirings nothing else in the suite can see. Both are silent when
// dropped: without WithReadTimeout/WithWriteTimeout a trickled body or a client
// that never reads /metrics holds a handler goroutine for as long as it likes
// (webhttp leaves both unset by default, bounding only the headers), and
// without WithSlogErrorLog net/http's own connection-level lines -- above all
// "http: Accept error: ...; retrying", the trace of an exhausted fd budget that
// stops every beat from being received -- fall back to the standard logger as
// unstructured, level-less text no level-based rule can match.
//
// The timeout halves assert the configured bound rather than a real trickle:
// requestTimeout is 30s, so the behavioral version of this test would have to
// wait that long. The error-log half is behavioral -- the bridge is driven and
// the resulting record's LEVEL is the assertion.
func TestNewServerBoundsWholeRequestsAndRoutesConnectionErrorsThroughSlog(t *testing.T) {
	// Serial (no t.Parallel): webhttp.WithSlogErrorLog resolves slog.Default()
	// as NewServer applies it, so the capture must be the default first.
	rec := capture.Default(t)

	srv := newServer(http.NotFoundHandler())

	if srv.ReadTimeout != requestTimeout {
		t.Errorf("ReadTimeout = %s, want %s: webhttp leaves it unset by default, and an unbounded read lets a trickled beat body hold a handler goroutine indefinitely", srv.ReadTimeout, requestTimeout)
	}
	if srv.WriteTimeout != requestTimeout {
		t.Errorf("WriteTimeout = %s, want %s: an unbounded write lets a client that requests /metrics and never reads the response pin the goroutine in Write", srv.WriteTimeout, requestTimeout)
	}
	if srv.ErrorLog == nil {
		t.Fatal("ErrorLog is nil, so net/http's connection-level errors go to the standard logger: an accept failure -- a whole-service outage here -- arrives as an unstructured, level-less line no log rule matches")
	}

	srv.ErrorLog.Print("http: Accept error: accept tcp [::]:9190: too many open files; retrying in 5ms")

	if n := rec.CountLevel(slog.LevelError, "Accept error"); n != 1 {
		t.Errorf("accept-error lines at ERROR = %d, want 1: knell's only job is answering pings, so an accept failure is an outage rather than a degradation; records = %v", n, rec.Records())
	}
}

// TestRunServesTheMarkerVerdictOnHealthz pins the composition root's wiring of
// health.Handler(marker) into webapi.Deps.Healthz -- the HTTP liveness
// endpoint the README documents and external monitoring probes. The webapi
// tests inject staticHealthz, a fixed verdict, so they cannot catch a root that
// wires the wrong handler here: with metrics.Handler in that slot /healthz
// answers 200 forever and an unhealthy switch is never reported, and with a nil
// or absent route it answers 404/503 forever and a live switch is paged as
// down. Removing the marker mid-run is what makes this a real oracle rather
// than a 200 check: the endpoint has to FOLLOW the marker, which is what
// Handler(marker) means.
func TestRunServesTheMarkerVerdictOnHealthz(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default, a
	// process-wide signal, and the shared health-marker path.
	//
	// A beat id no other test in this package pings, since the metrics
	// registry is a package-level singleton shared by the whole test binary.
	addr := prepareLifecycleRun(t, "healthz-probe")

	r := startLifecycleRun(t)
	waitForMarkerWithin(t, true, 10*time.Second)

	if status, body := getHealthz(t, addr); status != http.StatusOK || !strings.Contains(body, `"status":"OK"`) {
		t.Errorf("/healthz on a serving switch = %d %s, want 200 with the OK verdict; a root that wires the wrong handler here reports liveness that is not the marker's",
			status, body)
	}

	// Drop the marker under the running server: Marker.Healthy stats the file
	// per call, so a handler actually backed by it must now refuse.
	if err := os.Remove(health.DefaultPath); err != nil {
		t.Fatalf("removing the marker under the serving switch: %v", err)
	}
	if status, body := getHealthz(t, addr); status != http.StatusServiceUnavailable {
		t.Errorf("/healthz with the marker gone = %d %s, want 503: an endpoint that keeps answering healthy without the marker cannot report an unhealthy switch at all",
			status, body)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	r.waitForReturn(t, 10*time.Second, "after a shutdown signal")
}

// getHealthz probes the liveness endpoint of a serving knell, returning the
// status and the body a failure message should quote.
func getHealthz(t *testing.T, addr string) (int, string) {
	t.Helper()
	return getHealthzAsHost(t, addr, "")
}

// getHealthzAsHost probes the liveness endpoint with an explicit Host header,
// so the allowlist cases exercise the guard through the same bounded body read
// and close check every other /healthz probe uses. An empty host leaves the
// request's own Host untouched.
func getHealthzAsHost(t *testing.T, addr, host string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/healthz", nil)
	if err != nil {
		t.Fatalf("building healthz request: %v", err)
	}
	if host != "" {
		req.Host = host
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("probe /healthz: %v", err)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		t.Fatalf("read /healthz body: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close /healthz body: %v", err)
	}
	return resp.StatusCode, string(body)
}
