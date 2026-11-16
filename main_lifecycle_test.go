package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cplieger/go/health/v2"
	"github.com/cplieger/go/httpserver"
	"github.com/cplieger/go/slogx/capture"
	"github.com/cplieger/knell/internal/config"
	"github.com/cplieger/knell/internal/obs"
)

// stillRunningWarn is the message awaitWatchLoop logs when the watch loop
// outlives the shutdown grace, spelled out here so a reworded production line
// fails the tests that assert on it rather than silently matching nothing.
const stillRunningWarn = "watch loop still running at the end of the shutdown grace"

// waitForHealthyWithin blocks until /healthz at addr answers 200, or fails
// the test. A refused connection is not yet serving, so it keeps polling.
func waitForHealthyWithin(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("/healthz at %s never answered 200 within %s", addr, timeout)
}

// drainLine is the line the health verdict logs when shutdown turns it
// unhealthy: the signal that the drain has begun.
const drainLine = `msg="health state changed" healthy=false draining=true`

// waitForLogWithin blocks until logs holds line, or fails the test.
func waitForLogWithin(t *testing.T, logs *runLog, line string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), line) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no %q line within %s; log:\n%s", line, timeout, logs.String())
}

// prepareLifecycleRun installs the one process-global contract every full
// run() test shares and returns the ping and operations addresses run() will
// bind. It reserves and releases two ephemeral ports (so the test knows the
// addresses before run() chooses them), installs a complete beat/webhook environment with the _FILE and
// token variables explicitly cleared (a leaked one from another test changes
// what run() reads), swaps in a fresh slog default that is restored at test
// end. It is deliberately separate from startLifecycleRun, so a test can
// still observe state (e.g. a boot floor) between this setup and the boot.
func prepareLifecycleRun(t *testing.T, beat string) (addr, opsAddr string) {
	t.Helper()
	addr, opsAddr = unusedLoopbackAddr(t), unusedLoopbackAddr(t)
	installRunEnvOps(t, beat+":1m", addr, opsAddr)
	capture.Default(t)
	return addr, opsAddr
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
// test's self-signal too, so one real failure makes a later test's oracle
// attributable to the wrong process. Both registrations receive a process-wide SIGTERM, so a surviving
// run steals nothing; the damage is the interference.
func startLifecycleRun(t *testing.T) *lifecycleRun {
	t.Helper()
	r := &lifecycleRun{done: make(chan error, 1)}
	go func() { r.done <- run(nil) }()
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

// startHeldBeatRequest opens an authenticated beat request against a serving
// knell and returns once the handler is inside the body read, leaving it in
// flight. The 100-continue response is the proof, since net/http sends it on
// the body's first Read. Read the final response through the returned reader,
// which holds what the connection buffered past the interim response; the read
// deadline set here covers only the handshake.
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

// TestRunTracksHealthAcrossServeAndDrain pins the verdict /healthz reports to
// `knell health` across a run: healthy once the listener is serving (a dropped
// Set(true) would leave the container unhealthy forever while serving
// correctly), unhealthy while run() is still draining a real in-flight /beat
// request (a missing or post-Shutdown BeginDrain would call a draining
// container healthy for the whole stop window), and a SIGTERM returns run()
// with a nil error so the container exits 0 instead of looking crashed.
func TestRunTracksHealthAcrossServeAndDrain(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default and
	// os.Stderr, and a process-wide signal.
	addr, opsAddr := prepareLifecycleRun(t, "api")
	logs := captureRunLog(t)

	r := startLifecycleRun(t)
	waitForHealthyWithin(t, opsAddr, 10*time.Second)

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
	waitForLogWithin(t, logs, drainLine, time.Second)
	select {
	case err := <-r.done:
		r.stopped = true
		t.Fatalf("run() returned before its active request drained: %v", err)
	default:
	}
	// The operations listener outlives the ping drain, so a probe during it
	// gets an answer, 503, and never a refused connection.
	if status, body := getHealthz(t, opsAddr); status != http.StatusServiceUnavailable || !strings.Contains(body, notServingReason) {
		t.Errorf("/healthz on the ops port during the ping drain = %d %q, want 503 naming %q: the operations listener must stop only after the ping listener has drained", status, body, notServingReason)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("release slow beat request: %v", err)
	}
	r.waitForReturn(t, 10*time.Second, "after the active request drained")
}

// TestRunPublishesTheBootArmedBaselineFromProcessStart pins that run captures
// processStart before config.Load and hands it to watch.New, so every first
// deadline covers the whole boot, mounted-secret reads included. A baseline
// dropped, zeroed or re-captured after configuration boots every beat overdue
// and fires a false missing notice on each restart; watch's own tests pass
// their own start, so only main can catch it. The loadConfig seam holds
// config.Load across a second boundary because last-seen has second precision.
func TestRunPublishesTheBootArmedBaselineFromProcessStart(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default, a
	// process-wide signal.
	_, opsAddr := prepareLifecycleRun(t, "api")

	entered := make(chan struct{})
	held := make(chan struct{})
	release := sync.OnceFunc(func() { close(held) })
	realLoad := loadConfig
	loadConfig = func(maxNodeNameBytes int, opts ...httpserver.HostAllowlistOption) (config.Config, error) {
		close(entered)
		<-held
		return realLoad(maxNodeNameBytes, opts...)
	}
	t.Cleanup(func() { loadConfig = realLoad })
	bootFloor := time.Now().Add(-2 * time.Second)
	r := startLifecycleRun(t)
	// Registered after startLifecycleRun so it runs first: a held load would
	// keep run() from ever reaching the signal that cleanup sends.
	t.Cleanup(release)

	select {
	case <-entered:
	case runErr := <-r.done:
		r.stopped = true
		t.Fatalf("run() returned before loading configuration: %v", runErr)
	case <-time.After(5 * time.Second):
		t.Fatal("run() never started loading configuration")
	}

	// Hold configuration loading across this boundary so a baseline captured
	// after config cannot fall on the process-start side of it.
	configBoundary := time.Now().Truncate(time.Second).Add(time.Second)
	time.Sleep(time.Until(configBoundary) + 10*time.Millisecond)
	release()

	waitForHealthyWithin(t, opsAddr, 10*time.Second)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+opsAddr+"/metrics", nil)
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
	obs.Handler().ServeHTTP(rec, req)
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
// window end to end. srv.Shutdown waits for in-flight requests, so a ping
// admitted while live can reach the state machine after watch.Run took its
// undelivered snapshot; recorded there, it moves lastSeen, marks the beat fresh
// and can queue a recovery nobody delivers. The request is admitted before the
// signal (the 100-continue proves it) and completes after it, so acceptance
// must also close after the body read.
func TestBeatInFlightAtShutdownIsRefusedAndRecordsNothing(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default and
	// os.Stderr, and a process-wide signal.
	//
	// A beat id no other test in this package pings, since the metrics
	// registry is a package-level singleton shared by the whole test binary.
	const beat = "drain-guard"
	addr, opsAddr := prepareLifecycleRun(t, beat)
	logs := captureRunLog(t)

	received := `knell_beats_received_total{beat="` + beat + `"}`
	r := startLifecycleRun(t)
	waitForHealthyWithin(t, opsAddr, 10*time.Second)
	before := scrapeCounter(t, received)

	// Admit the ping while the app is live, then hold it inside the handler's
	// body read. Expect: 100-continue is the proof the handler was entered and
	// is reading: net/http emits it on the first Read of the body.
	conn, reader := startHeldBeatRequest(t, addr, beat, 1)

	// Cancel the shared context. The verdict flips in the pre-drain hook,
	// which httpserver invokes only after cancellation, so waiting for its line
	// makes the rest of this test deterministically post-cancellation.
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	waitForLogWithin(t, logs, drainLine, 5*time.Second)

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
	// process-wide signal.
	//
	// A beat id no other test in this package pings, since the metrics
	// registry is a package-level singleton shared by the whole test binary.
	const beat = "token-gate"
	// A token no other boot in this package uses, so the gate cannot pass by
	// admitting the value installRunEnv installs for every other test.
	const token = "lifecycle-token-gate-token"
	addr, opsAddr := prepareLifecycleRun(t, beat)
	// prepareLifecycleRun installs the shared testBeatToken; this overrides it
	// with the value under test.
	t.Setenv("BEAT_TOKENS", beat+":"+token)

	r := startLifecycleRun(t)
	waitForHealthyWithin(t, opsAddr, 10*time.Second)

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
	// process-wide signal.
	addr, opsAddr := prepareLifecycleRun(t, "host-gate")
	// Every other lifecycle boot leaves ALLOWED_HOSTS unset (the policy is
	// then inactive and admits any Host), so this is the variable under test.
	t.Setenv("ALLOWED_HOSTS", "knell.internal")

	r := startLifecycleRun(t)
	waitForHealthyWithin(t, opsAddr, 10*time.Second)

	blockedStatus, blockedBody := getHealthzAsHost(t, opsAddr, "attacker.example")
	if blockedStatus != http.StatusForbidden {
		t.Errorf("request with an unlisted Host = %d, want 403: an unwired ALLOWED_HOSTS policy leaves the DNS-rebinding guard disabled (body %s)", blockedStatus, blockedBody)
	}
	if !strings.Contains(blockedBody, "host_not_allowed") {
		t.Errorf("blocked-host body = %s, want the host_not_allowed code", blockedBody)
	}

	if allowedStatus, allowedBody := getHealthzAsHost(t, opsAddr, "knell.internal"); allowedStatus != http.StatusOK {
		t.Errorf("request with the configured Host = %d, want 200: the allowlist must admit the hostname the operator configured (body %s)", allowedStatus, allowedBody)
	}

	// The ping listener applies the same policy: run() must wire it into both.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+addr+"/beat/host-gate", nil)
	if err != nil {
		t.Fatalf("building the beat request: %v", err)
	}
	req.Host = "attacker.example"
	req.Header.Set("Authorization", "Bearer "+testBeatToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /beat/host-gate: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("ping with an unlisted Host = %d, want 403: ALLOWED_HOSTS covers the ping listener too", resp.StatusCode)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	r.waitForReturn(t, 10*time.Second, "after a shutdown signal")
}

// TestRunResolvesClientIPThroughConfiguredTrustedProxies pins the last Deps
// field main wires. Dropped, TRUSTED_PROXIES is still parsed and reported at
// startup while every access line names the proxy instead of an address an
// operator can block. client_ip is visible only in the access line, and run()
// replaces the capture prepareLifecycleRun installs, so captureRunLog redirects
// the writer slogx.Setup resolves.
func TestRunResolvesClientIPThroughConfiguredTrustedProxies(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default and
	// os.Stderr, and a process-wide signal.
	//
	// A beat id no other test in this package pings, since the metrics registry
	// is a package-level singleton shared by the whole test binary.
	const beat = "client-ip"
	addr, opsAddr := prepareLifecycleRun(t, beat)
	// Loopback, because that is what the socket peer of the request below is:
	// the trusted set has to contain the PEER for a forwarded header to be
	// consulted at all. Every other lifecycle boot leaves the variable unset.
	t.Setenv("TRUSTED_PROXIES", "127.0.0.0/8")
	// Installed before the boot: slogx.Setup resolves its output writer when
	// run() calls it, so a later swap would not be seen.
	logs := captureRunLog(t)

	r := startLifecycleRun(t)
	waitForHealthyWithin(t, opsAddr, 10*time.Second)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+addr+"/beat/"+beat, nil)
	if err != nil {
		t.Fatalf("building beat request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testBeatToken)
	req.Header.Set("X-Forwarded-For", forwardedSender)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("ping %s: %v", req.URL, err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close beat response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ping = %d, want 200: the access line under test is the one a RECORDED beat writes", resp.StatusCode)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	r.waitForReturn(t, 10*time.Second, "after a shutdown signal")

	// Poll: run() has returned, so the line is written, but the goroutine
	// draining the pipe still has to be scheduled.
	want := "client_ip=" + forwardedSender
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("no access line reports %s; an unwired Deps.TrustedProxies leaves client_ip on the trusted proxy itself, so a token-guessing run past the throttle is attributed to the proxy and no source address is left to block. Log:\n%s", want, logs.String())
}

// forwardedSender is the address the trusted proxy claims the request came
// from, chosen from the RFC 5737 documentation range and distinct from the
// loopback socket peer so the two possible client_ip values cannot be confused.
const forwardedSender = "203.0.113.7"

// runLog collects what run() logs. Writes come from run()'s own goroutines
// while the test body reads, so the buffer is mutex-guarded.
type runLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *runLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *runLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// captureRunLog redirects os.Stderr to a pipe for the test and returns what is
// written to it: run() calls slogx.Setup, whose handler replaces
// prepareLifecycleRun's capture and resolves os.Stderr when it is built. The pipe
// is drained continuously, since a full 64 KiB buffer would block run() in a log
// write and hang the shutdown the test waits for.
func captureRunLog(t *testing.T) *runLog {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("opening a pipe for run's log output: %v", err)
	}
	original := os.Stderr
	os.Stderr = w
	out := &runLog{}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		_, _ = io.Copy(out, r)
	}()
	t.Cleanup(func() {
		os.Stderr = original
		// Close the write end first: io.Copy returns on EOF, which cannot
		// arrive while this process still holds the writer open.
		if err := w.Close(); err != nil {
			t.Errorf("closing the log pipe writer: %v", err)
		}
		<-drained
		if err := r.Close(); err != nil {
			t.Errorf("closing the log pipe reader: %v", err)
		}
	})
	return out
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
// teardown hook's guard: httpserver.Run hands the hook a context from the
// deadline srv.Shutdown just spent, so with the loop stopped and the context
// done a single select would pick at random and log a false "watch loop still
// running" on half of ordinary shutdowns. One call catches a collapsed select
// only half the time, so the test repeats it 200 times.
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
	if !rec.HasAttr(stillRunningWarn, "retryable", "false") {
		t.Errorf("still-running warning omits retryable=false: this WARN is the only trace of the notices an abandoned loop still held, and the loss family promises the attribute so a log rule can key on consequence; records = %v", rec.Records())
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
// teardown when Serve returns before a signal: httpserver then skips the drain
// hooks, so this path alone closes beat admission, drains the health verdict
// and cancels the watch loop. The stop func closes watcherDone, so a teardown
// that waits before cancelling fails here. The server never starts, so
// TestTeardownAfterServeExitDrainsAcceptedHandlers owns the handler drain.
func TestTeardownAfterServeExitMarksUnhealthyThenCancelsAndWaits(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default.
	rec := capture.Default(t)

	st := health.NewStatus()
	st.Set(true)

	exitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	watcherDone := make(chan struct{})
	admissionClosed := 0
	closeAdmission := func() { admissionClosed++ }
	stopCalls := 0
	stop := func() {
		stopCalls++
		close(watcherDone)
	}

	start := time.Now()
	teardownAfterServeExit(exitCtx, newServer(http.NewServeMux()), st, closeAdmission, stop, watcherDone)
	waited := time.Since(start)

	if admissionClosed != 1 {
		t.Errorf("admission closed %d times, want exactly 1: this path SKIPS httpserver's pre-drain hook, so nothing else closes beat admission, and a connection accepted just before the listener died could still record a ping behind the watch loop's tally", admissionClosed)
	}
	if stopCalls != 1 {
		t.Errorf("stop() called %d times, want exactly 1: an uncancelled watcher keeps alerting behind a server that stopped serving", stopCalls)
	}
	if st.Healthy() {
		t.Error("verdict still healthy after the serve loop exited: /healthz would call a stopping process healthy")
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

// TestTeardownAfterServeExitDrainsAcceptedHandlers pins the srv.Shutdown drain:
// a handler accepted before the listener failed must finish before teardown
// lets the process exit, since closing admission does not stop a handler past
// its admission check. The sibling test above uses a never-started server, so
// it cannot see this drain being dropped.
func TestTeardownAfterServeExitDrainsAcceptedHandlers(t *testing.T) {
	// Serial (no t.Parallel): the teardown logs through the process-global slog
	// default.
	st := health.NewStatus()
	st.Set(true)

	entered := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})
	srv := newServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		close(handlerDone)
		w.WriteHeader(http.StatusOK)
	}))
	ln, lnErr := net.Listen("tcp", "127.0.0.1:0")
	if lnErr != nil {
		t.Fatalf("listen: %v", lnErr)
	}
	// Serve's result is the ORDERING WITNESS: http.Server.Shutdown closes the
	// listener before it waits for active handlers, so Serve returning
	// ErrServerClosed proves the drain is under way. A fixed real-time wait
	// would only prove a goroutine had time to be scheduled, and a teardown
	// with Shutdown deleted passes that on a loaded runner.
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(ln) }()
	go func() {
		resp, getErr := http.Get("http://" + ln.Addr().String() + "/")
		if getErr == nil {
			_ = resp.Body.Close()
		}
	}()
	<-entered

	released := false
	releaseHandler := func() {
		if !released {
			released = true
			close(release)
		}
	}
	// On an early failure the handler is still parked on <-release and the
	// listener is still open: free both, or a failing run hangs at exit.
	t.Cleanup(func() {
		releaseHandler()
		_ = srv.Close()
	})

	watcherDone := make(chan struct{})
	exitCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	returned := make(chan struct{})
	go func() {
		teardownAfterServeExit(exitCtx, srv, st, func() {}, func() { close(watcherDone) }, watcherDone)
		close(returned)
	}()
	select {
	case serveErr := <-serveDone:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			t.Fatalf("Serve returned %v, want http.ErrServerClosed: teardown must shut the server down rather than leave the listener up", serveErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve never returned: teardownAfterServeExit did not shut the server down, so an accepted handler's own loss diagnostic (the only trace of a discarded outage record) would be cut off by process exit")
	}
	// Shutdown has closed the listener and is now waiting on the parked
	// handler, so teardown MUST still be inside it.
	select {
	case <-returned:
		t.Fatal("teardownAfterServeExit returned while an accepted handler was still in flight: srv.Shutdown must drain it, or a handler's own loss diagnostic (the only trace of a discarded outage record) is cut off by process exit")
	default:
	}
	releaseHandler()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("teardownAfterServeExit did not return after the accepted handler finished")
	}
	select {
	case <-handlerDone:
	default:
		t.Fatal("teardown returned without the handler having completed")
	}
}

// TestTeardownAfterServeExitWarnsWhenTheDrainOutlivesTheGrace pins the WARN for
// srv.Shutdown returning with an accepted handler still in flight, the
// operator's only notice that the handler may not have finished. It carries
// retryable=false like the rest of the loss family, so a Loki rule keyed on
// consequence matches it.
func TestTeardownAfterServeExitWarnsWhenTheDrainOutlivesTheGrace(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default.
	rec := capture.Default(t)

	st := health.NewStatus()
	st.Set(true)

	entered := make(chan struct{})
	release := make(chan struct{})
	srv := newServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	ln, lnErr := net.Listen("tcp", "127.0.0.1:0")
	if lnErr != nil {
		t.Fatalf("listen: %v", lnErr)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(ln) }()
	go func() {
		resp, getErr := http.Get("http://" + ln.Addr().String() + "/")
		if getErr == nil {
			_ = resp.Body.Close()
		}
	}()
	<-entered

	released := false
	releaseHandler := func() {
		if !released {
			released = true
			close(release)
		}
	}
	// On an early failure the handler is still parked on <-release and the
	// listener is still open: free both, or a failing run hangs at exit.
	t.Cleanup(func() {
		releaseHandler()
		_ = srv.Close()
	})

	// An already-expired grace: Shutdown still closes the listener, cannot
	// drain the parked handler, and returns the deadline error -- the branch
	// under test, unreachable while a drain succeeds.
	exitCtx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	<-exitCtx.Done()

	watcherDone := make(chan struct{})
	teardownAfterServeExit(exitCtx, srv, st, func() {}, func() { close(watcherDone) }, watcherDone)

	const inFlightWarn = "accepted requests were still in flight"
	if n := rec.CountLevel(slog.LevelWarn, inFlightWarn); n != 1 {
		t.Errorf("%q at WARN = %d, want exactly 1: this line is the only notice that a cut-off handler's loss diagnostic may be missing; messages = %v", inFlightWarn, n, rec.Messages())
	}
	if !rec.HasAttr(inFlightWarn, "retryable", "false") {
		t.Errorf("in-flight warning omits retryable=false: the loss family promises the attribute so a log rule can key on consequence; records = %v", rec.Records())
	}
	if !rec.HasAttr(inFlightWarn, "grace", shutdownGrace.String()) {
		t.Errorf("in-flight warning omits grace=%s: nothing then names the constant an operator would raise; records = %v", shutdownGrace, rec.Records())
	}

	releaseHandler()
	select {
	case serveErr := <-serveDone:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			t.Errorf("Serve returned %v, want http.ErrServerClosed: even an expired grace must still close the listener", serveErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve never returned after the handler was released")
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

// TestNewServerBoundsWholeRequestsAndRoutesConnectionErrorsThroughSlog pins
// server wiring that is silent when dropped: the read and write bounds (else a
// trickled body holds a goroutine), the MaxHeaderBytes cap (else one caller
// makes knell read 1 MiB of headers per connection) and WithSlogErrorLog (else
// "Accept error" lines lose their level). The header cap is read from
// config.MaxRequestHeaderBytes so the two cannot drift. The grace half pins
// that max(read, header + write) leaves awaitWatchLoop its reserve.
func TestNewServerBoundsWholeRequestsAndRoutesConnectionErrorsThroughSlog(t *testing.T) {
	// Serial (no t.Parallel): httpserver.WithSlogErrorLog resolves slog.Default()
	// as NewServer applies it, so the capture must be the default first.
	rec := capture.Default(t)

	srv := newServer(http.NotFoundHandler())

	if srv.ReadHeaderTimeout != requestHeaderTimeout {
		t.Errorf("ReadHeaderTimeout = %s, want %s: net/http arms only the header deadline while the headers are being read (the whole-request read deadline is installed after they are parsed), so httpserver's 10s default is a live slowloris window the read bound never cuts short -- a header term on its own past the whole-request budget the grace-margin check below keeps inside shutdownGrace", srv.ReadHeaderTimeout, requestHeaderTimeout)
	}
	if srv.ReadTimeout != requestReadTimeout {
		t.Errorf("ReadTimeout = %s, want %s: httpserver leaves it unset by default, and an unbounded read lets a trickled beat body hold a handler goroutine indefinitely", srv.ReadTimeout, requestReadTimeout)
	}
	if srv.WriteTimeout != requestWriteTimeout {
		t.Errorf("WriteTimeout = %s, want %s: an unbounded write lets a client that requests /metrics and never reads the response pin the goroutine in Write", srv.WriteTimeout, requestWriteTimeout)
	}
	// srv.Shutdown waits for in-flight requests out of the same budget as the
	// pre-drain and awaitWatchLoop, so the worst-case request must finish with
	// grace left. net/http starts both read deadlines from one instant and arms the
	// write deadline after the headers, so the worst case is the longer of the read
	// bound and header plus write; asserting only one lets the other grow past it.
	const teardownReserve = 3 * time.Second // what the constants' comment promises awaitWatchLoop
	if stop := shutdownGrace + opsShutdownGrace; stop >= 10*time.Second {
		t.Errorf("shutdownGrace + opsShutdownGrace = %s, want under Docker's default 10s stop timeout: the operations listener drains after the ping listener, so the two budgets add, and SIGKILL would cut the abandoned-loop WARN", stop)
	}
	if worst := max(requestReadTimeout, requestHeaderTimeout+requestWriteTimeout); worst > shutdownGrace-teardownReserve {
		t.Errorf("worst-case active request = %s, want <= %s (shutdownGrace %s less the %s the constants' comment reserves for the watch-loop teardown): a bound this size lets one authenticated slow caller eat the grace awaitWatchLoop needs to collect the loop and flush its abandoned-notice WARN", worst, shutdownGrace-teardownReserve, shutdownGrace, teardownReserve)
	}
	if srv.MaxHeaderBytes != config.MaxRequestHeaderBytes {
		t.Errorf("MaxHeaderBytes = %d, want %d: the default is net/http's 1 MiB, so an unauthenticated caller can make this process read a megabyte of header per connection, and the beat token maximum config enforces at startup stops bounding what a ping can actually carry", srv.MaxHeaderBytes, config.MaxRequestHeaderBytes)
	}
	if srv.ErrorLog == nil {
		t.Fatal("ErrorLog is nil, so net/http's connection-level errors go to the standard logger: an accept failure -- a whole-service outage here -- arrives as an unstructured, level-less line")
	}

	srv.ErrorLog.Print("http: Accept error: accept tcp [::]:9190: too many open files; retrying in 5ms")

	if n := rec.CountLevel(slog.LevelError, "Accept error"); n != 1 {
		t.Errorf("accept-error lines at ERROR = %d, want 1: knell's only job is answering pings, so an accept failure is an outage rather than a degradation; records = %v", n, rec.Records())
	}
}

// TestRunServesTheHealthVerdictOnHealthz pins the composition root's /healthz
// wiring: the webapi tests inject a fixed verdict, so only this test sees that
// run() puts the health handler in that slot. With obs.Handler there /healthz
// answers 200 with the wrong body, and an unwired route answers 404. It also
// pins that run() publishes knell_build_info, which KnellRestartChurn joins on.
func TestRunServesTheHealthVerdictOnHealthz(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default, a
	// process-wide signal.
	//
	// A beat id no other test in this package pings, since the metrics
	// registry is a package-level singleton shared by the whole test binary.
	addr, opsAddr := prepareLifecycleRun(t, "healthz-probe")

	r := startLifecycleRun(t)
	waitForHealthyWithin(t, opsAddr, 10*time.Second)

	if status, _ := getHealthz(t, addr); status != http.StatusNotFound {
		t.Errorf("GET /healthz on the ping port = %d, want 404: /healthz and /metrics are served only on OPS_LISTEN_ADDR", status)
	}
	if status, body := getHealthz(t, opsAddr); status != http.StatusOK || body != `{"status":"ok"}`+"\n" {
		t.Errorf("/healthz on a serving switch = %d %q, want 200 {\"status\":\"ok\"}", status, body)
	}
	if got := scrapeCounter(t, `knell_build_info{version="`+version+`"}`); got != 1 {
		t.Errorf("knell_build_info{version=%q} = %v, want 1: a rule joining on it drops a running knell", version, got)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	r.waitForReturn(t, 10*time.Second, "after a shutdown signal")
}

// TestHealthzHandlerFollowsTheVerdict pins what /healthz answers for each
// state of the verdict run() keeps: unhealthy before the listener is up,
// healthy once it serves, and unhealthy for good once shutdown begins.
func TestHealthzHandlerFollowsTheVerdict(t *testing.T) {
	t.Parallel()
	const unhealthy = `{"status":"unhealthy","reasons":["` + notServingReason + `"]}` + "\n"
	st := health.NewStatus(health.WithLogger(slog.New(slog.DiscardHandler)))
	h := healthzHandler(st)
	answer := func() (int, string) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil))
		return w.Code, w.Body.String()
	}
	if code, body := answer(); code != http.StatusServiceUnavailable || body != unhealthy {
		t.Errorf("before Set(true) = %d %q, want 503 %q", code, body, unhealthy)
	}
	st.Set(true)
	if code, body := answer(); code != http.StatusOK || body != `{"status":"ok"}`+"\n" {
		t.Errorf("serving = %d %q, want 200 {\"status\":\"ok\"}", code, body)
	}
	st.BeginDrain()
	st.Set(true)
	if code, body := answer(); code != http.StatusServiceUnavailable || body != unhealthy {
		t.Errorf("after BeginDrain = %d %q, want 503 %q", code, body, unhealthy)
	}
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

// failingListener is a listener whose Accept fails for good at once, the way
// an exhausted or revoked socket makes Serve return.
type failingListener struct{ net.Listener }

func (failingListener) Accept() (net.Conn, error) { return nil, errors.New("accept failed for good") }

// TestServeListenersStopsKnellWhenTheOpsListenerFails pins the early-exit rule:
// an operations listener that stops on its own cancels the ping listener as a
// signal would, and the returned error makes knell exit non-zero so the
// restart policy brings /healthz and /metrics back. A signal instead stops
// both with no error.
func TestServeListenersStopsKnellWhenTheOpsListenerFails(t *testing.T) {
	// serve runs serveListeners with ops as the operations socket, sending the
	// shutdown signal itself when signal is set, and returns its outcome.
	serve := func(t *testing.T, ops net.Listener, signal bool) (context.Context, error) {
		t.Helper()
		ping, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Skipf("cannot bind: %v", err)
		}
		ctx, stop := context.WithCancel(t.Context())
		t.Cleanup(stop)
		srv := func() *http.Server {
			return &http.Server{Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}
		}
		done := make(chan error, 1)
		go func() {
			done <- serveListeners(ctx, stop, srv(), ping, srv(), ops)
		}()
		if signal {
			stop()
		}
		select {
		case err := <-done:
			return ctx, err
		case <-time.After(10 * time.Second):
			t.Fatal("serveListeners did not return")
			return ctx, nil
		}
	}

	t.Run("ops listener fails on its own", func(t *testing.T) {
		inner, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Skipf("cannot bind: %v", err)
		}
		t.Cleanup(func() { _ = inner.Close() })
		ctx, err := serve(t, failingListener{inner}, false)
		if err == nil || !strings.Contains(err.Error(), "operations listener on "+inner.Addr().String()+" stopped") {
			t.Errorf("serveListeners = %v, want the operations failure naming its address, so knell exits non-zero", err)
		}
		if ctx.Err() == nil {
			t.Error("the ping listener's context is still live: a failed operations listener must drain the ping listener")
		}
	})

	// A scrape still in flight when the ping listener has drained is cut off
	// at opsShutdownGrace, not httpserver's longer default, and that cutoff
	// is no exit error.
	t.Run("a stuck operations request is cut off at opsShutdownGrace", func(t *testing.T) {
		ping, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Skipf("cannot bind: %v", err)
		}
		ops, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Skipf("cannot bind: %v", err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { close(release) })
		stuck := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			close(entered)
			<-release
		})
		ctx, stop := context.WithCancel(t.Context())
		t.Cleanup(stop)
		done := make(chan error, 1)
		go func() {
			done <- serveListeners(ctx, stop,
				&http.Server{Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}, ping,
				&http.Server{Handler: stuck, ReadHeaderTimeout: time.Second}, ops)
		}()
		go func() {
			resp, err := http.Get("http://" + ops.Addr().String() + "/metrics")
			if err == nil {
				_ = resp.Body.Close()
			}
		}()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the operations request never reached its handler")
		}
		stopped := time.Now()
		stop()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serveListeners = %v, want nil: a scrape cut off at the end of the operations grace is no exit error", err)
			}
			if took := time.Since(stopped); took > opsShutdownGrace+time.Second {
				t.Errorf("the stop took %s, want at most opsShutdownGrace (%s) plus 1s: the operations drain must not run to httpserver's default grace", took, opsShutdownGrace)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("serveListeners did not return")
		}
	})

	t.Run("a signal stops both cleanly", func(t *testing.T) {
		ops, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Skipf("cannot bind: %v", err)
		}
		if _, err := serve(t, ops, true); err != nil {
			t.Errorf("serveListeners after a signal = %v, want nil", err)
		}
	})
}
