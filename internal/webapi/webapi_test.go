package webapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cplieger/knell/internal/metrics"
	"github.com/cplieger/knell/internal/watch"
	"github.com/cplieger/slogx/capture"
)

// fakeBeater accepts a fixed id set and records what was recorded.
type fakeBeater struct {
	known map[string]bool
	seen  []string
}

func (f *fakeBeater) Beat(id string) bool {
	if !f.known[id] {
		return false
	}
	f.seen = append(f.seen, id)
	return true
}

// newTestHandler assembles the routed handler around b with a healthy
// liveness endpoint and a LIVE application context (nothing shutting down);
// token gates the beat endpoint exactly as in production ("" = open).
func newTestHandler(b *fakeBeater, token string) http.Handler {
	return newTestHandlerCtx(context.Background(), b, token)
}

// newTestHandlerCtx is newTestHandler with the shared application context
// under test control, so a test can close beat acceptance the way SIGTERM
// does: cancel it, and every later ping must be refused.
func newTestHandlerCtx(appCtx context.Context, b *fakeBeater, token string) http.Handler {
	return newTestHandlerHealthz(appCtx, b, token, http.StatusOK)
}

// newTestHandlerHealthz is newTestHandlerCtx with the liveness status under
// test control, so a test can drive a FAILING probe: health.Handler answers 503
// whenever the liveness marker is absent (boot, or after the pre-drain flip).
func newTestHandlerHealthz(appCtx context.Context, b *fakeBeater, token string, healthzStatus int) http.Handler {
	return New(appCtx, b, token, Routes{Healthz: staticHealthz(healthzStatus), Metrics: metrics.Handler()})
}

// staticHealthz stands in for health.Handler with a fixed verdict.
func staticHealthz(status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	})
}

func TestBeatEndpoint(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantSeen   int
	}{
		{name: "post known", method: http.MethodPost, path: "/beat/api", body: `{"alerts":[]}`, wantStatus: 200, wantSeen: 1},
		{name: "get known", method: http.MethodGet, path: "/beat/api", wantStatus: 200, wantSeen: 1},
		{name: "post unknown", method: http.MethodPost, path: "/beat/ghost", wantStatus: 404},
		{name: "missing id segment", method: http.MethodPost, path: "/beat/", wantStatus: 404},
		{name: "head rejected without recording", method: http.MethodHead, path: "/beat/api", wantStatus: 405},
		{name: "delete rejected", method: http.MethodDelete, path: "/beat/api", wantStatus: 405},
		{name: "nested path rejected", method: http.MethodPost, path: "/beat/api/extra", wantStatus: 404},
		// The decoded path segment reaches the state machine verbatim, so these
		// pin what an arbitrary request path can do to a metric label. An escaped
		// slash stays inside the {id} segment (it does not span routes), and a
		// control character or quote is rejected by the configured-id lookup
		// rather than sanitized here. A percent-encoded spelling of a configured
		// id is the same id, so it records normally.
		{name: "escaped slash in id rejected", method: http.MethodPost, path: "/beat/a%2Fb", wantStatus: 404},
		{name: "nul byte in id rejected", method: http.MethodPost, path: "/beat/api%00", wantStatus: 404},
		{name: "newline in id rejected", method: http.MethodPost, path: "/beat/api%0Ax", wantStatus: 404},
		{name: "quote in id rejected", method: http.MethodPost, path: "/beat/api%22x", wantStatus: 404},
		{name: "percent-encoded known id recorded", method: http.MethodPost, path: "/beat/%61pi", wantStatus: 200, wantSeen: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{"api": true}}
			h := newTestHandler(b, "")
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("%s %s = %d, want %d (body %s)", tt.method, tt.path, rec.Code, tt.wantStatus, rec.Body.String())
			}
			if len(b.seen) != tt.wantSeen {
				t.Errorf("recorded beats = %v, want %d", b.seen, tt.wantSeen)
			}
			if tt.wantStatus == http.StatusOK && !strings.Contains(rec.Body.String(), `"ok":true`) {
				t.Errorf("ok body = %s", rec.Body.String())
			}
			if tt.wantStatus == http.StatusNotFound && tt.path == "/beat/ghost" &&
				!strings.Contains(rec.Body.String(), "unknown_beat") {
				t.Errorf("404 body = %s, want unknown_beat code", rec.Body.String())
			}
		})
	}
}

func TestMetricsExposition(t *testing.T) {
	// Declare a beat so the per-beat series exist even when this package's
	// tests run in isolation (labeled metrics emit no output until a first
	// series is recorded). The notification counters need no touch: the
	// metrics package pre-mints every kind at init.
	metrics.InitBeat("webapi-test", time.Unix(0, 0))

	h := newTestHandler(&fakeBeater{}, "")
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"knell_beats_received_total",
		"knell_beat_fresh",
		"knell_notifications_sent_total",
		"process_start_time_seconds",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("exposition missing %s", want)
		}
	}
}

// TestProbePathAccessLogLevels pins the ProbeLogLevel policy on knell's two
// machine-probe endpoints. It carries forward the guard the former
// WithSkipPaths("/healthz", "/metrics") existed for — routine probe traffic
// must not pollute the log stream at the operating level, so no probe line may
// land at Info — and adds the direction a skip list could not express: a probe
// that FAILS has to be visible. /healthz and /metrics are the endpoints
// carrying knell's quorum signal, so a scrape answering 4xx/5xx or a liveness
// probe gone 503 must be greppable rather than silently dropped.
func TestProbePathAccessLogLevels(t *testing.T) {
	tests := map[string]struct {
		method        string
		path          string
		healthzStatus int
		wantStatus    int
		wantLevel     slog.Level
	}{
		// A liveness probe every 30s and a scrape every 15s are noise while
		// they succeed: Debug keeps them out of the shipped stream but
		// reachable under LOG_LEVEL=debug ("is the probe even arriving?").
		"healthy healthz probe": {
			method: http.MethodGet, path: "/healthz", healthzStatus: http.StatusOK,
			wantStatus: http.StatusOK, wantLevel: slog.LevelDebug,
		},
		"healthy metrics scrape": {
			method: http.MethodGet, path: "/metrics", healthzStatus: http.StatusOK,
			wantStatus: http.StatusOK, wantLevel: slog.LevelDebug,
		},
		// health.Handler answers 503 whenever the liveness marker is absent.
		// That is the failing dead-man-switch signal, at Error.
		"failing healthz probe": {
			method: http.MethodGet, path: "/healthz", healthzStatus: http.StatusServiceUnavailable,
			wantStatus: http.StatusServiceUnavailable, wantLevel: slog.LevelError,
		},
		// A prober or scraper aimed with the wrong method gets 405 from the
		// mux: the "my scrape never lands" class, now at Warn instead of
		// silence.
		"wrong-method healthz probe": {
			method: http.MethodPost, path: "/healthz", healthzStatus: http.StatusOK,
			wantStatus: http.StatusMethodNotAllowed, wantLevel: slog.LevelWarn,
		},
		"wrong-method metrics scrape": {
			method: http.MethodPost, path: "/metrics", healthzStatus: http.StatusOK,
			wantStatus: http.StatusMethodNotAllowed, wantLevel: slog.LevelWarn,
		},
		// Everything off the probe list keeps the default Info access line.
		"beat ping stays at info": {
			method: http.MethodPost, path: "/beat/api", healthzStatus: http.StatusOK,
			wantStatus: http.StatusOK, wantLevel: slog.LevelInfo,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			// Serial (no t.Parallel): capture.Default swaps the process-global
			// slog default. It must be installed BEFORE New, because
			// webhttp.Logging resolves slog.Default() when the chain is built.
			rec := capture.Default(t)
			h := newTestHandlerHealthz(context.Background(), &fakeBeater{known: map[string]bool{"api": true}}, "", tt.healthzStatus)

			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != tt.wantStatus {
				t.Fatalf("%s %s = %d, want %d (body %s)", tt.method, tt.path, w.Code, tt.wantStatus, w.Body.String())
			}

			if got := rec.CountExact("http"); got != 1 {
				t.Fatalf("access lines = %d %v, want exactly 1: no path is skipped any more, so every request leaves one line",
					got, rec.Messages())
			}
			for _, lvl := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
				want := 0
				if lvl == tt.wantLevel {
					want = 1
				}
				if got := rec.CountLevel(lvl, "http"); got != want {
					t.Errorf("access lines at %v = %d, want %d (%s %s answered %d)",
						lvl, got, want, tt.method, tt.path, tt.wantStatus)
				}
			}
			// The line is only useful if it names what happened: an operator
			// greps the Warn/Error probe line for its status.
			if !rec.HasAttr("http", "status", strconv.Itoa(tt.wantStatus)) {
				t.Errorf("access line missing status=%d; records = %v", tt.wantStatus, rec.Records())
			}
			if !rec.HasAttr("http", "path", tt.path) {
				t.Errorf("access line missing path=%s; records = %v", tt.path, rec.Records())
			}
		})
	}
}

func TestSecurityHeadersPresent(t *testing.T) {
	h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, "")
	req := httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q", got)
	}
}

// TestNoStoreOnEveryRoute pins that no response knell serves is cacheable: a
// cached GET ping would never reach the observer (false MISSING notice) and a
// cached /metrics exposition would report a stale beat_fresh=1 to the scraper,
// which is the direction that masks the quorum alert.
func TestNoStoreOnEveryRoute(t *testing.T) {
	h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, "")
	for _, path := range []string{"/beat/api", "/healthz", "/metrics"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if got := rec.Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf("Cache-Control on GET %s = %q, want %q", path, got, "no-store")
			}
		})
	}
}

// panicBeater panics on every ping, standing in for a bug anywhere below the
// beat handler.
type panicBeater struct{}

func (panicBeater) Beat(string) bool { panic("beat exploded") }

func TestPanicUnderBeatHandlerAnswers500AndIsLogged(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and it must be installed BEFORE New, because webhttp resolves
	// slog.Default() when the chain is built.
	//
	// Without the chain's Recoverer, a panic under the beat handler unwinds to
	// net/http: the sender sees a reset connection rather than a 500, and the
	// access log never reports a status for the endpoint that feeds the switch.
	rec := capture.Default(t)
	h := New(context.Background(), panicBeater{}, "", Routes{
		Healthz: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		Metrics: metrics.Handler(),
	})

	req := httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("panicking beat handler = %d, want 500 (Recoverer must convert the panic into a response)", w.Code)
	}
	if got := rec.CountLevel(slog.LevelError, "recovered from panic"); got != 1 {
		t.Errorf("panic log lines at Error = %d, want exactly 1: %v", got, rec.Messages())
	}
	if !rec.HasAttr("http", "status", "500") {
		t.Errorf("access line does not report the 500; records = %v", rec.Records())
	}
}

// countingReader counts Read calls so tests can assert the handler never
// touches the body of a rejected request.
type countingReader struct {
	reads int
}

func (c *countingReader) Read([]byte) (int, error) {
	c.reads++
	return 0, io.EOF
}

// unboundedReader serves an endless zero stream and counts bytes read, so a
// test can observe exactly how much of a hostile body the handler drains.
type unboundedReader struct {
	n int64
}

func (r *unboundedReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	r.n += int64(len(p))
	return len(p), nil
}

func TestBeatBodyDrainIsBounded(t *testing.T) {
	// The handler drains the ignored body so keep-alive connections stay
	// reusable, but only up to maxBeatBody: a hostile endless body must
	// not tie the handler goroutine to an unbounded read.
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, "")
	body := &unboundedReader{}
	req := httptest.NewRequest(http.MethodPost, "/beat/api", body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if body.n != 1<<20 {
		t.Errorf("drained %d bytes, want exactly 1 MiB (drain must happen for connection reuse and stop at the documented cap)", body.n)
	}
}

func TestBeatTokenGate(t *testing.T) {
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, "s3cret")

	tests := []struct {
		name       string
		auth       string
		wantStatus int
		wantSeen   int
	}{
		{name: "no header", auth: "", wantStatus: 401},
		{name: "wrong token", auth: "Bearer nope", wantStatus: 401},
		{name: "correct token", auth: "Bearer s3cret", wantStatus: 200, wantSeen: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(b.seen)
			req := httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if got := len(b.seen) - before; got != tt.wantSeen {
				t.Errorf("recorded beats = %d, want %d (unauthorized pings must not be recorded)", got, tt.wantSeen)
			}
		})
	}

	t.Run("unauthorized body never read", func(t *testing.T) {
		body := &countingReader{}
		req := httptest.NewRequest(http.MethodPost, "/beat/api", body)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}
		if body.reads != 0 {
			t.Errorf("body reads = %d, want 0 (rejected requests must not be drained)", body.reads)
		}
	})
}

// FuzzBeatTokenAcceptsOnlyTheExactBearerHeader fuzzes the untrusted header the
// beat gate reads. BEAT_TOKEN is the only thing between the public internet and
// a sender that can keep the dead-man switch armed with no real heartbeat
// behind it, and acceptance is documented as exactly "Authorization: Bearer
// <token>". The invariant is a security equality, not crash-freedom: for ANY
// header value, the request is authorized iff the value equals the expected
// string exactly, an unauthorized ping records no beat, and the 401 body never
// echoes the configured token.
func FuzzBeatTokenAcceptsOnlyTheExactBearerHeader(f *testing.F) {
	const token = "s3cret"
	const expected = "Bearer " + token
	f.Add(expected)
	f.Add("")
	f.Add("bearer s3cret")
	f.Add("BEARER s3cret")
	f.Add("Bearer s3cret ")
	f.Add(" Bearer s3cret")
	f.Add("Bearer  s3cret")
	f.Add("Bearer\ts3cret")
	f.Add("Bearer s3cretx")
	f.Add("Bearer s3cre")
	f.Add("Bearer s3cret\x00")
	f.Add("Basic czNjcmV0OA==")
	f.Add("Bearer s3cret, Bearer s3cret")
	f.Add(strings.Repeat("Bearer s3cret", 40))
	f.Fuzz(func(t *testing.T, auth string) {
		b := &fakeBeater{known: map[string]bool{"api": true}}
		h := newTestHandler(b, token)
		req := httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
		req.Header.Set("Authorization", auth)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		wantStatus, wantSeen := http.StatusUnauthorized, 0
		if auth == expected {
			wantStatus, wantSeen = http.StatusOK, 1
		}
		if rec.Code != wantStatus {
			t.Fatalf("Authorization %q = %d, want %d (only the exact %q value may pass the gate)", auth, rec.Code, wantStatus, expected)
		}
		if len(b.seen) != wantSeen {
			t.Fatalf("Authorization %q recorded %d beats, want %d (an unauthorized ping must never feed the switch)", auth, len(b.seen), wantSeen)
		}
		if wantStatus == http.StatusUnauthorized && strings.Contains(rec.Body.String(), token) {
			t.Fatalf("401 body %q echoes the configured token", rec.Body.String())
		}
	})
}

func TestTokenGateScopedToBeatEndpoint(t *testing.T) {
	// /healthz and /metrics must stay reachable without the beat token:
	// the docker healthcheck and the Prometheus scraper carry no
	// Authorization header, and gating them would break liveness and the
	// quorum ground truth the moment BEAT_TOKEN is set.
	h := newTestHandler(&fakeBeater{}, "s3cret")

	for _, path := range []string{"/healthz", "/metrics"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s without token = %d, want 200 (token gates only /beat)", path, rec.Code)
		}
	}
}

func TestBeatTokenGateAppliesToGet(t *testing.T) {
	// GET /beat/{id} records a ping exactly like POST, so the token must
	// gate it identically: an ungated GET route would let any sender feed
	// the switch without the credential.
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, "s3cret")

	req := httptest.NewRequest(http.MethodGet, "/beat/api", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET without token = %d, want 401", rec.Code)
	}
	if len(b.seen) != 0 {
		t.Errorf("unauthorized GET recorded a beat: %v", b.seen)
	}

	req = httptest.NewRequest(http.MethodGet, "/beat/api", nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET with token = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if len(b.seen) != 1 {
		t.Errorf("authorized GET recorded %d beats, want 1", len(b.seen))
	}
}

func TestHeadRejectionSetsAllowHeader(t *testing.T) {
	h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, "")
	req := httptest.NewRequest(http.MethodHead, "/beat/api", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("HEAD /beat/api = %d, want 405", rec.Code)
	}
	if got := rec.Header().Get("Allow"); got != "GET, POST" {
		t.Errorf("Allow = %q, want \"GET, POST\" (a 405 must name the permitted methods so a HEAD-only prober learns how pings are recorded)", got)
	}
}

// fakeClock is a manual clock for the shutdown tests. watch.Run reads it from
// its sweep and gauge goroutines while the test advances it, so the mutex is
// what keeps these tests race-clean.
type fakeClock struct {
	now time.Time
	mu  sync.Mutex
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// recordingNotifier is a watch.Notifier that delivers everything successfully
// and counts what it was asked to deliver. A delivered send is what flips a
// beat to alerted, which is the state a ping turns into a recovered
// notification.
type recordingNotifier struct {
	missing   int
	recovered int
	history   int
	mu        sync.Mutex
}

func (n *recordingNotifier) BeatMissing(context.Context, string, time.Duration) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.missing++
	return nil
}

func (n *recordingNotifier) BeatRecovered(context.Context, string, time.Duration) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.recovered++
	return nil
}

func (n *recordingNotifier) BeatOutageHistory(context.Context, string, []watch.Outage) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.history++
	return nil
}

// counts returns the delivered message counts, per kind.
func (n *recordingNotifier) counts() (missing, recovered, history int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.missing, n.recovered, n.history
}

// waitUntil polls cond until it holds, failing the test on timeout.
func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// scrapeExposition returns the /metrics body served through the routed handler,
// asserting the scrape itself succeeded. Called with a cancelled application
// context it doubles as the check that the exposition keeps serving through the
// drain: only the beat endpoint refuses.
func scrapeExposition(t *testing.T, h http.Handler) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200: the exposition is the quorum ground truth and must keep serving", rec.Code)
	}
	return rec.Body.String()
}

// seriesValue reads one exposition sample by its full "name{labels}" prefix.
func seriesValue(t *testing.T, exposition, series string) (float64, bool) {
	t.Helper()
	for line := range strings.SplitSeq(exposition, "\n") {
		rest, ok := strings.CutPrefix(line, series)
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			t.Fatalf("parsing %q: %v", line, err)
		}
		return v, true
	}
	return 0, false
}

// mustSeriesValue is seriesValue for a sample that has to exist.
func mustSeriesValue(t *testing.T, exposition, series string) float64 {
	t.Helper()
	v, ok := seriesValue(t, exposition, series)
	if !ok {
		t.Fatalf("%s missing from the exposition:\n%s", series, exposition)
	}
	return v
}

// beatRequest sends one ping through the routed handler.
func beatRequest(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(""))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestBeatRefusedOnceTheApplicationContextIsCancelled pins the acceptance
// window against the real state machine and the real exposition. On SIGTERM the
// shared context is cancelled, which returns watch.Run (after it snapshots its
// undelivered work) while webhttp keeps the HTTP surface live for up to the
// shutdown grace. A ping accepted in that window is recorded behind a sender
// that no longer exists: lastSeen moves, knell_beats_received_total moves,
// knell_beat_fresh is republished as 1 — a false "all good" sample for the
// quorum rules — and for an alerted beat a recovered notification is queued on
// a channel nobody reads again. So from the instant the context is cancelled
// the endpoint must record NOTHING and say so honestly, while /healthz and
// /metrics keep serving through the drain.
func TestBeatRefusedOnceTheApplicationContextIsCancelled(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and it must be installed before New (webhttp.Logging resolves
	// slog.Default() when the chain is built).
	capture.Default(t)
	// Ids unique to this test: the metrics registry is a package-level
	// singleton shared by the whole test binary.
	const id = "webapi-shutdown-guard"
	const ghost = "webapi-shutdown-guard-ghost"
	start := time.Unix(1_700_000_000, 0).UTC()
	clock := &fakeClock{now: start}
	watcher := watch.New([]watch.Beat{{ID: id, Deadline: time.Minute}}, &recordingNotifier{}, clock.Now, start)

	appCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := New(appCtx, watcher, "", Routes{Healthz: staticHealthz(http.StatusOK), Metrics: metrics.Handler()})

	receivedSeries := `knell_beats_received_total{beat="` + id + `"}`
	lastSeenSeries := `knell_beat_last_seen_timestamp_seconds{beat="` + id + `"}`
	freshSeries := `knell_beat_fresh{beat="` + id + `"}`

	// The normal path first, so the refusal below is not a handler that was
	// broken for every ping: while the app is live a ping is recorded. The
	// counter is read as a DELTA — the registry is a package-level singleton, so
	// a repeated run (go test -count=2) carries the previous run's total.
	receivedBefore, _ := seriesValue(t, scrapeExposition(t, h), receivedSeries)
	if rec := beatRequest(t, h, http.MethodPost, "/beat/"+id); rec.Code != http.StatusOK {
		t.Fatalf("POST /beat/%s before cancellation = %d, want 200 (body %s)", id, rec.Code, rec.Body.String())
	}
	exposition := scrapeExposition(t, h)
	if got := mustSeriesValue(t, exposition, receivedSeries); got != receivedBefore+1 {
		t.Fatalf("%s after one live ping = %v, want %v", receivedSeries, got, receivedBefore+1)
	}
	if got := mustSeriesValue(t, exposition, lastSeenSeries); got != float64(start.Unix()) {
		t.Fatalf("%s after one live ping = %v, want %d", lastSeenSeries, got, start.Unix())
	}

	// Publish the beat overdue, the state a sweep leaves behind for a silent
	// beat: it is the sample the quorum rules alert on, and the one an accepted
	// ping would flip back to 1.
	metrics.SetBeatFresh(id, false)
	// Advance the clock so a recorded ping would be VISIBLE in lastSeen rather
	// than landing on the same second as the live one.
	clock.advance(time.Hour)

	// SIGTERM's effect on the app, and the only trigger the guard may key on.
	cancel()

	for _, method := range []string{http.MethodPost, http.MethodGet} {
		t.Run("refused "+method, func(t *testing.T) {
			rec := beatRequest(t, h, method, "/beat/"+id)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("%s /beat/%s after cancellation = %d, want 503 (body %s)",
					method, id, rec.Code, rec.Body.String())
			}
			body := rec.Body.String()
			if !strings.Contains(body, "shutting_down") {
				t.Errorf("503 body = %s, want the shutting_down code so a sender can tell a refusal from a 404", body)
			}
			if strings.Contains(body, id) {
				t.Errorf("503 body = %s, must not echo the beat id", body)
			}
		})
	}

	// Nothing may have moved: this is the tally watch.Run already reported.
	exposition = scrapeExposition(t, h)
	if got := mustSeriesValue(t, exposition, receivedSeries); got != receivedBefore+1 {
		t.Errorf("%s after a refused ping = %v, want it still %v: a refused ping must not be counted",
			receivedSeries, got, receivedBefore+1)
	}
	if got := mustSeriesValue(t, exposition, lastSeenSeries); got != float64(start.Unix()) {
		t.Errorf("%s after a refused ping = %v, want it still %d: a refused ping must not move lastSeen",
			lastSeenSeries, got, start.Unix())
	}
	if got := mustSeriesValue(t, exposition, freshSeries); got != 0 {
		t.Errorf("%s after a refused ping = %v, want it still 0: a refused ping must not republish a silent beat as fresh, which is exactly the sample the quorum rules read",
			freshSeries, got)
	}

	t.Run("unknown id refused without minting a series", func(t *testing.T) {
		rec := beatRequest(t, h, http.MethodPost, "/beat/"+ghost)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("POST /beat/%s after cancellation = %d, want 503 (body %s)", ghost, rec.Code, rec.Body.String())
		}
		if body := rec.Body.String(); strings.Contains(body, ghost) {
			t.Errorf("503 body = %s, must not echo the unknown beat id", body)
		}
		if exposition := scrapeExposition(t, h); strings.Contains(exposition, ghost) {
			t.Errorf("exposition mentions %s: a refused ping must mint no series, like the 404 path", ghost)
		}
	})

	t.Run("refused without reading the body", func(t *testing.T) {
		// The refusal lands before the body drain, like the 401 one: a ping
		// arriving during the drain must not be able to hold a handler
		// goroutine — and with it srv.Shutdown — open by trickling a payload.
		body := &countingReader{}
		req := httptest.NewRequest(http.MethodPost, "/beat/"+id, body)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
		if body.reads != 0 {
			t.Errorf("body reads = %d, want 0 (a refused ping must not be drained)", body.reads)
		}
	})

	t.Run("healthz and metrics keep serving", func(t *testing.T) {
		// The orchestrator has to observe the health flip, and a last scrape
		// during the drain is useful. Only the beat endpoint refuses.
		for _, path := range []string{"/healthz", "/metrics"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("GET %s with the app context cancelled = %d, want 200", path, rec.Code)
			}
		}
	})
}

// TestPingRefusedDuringShutdownQueuesNoRecovery pins the half of the loss that
// no counter and no log line can show: the recovered notification. A ping on an
// ALERTED beat queues a recoveryEvent on watch's bounded channel, whose only
// reader is watch.Run — which returned the moment the shared context was
// cancelled. Pre-fix the ping was accepted there and the notice died in the
// channel silently; the beat was even re-armed, so nothing ever reported the
// outage as over. The queue is unreadable from outside the watch package, so
// the oracle here is a fresh reader: give the channel a Run again and prove
// nothing comes out of it.
func TestPingRefusedDuringShutdownQueuesNoRecovery(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and it must be installed before New.
	capture.Default(t)
	const id = "webapi-recovery-guard"
	start := time.Unix(1_700_100_000, 0).UTC()
	clock := &fakeClock{now: start}
	notifier := &recordingNotifier{}
	watcher := watch.New([]watch.Beat{{ID: id, Deadline: time.Minute}}, notifier, clock.Now, start)

	appCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := New(appCtx, watcher, "", Routes{Healthz: staticHealthz(http.StatusOK), Metrics: metrics.Handler()})

	// Drive the beat into the alerted state: silence past its deadline, then one
	// sweep DELIVERS the missing notice (alerted flips only on a delivered
	// send). Only an alerted beat turns its next ping into a recovery.
	clock.advance(2 * time.Minute)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		watcher.Run(appCtx, time.Millisecond)
	}()
	waitUntil(t, 10*time.Second, "the missing notice to be delivered", func() bool {
		missing, _, _ := notifier.counts()
		return missing == 1
	})

	// SIGTERM's effect: Run returns after taking its undelivered-work snapshot,
	// while the HTTP surface is still live for the rest of the drain.
	cancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("watch.Run did not return after the application context was cancelled")
	}

	if rec := beatRequest(t, h, http.MethodPost, "/beat/"+id); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST /beat/%s after the watch loop stopped = %d, want 503 (body %s)", id, rec.Code, rec.Body.String())
	}

	// Give the recovery channel a reader again. Anything the ping queued would
	// be delivered on this loop's very next select; a long tick keeps its sweep
	// out of the way so a delivered recovery can only have come from the queue.
	// The wait for ABSENCE is bounded rather than deterministic: a ready channel
	// is consumed on the loop's first select, so with the guard reverted this
	// breaks out in microseconds (verified by reverting it).
	drainCtx, stopDrain := context.WithCancel(context.Background())
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		watcher.Run(drainCtx, time.Hour)
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, recovered, _ := notifier.counts(); recovered > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	stopDrain()
	select {
	case <-drainDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the second watch.Run did not return after cancellation")
	}

	missing, recovered, history := notifier.counts()
	if recovered != 0 {
		t.Errorf("recovered notifications delivered = %d, want 0: a ping refused during shutdown must queue no recovery, or the notice dies in a channel with no reader and nothing counts it",
			recovered)
	}
	if missing != 1 || history != 0 {
		t.Errorf("delivered messages = missing %d, history %d, want missing 1, history 0: the refused ping must not have changed the outage's delivery state either",
			missing, history)
	}
}
