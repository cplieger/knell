package webapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

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
	//
	// The exact-read oracle is one byte PAST the cap, and that byte is the
	// point of the http.MaxBytesReader the drain is built on
	// (webhttp.LimitBody): it is what distinguishes a body that ended exactly
	// at the cap from one that runs past it, so the overrun can be detected
	// instead of read as a clean end-of-body. A bare io.LimitReader stops at
	// 1 MiB exactly and cannot tell the two apart.
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, "")
	body := &unboundedReader{}
	req := httptest.NewRequest(http.MethodPost, "/beat/api", body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if body.n != 1<<20+1 {
		t.Errorf("drained %d bytes, want exactly 1 MiB + 1 (drain must happen for connection reuse, stop at the documented cap, and read one byte past it to detect the overrun)", body.n)
	}
	// An over-limit body is refused as a body, never as a ping: the payload is
	// irrelevant to a heartbeat, so the beat is still recorded.
	if len(b.seen) != 1 {
		t.Errorf("recorded beats = %v, want exactly one: an oversized payload must not cost a legitimate sender its ping", b.seen)
	}
}

// TestBeatOverLimitBodyWarnsAndStillRecords pins what an over-limit payload is
// observable as, which is the whole reason the drain caps with
// webhttp.LimitBody (http.MaxBytesReader) instead of io.LimitReader: the
// overrun becomes an *http.MaxBytesError the handler can report, where a
// LimitReader would have ended the read as if the body simply stopped there and
// nothing would ever say a sender is shipping payloads knell refuses to read.
//
// The report is a WARN and nothing else. The sender still gets its 200 and the
// beat is still recorded, because a heartbeat's payload is irrelevant and an
// oversized body must never turn a legitimate ping into a lost one — on a
// dead-man's switch a lost ping is a false alert or, worse, a false all-clear.
// An in-cap ping is silent, so the line means what it says.
func TestBeatOverLimitBodyWarnsAndStillRecords(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default.
	logs := capture.Default(t)
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, "")
	const warning = "beat body exceeded the cap"

	// A normal-sized payload is drained without a word about it.
	inCap := httptest.NewRecorder()
	h.ServeHTTP(inCap, httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader(strings.Repeat("x", 4096))))
	if inCap.Code != http.StatusOK {
		t.Fatalf("in-cap ping = %d, want 200 (body %s)", inCap.Code, inCap.Body.String())
	}
	if got := logs.CountLevel(slog.LevelWarn, warning); got != 0 {
		t.Errorf("in-cap ping produced %d body warnings, want 0: an ordinary payload must not warn, or the line means nothing when it fires: %v", got, logs.Messages())
	}

	// One byte past the cap is enough: the endless reader runs the drain into
	// the limit.
	over := httptest.NewRecorder()
	h.ServeHTTP(over, httptest.NewRequest(http.MethodPost, "/beat/api", &unboundedReader{}))
	if over.Code != http.StatusOK {
		t.Fatalf("over-limit ping = %d, want 200: an oversized payload must not cost a legitimate sender its ping (body %s)",
			over.Code, over.Body.String())
	}
	if got := logs.CountLevel(slog.LevelWarn, warning); got != 1 {
		t.Errorf("over-limit body warnings at Warn = %d, want exactly 1 (the overrun must be reported, not silently swallowed): %v", got, logs.Messages())
	}
	if !logs.HasAttr(warning, "limit_bytes", strconv.Itoa(1<<20)) {
		t.Errorf("warning does not report the 1 MiB cap that was exceeded; records = %v", logs.Records())
	}
	// The line carries no beat id (the id is still an unvalidated path segment
	// at that point), so the request id is what ties it to the access line.
	if id, ok := logs.AttrValue(warning, "request_id"); !ok || id == "" {
		t.Errorf("warning has no request_id (%q, present=%v); without it an operator cannot tie the overrun to the access line that names the path", id, ok)
	}
	if len(b.seen) != 2 {
		t.Errorf("recorded beats = %v, want both pings recorded", b.seen)
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

// TestEveryRejectedMethodAnswersTheSameRefusal pins that the Allow header is
// TRUE for every rejected method, not just for HEAD. Without the
// method-agnostic /beat/{id} route, PUT/DELETE/PATCH/OPTIONS fall to
// net/http's built-in 405, which assembles Allow from the registered patterns
// and so answers "GET, HEAD, POST" — advertising as permitted the one method
// this file registers a route for in order to REFUSE (a HEAD that recorded a
// ping would keep the switch armed with no heartbeat behind it). A prober that
// discovers methods from Allow would be steered straight at it.
func TestEveryRejectedMethodAnswersTheSameRefusal(t *testing.T) {
	for _, method := range []string{
		http.MethodHead, http.MethodPut, http.MethodDelete,
		http.MethodPatch, http.MethodOptions, "WHATEVER",
	} {
		t.Run(method, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{"api": true}}
			h := newTestHandler(b, "")
			req := httptest.NewRequest(method, "/beat/api", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s /beat/api = %d, want 405", method, rec.Code)
			}
			if got := rec.Header().Get("Allow"); got != "GET, POST" {
				t.Errorf("%s /beat/api Allow = %q, want \"GET, POST\": a rejected method must not be told that a method which does not record a beat is permitted", method, got)
			}
			// The coded JSON envelope, not net/http's plain "Method Not
			// Allowed\n": every other refusal on this surface (401, 404, 405,
			// 503) is the same shape, so a sender parsing it must not hit
			// unparseable text on exactly these methods.
			if !strings.Contains(rec.Body.String(), `"method_not_allowed"`) {
				t.Errorf("%s /beat/api body = %q, want the coded method_not_allowed envelope", method, rec.Body.String())
			}
			// Nothing may be recorded on a refused method: the id feeds a
			// metric label, and a recorded ping would re-arm the switch.
			if len(b.seen) != 0 {
				t.Errorf("%s /beat/api recorded %v, want nothing recorded on a refused method", method, b.seen)
			}
		})
	}
}

// TestAccessLogPathIsBounded pins the access log's path policy. r.URL.Path is
// attacker-controlled and reaches the log line BEFORE the token gate runs
// (webhttp.Logging is outermost), so an unauthenticated caller would otherwise
// size knell's log lines at will and could push the undelivered-notice
// warnings — the only trace of a permanently lost notice, which has no counter
// behind it — out of the retained log window. Two halves: every legitimate
// path is logged UNCHANGED (the fix must not cost an operator the beat id), and
// an over-long one is truncated on a rune boundary (never mid-rune, which
// would put invalid UTF-8 into the log stream).
func TestAccessLogPathIsBounded(t *testing.T) {
	// A 3-byte rune after a 6-byte prefix guarantees the byte at the cap
	// lands MID-RUNE (6 + 3k never equals 128), which is exactly the case a
	// naive p[:128] would corrupt.
	longMultibyte := "/beat/" + strings.Repeat("€", 200)

	tests := map[string]struct {
		path      string
		wantExact string // "" = expect truncation instead
	}{
		"healthz unchanged":   {path: "/healthz", wantExact: "/healthz"},
		"metrics unchanged":   {path: "/metrics", wantExact: "/metrics"},
		"beat ping unchanged": {path: "/beat/api", wantExact: "/beat/api"},
		// The longest id internal/config accepts (64 chars) is still well
		// under the cap, so a real deployment never sees truncation.
		"longest configurable id unchanged": {
			path:      "/beat/" + strings.Repeat("a", 64),
			wantExact: "/beat/" + strings.Repeat("a", 64),
		},
		// Exactly at the cap: still verbatim (the bound is inclusive).
		"path exactly at the cap unchanged": {
			path:      "/beat/" + strings.Repeat("a", maxLoggedPath-6),
			wantExact: "/beat/" + strings.Repeat("a", maxLoggedPath-6),
		},
		"one byte over the cap is truncated": {
			path: "/beat/" + strings.Repeat("a", maxLoggedPath-5),
		},
		"megabyte path is truncated":  {path: "/beat/" + strings.Repeat("a", 1<<20)},
		"multibyte path is truncated": {path: longMultibyte},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			// Serial (no t.Parallel): capture.Default swaps the process-global
			// slog default, and it must be installed BEFORE New because
			// webhttp.Logging resolves slog.Default() when the chain is built.
			rec := capture.Default(t)
			h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, "")

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			h.ServeHTTP(httptest.NewRecorder(), req)

			logged, ok := rec.AttrValue("http", "path")
			if !ok {
				t.Fatalf("no path attribute on the access line; records = %v", rec.Records())
			}
			// Fail-closed placeholder: webhttp substitutes it when the
			// transform returns "" or panics. Seeing it means the policy
			// broke, and the raw path is then lost to the operator entirely.
			if logged == "(path-redaction-failed)" {
				t.Fatalf("path = %q: the path policy failed instead of truncating", logged)
			}
			if tt.wantExact != "" {
				if logged != tt.wantExact {
					t.Errorf("path = %q, want the legitimate path logged verbatim (%q)", logged, tt.wantExact)
				}
				return
			}
			if len(logged) > maxLoggedPath+len("...(truncated)") {
				t.Errorf("logged path is %d bytes, want it bounded by the %d-byte cap plus the marker: an unauthenticated caller must not size log lines",
					len(logged), maxLoggedPath)
			}
			if !strings.HasSuffix(logged, "...(truncated)") {
				t.Errorf("path = %q, want a truncation marker so an operator can tell a bounded path from a real one", logged)
			}
			if !utf8.ValidString(logged) {
				t.Errorf("path = %q is not valid UTF-8: truncation must land on a rune boundary", logged)
			}
			// The kept prefix must be a genuine prefix of the request path,
			// so the beat id an operator needs is still readable.
			if !strings.HasPrefix(tt.path, strings.TrimSuffix(logged, "...(truncated)")) {
				t.Errorf("path = %q is not a prefix of the request path: truncation must not alter what it keeps", logged)
			}
		})
	}
}

// TestLoggedPathMapsTheEmptyPathToRoot covers the one case an
// httptest.NewRequest cannot express: r.URL.Path == "". The transform must not
// return "", because webhttp reads an empty return as a FAILED redaction and
// records its placeholder instead, losing the access record's path entirely.
func TestLoggedPathMapsTheEmptyPathToRoot(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.URL.Path = ""
	if got := loggedPath(req); got != "/" {
		t.Errorf("loggedPath(empty) = %q, want \"/\": an empty return degrades to webhttp's redaction-failure placeholder", got)
	}
}

// FuzzLoggedPathIsBounded fuzzes the untrusted text the access-log path policy
// sanitizes. r.URL.Path is attacker-controlled, net/http accepts a megabyte of
// it, and loggedPath runs BEFORE the token gate, so the transform is the only
// thing bounding an unauthenticated caller's influence on knell's log lines --
// the channel that carries the undelivered-notice warnings no counter backs.
// TestAccessLogPathIsBounded pins named shapes; this pins the invariant set
// over arbitrary bytes, including the class that table has none of: a path
// carrying raw non-UTF-8 bytes (%80 decodes to one), where the rune-boundary
// backoff can walk the cut all the way to zero.
func FuzzLoggedPathIsBounded(f *testing.F) {
	const marker = "...(truncated)"
	f.Add("/beat/api")
	f.Add("")
	f.Add("/")
	f.Add("/beat/" + strings.Repeat("a", maxLoggedPath-6))
	f.Add("/beat/" + strings.Repeat("a", maxLoggedPath-5))
	f.Add("/beat/" + strings.Repeat("\u20ac", 200))
	f.Add("/beat/" + strings.Repeat("\U0001F600", 100))
	f.Add("/beat/" + strings.Repeat("\x80", 200))
	f.Add(strings.Repeat("\x80", 200))
	f.Fuzz(func(t *testing.T, path string) {
		got := loggedPath(&http.Request{URL: &url.URL{Path: path}})

		// An empty return is read by webhttp as a FAILED redaction, which
		// replaces the whole attribute with its placeholder: the operator
		// then has no path at all, for any input.
		if got == "" {
			t.Fatalf("loggedPath(%q) = %q: an empty return degrades to webhttp's redaction-failure placeholder", path, got)
		}
		if len(got) > maxLoggedPath+len(marker) {
			t.Fatalf("loggedPath(%q) is %d bytes, want at most %d: an unauthenticated caller must not size log lines", path, len(got), maxLoggedPath+len(marker))
		}
		// Truncating mid-rune would put invalid UTF-8 into the log stream.
		if utf8.ValidString(path) && !utf8.ValidString(got) {
			t.Fatalf("loggedPath(%q) = %q is not valid UTF-8: truncation must land on a rune boundary", path, got)
		}
		switch {
		case path == "":
			if got != "/" {
				t.Fatalf("loggedPath(empty) = %q, want %q", got, "/")
			}
		case len(path) <= maxLoggedPath:
			if got != path {
				t.Fatalf("loggedPath(%q) = %q, want it logged verbatim: the bound must not cost an operator the beat id", path, got)
			}
		default:
			kept, ok := strings.CutSuffix(got, marker)
			if !ok {
				t.Fatalf("loggedPath(%q) = %q, want the truncation marker so an operator can tell a bounded path from a real one", path, got)
			}
			if !strings.HasPrefix(path, kept) {
				t.Fatalf("loggedPath(%q) kept %q, which is not a prefix of the request path: truncation must not alter what it keeps", path, kept)
			}
		}
	})
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

// deliveringNotifier is a watch.Notifier that delivers everything successfully.
// A delivered send is what flips a beat to alerted, so a watcher wired to it
// reaches the states a live deployment reaches. It counts nothing: the tests
// below assert on the exposition, which is the operator-visible ground truth.
type deliveringNotifier struct{}

func (n *deliveringNotifier) BeatMissing(context.Context, string, watch.Transition) error {
	return nil
}

func (n *deliveringNotifier) BeatRecovered(context.Context, string, watch.Transition) error {
	return nil
}

func (n *deliveringNotifier) BeatOutageHistory(context.Context, string, []watch.Outage) error {
	return nil
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
	watcher := watch.New([]watch.Beat{{ID: id, Deadline: time.Minute}}, &deliveringNotifier{}, clock.Now, start)

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

// TestBeatRefusedWhenCancellationHappensDuringBodyDrain pins the lifecycle
// check after the ignored request body is drained. The pipe write proves the
// handler passed the first context check and entered the body read before
// cancellation, so a 503 can only come from the post-drain check.
func TestBeatRefusedWhenCancellationHappensDuringBodyDrain(t *testing.T) {
	appCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandlerCtx(appCtx, b, "")

	bodyReader, bodyWriter := io.Pipe()
	req := httptest.NewRequest(http.MethodPost, "/beat/api", bodyReader)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(rec, req)
	}()

	wrote := make(chan error, 1)
	go func() {
		_, err := bodyWriter.Write([]byte("ping"))
		wrote <- err
	}()
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatalf("write request body: %v", err)
		}
	case <-done:
		// The write is a barrier, not a fixture: if the handler ever refuses
		// BEFORE reading the body, nothing drains the pipe and a synchronous
		// write here would block until the go-test timeout killed the whole
		// package. Racing it against handler completion turns that regression
		// into this named failure. Reading rec.Code is safe: the handler
		// goroutine has finished.
		t.Fatalf("handler returned without reading the body (status %d): the barrier write would have deadlocked", rec.Code)
	}
	cancel()
	if err := bodyWriter.Close(); err != nil {
		t.Fatalf("close request body: %v", err)
	}
	<-done

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST canceled during body drain = %d, want 503 (body %s)", rec.Code, rec.Body.String())
	}
	if len(b.seen) != 0 {
		t.Fatalf("beats recorded after cancellation during body drain = %v, want none", b.seen)
	}
	if !strings.Contains(rec.Body.String(), "shutting_down") {
		t.Errorf("503 body = %s, want the shutting_down code", rec.Body.String())
	}
}

// httpRequestSeries returns every knell_http_requests_total label-set present in
// the exposition, keyed by its raw "{...}" text. Used to assert what the metric
// CAN grow into, not just what one request recorded.
func httpRequestSeries(exposition string) map[string]bool {
	out := map[string]bool{}
	for line := range strings.SplitSeq(exposition, "\n") {
		if !strings.HasPrefix(line, "knell_http_requests_total{") {
			continue
		}
		labels, _, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		out[labels] = true
	}
	return out
}

// TestRefusedPingIsVisibleInTheRequestCounter is the whole point of the request
// metric: knell_beats_received_total counts ACCEPTED pings only, so before this
// counter existed every refusal — a rotated BEAT_TOKEN (401), a misspelled beat
// id (404), a disallowed method (405), a ping during the drain (503) — was
// invisible to a scrape and could only be found by reading access logs. knell's
// own README points alert rules at a second vantage point scraping /metrics, so
// a refusal class with no series behind it cannot be alerted on at all: the
// operator's first hard signal would be the missing notice a full deadline
// later. Each case pins that the refusal lands on its own method/path/status
// series AND that it still records no beat.
func TestRefusedPingIsVisibleInTheRequestCounter(t *testing.T) {
	const id = "api"

	// cancelledCtx is the drain state: the shared application context is
	// cancelled, so beat acceptance is closed for the rest of the process.
	cancelledCtx := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}

	tests := map[string]struct {
		token      string
		ctx        context.Context
		method     string
		path       string
		wantStatus int
		wantSeries string
		wantSeen   int
	}{
		// The accepted path first: it proves the counter is not simply
		// counting everything as a refusal, and it is the denominator an
		// operator compares a refusal rate against.
		"accepted ping": {
			ctx: context.Background(), method: http.MethodPost, path: "/beat/" + id,
			wantStatus: http.StatusOK, wantSeen: 1,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{id}",status="200"}`,
		},
		// A rotated or mistyped BEAT_TOKEN. The gate is outermost, so this is
		// the refusal an unauthenticated caller reaches first.
		"unauthorized ping": {
			token: "s3cret", ctx: context.Background(), method: http.MethodPost, path: "/beat/" + id,
			wantStatus: http.StatusUnauthorized,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{id}",status="401"}`,
		},
		// A typo in a sender's URL: the beat stays silent while the sender
		// believes it is pinging.
		"unknown beat id": {
			ctx: context.Background(), method: http.MethodPost, path: "/beat/ghost",
			wantStatus: http.StatusNotFound,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{id}",status="404"}`,
		},
		// HEAD has its own registered route (so a HEAD probe cannot record a
		// ping), and that route NAMES the method, so the label is truthful.
		"head refused": {
			ctx: context.Background(), method: http.MethodHead, path: "/beat/" + id,
			wantStatus: http.StatusMethodNotAllowed,
			wantSeries: `knell_http_requests_total{method="HEAD",path="/beat/{id}",status="405"}`,
		},
		// PUT falls to the method-AGNOSTIC /beat/{id} route, which names no
		// method — so the method label collapses to "other" rather than
		// echoing a caller-chosen token. See recordHTTPMetric.
		"disallowed method refused": {
			ctx: context.Background(), method: http.MethodPut, path: "/beat/" + id,
			wantStatus: http.StatusMethodNotAllowed,
			wantSeries: `knell_http_requests_total{method="other",path="/beat/{id}",status="405"}`,
		},
		// A ping arriving during the drain, after watch.Run already took its
		// undelivered-work snapshot.
		"ping during drain": {
			ctx: cancelledCtx(), method: http.MethodPost, path: "/beat/" + id,
			wantStatus: http.StatusServiceUnavailable,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{id}",status="503"}`,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{id: true}}
			h := newTestHandlerCtx(tt.ctx, b, tt.token)

			// Deltas, not absolutes: the registry is a package-level singleton
			// shared by the whole test binary (and by a -count=2 rerun).
			before, _ := seriesValue(t, scrapeExposition(t, h), tt.wantSeries)

			rec := beatRequest(t, h, tt.method, tt.path)
			if rec.Code != tt.wantStatus {
				t.Fatalf("%s %s = %d, want %d (body %s)", tt.method, tt.path, rec.Code, tt.wantStatus, rec.Body.String())
			}

			exposition := scrapeExposition(t, h)
			got, ok := seriesValue(t, exposition, tt.wantSeries)
			if !ok {
				t.Fatalf("%s missing from the exposition after %s %s answered %d: this refusal class is unalertable without it\n%s",
					tt.wantSeries, tt.method, tt.path, tt.wantStatus, exposition)
			}
			if got != before+1 {
				t.Errorf("%s = %v, want %v (one request, one increment)", tt.wantSeries, got, before+1)
			}
			if len(b.seen) != tt.wantSeen {
				t.Errorf("recorded beats = %v, want %d: the request counter must not change what gets recorded", b.seen, tt.wantSeen)
			}
		})
	}
}

// TestRequestMetricLabelsBoundedByTheRouteTable is the cardinality guard on the
// new counter, and the reason recordHTTPMetric derives BOTH labels from the
// matched route instead of from the request. webhttp.Logging is outermost, so
// this hook fires before beatHandler's token gate: the inputs below arrive from
// an UNAUTHENTICATED caller, and a Prometheus series once minted is permanent
// for the process lifetime here and in every observer scraping knell. So the
// label set must be bounded by the ROUTE TABLE and by nothing the caller sends.
//
// Two attack shapes, and knell is uniquely exposed to the second. The path is
// the obvious one. The METHOD is the one the fleet siblings do not face:
// registry-stats and subflux register only method-bearing patterns, so their
// r.Method is bounded by the mux, while knell deliberately registers a
// method-agnostic /beat/{id} catch-all (so a 405 can carry a truthful Allow),
// and net/http routes ANY valid token there — "XYZZY" and friends reach it and
// answer 405. Recording r.Method there would hand a scanner an unbounded label.
func TestRequestMetricLabelsBoundedByTheRouteTable(t *testing.T) {
	// The complete label vocabulary knell's route table can produce. Anything
	// outside this is a caller-controlled value that reached a label.
	allowedMethods := map[string]bool{
		"GET": true, "POST": true, "HEAD": true,
		otherMethodLabel: true, unmatchedLabel: true,
	}
	allowedPaths := map[string]bool{
		"/beat/{id}": true, "/healthz": true, "/metrics": true,
		unmatchedLabel: true,
	}

	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandlerCtx(context.Background(), b, "")
	before := httpRequestSeries(scrapeExposition(t, h))

	// Every hostile token below is distinct, so a naive implementation mints a
	// new series per request and the assertions fail loudly rather than subtly.
	var hostile []struct{ method, path string }
	add := func(method, path string) {
		hostile = append(hostile, struct{ method, path string }{method, path})
	}
	for i := range 12 {
		suffix := strconv.Itoa(i)
		// Distinct beat ids: all must collapse onto the /beat/{id} TEMPLATE,
		// known and unknown alike (an unknown id answers 404 and must mint
		// nothing, the same rule that protects the per-beat series).
		add(http.MethodPost, "/beat/ghost"+suffix)
		// Distinct method tokens on the method-agnostic route.
		add("XYZZY"+suffix, "/beat/api")
		// Distinct unmatched paths: scanner traffic.
		add(http.MethodGet, "/wp-admin/"+suffix+"/setup.php")
		// A distinct method AND a distinct unmatched path at once.
		add("BLARG"+suffix, "/nope/"+suffix)
	}
	// Percent-encoded and traversal-shaped spellings of a real route: r.URL.Path
	// varies without bound while the pattern does not.
	add(http.MethodPost, "/beat/%61pi")
	add(http.MethodGet, "/beat/a%2Fb")
	add(http.MethodGet, "//healthz")
	add(http.MethodGet, "/./metrics")

	for _, req := range hostile {
		beatRequest(t, h, req.method, req.path)
	}

	exposition := scrapeExposition(t, h)
	after := httpRequestSeries(exposition)

	// Nothing a caller sent may appear anywhere in the exposition. This catches
	// a label leak even if the membership checks below were relaxed.
	for _, req := range hostile {
		for _, token := range []string{req.method, req.path} {
			if !allowedMethods[token] && !allowedPaths[token] && strings.Contains(exposition, token) {
				t.Errorf("exposition contains the caller-supplied token %q: a request value reached a metric label\n%s", token, exposition)
			}
		}
	}

	// Every series, pre-existing or new, must be spelled from the route table.
	for series := range after {
		labels := strings.TrimSuffix(strings.TrimPrefix(series, "knell_http_requests_total{"), "}")
		for pair := range strings.SplitSeq(labels, ",") {
			name, value, ok := strings.Cut(pair, "=")
			if !ok {
				t.Errorf("unparseable label pair %q in %q", pair, series)
				continue
			}
			value = strings.Trim(value, `"`)
			switch name {
			case "method":
				if !allowedMethods[value] {
					t.Errorf("series %s carries method=%q, which is not one the route table can produce: an unauthenticated caller can mint series without bound", series, value)
				}
			case "path":
				if !allowedPaths[value] {
					t.Errorf("series %s carries path=%q, which is not a registered route template", series, value)
				}
			}
		}
	}

	// The route table can produce at most 5 methods x 4 paths x the handful of
	// statuses knell answers; 52 hostile requests must land inside that, not
	// grow it. The bound is deliberately loose — the membership checks above are
	// the precise guard, this catches unbounded GROWTH regardless of spelling.
	const maxNewSeries = 8
	if grew := len(after) - len(before); grew > maxNewSeries {
		t.Errorf("%d hostile requests added %d new series (want at most %d): the label set must be bounded by the route table, not by the caller\n%s",
			len(hostile), grew, maxNewSeries, exposition)
	}
}

// TestUnroutedRequestsAreCountedUnderTheCollapsedSeries pins the FIRST case of
// recordHTTPMetric's contract: a request that matched no route is still
// counted, under the single collapsed unmatched/unmatched label set.
// TestRequestMetricLabelsBoundedByTheRouteTable only proves such a request
// cannot MINT a caller-spelled series, so a hook that skipped unmatched
// requests entirely passes it -- and then scanner floods and every misrouted
// sender (a wrong-method scrape included) are invisible to the vantage point
// knell's own alert rules read.
func TestUnroutedRequestsAreCountedUnderTheCollapsedSeries(t *testing.T) {
	const series = `knell_http_requests_total{method="unmatched",path="unmatched",status=`
	tests := map[string]struct {
		method, path string
		wantStatus   string
	}{
		// Scanner traffic: no pattern matches at all, so net/http answers 404.
		"off-route probe": {method: http.MethodGet, path: "/wp-admin/setup.php", wantStatus: "404"},
		// A scrape aimed with the wrong method: /metrics is registered GET-only,
		// so net/http's own 405 fires with no pattern matched.
		"wrong-method scrape": {method: http.MethodPost, path: "/metrics", wantStatus: "405"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{"api": true}}
			h := newTestHandlerCtx(context.Background(), b, "")
			want := series + `"` + tt.wantStatus + `"}`
			// Deltas, not absolutes: the registry is a package-level singleton
			// shared by the whole test binary (and by a -count=2 rerun).
			before, _ := seriesValue(t, scrapeExposition(t, h), want)

			if rec := beatRequest(t, h, tt.method, tt.path); rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s %s = %d, want an unrouted refusal (body %s)", tt.method, tt.path, rec.Code, rec.Body.String())
			}
			got, ok := seriesValue(t, scrapeExposition(t, h), want)
			if !ok {
				t.Fatalf("%s missing from the exposition after %s %s: unrouted traffic is unalertable without it", want, tt.method, tt.path)
			}
			if got != before+1 {
				t.Errorf("%s = %v, want %v (one unrouted request, one increment)", want, got, before+1)
			}
		})
	}
}

// TestAccessLogMethodIsBoundedForRefusedRequests pins boundMethod. The access
// log's method is as caller-controlled as its path, and webhttp logs r.Method
// verbatim with no transform hook, so the cap has to sit OUTSIDE Logging.
// net/http accepts any RFC 9110 token as a method with no length cap of its
// own, and the request line is bounded only by MaxHeaderBytes+4096 (1 MiB by
// default), so without this cap one unauthenticated caller writes ~1 MiB of its
// own text into a single access line and pushes knell's permanently-lost-notice
// WARNs out of the retained log window -- the same consequence maxLoggedPath
// exists to prevent, on a request the token gate never sees (Logging is
// outermost). The metric side of the same request is already bounded
// (otherMethodLabel), which is what left the log as the last unbounded sink.
func TestAccessLogMethodIsBoundedForRefusedRequests(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and must be installed BEFORE New, because webhttp.Logging
	// resolves slog.Default() when the chain is built.
	logs := capture.Default(t)
	h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, "")

	overlong := strings.Repeat("A", maxLoggedMethod+4)
	rec := beatRequest(t, h, overlong, "/beat/api")

	// The refusal itself is unchanged: the bogus method still routes to the
	// method-agnostic catch-all, which answers 405 with a truthful Allow.
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("%d-byte bogus method = %d, want 405 (body %s)", len(overlong), rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Allow"); got != "GET, POST" {
		t.Errorf("Allow = %q, want \"GET, POST\": bounding the logged method must not change the refusal", got)
	}

	// The logged method is the placeholder, never the caller's bytes.
	if !logs.HasAttr("http", "method", overlongMethodLabel) {
		t.Errorf("access line does not report method=%s; records = %v", overlongMethodLabel, logs.Records())
	}
	if logs.HasAttr("http", "method", overlong) {
		t.Errorf("access line carries the caller's %d-byte method verbatim: an unauthenticated caller writes the text of knell's own log lines; records = %v",
			len(overlong), logs.Records())
	}

	// A real method passes through untouched: this is a bound, not a rewrite.
	if got := beatRequest(t, h, http.MethodPost, "/beat/api"); got.Code != http.StatusOK {
		t.Fatalf("post known = %d, want 200 (body %s)", got.Code, got.Body.String())
	}
	if !logs.HasAttr("http", "method", http.MethodPost) {
		t.Errorf("access line does not report method=POST for an ordinary ping; records = %v", logs.Records())
	}
}
