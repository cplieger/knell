package webapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/cplieger/knell/internal/metrics"
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
// liveness endpoint; token gates the beat endpoint exactly as in production
// ("" = open).
func newTestHandler(b *fakeBeater, token string) http.Handler {
	return newTestHandlerHealthz(b, token, http.StatusOK)
}

// newTestHandlerHealthz is newTestHandler with the liveness status under test
// control, so a test can drive a FAILING probe: health.Handler answers 503
// whenever the liveness marker is absent (boot, or after the pre-drain flip).
func newTestHandlerHealthz(b *fakeBeater, token string, healthzStatus int) http.Handler {
	healthz := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(healthzStatus)
	})
	return New(b, token, Routes{Healthz: healthz, Metrics: metrics.Registry.Handler()})
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
	// Touch the asserted metrics so their series exist even when this
	// package's tests run in isolation (labeled metrics emit no output
	// until a first series is recorded).
	metrics.BeatsReceived.Add(0, "webapi-test")
	metrics.BeatFresh.Set(1, "webapi-test")
	metrics.NotificationsSent.Add(0, "missing")

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
			h := newTestHandlerHealthz(&fakeBeater{known: map[string]bool{"api": true}}, "", tt.healthzStatus)

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
	h := New(panicBeater{}, "", Routes{
		Healthz: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		Metrics: metrics.Registry.Handler(),
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
