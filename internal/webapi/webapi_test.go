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
	"unicode/utf8"

	"github.com/cplieger/knell/internal/metrics"
	"github.com/cplieger/knell/internal/watch"
	"github.com/cplieger/slogx/capture"
	"github.com/cplieger/webhttp"
)

// fakeBeater accepts a fixed id set and records what was recorded. closed
// stands in for a watcher that has shut admission (watch.Watcher.stopAccepting).
type fakeBeater struct {
	known  map[string]bool
	seen   []string
	closed bool
}

func (f *fakeBeater) Beat(id string) (recorded, accepting bool) {
	if f.closed {
		return false, false
	}
	if !f.known[id] {
		return false, true
	}
	f.seen = append(f.seen, id)
	return true, true
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
	return New(appCtx, b, Deps{Healthz: staticHealthz(healthzStatus), BeatToken: token})
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
		// The bare /beat prefix has its own test with the stronger oracle:
		// TestBareBeatPathAnswersTheCodedNotFound.
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
			// EVERY 404 on this surface carries the coded envelope, the two
			// ROUTE-level spellings included (/beat/ and /beat/{id}/...). That is
			// the whole point of the /beat/{$} and /beat/{id}/{rest...} routes:
			// without them net/http answers plain "404 page not found". Gating
			// this on one path left both routes deletable with the suite green.
			if tt.wantStatus == http.StatusNotFound &&
				!strings.Contains(rec.Body.String(), "unknown_beat") {
				t.Errorf("%s 404 body = %s, want the unknown_beat coded envelope", tt.path, rec.Body.String())
			}
		})
	}
}

// TestBareBeatPathAnswersTheCodedNotFound pins the bare /beat prefix, the one
// misconfigured sender URL net/http used to answer for knell: with only
// /beat/{$} and the {id} patterns registered, the mux synthesized a 307 to
// /beat/ for it. 307 is a SUCCESS status, so the documented `curl -fsS` sender
// (an unset or truncated variable in the URL) exited 0 having recorded nothing,
// and the beat read as missing a full deadline later with nothing anywhere
// saying the URL was wrong. So this pins the refusal on both accepted methods:
// the coded 404 body, no redirect, nothing recorded, and no per-beat series
// minted from a path that names no beat.
func TestBareBeatPathAnswersTheCodedNotFound(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{"api": true}}
			h := newTestHandler(b, "")
			beatSeriesBefore := beatSeriesLines(scrapeExposition(t, h))

			rec := beatRequest(t, h, method, "/beat")

			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s /beat = %d, want 404: a 3xx is a SUCCESS status to `curl -fsS`, so a truncated sender URL would exit 0 having recorded nothing (body %s)",
					method, rec.Code, rec.Body.String())
			}
			if got := rec.Header().Get("Location"); got != "" {
				t.Errorf("%s /beat carries Location: %q, want no redirect", method, got)
			}
			if !strings.Contains(rec.Body.String(), "unknown_beat") {
				t.Errorf("%s /beat body = %s, want the unknown_beat coded envelope a sender can parse", method, rec.Body.String())
			}
			if len(b.seen) != 0 {
				t.Errorf("recorded beats = %v, want none: a path that names no beat must never record one", b.seen)
			}
			// The path names no beat, so it must mint no per-beat series: the
			// beat id is a metric label, and a series is permanent for the
			// process lifetime here and in every observer scraping knell.
			exposition := scrapeExposition(t, h)
			if got, before := beatSeriesLines(exposition), beatSeriesBefore; len(got) != len(before) {
				t.Errorf("%s /beat changed the per-beat series set (%d -> %d): a bare prefix must mint nothing\n%s",
					method, len(before), len(got), exposition)
			}
			// The refusal is still counted, under the registered pattern rather
			// than the "unmatched" bucket, so a misconfigured sender is
			// distinguishable from scanner traffic.
			series := `knell_http_requests_total{method="` + method + `",path="/beat",status="404"}`
			if _, ok := seriesValue(t, exposition, series); !ok {
				t.Errorf("%s missing from the exposition: a truncated sender URL is unalertable without it\n%s", series, exposition)
			}
		})
	}
}

// beatSeriesLines returns every per-beat series line in an exposition, so a test
// can pin that a refusal minted no new one.
func beatSeriesLines(exposition string) []string {
	var lines []string
	for line := range strings.SplitSeq(exposition, "\n") {
		if strings.HasPrefix(line, "knell_beat") {
			lines = append(lines, line)
		}
	}
	return lines
}

// TestNonCanonicalBeatPathsAnswerTheCodedNotFound pins the redirect class no
// route pattern can close. ServeMux sanitizes repeated slashes and literal dot
// segments in a pass that runs BEFORE pattern matching, so without
// canonicalBeatPath every spelling below answers 307 + Location — and a 307 is
// a SUCCESS status to the documented `curl -fsS` sender, which does not follow
// redirects unless asked. Such a sender exits 0 having recorded nothing, and
// the beat reads as missing one full deadline later with nothing saying the URL
// was malformed. Each case must instead be the coded 404, carry no Location,
// and record no beat.
func TestNonCanonicalBeatPathsAnswerTheCodedNotFound(t *testing.T) {
	// "//beat/api" enters the /beat namespace only after cleaning;
	// "/beat/api/../ghost" leaves one id for another; the rest are the
	// repeated-slash and dot-segment spellings a URL join produces.
	for _, target := range []string{"/beat//", "//beat/api", "/beat/./api", "/beat/api/../ghost", "/beat/api//"} {
		t.Run(target, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{"api": true}}
			h := newTestHandler(b, "")

			rec := beatRequest(t, h, http.MethodPost, target)

			if rec.Code != http.StatusNotFound {
				t.Fatalf("POST %s = %d, want 404: a 3xx is a SUCCESS status to `curl -fsS`, so a malformed sender URL would exit 0 having recorded nothing (body %s)",
					target, rec.Code, rec.Body.String())
			}
			if got := rec.Header().Get("Location"); got != "" {
				t.Errorf("POST %s carries Location: %q, want no redirect", target, got)
			}
			if !strings.Contains(rec.Body.String(), "unknown_beat") {
				t.Errorf("POST %s body = %s, want the unknown_beat coded envelope a sender can parse", target, rec.Body.String())
			}
			if len(b.seen) != 0 {
				t.Errorf("POST %s recorded %v, want nothing: a non-canonical path names no beat", target, b.seen)
			}
			// The refusal is counted under its OWN class, not in the "unmatched"
			// bucket it would otherwise share with port scans: knell's alert rules
			// read /metrics, so a malformed sender URL has to be tellable apart
			// from scanner traffic.
			series := `knell_http_requests_total{method="POST",path="` + nonCanonicalBeatPattern + `",status="404"}`
			if _, ok := seriesValue(t, scrapeExposition(t, h), series); !ok {
				t.Errorf("%s missing from the exposition: a malformed sender URL is unalertable without it", series)
			}
		})
	}

	// The canonical spelling still records: the guard must refuse the rewritten
	// shapes only, never a well-formed ping.
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, "")
	if rec := beatRequest(t, h, http.MethodPost, "/beat/api"); rec.Code != http.StatusOK {
		t.Fatalf("POST /beat/api = %d, want 200: the canonical ping must still record (body %s)", rec.Code, rec.Body.String())
	}
	if len(b.seen) != 1 {
		t.Errorf("recorded beats = %v, want one: the canonical ping must still record", b.seen)
	}
}

// FuzzBeatPathNeverRedirectsOrRecordsNonCanonically fuzzes the untrusted text
// canonicalBeatPath judges. r.URL.Path is attacker-controlled, and the guard is
// the only thing standing between a malformed sender URL and net/http's
// redirect pass: ServeMux sanitizes repeated slashes and dot segments BEFORE
// pattern selection, so without the guard every rewritten spelling answers 307
// + Location — a SUCCESS status to the documented `curl -fsS` sender, which
// then exits 0 having recorded nothing while the beat reads as missing one full
// deadline later.
//
// TestNonCanonicalBeatPathsAnswerTheCodedNotFound pins five NAMED spellings.
// This target pins the CLASS for arbitrary bytes, which is what a rewrite of
// sanitizedPath, inBeatNamespace, or the chain's ordering can break in a
// spelling no table names (a repeated slash behind a long id, a non-UTF-8 byte,
// a path that only ENTERS the namespace after cleaning). It drives the DECODED
// path only: the harness assigns r.URL.Path and leaves RawPath empty, so
// EscapedPath re-escapes every '%' and no percent-encoded segment reaches the
// mux — the "/beat/%2e%2e/ghost" seed is a literal-percent path here, not the
// encoded dot segment a real request carries (that spelling answers the same
// coded 404 either way; see canonicalBeatPath's own doc). Three invariants, all
// decided from the request alone:
//
//   - a request in (or cleaning into) the /beat namespace never answers a
//     redirect, so no sender can be told "success" without recording;
//   - its status stays inside the coded set knell answers there;
//   - a beat is recorded ONLY for the exact canonical spelling, so no rewritten
//     path can re-arm the switch under an id its sender did not name.
func FuzzBeatPathNeverRedirectsOrRecordsNonCanonically(f *testing.F) {
	for _, seed := range []string{
		"/beat/api", "/beat", "/beat/", "/beat//", "//beat/api", "/beat/./api",
		"/beat/api/../ghost", "/beat/api//", "/beat/api/", "/beat/%2e%2e/ghost",
		"/beat/api/../../beat/api", "/beat/../beat/api", "/beat/.", "/beat/..",
		"", "/", "/healthz", "/beat/" + strings.Repeat("a", 300),
		"/beat/\x80", "/beat/a\nb", "/beat/api/./", "/beat///api",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, path string) {
		// Serial by construction (the fuzz function runs one input at a time):
		// capture.Default swaps the process-global slog default, and keeps a
		// million-exec run from writing an access line per input to stderr.
		capture.Default(t)
		b := &fakeBeater{known: map[string]bool{"api": true}}
		h := newTestHandler(b, "")

		// Built by hand rather than via a target string: httptest.NewRequest
		// panics on a target it cannot parse, and arbitrary bytes are exactly
		// the input this target exists for.
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
		req.URL.Path = path
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		// Outside the beat namespace net/http keeps its own behavior, redirects
		// included (an empty path is cleaned to "/" with a 307): the guard
		// deliberately does not reach there.
		if !inBeatNamespace(path) && !inBeatNamespace(sanitizedPath(path)) {
			return
		}
		switch rec.Code {
		case http.StatusOK, http.StatusNotFound, http.StatusMethodNotAllowed:
		default:
			t.Fatalf("path %q = %d, want one of 200/404/405: every /beat spelling answers this file's coded envelope (body %s)",
				path, rec.Code, rec.Body.String())
		}
		if loc := rec.Header().Get("Location"); loc != "" {
			t.Fatalf("path %q answered %d with Location %q: a 3xx is a SUCCESS status to `curl -fsS`, so this sender would exit 0 having recorded nothing",
				path, rec.Code, loc)
		}
		if recorded := len(b.seen) != 0; recorded != (path == "/beat/api") {
			t.Fatalf("path %q recorded=%v (seen %v), want a beat recorded only for the canonical spelling: a rewritten path must never re-arm the switch",
				path, recorded, b.seen)
		}
	})
}

func TestMetricsExposition(t *testing.T) {
	// Declare a beat so the per-beat series exist even when this package's
	// tests run in isolation (labeled metrics emit no output until a first
	// series is recorded). The notification counters need no touch: the
	// metrics package pre-mints every kind at init.
	metrics.InitBeat("webapi-test", 20*time.Minute, time.Unix(0, 0))
	// The freshness verdict is published by the watch state machine, not by
	// InitBeat, so this test mints the gauge series itself.
	metrics.SetBeatFresh("webapi-test", true)

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

func (panicBeater) Beat(string) (bool, bool) { panic("beat exploded") }

func TestPanicUnderBeatHandlerAnswers500AndIsLogged(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and it must be installed BEFORE New, because webhttp resolves
	// slog.Default() when the chain is built.
	//
	// Without the chain's Recoverer, a panic under the beat handler unwinds to
	// net/http: the sender sees a reset connection rather than a 500, and the
	// access log never reports a status for the endpoint that feeds the switch.
	rec := capture.Default(t)
	h := New(context.Background(), panicBeater{}, Deps{
		Healthz: staticHealthz(http.StatusOK),
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
	clear(p)
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

// interruptedReader ends mid-body with a non-MaxBytesError, the shape a sender
// that disconnects part-way through its payload produces.
type interruptedReader struct{}

func (*interruptedReader) Read(p []byte) (int, error) {
	return copy(p, "partial"), io.ErrUnexpectedEOF
}

// TestBeatBodyReadFailureStillRecords pins that a body read failure other than
// the cap overrun does not cost the sender its ping: drainBeatBody deliberately
// ignores it, because a heartbeat's payload is irrelevant and treating a partial
// sender disconnect as fatal would discard a valid beat and, one deadline later,
// ring a false missing notice.
func TestBeatBodyReadFailureStillRecords(t *testing.T) {
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, "")
	req := httptest.NewRequest(http.MethodPost, "/beat/api", &interruptedReader{})
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ping with an interrupted body = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if len(b.seen) != 1 || b.seen[0] != "api" {
		t.Errorf("recorded beats = %v, want [api]: a body read failure must not discard the heartbeat", b.seen)
	}
	if !strings.Contains(rec.Body.String(), `"ok":true`) {
		t.Errorf("ok body = %s", rec.Body.String())
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
			// RFC 9110 §11.6.1: every 401 names its challenge, so a sender
			// reads the expected scheme off the protocol, not off the README.
			if tt.wantStatus == http.StatusUnauthorized {
				if got := rec.Header().Get("WWW-Authenticate"); got != "Bearer" {
					t.Errorf("WWW-Authenticate = %q, want %q", got, "Bearer")
				}
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
		http.MethodPatch, http.MethodOptions,
		// CONNECT is the one method net/http routes by another code path:
		// ServeMux skips path canonicalization for it and matches on
		// r.URL.Host, so it reaches the method-agnostic route without the
		// cleaning pass. It must still answer the same coded 405 with a
		// truthful Allow — a CONNECT routed to the GET/POST pattern would
		// RECORD a ping, and net/http's built-in 405 would advertise HEAD.
		http.MethodConnect, "WHATEVER",
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

// TestAccessLogPathIsBounded pins the access log's path bound, which knell now
// gets from webhttp (WithMaxLoggedPath(loggedPathCap)) rather than from a local
// transform. r.URL.Path is attacker-controlled and reaches the log line BEFORE
// the token gate runs (webhttp.Logging is outermost), so an unauthenticated
// caller would otherwise size knell's log lines at will and could push the
// undelivered-notice warnings — the only trace of a permanently lost notice,
// which has no counter behind it — out of the retained log window. This test
// covers the WIRING, which is the half that is still knell's: every legitimate
// path is logged UNCHANGED (the bound must not cost an operator the beat id),
// and an over-long one is truncated at knell's 128-byte figure rather than the
// library's 512-byte default, on a rune boundary (never mid-rune, which would
// put invalid UTF-8 into the log stream).
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
			path:      "/beat/" + strings.Repeat("a", loggedPathCap-6),
			wantExact: "/beat/" + strings.Repeat("a", loggedPathCap-6),
		},
		"one byte over the cap is truncated": {
			path: "/beat/" + strings.Repeat("a", loggedPathCap-5),
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
			// The fail-closed placeholder belongs to webhttp's path-POLICY hook
			// (WithPathFunc), which knell no longer installs — seeing it would
			// mean a policy crept back in and broke.
			if logged == "(path-redaction-failed)" {
				t.Fatalf("path = %q: a path policy failed instead of the cap truncating", logged)
			}
			if tt.wantExact != "" {
				if logged != tt.wantExact {
					t.Errorf("path = %q, want the legitimate path logged verbatim (%q)", logged, tt.wantExact)
				}
				return
			}
			if len(logged) > loggedPathCap+len(truncationMarker) {
				t.Errorf("logged path is %d bytes, want it bounded by the %d-byte cap plus the marker: an unauthenticated caller must not size log lines",
					len(logged), loggedPathCap)
			}
			if !strings.HasSuffix(logged, truncationMarker) {
				t.Errorf("path = %q, want a truncation marker so an operator can tell a bounded path from a real one", logged)
			}
			if !utf8.ValidString(logged) {
				t.Errorf("path = %q is not valid UTF-8: truncation must land on a rune boundary", logged)
			}
			// The kept prefix must be a genuine prefix of the request path,
			// so the beat id an operator needs is still readable.
			if !strings.HasPrefix(tt.path, strings.TrimSuffix(logged, truncationMarker)) {
				t.Errorf("path = %q is not a prefix of the request path: truncation must not alter what it keeps", logged)
			}
		})
	}
}

// FuzzLoggedPathIsBounded fuzzes the untrusted text the access line records as
// its path. r.URL.Path is attacker-controlled, net/http accepts a megabyte of
// it, and the access line is emitted BEFORE the token gate (webhttp.Logging is
// outermost), so the bound is the only thing limiting an unauthenticated
// caller's influence on knell's log lines -- the channel that carries the
// undelivered-notice warnings no counter backs.
//
// The bound itself is webhttp's now, so what this pins is knell's WIRING of it:
// that every request really does travel through a logger carrying
// WithMaxLoggedPath(loggedPathCap), for arbitrary bytes rather than only for the
// shapes TestAccessLogPathIsBounded names -- including the class that table has
// none of, a path carrying raw non-UTF-8 bytes (%80 decodes to one), where the
// rune-boundary backoff can walk the cut all the way to zero. It drives the real
// assembled handler because there is no local transform left to call, which
// also means a future middleware that re-wrote the path before Logging saw it
// would be caught here.
func FuzzLoggedPathIsBounded(f *testing.F) {
	f.Add("/beat/api")
	f.Add("")
	f.Add("/")
	f.Add("/beat/" + strings.Repeat("a", loggedPathCap-6))
	f.Add("/beat/" + strings.Repeat("a", loggedPathCap-5))
	f.Add("/beat/" + strings.Repeat("\u20ac", 200))
	f.Add("/beat/" + strings.Repeat("\U0001F600", 100))
	f.Add("/beat/" + strings.Repeat("\x80", 200))
	f.Add(strings.Repeat("\x80", 200))
	f.Fuzz(func(t *testing.T, path string) {
		// Serial by construction (the fuzz function runs one input at a time):
		// capture.Default swaps the process-global slog default, and the
		// handler must be built after it because webhttp.Logging resolves
		// slog.Default() when the chain is built.
		logs := capture.Default(t)
		h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, "")

		// Built by hand rather than via a target string: httptest.NewRequest
		// panics on a target it cannot parse, and arbitrary bytes are exactly
		// the input this target exists for.
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.URL.Path = path
		h.ServeHTTP(httptest.NewRecorder(), req)

		got, ok := logs.AttrValue("http", "path")
		if !ok {
			t.Fatalf("path %q produced no access line with a path attribute: records = %v", path, logs.Records())
		}
		// The placeholder means a path POLICY broke. knell installs none, so
		// it must never appear whatever arrives.
		if got == "(path-redaction-failed)" {
			t.Fatalf("path %q logged the redaction-failure placeholder: knell installs no path policy, so nothing can fail", path)
		}
		if len(got) > loggedPathCap+len(truncationMarker) {
			t.Fatalf("path %q logged %d bytes, want at most %d: an unauthenticated caller must not size log lines", path, len(got), loggedPathCap+len(truncationMarker))
		}
		// Truncating mid-rune would put invalid UTF-8 into the log stream.
		if utf8.ValidString(path) && !utf8.ValidString(got) {
			t.Fatalf("path %q logged %q, which is not valid UTF-8: truncation must land on a rune boundary", path, got)
		}
		if len(path) <= loggedPathCap {
			if got != path {
				t.Fatalf("path %q logged as %q, want it verbatim: the bound must not cost an operator the beat id", path, got)
			}
			return
		}
		kept, cut := strings.CutSuffix(got, truncationMarker)
		if !cut {
			t.Fatalf("path %q logged as %q, want the truncation marker so an operator can tell a bounded path from a real one", path, got)
		}
		if !strings.HasPrefix(path, kept) {
			t.Fatalf("path %q logged a kept prefix %q that is not a prefix of the request path: truncation must not alter what it keeps", path, kept)
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

// newShutdownHarness builds the routed handler over the real state machine for
// one beat, on a fake clock, behind a cancellable application context —
// cancel() is SIGTERM's effect on the app, and the only trigger the acceptance
// guard may key on. Each caller passes its OWN beat id: the metrics registry is
// a package-level singleton shared by the whole test binary. capture.Default is
// installed before New because webhttp.Logging resolves slog.Default() when the
// chain is built; it swaps the process-global default, so every caller stays
// serial (no t.Parallel).
func newShutdownHarness(t *testing.T, id string) (http.Handler, context.CancelFunc, *fakeClock, time.Time) {
	t.Helper()
	capture.Default(t)
	start := time.Unix(1_700_000_000, 0).UTC()
	clock := &fakeClock{now: start}
	watcher := watch.New([]watch.Beat{{ID: id, Deadline: time.Minute}}, &deliveringNotifier{}, clock.Now, start)
	appCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := New(appCtx, watcher, Deps{Healthz: staticHealthz(http.StatusOK)})
	return h, cancel, clock, start
}

// assertBeatRefused drives one ping into a handler whose application context is
// already cancelled and pins the whole refusal envelope: 503, the shutting_down
// code so a sender can tell a refusal from a 404, and no echo of the beat id —
// configured or not — so the refusal leaks as little as the 404 path does about
// which ids exist.
func assertBeatRefused(t *testing.T, h http.Handler, method, path string) {
	t.Helper()
	rec := beatRequest(t, h, method, path)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("%s %s after cancellation = %d, want 503 (body %s)", method, path, rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "shutting_down") {
		t.Errorf("503 body = %s, want the shutting_down code so a sender can tell a refusal from a 404", body)
	}
	if id, ok := strings.CutPrefix(path, "/beat/"); ok && strings.Contains(body, id) {
		t.Errorf("503 body = %s, must not echo the beat id", body)
	}
}

// TestBeatRefusedOnceTheApplicationContextIsCancelled pins the acceptance
// window against the real state machine. On SIGTERM the shared context is
// cancelled, which returns watch.Run (after it snapshots its undelivered work)
// while webhttp keeps the HTTP surface live for up to the shutdown grace. A
// ping accepted in that window is recorded behind a sender that no longer
// exists, so from the instant the context is cancelled both accepted methods
// must refuse and say so honestly. What accepting one would cost is pinned by
// the siblings below: TestCancelledBeatLeavesMetricsUnchanged (the exposition),
// TestCancelledUnknownBeatMintsNoSeries (label cardinality),
// TestCancelledBeatDoesNotReadBody (the drain), and
// TestProbeRoutesServeWhileBeatAcceptanceIsClosed (the probes keep serving).
func TestBeatRefusedOnceTheApplicationContextIsCancelled(t *testing.T) {
	const id = "webapi-shutdown-guard"
	h, cancel, _, _ := newShutdownHarness(t, id)

	cancel()

	for _, method := range []string{http.MethodPost, http.MethodGet} {
		t.Run("refused "+method, func(t *testing.T) {
			assertBeatRefused(t, h, method, "/beat/"+id)
		})
	}
}

// TestCancelledBeatLeavesMetricsUnchanged pins the exposition across the
// refusal. A ping accepted during the drain moves lastSeen, moves
// knell_beats_received_total, and republishes knell_beat_fresh as 1 — a false
// "all good" sample for the quorum rules, behind a sender that no longer exists
// (and for an alerted beat, a recovered notification queued on a channel nobody
// reads again). The tally the endpoint carries into the drain must stay the one
// watch.Run already reported.
func TestCancelledBeatLeavesMetricsUnchanged(t *testing.T) {
	const id = "webapi-shutdown-metrics"
	h, cancel, clock, start := newShutdownHarness(t, id)

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

	assertBeatRefused(t, h, http.MethodPost, "/beat/"+id)

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
}

// TestCancelledUnknownBeatMintsNoSeries pins label cardinality on the refusal
// path: an unknown id is a metric label an unauthenticated caller controls, so a
// refused ping must mint no series at all, exactly like the 404 path it replaces
// during the drain.
func TestCancelledUnknownBeatMintsNoSeries(t *testing.T) {
	const id = "webapi-shutdown-ghost-guard"
	const ghost = "webapi-shutdown-ghost-unknown"
	h, cancel, _, _ := newShutdownHarness(t, id)

	cancel()

	assertBeatRefused(t, h, http.MethodPost, "/beat/"+ghost)
	if exposition := scrapeExposition(t, h); strings.Contains(exposition, ghost) {
		t.Errorf("exposition mentions %s: a refused ping must mint no series, like the 404 path", ghost)
	}
}

// TestCancelledBeatDoesNotReadBody pins that the refusal lands BEFORE the body
// drain, like the 401 one: a ping arriving during the drain must not be able to
// hold a handler goroutine — and with it srv.Shutdown — open by trickling a
// payload.
func TestCancelledBeatDoesNotReadBody(t *testing.T) {
	const id = "webapi-shutdown-body"
	h, cancel, _, _ := newShutdownHarness(t, id)

	cancel()

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
}

// TestProbeRoutesServeWhileBeatAcceptanceIsClosed pins the other half of the
// drain: only the beat endpoint refuses. The orchestrator has to observe the
// health flip, and a last scrape during the drain is useful.
func TestProbeRoutesServeWhileBeatAcceptanceIsClosed(t *testing.T) {
	const id = "webapi-shutdown-probes"
	h, cancel, _, _ := newShutdownHarness(t, id)

	cancel()

	for _, path := range []string{"/healthz", "/metrics"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s with the app context cancelled = %d, want 200", path, rec.Code)
		}
	}
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

// TestBeatRefusedWhenWatcherClosedAdmission pins the LAST refusal, the only
// one that is atomic with the recording: the app context is still live, so
// both handler-side checks pass, and the 503 can only come from the watcher
// reporting accepting=false (watch.Watcher closes admission under the mutex
// that guards the beat mutation). A 200 here would tell a sender its heartbeat
// landed while the watcher recorded nothing.
func TestBeatRefusedWhenWatcherClosedAdmission(t *testing.T) {
	t.Parallel()

	b := &fakeBeater{known: map[string]bool{"api": true}, closed: true}
	h := newTestHandler(b, "")

	req := httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader("ping"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST with admission closed = %d, want 503 (body %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "shutting_down") {
		t.Errorf("503 body = %s, want the shutting_down code", rec.Body.String())
	}
	if len(b.seen) != 0 {
		t.Fatalf("beats recorded with admission closed = %v, want none", b.seen)
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
		headers    map[string]string
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
		// A sender URL built from an unset variable: the empty id routes to
		// /beat/{$}, so the refusal keeps the coded envelope and its own
		// series instead of falling into net/http's unmatched-bucket 404.
		"empty beat id": {
			ctx: context.Background(), method: http.MethodPost, path: "/beat/",
			wantStatus: http.StatusNotFound,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{$}",status="404"}`,
		},
		// A trailing-slash or extra-segment URL join: routes to
		// /beat/{id}/{rest...}, same coded 404, its own series.
		"nested beat path": {
			ctx: context.Background(), method: http.MethodPost, path: "/beat/" + id + "/",
			wantStatus: http.StatusNotFound,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{id}/{rest...}",status="404"}`,
		},
		// HEAD has its own registered route (so a HEAD probe cannot record a
		// ping), and that route NAMES the method, so the label is truthful.
		"head refused": {
			ctx: context.Background(), method: http.MethodHead, path: "/beat/" + id,
			wantStatus: http.StatusMethodNotAllowed,
			wantSeries: `knell_http_requests_total{method="HEAD",path="/beat/{id}",status="405"}`,
		},
		// PUT falls to the method-AGNOSTIC /beat/{id} route, which names no
		// method — the case knell's own derivation used to collapse to
		// "other". webhttp's derivation keeps a STANDARD method real (PUT is
		// one of nine) and buckets only a non-standard token, so a
		// method-probing scanner stays bounded while an operator can still see
		// which real method a sender is misconfigured to use. See
		// TestRequestMetricLabelsBoundedByTheRouteTable for the bucket half.
		"disallowed method refused": {
			ctx: context.Background(), method: http.MethodPut, path: "/beat/" + id,
			wantStatus: http.StatusMethodNotAllowed,
			wantSeries: `knell_http_requests_total{method="PUT",path="/beat/{id}",status="405"}`,
		},
		// A browser page tricked into the ping (the confused-deputy shape the
		// Sec-Fetch guard refuses). Nothing else makes this class visible: it
		// records no beat, so beatsReceived never moves, and an operator
		// watching for forged pings has only this series to alert on.
		"browser page ping": {
			ctx: context.Background(), method: http.MethodPost, path: "/beat/" + id,
			headers:    map[string]string{"Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "no-cors"},
			wantStatus: http.StatusForbidden,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{id}",status="403"}`,
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

			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(""))
			for name, value := range tt.headers {
				req.Header.Set(name, value)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
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

// hostileRequest is one caller-controlled method/path pair driven at the routed
// handler to see whether either value can reach a metric label.
type hostileRequest struct{ method, path string }

// hostileRequestSet is the caller-controlled traffic the cardinality guard
// drives. Every token below is distinct, so a naive implementation mints a new
// series per request and the assertions fail loudly rather than subtly.
func hostileRequestSet() []hostileRequest {
	var hostile []hostileRequest
	add := func(method, path string) {
		hostile = append(hostile, hostileRequest{method, path})
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
	// A 300-byte method token: LENGTH must not widen the label set either, and
	// no local middleware caps it any more — the closed method set is what
	// buckets it, exactly as it buckets a short bogus token.
	add(strings.Repeat("A", 300), "/beat/api")
	// A lowercase spelling of a standard method is NOT that method (methods are
	// case-sensitive, RFC 9110 §9.1), so it must bucket rather than hand a
	// caller a second spelling of the GET series.
	add("get", "/beat/api")
	return hostile
}

// assertNoCallerTokens pins that nothing a caller sent appears ANYWHERE in the
// exposition. This catches a label leak even if the per-series membership checks
// were relaxed.
func assertNoCallerTokens(t *testing.T, exposition string, requests []hostileRequest, allowedMethods, allowedPaths map[string]bool) {
	t.Helper()
	for _, req := range requests {
		for _, token := range []string{req.method, req.path} {
			if !allowedMethods[token] && !allowedPaths[token] && strings.Contains(exposition, token) {
				t.Errorf("exposition contains the caller-supplied token %q: a request value reached a metric label\n%s", token, exposition)
			}
		}
	}
}

// assertRequestSeriesVocabulary pins that every series, pre-existing or new, is
// spelled from the closed method set crossed with the route table.
func assertRequestSeriesVocabulary(t *testing.T, series, allowedMethods, allowedPaths map[string]bool) {
	t.Helper()
	for s := range series {
		labels := strings.TrimSuffix(strings.TrimPrefix(s, "knell_http_requests_total{"), "}")
		for pair := range strings.SplitSeq(labels, ",") {
			name, value, ok := strings.Cut(pair, "=")
			if !ok {
				t.Errorf("unparseable label pair %q in %q", pair, s)
				continue
			}
			value = strings.Trim(value, `"`)
			switch name {
			case "method":
				if !allowedMethods[value] {
					t.Errorf("series %s carries method=%q, which is not one the route table can produce: an unauthenticated caller can mint series without bound", s, value)
				}
			case "path":
				if !allowedPaths[value] {
					t.Errorf("series %s carries path=%q, which is not a registered route template", s, value)
				}
			}
		}
	}
}

// TestRequestMetricLabelsBoundedByTheRouteTable is the cardinality guard on the
// request counter, and the reason knell hands webhttp.WithRecordRouteMetric the
// job of deriving both labels instead of deriving them itself.
// webhttp.Logging is outermost, so the hook fires before beatHandler's token
// gate: the inputs below arrive from an UNAUTHENTICATED caller, and a Prometheus
// series once minted is permanent for the process lifetime here and in every
// observer scraping knell. So the label set must be bounded by the ROUTE TABLE
// plus a closed method vocabulary, and by nothing the caller sends.
//
// Two attack shapes, and knell is uniquely exposed to the second. The path is
// the obvious one, and it collapses onto registered templates (or the single
// "unmatched" marker). The METHOD is the one the fleet siblings do not face:
// registry-stats and subflux register only method-bearing patterns, while knell
// deliberately registers a method-agnostic /beat/{id} catch-all (so a 405 can
// carry a truthful Allow), and net/http routes ANY valid token there — "XYZZY"
// and friends reach it and answer 405. The bound is no longer a collapse of that
// route's method but webhttp's closed set: the nine standard methods stay
// themselves and every other token, at any length, buckets into "other".
func TestRequestMetricLabelsBoundedByTheRouteTable(t *testing.T) {
	// The complete label vocabulary knell's surface can produce: webhttp's
	// closed method set (nine standard methods plus the "other" bucket) crossed
	// with this route table's templates plus the "unmatched" marker. Anything
	// outside this is a caller-controlled value that reached a label.
	allowedMethods := map[string]bool{
		http.MethodGet: true, http.MethodHead: true, http.MethodPost: true,
		http.MethodPut: true, http.MethodDelete: true, http.MethodConnect: true,
		http.MethodOptions: true, http.MethodTrace: true, http.MethodPatch: true,
		otherMethodLabel: true,
	}
	allowedPaths := map[string]bool{
		"/beat/{id}": true, "/beat": true, "/beat/{$}": true, "/beat/{id}/{rest...}": true,
		"/healthz": true, "/metrics": true,
		nonCanonicalBeatPattern: true,
		unmatchedPathLabel:      true,
	}

	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandlerCtx(context.Background(), b, "")
	before := httpRequestSeries(scrapeExposition(t, h))

	hostile := hostileRequestSet()
	for _, req := range hostile {
		beatRequest(t, h, req.method, req.path)
	}

	exposition := scrapeExposition(t, h)
	after := httpRequestSeries(exposition)

	assertNoCallerTokens(t, exposition, hostile, allowedMethods, allowedPaths)
	assertRequestSeriesVocabulary(t, after, allowedMethods, allowedPaths)

	// The vocabulary can produce at most 10 methods x 7 paths x the handful of
	// statuses knell answers; the hostile requests above must land inside that,
	// not grow it. The bound is deliberately loose — the membership checks above
	// are the precise guard, this catches unbounded GROWTH regardless of
	// spelling.
	const maxNewSeries = 8
	if grew := len(after) - len(before); grew > maxNewSeries {
		t.Errorf("%d hostile requests added %d new series (want at most %d): the label set must be bounded by the route table, not by the caller\n%s",
			len(hostile), grew, maxNewSeries, exposition)
	}
}

// TestUnroutedRequestsAreCountedUnderTheCollapsedSeries pins the unmatched case
// of webhttp's label derivation as knell wires it: a request that matched no
// route is still counted, with its PATH collapsed onto the single "unmatched"
// marker and its real method kept.
// TestRequestMetricLabelsBoundedByTheRouteTable only proves such a request
// cannot MINT a caller-spelled series, so a hook that skipped unmatched
// requests entirely passes it -- and then scanner floods and every misrouted
// sender (a wrong-method scrape included) are invisible to the vantage point
// knell's own alert rules read.
//
// Only the path collapses. knell's own derivation used to collapse the method
// here too, because an unrouted request's method was caller-chosen text; the
// closed method set removes that reason, so a 404 flood stays visible per method
// (GET scanners vs POST senders) at no cardinality cost. That is a label change:
// the method="unmatched" value no longer exists in this exposition.
func TestUnroutedRequestsAreCountedUnderTheCollapsedSeries(t *testing.T) {
	tests := map[string]struct {
		method, path string
		wantMethod   string
		wantStatus   string
	}{
		// Scanner traffic: no pattern matches at all, so net/http answers 404.
		"off-route probe": {
			method: http.MethodGet, path: "/wp-admin/setup.php",
			wantMethod: http.MethodGet, wantStatus: "404",
		},
		// A scrape aimed with the wrong method: /metrics is registered GET-only,
		// so net/http's own 405 fires with no pattern matched.
		"wrong-method scrape": {
			method: http.MethodPost, path: "/metrics",
			wantMethod: http.MethodPost, wantStatus: "405",
		},
		// An unrouted request whose METHOD is also caller-chosen text: the path
		// collapses and the method buckets, so the pair stays bounded on both
		// axes without either one disappearing.
		"off-route probe with a bogus method": {
			method: "XYZZY", path: "/nope",
			wantMethod: otherMethodLabel, wantStatus: "404",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{"api": true}}
			h := newTestHandlerCtx(context.Background(), b, "")
			want := `knell_http_requests_total{method="` + tt.wantMethod +
				`",path="` + unmatchedPathLabel + `",status="` + tt.wantStatus + `"}`
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

// TestAccessLogMethodIsBoundedForRefusedRequests pins the access line's method
// bound, which is webhttp's and carries no knob (its ceiling follows from the
// IANA method registry, whose longest entry is UPDATEREDIRECTREF at 17
// characters, not from anything knell serves). The bound has to exist
// somewhere: net/http accepts any RFC 9110 token as a method with no length cap
// of its own, and the request line is bounded only by MaxHeaderBytes+4096 (1 MiB
// by default), so without it one unauthenticated caller writes ~1 MiB of its own
// text into a single access line and pushes knell's permanently-lost-notice
// WARNs out of the retained log window — the same consequence the path cap
// exists to prevent, on a request the token gate never sees (Logging is
// outermost).
func TestAccessLogMethodIsBoundedForRefusedRequests(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and must be installed BEFORE New, because webhttp.Logging
	// resolves slog.Default() when the chain is built.
	logs := capture.Default(t)
	h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, "")

	overlong := strings.Repeat("A", 300)
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
	if !logs.HasAttr("http", "method", overlongMethodMarker) {
		t.Errorf("access line does not report method=%s; records = %v", overlongMethodMarker, logs.Records())
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

// The label and marker vocabulary webhttp produces, restated here because these
// tests assert on the exposition and the access line an operator reads. The
// library exports the derivation (RouteMetricLabels), not the strings, so if a
// version bump ever changed one of them, these assertions are where knell finds
// out — which is the point of naming them rather than inlining the literals.
const (
	// otherMethodLabel is the bucket every non-standard method collapses into.
	otherMethodLabel = "other"
	// unmatchedPathLabel is the path label for a request that matched no route.
	// There is deliberately no method twin: only the path collapses.
	unmatchedPathLabel = "unmatched"
	// overlongMethodMarker replaces a method over webhttp's 24-byte log cap.
	// Unforgeable: parentheses are not token characters, so net/http answers a
	// request line spelling it 400 before a handler runs.
	overlongMethodMarker = "(overlong)"
	// truncationMarker is what webhttp appends to a path its cap cut.
	truncationMarker = "...(truncated)"
)

func TestBeatRefusesPageInitiatedBrowserRequests(t *testing.T) {
	// Only browsers send Sec-Fetch-*, so the documented senders (curl, an
	// Alertmanager webhook_config, a CI hook) send none of these headers and
	// must stay accepted, as must the one browser shape the README invites: a
	// navigation the user starts themselves, which carries Sec-Fetch-Site:
	// none. Everything else a browser sends names an initiating page, which is
	// the confused-deputy shape — including a cross-site NAVIGATION, because an
	// iframe load is one. Without this test the guard can be deleted with the
	// whole suite green: every other beat test sends no Sec-Fetch header at all,
	// so nothing exercises the refusing side of the check.
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, "")

	tests := []struct {
		name       string
		method     string
		site       string
		mode       string
		origin     string
		wantStatus int
		wantSeen   int
	}{
		{name: "documented sender sends no Sec-Fetch", wantStatus: 200, wantSeen: 1},
		{name: "address bar", site: "none", mode: "navigate", wantStatus: 200, wantSeen: 1},
		{name: "bookmark without a mode", site: "none", wantStatus: 200, wantSeen: 1},
		{name: "cross-site image load", site: "cross-site", mode: "no-cors", wantStatus: 403},
		{name: "cross-site fetch", site: "cross-site", mode: "cors", wantStatus: 403},
		// An iframe: a NAVIGATION with an initiating document, which the old
		// Sec-Fetch-Mode carve-out admitted and this rule refuses.
		{name: "cross-site iframe navigation", site: "cross-site", mode: "navigate", wantStatus: 403},
		{name: "same-origin fetch", site: "same-origin", mode: "cors", wantStatus: 403},
		{name: "same-origin subresource", site: "same-origin", mode: "no-cors", wantStatus: 403},
		{name: "same-origin navigation from a page", site: "same-origin", mode: "navigate", wantStatus: 403},
		// A compromised sibling origin on an allowed hostname.
		{name: "same-site fetch", site: "same-site", mode: "cors", wantStatus: 403},
		{name: "same-site navigation", site: "same-site", mode: "navigate", wantStatus: 403},
		// knell's documented endpoint is plain HTTP, where a browser sends NO
		// Fetch Metadata at all (it is appended only for a potentially
		// trustworthy URL). Origin is the signal that survives, so these cases
		// are the ones the Sec-Fetch-Site half cannot express — and a
		// documented sender still sends neither header, so it stays accepted.
		{name: "plain-http cross-origin fetch POST", method: http.MethodPost, origin: "http://evil.example", wantStatus: 403},
		{name: "plain-http form POST with a nulled origin", method: http.MethodPost, origin: "null", wantStatus: 403},
		{name: "documented POST sender sends no Origin either", method: http.MethodPost, wantStatus: 200, wantSeen: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(b.seen)
			// Default GET: every Sec-Fetch row above is a browser GET. The
			// Origin rows are POSTs, the method Origin is appended for
			// unconditionally and knell's canonical ping.
			method := tt.method
			if method == "" {
				method = http.MethodGet
			}
			req := httptest.NewRequest(method, "/beat/api", nil)
			if tt.site != "" {
				req.Header.Set("Sec-Fetch-Site", tt.site)
			}
			if tt.mode != "" {
				req.Header.Set("Sec-Fetch-Mode", tt.mode)
			}
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantStatus == http.StatusForbidden &&
				!strings.Contains(rec.Body.String(), "browser_page_request") {
				t.Errorf("body = %s, want the browser_page_request coded envelope", rec.Body.String())
			}
			if got := len(b.seen) - before; got != tt.wantSeen {
				t.Errorf("recorded beats = %d, want %d", got, tt.wantSeen)
			}
		})
	}

	t.Run("refused body never read", func(t *testing.T) {
		body := &countingReader{}
		req := httptest.NewRequest(http.MethodPost, "/beat/api", body)
		req.Header.Set("Sec-Fetch-Site", "cross-site")
		req.Header.Set("Sec-Fetch-Mode", "no-cors")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", rec.Code)
		}
		if body.reads != 0 {
			t.Errorf("body reads = %d, want 0 (refused requests must not be drained)", body.reads)
		}
		if code := rec.Body.String(); !strings.Contains(code, "browser_page_request") {
			t.Errorf("body = %s, want the browser_page_request coded envelope", code)
		}
	})
}

// TestBrowserGuardAppliesWithTokenGateConfigured pins the SCOPE of the
// browser-origin guard: it lives in record, so it refuses in the token
// configuration too, and the credential gate stays outside it. Every other
// browser-guard case builds an OPEN endpoint, so moving the check into
// beatHandler's token == "" branch would leave a gated deployment with no
// confused-deputy layer at all and the whole suite green.
func TestBrowserGuardAppliesWithTokenGateConfigured(t *testing.T) {
	const token = "s3cret"
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, token)

	tests := []struct {
		name       string
		auth       string
		site       string
		mode       string
		wantStatus int
		wantCode   string
		wantSeen   int
	}{
		// An authorized sender is still not allowed to be a browser tricked
		// into the ping: the credential proves who, not what.
		{
			name: "authorized cross-site subresource refused",
			auth: "Bearer " + token, site: "cross-site", mode: "no-cors",
			wantStatus: http.StatusForbidden, wantCode: "browser_page_request",
		},
		// Same-site is refused under the token too: a compromised sibling
		// origin on an allowed hostname is exactly the caller a credential
		// cannot tell apart from the operator.
		{
			name: "authorized same-site request refused",
			auth: "Bearer " + token, site: "same-site", mode: "cors",
			wantStatus: http.StatusForbidden, wantCode: "browser_page_request",
		},
		// The credential gate is outermost, so an unauthenticated cross-site
		// ping answers 401 and never learns the guard exists.
		{
			name: "unauthorized cross-site subresource answers the credential refusal",
			site: "cross-site", mode: "no-cors",
			wantStatus: http.StatusUnauthorized, wantCode: "unauthorized",
		},
		// The documented senders carry no Sec-Fetch headers and keep working
		// with the token set: the guard costs them nothing.
		{
			name:       "authorized documented sender still records",
			auth:       "Bearer " + token,
			wantStatus: http.StatusOK, wantSeen: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(b.seen)
			req := httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			if tt.site != "" {
				req.Header.Set("Sec-Fetch-Site", tt.site)
			}
			if tt.mode != "" {
				req.Header.Set("Sec-Fetch-Mode", tt.mode)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantCode != "" && !strings.Contains(rec.Body.String(), tt.wantCode) {
				t.Errorf("body = %s, want the %s coded envelope", rec.Body.String(), tt.wantCode)
			}
			if got := len(b.seen) - before; got != tt.wantSeen {
				t.Errorf("recorded beats = %d, want %d", got, tt.wantSeen)
			}
		})
	}
}

// hostPolicy builds the allowlist exactly as internal/config does, so this
// file exercises the policy shape knell actually ships (loopback exempt, the
// ALLOWED_HOSTS-naming 403).
func hostPolicy(t *testing.T, entries string) *webhttp.HostPolicy {
	t.Helper()
	policy, invalid := webhttp.ParseHostList(strings.Split(entries, ","),
		webhttp.WithLoopbackExempt(),
		webhttp.WithHostAllowlistError("host_not_allowed",
			"host not allowed; add it to ALLOWED_HOSTS to serve this hostname"))
	if len(invalid) > 0 {
		t.Fatalf("ParseHostList(%q) reported invalid entries %v; the fixture must be usable", entries, invalid)
	}
	return policy
}

// TestHostAllowlistBreaksDNSRebinding pins the one defense that stops a
// rebinding page from reading knell's state under the attacker's hostname. The
// attack request is indistinguishable from a legitimate one by every OTHER check
// knell runs on /healthz and /metrics: browserPageRequest guards the BEAT
// handler only, and on the documented default the endpoint is open. Only the
// textual Host check refuses it, because the attacker controls what their
// hostname resolves to but not what the browser puts in Host — and the /beat
// case below proves the allowlist, not the handler's own guard, is what answers
// first.
func TestHostAllowlistBreaksDNSRebinding(t *testing.T) {
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := New(context.Background(), b, Deps{
		Healthz: staticHealthz(http.StatusOK),
		Hosts:   hostPolicy(t, "knell.example"),
	})

	tests := map[string]struct {
		host       string
		path       string
		method     string // empty = POST
		remoteAddr string
		site       string
		wantStatus int
		wantSeen   int
	}{
		"allowed host records a beat":   {host: "knell.example", path: "/beat/api", wantStatus: http.StatusOK, wantSeen: 1},
		"allowed host with port":        {host: "knell.example:9190", path: "/beat/api", wantStatus: http.StatusOK, wantSeen: 1},
		"allowed host case-insensitive": {host: "KNELL.example", path: "/beat/api", wantStatus: http.StatusOK, wantSeen: 1},
		"rebinding ping refused":        {host: "attacker.example", path: "/beat/api", site: "same-origin", wantStatus: http.StatusForbidden},
		"foreign host refused on beat":  {host: "attacker.example", path: "/beat/api", wantStatus: http.StatusForbidden},
		// A non-canonical spelling under a foreign Host: the Host policy answers
		// FIRST, so this is host_not_allowed, not canonicalBeatPath's 404. Pins
		// that canonicalBeatPath stays INSIDE deps.Hosts.Middleware() in Chain.
		"foreign host refused on a non-canonical beat path": {host: "attacker.example", path: "/beat/api//", wantStatus: http.StatusForbidden},
		"foreign host refused on metrics":                   {host: "attacker.example", path: "/metrics", wantStatus: http.StatusForbidden},
		"foreign host refused on healthz":                   {host: "attacker.example", path: "/healthz", wantStatus: http.StatusForbidden},
		"allowed host serves metrics":                       {host: "knell.example", path: "/metrics", method: http.MethodGet, wantStatus: http.StatusOK},
		"allowed host serves healthz":                       {host: "knell.example", path: "/healthz", method: http.MethodGet, wantStatus: http.StatusOK},
		// The loopback carve-out is what keeps in-container HTTP clients (a
		// localhost curl, a sidecar probe) working under an allowlist of
		// browser-facing names; the baked `knell health` probe stats the
		// marker file and never speaks HTTP, so it needs no exemption.
		// Both halves must be loopback: a rebinding
		// request carries the attacker's hostname, and a remote client forging
		// Host: 127.0.0.1 is not a loopback socket peer.
		"loopback probe exempt":         {host: "127.0.0.1:9190", path: "/healthz", method: http.MethodGet, remoteAddr: "127.0.0.1:54321", wantStatus: http.StatusOK},
		"forged loopback host refused":  {host: "127.0.0.1:9190", path: "/beat/api", remoteAddr: "203.0.113.7:44444", wantStatus: http.StatusForbidden},
		"malformed host cannot match":   {host: "knell.example:notaport", path: "/beat/api", wantStatus: http.StatusForbidden},
		"bracketed hostname is refused": {host: "[knell.example]", path: "/beat/api", wantStatus: http.StatusForbidden},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			before := len(b.seen)
			method := tt.method
			if method == "" {
				method = http.MethodPost
			}
			req := httptest.NewRequest(method, tt.path, nil)
			req.Host = tt.host
			if tt.remoteAddr != "" {
				req.RemoteAddr = tt.remoteAddr
			}
			if tt.site != "" {
				req.Header.Set("Sec-Fetch-Site", tt.site)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantStatus == http.StatusForbidden && !strings.Contains(rec.Body.String(), "host_not_allowed") {
				t.Errorf("body = %s, want the host_not_allowed coded envelope naming ALLOWED_HOSTS", rec.Body.String())
			}
			if got := len(b.seen) - before; got != tt.wantSeen {
				t.Errorf("recorded beats = %d, want %d: a refused Host must never feed the switch", got, tt.wantSeen)
			}
		})
	}
}

// TestNilHostPolicyAcceptsEveryHost pins the backward-compatible default:
// ALLOWED_HOSTS is unset in every documented deployment, so an inactive policy
// must remove no capability. Without this, tightening the guard to fail closed
// on an unset variable would brick every existing sender and leave the suite
// green.
func TestNilHostPolicyAcceptsEveryHost(t *testing.T) {
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, "") // Deps.Hosts is nil, exactly like an unset ALLOWED_HOSTS

	for _, host := range []string{"knell.example", "attacker.example", "192.0.2.9:9190", ""} {
		req := httptest.NewRequest(http.MethodPost, "/beat/api", nil)
		req.Host = host
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("Host %q: status = %d, want 200 (a nil policy is a pass-through)", host, rec.Code)
		}
	}
	if len(b.seen) != 4 {
		t.Errorf("recorded beats = %d, want 4", len(b.seen))
	}
}

// scrapeAs is scrapeExposition with an explicit Host, so a handler under an
// ACTIVE allowlist can still be scraped by the test: scrapeExposition sends
// httptest's default Host, which the allowlist refuses.
func scrapeAs(t *testing.T, h http.Handler, host string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Host = host
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics as %q = %d, want 200", host, rec.Code)
	}
	return rec.Body.String()
}

// TestHostRefusalKeepsTheStandardEnvelope pins WHERE the Host allowlist sits in
// the chain, which New's own comment claims but nothing checked: inside the
// standard wrappers and outside canonicalBeatPath, so
// the 403 still carries SecurityHeaders and Cache-Control: no-store, and still
// rides webhttp.Logging -- so a rebinding attempt appears in the access log and
// in the request counter. Hoisting the policy above webhttp.Logging keeps every
// other test green while the refusal becomes invisible to both vantage points,
// which is the same alertability gap TestRefusedPingIsVisibleInTheRequestCounter
// exists to close for the other 403.
func TestHostRefusalKeepsTheStandardEnvelope(t *testing.T) {
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := New(context.Background(), b, Deps{
		Healthz: staticHealthz(http.StatusOK),
		Hosts:   hostPolicy(t, "knell.example"),
	})

	// The refusal happens before the mux routes, so the path label is the
	// "unmatched" marker rather than a /beat template.
	const series = `knell_http_requests_total{method="POST",path="unmatched",status="403"}`
	before, _ := seriesValue(t, scrapeAs(t, h, "knell.example"), series)

	req := httptest.NewRequest(http.MethodPost, "/beat/api", nil)
	req.Host = "attacker.example"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store: the host refusal must stay INSIDE noStore", got)
	}
	if rec.Header().Get("X-Content-Type-Options") == "" {
		t.Errorf("host refusal lost SecurityHeaders: %v", rec.Header())
	}
	if after, ok := seriesValue(t, scrapeAs(t, h, "knell.example"), series); !ok || after <= before {
		t.Errorf("%s did not move (%v -> %v, present=%v): a refused Host must stay visible to a scrape",
			series, before, after, ok)
	}
	if len(b.seen) != 0 {
		t.Errorf("recorded beats = %v, want none", b.seen)
	}
}
