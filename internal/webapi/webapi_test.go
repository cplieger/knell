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

	"github.com/cplieger/knell/internal/obs"
	"github.com/cplieger/knell/internal/watch"
	"github.com/cplieger/slogx/capture"
	"github.com/cplieger/webhttp/v2"
)

// fakeBeater accepts a fixed id set and records what was recorded. closed
// stands in for a watcher that has shut admission (watch.Watcher.StopAccepting).
type fakeBeater struct {
	known  map[string]bool
	seen   []string
	closed bool
}

func (f *fakeBeater) Beat(id string) watch.BeatOutcome {
	if f.closed {
		return watch.BeatClosed
	}
	if !f.known[id] {
		return watch.BeatUnknown
	}
	f.seen = append(f.seen, id)
	return watch.BeatRecorded
}

// testBeatToken is the credential every test handler is built with. BEAT_TOKEN
// is required in production (internal/config refuses to start without it), so
// there is no ungated handler to test against: a test that is not about the gate
// itself presents this token and gets the endpoint's real behaviour.
const testBeatToken = "unit-test-beat-token"

// authFailBurst mirrors the burst of the failed-auth token bucket knell's
// throttle rides — webhttp.FailedAuthRateLimit's preset, not knell's own tuning
// any more (the library owns the numbers so the services guarding one static
// bearer on one route cannot drift apart). It is unexported there, so the tests
// that need to say HOW MANY attempts fit inside the burst carry this mirror.
//
// It is self-verifying rather than a second source of truth: the first subtest
// of TestFailedAuthIsThrottledInAggregate demands exactly this many 401s and
// then a 429, so a preset retuned upstream fails there loudly instead of
// quietly weakening every assertion below.
const authFailBurst = 10

// newTestHandler assembles the routed handler around b with a healthy
// liveness endpoint; token is the required beat credential, exactly as in
// production. Beat acceptance is whatever b reports: a fakeBeater with
// closed=true is a watcher that has shut admission (watch.Watcher.StopAccepting),
// which is the endpoint's only shutdown state.
func newTestHandler(b *fakeBeater, token string) http.Handler {
	return newTestHandlerHealthz(b, token, http.StatusOK)
}

// newTestHandlerHealthz is newTestHandler with the liveness status under
// test control, so a test can drive a FAILING probe: health.Handler answers 503
// whenever the liveness marker is absent (boot, or after the pre-drain flip).
func newTestHandlerHealthz(b *fakeBeater, token string, healthzStatus int) http.Handler {
	return New(b, Deps{Healthz: staticHealthz(healthzStatus), BeatToken: token})
}

// newBeatRequest builds a request presenting testBeatToken. The bearer gate is
// the beat endpoint's only gate and it is required, so every test that is not
// about the credential itself has to authenticate to reach the behaviour it is
// pinning.
func newBeatRequest(method, target string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, target, body)
	req.Header.Set("Authorization", "Bearer "+testBeatToken)
	return req
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
		{name: "post unknown", method: http.MethodPost, path: "/beat/ghost", wantStatus: 404},
		{name: "missing id segment", method: http.MethodPost, path: "/beat/", wantStatus: 404},
		// The bare /beat prefix has its own test with the stronger oracle:
		// TestBareBeatPathAnswersTheCodedNotFound.
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
			h := newTestHandler(b, testBeatToken)
			req := newBeatRequest(tt.method, tt.path, strings.NewReader(tt.body))
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
			h := newTestHandler(b, testBeatToken)
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
// and record no beat — and must stay countable: the refusal answers before the
// mux, so the route metric can only call it "unmatched", and knell's own
// pre-route refusal counter is what names the cause for an operator.
// The two series a non-canonical refusal must move, spelled once so the route
// label and the reason label cannot drift apart in the assertions below: the
// pre-route class shares the request counter's "unmatched" path label with
// scanner traffic, and knell's own reason counter is what separates them.
const (
	nonCanonicalRouteSeries  = `knell_http_requests_total{method="POST",path="` + unmatchedPathLabel + `",status="404"}`
	nonCanonicalReasonSeries = `knell_pre_route_refusals_total{reason="non_canonical_beat_path"}`
)

func TestNonCanonicalBeatPathsAnswerTheCodedNotFound(t *testing.T) {
	// "//beat/api" enters the /beat namespace only after cleaning;
	// "/beat/api/../ghost" leaves one id for another; the rest are the
	// repeated-slash and dot-segment spellings a URL join produces.
	for _, target := range []string{"/beat//", "//beat/api", "/beat/./api", "/beat/api/../ghost", "/beat/api//"} {
		t.Run(target, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{"api": true}}
			h := newTestHandler(b, testBeatToken)
			// Deltas, not absolutes: the metrics registry is a package-level
			// singleton shared by the whole test binary (and by a -count=2
			// rerun). The reason series has to be present ALREADY, before any
			// refusal in this subtest - that is the pre-minting contract, and
			// mustSeriesValue is what fails if it is ever lost.
			before := scrapeExposition(t, h)
			unmatchedBefore, _ := seriesValue(t, before, nonCanonicalRouteSeries)
			reasonBefore := mustSeriesValue(t, before, nonCanonicalReasonSeries)

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
			// The refusal answers before the mux routes, so the ROUTE metric
			// can only bucket it as "unmatched" beside scanner traffic. What
			// tells the two apart is knell's own pre-route refusal counter,
			// keyed by cause: an operator investigating a beat that stopped
			// arriving reads it to see that a sender is pinging a malformed URL.
			after := scrapeExposition(t, h)
			if got := mustSeriesValue(t, after, nonCanonicalRouteSeries); got != unmatchedBefore+1 {
				t.Errorf("%s = %v, want %v: a pre-route refusal is still one served request",
					nonCanonicalRouteSeries, got, unmatchedBefore+1)
			}
			if got := mustSeriesValue(t, after, nonCanonicalReasonSeries); got != reasonBefore+1 {
				t.Errorf("%s = %v, want %v: without it a malformed sender URL is indistinguishable from a port scan",
					nonCanonicalReasonSeries, got, reasonBefore+1)
			}
		})
	}

	// The canonical spelling still records: the guard must refuse the rewritten
	// shapes only, never a well-formed ping.
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, testBeatToken)
	if rec := beatRequest(t, h, http.MethodPost, "/beat/api"); rec.Code != http.StatusOK {
		t.Fatalf("POST /beat/api = %d, want 200: the canonical ping must still record (body %s)", rec.Code, rec.Body.String())
	}
	if len(b.seen) != 1 {
		t.Errorf("recorded beats = %v, want one: the canonical ping must still record", b.seen)
	}
}

// TestEscapedSeparatorBeatPathNeverRecordsOrRedirects pins the one spelling
// family where the decoded and escaped views of a path disagree about /beat
// namespace membership: /beat%2Fapi decodes to /beat/api but matches no
// pattern, since ServeMux matches escaped segments. canonicalBeatPath judges
// the decoded view and passes it, so the refusal here is net/http's plain
// 404 rather than the coded envelope (see writeUnknownBeat's doc) — this
// test pins the properties that make that acceptable: a failure status, no
// redirect, nothing recorded, and no per-beat series minted.
func TestEscapedSeparatorBeatPathNeverRecordsOrRedirects(t *testing.T) {
	for _, target := range []string{"/beat%2Fapi", "/beat%2F"} {
		t.Run(target, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{"api": true}}
			h := newTestHandler(b, testBeatToken)

			rec := beatRequest(t, h, http.MethodPost, target)

			if rec.Code < 400 {
				t.Errorf("POST %s = %d, want a failure status: a fused-separator spelling must never read as success", target, rec.Code)
			}
			if loc := rec.Header().Get("Location"); loc != "" {
				t.Errorf("POST %s answered Location %q: a redirect is a success to `curl -fsS`", target, loc)
			}
			if len(b.seen) != 0 {
				t.Errorf("POST %s recorded %v, want nothing: the escaped view must not re-arm the switch under a decoded id", target, b.seen)
			}
		})
	}
}

// TestNonCanonicalPathOutsideTheBeatNamespaceIsLeftToTheMux pins the other edge
// of the same guard: canonicalBeatPath refuses the /beat spellings net/http
// would rewrite, and it must claim nothing else. Two things break if its
// namespace test ever widens. Traffic that names no beat starts answering
// knell's unknown_beat envelope, which tells whoever reads it to go check an id
// against BEATS when no beat was ever addressed; and the pre-route reason
// counter inflates with it. That counter is the ONLY series separating a
// malformed sender URL from a port scan — the route metric buckets both as
// "unmatched" — so drowning it costs the diagnostic
// TestNonCanonicalBeatPathsAnswerTheCodedNotFound exists to protect, on the one
// occasion an operator needs it: a beat that stopped arriving because its
// sender's URL is malformed. Off the namespace the rewrite therefore stays
// net/http's, and a Location header is exactly what this class must KEEP —
// the inverse of the assertion the beat cases carry.
func TestNonCanonicalPathOutsideTheBeatNamespaceIsLeftToTheMux(t *testing.T) {
	// Non-canonical spellings of a real route and of no route at all. Neither
	// the raw nor the cleaned form sits under /beat, which is what puts each
	// one outside the guard.
	tests := []struct {
		name string
		path string
	}{
		{name: "double slash before a routed path", path: "//healthz"},
		{name: "double slash after a routed path", path: "/healthz//"},
		{name: "dot segment on an unrouted path", path: "/nope/../ghost"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{"api": true}}
			h := newTestHandler(b, testBeatToken)
			// Deltas against the shared registry, as above; the reason series
			// is pre-minted, so mustSeriesValue reads it before any refusal.
			before := scrapeExposition(t, h)
			reasonBefore := mustSeriesValue(t, before, nonCanonicalReasonSeries)

			// The path is assigned rather than passed as a target: "//healthz"
			// is a protocol-relative URL, so httptest.NewRequest would read
			// "healthz" as the host and leave the path empty. No credential is
			// presented, because the bearer gate covers the beat route only —
			// any refusal here could only be the path guard's.
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.URL.Path = tt.path
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if strings.Contains(rec.Body.String(), "unknown_beat") {
				t.Errorf("POST %s = %d %s, want no unknown_beat envelope: nothing here addresses a beat, so naming one sends an operator after a sender that does not exist",
					tt.path, rec.Code, rec.Body.String())
			}
			if loc := rec.Header().Get("Location"); loc == "" {
				t.Errorf("POST %s = %d with no Location, want net/http's own rewrite: the beat guard must not answer for a path outside /beat",
					tt.path, rec.Code)
			}
			after := scrapeExposition(t, h)
			if got := mustSeriesValue(t, after, nonCanonicalReasonSeries); got != reasonBefore {
				t.Errorf("POST %s moved %s to %v, want %v: counting non-beat traffic there drowns the one signal that names a malformed sender URL",
					tt.path, nonCanonicalReasonSeries, got, reasonBefore)
			}
		})
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
// canonicalBeatPath, inBeatNamespace, or the chain's ordering can break in a
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
		h := newTestHandler(b, testBeatToken)

		// Built by hand rather than via a target string: httptest.NewRequest
		// panics on a target it cannot parse, and arbitrary bytes are exactly
		// the input this target exists for. It presents the credential, so a
		// refusal here is the path guard's verdict and never the gate's.
		req := newBeatRequest(http.MethodPost, "/", strings.NewReader(""))
		req.URL.Path = path
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		// Outside the beat namespace net/http keeps its own behavior, redirects
		// included (an empty path is cleaned to "/" with a 307): the guard
		// deliberately does not reach there. The cleaned spelling comes from the
		// same library function the guard itself judges with, so this exemption
		// cannot drift from the guard's own namespace test.
		clean, _ := webhttp.CanonicalRequestPath(path)
		if !inBeatNamespace(path) && !inBeatNamespace(clean) {
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
	// obs package pre-mints every kind at init.
	obs.InitBeat("webapi-test", 20*time.Minute, time.Unix(0, 0))
	// The freshness verdict is published by the watch state machine, not by
	// InitBeat, so this test mints the gauge series itself.
	obs.SetBeatFresh("webapi-test", true)

	h := newTestHandler(&fakeBeater{}, testBeatToken)
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
			h := newTestHandlerHealthz(&fakeBeater{known: map[string]bool{"api": true}}, testBeatToken, tt.healthzStatus)

			req := newBeatRequest(tt.method, tt.path, nil)
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

// TestNoStoreOnEveryRoute pins that no response knell serves is cacheable: a
// cached ping response would let a sender believe a beat was recorded when the
// request never reached the observer (false MISSING notice), and a cached
// /metrics exposition would report a stale beat_fresh=1 to the scraper, which is
// the direction that masks the quorum alert.
func TestNoStoreOnEveryRoute(t *testing.T) {
	h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, testBeatToken)
	// POST for the beat route: the case the comment above reasons about is the
	// ACCEPTED ping's 200, and a GET there is answered 405 instead.
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/beat/api"},
		{http.MethodGet, "/healthz"},
		{http.MethodGet, "/metrics"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			rec := beatRequest(t, h, tc.method, tc.path)
			if got := rec.Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf("Cache-Control on %s %s = %q, want %q", tc.method, tc.path, got, "no-store")
			}
		})
	}

	// The throttle answers its 429 itself, ahead of webhttp.Logging, so the two
	// header baselines hoisted above it in the chain are the only thing that can
	// give that refusal an envelope: every routed case above would still pass
	// with them back inside the throttle.
	t.Run("throttled 429", func(t *testing.T) {
		h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, testBeatToken)
		var rec *httptest.ResponseRecorder
		for range authFailBurst + 1 {
			req := httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
			req.Header.Set("Authorization", "Bearer wrong")
			rec = httptest.NewRecorder()
			h.ServeHTTP(rec, req)
		}
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("attempt %d = %d, want 429: this case needs the throttle to have fired (body %s)",
				authFailBurst+1, rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Cache-Control"); got != "no-store" {
			t.Errorf("Cache-Control on the throttled 429 = %q, want %q: a cache may not re-serve \"you are throttled\" to a sender whose budget has refilled",
				got, "no-store")
		}
		if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("X-Content-Type-Options on the throttled 429 = %q, want %q", got, "nosniff")
		}
	})
}

// panicBeater panics on every ping, standing in for a bug anywhere below the
// beat handler.
type panicBeater struct{}

func (panicBeater) Beat(string) watch.BeatOutcome { panic("beat exploded") }

func TestPanicUnderBeatHandlerAnswers500AndIsLogged(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and it must be installed BEFORE New, because webhttp resolves
	// slog.Default() when the chain is built.
	//
	// Without the chain's Recoverer, a panic under the beat handler unwinds to
	// net/http: the sender sees a reset connection rather than a 500, and the
	// access log never reports a status for the endpoint that feeds the switch.
	rec := capture.Default(t)
	h := New(panicBeater{}, Deps{
		Healthz:   staticHealthz(http.StatusOK),
		BeatToken: testBeatToken,
	})

	req := newBeatRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
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
	h := newTestHandler(b, testBeatToken)
	body := &unboundedReader{}
	req := newBeatRequest(http.MethodPost, "/beat/api", body)
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
	h := newTestHandler(b, testBeatToken)
	const warning = "beat body exceeded the cap"

	// A normal-sized payload is drained without a word about it.
	inCap := httptest.NewRecorder()
	h.ServeHTTP(inCap, newBeatRequest(http.MethodPost, "/beat/api", strings.NewReader(strings.Repeat("x", 4096))))
	if inCap.Code != http.StatusOK {
		t.Fatalf("in-cap ping = %d, want 200 (body %s)", inCap.Code, inCap.Body.String())
	}
	if got := logs.CountLevel(slog.LevelWarn, warning); got != 0 {
		t.Errorf("in-cap ping produced %d body warnings, want 0: an ordinary payload must not warn, or the line means nothing when it fires: %v", got, logs.Messages())
	}

	// One byte past the cap is enough: the endless reader runs the drain into
	// the limit.
	over := httptest.NewRecorder()
	h.ServeHTTP(over, newBeatRequest(http.MethodPost, "/beat/api", &unboundedReader{}))
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
	h := newTestHandler(b, testBeatToken)
	req := newBeatRequest(http.MethodPost, "/beat/api", &interruptedReader{})
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

func TestUnauthorizedBeatBodyIsNeverRead(t *testing.T) {
	h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, "s3cret")

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
}

// FuzzBeatTokenAcceptsOnlyTheExactBearerHeader fuzzes the untrusted header the
// beat gate reads. BEAT_TOKEN is the only thing between the public internet and
// a sender that can keep the dead-man switch armed with no real heartbeat
// behind it, and acceptance is documented as exactly "Authorization: Bearer
// <token>". The invariant is a security equality, not crash-freedom: for ANY
// header value, the request is authorized iff the value equals the expected
// string exactly, an unauthorized ping records no beat, the 401 names its
// challenge (RFC 9110 §11.6.1, so a sender reads the expected scheme off the
// protocol rather than off the README), and the 401 body never echoes the
// configured token.
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
		if wantStatus == http.StatusUnauthorized {
			if got := rec.Header().Get("WWW-Authenticate"); got != "Bearer" {
				t.Fatalf("Authorization %q: WWW-Authenticate = %q, want Bearer", auth, got)
			}
			if strings.Contains(rec.Body.String(), token) {
				t.Fatalf("401 body %q echoes the configured token", rec.Body.String())
			}
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

// TestEmptyBeatTokenFailsClosed pins the wiring guard. internal/config refuses
// to start without a BEAT_TOKEN, so an empty one reaching webapi is a bug — and
// the dangerous way to handle it is the one this rules out: building the
// verifier over "Bearer "+"" would arm the gate for a credential every client
// can present verbatim, an open endpoint with a published password. The zero
// verifier refuses everything instead, including the bare scheme.
func TestEmptyBeatTokenFailsClosed(t *testing.T) {
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := New(b, Deps{Healthz: staticHealthz(http.StatusOK)})

	for _, auth := range []string{"", "Bearer ", "Bearer", "Bearer anything"} {
		t.Run("auth "+auth, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("Authorization %q against an empty configured token = %d, want 401 (body %s)",
					auth, rec.Code, rec.Body.String())
			}
		})
	}
	if len(b.seen) != 0 {
		t.Errorf("recorded beats = %v, want none: an empty configured token must refuse every ping, never admit one", b.seen)
	}
}

// TestFailedAuthIsThrottledInAggregate pins the failed-auth throttle. The token
// is the endpoint's only gate, so an unthrottled 401 path is both a guessing
// oracle at wire speed and a log-flood vector (one access line per attempt).
// The three halves that make the throttle safe are pinned together, because each
// one alone is a plausible mis-implementation: bad bearers are capped, a VALID
// ping never draws a token (so a healthy fleet cannot throttle itself, even
// behind a flood), and no other route or method draws one either. The 429 is
// also the one refusal knell answers outside webhttp.Logging, so the reason
// counter asserted below is its only vantage point.
func TestFailedAuthIsThrottledInAggregate(t *testing.T) {
	badBearer := func(h http.Handler) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
		req.Header.Set("Authorization", "Bearer wrong")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	t.Run("bad bearers are capped at the burst", func(t *testing.T) {
		b := &fakeBeater{known: map[string]bool{"api": true}}
		h := newTestHandler(b, testBeatToken)
		// The 429 is answered outside webhttp.Logging, so it reaches neither
		// the access log nor the request counter: this reason series is the
		// only vantage point on it. Deltas against the singleton registry.
		const reasonSeries = `knell_pre_route_refusals_total{reason="auth_throttled"}`
		reasonBefore := mustSeriesValue(t, scrapeExposition(t, h), reasonSeries)

		// The bucket starts full, so exactly authFailBurst attempts reach the
		// gate before it empties. A 429 inside that window would throttle an
		// operator retrying a rotated token by hand.
		for i := range authFailBurst {
			if rec := badBearer(h); rec.Code != http.StatusUnauthorized {
				t.Fatalf("attempt %d = %d, want 401: the burst must absorb %d attempts (body %s)",
					i+1, rec.Code, authFailBurst, rec.Body.String())
			}
		}
		// A 401 is the handler's refusal, not the throttle's: counting it here
		// would make the series read as a throttle that fired when it did not.
		if got := mustSeriesValue(t, scrapeExposition(t, h), reasonSeries); got != reasonBefore {
			t.Errorf("%s = %v, want %v after %d unthrottled 401s: only a 429 is a throttled refusal",
				reasonSeries, got, reasonBefore, authFailBurst)
		}
		rec := badBearer(h)
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("attempt %d = %d, want 429: an unbounded 401 path is a guessing oracle and a log flood (body %s)",
				authFailBurst+1, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "too_many_auth_failures") {
			t.Errorf("429 body = %s, want the too_many_auth_failures coded envelope", rec.Body.String())
		}
		if got := mustSeriesValue(t, scrapeExposition(t, h), reasonSeries); got != reasonBefore+1 {
			t.Errorf("%s = %v, want %v: the 429 is logged and counted nowhere else, so an operator has no other way to see the throttle fired",
				reasonSeries, got, reasonBefore+1)
		}
		// A sender that is merely early has to learn when to come back; the
		// hint is what makes the refusal actionable rather than opaque.
		if got := rec.Header().Get("Retry-After"); got == "" {
			t.Errorf("429 carries no Retry-After: a throttled sender cannot tell when to retry")
		}
		if len(b.seen) != 0 {
			t.Errorf("recorded beats = %v, want none: no failed-auth attempt may record", b.seen)
		}
	})

	t.Run("a valid ping is never throttled", func(t *testing.T) {
		b := &fakeBeater{known: map[string]bool{"api": true}}
		h := newTestHandler(b, testBeatToken)

		// Empty the bucket first: the point is that a valid ping does not draw
		// from it at all, so it must succeed even while it is empty.
		for range authFailBurst + 5 {
			badBearer(h)
		}
		for i := range authFailBurst + 5 {
			if rec := beatRequest(t, h, http.MethodPost, "/beat/api"); rec.Code != http.StatusOK {
				t.Fatalf("valid ping %d during a failed-auth flood = %d, want 200: the fleet's own senders must never be throttled (body %s)",
					i+1, rec.Code, rec.Body.String())
			}
		}
		if len(b.seen) != authFailBurst+5 {
			t.Errorf("recorded beats = %d, want %d: every valid ping must record", len(b.seen), authFailBurst+5)
		}
	})

	t.Run("other requests draw no token", func(t *testing.T) {
		b := &fakeBeater{known: map[string]bool{"api": true}}
		h := newTestHandler(b, testBeatToken)

		// None of these is a failed authentication: the probes are ungated, and
		// the /beat spellings are refused as a method or as an unknown beat
		// before any credential matters. Drawing on them would let unrelated
		// traffic throttle the real senders' 401 budget.
		for range authFailBurst + 5 {
			for _, r := range []struct{ method, path string }{
				{http.MethodGet, "/healthz"},
				{http.MethodGet, "/metrics"},
				{http.MethodGet, "/beat/api"},
				{http.MethodPost, "/beat/api/extra"},
				{http.MethodPost, "/beat"},
			} {
				req := httptest.NewRequest(r.method, r.path, nil)
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code == http.StatusTooManyRequests {
					t.Fatalf("%s %s = 429: only a failed authentication on the beat endpoint may draw a token", r.method, r.path)
				}
			}
		}
		// The budget is intact: the very next bad bearer is still a 401.
		if rec := badBearer(h); rec.Code != http.StatusUnauthorized {
			t.Fatalf("bad bearer after unrelated traffic = %d, want 401: the bucket was drained by requests that are not auth failures", rec.Code)
		}
	})
}

// TestEveryRejectedMethodAnswersTheSameRefusal pins that the Allow header is
// TRUE for every rejected method. The method-agnostic /beat/{id} route is the
// reason they all share one response: it catches GET, HEAD and every other
// non-POST verb and answers this file's coded 405 with Allow: POST. Without it
// net/http answers instead, with whatever it synthesizes from the remaining
// patterns, and none of those shapes carries a code a sender can parse. A GET
// or HEAD that recorded a ping would keep the switch armed with
// no heartbeat behind it, so a prober reading Allow must never be steered at
// them.
func TestEveryRejectedMethodAnswersTheSameRefusal(t *testing.T) {
	for _, method := range []string{
		http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete,
		http.MethodPatch, http.MethodOptions,
		// CONNECT is the one method net/http routes by another code path:
		// ServeMux skips path canonicalization for it and matches on
		// r.URL.Host, so it reaches the method-agnostic route without the
		// cleaning pass. It must still answer the same coded 405 with a
		// truthful Allow — a CONNECT routed to the POST pattern would
		// RECORD a ping, and with no method-agnostic route the answer
		// would come from net/http, uncoded.
		http.MethodConnect, "WHATEVER",
	} {
		t.Run(method, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{"api": true}}
			h := newTestHandler(b, testBeatToken)
			req := newBeatRequest(method, "/beat/api", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s /beat/api = %d, want 405", method, rec.Code)
			}
			if got := rec.Header().Get("Allow"); got != "POST" {
				t.Errorf("%s /beat/api Allow = %q, want \"POST\": a rejected method must not be told that a method which does not record a beat is permitted", method, got)
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

// FuzzLoggedPathIsBounded fuzzes the untrusted text the access line records as
// its path. r.URL.Path is attacker-controlled, net/http accepts a megabyte of
// it, and webhttp.Logging sits OUTSIDE the mux, so the access line is emitted
// before beatHandler's token gate and the bound is the only thing limiting an
// unauthenticated caller's influence on knell's log lines -- the channel that
// carries the undelivered-notice warnings no counter backs.
//
// The bound itself is webhttp's now, so what this pins is knell's WIRING of it:
// that every request really does travel through a logger carrying
// WithMaxLoggedPath(loggedPathCap), for arbitrary bytes as well as for the
// shapes its committed seeds name -- the two probe endpoints and an ordinary
// ping, the longest configurable beat id, the cap boundary from both sides, a
// multibyte path, a megabyte path, and a path carrying raw non-UTF-8 bytes (%80
// decodes to one), where the rune-boundary backoff can walk the cut all the way
// to zero. It drives the real assembled handler because there is no local
// transform left to call, which also means a future middleware that re-wrote
// the path before Logging saw it would be caught here.
func FuzzLoggedPathIsBounded(f *testing.F) {
	f.Add("/beat/api")
	f.Add("")
	f.Add("/")
	f.Add("/healthz")
	f.Add("/metrics")
	f.Add("/beat/" + strings.Repeat("a", 64))
	f.Add("/beat/" + strings.Repeat("a", loggedPathCap-6))
	f.Add("/beat/" + strings.Repeat("a", loggedPathCap-5))
	f.Add("/beat/" + strings.Repeat("a", 1<<20))
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
		h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, testBeatToken)

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

// beatRequest sends one authenticated ping through the routed handler.
func beatRequest(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := newBeatRequest(method, path, strings.NewReader(""))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// newShutdownHarness builds the routed handler over the real state machine for
// one beat, on a fake clock. The returned func closes beat admission the way
// the composition root does at the start of the drain (webhttp's pre-drain hook
// calls watch.Watcher.StopAccepting), and it is the only trigger the endpoint's
// refusal keys on. Each caller passes its OWN beat id: the metrics registry is
// a package-level singleton shared by the whole test binary. capture.Default is
// installed before New because webhttp.Logging resolves slog.Default() when the
// chain is built; it swaps the process-global default, so every caller stays
// serial (no t.Parallel).
func newShutdownHarness(t *testing.T, id string) (http.Handler, func(), *fakeClock, time.Time) {
	t.Helper()
	capture.Default(t)
	start := time.Unix(1_700_000_000, 0).UTC()
	clock := &fakeClock{now: start}
	watcher := watch.New([]watch.Beat{{ID: id, Deadline: time.Minute}}, &deliveringNotifier{}, clock.Now, start)
	h := New(watcher, Deps{Healthz: staticHealthz(http.StatusOK), BeatToken: testBeatToken})
	return h, watcher.StopAccepting, clock, start
}

// assertBeatRefused drives one ping into a handler whose watcher has already
// closed admission and pins the whole refusal envelope: 503, the shutting_down
// code so a sender can tell a refusal from a 404, and no echo of the beat id —
// configured or not — so the refusal leaks as little as the 404 path does about
// which ids exist.
func assertBeatRefused(t *testing.T, h http.Handler, method, path string) {
	t.Helper()
	rec := beatRequest(t, h, method, path)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("%s %s with admission closed = %d, want 503 (body %s)", method, path, rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "shutting_down") {
		t.Errorf("503 body = %s, want the shutting_down code so a sender can tell a refusal from a 404", body)
	}
	if id, ok := strings.CutPrefix(path, "/beat/"); ok && strings.Contains(body, id) {
		t.Errorf("503 body = %s, must not echo the beat id", body)
	}
}

// TestRefusedBeatLeavesMetricsUnchanged pins the exposition across the
// refusal. A ping accepted during the drain moves lastSeen, moves
// knell_beats_received_total, and republishes knell_beat_fresh as 1 — a false
// "all good" sample for the quorum rules, behind a sender that no longer exists
// (and for an alerted beat, a recovered notification queued on a channel nobody
// reads again). The tally the endpoint carries into the drain must stay the one
// watch.Run already reported.
func TestRefusedBeatLeavesMetricsUnchanged(t *testing.T) {
	const id = "webapi-shutdown-metrics"
	h, closeAdmission, clock, start := newShutdownHarness(t, id)

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
	obs.SetBeatFresh(id, false)
	// Advance the clock so a recorded ping would be VISIBLE in lastSeen rather
	// than landing on the same second as the live one.
	clock.advance(time.Hour)

	// The composition root's pre-drain close, and the only trigger the
	// refusal may key on.
	closeAdmission()

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

// TestRefusedUnknownBeatMintsNoSeries pins label cardinality on the refusal
// path: an unknown id is a metric label an unauthenticated caller controls, so a
// refused ping must mint no series at all, exactly like the 404 path it replaces
// during the drain.
func TestRefusedUnknownBeatMintsNoSeries(t *testing.T) {
	const id = "webapi-shutdown-ghost-guard"
	const ghost = "webapi-shutdown-ghost-unknown"
	h, closeAdmission, _, _ := newShutdownHarness(t, id)

	closeAdmission()

	assertBeatRefused(t, h, http.MethodPost, "/beat/"+ghost)
	if exposition := scrapeExposition(t, h); strings.Contains(exposition, ghost) {
		t.Errorf("exposition mentions %s: a refused ping must mint no series, like the 404 path", ghost)
	}
}

// TestRefusedBeatStillDrainsTheBody pins the accepted cost of letting the
// watcher own admission alone: the verdict comes from watch.Watcher.Beat, which
// is only asked on the far side of the ignored body, so a ping refused during
// the drain has been READ before it is refused. That is deliberate — the read is
// what keeps the connection reusable, and it is bounded twice over (maxBeatBody
// caps the bytes, the server's read timeout caps the time), so a trickling
// sender delays only its own refusal. The assertion is here so the cost is
// visible and pinned rather than rediscovered: if a future change wants to
// refuse before reading, it needs a second lifecycle view in this package, which
// is exactly what New explains it must not have.
func TestRefusedBeatStillDrainsTheBody(t *testing.T) {
	const id = "webapi-shutdown-body"
	h, closeAdmission, _, _ := newShutdownHarness(t, id)

	closeAdmission()

	body := &countingReader{}
	req := newBeatRequest(http.MethodPost, "/beat/"+id, body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if body.reads == 0 {
		t.Errorf("body reads = 0, want the body drained before the watcher's verdict: the refusal is Beat's answer, which is asked after the drain, and skipping the drain would leave the connection unreusable")
	}
}

// TestBeatRefusedWhenAdmissionClosesDuringBodyDrain pins the refusal for the
// request that motivates the whole ordering: one ADMITTED while the app was
// live, held inside the ignored body read (which the 30s read timeout bounds, so
// this is routine rather than exotic), and finishing after admission closed. The
// pipe write proves the handler really entered the body read before the close,
// so a 503 can only come from the watcher's verdict on the far side of it — the
// one check that is atomic with the recording. A 200 here would tell a sender its
// heartbeat landed while the watcher recorded nothing.
func TestBeatRefusedWhenAdmissionClosesDuringBodyDrain(t *testing.T) {
	t.Parallel()

	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, testBeatToken)

	bodyReader, bodyWriter := io.Pipe()
	req := newBeatRequest(http.MethodPost, "/beat/api", bodyReader)
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
	// Close admission while the handler is parked in the read, then release the
	// read. The write below and the pipe close order this against the handler
	// goroutine's later Beat call, so the flip needs no lock of its own.
	b.closed = true
	if err := bodyWriter.Close(); err != nil {
		t.Fatalf("close request body: %v", err)
	}
	<-done

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST refused mid-drain = %d, want 503 (body %s)", rec.Code, rec.Body.String())
	}
	if len(b.seen) != 0 {
		t.Fatalf("beats recorded after admission closed during the body drain = %v, want none", b.seen)
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

	tests := map[string]struct {
		token      string
		closed     bool
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
			token: testBeatToken, method: http.MethodPost, path: "/beat/" + id,
			wantStatus: http.StatusOK, wantSeen: 1,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{id}",status="200"}`,
		},
		// A rotated or mistyped BEAT_TOKEN. The credential is checked before the
		// body drain and before the id lookup, so a caller with no valid token
		// gets 401 rather than 404 or 503.
		"unauthorized ping": {
			token: "a-different-token-entirely", method: http.MethodPost, path: "/beat/" + id,
			wantStatus: http.StatusUnauthorized,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{id}",status="401"}`,
		},
		// A typo in a sender's URL: the beat stays silent while the sender
		// believes it is pinging.
		"unknown beat id": {
			token: testBeatToken, method: http.MethodPost, path: "/beat/ghost",
			wantStatus: http.StatusNotFound,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{id}",status="404"}`,
		},
		// A sender URL built from an unset variable: the empty id routes to
		// /beat/{$}, so the refusal keeps the coded envelope and its own
		// series instead of falling into net/http's unmatched-bucket 404.
		"empty beat id": {
			token: testBeatToken, method: http.MethodPost, path: "/beat/",
			wantStatus: http.StatusNotFound,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{$}",status="404"}`,
		},
		// A trailing-slash or extra-segment URL join: routes to
		// /beat/{id}/{rest...}, same coded 404, its own series.
		"nested beat path": {
			token: testBeatToken, method: http.MethodPost, path: "/beat/" + id + "/",
			wantStatus: http.StatusNotFound,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{id}/{rest...}",status="404"}`,
		},
		// HEAD has its own registered route (so a HEAD probe cannot record a
		// ping), and that route NAMES the method, so the label is truthful.
		"head refused": {
			token: testBeatToken, method: http.MethodHead, path: "/beat/" + id,
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
			token: testBeatToken, method: http.MethodPut, path: "/beat/" + id,
			wantStatus: http.StatusMethodNotAllowed,
			wantSeries: `knell_http_requests_total{method="PUT",path="/beat/{id}",status="405"}`,
		},
		// GET has its own registered route refusing it, so a sender (or
		// anything that fetched the URL for one) learns the method is not the
		// recording one, and the label names the method truthfully.
		"get refused": {
			token: testBeatToken, method: http.MethodGet, path: "/beat/" + id,
			wantStatus: http.StatusMethodNotAllowed,
			wantSeries: `knell_http_requests_total{method="GET",path="/beat/{id}",status="405"}`,
		},
		// A ping arriving during the drain, after watch.Run already took its
		// undelivered-work snapshot.
		"ping during drain": {
			token: testBeatToken, closed: true, method: http.MethodPost, path: "/beat/" + id,
			wantStatus: http.StatusServiceUnavailable,
			wantSeries: `knell_http_requests_total{method="POST",path="/beat/{id}",status="503"}`,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{id: true}, closed: tt.closed}
			h := newTestHandler(b, tt.token)

			// Deltas, not absolutes: the registry is a package-level singleton
			// shared by the whole test binary (and by a -count=2 rerun).
			before, _ := seriesValue(t, scrapeExposition(t, h), tt.wantSeries)

			req := newBeatRequest(tt.method, tt.path, strings.NewReader(""))
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
// webhttp.Logging sits OUTSIDE the mux, so the route-metric hook fires before
// beatHandler's token gate: the inputs below arrive from an UNAUTHENTICATED
// caller, and a Prometheus series once minted is permanent for the process
// lifetime here and in every observer scraping knell. So the label set must be
// bounded by the ROUTE TABLE plus a closed method vocabulary, and by nothing
// the caller sends.
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
		unmatchedPathLabel: true,
	}

	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := newTestHandler(b, testBeatToken)
	before := httpRequestSeries(scrapeExposition(t, h))

	hostile := hostileRequestSet()
	for _, req := range hostile {
		beatRequest(t, h, req.method, req.path)
	}

	exposition := scrapeExposition(t, h)
	after := httpRequestSeries(exposition)

	assertNoCallerTokens(t, exposition, hostile, allowedMethods, allowedPaths)
	assertRequestSeriesVocabulary(t, after, allowedMethods, allowedPaths)

	// The vocabulary can produce at most 10 methods x 6 paths x the handful of
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
			h := newTestHandler(b, testBeatToken)
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
// exists to prevent, on a request the token gate never sees (webhttp.Logging
// sits outside the mux, ahead of beatHandler's token gate).
func TestAccessLogMethodIsBoundedForRefusedRequests(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and must be installed BEFORE New, because webhttp.Logging
	// resolves slog.Default() when the chain is built.
	logs := capture.Default(t)
	h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, testBeatToken)

	overlong := strings.Repeat("A", 300)

	// Both subtests share the capture above, so neither may take t.Parallel.
	t.Run("overlong method is bounded", func(t *testing.T) {
		rec := beatRequest(t, h, overlong, "/beat/api")

		// The refusal itself is unchanged: the bogus method still routes to the
		// method-agnostic catch-all, which answers 405 with a truthful Allow.
		// It also gates the access-line assertions below: a request refused
		// somewhere else writes a line about something else.
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%d-byte bogus method = %d, want 405 (body %s)", len(overlong), rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Allow"); got != "POST" {
			t.Errorf("Allow = %q, want \"POST\": bounding the logged method must not change the refusal", got)
		}

		// The logged method is the placeholder, never the caller's bytes.
		if !logs.HasAttr("http", "method", overlongMethodMarker) {
			t.Errorf("access line does not report method=%s; records = %v", overlongMethodMarker, logs.Records())
		}
		if logs.HasAttr("http", "method", overlong) {
			t.Errorf("access line carries the caller's %d-byte method verbatim: an unauthenticated caller writes the text of knell's own log lines; records = %v",
				len(overlong), logs.Records())
		}
	})

	// A real method passes through untouched: this is a bound, not a rewrite.
	t.Run("real method passes through", func(t *testing.T) {
		rec := beatRequest(t, h, http.MethodPost, "/beat/api")
		if rec.Code != http.StatusOK {
			t.Fatalf("post known = %d, want 200 (body %s)", rec.Code, rec.Body.String())
		}
		if !logs.HasAttr("http", "method", http.MethodPost) {
			t.Errorf("access line does not report method=POST for an ordinary ping; records = %v", logs.Records())
		}
	})
}

// TestAccessLogClientIPResolvesOnlyThroughTrustedProxies pins the wiring of
// Deps.TrustedProxies into webhttp.WithClientIP. internal/config pins
// TRUSTED_PROXIES parsing and webhttp pins ClientIP's spoof model, so neither can
// catch a Deps field that never reaches the option: an operator's TRUSTED_PROXIES
// would then be parsed, reported as trusted_proxies=1 at startup, and silently
// ignored, leaving every access line — including the 401s a token-guessing run
// writes past the throttle — naming the proxy instead of an address an operator
// can block. Losing the variadic expansion (or the field on the composition
// root's Deps literal) is a one-character edit that nothing else in the module
// fails on.
//
// Both halves matter: the trusted half proves the set REACHES the option, the
// empty half pins the spoof-proof default an unset TRUSTED_PROXIES has to keep.
// Deliberately at the webapi level rather than in main_lifecycle_test.go, where
// the other Deps fields are pinned: their oracles are HTTP-observable (200/401/
// 403), while client_ip is observable only in the access line, and run() calls
// slogx.Setup, which replaces the capture a lifecycle test installs.
func TestAccessLogClientIPResolvesOnlyThroughTrustedProxies(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and must be installed BEFORE New, because webhttp.Logging
	// resolves slog.Default() when the chain is built.
	logs := capture.Default(t)

	trusted, invalid := webhttp.ParseCIDRs([]string{"192.0.2.0/24"})
	if len(invalid) > 0 {
		t.Fatalf("ParseCIDRs rejected %v; the fixture must be a valid trusted-proxy set", invalid)
	}
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := New(b, Deps{Healthz: staticHealthz(http.StatusOK), BeatToken: testBeatToken, TrustedProxies: trusted})

	// httptest.NewRequest's RemoteAddr is 192.0.2.1:1234, inside the set above,
	// so the forwarded header is a trusted proxy's own report of the sender.
	req := newBeatRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
	req.Header.Set("X-Forwarded-For", "203.0.113.7")
	proxied := httptest.NewRecorder()
	h.ServeHTTP(proxied, req)
	if proxied.Code != http.StatusOK {
		t.Fatalf("proxied ping = %d, want 200: the fixture must reach the access log through a recorded beat (body %s)", proxied.Code, proxied.Body.String())
	}
	if !logs.HasAttr("http", "client_ip", "203.0.113.7") {
		t.Errorf("access line does not resolve client_ip through the trusted proxy's X-Forwarded-For: an unwired Deps.TrustedProxies leaves every proxied access line naming the proxy; records = %v", logs.Records())
	}

	// The same request with NO trusted set must report the socket peer.
	bare := capture.Default(t)
	plain := New(b, Deps{Healthz: staticHealthz(http.StatusOK), BeatToken: testBeatToken})
	spoofed := newBeatRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
	spoofed.Header.Set("X-Forwarded-For", "203.0.113.7")
	plain.ServeHTTP(httptest.NewRecorder(), spoofed)

	if !bare.HasAttr("http", "client_ip", "192.0.2.1") {
		t.Errorf("access line with no trusted set does not report the socket peer; records = %v", bare.Records())
	}
	if bare.HasAttr("http", "client_ip", "203.0.113.7") {
		t.Errorf("access line honors X-Forwarded-For with no trusted proxies: any caller can then choose the address knell attributes its 401s to; records = %v", bare.Records())
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

// hostPolicy builds the allowlist by calling the SHIPPED builder
// (HostPolicyOptions), so this file exercises the policy shape knell
// actually serves (loopback exempt, the ALLOWED_HOSTS-naming 403) rather than a
// hand-copied twin that cannot fail when production's option set changes.
func hostPolicy(t *testing.T, entries string) *webhttp.HostPolicy {
	t.Helper()
	policy, invalid := webhttp.ParseHostList(strings.Split(entries, ","), HostPolicyOptions()...)
	if len(invalid) > 0 {
		t.Fatalf("ParseHostList(%q) reported invalid entries %v; the fixture must be usable", entries, invalid)
	}
	return policy
}

// TestHostAllowlistBreaksDNSRebinding pins the one defense that stops a
// rebinding page from reading knell's state under the attacker's hostname. The
// attack request is indistinguishable from a legitimate one by every OTHER check
// knell runs on /healthz and /metrics: the bearer gate covers the BEAT route
// only, and those two endpoints must stay reachable for probes and scrapes.
// Only the textual Host check refuses it, because the attacker controls what
// their hostname resolves to but not what the browser puts in Host — and the
// /beat case below proves the allowlist answers before the route's own refusals.
func TestHostAllowlistBreaksDNSRebinding(t *testing.T) {
	b := &fakeBeater{known: map[string]bool{"api": true}}
	h := New(b, Deps{
		Healthz:   staticHealthz(http.StatusOK),
		Hosts:     hostPolicy(t, "knell.example"),
		BeatToken: testBeatToken,
	})

	tests := map[string]struct {
		host       string
		path       string
		method     string // empty = POST
		remoteAddr string
		wantStatus int
		wantSeen   int
	}{
		"allowed host records a beat":   {host: "knell.example", path: "/beat/api", wantStatus: http.StatusOK, wantSeen: 1},
		"allowed host with port":        {host: "knell.example:9190", path: "/beat/api", wantStatus: http.StatusOK, wantSeen: 1},
		"allowed host case-insensitive": {host: "KNELL.example", path: "/beat/api", wantStatus: http.StatusOK, wantSeen: 1},
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
			req := newBeatRequest(method, tt.path, nil)
			req.Host = tt.host
			if tt.remoteAddr != "" {
				req.RemoteAddr = tt.remoteAddr
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
	h := New(b, Deps{
		Healthz:   staticHealthz(http.StatusOK),
		Hosts:     hostPolicy(t, "knell.example"),
		BeatToken: testBeatToken,
	})

	// The refusal happens before the mux routes, so the path label is the
	// "unmatched" marker rather than a /beat template — which is why the cause
	// is also counted by reason on knell's own pre-route refusal counter.
	const (
		series       = `knell_http_requests_total{method="POST",path="unmatched",status="403"}`
		reasonSeries = `knell_pre_route_refusals_total{reason="host_not_allowed"}`
	)
	baseline := scrapeAs(t, h, "knell.example")
	before, _ := seriesValue(t, baseline, series)
	reasonBefore := mustSeriesValue(t, baseline, reasonSeries)

	req := newBeatRequest(http.MethodPost, "/beat/api", nil)
	req.Host = "attacker.example"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store: the host refusal must stay INSIDE webhttp.NoStore", got)
	}
	if rec.Header().Get("X-Content-Type-Options") == "" {
		t.Errorf("host refusal lost SecurityHeaders: %v", rec.Header())
	}
	after := scrapeAs(t, h, "knell.example")
	if got, ok := seriesValue(t, after, series); !ok || got <= before {
		t.Errorf("%s did not move (%v -> %v, present=%v): a refused Host must stay visible to a scrape",
			series, before, got, ok)
	}
	if got := mustSeriesValue(t, after, reasonSeries); got != reasonBefore+1 {
		t.Errorf("%s = %v, want %v: in the unmatched bucket a rebinding attempt is indistinguishable from a port scan unless its cause is counted",
			reasonSeries, got, reasonBefore+1)
	}
	if len(b.seen) != 0 {
		t.Errorf("recorded beats = %v, want none", b.seen)
	}
}

// TestThrottledAuthFailureWritesNoAccessLine pins WHY beatAuthFailureLimiter
// sits OUTSIDE webhttp.Logging: an over-budget attempt must be answered before
// the access logger, so a guessing run at wire speed cannot write one
// access line per attempt and push knell's permanently-lost-notice WARNs out of
// the retained log window. Nothing else pins that ordering -- the throttle's
// other tests assert the 429 and its reason counter, both of which survive the
// limiter being moved inside Logging, which restores the flood with the suite
// green. So: the unthrottled attempts each leave exactly one line, and the
// throttled ones leave none at all.
func TestThrottledAuthFailureWritesNoAccessLine(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and must be installed BEFORE New, because webhttp.Logging
	// resolves slog.Default() when the chain is built.
	logs := capture.Default(t)
	h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}}, testBeatToken)

	badBearer := func() int {
		req := httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
		req.Header.Set("Authorization", "Bearer wrong")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	// The burst reaches the handler, so each of these IS logged: the bound is on
	// the flood, not on reporting a failed authentication at all.
	for i := range authFailBurst {
		if got := badBearer(); got != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i+1, got)
		}
	}
	if got := logs.CountExact("http"); got != authFailBurst {
		t.Fatalf("access lines after %d unthrottled 401s = %d, want %d: a refused ping must still be greppable",
			authFailBurst, got, authFailBurst)
	}

	const flood = 50
	for i := range flood {
		if got := badBearer(); got != http.StatusTooManyRequests {
			t.Fatalf("throttled attempt %d = %d, want 429", i+1, got)
		}
	}
	if got := logs.CountExact("http"); got != authFailBurst {
		t.Errorf("%d throttled attempts added %d access lines, want 0: the throttle must answer OUTSIDE webhttp.Logging, or a guessing run writes knell's log at wire speed",
			flood, got-authFailBurst)
	}
}

// TestFailedAuthDrawsATokenForEverySpellingTheGateAnswers pins the throttle
// against the ROUTE rather than one spelling of it. The bucket's endpoint half
// is the mux's own verdict (failedBeatAuth compares mux.Handler's pattern to
// beatRoutePattern), and ServeMux matches patterns on the ESCAPED path, so an
// encoded slash keeps the request on POST /beat/{id} and the bearer gate still
// answers it 401. Any spelling the gate answers is a guessing attempt against
// the endpoint's only credential and one access line per attempt, so it has to
// draw from the same bucket: otherwise a caller who appends %2F to the id has an
// unbounded oracle and an unbounded log flood, and every existing throttle test
// stays green.
func TestFailedAuthDrawsATokenForEverySpellingTheGateAnswers(t *testing.T) {
	// The families the mux reaches by different rules: an escaped character inside
	// the {id} segment (matched on the ESCAPED path), an escaped character in the
	// LITERAL segment (matched after unescaping it), and BOTH at once -- which
	// routes to POST /beat/{id} while satisfying neither raw view on its own. A
	// predicate that MODELLED the router instead of asking it exempted that last
	// family and handed out an unbounded guessing oracle; these cases now verify
	// the router-verdict predicate, so they are the regression net for ever
	// re-deriving the match textually.
	for _, target := range []string{
		"/beat/api", "/beat/%61pi", "/beat/a%2Fb", "/beat/ghost%2F",
		"/%62eat/api", "/%62eat/%61pi",
		"/%62eat/a%2Fb", "/%62eat/ghost%2F",
	} {
		t.Run(target, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{"api": true}}
			h := newTestHandler(b, testBeatToken)

			var throttled bool
			for i := range authFailBurst * 3 {
				req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(""))
				req.Header.Set("Authorization", "Bearer wrong")
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code == http.StatusTooManyRequests {
					throttled = true
					break
				}
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("attempt %d on %s with a bad bearer = %d, want 401 or 429 (body %s)",
						i+1, target, rec.Code, rec.Body.String())
				}
			}
			if !throttled {
				t.Errorf("POST %s answered %d failed authentications unthrottled: the token is the endpoint's only gate, so every spelling the gate answers must draw from the same bucket",
					target, authFailBurst*3)
			}
			if len(b.seen) != 0 {
				t.Errorf("recorded %v, want nothing: no failed-auth attempt may record", b.seen)
			}
		})
	}
}
