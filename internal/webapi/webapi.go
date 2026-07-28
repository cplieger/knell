// Package webapi assembles knell's HTTP surface: the beat ingestion
// endpoint, the health endpoint, and Prometheus metrics, wrapped in the
// standard middleware stack.
package webapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cplieger/knell/internal/metrics"
	"github.com/cplieger/webhttp"
)

// maxBeatBody caps how much of a ping request body is drained. Senders like
// an Alertmanager webhook attach JSON payloads knell ignores; draining keeps
// connections reusable, the cap keeps a hostile body from tying the handler.
// An over-limit body is refused as a BODY, never as a ping: see the drain in
// beatHandler.
const maxBeatBody = 1 << 20

// maxLoggedPath bounds the path attribute of an access-log line. Every path
// knell legitimately serves is short (/healthz, /metrics, /beat/{id} with an
// id capped at 64 chars by internal/config), while r.URL.Path is
// attacker-controlled and net/http accepts a megabyte of it, so the raw
// value is logged truncated: the concrete beat id an operator needs stays
// intact, and a flood cannot push knell's own undelivered-notice warnings
// out of the retained log window.
const maxLoggedPath = 128

// maxLoggedMethod bounds the request method the access log records. webhttp
// logs r.Method verbatim and offers no transform hook, while net/http accepts
// any RFC 9110 token up to the whole request-line limit (MaxHeaderBytes +
// 4096, 1 MiB by default), so an unauthenticated caller chooses the text. The
// bound is set by what knell SERVES, not by the IANA registry (whose longest
// entry, UPDATEREDIRECTREF, is 17): 16 bytes carries GET and POST and every
// common method a prober tries, and anything longer is a method knell refuses
// with 405 whatever the log keeps.
const maxLoggedMethod = 16

// overlongMethodLabel replaces a method too long to be a real one, the
// method-side twin of maxLoggedPath. Such a request is refused either way (it
// matches no method-bearing pattern, so /beat/{id} answers 405 and every other
// path gets net/http's own 404/405), and the metric already collapses it
// (otherMethodLabel on the /beat/{id} match, unmatchedLabel elsewhere), so
// nothing but the logged text changes.
const overlongMethodLabel = "OVERLONG"

// boundMethod caps the method before webhttp.Logging reads it. It is listed
// FIRST in Chain so it wraps Logging; the request is shallow-copied rather
// than mutated in place, the same way http.Request.WithContext does.
func boundMethod(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.Method) > maxLoggedMethod {
			r2 := *r
			r2.Method = overlongMethodLabel
			r = &r2
		}
		next.ServeHTTP(w, r)
	})
}

// loggedPath is the access log's path policy: the request path, truncated on
// a rune boundary. The empty case is mapped to "/" rather than left empty,
// because webhttp coerces an empty return to its redaction-failure
// placeholder (a path this transform never actually fails on).
func loggedPath(r *http.Request) string {
	p := r.URL.Path
	if p == "" {
		return "/"
	}
	if len(p) <= maxLoggedPath {
		return p
	}
	cut := maxLoggedPath
	for cut > 0 && !utf8.RuneStart(p[cut]) {
		cut--
	}
	return p[:cut] + "...(truncated)"
}

// Beater records pings. Implemented by watch.Watcher.
type Beater interface {
	// Beat records a ping for id, returning false for unknown ids.
	Beat(id string) bool
}

// Routes carries the pre-built handlers webapi serves beside the beat
// endpoint. They are named rather than positional because both are plain
// http.Handler values: a transposed pair would compile and quietly serve
// the metrics exposition at /healthz.
type Routes struct {
	// Healthz answers liveness.
	Healthz http.Handler
	// Metrics serves the Prometheus exposition.
	Metrics http.Handler
}

// New assembles the routed and middleware-wrapped root handler.
// token optionally gates the beat endpoint (empty = open).
//
// appCtx is the shared application context: the one main cancels on SIGTERM,
// and the very same one watch.Run stops on. Beat ACCEPTANCE closes the instant
// it is cancelled — from then on /beat/{id} refuses with 503 and records
// nothing at all.
//
// The gate is the context itself rather than a flag flipped by the server's
// pre-drain hook, and that is the whole point: pre-drain runs on
// webhttp.Run's goroutine while watch.Run returns on its own, so which of the
// two happens first is a race. A ping accepted in that window is fully
// recorded — lastSeen moves, the received counter and the freshness gauge move,
// and a recovery event is queued onto a channel whose only reader has already
// returned — AFTER watch.Run took its undelivered-work snapshot, so the notice
// is lost with no counter and no warning to show it. Cancellation is the one
// instant both goroutines observe, so gating on it closes acceptance at that
// same instant whatever order they run in, instead of leaving the endpoint
// fully live for the rest of the drain.
//
// Only the beat endpoint refuses. /healthz and /metrics keep serving through
// the whole drain: the orchestrator has to see the liveness marker flip, and a
// last scrape of the freshness exposition during the drain is useful.
func New(appCtx context.Context, b Beater, token string, routes Routes) http.Handler {
	mux := http.NewServeMux()
	// POST is the canonical ping; GET is accepted too so ad-hoc senders
	// (curl without flags, simple healthcheck hooks) can participate.
	beat := beatHandler(appCtx, b, token)
	mux.HandleFunc("POST /beat/{id}", beat)
	mux.HandleFunc("GET /beat/{id}", beat)
	// HEAD is registered explicitly ONLY to override net/http's rule that a
	// GET pattern also matches HEAD: without this route a HEAD probe would
	// reach beat and RECORD a ping, so any monitoring HEAD check pointed at
	// /beat/{id} would keep the switch armed forever with no real heartbeat
	// behind it. Do not delete it as redundant boilerplate.
	mux.HandleFunc("HEAD /beat/{id}", writeMethodNotAllowed)
	// Every OTHER method (PUT, DELETE, PATCH, OPTIONS, an unknown verb) would
	// otherwise fall to net/http's built-in 405, which assembles Allow from the
	// registered patterns and so answers "GET, HEAD, POST" -- advertising as
	// permitted the very method the route above exists to refuse -- with a
	// plain-text body instead of this file's coded envelope. The
	// method-agnostic pattern is less specific than the three method-bearing
	// ones, so GET, POST and HEAD still route above.
	mux.HandleFunc("/beat/{id}", writeMethodNotAllowed)
	mux.Handle("GET /healthz", routes.Healthz)
	mux.Handle("GET /metrics", routes.Metrics)

	return webhttp.Chain(mux,
		// boundMethod is FIRST so it is the OUTERMOST wrapper and caps
		// r.Method before Logging reads it: webhttp logs the method verbatim
		// with no transform hook, and the method is as caller-controlled as
		// the path WithPathFunc bounds below.
		boundMethod,
		// /healthz and /metrics are machine probes, so they ride the
		// fleet-standard ProbeLogLevel rather than a skip list: a HEALTHY
		// probe logs at Debug (out of the shipped stream at the default
		// level, visible under LOG_LEVEL=debug), a 4xx at Warn and a 5xx at
		// Error. Skipping them silenced the failures too — and these are the
		// two endpoints carrying knell's quorum signal, so a scrape that
		// stopped landing or a liveness probe answering 503 has to be
		// greppable. Skip lists stay for streams, of which knell has none.
		//
		// WithPathFunc bounds the logged path: Chain's FIRST entry is its
		// OUTERMOST wrapper, and only boundMethod sits ahead of Logging here,
		// so this line is emitted for every request BEFORE beatHandler's token
		// gate runs, which is why BEAT_TOKEN does not bound it and the policy
		// has to.
		//
		// WithRecordMetricRequest is the same story for the exposition: it is
		// the only place a REFUSED ping (401, 404, 405, 503) becomes visible
		// to a scrape, since none of them reach beatsReceived. It gets the
		// request-aware hook rather than the legacy WithRecordMetric because
		// the labels must key on the matched route, which only the request
		// carries (r.Pattern); recordHTTPMetric owns that derivation.
		webhttp.Logging(
			webhttp.ProbeLogLevel("/healthz", "/metrics"),
			webhttp.WithPathFunc(loggedPath),
			webhttp.WithRecordMetricRequest(recordHTTPMetric),
		),
		webhttp.Recoverer(),
		webhttp.SecurityHeaders(),
		noStore,
	)
}

// noStore marks every response uncacheable. Every route knell serves is
// dynamic state: a ping acknowledgement, a liveness verdict, and the
// freshness exposition that IS the quorum ground truth. None of it may be
// answered from a cache, and a GET ping is a documented sender mode, so the
// header goes on the whole surface rather than one route. It is listed last
// in Chain (innermost) so it lands before any handler writes a status.
func noStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

// unmatchedLabel is the method AND path recorded for a request that matched no
// route. They collapse together deliberately: such a request has nothing
// route-derived to report and both of its real values are caller-chosen, so a
// single series absorbs every scanner probe instead of one per probe.
const unmatchedLabel = "unmatched"

// otherMethodLabel is the method recorded for a request that matched the
// method-agnostic /beat/{id} route (the 405 catch-all New registers above). That
// pattern names no method, so r.Method there is whatever token the caller put
// on the request line, and net/http accepts any RFC 9110 token — measured over
// a real socket, both "XYZZY" and "M!#$%&'*+-.^_`|~" route to that pattern and
// answer 405. Recording r.Method there would therefore let one unauthenticated
// caller mint series without bound, so the metric records only that SOME
// disallowed method was refused; the real method is still in the access line.
const otherMethodLabel = "other"

// recordHTTPMetric feeds webhttp.Logging's request-aware metric hook and is the
// single home of knell's http_requests_total label derivation. Both labels come
// from the MATCHED ROUTE, never from the request line: Logging is outermost, so
// this hook fires for every request BEFORE beatHandler's token gate, meaning an
// unauthenticated caller chooses the input. A series once minted is permanent
// for the process lifetime — here and in every observer scraping knell — so the
// label set must stay bounded by the ROUTE TABLE. Three cases:
//
//   - Nothing matched (r.Pattern == ""): net/http's own 404, and its own 405
//     for a method that missed a method-bearing route (POST /healthz). Both
//     labels collapse to unmatchedLabel.
//   - A method-BEARING pattern matched ("GET /beat/{id}"): r.Method is recorded
//     as-is and is provably bounded, because ServeMux routes there only for
//     that pattern's method or for HEAD against a GET pattern — so a HEAD
//     /healthz probe records method HEAD while matching "GET /healthz", which
//     is the truthful reading. The path is the pattern's TEMPLATE, so every
//     beat id records as "/beat/{id}" and an unknown id mints no new series.
//   - The method-AGNOSTIC "/beat/{id}" pattern matched: method collapses to
//     otherMethodLabel. This is why knell cannot simply pass r.Method the way
//     the fleet siblings do — registry-stats and subflux register only
//     method-bearing patterns, so r.Method is bounded there and is not here.
func recordHTTPMetric(r *http.Request, status int, d time.Duration) {
	method, path := unmatchedLabel, unmatchedLabel
	if r.Pattern != "" {
		method, path = r.Method, r.Pattern
		if _, template, ok := strings.Cut(r.Pattern, " "); ok {
			path = template
		} else {
			method = otherMethodLabel
		}
	}
	metrics.RecordHTTP(method, path, status, d)
}

// writeMethodNotAllowed refuses a beat request whose method is not GET or
// POST: 405 in this file's standard coded envelope. It is the single home of
// the refusal EVERY rejected method answers with -- HEAD included -- so no
// response can tell a sender that a method which does not record a beat is
// allowed to.
//
// webhttp.SetAllow renders the Allow field (two methods, so
// webhttp.RequireMethod's single-method guard does not fit), and knell keeps
// its own message: webhttp.MethodNotAllowed would write the library's generic
// "method not allowed" body, while this one names the two methods a sender
// should use instead. HEAD is deliberately absent from the set even though
// ServeMux would serve it from the GET pattern -- New registers it separately
// to refuse it, so advertising it would contradict the refusal.
func writeMethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	webhttp.SetAllow(w, http.MethodGet, http.MethodPost)
	webhttp.WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed",
		"use GET or POST to record a beat")
}

// writeShuttingDown refuses a beat because acceptance is closed: 503 in this
// file's standard coded envelope. It names no beat id — not even an unknown one
// — so the refusal leaks as little as the 404 path does about which ids are
// configured, and it is the single home of the refusal both checks in record
// answer with.
func writeShuttingDown(w http.ResponseWriter, r *http.Request) {
	webhttp.WriteError(w, r, http.StatusServiceUnavailable, "shutting_down",
		"knell is shutting down and is no longer accepting beats")
}

// beatHandler records a ping and answers {"ok":true}, or 404 for an id that
// is not configured. Unknown ids are never recorded or counted: the id feeds
// a metric label, so arbitrary paths must not mint series. A non-empty token
// requires senders to present Authorization: Bearer <token>. Once appCtx is
// cancelled the endpoint accepts nothing more (see New).
func beatHandler(appCtx context.Context, b Beater, token string) http.HandlerFunc {
	record := func(w http.ResponseWriter, r *http.Request) {
		// Acceptance is closed for the rest of this process's life: refuse
		// before the body is touched, so a ping arriving during the drain
		// cannot hold a handler goroutine (and with it the drain) open by
		// trickling a payload. A refused request is left undrained exactly
		// like the 401 one below.
		if appCtx.Err() != nil {
			writeShuttingDown(w, r)
			return
		}
		// Cap the body, then drain what fits so keep-alive connections stay
		// reusable; the payload itself is deliberately ignored. The cap is
		// webhttp.LimitBody (an http.MaxBytesReader over r.Body) rather than a
		// bare io.LimitReader, because a LimitReader ends the read SILENTLY at
		// the cap: an over-limit body would be indistinguishable from one that
		// just ended, so nothing would ever say a sender is shipping payloads
		// knell refuses to read. MaxBytesReader surfaces the overrun as an
		// *http.MaxBytesError, which is what the WARN below reports.
		//
		// The overrun is reported, never acted on: the ping still answers 200
		// and the beat below is recorded whether the body fit, overran, or the
		// sender hung up mid-send, because a heartbeat's payload is irrelevant
		// and a dropped ping is what this whole app exists to notice. Only the
		// overrun warns; a mid-body disconnect is ordinary.
		//
		// net/http does close the connection under that 200 (LimitBody reaches
		// net/http's own ResponseWriter through the StatusRecorder Unwrap chain
		// webhttp.Logging and Recoverer wrap this handler in). The close is
		// observable only on the SENDER's side of the wire, and an
		// httptest.ResponseRecorder cannot observe it at all, so the WARN stays
		// both the only channel through which a knell OPERATOR learns a sender
		// is over the cap and the thing the handler tests pin.
		webhttp.LimitBody(w, r, maxBeatBody)
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				// No beat id and no sender-supplied text: the id here is still
				// an unvalidated path segment (the 404 gate is below), and the
				// request id ties this line to the access line that does carry
				// the truncated path.
				slog.WarnContext(r.Context(), "beat body exceeded the cap and was not fully read",
					"limit_bytes", maxBeatBody,
					"request_id", webhttp.RequestIDFromContext(r.Context()))
			}
		}
		id := r.PathValue("id")
		// Re-check on the far side of the body read: this is the check that
		// closes the window that matters, because the read above can block for
		// the whole 30s read timeout, so a request ADMITTED while the app was
		// still live routinely arrives here after cancellation — that is the
		// window srv.Shutdown keeps open by design, since it waits for
		// in-flight requests. Recording now would move lastSeen, count the
		// ping, republish the freshness gauge, and for an alerted beat queue a
		// recovered notification onto a channel whose only reader (watch.Run)
		// has already returned: a notice lost with no counter and no warning,
		// behind a tally watch.Run has already taken.
		if appCtx.Err() != nil {
			writeShuttingDown(w, r)
			return
		}
		if !b.Beat(id) {
			webhttp.WriteError(w, r, http.StatusNotFound, "unknown_beat", "unknown beat id")
			return
		}
		webhttp.Ok(w)
	}
	if token == "" {
		// Open endpoint: skip the gate explicitly (an armed verifier must
		// never exist for the empty-token configuration).
		return record
	}
	// The verifier is built once, outside the request path, over the full
	// expected header value so acceptance stays exactly
	// "Authorization: Bearer <token>".
	verifier := webhttp.NewStaticTokenVerifier("Bearer " + token)
	return func(w http.ResponseWriter, r *http.Request) {
		// Authorize before touching the body: a rejected sender must not
		// be able to hold the handler open by trickling a payload. The
		// credential gate therefore stays outermost, so an unauthorized ping
		// arriving during the drain answers 401 rather than 503 — either way
		// nothing is recorded, and the gate's verdict does not depend on the
		// lifecycle phase.
		if !verifier.Verify(r.Header.Get("Authorization")) {
			webhttp.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing or invalid beat token")
			return
		}
		record(w, r)
	}
}
