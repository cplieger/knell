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
	"path"
	"strings"
	"time"

	"github.com/cplieger/knell/internal/metrics"
	"github.com/cplieger/knell/internal/watch"
	"github.com/cplieger/webhttp"
)

// maxBeatBody caps how much of a ping request body is drained. Senders like
// an Alertmanager webhook attach JSON payloads knell ignores; draining keeps
// connections reusable, the cap keeps a hostile body from tying the handler.
// An over-limit body is refused as a BODY, never as a ping: see drainBeatBody.
const maxBeatBody = 1 << 20

// loggedPathCap is the byte cap knell asks webhttp to apply to the access
// line's path attribute, tightening the library's 512-byte default. Every path
// knell legitimately serves is short (/healthz, /metrics, /beat/{id} with an id
// capped at 64 chars by internal/config), while r.URL.Path is
// attacker-controlled and net/http accepts a megabyte of it, so 128 keeps the
// concrete beat id an operator needs intact while a flood cannot push knell's
// own undelivered-notice warnings out of the retained log window.
const loggedPathCap = 128

// bearerPrefix is the Authorization scheme senders present the beat token
// under. The verifier is built over the WHOLE expected field value
// (bearerPrefix + token), so acceptance is exactly one header value and knell
// never parses the credential out of the header.
const bearerPrefix = "Bearer "

// authFailBurst and authFailRefill tune the AGGREGATE failed-auth throttle on
// the beat endpoint: every request presenting an absent or invalid bearer draws
// one token from a single process-wide bucket, and knell answers 429 once it is
// empty. A valid ping never draws, so the numbers do not have to leave room for
// knell's own senders however many there are.
//
// They are pg-autodump's numbers deliberately: the two apps guard the same
// shape (one static bearer on one POST route), the bounded resource is the same
// (a log line and the token digests an attempt costs, not the beat recording
// itself), and one tuning across the fleet is one thing to reason about. Ten
// back-to-back
// attempts absorb an operator retrying a rotated token by hand; the 6s refill
// turns a guessing run at wire speed into ten attempts per minute.
const (
	authFailBurst  = 10
	authFailRefill = 6 * time.Second
)

// Beater records pings. Implemented by watch.Watcher.
type Beater interface {
	// Beat records id when admission is open and reports which of the three
	// outcomes it was: watch.BeatUnknown for an unknown id, watch.BeatClosed
	// once the watcher has closed admission for shutdown (nothing recorded in
	// either case), watch.BeatRecorded otherwise.
	Beat(id string) watch.BeatOutcome
}

// Deps carries what the composition root supplies and webapi cannot reach on
// its own: the liveness handler (built over main's health.Marker), the Host
// allowlist, and the beat credential. They are named rather than positional
// because the three are otherwise indistinguishable at a call site. The
// Prometheus exposition is deliberately absent: webapi already imports
// internal/metrics for the route-metric hook, so it serves metrics.Handler()
// itself rather than having the same dependency injected a second way.
type Deps struct {
	// Healthz answers liveness.
	Healthz http.Handler
	// Hosts is the exact-match Host allowlist (ALLOWED_HOSTS). A nil or
	// inactive policy accepts every Host, which is the documented default;
	// an active one is what breaks DNS rebinding, since a rebinding request
	// carries the ATTACKER's hostname in Host and nothing else on /healthz or
	// /metrics looks at that: BeatToken gates the BEAT route only, so without
	// an allowlist a rebinding page still reads the exposition that
	// enumerates every beat and its freshness.
	//
	// Because that is the exposure, the gate wraps the WHOLE mux rather than
	// the beat route: an ACTIVE allowlist refuses a foreign Host on /healthz
	// and /metrics too, not just /beat/{id}. That is deliberate — exempting the
	// probe routes would leave the exposition readable by the request this
	// exists to stop — but it means every hostname or IP a probe or a scraper
	// reaches knell by has to be listed, or those routes 403 for the operator's
	// own monitoring. The README's ALLOWED_HOSTS row says so for operators;
	// this says so for the next reader of New.
	Hosts *webhttp.HostPolicy
	// BeatToken is the REQUIRED bearer credential for POST /beat/{id} and the
	// endpoint's only gate (internal/config refuses to start without it). An
	// empty value is a wiring bug rather than a mode: the gate then fails
	// CLOSED and every ping is refused. Last because a string's length word is
	// not a pointer, which keeps the struct's pointer-scanned prefix at 32
	// bytes (govet fieldalignment).
	BeatToken string
}

// New assembles the routed and middleware-wrapped root handler.
// deps.BeatToken is the required credential gating the beat endpoint.
//
// appCtx is the shared application context: the one main cancels on SIGTERM,
// and the very same one watch.Run stops on. Cancelling it closes the beat
// endpoint's two EARLY refusal points at once — a ping that arrives after
// cancellation is refused with 503 before its body is touched, and one already
// draining a body is refused when that read returns — so a shutting-down
// endpoint does no recording work it does not have to.
//
// Those checks read the context rather than a flag flipped by the server's
// pre-drain hook because pre-drain runs on webhttp.Run's goroutine while
// watch.Run returns on its own, so which of the two happens first is a race,
// and a flag one of them owns leaves /beat/{id} fully live for the rest of the
// drain. Cancellation is the one instant both goroutines observe.
//
// What cancellation does NOT do is decide acceptance. A ping that passes the
// final context check can still be descheduled and then win the watcher mutex
// before watch.Run reaches its ctx.Done arm and calls stopAccepting; that ping
// is accepted and fully recorded. That is safe, and it is why the boundary
// lives there: Beat decides admission under the same mutex that guards the
// state change, so such a ping completes before admission closes and is
// therefore counted in the shutdown tally logUndelivered reports (see
// watch.Watcher.Beat) rather than landing behind a tally already taken.
//
// Only the beat endpoint refuses. /healthz and /metrics keep serving through
// the whole drain: the orchestrator has to see the liveness marker flip, and a
// last scrape of the freshness exposition during the drain is useful. That is a
// statement about the DRAIN only — an active ALLOWED_HOSTS allowlist refuses a
// foreign Host on all three routes at every point in the lifecycle, drain
// included (see Deps.Hosts).
func New(appCtx context.Context, b Beater, deps Deps) http.Handler {
	mux := http.NewServeMux()
	// POST is the ONLY method that records. GET and HEAD are registered
	// explicitly to REFUSE, for the same reason: a recording GET is reachable
	// from anything that fetches a URL without being asked to — a chat client's
	// link preview, a crawler, an <img> on a page an operator opens — and such a
	// fetch would re-arm the switch with no heartbeat behind it. Registering
	// them also overrides net/http's rule that a GET pattern matches HEAD, so
	// neither can ever fall through to the recorder. Do not delete either as
	// redundant boilerplate.
	verifier := beatTokenVerifier(deps.BeatToken)
	mux.HandleFunc("POST /beat/{id}", beatHandler(appCtx, b, verifier))
	mux.HandleFunc("GET /beat/{id}", writeMethodNotAllowed)
	mux.HandleFunc("HEAD /beat/{id}", writeMethodNotAllowed)
	// Every OTHER method (PUT, DELETE, PATCH, OPTIONS, an unknown verb) would
	// otherwise fall to net/http's built-in 405, which assembles Allow from the
	// registered patterns and so answers "GET, HEAD, POST" -- advertising as
	// permitted the two methods the routes above exist to refuse -- with a
	// plain-text body instead of this file's coded envelope. The
	// method-agnostic pattern is less specific than the three method-bearing
	// ones, so GET, POST and HEAD still route above.
	mux.HandleFunc("/beat/{id}", writeMethodNotAllowed)
	// A /beat path that names no configured beat answers this file's coded 404
	// rather than net/http's plain-text one. /beat is the bare prefix a sender
	// built from a truncated URL sends; /beat/{$} is the EMPTY id a sender
	// built from an unset variable sends; /beat/{id}/{rest...} is the
	// trailing-slash or extra-segment URL join. Without a route all three fall
	// to net/http's own 404 (or, for the bare path, its 307), which carries no
	// code for a sender to parse and lands
	// in the request counter's "unmatched" bucket beside scanner traffic -- so
	// the one refusal class that means "this beat is never being pinged" cannot
	// be told apart from a port scan. Three patterns rather than one /beat/
	// subtree so the path label still says WHICH shape arrived. All three are
	// less specific than the four /beat/{id} patterns above, so a real ping
	// still routes there.
	//
	// The bare /beat carries a reason of its own. Without an exact pattern for
	// it, net/http synthesizes a 307 to /beat/ -- and 307 is a SUCCESS status,
	// so the documented `curl -fsS` sender pointed at /beat (a truncated
	// variable, a bad copy-paste) EXITS 0 having recorded nothing, leaving the
	// beat to read as missing a full deadline later with nothing anywhere saying
	// the URL was wrong. The coded 404 makes such a sender fail at the ping.
	mux.HandleFunc("/beat", writeUnknownBeat)
	mux.HandleFunc("/beat/{$}", writeUnknownBeat)
	mux.HandleFunc("/beat/{id}/{rest...}", writeUnknownBeat)
	mux.Handle("GET /healthz", deps.Healthz)
	mux.Handle("GET /metrics", metrics.Handler())

	return webhttp.Chain(mux,
		// Header baselines first, ahead of the throttle below: that refusal is
		// answered before webhttp.Logging runs, and it is still a knell response
		// -- as uncacheable as the success it replaces, and entitled to the same
		// security headers. Both middlewares only Set headers before next runs,
		// so wrapping the throttle costs a routed request nothing.
		webhttp.SecurityHeaders(),
		// Every route knell serves is dynamic state: a ping acknowledgement, a
		// liveness verdict, and the freshness exposition that IS the quorum
		// ground truth. None of it may be answered from a cache, and every
		// refusal is as time-dependent as the success it replaces, so no-store
		// goes on the whole surface rather than one route -- including the
		// throttle's 429 and the Host policy's 403, both of which are answered
		// before any route runs.
		webhttp.NoStore(),
		// OUTERMOST among the refusing middleware, ahead of the access logger on
		// purpose: a request that presents no valid beat token is answered 429
		// here without reaching Logging, so a guessing run at wire speed cannot
		// flood the log it would otherwise write one line per attempt to. A
		// ping with a valid token never draws a token from the bucket, so
		// knell's own senders can never throttle themselves. See
		// beatAuthFailureLimiter.
		beatAuthFailureLimiter(verifier),
		// /healthz and /metrics are machine probes, so they ride the
		// fleet-standard ProbeLogLevel rather than a skip list: a HEALTHY
		// probe logs at Debug (out of the shipped stream at the default
		// level, visible under LOG_LEVEL=debug), a 4xx at Warn and a 5xx at
		// Error. Skipping them silenced the failures too — and these are the
		// two endpoints carrying knell's quorum signal, so a scrape that
		// stopped landing or a liveness probe answering 503 has to be
		// greppable. Skip lists stay for streams, of which knell has none.
		//
		// Both attacker-controlled halves of the access line are bounded by
		// webhttp itself, which matters because this line is emitted for every
		// request that gets past the throttle above and BEFORE beatHandler's
		// token gate runs, so BEAT_TOKEN bounds neither. The
		// method needs nothing from knell (24 bytes, then "(overlong)" — a
		// ceiling that follows from the method registry, not from this app),
		// and WithMaxLoggedPath only tightens the library's 512-byte path cap
		// to the one this three-route table can justify.
		//
		// WithRecordRouteMetric is the same story for the exposition: it is
		// the only place a REFUSED ping (401, 404, 405, 503) becomes visible
		// to a scrape, since none of them reach beatsReceived. The LIBRARY
		// derives the label pair (see webhttp.RouteMetricLabels) and hands it
		// straight to metrics.RecordHTTP, so knell has no derivation of its own
		// left to get wrong: the method is one of nine standard tokens or the
		// "other" bucket, and the path is a pattern this mux registered or the
		// "unmatched" marker. That bound is what keeps a scanner from minting
		// series through an endpoint the token gate has not seen yet.
		//
		// A 429 from the throttle above is deliberately NOT counted here: it is
		// answered outside this wrapper, so it is neither logged nor exposed on
		// this counter — it is counted by reason on knell's pre-route refusal
		// counter instead, along with the other two refusals that answer before
		// the mux routes.
		webhttp.Logging(
			webhttp.ProbeLogLevel("/healthz", "/metrics"),
			webhttp.WithMaxLoggedPath(loggedPathCap),
			webhttp.WithRecordRouteMetric(metrics.RecordHTTP),
		),
		webhttp.Recoverer(),
		// Immediately outside the Host policy, counting the refusal the policy
		// itself answers: a pre-route 403 collapses onto the request counter's
		// "unmatched" path label, so its cause is only nameable here. Refuses
		// nothing of its own. See countHostRefusals.
		countHostRefusals(deps.Hosts),
		// Inside the standard wrappers, so a rejected Host still answers with
		// security and no-store headers, but outside canonicalBeatPath, every
		// route, and beatHandler's token gate. Inactive when ALLOWED_HOSTS is
		// unset: HostPolicy.Middleware then returns next unwrapped.
		deps.Hosts.Middleware(),
		// Inside the Host policy, and inside nothing else: this guard answers
		// for knell's own routes only, so a refused Host must still be refused
		// as a Host (403) rather than as an unknown beat. See canonicalBeatPath.
		canonicalBeatPath,
	)
}

// canonicalBeatPath refuses a /beat request whose path net/http would rewrite
// before route matching, so no /beat spelling can answer a redirect.
//
// ServeMux sanitizes repeated slashes and literal dot segments in its own
// pass, which runs BEFORE pattern selection: /beat//, /beat/./api,
// /beat/api/../ghost and //beat/api all answer 307 with a Location, and no
// registered pattern can catch them (the exact /beat, /beat/{$} and
// /beat/{id}/{rest...} routes only close the spellings that reach matching).
// A 307 is a SUCCESS status to the documented `curl -fsS` sender, so such a
// sender exits 0 having recorded nothing and the beat reads as missing a full
// deadline later, with nothing anywhere saying the URL was malformed — the
// same failure the bare /beat route exists to prevent, one spelling class over.
//
// The comparison is against the sanitation net/http itself performs
// (path.Clean, with a trailing slash preserved — see net/http's cleanPath), so
// the guard covers every spelling ServeMux would rewrite. It is applied to the
// DECODED r.URL.Path while ServeMux compares the ESCAPED one, so it is slightly
// WIDER: an encoded dot segment (/beat/%2e%2e/ghost) draws no redirect from
// net/http but is refused here, and that is accepted deliberately: the decoded
// path is the one a sender believed it was pinging. EVERY refusal here lands
// before the mux routes, so none of them can inherit a route label — the whole
// class shares the request counter's "unmatched" path label with scanner
// traffic, and what tells the two apart is metrics.RecordPreRouteRefusal, the
// app-owned counter this guard increments by reason. /beat/ and
// /beat/{id}/ are already canonical either way and keep routing to their own
// patterns (which is what keeps their own request-counter labels).
// The guard fires only when the request is in (or cleans into) the /beat
// namespace, so every other path keeps net/http's own behavior. The verdict is
// writeUnknownBeat: a path that is not a canonical /beat/{id} names no
// configured beat, which is exactly what the other malformed-URL shapes
// already answer.
func canonicalBeatPath(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := r.URL.Path
		if clean := sanitizedPath(raw); clean != raw &&
			(inBeatNamespace(raw) || inBeatNamespace(clean)) {
			// Count the class by REASON, since the route label the mux never
			// assigned cannot say what this was: the request counter buckets it
			// as unmatched, and this is what names it for an operator reading
			// /metrics while investigating a beat that stopped arriving.
			metrics.RecordPreRouteRefusal(metrics.RefusalNonCanonicalBeatPath)
			writeUnknownBeat(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// sanitizedPath mirrors net/http's own cleanPath: path.Clean, with a trailing
// slash preserved. The trailing slash is load-bearing here, because ServeMux
// does NOT redirect a path that only ends in one when a pattern matches it —
// /beat/ and /beat/{id}/ are served by this file's own routes — so folding it
// away would make canonicalBeatPath swallow two refusals that already answer
// correctly under their own patterns.
func sanitizedPath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	clean := path.Clean(p)
	if p[len(p)-1] == '/' && clean != "/" {
		clean += "/"
	}
	return clean
}

// inBeatNamespace reports whether p is the bare /beat prefix or a path under
// it. Both the raw and the sanitized spelling matter to canonicalBeatPath: a
// request can ENTER the namespace only after cleaning (//beat/api) or leave one
// id for another (/beat/api/../ghost), and either way the sender believed it
// was pinging a beat.
func inBeatNamespace(p string) bool {
	return p == "/beat" || strings.HasPrefix(p, "/beat/")
}

// writeMethodNotAllowed refuses a beat request whose method is not POST: 405 in
// this file's standard coded envelope. It is the single home of the refusal
// EVERY rejected method answers with -- GET and HEAD included -- so no response
// can tell a sender that a method which does not record a beat is allowed to.
//
// webhttp.SetAllow renders the Allow field, and knell keeps its own message and
// its own writer rather than webhttp.RequireMethod: RequireMethod guards ONE
// handler from inside, while this refusal is the handler four routes are
// registered to, and webhttp.MethodNotAllowed would write the library's generic
// "method not allowed" body where this one names the method a sender should use
// instead. GET and HEAD are deliberately absent from the Allow set even though
// ServeMux would serve them -- New registers both to refuse them, so
// advertising either would contradict the refusal.
func writeMethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	webhttp.SetAllow(w, http.MethodPost)
	webhttp.WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed",
		"use POST to record a beat")
}

// writeUnknownBeat refuses a ping that names no configured beat: 404 in this
// file's standard coded envelope. It is the single home of that refusal for
// every /beat path a sender can produce -- an id the config does not carry, the
// BARE prefix (/beat, what a truncated URL sends), an EMPTY id (/beat/, what a
// URL built from an unset variable sends), and a nested path under one
// (/beat/{id}/, a trailing-slash URL join) -- so a sender
// parsing knell's coded body never hits net/http's plain "404 page not found"
// (or its 307 redirect) on exactly the spellings a misconfigured sender URL
// produces.
func writeUnknownBeat(w http.ResponseWriter, r *http.Request) {
	webhttp.WriteError(w, r, http.StatusNotFound, "unknown_beat", "unknown beat id")
}

// writeShuttingDown refuses a beat because admission is closed: 503 in this
// file's standard coded envelope. It names no beat id — not even an unknown one
// — so the refusal leaks as little as the 404 path does about which ids are
// configured, and it is the single home of the refusal all three admission
// checks in beatRecorder answer with (the two context checks and the watcher's
// authoritative BeatClosed outcome).
func writeShuttingDown(w http.ResponseWriter, r *http.Request) {
	webhttp.WriteError(w, r, http.StatusServiceUnavailable, "shutting_down",
		"knell is shutting down and is no longer accepting beats")
}

// drainBeatBody drains the deliberately ignored ping payload so keep-alive
// connections stay reusable, capped at maxBeatBody. The cap is
// webhttp.LimitBody (an http.MaxBytesReader) rather than a bare io.LimitReader
// because a LimitReader ends the read SILENTLY at the cap: the overrun has to be
// reportable, and the WARN below is the only channel through which an operator
// learns a sender ships payloads knell refuses to read. Every read error stays
// nonfatal — the ping answers 200 and the beat is recorded either way, because a
// heartbeat's payload is irrelevant and a dropped ping is what this app exists
// to notice.
func drainBeatBody(w http.ResponseWriter, r *http.Request) {
	webhttp.LimitBody(w, r, maxBeatBody)
	if _, err := io.Copy(io.Discard, r.Body); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			// No beat id and no sender-supplied text: the id here is still an
			// unvalidated path segment (the 404 gate is downstream), so
			// correlate through the access line via the request id.
			slog.WarnContext(r.Context(), "beat body exceeded the cap and was not fully read",
				"limit_bytes", maxBeatBody,
				"request_id", webhttp.RequestIDFromContext(r.Context()))
		}
	}
}

// beatTokenVerifier builds the constant-time verifier for the beat gate, over
// the WHOLE expected field value so acceptance stays exactly
// "Authorization: Bearer <token>" and no credential is ever parsed out of the
// header. It is built once, outside the request path.
//
// An empty token yields the ZERO verifier, which fails CLOSED for every
// presented value (webhttp.StaticTokenVerifier's documented zero behaviour).
// internal/config refuses to start without a token, so an empty one reaching
// here is a wiring bug — and it must not be handled by skipping the gate:
// bearerPrefix alone is a non-empty configured secret any client could present
// verbatim, which would be an open endpoint with a published credential.
func beatTokenVerifier(token string) webhttp.StaticTokenVerifier {
	if token == "" {
		return webhttp.StaticTokenVerifier{}
	}
	return webhttp.NewStaticTokenVerifier(bearerPrefix + token)
}

// presentsValidBeatToken reports whether r carries the configured beat
// credential. Shared by the handler's gate and the failed-auth throttle's
// predicate so the two can never drift on what counts as a valid bearer: a
// predicate that disagreed with the gate would either throttle senders the gate
// admits or exempt the very attempts the throttle exists to bound.
func presentsValidBeatToken(verifier webhttp.StaticTokenVerifier, r *http.Request) bool {
	return verifier.Verify(r.Header.Get("Authorization"))
}

// beatEndpointRequest reports whether r is addressed at the beat endpoint
// itself: POST /beat/{id}, the one route whose handler runs the bearer gate.
// Every other /beat spelling is refused before the gate on its own terms (405
// for another method, 404 for a path naming no beat), so an absent credential
// there is not a failed authentication and must not draw a throttle token.
//
// The path test is textual because the throttle runs OUTSIDE the mux, so
// r.Pattern is not populated yet: one segment under /beat/, no deeper path.
// That is exactly the shape the POST route matches, and a non-canonical
// spelling of it is refused by canonicalBeatPath rather than gated.
func beatEndpointRequest(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	// BOTH spellings, because neither alone covers the class the gate answers:
	// ServeMux matches patterns against the ESCAPED path, so an encoded slash
	// (/beat/a%2Fb) stays inside the {id} segment and routes to POST
	// /beat/{id}, while it also unescapes each request segment before comparing
	// it to a literal one, so an escaped letter in the literal segment
	// (/%62eat/api) routes there too. Reading only one of the two exempts the
	// other from the throttle the gate depends on. Over-inclusion is harmless
	// (a spelling the mux 404s draws a token it would otherwise not), while
	// under-inclusion is an unbounded guessing oracle and log flood.
	return singleSegmentUnderBeat(r.URL.EscapedPath()) || singleSegmentUnderBeat(r.URL.Path)
}

// singleSegmentUnderBeat reports whether p is exactly one non-empty segment
// under /beat/, the shape the POST /beat/{id} route matches.
func singleSegmentUnderBeat(p string) bool {
	id, found := strings.CutPrefix(p, "/beat/")
	return found && id != "" && !strings.Contains(id, "/")
}

// failedBeatAuth reports whether r is a FAILED authentication on the beat
// endpoint: the exact class the throttle bounds. beatAuthFailureLimiter is its
// only caller and gates BOTH the bucket and the 429 attribution on this one
// verdict, so there is no second predicate that could disagree about which
// requests the throttle can possibly refuse.
func failedBeatAuth(verifier webhttp.StaticTokenVerifier, r *http.Request) bool {
	return beatEndpointRequest(r) && !presentsValidBeatToken(verifier, r)
}

// beatAuthFailureLimiter throttles FAILED authentication on the beat endpoint
// through one shared webhttp.RateLimiter bucket (the pg-autodump composition).
// Only a request presenting an absent or invalid bearer to POST /beat/{id} draws
// a token, so a valid ping is never throttled however large the sender fleet
// grows or how badly a flood is running beside it; over-budget attempts are
// answered 429 with a Retry-After hint.
//
// The bucket is deliberately AGGREGATE — no per-client identity, no config knob.
// It caps the total failed-auth rate, which is what bounds both the guessing
// rate against the credential that is now the endpoint's only gate and the
// one-line-per-attempt log flood a network-exposed listener would otherwise
// allow at wire speed. Per-client fairness would need a trusted client identity
// knell does not have, and a knob would be one more thing to configure wrong on
// a gate that must simply hold.
//
// The 429 is answered outside webhttp.Logging, so it is neither logged nor
// counted on the request metric: this wrapper is the only place it becomes
// visible to a scrape, and it counts by REASON on knell's own pre-route refusal
// counter. Only a request the predicate admits is wrapped in a StatusRecorder,
// and within that class the limiter is the one thing that can answer 429 (a
// failed authentication that reaches the handler answers 401), so the status
// test attributes the refusal exactly without duplicating the bucket's state.
func beatAuthFailureLimiter(verifier webhttp.StaticTokenVerifier) webhttp.Middleware {
	// No WithRateLimitWhen: the wrapper below hands the limiter ONLY
	// predicate-matching requests, so the bucket already sees exactly the
	// failed-auth class and a second copy of the predicate could only
	// disagree with the first. One predicate evaluation here rather than two,
	// so a failed attempt costs two token digests (this wrapper's and
	// beatHandler's own gate) instead of three.
	limit := webhttp.RateLimiter(authFailBurst, authFailRefill,
		webhttp.WithRateLimitError("too_many_auth_failures",
			"too many failed beat token attempts"),
	)
	return func(next http.Handler) http.Handler {
		limited := limit(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !failedBeatAuth(verifier, r) {
				// Straight to next, never through the bucket: with the When
				// option gone, the limiter draws a token from every request it
				// sees, so this class must not reach it. Under the old option
				// this branch's pass through `limited` was the same no-op hop.
				next.ServeHTTP(w, r)
				return
			}
			recorder := webhttp.NewStatusRecorder(w)
			limited.ServeHTTP(recorder, r)
			if recorder.Status() == http.StatusTooManyRequests {
				metrics.RecordPreRouteRefusal(metrics.RefusalAuthThrottled)
			}
		})
	}
}

// countHostRefusals counts an ALLOWED_HOSTS refusal by reason, and refuses
// nothing itself: the 403 is the webhttp host policy's, and this wrapper sits
// immediately outside it so the policy's own verdict is what decides. The
// verdict is read through the library's exported Allows predicate rather than
// re-derived here, so the count cannot drift from the refusal.
//
// The refusal answers before the mux routes, so the request counter buckets it
// as "unmatched" beside scanner traffic; naming the cause is what tells a
// forgotten hostname in a deployment apart from a rebinding attempt or a port
// scan. An inactive (or nil) policy refuses nothing, so this returns next
// unwrapped — the same "off" contract HostPolicy.Middleware and RateLimiter use.
func countHostRefusals(hosts *webhttp.HostPolicy) webhttp.Middleware {
	if !hosts.Active() {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !hosts.Allows(r) {
				metrics.RecordPreRouteRefusal(metrics.RefusalHostNotAllowed)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// beatHandler records a ping and answers {"ok":true}, or 404 for an id that
// is not configured. Unknown ids are never recorded or counted: the id feeds
// a metric label, so arbitrary paths must not mint series. Senders must present
// Authorization: Bearer <token>; the credential is required, so there is no
// ungated mode. Once appCtx is cancelled the endpoint accepts nothing more
// (see New).
func beatHandler(appCtx context.Context, b Beater, verifier webhttp.StaticTokenVerifier) http.HandlerFunc {
	record := beatRecorder(appCtx, b)
	return func(w http.ResponseWriter, r *http.Request) {
		// Authorize before touching the body: a rejected sender must not
		// be able to hold the handler open by trickling a payload. The
		// credential gate therefore stays outermost, so an unauthorized ping
		// arriving during the drain answers 401 rather than 503 — either way
		// nothing is recorded, and the gate's verdict does not depend on the
		// lifecycle phase.
		if !presentsValidBeatToken(verifier, r) {
			// RFC 9110 §11.6.1: a 401 MUST name at least one challenge, so a
			// sender learns the scheme from the protocol and not only from the
			// README. No realm: the endpoint has exactly one credential.
			w.Header().Set("WWW-Authenticate", "Bearer")
			webhttp.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing or invalid beat token")
			return
		}
		record(w, r)
	}
}

// beatRecorder builds the recording half of the beat endpoint: the two lifecycle
// checks around the body drain, watcher admission and the unknown-id 404.
// beatHandler composes it with the bearer gate, so this constructor holds the
// request-ordering decisions and beatHandler holds the credential one.
func beatRecorder(appCtx context.Context, b Beater) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Acceptance is closed for the rest of this process's life: refuse
		// before the body is touched, so a ping arriving during the drain
		// cannot hold a handler goroutine (and with it the drain) open by
		// trickling a payload. A refused request is left undrained exactly
		// like the 401 one in beatHandler.
		if appCtx.Err() != nil {
			writeShuttingDown(w, r)
			return
		}
		// Cap and drain the body (the payload is deliberately ignored) so
		// keep-alive connections stay reusable; an over-cap sender is reported,
		// never refused. See drainBeatBody.
		drainBeatBody(w, r)
		id := r.PathValue("id")
		// Re-check on the far side of the body read: the read above can block
		// for the whole 30s read timeout, so a request ADMITTED while the app
		// was still live routinely arrives here after cancellation — that is
		// the window srv.Shutdown keeps open by design, since it waits for
		// in-flight requests. This check is not atomic with the recording - a
		// context check never can be, because this goroutine can be
		// descheduled between the two while watch.Run reports and abandons its
		// undelivered work - so Beat decides admission under the mutex that
		// guards the state change (see watch.Watcher.Beat) and its returned
		// outcome is the authoritative refusal. This check is still not
		// redundant: watch.Run only closes admission when its ctx.Done arm
		// runs, so for the whole interval between cancellation and that arm
		// Beat still admits pings, and this is the only refusal
		// covering a ping admitted pre-cancel that resumes in it. Do not
		// delete it as duplicated by the outcome below.
		if appCtx.Err() != nil {
			writeShuttingDown(w, r)
			return
		}
		switch b.Beat(id) {
		case watch.BeatClosed:
			// The watcher closed admission while this handler was in flight:
			// nothing was recorded, so the sender must not be told 200.
			writeShuttingDown(w, r)
		case watch.BeatUnknown:
			writeUnknownBeat(w, r)
		case watch.BeatRecorded:
			webhttp.Ok(w)
		}
	}
}
