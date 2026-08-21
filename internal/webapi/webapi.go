// Package webapi assembles knell's HTTP surface: the beat ingestion
// endpoint, the health endpoint, and Prometheus metrics, wrapped in the
// standard middleware stack.
package webapi

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/cplieger/knell/internal/obs"
	"github.com/cplieger/knell/internal/watch"
	"github.com/cplieger/webhttp/v2"
)

// maxBeatBody caps how much of a ping request body is drained: draining keeps
// connections reusable, the cap keeps a hostile body from tying up the handler.
// An over-limit body is refused as a BODY, never as a ping.
const maxBeatBody = 1 << 20

// loggedPathCap tightens webhttp's 512-byte cap on the access line's path
// attribute. Every path knell legitimately serves is short while r.URL.Path is
// attacker-controlled, so 128 keeps a beat id intact and truncates whatever a
// scanner sends. It bounds one line's SIZE, not their number.
const loggedPathCap = 128

// bearerPrefix is the Authorization scheme senders present the beat token
// under. The verifier is built over the WHOLE expected field value, so knell
// never parses the credential out of the header.
const bearerPrefix = "Bearer "

// beatRoutePattern is the ONE spelling of the recording route: what New
// registers and what the failed-auth throttle compares the mux's own verdict
// against. Do not inline it at either site.
const beatRoutePattern = "POST /beat/{id}"

// Beater records pings. Implemented by watch.Watcher.
type Beater interface {
	// Beat records id when admission is open and reports the outcome:
	// BeatUnknown for an unknown id, BeatClosed once the watcher has closed
	// admission (nothing recorded either way), BeatRecorded otherwise.
	Beat(id string) watch.BeatOutcome
}

// Deps carries what the composition root supplies and webapi cannot reach on
// its own, named rather than positional because the four are otherwise
// indistinguishable at a call site.
type Deps struct {
	// Healthz answers liveness.
	Healthz http.Handler
	// Hosts is the exact-match Host allowlist (ALLOWED_HOSTS). An inactive
	// policy accepts every Host, the documented default; an active one is what
	// breaks DNS rebinding, since a rebinding request carries the ATTACKER's
	// hostname in Host and BeatToken gates the beat route only. It wraps the
	// WHOLE mux, so an active allowlist 403s /healthz and /metrics too unless
	// every hostname a probe reaches knell by is listed.
	Hosts *webhttp.HostPolicy
	// BeatToken is the REQUIRED bearer credential for POST /beat/{id} and the
	// endpoint's only gate. An empty value is a wiring bug rather than a mode:
	// the gate then fails CLOSED and every ping is refused.
	BeatToken string
	// TrustedProxies is the reverse-proxy CIDR set client_ip is resolved
	// against. Empty honors no X-Forwarded-For, the spoof-proof default.
	//
	// Last because a slice's length and capacity words are not pointers, which
	// keeps the pointer-scanned prefix at 48 bytes (govet fieldalignment).
	TrustedProxies []*net.IPNet
}

// New assembles the routed and middleware-wrapped root handler.
//
// The beat endpoint keeps NO lifecycle state of its own: watch.Watcher decides
// admission under the mutex that guards the beat mutation, and a second view of
// that question here could only approximate it, since no check made before Beat
// is atomic with the recording. Only the beat endpoint refuses during the drain;
// /healthz and /metrics keep serving so the orchestrator sees the marker flip
// and a last scrape of the freshness exposition still lands.
func New(b Beater, deps Deps) http.Handler {
	mux := http.NewServeMux()
	// POST is the ONLY method that records: a recording GET is reachable from
	// anything that fetches a URL unasked -- a link preview, a crawler, an
	// <img> -- and would re-arm the switch with no heartbeat behind it.
	verifier := beatTokenVerifier(deps.BeatToken)
	mux.HandleFunc(beatRoutePattern, beatHandler(b, verifier))
	// Every OTHER method is refused HERE, by this one method-agnostic pattern:
	// it is the ONLY registration that answers them, so delete it and net/http
	// answers instead, with whatever it synthesizes from the rest.
	mux.HandleFunc("/beat/{id}", writeMethodNotAllowed)
	// The /beat spellings a misconfigured sender URL produces. Without a route
	// they fall to net/http's own 404 -- or, for the bare path, a 307, a SUCCESS
	// status, so a `curl -fsS` sender EXITS 0 having recorded nothing.
	mux.HandleFunc("/beat", writeUnknownBeat)
	mux.HandleFunc("/beat/{$}", writeUnknownBeat)
	mux.HandleFunc("/beat/{id}/{rest...}", writeUnknownBeat)
	mux.Handle("GET /healthz", deps.Healthz)
	mux.Handle("GET /metrics", obs.Handler())

	return webhttp.Chain(mux,
		// Header baselines first, ahead of the throttle: its refusal is as
		// uncacheable as the success it replaces and entitled to the same
		// security headers.
		webhttp.SecurityHeaders(),
		// Every route knell serves is dynamic state and every refusal is as
		// time-dependent as the success it replaces, so no-store goes on the
		// whole surface rather than one route.
		webhttp.NoStore(),
		// OUTERMOST among the refusing middleware, ahead of the access logger:
		// a request with no valid beat token is answered 429 without reaching
		// Logging, so a guessing run cannot flood the log it would write a line
		// per attempt to. A valid ping never draws a token.
		beatAuthFailureLimiter(mux, verifier),
		// /healthz and /metrics are machine probes, so they ride ProbeLogLevel
		// rather than a skip list, which silenced the failures too on the two
		// endpoints carrying knell's quorum signal. WithRecordRouteMetric is
		// the only place a REFUSED ping (401, 404, 405, 503) becomes visible to
		// a scrape, since none reach beatsReceived; the LIBRARY derives the
		// label pair, which is what keeps a scanner from minting series. The
		// throttle's 429 is answered outside this wrapper and counted by reason
		// on the pre-route counter instead.
		webhttp.Logging(
			webhttp.ProbeLogLevel("/healthz", "/metrics"),
			webhttp.WithMaxLoggedPath(loggedPathCap),
			// EMPTY unless TRUSTED_PROXIES is set: with none it logs the socket
			// peer, and without it the 401 lines a guessing run writes name no
			// source at all. Trusting a forwarded header with no proxy in front
			// is how the field becomes spoofable.
			webhttp.WithClientIP(deps.TrustedProxies...),
			webhttp.WithRecordRouteMetric(obs.RecordHTTP),
		),
		webhttp.Recoverer(),
		// Immediately outside the Host policy, counting the refusal it answers:
		// a pre-route 403 collapses onto the "unmatched" label.
		countHostRefusals(deps.Hosts),
		// Inside the standard wrappers, so a rejected Host still answers with
		// security and no-store headers. Inactive when ALLOWED_HOSTS is unset.
		deps.Hosts.Middleware(),
		// Inside the Host policy, so a refused Host is still refused as a Host
		// (403) and not as an unknown beat; and inside webhttp.Logging, so its
		// 404 keeps an access line and a route-metric series.
		canonicalBeatPath,
	)
}

// canonicalBeatPath refuses a /beat request whose path net/http would rewrite
// before route matching, so no /beat spelling can answer a redirect: ServeMux
// sanitizes repeated slashes and dot segments BEFORE pattern selection, so
// /beat//, /beat/./api and //beat/api answer 307 -- a SUCCESS status to the
// documented `curl -fsS` sender. The trailing slash CanonicalRequestPath
// preserves is load-bearing: ServeMux does NOT redirect a path that only ends
// in one when a pattern matches it. knell passes the DECODED path, the one a
// sender believed it was pinging.
func canonicalBeatPath(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := r.URL.Path
		clean, canonical := webhttp.CanonicalRequestPath(raw)
		if !canonical && (inBeatNamespace(raw) || inBeatNamespace(clean)) {
			// This lands before the mux routes, so the request counter buckets
			// the whole class as "unmatched" beside scanner traffic.
			obs.RecordPreRouteRefusal(obs.RefusalNonCanonicalBeatPath)
			writeUnknownBeat(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// inBeatNamespace reports whether p is the bare /beat prefix or a path under
// it. Both the raw and the cleaned spelling matter to canonicalBeatPath: a
// request can ENTER the namespace only after cleaning (//beat/api) or leave one
// id for another (/beat/api/../ghost).
func inBeatNamespace(p string) bool {
	return p == "/beat" || strings.HasPrefix(p, "/beat/")
}

// writeMethodNotAllowed refuses a beat request whose method is not POST. It is
// the single home of that refusal, so no response can tell a sender that a
// method which does not record a beat is allowed to. GET and HEAD are
// deliberately absent from the Allow set: neither records a beat.
func writeMethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	webhttp.SetAllow(w, http.MethodPost)
	webhttp.WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed",
		"use POST to record a beat")
}

// writeUnknownBeat refuses a ping that names no configured beat. It answers
// every /beat path the route table matches and every non-canonical one
// canonicalBeatPath refuses, so a sender parsing knell's coded body does not
// hit net/http's plain 404 or its 307. The exception is a path whose ESCAPED
// form matches no pattern (/beat%2Fapi): canonicalBeatPath judges the decoded
// view, which is canonical there, so net/http's own plain 404 answers it.
func writeUnknownBeat(w http.ResponseWriter, r *http.Request) {
	webhttp.WriteError(w, r, http.StatusNotFound, "unknown_beat",
		"no beat at this URL: check the id against BEATS, and check the URL for extra or repeated path segments")
}

// drainBeatBody drains the deliberately ignored ping payload so keep-alive
// connections stay reusable, capped at maxBeatBody. The cap is a
// MaxBytesReader rather than a bare io.LimitReader because a LimitReader ends
// the read SILENTLY: the WARN below is the only channel through which an
// operator learns a sender ships payloads knell refuses to read. Every read
// error stays nonfatal -- a dropped ping is what this app exists to notice.
func drainBeatBody(w http.ResponseWriter, r *http.Request) {
	webhttp.LimitBody(w, r, maxBeatBody)
	_, err := io.Copy(io.Discard, r.Body)
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		// No beat id: it is still an unvalidated path segment here, so
		// correlate through the request id.
		slog.WarnContext(r.Context(), "beat body exceeded the cap and was not fully read",
			"limit_bytes", maxBeatBody,
			"request_id", webhttp.RequestIDFromContext(r.Context()))
	}
}

// beatTokenVerifier builds the constant-time verifier for the beat gate over
// the WHOLE expected field value, once and outside the request path. An empty
// token yields the ZERO verifier, which fails CLOSED; skipping the gate instead
// would leave bearerPrefix alone as a value any client could present verbatim.
func beatTokenVerifier(token string) webhttp.StaticTokenVerifier {
	if token == "" {
		return webhttp.StaticTokenVerifier{}
	}
	return webhttp.NewStaticTokenVerifier(bearerPrefix + token)
}

// presentsValidBeatToken reports whether r carries the configured beat
// credential. Shared by the handler's gate and the throttle's predicate, which
// would otherwise drift into throttling senders the gate admits or exempting
// the attempts the throttle bounds.
func presentsValidBeatToken(verifier webhttp.StaticTokenVerifier, r *http.Request) bool {
	return verifier.Verify(r.Header.Get("Authorization"))
}

// failedBeatAuth reports whether r is a FAILED authentication on the beat
// endpoint: the exact class the throttle bounds. The endpoint half of that
// verdict is the ROUTER's, not a model of it -- only beatRoutePattern runs the
// bearer gate, so an absent credential elsewhere must not draw a token, and
// ServeMux.Handler performs the full match, method included, so no copy of
// net/http's unescaping can drift from it. A non-canonical spelling still draws
// a token: over-inclusion is harmless, under-inclusion is a bypass.
func failedBeatAuth(mux *http.ServeMux, verifier webhttp.StaticTokenVerifier, r *http.Request) bool {
	_, pattern := mux.Handler(r)
	return pattern == beatRoutePattern && !presentsValidBeatToken(verifier, r)
}

// beatAuthFailureLimiter throttles FAILED authentication on the beat endpoint
// through one shared webhttp.FailedAuthRateLimit bucket, so a valid ping is
// never throttled however large the sender fleet grows. The tuning is the
// library preset's; knell keeps the POLICY. The bucket is deliberately
// AGGREGATE, with no per-client identity and no knob: it caps the total
// failed-auth rate, which bounds the guessing rate against the endpoint's only
// gate and the one-line-per-attempt log flood, and per-client fairness would
// need a trusted identity knell does not have.
func beatAuthFailureLimiter(mux *http.ServeMux, verifier webhttp.StaticTokenVerifier) webhttp.Middleware {
	// A nil predicate, deliberately: the wrapper below hands the limiter ONLY
	// predicate-matching requests, so a second copy could only disagree with
	// the first.
	limit := webhttp.FailedAuthRateLimit(nil, "too many failed beat token attempts")
	return func(next http.Handler) http.Handler {
		limited := limit(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !failedBeatAuth(mux, verifier, r) {
				// Straight to next, never through the bucket: the limiter draws
				// a token from every request it sees.
				next.ServeHTTP(w, r)
				return
			}
			recorder := webhttp.NewStatusRecorder(w)
			limited.ServeHTTP(recorder, r)
			if recorder.Status() == http.StatusTooManyRequests {
				obs.RecordPreRouteRefusal(obs.RefusalAuthThrottled)
			}
		})
	}
}

// countHostRefusals counts an ALLOWED_HOSTS refusal by reason and refuses
// nothing itself: the verdict is read through the policy's exported predicate
// rather than re-derived, so the count cannot drift from the refusal. An
// inactive policy returns next unwrapped.
func countHostRefusals(hosts *webhttp.HostPolicy) webhttp.Middleware {
	if !hosts.Active() {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !hosts.Allows(r) {
				obs.RecordPreRouteRefusal(obs.RefusalHostNotAllowed)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// HostPolicyOptions is the ALLOWED_HOSTS policy SHAPE knell ships:
// loopback-exempt, with the ALLOWED_HOSTS-naming 403 envelope. It lives here
// rather than beside the parsing because both halves are this package's, so the
// refusal's shape and its accounting cannot drift apart. WithLoopbackExempt
// admits a request only when BOTH the socket peer and the Host are loopback, so
// a rebinding request never qualifies.
func HostPolicyOptions() []webhttp.HostAllowlistOption {
	return []webhttp.HostAllowlistOption{
		webhttp.WithLoopbackExempt(),
		webhttp.WithHostAllowlistError(string(obs.RefusalHostNotAllowed),
			"host not allowed; add it to ALLOWED_HOSTS to serve this hostname"),
	}
}

// beatHandler records a ping and answers {"ok":true}, or 404 for an id that is
// not configured. Unknown ids are never recorded or counted: the id feeds a
// metric label. The credential is required, so there is no ungated mode.
func beatHandler(b Beater, verifier webhttp.StaticTokenVerifier) http.HandlerFunc {
	record := beatRecorder(b)
	return func(w http.ResponseWriter, r *http.Request) {
		// Authorize before touching the body, so a rejected sender cannot hold
		// the handler open by trickling a payload.
		if !presentsValidBeatToken(verifier, r) {
			// RFC 9110 §11.6.1: a 401 MUST name at least one challenge. No
			// realm: the endpoint has exactly one credential.
			w.Header().Set("WWW-Authenticate", "Bearer")
			webhttp.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing or invalid beat token")
			return
		}
		record(w, r)
	}
}

// beatRecorder builds the recording half of the beat endpoint: the body drain,
// watcher admission and the unknown-id 404. It holds the request-ordering
// decisions and beatHandler holds the credential one.
func beatRecorder(b Beater) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// An over-cap sender is reported, never refused.
		drainBeatBody(w, r)
		id := r.PathValue("id")
		// Beat is the ONE lifecycle check, and the authoritative one: it decides
		// admission under the mutex guarding the state change, so a ping
		// arriving during the drain gets its verdict on the far side of the read.
		switch b.Beat(id) {
		case watch.BeatClosed:
			// Nothing was recorded, so the sender must not be told 200. The
			// refusal names no beat id, so it leaks as little as the 404 path.
			webhttp.WriteError(w, r, http.StatusServiceUnavailable, "shutting_down",
				"knell is shutting down and is no longer accepting beats")
		case watch.BeatUnknown:
			writeUnknownBeat(w, r)
		case watch.BeatRecorded:
			webhttp.Ok(w)
		}
	}
}
