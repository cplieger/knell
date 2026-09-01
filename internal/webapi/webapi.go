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
const maxBeatBody = 1 << 20

// loggedPathCap tightens webhttp's 512-byte cap on the access line's path
// attribute; every path knell legitimately serves is short.
const loggedPathCap = 128

// bearerPrefix is the Authorization scheme senders present the beat token
// under.
const bearerPrefix = "Bearer "

// beatRoutePattern is the one spelling of the recording route: what New
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

// Deps carries what the composition root supplies, named rather than
// positional because the four are otherwise indistinguishable at a call site.
type Deps struct {
	// Healthz answers liveness.
	Healthz http.Handler
	// Hosts is the exact-match Host allowlist (ALLOWED_HOSTS). An inactive
	// policy accepts every Host; an active one is what breaks DNS rebinding,
	// since BeatToken gates the beat route only.
	Hosts *webhttp.HostPolicy
	// BeatToken is the required bearer credential for POST /beat/{id}. An empty
	// value fails CLOSED and every ping is refused, rather than opening a mode.
	BeatToken string
	// TrustedProxies is the reverse-proxy CIDR set client_ip is resolved
	// against. Empty honors no X-Forwarded-For, the spoof-proof default.
	TrustedProxies []*net.IPNet
}

// New assembles the routed and middleware-wrapped root handler. The beat
// endpoint keeps no lifecycle state of its own: watch.Watcher decides
// admission under the mutex that guards the beat mutation. Only the beat
// endpoint refuses during the drain; /healthz and /metrics keep serving.
func New(b Beater, deps Deps) http.Handler {
	mux := http.NewServeMux()
	// POST is the only method that records: a recording GET is reachable from
	// a link preview or crawler and would re-arm the switch with no heartbeat
	// behind it.
	verifier := beatTokenVerifier(deps.BeatToken)
	mux.HandleFunc(beatRoutePattern, beatHandler(b, verifier))
	mux.HandleFunc("/beat/{id}", writeMethodNotAllowed)
	// The /beat spellings a misconfigured sender URL produces. Without a route
	// they fall to net/http's own 404 -- or, for the bare path, a 307, a
	// SUCCESS status that would exit a `curl -fsS` sender 0.
	mux.HandleFunc("/beat", writeUnknownBeat)
	mux.HandleFunc("/beat/{$}", writeUnknownBeat)
	mux.HandleFunc("/beat/{id}/{rest...}", writeUnknownBeat)
	mux.Handle("GET /healthz", deps.Healthz)
	mux.Handle("GET /metrics", obs.Handler())

	return webhttp.Chain(mux,
		webhttp.SecurityHeaders(),
		webhttp.NoStore(),
		// Outermost among the refusing middleware, ahead of the access logger:
		// a request with no valid beat token is answered 429 without reaching
		// Logging, so a guessing run cannot flood the log.
		beatAuthFailureLimiter(mux, verifier),
		// /healthz and /metrics are machine probes, so they ride ProbeLogLevel
		// rather than a skip list, which would silence failures too on the two
		// endpoints carrying knell's quorum signal. WithRecordRouteMetric is
		// the only place a refused ping (401/404/405/503) becomes visible to a
		// scrape, since none reach beatsReceived. The throttle's 429 is counted
		// on the pre-route counter instead, outside this wrapper.
		webhttp.Logging(
			webhttp.ProbeLogLevel("/healthz", "/metrics"),
			webhttp.WithMaxLoggedPath(loggedPathCap),
			// Empty unless TRUSTED_PROXIES is set: trusting a forwarded header
			// with no proxy in front is how the field becomes spoofable.
			webhttp.WithClientIP(deps.TrustedProxies...),
			webhttp.WithRecordRouteMetric(func(m webhttp.RequestMetric) {
				obs.RecordHTTP(m.Method, m.Path, m.Status, m.Latency)
			}),
		),
		webhttp.Recoverer(),
		// Immediately outside the Host policy, so a pre-route 403 collapses
		// onto the "unmatched" label.
		countHostRefusals(deps.Hosts),
		deps.Hosts.Middleware(),
		// Inside the Host policy, so a refused Host is refused as a Host (403)
		// and not as an unknown beat.
		canonicalBeatPath,
	)
}

// canonicalBeatPath refuses a /beat request whose path net/http would rewrite
// before route matching: ServeMux sanitizes repeated slashes and dot segments
// before pattern selection, so /beat//, /beat/./api and //beat/api would
// otherwise answer 307 -- a success status to the documented `curl -fsS`
// sender.
func canonicalBeatPath(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := r.URL.Path
		clean, canonical := webhttp.CanonicalRequestPath(raw)
		if !canonical && (inBeatNamespace(raw) || inBeatNamespace(clean)) {
			obs.RecordPreRouteRefusal(obs.RefusalNonCanonicalBeatPath)
			writeUnknownBeat(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// inBeatNamespace reports whether p is the bare /beat prefix or a path under
// it, checked against both the raw and cleaned spelling because a request can
// enter the namespace only after cleaning (//beat/api).
func inBeatNamespace(p string) bool {
	return p == "/beat" || strings.HasPrefix(p, "/beat/")
}

// writeMethodNotAllowed refuses a beat request whose method is not POST. GET
// and HEAD are deliberately absent from the Allow set: neither records a beat.
func writeMethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	webhttp.SetAllow(w, http.MethodPost)
	webhttp.WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed",
		"use POST to record a beat")
}

// writeUnknownBeat refuses a ping that names no configured beat. It answers
// every /beat path the route table matches and every non-canonical one
// canonicalBeatPath refuses.
func writeUnknownBeat(w http.ResponseWriter, r *http.Request) {
	webhttp.WriteError(w, r, http.StatusNotFound, "unknown_beat",
		"no beat at this URL: check the id against BEATS, and check the URL for extra or repeated path segments")
}

// drainBeatBody drains the deliberately ignored ping payload so keep-alive
// connections stay reusable, capped at maxBeatBody. A MaxBytesReader is used
// rather than a bare io.LimitReader because a LimitReader ends the read
// silently: the WARN below is the only channel through which an operator
// learns a sender ships payloads knell refuses to read.
func drainBeatBody(w http.ResponseWriter, r *http.Request) {
	webhttp.LimitBody(w, r, maxBeatBody)
	_, err := io.Copy(io.Discard, r.Body)
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		slog.WarnContext(r.Context(), "beat body exceeded the cap and was not fully read",
			"limit_bytes", maxBeatBody,
			"request_id", webhttp.RequestIDFromContext(r.Context()))
	}
}

// beatTokenVerifier builds the constant-time verifier for the beat gate over
// the whole expected field value, once and outside the request path.
func beatTokenVerifier(token string) webhttp.StaticTokenVerifier {
	if token == "" {
		return webhttp.StaticTokenVerifier{}
	}
	return webhttp.NewStaticTokenVerifier(bearerPrefix + token)
}

// presentsValidBeatToken reports whether r carries the configured beat
// credential. Shared by the handler's gate and the throttle's predicate so the
// two cannot drift apart.
func presentsValidBeatToken(verifier webhttp.StaticTokenVerifier, r *http.Request) bool {
	return verifier.Verify(r.Header.Get("Authorization"))
}

// failedBeatAuth reports whether r is a failed authentication on the beat
// endpoint: the exact class the throttle bounds. The endpoint half of that
// verdict is read from the router (mux.Handler), never modeled independently,
// so no local unescaping logic can drift from net/http's own. A non-canonical
// spelling still draws a token: over-inclusion is harmless, under-inclusion is
// a bypass.
func failedBeatAuth(mux *http.ServeMux, verifier webhttp.StaticTokenVerifier, r *http.Request) bool {
	_, pattern := mux.Handler(r)
	return pattern == beatRoutePattern && !presentsValidBeatToken(verifier, r)
}

// beatAuthFailureLimiter throttles failed authentication on the beat endpoint
// through one shared webhttp.FailedAuthRateLimit bucket, so a valid ping is
// never throttled. The bucket is deliberately aggregate, with no per-client
// identity: it caps the total failed-auth rate and the one-line-per-attempt
// log flood, and per-client fairness would need a trusted identity knell does
// not have.
func beatAuthFailureLimiter(mux *http.ServeMux, verifier webhttp.StaticTokenVerifier) webhttp.Middleware {
	limit := webhttp.FailedAuthRateLimit(nil, "too many failed beat token attempts")
	return func(next http.Handler) http.Handler {
		limited := limit(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !failedBeatAuth(mux, verifier, r) {
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

// countHostRefusals counts an ALLOWED_HOSTS refusal by reason, reading the
// verdict through the policy's own predicate rather than re-deriving it. An
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

// HostPolicyOptions is the ALLOWED_HOSTS policy shape knell ships:
// loopback-exempt, with the ALLOWED_HOSTS-naming 403 envelope. WithLoopbackExempt
// admits a request only when both the socket peer and the Host are loopback,
// so a rebinding request never qualifies.
func HostPolicyOptions() []webhttp.HostAllowlistOption {
	return []webhttp.HostAllowlistOption{
		webhttp.WithLoopbackExempt(true),
		webhttp.WithHostAllowlistError(webhttp.ErrorCode(obs.RefusalHostNotAllowed),
			"host not allowed; add it to ALLOWED_HOSTS to serve this hostname"),
	}
}

// beatHandler records a ping and answers {"ok":true}, or 404 for an id that is
// not configured. Unknown ids are never recorded or counted.
func beatHandler(b Beater, verifier webhttp.StaticTokenVerifier) http.HandlerFunc {
	record := beatRecorder(b)
	return func(w http.ResponseWriter, r *http.Request) {
		// Authorize before touching the body, so a rejected sender cannot hold
		// the handler open by trickling a payload.
		if !presentsValidBeatToken(verifier, r) {
			// RFC 9110 §11.6.1: a 401 must name at least one challenge.
			w.Header().Set("WWW-Authenticate", "Bearer")
			webhttp.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing or invalid beat token")
			return
		}
		record(w, r)
	}
}

// beatRecorder builds the recording half of the beat endpoint: the body
// drain, watcher admission and the unknown-id 404.
func beatRecorder(b Beater) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		drainBeatBody(w, r)
		id := r.PathValue("id")
		// Beat decides admission under the mutex guarding the state change, so
		// a ping arriving during the drain gets its verdict on the far side of
		// the read.
		switch b.Beat(id) {
		case watch.BeatClosed:
			webhttp.WriteError(w, r, http.StatusServiceUnavailable, "shutting_down",
				"knell is shutting down and is no longer accepting beats")
		case watch.BeatUnknown:
			writeUnknownBeat(w, r)
		case watch.BeatRecorded:
			webhttp.Ok(w)
		}
	}
}
