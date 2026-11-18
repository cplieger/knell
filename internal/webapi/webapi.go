// Package webapi assembles knell's ping listener: the beat ingestion endpoint
// (New) in the standard middleware stack. The operations listener is
// httpserver.ListenOps, wired in main.
package webapi

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/netip"
	"strings"

	"github.com/cplieger/go/httpserver/v2"
	"github.com/cplieger/knell/internal/obs"
	"github.com/cplieger/knell/internal/watch"
)

// maxBeatBody caps how much of a ping request body is drained: draining keeps
// connections reusable, the cap keeps a hostile body from tying up the handler.
const maxBeatBody = 1 << 20

// loggedPathCap tightens httpserver's 512-byte cap on the access line's path
// attribute; every path knell legitimately serves is short.
const loggedPathCap = 128

// bearerPrefix is the Authorization scheme senders present the beat token
// under.
const bearerPrefix = "Bearer "

// beatRoutePattern is the recording route.
const beatRoutePattern = "POST /beat/{id}"

// Beater records pings. Implemented by watch.Watcher.
type Beater interface {
	// Beat records id when admission is open and reports the outcome:
	// BeatUnknown for an unknown id, BeatClosed once the watcher has closed
	// admission (nothing recorded either way), BeatRecorded otherwise.
	Beat(id string) watch.BeatOutcome
}

// Deps carries what the composition root supplies to the ping listener.
type Deps struct {
	// Hosts is the exact-match Host allowlist (ALLOWED_HOSTS). An inactive
	// policy accepts every Host; an active one is what breaks DNS rebinding.
	Hosts *httpserver.HostPolicy
	// Tokens are the configured beat tokens, and Covers[i] the beat ids token
	// i may ping. The zero TokenSet matches nothing, so every ping is refused.
	Tokens httpserver.TokenSet
	Covers [][]string
	// Beats are the configured beat ids, so a covered verdict always names
	// one.
	Beats []string
	// Limiter bounds failed tokens per client and across clients
	// (NewLimiter). Required.
	Limiter *httpserver.FailureLimiter
	// TrustedProxies is the reverse-proxy CIDR set client_ip is resolved
	// against. Empty honors no X-Forwarded-For, the spoof-proof default.
	TrustedProxies []netip.Prefix
}

// New assembles the ping listener's handler: the /beat routes only, so
// /healthz and /metrics answer 404 there. The beat endpoint keeps no
// lifecycle state of its own: watch.Watcher decides admission under the
// mutex that guards the beat mutation.
func New(b Beater, deps *Deps) http.Handler {
	mux := http.NewServeMux()
	// POST is the only method that records: a recording GET is reachable from
	// a link preview or crawler and would re-arm the switch with no heartbeat
	// behind it.
	beats := make(map[string]bool, len(deps.Beats))
	for _, id := range deps.Beats {
		beats[id] = true
	}
	mux.Handle(beatRoutePattern, &gate{
		beater: b, tokens: deps.Tokens, covers: deps.Covers, beats: beats,
		limiter: deps.Limiter, trusted: deps.TrustedProxies,
	})
	mux.HandleFunc("/beat/{id}", writeMethodNotAllowed)
	// The /beat spellings a misconfigured sender URL produces. Without a route
	// they fall to net/http's own 404 -- or, for the bare path, a 307, a
	// SUCCESS status that would exit a `curl -fsS` sender 0.
	mux.HandleFunc("/beat", writeUnknownBeat)
	mux.HandleFunc("/beat/{$}", writeUnknownBeat)
	mux.HandleFunc("/beat/{id}/{rest...}", writeUnknownBeat)

	return httpserver.Chain(mux,
		httpserver.SecurityHeaders(),
		httpserver.NoStore(),
		// WithRecordRouteMetric is the only scrape view of a 404, 405 or 503
		// ping; refused tokens also move knell_auth_failures_total.
		httpserver.Logging(
			httpserver.WithMaxLoggedPath(loggedPathCap),
			// A throttled client gets no Info line per attempt, so a guessing
			// run past its limit cannot flood the log.
			httpserver.WithLogLevel(throttledAtDebug),
			// Empty unless TRUSTED_PROXIES is set: trusting a forwarded header
			// with no proxy in front is how the field becomes spoofable.
			httpserver.WithClientIP(deps.TrustedProxies...),
			httpserver.WithRecordRouteMetric(func(m httpserver.RequestMetric) {
				obs.RecordHTTP(m.Method, m.Path, m.Status, m.Latency)
			}),
		),
		httpserver.Recoverer(),
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
		clean, canonical := httpserver.CanonicalRequestPath(raw)
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
	httpserver.SetAllow(w, http.MethodPost)
	httpserver.WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed",
		"use POST to record a beat")
}

// writeUnknownBeat refuses a ping that names no configured beat. It answers
// every /beat path the route table matches and every non-canonical one
// canonicalBeatPath refuses.
func writeUnknownBeat(w http.ResponseWriter, r *http.Request) {
	httpserver.WriteError(w, r, http.StatusNotFound, "unknown_beat",
		"no beat at this URL. Check the id against BEATS, and check the URL for extra or repeated path segments")
}

// drainBeatBody drains the deliberately ignored ping payload so keep-alive
// connections stay reusable, capped at maxBeatBody. A MaxBytesReader is used
// rather than a bare io.LimitReader because a LimitReader ends the read
// silently: the WARN below is the only channel through which an operator
// learns a sender ships payloads knell refuses to read.
func drainBeatBody(w http.ResponseWriter, r *http.Request) {
	httpserver.LimitBody(w, r, maxBeatBody)
	_, err := io.Copy(io.Discard, r.Body)
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		slog.WarnContext(r.Context(), "beat body exceeded the cap and was not fully read",
			"limit_bytes", maxBeatBody,
			"request_id", httpserver.RequestIDFromContext(r.Context()))
	}
}

// throttledAtDebug logs a 429 at Debug and every other request at Info.
func throttledAtDebug(_ *http.Request, status int) slog.Level {
	if status == http.StatusTooManyRequests {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}

// MetricsHandler is the /metrics body: never cached, since a stale beat_fresh
// would hide the quorum alert, and gzipped for a scraper that asks.
func MetricsHandler() http.Handler {
	return httpserver.Chain(obs.Handler(),
		httpserver.NoStore(),
		httpserver.Gzip(httpserver.WithGzipTypes("text/plain")),
	)
}

// HostPolicyOptions is the ALLOWED_HOSTS policy shape knell ships: exempt for
// knell's own probe (httpserver.WithLoopbackExempt), refusing through Errors.
func HostPolicyOptions() []httpserver.HostAllowlistOption {
	return []httpserver.HostAllowlistOption{
		httpserver.WithLoopbackExempt(true),
		httpserver.WithHostResponder(Errors),
	}
}

// Errors writes every refusal of both listeners. A refused Host is counted by
// reason and answers a message naming ALLOWED_HOSTS.
func Errors(w http.ResponseWriter, r *http.Request, status int, code httpserver.ErrorCode, msg string) {
	if code == hostNotAllowed {
		obs.RecordPreRouteRefusal(obs.RefusalHostNotAllowed)
		msg = "host not allowed. Add it to ALLOWED_HOSTS to serve this hostname"
	}
	httpserver.WriteError(w, r, status, code, msg)
}

const hostNotAllowed httpserver.ErrorCode = "host_not_allowed"
