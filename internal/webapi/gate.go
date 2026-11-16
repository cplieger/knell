package webapi

import (
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cplieger/go/httpserver"
	"github.com/cplieger/knell/internal/obs"
	"github.com/cplieger/knell/internal/watch"
)

// The failure limits. Per client key: 10 failed tokens per 15 minutes, the
// online-guessing bound access/ratelimit uses for an IP. Across every key not
// exempt: 60 per 15 minutes, so a spray over many addresses (a /48 holds
// 65,536 /64 keys) checks at most 5,760 tokens a day. Each token's 16 most
// recent senders are exempt from the global window while their own window is
// empty, so a spray cannot hold them behind 429.
const (
	failuresPerKey    = 10
	failuresPerGlobal = 60
	failureWindow     = 15 * time.Minute
	maxFailureKeys    = 10000
	trustedPerToken   = 16
	minTrustWindow    = time.Hour
)

// NewLimiter returns the failure limiter the ping listener uses. A sender
// stays trusted for trustWindow after its last recorded ping, never less than
// an hour.
func NewLimiter(trustWindow time.Duration) (*httpserver.FailureLimiter, error) {
	return newLimiter(trustWindow, nil)
}

// newLimiter is NewLimiter on clock now, nil meaning time.Now.
func newLimiter(trustWindow time.Duration, now func() time.Time) (*httpserver.FailureLimiter, error) {
	return httpserver.NewFailureLimiter(httpserver.FailureLimits{
		Now:           now,
		PerKey:        failuresPerKey,
		PerKeyWindow:  failureWindow,
		Global:        failuresPerGlobal,
		GlobalWindow:  failureWindow,
		MaxKeys:       maxFailureKeys,
		TrustPerGroup: trustedPerToken,
		TrustWindow:   max(trustWindow, minTrustWindow),
	})
}

// verdict is what one ping's credential proves about its beat. It is decided
// before anything is written, and only the client's throttle state decides
// whether the client may learn it.
type verdict uint8

const (
	// verdictNone: no configured token matches.
	verdictNone verdict = iota
	// verdictCovers: a token matches, and it covers the configured beat.
	verdictCovers
	// verdictNotCovered: a token matches, the beat is configured, and the
	// token does not cover it.
	verdictNotCovered
	// verdictUnknownID: a token matches and the id is not in BEATS.
	verdictUnknownID
)

// failure reports whether v is a credential failure, the only class the
// limiter charges.
func (v verdict) failure() bool { return v == verdictNone || v == verdictNotCovered }

// gate decides each ping: the credential's verdict, then one limiter
// decision, then the answer.
type gate struct {
	beater  Beater
	tokens  httpserver.TokenSet
	covers  [][]string
	beats   map[string]bool
	limiter *httpserver.FailureLimiter
	trusted []netip.Prefix
	// untrustedProxy warns once per process about a forwarded ping from a
	// peer outside TRUSTED_PROXIES.
	untrustedProxy sync.Once
}

// verdictOf matches r's bearer token against every configured token and
// reads the routed id.
func (g *gate) verdictOf(r *http.Request) (v verdict, entry int) {
	presented, ok := strings.CutPrefix(r.Header.Get("Authorization"), bearerPrefix)
	if !ok {
		return verdictNone, -1
	}
	i, ok := g.tokens.Match(presented)
	if !ok {
		return verdictNone, -1
	}
	id := r.PathValue("id")
	switch {
	case !g.beats[id]:
		return verdictUnknownID, i
	case slices.Contains(g.covers[i], id):
		return verdictCovers, i
	}
	return verdictNotCovered, i
}

// warnUntrustedProxy warns once when a ping with a configured token arrives
// forwarded from a peer outside TRUSTED_PROXIES, since every client behind it
// then shares one failure limit. A token is required so a client without one
// can neither raise the warning nor use it up.
func (g *gate) warnUntrustedProxy(res httpserver.ClientResolution, v verdict) {
	if v == verdictNone || !res.UntrustedForwarding || !res.IgnoredPublic {
		return
	}
	g.untrustedProxy.Do(func() {
		slog.Warn("a ping came through a proxy knell does not trust; set TRUSTED_PROXIES to its address, or every client behind it shares one failure limit",
			"peer", res.Peer.String())
	})
}

// ServeHTTP answers POST /beat/{id}. Every verdict asks the limiter once,
// before anything is written. A throttled client gets the same 429 whatever
// it presented, and a ping whose token covers its beat is recorded behind
// it, so throttling never causes a missing notice and a guess stays
// unconfirmed.
func (g *gate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	v, entry := g.verdictOf(r)
	res := httpserver.ResolveClient(r, g.trusted...)
	key := res.IP
	// After the answer, so the log write is never on the path a throttled
	// client times.
	defer g.warnUntrustedProxy(res, v)
	if throttled, wait := g.limiter.Decide(key, v.failure()); throttled {
		g.throttled(w, r, v, wait)
		return
	}
	switch v {
	case verdictNone:
		obs.RecordAuthFailure(obs.AuthInvalidToken)
		// RFC 9110 §11.6.1: a 401 must name at least one challenge.
		w.Header().Set("WWW-Authenticate", "Bearer")
		httpserver.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing or invalid beat token")
	case verdictNotCovered:
		obs.RecordAuthFailure(obs.AuthBeatNotAllowed)
		httpserver.WriteError(w, r, http.StatusForbidden, "beat_not_allowed", "this token does not cover this beat")
	case verdictUnknownID:
		writeUnknownBeat(w, r)
	case verdictCovers:
		if g.record(w, r) {
			g.limiter.MarkGood(strconv.Itoa(entry), key)
		}
	}
}

// throttled answers a throttled ping with one fixed 429 and flushes it before
// anything that depends on the verdict, then records a covering ping. The
// body is never read, for any verdict, so net/http's handling of an unread
// body cannot tell a right token from a wrong one.
func (g *gate) throttled(w http.ResponseWriter, r *http.Request, v verdict, wait time.Duration) {
	body, _ := json.Marshal(httpserver.ErrorResponse{
		Error:     "too many failed beat token attempts",
		Code:      "too_many_auth_failures",
		RequestID: httpserver.RequestIDFromContext(r.Context()),
	})
	body = append(body, '\n')
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	h.Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write(body)
	_ = http.NewResponseController(w).Flush()
	switch v {
	case verdictCovers:
		id := r.PathValue("id")
		if g.beater.Beat(id) == watch.BeatRecorded {
			obs.RecordBeatRecordedWhileThrottled(id)
		}
	case verdictNone, verdictNotCovered:
		obs.RecordAuthFailure(obs.AuthThrottled)
	case verdictUnknownID:
	}
}

// record drains the ping's body, records it, and answers. It reports whether
// the ping was recorded.
func (g *gate) record(w http.ResponseWriter, r *http.Request) bool {
	drainBeatBody(w, r)
	// Beat decides admission under the mutex guarding the state change, so a
	// ping arriving during the drain gets its verdict on the far side of the
	// read.
	switch g.beater.Beat(r.PathValue("id")) {
	case watch.BeatRecorded:
		httpserver.OK(w)
		return true
	case watch.BeatClosed:
		httpserver.WriteError(w, r, http.StatusServiceUnavailable, "shutting_down",
			"knell is shutting down and is no longer accepting beats")
	case watch.BeatUnknown:
		writeUnknownBeat(w, r)
	}
	return false
}
