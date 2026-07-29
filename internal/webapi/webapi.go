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

	"github.com/cplieger/knell/internal/metrics"
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

// Beater records pings. Implemented by watch.Watcher.
type Beater interface {
	// Beat records id when admission is open. recorded is false for an
	// unknown id; accepting is false once the watcher has closed admission
	// for shutdown, in which case nothing was recorded.
	Beat(id string) (recorded, accepting bool)
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
	// /metrics looks at that: browserPageRequest guards the BEAT handler only,
	// so without an allowlist a rebinding page still reads the exposition that
	// enumerates every beat and its freshness.
	Hosts *webhttp.HostPolicy
	// BeatToken optionally gates /beat/{id} (empty = open). Last because a
	// string's length word is not a pointer, which keeps the struct's
	// pointer-scanned prefix at 32 bytes (govet fieldalignment).
	BeatToken string
}

// New assembles the routed and middleware-wrapped root handler.
// deps.BeatToken optionally gates the beat endpoint (empty = open).
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
// last scrape of the freshness exposition during the drain is useful.
func New(appCtx context.Context, b Beater, deps Deps) http.Handler {
	mux := http.NewServeMux()
	// POST is the canonical ping; GET is accepted too so ad-hoc senders
	// (curl without flags, simple healthcheck hooks) can participate.
	beat := beatHandler(appCtx, b, deps.BeatToken)
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
		// webhttp itself, which matters because Chain's FIRST entry is its
		// OUTERMOST wrapper: this line is emitted for every request BEFORE
		// beatHandler's token gate runs, so BEAT_TOKEN bounds neither. The
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
		webhttp.Logging(
			webhttp.ProbeLogLevel("/healthz", "/metrics"),
			webhttp.WithMaxLoggedPath(loggedPathCap),
			webhttp.WithRecordRouteMetric(metrics.RecordHTTP),
		),
		webhttp.Recoverer(),
		webhttp.SecurityHeaders(),
		noStore,
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
// before the mux routes, so none of them can inherit a route label — the guard
// declares nonCanonicalBeatPattern itself instead of letting the whole class
// fall into the "unmatched" bucket beside scanner traffic. /beat/ and
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
			// Declare the route label the mux never got to assign, so this
			// refusal is countable as its own class rather than as unmatched
			// scanner traffic. webhttp reads r.Pattern from the same request
			// this handler received, so the assignment reaches the metric hook.
			r.Pattern = nonCanonicalBeatPattern
			writeUnknownBeat(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// nonCanonicalBeatPattern is the request-counter path label a canonicalBeatPath
// refusal records under. The guard answers BEFORE the mux routes, so nothing has
// populated r.Pattern and webhttp's derivation would bucket the whole class as
// the "unmatched" marker — beside scanner traffic, which is exactly the
// confusion the /beat, /beat/{$} and /beat/{id}/{rest...} routes exist to
// prevent for the other misconfigured-sender spellings. Naming the class costs
// one bounded series and keeps "a sender is pinging a malformed URL" alertable
// from /metrics. Parentheses are not legal in a ServeMux pattern, so the marker
// can never collide with a registered route.
const nonCanonicalBeatPattern = "/beat/(non-canonical)"

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

// noStore marks every response uncacheable. Every route knell serves is
// dynamic state: a ping acknowledgement, a liveness verdict, and the
// freshness exposition that IS the quorum ground truth. None of it may be
// answered from a cache, and a GET ping is a documented sender mode, so the
// header goes on the whole surface rather than one route. It wraps the Host
// policy as well as the mux so Host-refusal responses also carry no-store;
// keep it before deps.Hosts.Middleware in Chain.
func noStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
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

// browserPageRequest reports whether a browser PAGE (or worker) initiated this
// request, which is the confused-deputy shape: it would re-arm the switch with
// no real heartbeat behind it. The rule is deliberately blunt and reads two
// browser-emitted signals — refuse any request carrying a Sec-Fetch-Site header
// whose value is anything other than "none", OR carrying an Origin header at
// all; admit it when neither is present, or when Sec-Fetch-Site reads exactly
// "none" and no Origin came with it.
//
// Those predicates are enough because of who the senders are. Every sender
// knell documents is a machine client (curl -fsS, an Alertmanager
// webhook_configs target, a CI hook) and sends no Fetch Metadata and no Origin
// at all (Origin is a browser-managed forbidden header), while a browser sends
// Sec-Fetch-Site on every request to a potentially trustworthy URL and Origin
// on every page-initiated POST and cors-mode fetch regardless of transport —
// which is what keeps the guard live on knell's documented plain-HTTP LAN
// endpoint, where Fetch Metadata is omitted by specification. "none" is emitted
// only for a user-initiated navigation with NO initiating document (address bar,
// bookmark), which is the one browser shape the README invites ("GET works too,
// for ad-hoc senders"). An iframe never sends "none": an iframe load is a
// navigation, but it has an initiating document, so it reads cross-site or
// same-origin/same-site. So this closes the cross-site sub-resource, the
// cross-site navigation/iframe, AND the same-site sub-resource from a
// compromised sibling origin, with no Sec-Fetch-Mode, Sec-Fetch-Dest or
// user-activation inspection to get wrong.
//
// Either header is trivially spoofable by a non-browser client, and that is
// DELIBERATELY out of scope: this is confused-deputy protection, not
// authentication. A caller that can reach the port directly does not need to
// borrow anyone's browser, and BEAT_TOKEN is the control for that caller.
func browserPageRequest(r *http.Request) bool {
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "none" {
		return true
	}
	// Origin is the signal that survives plain HTTP. Fetch Metadata is
	// appended only for a potentially trustworthy URL, so on knell's
	// documented http:// LAN endpoint Sec-Fetch-Site is absent from a
	// browser request too; Origin has no such gate (Fetch, "append a
	// request Origin header": appended when response tainting is "cors",
	// or whenever the method is neither GET nor HEAD — a referrer policy
	// only rewrites the VALUE to "null"). So a page-initiated fetch POST,
	// a cors-mode fetch GET and a plain form POST all name themselves
	// here, and POST is knell's canonical ping. No documented sender can
	// lose its ping to this: Origin is a browser-managed forbidden header,
	// so curl, an Alertmanager webhook_configs target and a CI hook never
	// set it. A no-cors GET subresource (<img>, an iframe) still carries
	// neither header on plain HTTP — that residual gap is the README's
	// "put knell behind TLS, or set BEAT_TOKEN" condition, not this
	// guard's.
	return r.Header.Get("Origin") != ""
}

// writeBrowserPageRefused refuses a browser-forged ping: 403 in this file's
// standard coded envelope. It names no beat id, exactly like the 404 and 503
// refusals, so it leaks nothing about which ids are configured.
func writeBrowserPageRefused(w http.ResponseWriter, r *http.Request) {
	webhttp.WriteError(w, r, http.StatusForbidden, "browser_page_request",
		"a beat cannot be recorded by a request a browser page initiated")
}

// writeShuttingDown refuses a beat because admission is closed: 503 in this
// file's standard coded envelope. It names no beat id — not even an unknown one
// — so the refusal leaks as little as the 404 path does about which ids are
// configured, and it is the single home of the refusal all three admission
// checks in beatRecorder answer with (the two context checks and the watcher's
// authoritative accepting result).
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

// beatHandler records a ping and answers {"ok":true}, or 404 for an id that
// is not configured. Unknown ids are never recorded or counted: the id feeds
// a metric label, so arbitrary paths must not mint series. A non-empty token
// requires senders to present Authorization: Bearer <token>. Once appCtx is
// cancelled the endpoint accepts nothing more (see New).
func beatHandler(appCtx context.Context, b Beater, token string) http.HandlerFunc {
	record := beatRecorder(appCtx, b)
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

// beatRecorder builds the unauthenticated recording half of the beat endpoint:
// browser refusal, the two lifecycle checks around the body drain, watcher
// admission and the unknown-id 404. beatHandler composes it with the optional
// bearer gate, so this constructor holds the request-ordering decisions and
// beatHandler holds the credential one.
func beatRecorder(appCtx context.Context, b Beater) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// A ping the operator's browser was tricked into sending is not a
		// heartbeat: refuse it before anything else, so it can neither record
		// nor learn which phase the process is in. Only a navigation the user
		// started themselves (no initiating page) is admitted; see
		// browserPageRequest.
		if browserPageRequest(r) {
			writeBrowserPageRefused(w, r)
			return
		}
		// Acceptance is closed for the rest of this process's life: refuse
		// before the body is touched, so a ping arriving during the drain
		// cannot hold a handler goroutine (and with it the drain) open by
		// trickling a payload. A refused request is left undrained exactly
		// like the 401 one below.
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
		// guards the state change (see watch.Watcher.Beat) and its accepting
		// result is the authoritative refusal. This check is still not
		// redundant: watch.Run only closes admission when its ctx.Done arm
		// runs, so for the whole interval between cancellation and that arm
		// Beat still reports accepting=true, and this is the only refusal
		// covering a ping admitted pre-cancel that resumes in it. Do not
		// delete it as duplicated by the accepting result below.
		if appCtx.Err() != nil {
			writeShuttingDown(w, r)
			return
		}
		recorded, accepting := b.Beat(id)
		if !accepting {
			// The watcher closed admission while this handler was in flight:
			// nothing was recorded, so the sender must not be told 200.
			writeShuttingDown(w, r)
			return
		}
		if !recorded {
			writeUnknownBeat(w, r)
			return
		}
		webhttp.Ok(w)
	}
}
