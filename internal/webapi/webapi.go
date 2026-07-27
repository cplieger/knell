// Package webapi assembles knell's HTTP surface: the beat ingestion
// endpoint, the health endpoint, and Prometheus metrics, wrapped in the
// standard middleware stack.
package webapi

import (
	"context"
	"io"
	"net/http"

	"github.com/cplieger/webhttp"
)

// maxBeatBody caps how much of a ping request body is drained. Senders like
// an Alertmanager webhook attach JSON payloads knell ignores; draining keeps
// connections reusable, the cap keeps a hostile body from tying the handler.
const maxBeatBody = 1 << 20

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
	mux.HandleFunc("HEAD /beat/{id}", func(w http.ResponseWriter, r *http.Request) {
		// Two methods are permitted, so Allow is set here rather than by
		// webhttp.RequireMethod (single-method Allow); the 405 body itself is
		// the library's standard coded envelope, like every other error here.
		w.Header().Set("Allow", "GET, POST")
		webhttp.WriteError(w, r, http.StatusMethodNotAllowed, "method_not_allowed",
			"use GET or POST to record a beat")
	})
	mux.Handle("GET /healthz", routes.Healthz)
	mux.Handle("GET /metrics", routes.Metrics)

	return webhttp.Chain(mux,
		// /healthz and /metrics are machine probes, so they ride the
		// fleet-standard ProbeLogLevel rather than a skip list: a HEALTHY
		// probe logs at Debug (out of the shipped stream at the default
		// level, visible under LOG_LEVEL=debug), a 4xx at Warn and a 5xx at
		// Error. Skipping them silenced the failures too — and these are the
		// two endpoints carrying knell's quorum signal, so a scrape that
		// stopped landing or a liveness probe answering 503 has to be
		// greppable. Skip lists stay for streams, of which knell has none.
		webhttp.Logging(webhttp.ProbeLogLevel("/healthz", "/metrics")),
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
		// Drain a bounded amount of body so keep-alive connections stay
		// reusable; the payload itself is deliberately ignored.
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, maxBeatBody))
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
