// Package webapi assembles knell's HTTP surface: the beat ingestion
// endpoint, the health endpoint, and Prometheus metrics, wrapped in the
// standard middleware stack.
package webapi

import (
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
func New(b Beater, token string, routes Routes) http.Handler {
	mux := http.NewServeMux()
	// POST is the canonical ping; GET is accepted too so ad-hoc senders
	// (curl without flags, simple healthcheck hooks) can participate.
	beat := beatHandler(b, token)
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

// beatHandler records a ping and answers {"ok":true}, or 404 for an id that
// is not configured. Unknown ids are never recorded or counted: the id feeds
// a metric label, so arbitrary paths must not mint series. A non-empty token
// requires senders to present Authorization: Bearer <token>.
func beatHandler(b Beater, token string) http.HandlerFunc {
	record := func(w http.ResponseWriter, r *http.Request) {
		// Drain a bounded amount of body so keep-alive connections stay
		// reusable; the payload itself is deliberately ignored.
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, maxBeatBody))
		id := r.PathValue("id")
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
		// be able to hold the handler open by trickling a payload.
		if !verifier.Verify(r.Header.Get("Authorization")) {
			webhttp.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing or invalid beat token")
			return
		}
		record(w, r)
	}
}
