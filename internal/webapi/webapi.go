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

// New assembles the routed and middleware-wrapped root handler.
// token optionally gates the beat endpoint (empty = open); healthz answers
// liveness; metricsHandler serves the Prometheus exposition.
func New(b Beater, token string, healthz, metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	// POST is the canonical ping; GET is accepted too so ad-hoc senders
	// (curl without flags, simple healthcheck hooks) can participate.
	beat := beatHandler(b, token)
	mux.HandleFunc("POST /beat/{id}", beat)
	mux.HandleFunc("GET /beat/{id}", beat)
	mux.Handle("GET /healthz", healthz)
	mux.Handle("GET /metrics", metricsHandler)

	return webhttp.Chain(mux,
		webhttp.Logging(webhttp.WithSkipPaths("/healthz", "/metrics")),
		webhttp.Recoverer(),
		webhttp.SecurityHeaders(),
	)
}

// beatHandler records a ping and answers {"ok":true}, or 404 for an id that
// is not configured. Unknown ids are never recorded or counted: the id feeds
// a metric label, so arbitrary paths must not mint series. A non-empty token
// requires senders to present Authorization: Bearer <token>.
func beatHandler(b Beater, token string) http.HandlerFunc {
	// The verifier is built once, outside the request path, over the full
	// expected header value so acceptance stays exactly
	// "Authorization: Bearer <token>".
	verifier := webhttp.NewStaticTokenVerifier("Bearer " + token)
	return func(w http.ResponseWriter, r *http.Request) {
		// Authorize before touching the body: a rejected sender must not
		// be able to hold the handler open by trickling a payload.
		if token != "" && !verifier.Verify(r.Header.Get("Authorization")) {
			webhttp.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing or invalid beat token")
			return
		}
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
}
