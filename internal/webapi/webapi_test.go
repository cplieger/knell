package webapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cplieger/knell/internal/metrics"
)

// fakeBeater accepts a fixed id set and records what was recorded.
type fakeBeater struct {
	known map[string]bool
	seen  []string
}

func (f *fakeBeater) Beat(id string) bool {
	if !f.known[id] {
		return false
	}
	f.seen = append(f.seen, id)
	return true
}

func newTestHandler(b *fakeBeater) http.Handler {
	healthz := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return New(b, "", healthz, metrics.Registry.Handler())
}

func TestBeatEndpoint(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantSeen   int
	}{
		{name: "post known", method: http.MethodPost, path: "/beat/api", body: `{"alerts":[]}`, wantStatus: 200, wantSeen: 1},
		{name: "get known", method: http.MethodGet, path: "/beat/api", wantStatus: 200, wantSeen: 1},
		{name: "post unknown", method: http.MethodPost, path: "/beat/ghost", wantStatus: 404},
		{name: "missing id segment", method: http.MethodPost, path: "/beat/", wantStatus: 404},
		{name: "delete rejected", method: http.MethodDelete, path: "/beat/api", wantStatus: 405},
		{name: "nested path rejected", method: http.MethodPost, path: "/beat/api/extra", wantStatus: 404},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &fakeBeater{known: map[string]bool{"api": true}}
			h := newTestHandler(b)
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("%s %s = %d, want %d (body %s)", tt.method, tt.path, rec.Code, tt.wantStatus, rec.Body.String())
			}
			if len(b.seen) != tt.wantSeen {
				t.Errorf("recorded beats = %v, want %d", b.seen, tt.wantSeen)
			}
			if tt.wantStatus == http.StatusOK && !strings.Contains(rec.Body.String(), `"ok":true`) {
				t.Errorf("ok body = %s", rec.Body.String())
			}
			if tt.wantStatus == http.StatusNotFound && tt.path == "/beat/ghost" &&
				!strings.Contains(rec.Body.String(), "unknown_beat") {
				t.Errorf("404 body = %s, want unknown_beat code", rec.Body.String())
			}
		})
	}
}

func TestHealthzRouted(t *testing.T) {
	h := newTestHandler(&fakeBeater{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d", rec.Code)
	}
}

func TestMetricsExposition(t *testing.T) {
	// Touch the asserted metrics so their series exist even when this
	// package's tests run in isolation (labeled metrics emit no output
	// until a first series is recorded).
	metrics.BeatsReceived.Add(0, "webapi-test")
	metrics.BeatFresh.Set(1, "webapi-test")
	metrics.NotificationsSent.Add(0, "missing")

	h := newTestHandler(&fakeBeater{})
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"knell_beats_received_total",
		"knell_beat_fresh",
		"knell_notifications_sent_total",
		"process_start_time_seconds",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("exposition missing %s", want)
		}
	}
}

func TestSecurityHeadersPresent(t *testing.T) {
	h := newTestHandler(&fakeBeater{known: map[string]bool{"api": true}})
	req := httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q", got)
	}
}

func TestBeatTokenGate(t *testing.T) {
	b := &fakeBeater{known: map[string]bool{"api": true}}
	healthz := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := New(b, "s3cret", healthz, metrics.Registry.Handler())

	tests := []struct {
		name       string
		auth       string
		wantStatus int
		wantSeen   int
	}{
		{name: "no header", auth: "", wantStatus: 401},
		{name: "wrong token", auth: "Bearer nope", wantStatus: 401},
		{name: "correct token", auth: "Bearer s3cret", wantStatus: 200, wantSeen: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(b.seen)
			req := httptest.NewRequest(http.MethodPost, "/beat/api", strings.NewReader(""))
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if got := len(b.seen) - before; got != tt.wantSeen {
				t.Errorf("recorded beats = %d, want %d (unauthorized pings must not be recorded)", got, tt.wantSeen)
			}
		})
	}
}
