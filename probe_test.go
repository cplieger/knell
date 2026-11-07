package main

import (
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHealthzURLTargetsLoopbackOnTheListenPort(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		":9190":             "http://127.0.0.1:9190/healthz",
		"0.0.0.0:9190":      "http://127.0.0.1:9190/healthz",
		"[::]:9190":         "http://127.0.0.1:9190/healthz",
		"127.0.0.1:8080":    "http://127.0.0.1:8080/healthz",
		"192.0.2.10:9190":   "http://192.0.2.10:9190/healthz",
		"[2001:db8::1]:900": "http://[2001:db8::1]:900/healthz",
	}
	for addr, want := range cases {
		got, err := healthzURL(addr)
		if err != nil || got != want {
			t.Errorf("healthzURL(%q) = %q, %v; want %q", addr, got, err, want)
		}
	}
	if _, err := healthzURL("9190"); err == nil {
		t.Error("healthzURL(\"9190\") = nil error, want the malformed address refused")
	}
}

// TestProbeHealthPassesOnlyAnAnsweringHealthyListener pins the image's
// healthcheck verdict: 200 is healthy; a 503, a closed port, and a listener
// that is bound but has stopped accepting are all unhealthy, the last because
// a bound socket is exactly what the check must not take for a live switch.
func TestProbeHealthPassesOnlyAnAnsweringHealthyListener(t *testing.T) {
	t.Parallel()
	serving := func(status int) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/healthz" {
				http.NotFound(w, r)
				return
			}
			w.WriteHeader(status)
		}))
		t.Cleanup(srv.Close)
		return srv.Listener.Addr().String()
	}
	wedged, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = wedged.Close() })

	cases := map[string]struct {
		addr string
		want int
	}{
		"healthy":                       {serving(http.StatusOK), 0},
		"unhealthy":                     {serving(http.StatusServiceUnavailable), 1},
		"nothing listening":             {unusedLoopbackAddr(t), 1},
		"bound but no longer accepting": {wedged.Addr().String(), 1},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var stderr bytes.Buffer
			start := time.Now()
			if got := probeHealth(&stderr, tc.addr, 500*time.Millisecond); got != tc.want {
				t.Errorf("probeHealth = %d, want %d; stderr: %s", got, tc.want, stderr.String())
			}
			if elapsed := time.Since(start); elapsed > 3*time.Second {
				t.Errorf("probeHealth took %s, want it bounded by its timeout", elapsed)
			}
			if tc.want == 1 && strings.TrimSpace(stderr.String()) == "" {
				t.Error("an unhealthy verdict wrote no reason to stderr")
			}
		})
	}
}
