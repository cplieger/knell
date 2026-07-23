package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// webhookRecorder captures posted payloads and serves scripted status codes.
type webhookRecorder struct {
	statuses []int
	hits     atomic.Int64
	contents chan string
}

func newWebhookRecorder(statuses ...int) *webhookRecorder {
	return &webhookRecorder{statuses: statuses, contents: make(chan string, 16)}
}

func (rec *webhookRecorder) handler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		hit := rec.hits.Add(1)
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("content-type = %q", ct)
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			t.Errorf("reading body: %v", err)
		}
		var payload struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("payload not JSON: %v", err)
		}
		rec.contents <- payload.Content
		status := rec.statuses[min(int(hit)-1, len(rec.statuses)-1)]
		w.WriteHeader(status)
	}
}

func TestBeatMissingDelivers(t *testing.T) {
	t.Parallel()

	rec := newWebhookRecorder(http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	if err := d.BeatMissing(context.Background(), "api", 21*time.Minute+30*time.Second); err != nil {
		t.Fatalf("BeatMissing: %v", err)
	}
	content := <-rec.contents
	for _, want := range []string{"node-1", "api", "MISSING", "21m30s"} {
		if !strings.Contains(content, want) {
			t.Errorf("content %q missing %q", content, want)
		}
	}
}

func TestBeatRecoveredDelivers(t *testing.T) {
	t.Parallel()

	rec := newWebhookRecorder(http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	if err := d.BeatRecovered(context.Background(), "api", 45*time.Minute); err != nil {
		t.Fatalf("BeatRecovered: %v", err)
	}
	content := <-rec.contents
	for _, want := range []string{"node-1", "api", "recovered", "45m"} {
		if !strings.Contains(content, want) {
			t.Errorf("content %q missing %q", content, want)
		}
	}
}

func TestTransientFailureRetries(t *testing.T) {
	t.Parallel()

	// 503 is in httpx's transient set (502/503/504), so it retries within
	// the call. A plain 500 is deliberately terminal here: the watch sweep
	// retries the whole notification 15s later, which covers it.
	rec := newWebhookRecorder(http.StatusServiceUnavailable, http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	if err := d.BeatMissing(context.Background(), "api", time.Hour); err != nil {
		t.Fatalf("BeatMissing after retry: %v", err)
	}
	if got := rec.hits.Load(); got != 2 {
		t.Errorf("attempts = %d, want 2", got)
	}
}

func TestPermanentFailureDoesNotRetry(t *testing.T) {
	t.Parallel()

	rec := newWebhookRecorder(http.StatusNotFound)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing on 404 = nil, want error")
	}
	if got := rec.hits.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (no retry on 4xx)", got)
	}
}

func TestErrorsNeverLeakWebhookURL(t *testing.T) {
	t.Parallel()

	// Connection-refused transport error: the URL (with its secret path)
	// must not appear in the returned error.
	secretPath := "/api/webhooks/1234567890/verysecrettoken"
	d := New("http://127.0.0.1:9"+secretPath, "node-1")
	defer d.Close()

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("expected transport error")
	}
	if strings.Contains(err.Error(), "verysecrettoken") {
		t.Errorf("error leaks webhook secret: %v", err)
	}

	// Status-error path: a 404 body/error must not leak it either.
	rec := newWebhookRecorder(http.StatusNotFound)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()
	d2 := New(srv.URL+secretPath, "node-1")
	defer d2.Close()
	err = d2.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("expected status error")
	}
	if strings.Contains(err.Error(), "verysecrettoken") {
		t.Errorf("status error leaks webhook secret: %v", err)
	}
}

func TestRateLimitRetriesAfterRetryAfter(t *testing.T) {
	t.Parallel()

	// 429 is retried via WithRateLimitRetry, honoring Retry-After. Without
	// that option a 429 would be terminal like the 404 case above.
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	start := time.Now()
	if err := d.BeatMissing(context.Background(), "api", time.Hour); err != nil {
		t.Fatalf("BeatMissing after rate-limit retry: %v", err)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("attempts = %d, want 2", got)
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Errorf("retry waited %s, want >= 1s (Retry-After honored)", elapsed)
	}
}

func TestRequestBuildErrorNeverLeaksWebhookURL(t *testing.T) {
	t.Parallel()

	// A control character makes http.NewRequestWithContext reject the URL;
	// the raw parse error embeds the full URL (with its secret path), so
	// the returned error must be reduced to the cause only.
	d := New("http://127.0.0.1:9/api/webhooks/1234567890/verysecrettoken\x00", "node-1")
	defer d.Close()

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("expected request-build error")
	}
	if strings.Contains(err.Error(), "verysecrettoken") {
		t.Errorf("request-build error leaks webhook secret: %v", err)
	}
}

func TestTransientFailuresExhaustAttempts(t *testing.T) {
	t.Parallel()

	// Every attempt answers 503 (transient): delivery must stop after
	// maxAttempts total attempts and surface an error, never retry
	// unbounded against a hard-down webhook.
	rec := newWebhookRecorder(http.StatusServiceUnavailable)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing with persistent 503 = nil, want error")
	}
	if got := rec.hits.Load(); got != maxAttempts {
		t.Errorf("attempts = %d, want %d (maxAttempts is total, including the first)", got, maxAttempts)
	}
}
