package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cplieger/httpx/v4"
	"github.com/cplieger/knell/internal/watch"
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

func TestBeatOutageHistoryReportsOneEndedOutageInThePastTense(t *testing.T) {
	t.Parallel()

	rec := newWebhookRecorder(http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	recovered := time.Date(2026, 7, 23, 14, 7, 0, 0, time.UTC)
	outages := []watch.Outage{{
		Started:   recovered.Add(-12 * time.Minute),
		Recovered: recovered,
		Silence:   11 * time.Minute,
	}}
	if err := d.BeatOutageHistory(context.Background(), "api", outages); err != nil {
		t.Fatalf("BeatOutageHistory: %v", err)
	}
	content := <-rec.contents
	for _, want := range []string{"node-1", "api", "was missing for 12m0s", "recovered at 14:07 UTC"} {
		if !strings.Contains(content, want) {
			t.Errorf("content %q missing %q", content, want)
		}
	}
	// The whole point of the history notice: an outage that is over must
	// never read like the live alarm for a beat that is down right now.
	for _, forbidden := range []string{"MISSING", "The sender is down", "recovered: pings arriving again"} {
		if strings.Contains(content, forbidden) {
			t.Errorf("content %q reports an ended outage with live-incident wording %q", content, forbidden)
		}
	}
}

func TestBeatOutageHistorySummarizesSeveralEndedOutages(t *testing.T) {
	t.Parallel()

	rec := newWebhookRecorder(http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	base := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	last := time.Date(2026, 7, 23, 14, 7, 0, 0, time.UTC)
	outages := []watch.Outage{
		{Started: base, Recovered: base.Add(12 * time.Minute), Silence: 11 * time.Minute},
		// The longest outage is deliberately in the middle, so a summary
		// that just reports the last or first span fails here.
		{Started: base.Add(20 * time.Minute), Recovered: base.Add(67 * time.Minute), Silence: 11 * time.Minute},
		{Started: last.Add(-15 * time.Minute), Recovered: last, Silence: 11 * time.Minute},
	}
	if err := d.BeatOutageHistory(context.Background(), "api", outages); err != nil {
		t.Fatalf("BeatOutageHistory: %v", err)
	}
	content := <-rec.contents
	for _, want := range []string{
		"node-1", "api",
		"had 3 outages while notifications were failing",
		"longest 47m0s",
		"last recovered at 14:07 UTC",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("content %q missing %q", content, want)
		}
	}
	if strings.Contains(content, "MISSING") {
		t.Errorf("content %q reports ended outages with the live-alarm wording", content)
	}
}

func TestBeatOutageHistoryWithoutOutagesIsNotDelivered(t *testing.T) {
	t.Parallel()

	// watch never sends an empty history notice; if a future caller does,
	// posting a message that reports nothing is worse than refusing it.
	rec := newWebhookRecorder(http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	if err := d.BeatOutageHistory(context.Background(), "api", nil); err == nil {
		t.Fatal("BeatOutageHistory with no outages = nil, want error")
	}
	if got := rec.hits.Load(); got != 0 {
		t.Errorf("webhook hits = %d, want 0 (nothing to report must post nothing)", got)
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

func TestUnfollowedRedirectIsNotDelivery(t *testing.T) {
	t.Parallel()

	// A final 3xx (a redirect the client did not follow, here a bare 300
	// with no Location) means the webhook was NOT accepted: httpx accepts
	// only a 2xx as success, so postAttempt reports an error and the sweep
	// keeps retrying delivery.
	rec := newWebhookRecorder(http.StatusMultipleChoices)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing on 300 = nil, want error (unfollowed 3xx is not delivery)")
	}
	if got := rec.hits.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (non-transient status, no per-attempt retry)", got)
	}
	// The diagnosis must stay factual for THIS case: a bare 300 with no
	// Location involves no hop and no policy refusal, so the message may not
	// blame a cross-origin or method-changing redirect (that shape never
	// reaches this branch — CheckRedirect's error surfaces on the transport
	// path). TestMethodChangingRedirectIsNotDelivery pins the policy itself.
	if !strings.Contains(err.Error(), "nothing was delivered") {
		t.Errorf("err = %v, want it to say nothing was delivered (a bare \"HTTP 300\" reads like a webhook-side rejection)", err)
	}
	for _, wrong := range []string{"cross-origin", "method-changing"} {
		if strings.Contains(err.Error(), wrong) {
			t.Errorf("err = %v, names %q as the cause, but a bare 300 with no Location was refused by no policy", err, wrong)
		}
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

func TestLogSafeRedactsCanonicalWebhookURLForms(t *testing.T) {
	t.Parallel()

	// A webhook path carrying non-ASCII bytes is accepted by net/url, but
	// url.Parse CANONICALIZES it: req.URL.String() renders the credential
	// percent-escaped, so the canonical rendering does not contain the raw
	// configured string. httpx.LogSafeError's reduction is type-based and
	// passes an unrecognized error type through unchanged, so an error that
	// formats req.URL.String() (or just its path) would leak the credential
	// if logSafe scrubbed only the configured string. post's error and the
	// httpx retry log both carry exactly logSafe's output, so asserting on
	// it covers both surfaces.
	const secret = "vérysecrettoken"
	rawURL := "https://discord.example/api/webhooks/1234567890/" + secret
	d := New(rawURL, "node-1")
	defer d.Close()

	u, parseErr := url.Parse(rawURL)
	if parseErr != nil {
		t.Fatalf("url.Parse(%q) = %v", rawURL, parseErr)
	}
	escapedSecret := url.PathEscape(secret)
	if escapedSecret == secret || !strings.Contains(u.String(), escapedSecret) {
		t.Fatalf("setup: canonical URL %q does not percent-escape the credential, nothing to catch", u.String())
	}

	for name, msg := range map[string]string{
		"raw url":        fmt.Sprintf("some future transport error for %s: giving up", rawURL),
		"canonical url":  fmt.Sprintf("some future transport error for %s: giving up", u.String()),
		"canonical path": fmt.Sprintf("some future transport error for POST %s: giving up", u.EscapedPath()),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := d.logSafe(fmt.Errorf("%s: %w", msg, context.Canceled))
			if strings.Contains(got.Error(), secret) {
				t.Errorf("logSafe leaks the raw webhook credential: %v", got)
			}
			if strings.Contains(got.Error(), escapedSecret) {
				t.Errorf("logSafe leaks the percent-encoded webhook credential: %v", got)
			}
			if !strings.Contains(got.Error(), "REDACTED") {
				t.Errorf("logSafe = %q, want the credential replaced by REDACTED", got)
			}
			if !errors.Is(got, context.Canceled) {
				t.Errorf("logSafe broke the errors.Is chain: %v", got)
			}
		})
	}
}

func TestLogSafeFailsClosedWhenReductionYieldsNoError(t *testing.T) {
	t.Parallel()

	// httpx.LogSafeError returns a *url.Error's inner Err verbatim, so a
	// *url.Error carrying a nil cause reduces to nil. postAttempt's return IS
	// httpx.Do's success signal, so a nil there would report an UNDELIVERED
	// notification as delivered: watch would flip the beat to alerted and the
	// missing notice this app exists to send would never be retried. logSafe
	// must fail closed instead, and its substitute message must still be
	// URL-free.
	const secret = "verysecretclosedtoken"
	rawURL := "https://discord.example/api/webhooks/1234567890/" + secret
	d := New(rawURL, "node-1")
	defer d.Close()

	got := d.logSafe(&url.Error{Op: "Post", URL: rawURL})
	if got == nil {
		t.Fatal("logSafe(*url.Error with a nil cause) = nil, want an error (a nil reports an undelivered notification as delivered)")
	}
	if strings.Contains(got.Error(), secret) {
		t.Errorf("fail-closed error leaks the webhook credential: %v", got)
	}
	// The nil-in/nil-out half of the contract: post and postAttempt call
	// logSafe only on a real failure, so a nil must not become an error.
	if got := d.logSafe(nil); got != nil {
		t.Errorf("logSafe(nil) = %v, want nil", got)
	}
}

func TestDeliveryLogsNeverLeakWebhookURL(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// The returned-error assertions cannot cover the LOG surface. post
	// applies logSafe a SECOND time to whatever httpx.Do returns, so an
	// attempt-level error that embeds the URL is scrubbed in the error every
	// other test reads, while httpx.Do's per-attempt retry and exhausted
	// lines (both Debug here, via WithExhaustedLevel) log the RAW attempt
	// error through the type-based LogSafeError only. This pins that surface
	// end to end.
	const secret = "verysecretlogtoken"

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	// Connection refused is transient, so all maxAttempts run and both the
	// per-attempt and exhausted lines are emitted.
	d := New("http://127.0.0.1:9/api/webhooks/1234567890/"+secret, "node-1")
	t.Cleanup(d.Close)

	if err := d.BeatMissing(context.Background(), "api", time.Hour); err == nil {
		t.Fatal("BeatMissing against a refused connection = nil, want error")
	}
	if buf.Len() == 0 {
		t.Fatal("no delivery log lines captured, the leak assertion would be vacuous")
	}
	if got := buf.String(); strings.Contains(got, secret) {
		t.Errorf("delivery logs leak the webhook credential: %s", got)
	}
}

func TestStatusBodyDetailNeverLeaksWebhookURL(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// The other half of the log-surface contract. The refused-connection case
	// above covers only a transport error, which httpx.LogSafeError reduces by
	// TYPE. A transient STATUS carries a bounded prefix of the remote body
	// into the attempt error, and httpx.Do logs that error verbatim through
	// the type-based reduction alone -- post's logSafe runs too late to scrub
	// it. So a webhook fronted by a proxy whose 503 page echoes the request
	// URI would put the credential (which IS the URL path) in the retry and
	// exhausted lines. postAttempt scrubs at source; this pins it.
	const secret = "verysecretbodytoken"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The error-page shape that causes the leak: the body echoes the
		// request path, which for a Discord webhook is the credential.
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("502 Bad Gateway: upstream failed for " + r.URL.Path))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	d := New(srv.URL+"/api/webhooks/1234567890/"+secret, "node-1")
	t.Cleanup(d.Close)

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing against a persistent 503 = nil, want error")
	}
	if buf.Len() == 0 {
		t.Fatal("no delivery log lines captured, the leak assertion would be vacuous")
	}
	if got := buf.String(); strings.Contains(got, secret) {
		t.Errorf("retry/exhausted logs leak the webhook credential: %s", got)
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("delivery error leaks the webhook credential: %v", err)
	}
	// The detail must survive the scrub so delivery failures remain
	// diagnosable; only the credential is removed.
	if !strings.Contains(err.Error(), "Bad Gateway") {
		t.Errorf("delivery error dropped the body detail entirely: %v", err)
	}
}

func TestOversizedStatusBodyIsDroppedRatherThanTruncated(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// Redaction is exact-value replacement, so the size cap must never cut a
	// webhook URL the body echoed: a truncated path matches no candidate and
	// survives the scrub as a credential prefix. Enough filler ahead of the
	// echoed request URI puts the cut exactly there, so an oversized body is
	// dropped whole rather than truncated.
	const secret = "verysecretbodytoken"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(strings.Repeat("x", maxErrorBodyBytes-12) + r.URL.Path))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	d := New(srv.URL+"/api/webhooks/1234567890/"+secret, "node-1")
	t.Cleanup(d.Close)

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing against a persistent 503 = nil, want error")
	}
	if buf.Len() == 0 {
		t.Fatal("no delivery log lines captured, the leak assertion would be vacuous")
	}
	// "/api/webhook" is the truncation's residue: the cut lands inside the
	// credential-bearing path, so no part of it may reach an error or a log.
	for _, leak := range []string{secret, "/api/webhook"} {
		if got := buf.String(); strings.Contains(got, leak) {
			t.Errorf("retry/exhausted logs leak %q from a truncated body: %s", leak, got)
		}
		if strings.Contains(err.Error(), leak) {
			t.Errorf("delivery error leaks %q from a truncated body: %v", leak, err)
		}
	}
}

func TestPartiallyReadStatusBodyIsDropped(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// io.ReadAll returns the bytes it got ALONGSIDE a read error, and those
	// bytes can be a prefix of an echoed webhook URL that no exact-value
	// redaction can remove. A body that did not arrive whole is therefore not
	// usable diagnostic text.
	const secret = "verysecretbodytoken"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, connBuf, hijackErr := w.(http.Hijacker).Hijack()
		if hijackErr != nil {
			t.Errorf("hijack: %v", hijackErr)
			return
		}
		defer conn.Close()
		// Announce more body than is written, then drop the connection: the
		// client's read ends with partial bytes plus an unexpected-EOF error.
		detail := "503 upstream failed for " + r.URL.Path
		_, _ = fmt.Fprintf(connBuf, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: %d\r\n\r\n%s", len(detail)+64, detail)
		_ = connBuf.Flush()
	}))
	defer srv.Close()

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	d := New(srv.URL+"/api/webhooks/1234567890/"+secret, "node-1")
	t.Cleanup(d.Close)

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing against a truncated 503 response = nil, want error")
	}
	if buf.Len() == 0 {
		t.Fatal("no delivery log lines captured, the leak assertion would be vacuous")
	}
	// The absence assertions below only mean something if the STATUS branch
	// ran and dropped its detail. A setup that failed earlier (a transport
	// error before the headers are read) would satisfy them with no body text
	// in play at all, so pin the error to the status failure (which carries
	// the detail-dropped marker, never the partial bytes).
	if !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("err = %v, want the HTTP 503 status error with its detail dropped (the partially read body must be dropped, not the request failed)", err)
	}
	for _, leak := range []string{secret, "/api/webhook", "upstream failed"} {
		if got := buf.String(); strings.Contains(got, leak) {
			t.Errorf("logs carry %q from a partially read body: %s", leak, got)
		}
		if strings.Contains(err.Error(), leak) {
			t.Errorf("delivery error carries %q from a partially read body: %v", leak, err)
		}
	}
}

func TestStatusBodyCarryingOnlyTheCredentialSuffixIsRedacted(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// The leak class the complete-rendering candidates miss: a proxy or webhook
	// edge that reports only the credential-bearing TAIL of the request path,
	// with no scheme, host or leading slash around it. Such a body is short
	// enough to arrive whole under the size cap, so neither drop guard applies
	// and only the fragment candidates (credentialForms) can remove it. Before
	// they existed the suffix passed the scrub untouched, into httpx.Do's retry
	// and exhaustion lines and into the returned delivery error.
	const secret = "verysecretbodytoken"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("upstream failed for " + strings.TrimPrefix(r.URL.Path, "/api/webhooks/")))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	d := New(srv.URL+"/api/webhooks/1234567890/"+secret, "node-1")
	t.Cleanup(d.Close)

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing against a persistent 503 = nil, want error")
	}
	if buf.Len() == 0 {
		t.Fatal("no delivery log lines captured, the leak assertion would be vacuous")
	}
	// The body arrived whole and under the cap, so the detail IS attached;
	// asserting that pins the branch under test (a dropped body would satisfy
	// the absence assertions below without exercising the scrub at all).
	if !strings.Contains(err.Error(), "upstream failed for") {
		t.Fatalf("err = %v, want the attached body detail (the suffix scrub is what must remove the credential, not a drop)", err)
	}
	for _, leak := range []string{secret, "1234567890/" + secret} {
		if got := buf.String(); strings.Contains(got, leak) {
			t.Errorf("retry/exhausted logs leak %q from a suffix-only body: %s", leak, got)
		}
		if strings.Contains(err.Error(), leak) {
			t.Errorf("delivery error leaks %q from a suffix-only body: %v", leak, err)
		}
	}
}

func TestStatusBodyCarryingOnlyAShortCompletePathIsRedacted(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// A relay-style webhook may have a SHORT complete path ("/hook"), and the
	// path IS the credential. A body echoing only that path is under the
	// minCredentialCandidate floor, so the floor must not apply to a complete
	// path rendering: with the floor applied to it, "/hook" was dropped as a
	// candidate and the echoed path passed the scrub untouched, into httpx.Do's
	// retry/exhaustion lines and into the returned delivery error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("upstream rejected " + r.URL.Path))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	d := New(srv.URL+"/hook", "node-1")
	t.Cleanup(d.Close)

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing against a persistent 503 = nil, want error")
	}
	if buf.Len() == 0 {
		t.Fatal("no delivery log lines captured, the leak assertion would be vacuous")
	}
	// The body arrived whole and under the cap, so the detail IS attached:
	// the scrub, not a drop, is what has to remove the path.
	if !strings.Contains(err.Error(), "upstream rejected") {
		t.Fatalf("err = %v, want the attached body detail (the path scrub is what must remove the credential, not a drop)", err)
	}
	if !strings.Contains(err.Error(), "REDACTED") {
		t.Errorf("err = %v, want the echoed webhook path replaced by REDACTED", err)
	}
	if strings.Contains(err.Error(), "/hook") {
		t.Errorf("delivery error leaks the complete webhook path: %v", err)
	}
	if got := buf.String(); strings.Contains(got, "/hook") {
		t.Errorf("retry/exhausted logs leak the complete webhook path: %s", got)
	}
}

func TestAttemptTimeoutIsRetried(t *testing.T) {
	t.Parallel()

	// A per-attempt child deadline is a retryable condition, but
	// httpx.IsTransient rejects anything that unwraps to
	// context.DeadlineExceeded before it consults the Transient
	// interface, so postAttempt translates the timeout into a
	// dedicated no-Unwrap error. Giving that type an Unwrap method
	// makes httpx treat it as terminal again and silently reduces
	// every notification to one attempt; a recovered notice is
	// best-effort-once and would be lost outright.
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			// Outlast the first attempt's (shortened) deadline. The body is
			// drained first and the wait carries a hard bound because the
			// request context is NOT a reliable client-disconnect signal here
			// (with an unread body the server never notices the closed
			// connection), and an unbounded wait hangs the test binary
			// rather than failing it.
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	d := New(srv.URL, "node-1")
	t.Cleanup(d.Close)
	// Shorten only this notifier's per-attempt deadline: the branch under
	// test cares that a CHILD deadline fired while the caller's budget is
	// still live, not how long it took to fire.
	d.attemptTimeout = 100 * time.Millisecond

	if err := d.post(context.Background(), "missing probe", "body"); err != nil {
		t.Fatalf("post() after a retried attempt timeout = %v, want nil", err)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("delivery attempts = %d, want 2 (an attempt timeout must be retried)", got)
	}
	// The production constants must keep the attempt context as the effective
	// bound: a Client.Timeout error does not unwrap to context.DeadlineExceeded,
	// so an inverted ordering would silently disable the translation above.
	if d.client.Timeout <= attemptTimeout {
		t.Errorf("client timeout %s <= per-attempt timeout %s: the transport bound would preempt the attempt context", d.client.Timeout, attemptTimeout)
	}
}

func TestAttemptTimeoutErrorIsRetryableAndOpaque(t *testing.T) {
	t.Parallel()

	// postAttempt translates a fired per-attempt deadline into
	// attemptTimeoutError so httpx.Do retries it. Two properties keep that
	// working: httpx must classify it transient, and it must NOT unwrap to
	// context.DeadlineExceeded -- httpx rejects context errors as terminal
	// BEFORE consulting IsTransient, so adding an Unwrap would silently
	// restore the single-attempt loss this type exists to fix.
	if errors.Is(attemptTimeoutError{}, context.DeadlineExceeded) {
		t.Error("attemptTimeoutError unwraps to context.DeadlineExceeded: httpx.Do would treat it as terminal before consulting IsTransient")
	}
	if !httpx.IsTransient(attemptTimeoutError{}) {
		t.Error("httpx.IsTransient(attemptTimeoutError{}) = false: a timed-out attempt would not be retried")
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

// roundTripperFunc adapts a function to http.RoundTripper so a test can
// answer a delivery attempt without a listener, a socket or a dial.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTransientFailuresExhaustAttempts(t *testing.T) {
	t.Parallel()

	// Every attempt answers 503 (transient): delivery must stop after
	// maxAttempts total attempts and surface an error, never retry
	// unbounded against a hard-down webhook.
	//
	// The 503 comes from a stub transport rather than a listener because the
	// assertion is an exact attempt COUNT, and over a real socket that count
	// is not a property of this package: an attempt that fails BEFORE the
	// handler runs leaves it short while post still returns an error, so the
	// test reads as a retry-budget regression when nothing regressed. Two
	// such failures are reachable on a loaded machine -- a dial error under
	// fd/port pressure (EADDRNOTAVAIL is not in httpx's transient set, so
	// httpx.Do returns after that attempt) and the 10s per-attempt deadline
	// (retried, but the server never saw the request). The stub removes the
	// network and the clock from the oracle; the real-socket retry paths stay
	// covered by TestTransientFailureRetries and the redirect tests.
	var attempts atomic.Int64
	d := New("https://discord.example/api/webhooks/1234567890/plainsegment", "node-1")
	t.Cleanup(d.Close)
	d.client.Transport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		if _, copyErr := io.Copy(io.Discard, r.Body); copyErr != nil {
			t.Errorf("reading request body: %v", copyErr)
		}
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     "503 Service Unavailable",
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    r,
		}, nil
	})

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing with persistent 503 = nil, want error")
	}
	if got := attempts.Load(); got != int64(maxAttempts) {
		t.Errorf("attempts = %d, want %d (maxAttempts is total, including the first)", got, maxAttempts)
	}
}

func TestCanceledDeliveryErrorIsCanceled(t *testing.T) {
	t.Parallel()

	// watch.sweep and watch.sendRecovered key their shutdown handling on
	// errors.Is(err, context.Canceled): an abandoned send must not count
	// as failed (KnellNotifyFailing would page on every shutdown). That
	// contract crosses post's whole wrap chain (client error -> httpx
	// LogSafeError -> %w wrap), so pin it against the real notifier.
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
	}))
	defer srv.Close()
	defer close(release)

	d := New(srv.URL, "node-1")
	defer d.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()

	err := d.BeatMissing(ctx, "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing with canceled context = nil, want error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want errors.Is(err, context.Canceled) to hold through the wrap chain", err)
	}
}

func TestPlainServerErrorIsTerminalPerAttempt(t *testing.T) {
	t.Parallel()

	// httpx's transient set is 502/503/504: a plain 500 is terminal for
	// this delivery call by design; the watch sweep retries the whole
	// notification 15s later. If 500 ever joined the in-call retry set,
	// the second scripted status (204) would make this call succeed.
	rec := newWebhookRecorder(http.StatusInternalServerError, http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing on 500 = nil, want error (sweep-level retry owns this failure)")
	}
	if got := rec.hits.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (500 is not in the transient retry set)", got)
	}
}

func TestMethodChangingRedirectIsNotDelivery(t *testing.T) {
	t.Parallel()

	// A same-host 302 is a hop net/http would follow as a bodyless GET.
	// The client's redirect policy must refuse it, surfacing the 302, which
	// is not a delivery. This is also the only test pinning that New
	// installs a redirect policy at all: with none, net/http would follow
	// the hop, /finish would answer 204, and the send would look delivered
	// even though the payload never reached the webhook.
	var postHits, redirectedHits atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			postHits.Add(1)
		}
		http.Redirect(w, r, "/finish", http.StatusFound)
	})
	mux.HandleFunc("/finish", func(w http.ResponseWriter, _ *http.Request) {
		redirectedHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	d := New(srv.URL+"/start", "node-1")
	defer d.Close()

	if err := d.BeatMissing(context.Background(), "api", time.Hour); err == nil {
		t.Fatal("BeatMissing through a POST-to-GET redirect = nil, want delivery error")
	}
	if got := postHits.Load(); got != 1 {
		t.Errorf("initial POST hits = %d, want 1", got)
	}
	if got := redirectedHits.Load(); got != 0 {
		t.Errorf("redirect target hits = %d, want 0 (the webhook POST must not become GET)", got)
	}
}

func TestCrossHostRedirectIsNotDelivery(t *testing.T) {
	t.Parallel()

	// A 307 preserves the method, so WithPreserveMethod does not refuse it:
	// the same-host rule is the only thing standing between a hijacked
	// Location header and posting the notice to an origin the operator never
	// named. The target listens on a different HOSTNAME, not just a
	// different port, because httpx compares URL.Hostname() (a second
	// 127.0.0.1 listener is the same host by design).
	var targetHits atomic.Int64
	ln, lnErr := net.Listen("tcp", "127.0.0.2:0")
	if lnErr != nil {
		t.Skipf("no second loopback address available: %v", lnErr)
	}
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	target.Listener.Close()
	target.Listener = ln
	target.Start()
	defer target.Close()

	const secretPath = "/api/webhooks/1234567890/verysecrettoken"
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/relay", http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	d := New(origin.URL+secretPath, "node-1")
	defer d.Close()

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing through a cross-host 307 = nil, want a delivery error")
	}
	if got := targetHits.Load(); got != 0 {
		t.Errorf("cross-host redirect target hits = %d, want 0 (the notice must not reach another origin)", got)
	}
	if strings.Contains(err.Error(), "verysecrettoken") {
		t.Errorf("refused-redirect error leaks the webhook credential: %v", err)
	}
}

func TestSameHostRedirectIsFollowedAndDelivers(t *testing.T) {
	t.Parallel()

	// The other half of the redirect contract: the policy must still FOLLOW
	// the webhook's own same-host, method-preserving hop. Nothing else in
	// this package pins that -- dropping WithSameHost turns the policy into
	// "refuse every redirect" (httpx returns a refuse-all policy when no host
	// is allowed), which every other test here passes while a redirecting
	// webhook silently stops receiving notices.
	var finishHits atomic.Int64
	var finishBody atomic.Value
	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/finish", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("/finish", func(w http.ResponseWriter, r *http.Request) {
		finishHits.Add(1)
		if r.Method != http.MethodPost {
			t.Errorf("followed hop method = %s, want POST (body must survive the hop)", r.Method)
		}
		// io.ReadAll, not a single Read: one Read may return a short prefix of
		// the replayed body and the payload assertion below would flake.
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		finishBody.Store(string(body))
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	d := New(srv.URL+"/start", "node-1")
	defer d.Close()

	if err := d.BeatMissing(context.Background(), "api", time.Hour); err != nil {
		t.Fatalf("BeatMissing through a same-host 307 = %v, want delivery", err)
	}
	if got := finishHits.Load(); got != 1 {
		t.Errorf("redirect target hits = %d, want 1 (a same-host hop must be followed)", got)
	}
	if body, _ := finishBody.Load().(string); !strings.Contains(body, "MISSING") {
		t.Errorf("followed hop body = %q, want the notification payload", body)
	}
}
