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
	"syscall"
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
	// The timestamp is asserted WHOLE, date included: a time-only format
	// cannot satisfy this string, so dropping the date fails here rather
	// than shipping a recovery point that could be any day (the notice is
	// late by construction — see historyTimeFormat).
	for _, want := range []string{"node-1", "api", "was missing for 12m0s", "recovered at 2026-07-23 14:07 UTC"} {
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
		// Whole timestamp, date included: see the singular test.
		"last recovered at 2026-07-23 14:07 UTC",
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

func TestLogSafeReducesTransportErrorsWithoutBreakingTheChain(t *testing.T) {
	t.Parallel()

	// logSafe's whole remaining job, and the one that matters: the *url.Error
	// net/http builds around a transport failure is the only error shape that
	// embeds the full request URL, and for a webhook that URL's path IS the
	// credential. Reducing it to its cause is what keeps the URL out of post's
	// returned error and out of httpx.Do's retry lines, and the reduction must
	// not cost the errors.Is chain watch keys shutdown handling on
	// (context.Canceled) and httpx.Do keys transient classification on.
	//
	// There is deliberately no text-matching backstop for an error that
	// FORMATS a URL rendering into its own message: nothing in this package
	// publishes remote text or interpolates d.url, so no such error exists to
	// defend against, and pretending otherwise is what the two earlier leaks
	// in this file's history were made of.
	const secret = "verysecretchaintoken"
	rawURL := "https://discord.example/api/webhooks/1234567890/" + secret

	// Both shapes occur: postAttempt hands back client.Do's bare *url.Error,
	// and post applies logSafe again to what httpx.Do returns, where the same
	// error arrives wrapped in the retry plumbing's own text.
	for name, in := range map[string]error{
		"bare url error":    &url.Error{Op: "Post", URL: rawURL, Err: context.Canceled},
		"wrapped url error": fmt.Errorf("attempt 3 failed: %w", &url.Error{Op: "Post", URL: rawURL, Err: context.Canceled}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := logSafe(in)
			if !strings.Contains(in.Error(), secret) {
				t.Fatalf("setup: input %q does not carry the credential, the assertion would be vacuous", in)
			}
			for _, leak := range []string{secret, "/api/webhooks/", "discord.example"} {
				if strings.Contains(got.Error(), leak) {
					t.Errorf("logSafe kept %q from the request URL: %v", leak, got)
				}
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

	got := logSafe(&url.Error{Op: "Post", URL: rawURL})
	if got == nil {
		t.Fatal("logSafe(*url.Error with a nil cause) = nil, want an error (a nil reports an undelivered notification as delivered)")
	}
	if strings.Contains(got.Error(), secret) {
		t.Errorf("fail-closed error leaks the webhook credential: %v", got)
	}
	// The nil-in/nil-out half of the contract: post and postAttempt call
	// logSafe only on a real failure, so a nil must not become an error.
	if got := logSafe(nil); got != nil {
		t.Errorf("logSafe(nil) = %v, want nil", got)
	}
}

func TestDeliveryLogsNeverLeakWebhookURL(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// The returned-error assertions cannot cover the LOG surface. post
	// applies logSafe a SECOND time to whatever httpx.Do returns, so an
	// attempt-level error that embeds the URL is reduced in the error every
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

func TestStatusBodyEchoingTheRequestPathContributesNothing(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// The exact shape both real leaks in this file's history came from: an edge
	// in front of the webhook answers with an error page that echoes the
	// request URI, and for a Discord webhook that path IS the credential. The
	// body is no longer published in ANY form — not quoted, not filtered, not
	// partially — so the whole class is closed by construction instead of by
	// matching strings inside remote text.
	//
	// Both surfaces are asserted because they fail independently: post's
	// returned error, and httpx.Do's retry/exhausted lines, which log the
	// ATTEMPT error through the type-based LogSafeError alone (post's logSafe
	// runs too late for them). What survives is knell's own account of the
	// body: the status, the byte count, and that the detail was dropped —
	// enough to tell "the webhook answered and rejected us" from "nothing
	// answered", which is what keeps the failure diagnosable.
	const secret = "verysecretbodytoken"
	const secretPath = "/api/webhooks/1234567890/" + secret
	wantBody := "502 Bad Gateway: upstream failed for " + secretPath

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("502 Bad Gateway: upstream failed for " + r.URL.Path))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	d := New(srv.URL+secretPath, "node-1")
	t.Cleanup(d.Close)

	err := d.BeatMissing(context.Background(), "api", time.Hour)
	if err == nil {
		t.Fatal("BeatMissing against a persistent 503 = nil, want error")
	}
	if buf.Len() == 0 {
		t.Fatal("no delivery log lines captured, the leak assertion would be vacuous")
	}
	// The body is not Discord's error object, so the status branch reports it
	// as a measurement. Pinning that keeps the absence assertions below
	// meaningful: a run that failed before the body was read would satisfy
	// them without the branch under test ever running.
	if !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("err = %v, want the HTTP 503 status error (the body branch must have run)", err)
	}
	for _, want := range []string{
		"carried no Discord error code",
		fmt.Sprintf("%d bytes", len(wantBody)),
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v, want it to report %q (a bare status reads as \"the webhook explained nothing\")", err, want)
		}
	}
	// Not one byte of the body, and nothing derived from the request path.
	for _, leak := range []string{secret, "/api/webhook", "1234567890", "Bad Gateway", "upstream failed"} {
		if got := buf.String(); strings.Contains(got, leak) {
			t.Errorf("retry/exhausted logs carry %q from the response body: %s", leak, got)
		}
		if strings.Contains(err.Error(), leak) {
			t.Errorf("delivery error carries %q from the response body: %v", leak, err)
		}
	}
}

func TestOversizedStatusBodyDropsTheDetail(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// A body past the cap is not Discord's error object at all (that object is
	// a few hundred bytes), so there is no code to read and nothing to report
	// but the size. maxErrorBodyBytes+1 is read so the over-cap case is
	// DETECTABLE rather than silently truncated, and the size is reported as
	// the fact it is.
	const secret = "verysecretbodytoken"
	filler := strings.Repeat("x", maxErrorBodyBytes-12)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(filler + r.URL.Path))
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
	if want := fmt.Sprintf("response body over %d bytes, detail dropped", maxErrorBodyBytes); !strings.Contains(err.Error(), want) {
		t.Errorf("err = %v, want it to report %q", err, want)
	}
	// "/api/webhook" is where the cut would land: no part of the body, cut or
	// whole, may reach an error or a log line.
	for _, leak := range []string{secret, "/api/webhook", filler[:32]} {
		if got := buf.String(); strings.Contains(got, leak) {
			t.Errorf("retry/exhausted logs carry %q from an oversized body: %s", leak, got)
		}
		if strings.Contains(err.Error(), leak) {
			t.Errorf("delivery error carries %q from an oversized body: %v", leak, err)
		}
	}
}

func TestPartiallyReadStatusBodyReportsOnlyTheReadFailure(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// io.ReadAll returns the bytes it got ALONGSIDE the read error, and BOTH
	// halves are remote-authored: the partial bytes can be a prefix of an
	// echoed request URI, and the error's own text can embed remote bytes
	// (net/textproto renders a malformed trailer as "malformed MIME header
	// line: <bytes>"). So neither is printed. What is reported is knell's own
	// account: the byte count, and the failure classified structurally — a
	// response that broke mid-body points at the path between here and
	// Discord, not at Discord's verdict, and the status cannot tell them apart.
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
	// in play at all, so pin the error to the status failure and to the
	// structural classification of the read failure.
	for _, want := range []string{
		"HTTP 503",
		"response body unreadable after",
		"the connection closed before the body was complete",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want it to report %q (a partially read body must be reported structurally, not printed)", err, want)
		}
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

func TestStatusBodyReportsDiscordErrorCode(t *testing.T) {
	t.Parallel()

	// Discord names WHY it rejected a webhook POST as a numeric code in its
	// JSON error body, and that number is the only part of the body knell
	// publishes: a number cannot carry a credential, while the object's
	// "message" string and nested "errors" object are authored by the other
	// end. The split the mapped codes must preserve is the one an operator
	// acts on -- a webhook that no longer accepts this knell (recreate it) vs
	// a payload Discord refused (a knell bug no config change fixes) -- and an
	// unmapped code must report the bare number rather than invent a meaning.
	//
	// All cases use a non-transient status so each runs exactly one attempt.
	for name, tc := range map[string]struct {
		status  int
		body    string
		want    []string
		notWant []string
	}{
		"unknown webhook": {
			status:  http.StatusNotFound,
			body:    `{"message": "Unknown Webhook", "code": 10015}`,
			want:    []string{"HTTP 404", "Discord error code 10015", "recreate the webhook"},
			notWant: []string{"Unknown Webhook"},
		},
		"invalid token": {
			status:  http.StatusUnauthorized,
			body:    `{"message": "Invalid Webhook Token", "code": 50027}`,
			want:    []string{"Discord error code 50027", "recreate the webhook"},
			notWant: []string{"Invalid Webhook Token"},
		},
		"empty message is a knell bug": {
			status:  http.StatusBadRequest,
			body:    `{"message": "Cannot send an empty message", "code": 50006}`,
			want:    []string{"HTTP 400", "Discord error code 50006", "knell bug"},
			notWant: []string{"Cannot send an empty message", "recreate the webhook"},
		},
		"invalid request body is a knell bug": {
			status:  http.StatusBadRequest,
			body:    `{"message": "Invalid Form Body", "code": 50035, "errors": {"content": {"_errors": [{"code": "BASE_TYPE_MAX_LENGTH", "message": "Must be 2000 or fewer in length."}]}}}`,
			want:    []string{"Discord error code 50035", "knell bug"},
			notWant: []string{"Invalid Form Body", "BASE_TYPE_MAX_LENGTH", "2000 or fewer"},
		},
		"unmapped code is reported bare": {
			status: http.StatusBadRequest,
			body:   `{"message": "Some Future Rejection", "code": 40333}`,
			want:   []string{"Discord error code 40333"},
			// No invented meaning, and none of Discord's own words: a
			// parenthesis after the number would be knell claiming to know
			// what 40333 means.
			notWant: []string{"Some Future Rejection", "40333 ("},
		},
		"non-JSON body reports only its size": {
			status:  http.StatusBadRequest,
			body:    "Bad Request",
			want:    []string{"HTTP 400", "carried no Discord error code", "11 bytes"},
			notWant: []string{"Bad Request"},
		},
		"JSON without a code reports only its size": {
			status:  http.StatusBadRequest,
			body:    `{"message": "You are being rate limited.", "retry_after": 0.5}`,
			want:    []string{"carried no Discord error code", "62 bytes"},
			notWant: []string{"rate limited", "retry_after"},
		},
		"non-numeric code is no code": {
			status:  http.StatusBadRequest,
			body:    `{"code": "10015"}`,
			want:    []string{"carried no Discord error code"},
			notWant: []string{"Discord error code 10015"},
		},
		"empty body leaves the status alone": {
			status:  http.StatusBadRequest,
			body:    "",
			want:    []string{"HTTP 400"},
			notWant: []string{"detail dropped", "Discord error code"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			d := New(srv.URL+"/api/webhooks/1234567890/plainsegment", "node-1")
			defer d.Close()

			err := d.BeatMissing(context.Background(), "api", time.Hour)
			if err == nil {
				t.Fatalf("BeatMissing against a %d = nil, want error", tc.status)
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("err = %v, want it to report %q", err, want)
				}
			}
			for _, notWant := range tc.notWant {
				if strings.Contains(err.Error(), notWant) {
					t.Errorf("err = %v, must not report %q", err, notWant)
				}
			}
		})
	}
}

func TestReadFailureIsClassifiedStructurally(t *testing.T) {
	t.Parallel()

	// The read error's TEXT is remote-authored input: net/textproto renders a
	// malformed chunked trailer as "malformed MIME header line: <remote
	// bytes>", so a webhook edge could put the echoed request URI (the
	// credential) in it. readFailure therefore classifies with errors.Is
	// against a fixed set and speaks knell's own words; the deadline and
	// connection-reset shapes are here because neither is reachable from a
	// httptest server without making the test a timing experiment.
	const leaked = "/api/webhooks/1234567890/verysecrettrailertoken"
	for name, tc := range map[string]struct {
		in   error
		want string
	}{
		"unexpected EOF": {
			in:   fmt.Errorf("reading body: %w", io.ErrUnexpectedEOF),
			want: "the connection closed before the body was complete",
		},
		"attempt deadline": {
			in:   fmt.Errorf("reading body: %w", context.DeadlineExceeded),
			want: "the attempt deadline expired mid-body",
		},
		"connection reset": {
			in:   fmt.Errorf("reading body: %w", &net.OpError{Op: "read", Err: syscall.ECONNRESET}),
			want: "the connection was reset",
		},
		"unrecognized failure carrying remote bytes": {
			in:   errors.New("malformed MIME header line: " + leaked),
			want: "the read failed",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := readFailure(tc.in)
			if got != tc.want {
				t.Errorf("readFailure(%v) = %q, want %q", tc.in, got, tc.want)
			}
			if strings.Contains(got, leaked) || strings.Contains(got, "MIME") {
				t.Errorf("readFailure(%v) = %q, want knell's own words, never the error's text", tc.in, got)
			}
		})
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
