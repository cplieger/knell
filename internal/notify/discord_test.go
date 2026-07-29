package notify

import (
	"context"
	"crypto/tls"
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
	"github.com/cplieger/slogx/capture"
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

// captureDeliveryLogs captures slog.Default()'s records for the duration of
// the test and restores the previous default afterwards, so a test can assert
// on httpx.Do's per-attempt and exhausted lines. Records are asserted at the
// record level (message and raw attribute values) rather than through a
// handler's rendered text, so an assertion does not depend on a rendering step
// production may not use. slog.Default() is process-global, so a caller must
// NOT use t.Parallel.
func captureDeliveryLogs(t *testing.T) *capture.Recorder {
	t.Helper()
	return capture.Default(t)
}

// requireLogged fails the test when httpx.Do's delivery lines are absent: every
// absence assertion against the log surface is vacuous if the records it scans
// were never emitted.
func requireLogged(t *testing.T, rec *capture.Recorder) {
	t.Helper()
	// Both of httpx.Do's lines, not just "something logged": the per-attempt
	// retry line and the exhausted line are the surface these leak assertions
	// scan, so a run that emitted neither would satisfy them vacuously.
	if rec.Count("failed, retrying") == 0 || rec.Count("retries exhausted") == 0 {
		t.Fatalf("delivery logs missing httpx's retry/exhausted lines, the leak assertion would be vacuous; records = %v", rec.Records())
	}
}

// liveSilence builds the value a live notice carries for a beat that has been
// unseen for d: the two instants watch measures the silence between. It exists
// so these tests construct the new watch.Transition parameter; the figure the
// notice renders is still exactly d (Transition.DownFor), so every expected
// string below is unchanged.
func liveSilence(d time.Duration) watch.Transition {
	started := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	return watch.Transition{Started: started, Observed: started.Add(d)}
}

func TestBeatMissingDelivers(t *testing.T) {
	t.Parallel()

	rec := newWebhookRecorder(http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	if err := d.BeatMissing(context.Background(), "api", liveSilence(21*time.Minute+30*time.Second)); err != nil {
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

	if err := d.BeatRecovered(context.Background(), "api", liveSilence(45*time.Minute)); err != nil {
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
		// The case the history path was built for: a sweep raised the alert
		// and the webhook was unreachable, so the notice arrives after the
		// outage it reports.
		LateReason: watch.LateUndelivered,
	}}
	if err := d.BeatOutageHistory(context.Background(), "api", outages); err != nil {
		t.Fatalf("BeatOutageHistory: %v", err)
	}
	content := <-rec.contents
	// The timestamp is asserted WHOLE, date included: a time-only format
	// cannot satisfy this string, so dropping the date fails here rather
	// than shipping a recovery point that could be any day (the notice is
	// late by construction — see historyTimeFormat).
	for _, want := range []string{
		"node-1", "api", "was missing for 12m0s", "recovered at 2026-07-23 14:07 UTC",
		"delivery was delayed", "check the webhook",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("content %q missing %q", content, want)
		}
	}
	// The whole point of the history notice: an outage that is over must
	// never read like the live alarm for a beat that is down right now.
	for _, forbidden := range []string{"MISSING", "check the sender", "recovered: pings arriving again"} {
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
		{Started: base, Recovered: base.Add(12 * time.Minute)},
		// The longest outage is deliberately in the middle, so a summary
		// that just reports the last or first span fails here.
		{Started: base.Add(20 * time.Minute), Recovered: base.Add(67 * time.Minute)},
		{Started: last.Add(-15 * time.Minute), Recovered: last},
	}
	if err := d.BeatOutageHistory(context.Background(), "api", outages); err != nil {
		t.Fatalf("BeatOutageHistory: %v", err)
	}
	content := <-rec.contents
	for _, want := range []string{
		"node-1", "api",
		"had 3 outages",
		"longest 47m0s",
		// Whole timestamp, date included: see the singular test.
		"last recovered at 2026-07-23 14:07 UTC",
		// Every entry above carries the zero LateReason (LateUndelivered),
		// which is also what a producer that names no reason gets.
		"Delivery was delayed for every outage", "check the webhook",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("content %q missing %q", content, want)
		}
	}
	if strings.Contains(content, "MISSING") {
		t.Errorf("content %q reports ended outages with the live-alarm wording", content)
	}
}

// TestBeatOutageHistoryStatesTheTrueReasonForALateNotice pins the mapping from
// watch.LateReason to the clause an operator acts on, for every shape of batch.
// Each case asserts BOTH the wording that belongs to its reason and the wording
// that belongs to the OTHER one: a want-only assertion still passes if the two
// clauses are swapped (both mention the outage and a duration), and swapping
// them is precisely the bug this fixes — telling an operator to inspect a
// webhook that delivered this message on its first attempt.
func TestBeatOutageHistoryStatesTheTrueReasonForALateNotice(t *testing.T) {
	t.Parallel()

	recovered := time.Date(2026, 7, 23, 14, 7, 0, 0, time.UTC)
	// outage builds one ended outage of span with the given late reason; the
	// spans differ per entry only so the summary's "longest" is unambiguous.
	outage := func(span time.Duration, reason watch.LateReason) watch.Outage {
		return watch.Outage{
			Started:    recovered.Add(-span),
			Recovered:  recovered,
			LateReason: reason,
		}
	}
	const (
		webhookClause = "check the webhook"
		delayedOne    = "This notice is late because delivery was delayed"
		delayedAll    = "Delivery was delayed for every outage"
		selfResolved  = "ended before a sweep detected it"
		deliveryFine  = "nothing was wrong with delivery"
		// The sweep's send budget can defer a record before any send is
		// started, so no notice may claim a delivery ATTEMPT happened.
		noAttemptClaim = "attempt"
	)
	cases := map[string]struct {
		outages []watch.Outage
		want    []string
		forbid  []string
	}{
		"one outage that ended before any sweep saw it": {
			outages: []watch.Outage{outage(12*time.Minute, watch.LateEndedBeforeDetection)},
			want:    []string{"was missing for 12m0s", selfResolved, deliveryFine},
			forbid:  []string{webhookClause, delayedOne, "notifications were failing"},
		},
		"one outage whose alert the webhook never took": {
			outages: []watch.Outage{outage(12*time.Minute, watch.LateUndelivered)},
			want:    []string{"was missing for 12m0s", delayedOne, webhookClause},
			// LateUndelivered is also reached without any send having been
			// tried (the sweep's send budget defers a record before its turn),
			// so the clause must point at delivery without claiming an
			// attempt was made.
			forbid: []string{selfResolved, deliveryFine, noAttemptClaim},
		},
		"a batch of outages that all ended before a sweep saw them": {
			outages: []watch.Outage{
				outage(12*time.Minute, watch.LateEndedBeforeDetection),
				outage(47*time.Minute, watch.LateEndedBeforeDetection),
			},
			want:   []string{"had 2 outages", "longest 47m0s", selfResolved, deliveryFine},
			forbid: []string{webhookClause, delayedAll},
		},
		"a batch of outages whose alerts were all undelivered": {
			outages: []watch.Outage{
				outage(12*time.Minute, watch.LateUndelivered),
				outage(47*time.Minute, watch.LateUndelivered),
			},
			want:   []string{"had 2 outages", "longest 47m0s", delayedAll, webhookClause},
			forbid: []string{selfResolved, deliveryFine, noAttemptClaim},
		},
		// The batch a real webhook outage produces on a flapping beat: some
		// alerts held back, some outages over before a sweep could see them.
		// Both counts must be stated; picking one reason for the batch says
		// something false about the other outages.
		"a batch that mixes both reasons": {
			outages: []watch.Outage{
				outage(12*time.Minute, watch.LateUndelivered),
				outage(47*time.Minute, watch.LateUndelivered),
				outage(9*time.Minute, watch.LateEndedBeforeDetection),
			},
			want: []string{
				"had 3 outages", "longest 47m0s",
				"Delivery was delayed for 2 (check the webhook)", "1 ended before a sweep detected it", webhookClause,
			},
			forbid: []string{
				// Neither single-reason clause may stand in for a mixed batch.
				delayedAll, "Each " + selfResolved, noAttemptClaim,
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rec := newWebhookRecorder(http.StatusNoContent)
			srv := httptest.NewServer(rec.handler(t))
			defer srv.Close()

			d := New(srv.URL, "node-1")
			defer d.Close()

			if err := d.BeatOutageHistory(context.Background(), "api", tc.outages); err != nil {
				t.Fatalf("BeatOutageHistory: %v", err)
			}
			content := <-rec.contents
			for _, want := range tc.want {
				if !strings.Contains(content, want) {
					t.Errorf("content %q missing %q", content, want)
				}
			}
			for _, forbidden := range tc.forbid {
				if strings.Contains(content, forbidden) {
					t.Errorf("content %q states %q, which belongs to the other late reason", content, forbidden)
				}
			}
			// Past tense and the recovery point are the same for both reasons;
			// only the explanation differs.
			if !strings.Contains(content, "recovered at 2026-07-23 14:07 UTC") {
				t.Errorf("content %q does not report the recovery point", content)
			}
		})
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

	if err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour)); err != nil {
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

	err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
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

	err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
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

	err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
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
	err = d2.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
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
			// safeTransportError is what postAttempt actually returns, and it
			// replaces the message entirely -- so the chain it exposes through
			// Unwrap is the ONLY thing left for watch's shutdown exemption
			// (errors.Is(err, context.Canceled), which decides whether a
			// failed send is logged as an error or as a cancelled sweep) and
			// for httpx's transient classification to read.
			safe := safeTransportError(in)
			if !errors.Is(safe, context.Canceled) {
				t.Errorf("safeTransportError broke the errors.Is chain: %v", safe)
			}
			for _, leak := range []string{secret, "/api/webhooks/", "discord.example"} {
				if strings.Contains(safe.Error(), leak) {
					t.Errorf("safeTransportError kept %q from the request URL: %v", leak, safe)
				}
			}
			if safeTransportError(nil) != nil {
				t.Error("safeTransportError(nil) != nil, want nil (a nil must not become a delivery failure)")
			}
		})
	}
}

// TestNestedURLErrorsAreFullyReducedBeforeClassification pins the loop in
// safeTransportError that reduces until no *url.Error is left ANYWHERE in the
// chain, not just at the top. httpx.LogSafeError SEARCHES a chain with
// errors.As and RETURNS what it finds, so one *url.Error still nested under
// the cause lets it unwrap PAST transportError and render that error's own
// text instead of knell's phrase — in post's returned error and in httpx.Do's
// per-attempt lines. net/http produces exactly this shape when a redirected
// request fails: the *url.Error for the hop carries the *url.Error of the
// inner request, whose cause is written from the response's Location header,
// so the text that surfaces is remote-authored and can echo the webhook path
// (which IS the credential).
func TestNestedURLErrorsAreFullyReducedBeforeClassification(t *testing.T) {
	t.Parallel()

	const secret = "verysecretnestedtoken"
	remote := errors.New(`failed to parse Location header "/api/webhooks/1234567890/` + secret + `%zz"`)
	nested := &url.Error{
		Op:  "Post",
		URL: "https://discord.example/api/webhooks/1234567890/" + secret,
		Err: &url.Error{
			Op:  "Get",
			URL: "https://redirect.example/api/webhooks/1234567890/" + secret,
			Err: remote,
		},
	}
	if !strings.Contains(nested.Error(), secret) {
		t.Fatal("setup: the input does not carry the credential, the assertion would be vacuous")
	}

	got := safeTransportError(nested)
	if got.Error() != "webhook transport failed" {
		t.Errorf("safeTransportError(nested *url.Error).Error() = %q, want knell's own phrase", got.Error())
	}
	// The three surfaces the reduction has to hold for: the error itself, the
	// reduction httpx.Do logs every attempt through, and post's own logSafe of
	// what httpx.Do returns. They fail independently.
	for surface, rendered := range map[string]error{
		"transport error":                got,
		"attempt error as httpx logs it": httpx.LogSafeError(got),
		"post's reduction":               logSafe(got),
	} {
		if rendered == nil {
			t.Errorf("%s = nil, want an error (a nil reports an undelivered notification as delivered)", surface)
			continue
		}
		for _, leak := range []string{secret, "/api/webhooks/", "discord.example", "redirect.example", "Location header"} {
			if strings.Contains(rendered.Error(), leak) {
				t.Errorf("%s kept %q from a nested *url.Error: %v", surface, leak, rendered)
			}
		}
	}
	// The reduction must not cost the chain: watch keys its shutdown exemption
	// and httpx keys transient classification on errors.Is/As.
	if !errors.Is(got, remote) {
		t.Errorf("safeTransportError dropped the cause from the chain: %v", got)
	}
}

func TestLogSafeNeverReducesAFailureToNil(t *testing.T) {
	t.Parallel()

	// A *url.Error carrying a nil cause must not reduce to nil: postAttempt's
	// return IS httpx.Do's success signal, so a nil there would report an
	// UNDELIVERED notification as delivered — watch would flip the beat to
	// alerted and the missing notice this app exists to send would never be
	// retried. httpx v4.2.0 guarantees this at the source (LogSafeError
	// substitutes a contentless, URL-free stand-in), which is why knell no
	// longer carries its own fail-closed guard; this pins the contract knell
	// depends on, so a regression in the library fails here rather than
	// silently disarming the switch.
	const secret = "verysecretclosedtoken"
	rawURL := "https://discord.example/api/webhooks/1234567890/" + secret

	got := logSafe(&url.Error{Op: "Post", URL: rawURL})
	if got == nil {
		t.Fatal("logSafe(*url.Error with a nil cause) = nil, want an error (a nil reports an undelivered notification as delivered)")
	}
	if strings.Contains(got.Error(), secret) {
		t.Errorf("the reduced error leaks the webhook credential: %v", got)
	}
	// The nil-in/nil-out half of the contract: post and postAttempt call
	// logSafe only on a real failure, so a nil must not become an error.
	if got := logSafe(nil); got != nil {
		t.Errorf("logSafe(nil) = %v, want nil", got)
	}
}

func TestRedirectDerivedTransportErrorsCarryNoRemoteText(t *testing.T) {
	t.Parallel()

	// The third channel remote text can enter by, and the one no body-side
	// defense covers: a REDIRECT. net/http writes two of its transport causes
	// from the response's Location header -- a malformed one as `failed to
	// parse Location header "<the header>"`, and httpx's policy refusal as
	// `refusing redirect to <the header's host>` -- and stripping the
	// *url.Error wrapper leaves exactly that text. An endpoint that answers a
	// webhook POST with a redirect echoing the request URI would therefore put
	// the credential (for a webhook the URL path IS the credential) into the
	// returned error and into httpx.Do's attempt lines, without ever sending a
	// response body. safeTransportError closes it by classifying the cause
	// instead of rendering it.
	//
	// Both error surfaces are asserted: post's returned error, and postAttempt's
	// return, which is verbatim what httpx.Do logs for each attempt (through
	// the type-based LogSafeError, which can only shrink it) -- so the log
	// surface is covered by asserting on the attempt error and on that
	// reduction of it.
	const secret = "verysecretlocationtoken"
	for name, tc := range map[string]struct {
		location string
		leaks    []string
		status   int
	}{
		// Location parsing fails before any policy runs, so this shape needs
		// only a redirect net/http would follow.
		"malformed location echoing the request path": {
			status:   http.StatusFound,
			location: "/api/webhooks/1234567890/" + secret + "%zz",
			leaks:    []string{secret, "/api/webhooks/", "%zz"},
		},
		// A method-PRESERVING cross-host hop is the one the policy refuses
		// with an error (a method-changing one surfaces its 3xx instead, which
		// TestMethodChangingRedirectIsNotDelivery covers).
		"cross-host location whose hostname carries the credential": {
			status:   http.StatusTemporaryRedirect,
			location: "https://" + secret + ".redirect.example/hooks/1",
			leaks:    []string{secret, "redirect.example"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", tc.location)
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(srv.Close)

			d := New(srv.URL+"/api/webhooks/1234567890/"+secret, "node-1")
			t.Cleanup(d.Close)

			_, attemptErr := d.postAttempt(context.Background(), []byte(`{"content":"probe"}`))
			postErr := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
			if attemptErr == nil || postErr == nil {
				t.Fatalf("postAttempt = %v, BeatMissing = %v against a hostile redirect, want errors on both (nothing was delivered)", attemptErr, postErr)
			}
			// The attempt failed on the transport path, so knell's own phrase
			// for it is what the operator reads. Pinning it keeps the absence
			// assertions below meaningful: a run that failed for some other
			// reason would satisfy them without the branch under test running.
			if !strings.Contains(attemptErr.Error(), "webhook transport failed") {
				t.Errorf("attempt error = %v, want knell's own transport phrase", attemptErr)
			}
			for _, leak := range tc.leaks {
				for surface, got := range map[string]error{
					"attempt error":                  attemptErr,
					"attempt error as httpx logs it": httpx.LogSafeError(attemptErr),
					"returned error":                 postErr,
				} {
					if strings.Contains(got.Error(), leak) {
						t.Errorf("%s kept %q from the Location header: %v", surface, leak, got)
					}
				}
			}
		})
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

	rec := captureDeliveryLogs(t)

	// Connection refused is transient, so all maxAttempts run and both the
	// per-attempt and exhausted lines are emitted.
	d := New("http://127.0.0.1:9/api/webhooks/1234567890/"+secret, "node-1")
	t.Cleanup(d.Close)

	if err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour)); err == nil {
		t.Fatal("BeatMissing against a refused connection = nil, want error")
	}
	requireLogged(t, rec)
	if rec.Contains(secret) || rec.AttrContains("", "", secret) {
		t.Errorf("delivery logs leak the webhook credential; records = %v", rec.Records())
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

	rec := captureDeliveryLogs(t)

	d := New(srv.URL+secretPath, "node-1")
	t.Cleanup(d.Close)

	err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
	if err == nil {
		t.Fatal("BeatMissing against a persistent 503 = nil, want error")
	}
	requireLogged(t, rec)
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
		if rec.Contains(leak) || rec.AttrContains("", "", leak) {
			t.Errorf("retry/exhausted logs carry %q from the response body; records = %v", leak, rec.Records())
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

	rec := captureDeliveryLogs(t)

	d := New(srv.URL+"/api/webhooks/1234567890/"+secret, "node-1")
	t.Cleanup(d.Close)

	err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
	if err == nil {
		t.Fatal("BeatMissing against a persistent 503 = nil, want error")
	}
	requireLogged(t, rec)
	if want := fmt.Sprintf("response body over %d bytes, detail dropped", maxErrorBodyBytes); !strings.Contains(err.Error(), want) {
		t.Errorf("err = %v, want it to report %q", err, want)
	}
	// "/api/webhook" is where the cut would land: no part of the body, cut or
	// whole, may reach an error or a log line.
	for _, leak := range []string{secret, "/api/webhook", filler[:32]} {
		if rec.Contains(leak) || rec.AttrContains("", "", leak) {
			t.Errorf("retry/exhausted logs carry %q from an oversized body; records = %v", leak, rec.Records())
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

	rec := captureDeliveryLogs(t)

	d := New(srv.URL+"/api/webhooks/1234567890/"+secret, "node-1")
	t.Cleanup(d.Close)

	err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
	if err == nil {
		t.Fatal("BeatMissing against a truncated 503 response = nil, want error")
	}
	requireLogged(t, rec)
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
		if rec.Contains(leak) || rec.AttrContains("", "", leak) {
			t.Errorf("logs carry %q from a partially read body; records = %v", leak, rec.Records())
		}
		if strings.Contains(err.Error(), leak) {
			t.Errorf("delivery error carries %q from a partially read body: %v", leak, err)
		}
	}
}

func TestSuccessfulDeliveryDrainsTheBodyWithoutLoggingItsReadError(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// The third path remote-authored text can reach the logs by, and the only
	// one no status-side guard covers: the drain of a SUCCESSFUL response. A
	// 2xx reaches the drain with no other error in play, so statusDetail
	// never sees it.
	//
	// The guard now lives in the LIBRARY, not in knell: httpx.DrainClose logs
	// a failed drain at Debug through the PACKAGE-level slog.Default() (no
	// httpx option can redirect or silence it), and as of v4.2.1 it logs a
	// bare line carrying no error value. Before that it attached the error,
	// whose text is written by the other end — net/http renders a malformed
	// chunked trailer as `malformed MIME header line: <remote bytes>`, the
	// same class readFailure classifies structurally on the status path — so
	// knell carried a local drain helper to keep that error away from a
	// logger, and this test pinned the helper.
	//
	// knell keeps the assertion anyway, now against the real httpx.DrainClose:
	// the invariant it defends is knell's (no remote-authored text in knell's
	// own logs) even though the code enforcing it no longer is, and an httpx
	// regression would silently reopen the leak in knell's log stream with
	// nothing else in this package left to catch it. Pinned at the Debug level
	// an operator raises precisely while diagnosing failing deliveries.
	const secret = "verysecrettrailertoken"
	const secretPath = "/api/webhooks/1234567890/" + secret
	malformedTrailer := "this-is-not-a-header" + secretPath

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, connBuf, hijackErr := w.(http.Hijacker).Hijack()
		if hijackErr != nil {
			t.Errorf("hijack: %v", hijackErr)
			return
		}
		defer conn.Close()
		// A well-formed 200 with chunked framing whose trailer section
		// carries a header line with no colon: the status says delivered, and
		// the read fails only once the drain reaches the trailer.
		_, _ = fmt.Fprintf(connBuf,
			"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nTrailer: X-Knell-Check\r\n\r\n2\r\n{}\r\n0\r\n%s\r\n\r\n",
			malformedTrailer)
		_ = connBuf.Flush()
	}))
	defer srv.Close()

	// Non-vacuity, measured rather than assumed: reading this exact response
	// really does produce an error whose text carries the credential, so an
	// implementation that logged it WOULD leak. Without this, the absence
	// assertions below would pass just as well against a body that reads
	// cleanly.
	probe, probeErr := http.Get(srv.URL + secretPath)
	if probeErr != nil {
		t.Fatalf("control request: %v", probeErr)
	}
	_, readErr := io.ReadAll(probe.Body)
	probe.Body.Close()
	if readErr == nil {
		t.Fatal("setup: the control read of the malformed trailer succeeded, the leak assertions would be vacuous")
	}
	for _, want := range []string{secret, "MIME"} {
		if !strings.Contains(readErr.Error(), want) {
			t.Fatalf("setup: control read error %q carries no %q, the leak assertions would be vacuous", readErr, want)
		}
	}

	// Captured after the control probe above, so only the delivery below can
	// contribute lines. requireLogged is deliberately NOT used: a delivery
	// that succeeds on the first attempt logs nothing, and this test's
	// non-vacuity comes from the control read of the malformed trailer
	// instead.
	rec := captureDeliveryLogs(t)

	d := New(srv.URL+secretPath, "node-1")
	t.Cleanup(d.Close)

	// A 2xx is a delivery, malformed trailer or not: the drain exists for
	// connection reuse alone, and forfeiting it must not turn a delivered
	// notice into a retry (watch flips alerted only on a delivered send).
	if err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour)); err != nil {
		t.Fatalf("BeatMissing against a 200 with a malformed trailer = %v, want nil (a 2xx is delivered)", err)
	}
	// The remote-authored text must be absent. "drain" is deliberately NOT in
	// this list any more: httpx v4.2.1 logs a bare `msg="failed to drain
	// response body"` line with no attributes, and that line is the library's
	// own fixed wording, not remote input. The token belonged here only while
	// knell's local helper logged NOTHING at all, where its appearance meant
	// the library's drain had run instead; asserting it now would forbid the
	// very design that closed the leak. "MIME" is the load-bearing proxy that
	// replaces it — it is net/textproto's own wording for the read error, so
	// any regression that renders that error's text again trips on it, whether
	// or not the remote bytes happen to look like a credential.
	for _, leak := range []string{secret, "/api/webhook", "MIME", "this-is-not-a-header"} {
		if rec.Contains(leak) || rec.AttrContains("", "", leak) {
			t.Errorf("delivery logs carry %q from the body drain's read error; records = %v", leak, rec.Records())
		}
	}
	// The invariant stated directly rather than by proxy: the drain's error
	// VALUE reaches no log attribute. Pre-v4.2.1 httpx rendered it as
	// error="malformed MIME header ..."; the assertion is scoped to the drain
	// record itself, so it states the invariant instead of matching a
	// handler-specific `error=` rendering that a JSON handler would not emit.
	if value, found := rec.AttrValue("failed to drain response body", "error"); found {
		t.Errorf("drain line carries an error attribute from the body read: %q", value)
	}
}

func TestSuccessfulResponsesAreDrainedForConnectionReuse(t *testing.T) {
	// NOT t.Parallel(): the oracle is idle-connection reuse, and
	// httpx.NewClient's client rides the process-wide http.DefaultTransport
	// whose idle pool every other test in this package shares. Run
	// concurrently, their churn evicts this notifier's idle connection between
	// the two notices and the second one dials again -- a failure about the
	// suite's parallelism, not about knell's drain (measured: passes alone,
	// fails in the full package with connections = 2). Serial execution makes
	// the reuse observation belong to this test alone.
	// The drain's other half, and the one no absence assertion can reach: that
	// knell's delivery path actually DRAINS a successful response rather than
	// just closing it. Read VOLUME is not observable from a handler, so the
	// oracle is the consequence -- an undrained body makes net/http discard the
	// connection, so two notices open two connections instead of reusing one,
	// and every notice pays a fresh TLS handshake. Driving the public
	// BeatMissing keeps httpx.DrainClose's own bound and read-then-close order
	// the library's business to test, and pins only what knell depends on.
	var connections atomic.Int64
	var requests atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, 1024))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)

	d := New(srv.URL, "node-1")
	t.Cleanup(d.Close)
	live := liveSilence(time.Hour)

	if err := d.BeatMissing(context.Background(), "api", live); err != nil {
		t.Fatalf("first BeatMissing: %v", err)
	}
	if err := d.BeatMissing(context.Background(), "db", live); err != nil {
		t.Fatalf("second BeatMissing: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("requests = %d, want 2", got)
	}
	if got := connections.Load(); got != 1 {
		t.Errorf("connections = %d, want 1 (a successful response body must be drained so the connection is reusable)", got)
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
	// a payload Discord refused -- and an unmapped code must report the bare
	// number rather than invent a meaning. 50035 is Discord's content-length
	// rejection, which config's startup validation makes unreachable, so its
	// wording must report a knell bug and name NO operator setting: pointing
	// at an input startup already accepted would send the operator to inspect
	// the one value the app proved innocent. The notWant on "NODE_NAME" pins
	// that, because this wording has drifted toward naming it twice before.
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
		"invalid request body is a knell bug and names no operator setting": {
			status:  http.StatusBadRequest,
			body:    `{"message": "Invalid Form Body", "code": 50035, "errors": {"content": {"_errors": [{"code": "BASE_TYPE_MAX_LENGTH", "message": "Must be 2000 or fewer in length."}]}}}`,
			want:    []string{"Discord error code 50035", "knell bug"},
			notWant: []string{"Invalid Form Body", "BASE_TYPE_MAX_LENGTH", "2000 or fewer", "NODE_NAME"},
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

			err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
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

func TestAuthRejectionsNameTheWebhookURLNotAnAPIKey(t *testing.T) {
	t.Parallel()

	// CheckHTTPStatus renders 401/403 as "invalid API key (401)" / "access
	// denied (403)" - wording for a keyed API. knell sends no API key
	// (DISCORD_WEBHOOK_URL's own path and token ARE the credential), and
	// BEAT_TOKEN, the only key-shaped setting it has, gates the INBOUND /beat
	// endpoint. So both statuses must name DISCORD_WEBHOOK_URL themselves,
	// INCLUDING on the paths where statusDetail contributes no Discord code: an
	// edge or WAF in front of the webhook answers 401/403 with its own HTML or
	// an empty body, never Discord's error object, and that is precisely when
	// the operator has nothing else to go on. Asserting the credential is named
	// rather than the exact sentence, so the wording stays free to change.
	for name, tc := range map[string]struct {
		status int
		body   string
	}{
		"401 with Discord's code":    {status: http.StatusUnauthorized, body: `{"code": 50027}`},
		"401 from an edge with none": {status: http.StatusUnauthorized, body: "<html>Forbidden</html>"},
		"401 with no body at all":    {status: http.StatusUnauthorized, body: ""},
		"403 with Discord's code":    {status: http.StatusForbidden, body: `{"code": 10015}`},
		"403 from an edge with none": {status: http.StatusForbidden, body: "<html>Forbidden</html>"},
		"403 with no body at all":    {status: http.StatusForbidden, body: ""},
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

			err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
			if err == nil {
				t.Fatalf("BeatMissing against a %d = nil, want error", tc.status)
			}
			// The verdict must stand on its own, so it is asserted on every
			// body shape and not only the one carrying Discord's code.
			for _, want := range []string{"DISCORD_WEBHOOK_URL", "knell sends no API key"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("err = %v, want it to name %q", err, want)
				}
			}
			// Neither the body's own words nor the credential itself — and
			// not httpx's keyed-API diagnosis either: rendering "invalid API
			// key" beside "knell sends no API key" contradicts itself.
			for _, leak := range []string{"plainsegment", "<html>", "invalid API key", "access denied"} {
				if strings.Contains(err.Error(), leak) {
					t.Errorf("err = %v, kept %q", err, leak)
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
		// Shutdown, not a fault on the path to Discord: the attempt context
		// governs the body read too, so a canceled sweep surfaces here and
		// must not read as a broken connection.
		"canceled sweep": {
			in:   fmt.Errorf("reading rejected response body: %w", context.Canceled),
			want: "delivery was canceled before the body was complete",
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

// transportPhraseTimeoutError is a net.Error that only times out, for the
// timeout arm of TestTransportPhraseClassifiesStructurally.
type transportPhraseTimeoutError struct{ msg string }

func (e transportPhraseTimeoutError) Error() string { return e.msg }
func (transportPhraseTimeoutError) Timeout() bool   { return true }
func (transportPhraseTimeoutError) Temporary() bool { return false }

func TestTransportPhraseClassifiesStructurally(t *testing.T) {
	t.Parallel()

	// transportPhrase is what an operator reads for every failed attempt, and
	// it must name the cause from its STRUCTURE only: a transport cause can be
	// written from a response header (see
	// TestRedirectDerivedTransportErrorsCarryNoRemoteText), so no case may
	// render cause.Error(). Each case therefore asserts the phrase AND that
	// the cause's own text did not survive.
	//
	// The proxyconnect cases pin the ORDER of the switch, not just its
	// mapping: net/http wraps a proxy dial failure as
	// *net.OpError{Op: "proxyconnect"} around the same ECONNREFUSED/DNS
	// causes the webhook-host branches match, so without that branch first
	// knell names the wrong endpoint during an egress-proxy outage.
	const remote = "remotelyauthoredtext"
	for name, tc := range map[string]struct {
		cause error
		want  string
	}{
		"canceled": {cause: context.Canceled, want: "webhook delivery was canceled"},
		"deadline names the stage": {
			cause: &net.OpError{Op: "dial", Err: fmt.Errorf("%s: %w", remote, context.DeadlineExceeded)},
			want:  "a deadline expired during dial",
		},
		"dns": {
			cause: &net.DNSError{Err: remote, Name: remote},
			want:  "the webhook host could not be resolved",
		},
		"refused": {
			cause: &net.OpError{Op: "dial", Err: fmt.Errorf("%s: %w", remote, syscall.ECONNREFUSED)},
			want:  "the webhook host refused the connection",
		},
		"a refused egress proxy is not the webhook host": {
			cause: &net.OpError{Op: "proxyconnect", Err: &net.OpError{Op: "dial", Err: fmt.Errorf("%s: %w", remote, syscall.ECONNREFUSED)}},
			want:  "the egress proxy could not be reached",
		},
		"an unresolvable egress proxy is not the webhook host": {
			cause: &net.OpError{Op: "proxyconnect", Err: &net.DNSError{Err: remote, Name: remote}},
			want:  "the egress proxy could not be reached",
		},
		"a timed-out egress proxy still names the proxy as the stage": {
			cause: &net.OpError{Op: "proxyconnect", Err: &net.OpError{Op: "dial", Err: fmt.Errorf("%s: %w", remote, context.DeadlineExceeded)}},
			want:  "a deadline expired during proxyconnect",
		},
		"reset": {
			cause: &net.OpError{Op: "read", Err: fmt.Errorf("%s: %w", remote, syscall.ECONNRESET)},
			want:  "the connection to the webhook was reset",
		},
		"no route": {
			cause: &net.OpError{Op: "dial", Err: fmt.Errorf("%s: %w", remote, syscall.EHOSTUNREACH)},
			want:  "no network route to the webhook host",
		},
		"network unreachable": {
			cause: &net.OpError{Op: "dial", Err: fmt.Errorf("%s: %w", remote, syscall.ENETUNREACH)},
			want:  "no network route to the webhook host",
		},
		"tls": {
			cause: &tls.CertificateVerificationError{Err: errors.New(remote)},
			want:  "the webhook's TLS certificate could not be verified",
		},
		"timeout names the stage": {
			cause: &net.OpError{Op: "read", Err: transportPhraseTimeoutError{msg: remote}},
			want:  "the webhook did not answer in time during read",
		},
		"eof": {
			cause: io.ErrUnexpectedEOF,
			want:  "the connection closed before the webhook answered",
		},
		"unrecognized reports only the stage": {
			cause: &net.OpError{Op: "write", Err: errors.New(remote)},
			want:  "webhook transport failed during write",
		},
		"unrecognized without an OpError": {
			cause: errors.New(remote),
			want:  "webhook transport failed",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := transportPhrase(tc.cause)
			if got != tc.want {
				t.Errorf("transportPhrase(%v) = %q, want %q", tc.cause, got, tc.want)
			}
			if strings.Contains(got, remote) {
				t.Errorf("transportPhrase(%v) = %q, rendered the cause's own text", tc.cause, got)
			}
		})
	}
}

func TestAttemptTimeoutIsRetried(t *testing.T) {
	t.Parallel()

	// A per-attempt deadline is a retryable condition, but httpx.IsTransient
	// rejects a bare context.DeadlineExceeded as terminal caller cancellation
	// before it consults the Transient interface. httpx.WithAttemptTimeout is
	// what tells it apart: the retry loop installs the bound itself and marks
	// an expiry that fired while the CALLER's context was still live. Without
	// the option every notification silently reduces to one attempt, and a
	// recovered notice is best-effort-once and would be lost outright.
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
	// test cares that the ATTEMPT's bound fired while the caller's budget is
	// still live, not how long it took to fire.
	d.attemptTimeout = 100 * time.Millisecond

	if err := d.post(context.Background(), "missing probe", "body"); err != nil {
		t.Fatalf("post() after a retried attempt timeout = %v, want nil", err)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("delivery attempts = %d, want 2 (an attempt timeout must be retried)", got)
	}
	// The production constants must keep the attempt context as the effective
	// bound: a Client.Timeout error does not carry context.DeadlineExceeded, so
	// an inverted ordering would preempt the attempt deadline and report the
	// failure as an anonymous client timeout instead of the classified phrase
	// (it is still retried, but the diagnostic is the point).
	if d.client.Timeout <= attemptTimeout {
		t.Errorf("client timeout %s <= per-attempt timeout %s: the transport bound would preempt the attempt context", d.client.Timeout, attemptTimeout)
	}
}

func TestAttemptTimeoutReportsSafeDiagnostic(t *testing.T) {
	t.Parallel()

	// The exhausted attempt timeout is the whole incident record an operator
	// reads (httpx.Do returns it verbatim, watch logs it at Error). Four
	// properties keep it useful and safe, and only driving the real retry loop
	// exercises them: the classified cause is carried, httpx classifies it
	// transient (so every attempt is spent, not just the first), the deadline
	// stays visible to errors.Is for knell's own callers, and no part of the
	// webhook URL reaches the message.
	const secret = "verysecrettimeouttoken"
	d := New("https://discord.example/api/webhooks/1234567890/"+secret, "node-1")
	t.Cleanup(d.Close)
	d.attemptTimeout = time.Millisecond
	var hits atomic.Int32
	d.client.Transport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		hits.Add(1)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})

	err := d.post(context.Background(), "missing probe", "body")
	if err == nil {
		t.Fatal("post() with every attempt timing out = nil, want error")
	}
	if got := hits.Load(); got != maxAttempts {
		t.Errorf("delivery attempts = %d, want %d (an attempt timeout must be retried, not terminal)", got, maxAttempts)
	}
	// "a deadline expired" is safeTransportError's classified phrase for the
	// cause; the cause's own text is never rendered, because a transport cause
	// can be written from a response header (see
	// TestRedirectDerivedTransportErrorsCarryNoRemoteText).
	if !strings.Contains(err.Error(), "a deadline expired") {
		t.Errorf("timeout error = %q, want it to contain the classified phrase", err)
	}
	if !httpx.IsAttemptTimeout(err) {
		t.Errorf("httpx.IsAttemptTimeout(%v) = false, want the per-attempt bound to be marked", err)
	}
	if !httpx.IsTransient(err) {
		t.Errorf("httpx.IsTransient(%v) = false, want a retryable attempt timeout", err)
	}
	// The mark WRAPS instead of replacing, so the timeout stays legible to
	// knell's own callers. The old hand-rolled error deliberately hid this to
	// stay retryable; WithAttemptTimeout does not have to.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("timeout error = %v, want the expired deadline to remain visible to errors.Is", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("timeout error leaks the webhook credential: %v", err)
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
	if err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour)); err != nil {
		t.Fatalf("BeatMissing after rate-limit retry: %v", err)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("attempts = %d, want 2", got)
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Errorf("retry waited %s, want >= 1s (Retry-After honored)", elapsed)
	}
}

func TestRateLimitWaitIsCappedByKnellsOwnCeiling(t *testing.T) {
	t.Parallel()

	// A 429's Retry-After is the OTHER end's number, and honoring it verbatim
	// parks the sweep's single sender goroutine for as long as it says: no
	// beat's notice is delivered during that window while /healthz stays
	// green. httpx waits min(Retry-After, ceiling), so the ceiling knell hands
	// WithRateLimitRetry is the whole bound. Nothing else pins it:
	// TestRateLimitRetriesAfterRetryAfter asserts a >= 1s LOWER bound against
	// a Retry-After of 1, which a ceiling of 30s, of 30 minutes, or of 0
	// (httpx's own 60s fallback) all satisfy identically.
	var attempts atomic.Int64
	d := New("https://discord.example/api/webhooks/1234567890/plainsegment", "node-1")
	t.Cleanup(d.Close)
	// The seam is a test affordance, not the policy, so pin the production
	// wiring BEFORE shortening it: a New that drops the assignment leaves the
	// field zero and httpx falls back to its own 60s cap, and a ceiling raised
	// past two sweep ticks parks the single sender across sweeps. Neither
	// shows up in the timing assertions below, which run against this
	// notifier's shortened ceiling.
	if d.rateLimitMaxWait != rateLimitMaxWait {
		t.Fatalf("New() rateLimitMaxWait = %s, want %s: httpx falls back to its own 60s cap for a non-positive ceiling", d.rateLimitMaxWait, rateLimitMaxWait)
	}
	if rateLimitMaxWait <= 0 || rateLimitMaxWait > 2*watch.DefaultTick {
		t.Errorf("rateLimitMaxWait = %s, want a positive ceiling within two %s sweep ticks: the sweep is the single sender, so a longer wait holds every other beat's notice", rateLimitMaxWait, watch.DefaultTick)
	}
	// Shorten only this notifier's ceiling: the branch under test cares that
	// knell's own ceiling bounds the wait, not how long the production one is.
	const ceiling = 50 * time.Millisecond
	d.rateLimitMaxWait = ceiling
	d.client.Transport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		if _, copyErr := io.Copy(io.Discard, r.Body); copyErr != nil {
			t.Errorf("reading request body: %v", copyErr)
		}
		header := make(http.Header)
		// An hour: the shape a global rate limit or an edge in front of the
		// webhook answers with, and far past any wait this app can afford.
		header.Set("Retry-After", "3600")
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Status:     "429 Too Many Requests",
			Header:     header,
			Body:       http.NoBody,
			Request:    r,
		}, nil
	})

	// The caller's budget is bounded so an UNCAPPED wait fails this test in
	// seconds instead of hanging the package for the hour the response asked
	// for: httpx observes the dead context before its next retry, so the
	// attempt count below is what reports the regression.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err := d.post(ctx, "missing probe", "body")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("post() against a permanent 429 = nil, want error")
	}
	if got := attempts.Load(); got != int64(maxAttempts) {
		t.Errorf("delivery attempts = %d, want %d: every rate-limited attempt must be spent within the caller's budget, which only knell's own ceiling on the wait guarantees", got, maxAttempts)
	}
	// The ceiling was the wait, and it was actually waited: a ceiling silently
	// reduced to zero would spend the attempts with no back-pressure at all.
	if elapsed < 2*ceiling {
		t.Errorf("delivery took %s, want at least %s (one ceiling-long wait before each of the %d retries)", elapsed, 2*ceiling, maxAttempts-1)
	}
}

// TestNoticesEscapeDiscordMarkdownInConfiguredValues is the oracle for the
// escaping every notice depends on. Without it, dropping escapeMarkdown at any
// of the three call sites leaves the whole suite green.
func TestNoticesEscapeDiscordMarkdownInConfiguredValues(t *testing.T) {
	t.Parallel()

	// The two values a notice interpolates that knell did not write itself: a
	// beat id (config's grammar admits "_") and a node name (arbitrary, only
	// trimmed and byte-capped, so it can carry Discord's masked-link
	// delimiters). Left literal, Discord CONSUMES the markup characters, so the
	// notice names a beat matching nothing in BEATS and nothing on /beat/{id},
	// and the node reads as something other than the configured identity.
	const (
		id       = "db_backup_nightly"
		node     = "obs*1[x](https://example)"
		wantID   = `db\_backup\_nightly`
		wantNode = `obs\*1\[x\](https://example)`
	)
	outage := watch.Outage{
		Started:    time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
		Recovered:  time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
		LateReason: watch.LateUndelivered,
	}
	sends := map[string]func(*Discord) error{
		"missing":   func(d *Discord) error { return d.BeatMissing(context.Background(), id, liveSilence(time.Hour)) },
		"recovered": func(d *Discord) error { return d.BeatRecovered(context.Background(), id, liveSilence(time.Hour)) },
		"history one": func(d *Discord) error {
			return d.BeatOutageHistory(context.Background(), id, []watch.Outage{outage})
		},
		"history several": func(d *Discord) error {
			return d.BeatOutageHistory(context.Background(), id, []watch.Outage{outage, outage})
		},
	}
	for name, send := range sends {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rec := newWebhookRecorder(http.StatusNoContent)
			srv := httptest.NewServer(rec.handler(t))
			defer srv.Close()
			d := New(srv.URL, node)
			defer d.Close()

			if err := send(d); err != nil {
				t.Fatalf("sending the %s notice: %v", name, err)
			}
			content := <-rec.contents
			for _, want := range []string{wantID, wantNode} {
				if !strings.Contains(content, want) {
					t.Errorf("the %s notice = %q, want it to carry %q: Discord eats an unescaped markup character instead of styling it, so the operator cannot copy the beat id or the node name out of the notice", name, content, want)
				}
			}
		})
	}
}

func TestRequestBuildErrorNeverLeaksWebhookURL(t *testing.T) {
	t.Parallel()

	// A control character makes http.NewRequestWithContext reject the URL;
	// the raw parse error embeds the full URL (with its secret path), so
	// the returned error must be reduced to the cause only.
	d := New("http://127.0.0.1:9/api/webhooks/1234567890/verysecrettoken\x00", "node-1")
	defer d.Close()

	err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
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

	err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
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
	// contract crosses post's whole wrap chain (client error -> safeTransportError,
	// whose Error() renders only knell's phrase and whose Unwrap is the only thing
	// carrying the cause -> logSafe -> %w wrap), so pin it against the real
	// notifier: dropping transportError.Unwrap fails here, not in watch.
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

	err := d.BeatMissing(ctx, "api", liveSilence(time.Hour))
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

	err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
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

	if err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour)); err == nil {
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

	err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
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
		if ref := r.Header.Get("Referer"); ref != "" {
			t.Errorf("followed hop Referer = %q, want none: net/http writes the previous request's full URL there, and for a webhook that path IS the credential", ref)
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

	if err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour)); err != nil {
		t.Fatalf("BeatMissing through a same-host 307 = %v, want delivery", err)
	}
	if got := finishHits.Load(); got != 1 {
		t.Errorf("redirect target hits = %d, want 1 (a same-host hop must be followed)", got)
	}
	if body, _ := finishBody.Load().(string); !strings.Contains(body, "MISSING") {
		t.Errorf("followed hop body = %q, want the notification payload", body)
	}
}

func TestDeliveryIdentifiesKnellToTheWebhookEdge(t *testing.T) {
	t.Parallel()

	// Go sends "Go-http-client/1.1" when User-Agent is unset, which an edge or
	// WAF in front of a webhook commonly refuses -- and that refusal arrives as
	// a non-transient 4xx the sweep re-posts forever, so every notice for the
	// beat is lost while /metrics still reports a healthy observer. Nothing
	// else in this package pins the header, so dropping the Set (or emptying
	// the constant, which makes net/http fall back to its own default) is
	// invisible to the suite.
	//
	// A stub transport rather than a listener: the assertion is a request
	// header, so no socket, no dial and no clock belong in the oracle. The
	// exact string is deliberately NOT compared against userAgent -- that
	// assertion passes even when the constant is emptied.
	var sent http.Header
	d := New("https://discord.example/api/webhooks/1234567890/plainsegment", "node-1")
	t.Cleanup(d.Close)
	d.client.Transport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		sent = r.Header.Clone()
		if _, copyErr := io.Copy(io.Discard, r.Body); copyErr != nil {
			t.Errorf("reading request body: %v", copyErr)
		}
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Status:     "204 No Content",
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    r,
		}, nil
	})

	if err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour)); err != nil {
		t.Fatalf("BeatMissing: %v", err)
	}
	ua := sent.Get("User-Agent")
	if ua == "" || strings.HasPrefix(ua, "Go-http-client/") {
		t.Errorf("outbound User-Agent = %q, want knell's own agent (an edge in front of the webhook refuses Go's default, and that 4xx is re-posted forever)", ua)
	}
	if !strings.Contains(ua, "knell") {
		t.Errorf("outbound User-Agent = %q, want it to identify knell", ua)
	}
}

func TestStatusBodyAtTheReadCapKeepsItsDiscordErrorCode(t *testing.T) {
	t.Parallel()

	// The cap decides whether an operator gets Discord's numeric code -- the
	// only fact knell publishes from a rejection -- so both sides of the
	// boundary are pinned. One byte past the cap is READ (statusDetail's
	// +1) so an over-cap body is detectable rather than silently truncated
	// into an unparseable fragment reported as "carried no Discord error
	// code"; a body of exactly the cap is Discord's object and keeps its code.
	// Neither an off-by-one in the comparison nor a lost +1 in the LimitReader
	// changes any other test in this file.
	//
	// padTo builds a valid Discord error object of exactly n bytes.
	padTo := func(n int) string {
		body := `{"code":10015,"pad":""}`
		if n < len(body) {
			t.Fatalf("cannot build a %d-byte error object, the envelope alone is %d", n, len(body))
		}
		return `{"code":10015,"pad":"` + strings.Repeat("x", n-len(body)) + `"}`
	}
	for name, tc := range map[string]struct {
		body    string
		want    []string
		notWant []string
	}{
		"a body of exactly the cap keeps its code": {
			body:    padTo(maxErrorBodyBytes),
			want:    []string{"Discord error code 10015", "recreate the webhook"},
			notWant: []string{"detail dropped"},
		},
		"one byte past the cap is reported as over-cap, not as codeless": {
			body:    padTo(maxErrorBodyBytes + 1),
			want:    []string{fmt.Sprintf("response body over %d bytes, detail dropped", maxErrorBodyBytes)},
			notWant: []string{"Discord error code", "carried no Discord error code"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if len(tc.body) > maxErrorBodyBytes+1 || len(tc.body) < maxErrorBodyBytes {
				t.Fatalf("setup: body is %d bytes, want the cap boundary", len(tc.body))
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			d := New(srv.URL+"/api/webhooks/1234567890/plainsegment", "node-1")
			defer d.Close()

			err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour))
			if err == nil {
				t.Fatal("BeatMissing against a 404 = nil, want error")
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

func TestExhaustedDeliveryIsLoggedBelowAlarmLevel(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// watch already publishes the terminal verdict for a failed delivery at
	// Error (with the beat, the silence and the retry plan), so httpx's own
	// exhaustion line is a second, thinner record of the SAME event.
	// WithExhaustedLevel demotes it to Debug; dropping the option restores
	// httpx's default Warn and every failed notification then raises two
	// alarm-level lines, which is exactly the duplicate an operator reading
	// the log during a Discord outage has to reconcile.
	rec := captureDeliveryLogs(t)

	// Connection refused is transient, so all maxAttempts run and the
	// exhausted line is emitted.
	d := New("http://127.0.0.1:9/api/webhooks/1234567890/plainsegment", "node-1")
	t.Cleanup(d.Close)

	if err := d.BeatMissing(context.Background(), "api", liveSilence(time.Hour)); err == nil {
		t.Fatal("BeatMissing against a refused connection = nil, want error")
	}
	requireLogged(t, rec)
	// Both lines must still say WHICH notice failed. httpx builds them as
	// "<label> failed, retrying" and "<label> retries exhausted" from post's
	// WithLabel; with the option dropped they read "operation ...", so the
	// per-attempt record during a webhook outage names neither the
	// notification kind nor the beat. requireLogged matches httpx's own words
	// only, so nothing else in this package notices.
	const label = "discord webhook missing api"
	for _, line := range []string{label + " failed, retrying", label + " retries exhausted"} {
		if rec.Count(line) == 0 {
			t.Errorf("delivery logs have no %q line; records = %v", line, rec.Records())
		}
	}
	// The level asserted as a level, not as TextHandler's `level=WARN`
	// rendering: CountLevel reads the record's own Level, so the assertion
	// survives a handler change and cannot be retired by a rendering one.
	for _, alarm := range []slog.Level{slog.LevelWarn, slog.LevelError} {
		if n := rec.CountLevel(alarm, ""); n > 0 {
			t.Errorf("delivery logs contain %d %s records: %v (watch owns the alarm-level verdict for a failed delivery)", n, alarm, rec.Records())
		}
	}
}

func TestHistoryTimestampRendersUTCWhateverZoneTheProducerUsed(t *testing.T) {
	t.Parallel()

	rec := newWebhookRecorder(http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	d := New(srv.URL, "node-1")
	defer d.Close()

	// The producer's instant carries a NON-UTC zone: main.go passes time.Now,
	// so every Outage.Recovered is a time.Local value, and a UTC-built fixture
	// cannot tell a rendered .UTC() apart from a UTC-zoned input.
	offset := time.FixedZone("+0430", 4*3600+30*60)
	recovered := time.Date(2026, 7, 23, 18, 37, 0, 0, offset)
	outages := []watch.Outage{{Started: recovered.Add(-12 * time.Minute), Recovered: recovered}}
	if err := d.BeatOutageHistory(context.Background(), "api", outages); err != nil {
		t.Fatalf("BeatOutageHistory: %v", err)
	}
	content := <-rec.contents
	// 18:37 +04:30 is 14:07 UTC; without the .UTC() conversion the notice
	// renders "2026-07-23 18:37 +0430" and this fails.
	if want := "recovered at 2026-07-23 14:07 UTC"; !strings.Contains(content, want) {
		t.Errorf("content %q missing %q: historyMessage must convert the recovery point to UTC before formatting", content, want)
	}
}
