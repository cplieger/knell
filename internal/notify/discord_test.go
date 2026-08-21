package notify

import (
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

	"github.com/cplieger/httpx/v5"
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

	if err := d.BeatMissing(t.Context(), "api", liveSilence(21*time.Minute+30*time.Second)); err != nil {
		t.Fatalf("BeatMissing: %v", err)
	}
	content := <-rec.contents
	for _, want := range []string{"node-1", "api", "MISSING", "21m30s"} {
		if !strings.Contains(content, want) {
			t.Errorf("content %q missing %q", content, want)
		}
	}
}

// TestEveryNoticeSuppressesMentions pins the payload field that is the only
// mention control: escapeMarkdown deliberately leaves "@" alone (a backslash
// before it is not a Discord escape), so without allowed_mentions.parse = []
// a NODE_NAME carrying "@everyone" pings the channel on every notice.
// Discord ignores unknown JSON fields, so a typoed key fails no request and
// only this decode can catch it. One notice suffices: post is the single
// funnel every shape passes through.
func TestEveryNoticeSuppressesMentions(t *testing.T) {
	t.Parallel()

	bodies := make(chan []byte, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		bodies <- body
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	d := New(srv.URL, "@everyone")
	t.Cleanup(d.Close)

	if err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour)); err != nil {
		t.Fatalf("BeatMissing: %v", err)
	}
	var payload struct {
		Content         string `json:"content"`
		AllowedMentions *struct {
			Parse *[]string `json:"parse"`
		} `json:"allowed_mentions"`
	}
	if err := json.Unmarshal(<-bodies, &payload); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if payload.AllowedMentions == nil || payload.AllowedMentions.Parse == nil || len(*payload.AllowedMentions.Parse) != 0 {
		t.Errorf("payload allowed_mentions = %+v, want parse present and empty: it is the only mention control", payload.AllowedMentions)
	}
	if !strings.Contains(payload.Content, "@everyone") {
		t.Errorf("content = %q, want the raw node name: suppression is the payload's job, not the escaper's", payload.Content)
	}
}

func TestMissingNoticeDoesNotPresumeTheBeatEverPinged(t *testing.T) {
	t.Parallel()

	// BeatMissing's wording has to fit a beat that was configured in BEATS and
	// never wired to a sender at all -- a typo'd id, a sender pointed at the
	// wrong observer -- because the boot-armed clock fires this very notice one
	// deadline after start, and notify cannot tell that case from a sender that
	// pinged for weeks and stopped (watch.Transition carries no "was ever seen"
	// bit, and Started collapses the last ping and the process-start baseline
	// into one field). A rewording that presumes a previous ping sends the
	// operator to inspect a sender that never existed, and every other
	// assertion on this notice -- the MISSING keyword, the beat id, the
	// truncated duration, the length budget -- still passes.
	rec := newWebhookRecorder(http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	t.Cleanup(srv.Close)

	d := New(srv.URL, "node-1")
	t.Cleanup(d.Close)

	if err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour)); err != nil {
		t.Fatalf("BeatMissing: %v", err)
	}
	content := <-rec.contents
	for _, want := range []string{"check the sender", "anything is pinging this beat id at all"} {
		if !strings.Contains(content, want) {
			t.Errorf("the missing notice = %q, want it to say %q: the beat may never have been pinged at all, and this notice is the only place that says so", content, want)
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

	if err := d.BeatRecovered(t.Context(), "api", liveSilence(45*time.Minute)); err != nil {
		t.Fatalf("BeatRecovered: %v", err)
	}
	content := <-rec.contents
	for _, want := range []string{"node-1", "api", "recovered", "45m"} {
		if !strings.Contains(content, want) {
			t.Errorf("content %q missing %q", content, want)
		}
	}
}

// TestBeatOutageHistoryStatesTheTrueReasonForALateNotice pins the mapping from
// a record's delivery blame to the clause an operator acts on, for every shape of
// batch. Each case asserts BOTH the wording that belongs to its case and the
// wording that belongs to the OTHER one: a want-only assertion still passes if
// the clauses are swapped (they all mention the outage and a duration), and
// swapping them is precisely the bug this prevents — telling an operator to
// inspect a webhook that delivered this message on its first attempt, or
// vouching for one that had just refused it.
func TestBeatOutageHistoryStatesTheTrueReasonForALateNotice(t *testing.T) {
	t.Parallel()

	recovered := time.Date(2026, 7, 23, 14, 7, 0, 0, time.UTC)
	// outage builds one ended outage of span, blaming delivery or not; the spans
	// differ per entry only so the summary's "longest" is unambiguous.
	outage := func(span time.Duration, undelivered bool) watch.Outage {
		return watch.Outage{
			Started:     recovered.Add(-span),
			Recovered:   recovered,
			Undelivered: undelivered,
		}
	}
	const (
		webhookClause = "check the webhook"
		delayedOne    = "This notice is late because delivery was delayed"
		delayedAll    = "Delivery was delayed for every outage"
		// The nothing-attempted clauses. Only a refused delivery may send an
		// operator to the webhook, so these must not contain webhookClause.
		noAttemptOne = "no delivery was ever attempted for it"
		noAttemptAll = "No delivery was ever attempted for any of them"
		notThePlace  = "the webhook is not the place to look"
	)
	cases := map[string]struct {
		outages []watch.Outage
		want    []string
		forbid  []string
	}{
		// The reason the split exists: nothing was ever sent for this outage, so
		// the notice must not spend the operator's evening on a healthy webhook.
		"one outage nothing was attempted for": {
			outages: []watch.Outage{outage(12*time.Minute, false)},
			want:    []string{"was missing for 12m0s", noAttemptOne, notThePlace},
			forbid:  []string{webhookClause, delayedOne, "notifications were failing"},
		},
		"one outage whose alert the webhook refused": {
			outages: []watch.Outage{outage(12*time.Minute, true)},
			want:    []string{"was missing for 12m0s", delayedOne, webhookClause},
			forbid:  []string{noAttemptOne, notThePlace},
		},
		"a batch of outages nothing was attempted for": {
			outages: []watch.Outage{
				outage(12*time.Minute, false),
				outage(47*time.Minute, false),
			},
			want:   []string{"had 2 outages", "longest 47m0s", noAttemptAll, notThePlace},
			forbid: []string{webhookClause, delayedAll},
		},
		"a batch of outages whose alerts the webhook all refused": {
			outages: []watch.Outage{
				outage(12*time.Minute, true),
				outage(47*time.Minute, true),
			},
			want:   []string{"had 2 outages", "longest 47m0s", delayedAll, webhookClause},
			forbid: []string{noAttemptAll, notThePlace},
		},
		// The batch a real webhook outage produces on a flapping beat: some
		// alerts refused, some outages over before a sweep could see them. Both
		// counts must be stated; picking one for the batch says something false
		// about the other outages.
		"a batch that mixes a refusal with an unattempted outage": {
			outages: []watch.Outage{
				outage(12*time.Minute, true),
				outage(47*time.Minute, true),
				outage(9*time.Minute, false),
			},
			want: []string{
				"had 3 outages", "longest 47m0s",
				"Delivery was delayed for 2 (check the webhook)", "1 had nothing attempted", webhookClause,
			},
			// No single-case clause may stand in for a mixed batch.
			forbid: []string{delayedAll, noAttemptAll},
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

			if err := d.BeatOutageHistory(t.Context(), "api", tc.outages); err != nil {
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
					t.Errorf("content %q states %q, which belongs to the other case", content, forbidden)
				}
			}
			// Past tense and the recovery point are the same either way;
			// only the explanation differs.
			if !strings.Contains(content, "recovered at 2026-07-23 14:07 UTC") {
				t.Errorf("content %q does not report the recovery point", content)
			}
		})
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

	if err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour)); err != nil {
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

	err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour))
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

	err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour))
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

func TestLogSafeReducesTransportErrorsWithoutBreakingTheChain(t *testing.T) {
	t.Parallel()

	// httpx.LogSafeError's whole remaining job, and the one that matters: the
	// *url.Error net/http builds around a transport failure is the only error
	// shape that embeds the full request URL, and for a webhook that URL's path
	// IS the credential. Reducing it to its cause is what keeps the URL out of
	// post's returned error and out of httpx.Do's retry lines, and the reduction
	// must not cost the errors.Is chain watch keys shutdown handling on
	// (context.Canceled) and httpx.Do keys transient classification on.
	//
	// There is deliberately no text-matching backstop for an error that
	// FORMATS a URL rendering into its own message: nothing in this package
	// publishes remote text or interpolates d.url, so no such error exists to
	// defend against, and pretending otherwise is what the two earlier leaks
	// in this file's history were made of.
	const secret = "verysecretchaintoken"
	rawURL := "https://discord.example/api/webhooks/1234567890/" + secret

	// Both POSITIONS, not both call sites: LogSafeError searches with errors.As
	// rather than matching the top, which is what safeTransportError's loop rests on.
	for name, in := range map[string]error{
		"bare url error":    &url.Error{Op: "Post", URL: rawURL, Err: context.Canceled},
		"wrapped url error": fmt.Errorf("attempt 3 failed: %w", &url.Error{Op: "Post", URL: rawURL, Err: context.Canceled}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := httpx.LogSafeError(in)
			if !strings.Contains(in.Error(), secret) {
				t.Fatalf("setup: input %q does not carry the credential, the assertion would be vacuous", in)
			}
			for _, leak := range []string{secret, "/api/webhooks/", "discord.example"} {
				if strings.Contains(got.Error(), leak) {
					t.Errorf("httpx.LogSafeError kept %q from the request URL: %v", leak, got)
				}
			}
			if !errors.Is(got, context.Canceled) {
				t.Errorf("httpx.LogSafeError broke the errors.Is chain: %v", got)
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
	// Two surfaces, failing independently: the error itself, and the reduction
	// httpx.Do's own attempt lines put it through.
	for surface, rendered := range map[string]error{
		"transport error":                got,
		"reduced error as httpx logs it": httpx.LogSafeError(got),
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

	got := httpx.LogSafeError(&url.Error{Op: "Post", URL: rawURL})
	if got == nil {
		t.Fatal("httpx.LogSafeError(*url.Error with a nil cause) = nil, want an error (a nil reports an undelivered notification as delivered)")
	}
	if strings.Contains(got.Error(), secret) {
		t.Errorf("the reduced error leaks the webhook credential: %v", got)
	}
	// The nil-in/nil-out half of the contract: postAttempt reduces only on a
	// real failure, so a nil must not become an error.
	if got := httpx.LogSafeError(nil); got != nil {
		t.Errorf("httpx.LogSafeError(nil) = %v, want nil", got)
	}
}

func TestDeliveryLogsNeverLeakWebhookURL(t *testing.T) {
	// Deliberately NOT t.Parallel: slog.Default() is process-global.
	//
	// The returned-error assertions cannot cover the LOG surface: postAttempt
	// constructs the returned error URL-free, while httpx.Do's per-attempt retry
	// and exhausted lines (both Debug here, via WithExhaustedLevel) log the RAW
	// attempt error through the type-based LogSafeError only. This pins that
	// surface end to end.
	const secret = "verysecretlogtoken"

	rec := captureDeliveryLogs(t)

	// Connection refused is transient, so all maxAttempts run and both the
	// per-attempt and exhausted lines are emitted.
	d := New("http://127.0.0.1:9/api/webhooks/1234567890/"+secret, "node-1")
	t.Cleanup(d.Close)

	if err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour)); err == nil {
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
	// ATTEMPT error through the type-based LogSafeError alone (post's own
	// reduction runs too late for them). What survives is the status, which is
	// what keeps "the webhook answered and rejected us" apart from "nothing
	// answered".
	const secret = "verysecretbodytoken"
	const secretPath = "/api/webhooks/1234567890/" + secret

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("502 Bad Gateway: upstream failed for " + r.URL.Path))
	}))
	defer srv.Close()

	rec := captureDeliveryLogs(t)

	d := New(srv.URL+secretPath, "node-1")
	t.Cleanup(d.Close)

	err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour))
	if err == nil {
		t.Fatal("BeatMissing against a persistent 503 = nil, want error")
	}
	requireLogged(t, rec)
	// The body is read and dropped, so the status is the whole verdict. Pinning
	// it keeps the absence assertions below meaningful: a run that failed before
	// the body was read would satisfy them without the branch under test ever
	// running.
	if !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("err = %v, want the HTTP 503 status error (the body branch must have run)", err)
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

	if err := d.BeatMissing(t.Context(), "api", live); err != nil {
		t.Fatalf("first BeatMissing: %v", err)
	}
	if err := d.BeatMissing(t.Context(), "db", live); err != nil {
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
	// end. knell claims no meaning for a code, so the number is always
	// reported bare and a body carrying none leaves the status as the whole
	// verdict.
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
			want:    []string{"HTTP 404", "Discord error code 10015"},
			notWant: []string{"Unknown Webhook", "10015 ("},
		},
		"invalid token": {
			status:  http.StatusUnauthorized,
			body:    `{"message": "Invalid Webhook Token", "code": 50027}`,
			want:    []string{"Discord error code 50027"},
			notWant: []string{"Invalid Webhook Token", "50027 ("},
		},
		"empty message": {
			status:  http.StatusBadRequest,
			body:    `{"message": "Cannot send an empty message", "code": 50006}`,
			want:    []string{"HTTP 400", "Discord error code 50006"},
			notWant: []string{"Cannot send an empty message"},
		},
		"invalid request body": {
			status:  http.StatusBadRequest,
			body:    `{"message": "Invalid Form Body", "code": 50035, "errors": {"content": {"_errors": [{"code": "BASE_TYPE_MAX_LENGTH", "message": "Must be 2000 or fewer in length."}]}}}`,
			want:    []string{"Discord error code 50035"},
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
		"non-JSON body leaves the status alone": {
			status:  http.StatusBadRequest,
			body:    "Bad Request",
			want:    []string{"HTTP 400"},
			notWant: []string{"Bad Request", "Discord error code"},
		},
		"JSON without a code leaves the status alone": {
			status:  http.StatusBadRequest,
			body:    `{"message": "You are being rate limited.", "retry_after": 0.5}`,
			want:    []string{"HTTP 400"},
			notWant: []string{"rate limited", "retry_after", "Discord error code"},
		},
		"non-numeric code is no code": {
			status:  http.StatusBadRequest,
			body:    `{"code": "10015"}`,
			want:    []string{"HTTP 400"},
			notWant: []string{"Discord error code"},
		},
		"empty body leaves the status alone": {
			status:  http.StatusBadRequest,
			body:    "",
			want:    []string{"HTTP 400"},
			notWant: []string{"Discord error code"},
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

			err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour))
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
	// The seam is a test affordance, not the policy, so pin the production
	// wiring BEFORE shortening it: a New that drops the assignment leaves the
	// field zero, which httpx reads as the option's ABSENCE (no per-attempt
	// bound at all), so a stalled webhook is bounded only by the client
	// timeout and its expiry is classified terminal instead of retryable.
	// Nothing below notices, because the assertions run against this
	// notifier's shortened deadline.
	if d.attemptTimeout != attemptTimeout {
		t.Fatalf("New() attemptTimeout = %s, want %s: a non-positive bound is httpx's option-absent path, so no attempt deadline is applied", d.attemptTimeout, attemptTimeout)
	}
	// Shorten only this notifier's per-attempt deadline: the branch under
	// test cares that the ATTEMPT's bound fired while the caller's budget is
	// still live, not how long it took to fire.
	d.attemptTimeout = 100 * time.Millisecond

	if err := d.post(t.Context(), "missing probe", "body"); err != nil {
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

// TestNewUsesProductionAttemptTimeout pins the per-attempt bound against a
// LITERAL, because every other assertion on it compares the wired field with
// the same production constant and therefore moves with any change to it. The
// value is a policy an operator depends on: shrinking it turns a slow-but-
// working webhook into a retried failure, and growing it lets a stalled POST
// eat the client timeout before the retries are spent.
func TestNewUsesProductionAttemptTimeout(t *testing.T) {
	t.Parallel()

	d := New("https://discord.example/api/webhooks/1234567890/plainsegment", "node-1")
	t.Cleanup(d.Close)

	if got, want := d.attemptTimeout, 10*time.Second; got != want {
		t.Errorf("New() attempt timeout = %s, want %s", got, want)
	}
}

func TestAttemptTimeoutReportsSafeDiagnostic(t *testing.T) {
	t.Parallel()

	// The exhausted attempt timeout is the whole incident record an operator
	// reads (httpx.Do returns it verbatim, watch logs it at Error). Four
	// properties keep it useful and safe, and only driving the real retry loop
	// exercises them: knell's own phrase is carried, httpx classifies it
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

	err := d.post(t.Context(), "missing probe", "body")
	if err == nil {
		t.Fatal("post() with every attempt timing out = nil, want error")
	}
	if got := hits.Load(); got != maxAttempts {
		t.Errorf("delivery attempts = %d, want %d (an attempt timeout must be retried, not terminal)", got, maxAttempts)
	}
	// "webhook transport failed" is safeTransportError's phrase for the cause;
	// the cause's own text is never rendered, because a transport cause can be
	// written from a response header (see
	// FuzzRedirectResponsesNeverCarryLocationText).
	if !strings.Contains(err.Error(), "webhook transport failed") {
		t.Errorf("timeout error = %q, want it to contain knell's own phrase", err)
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

func TestRateLimitWaitIsCappedByKnellsOwnCeiling(t *testing.T) {
	t.Parallel()

	// A 429's Retry-After is the OTHER end's number, and honoring it verbatim
	// parks the sweep's single sender: no beat's notice is delivered during that
	// window while /healthz stays green. Two bounds, and only both together
	// bound the park: this ceiling caps ONE wait (and IS the wait when no
	// Retry-After header parses), and sendBudget caps the delivery. Nothing else
	// in the package pins either -- every other assertion here is satisfied by a
	// ceiling of 30s, of 30 minutes, or of 0 (httpx's own 60s fallback)
	// identically.
	var attempts atomic.Int64
	d := New("https://discord.example/api/webhooks/1234567890/plainsegment", "node-1")
	t.Cleanup(d.Close)
	// The seam is a test affordance, not the policy, so pin the production
	// wiring BEFORE shortening it: a New that drops either assignment leaves the
	// field zero, which is httpx's option-absent path -- its own 60s cap for the
	// ceiling, and no budget at all for the delivery. Neither shows up in the
	// timing assertions below, which run against this notifier's shortened
	// values.
	if d.rateLimitMaxWait != rateLimitMaxWait {
		t.Fatalf("New() rateLimitMaxWait = %s, want %s: httpx falls back to its own 60s cap for a non-positive ceiling", d.rateLimitMaxWait, rateLimitMaxWait)
	}
	if d.sendBudget != sendBudget {
		t.Fatalf("New() sendBudget = %s, want %s: a non-positive budget is ContextWithDefaultTimeout's pass-through path, so the delivery keeps whatever the caller brought -- for watch's ctx, no deadline at all", d.sendBudget, sendBudget)
	}
	if rateLimitMaxWait <= 0 || sendBudget > 4*watch.DefaultTick {
		t.Errorf("rateLimitMaxWait = %s gives sendBudget = %s, want a positive ceiling whose delivery budget stays within four %s sweep ticks: the sweep is the single sender, so the budget is what holds every other beat's notice", rateLimitMaxWait, sendBudget, watch.DefaultTick)
	}
	// That upper bound has a floor to clear, and the floor is the schedule the
	// budget is derived from rather than the derivation itself: httpx parks up
	// to rateLimitMaxWait before a retry and then spends up to attemptTimeout
	// running it, so a budget that cannot fit one park plus one attempt is
	// spent on the first attempt alone. maxAttempts would then read as 3 and
	// behave as 1 the moment anything in front of Discord answers a
	// header-less 429 -- and a non-positive budget is worse again, being
	// ContextWithDefaultTimeout's pass-through path, which leaves the delivery
	// with no deadline at all and the sweep's single sender parked on it.
	if sendBudget <= rateLimitMaxWait+attemptTimeout {
		t.Errorf("sendBudget = %s, want more than one %s rate-limit park plus one %s attempt: a budget the retry schedule cannot fit inside is spent on attempt one, so the other %d never run", sendBudget, rateLimitMaxWait, attemptTimeout, maxAttempts-1)
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
	// attempt count below is what reports the regression. That deadline is also
	// what the door defers to, so this half measures the ceiling alone.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
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
	// The budget, not the sum of the waits, is what the sender pays. The same
	// permanent 429 under a budget shorter than two waits, on a ctx carrying no
	// deadline of its own -- watch's shape, and the only one the door bounds:
	// the loop is cut mid-park, so the last attempt never runs. For a missing or
	// history notice the sweep re-posts it; for the fire-once recovered notice
	// that cut attempt is a permanently dropped message.
	d.sendBudget = ceiling + ceiling/2
	attempts.Store(0)
	err = d.post(t.Context(), "missing probe", "body")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("post() against a permanent 429 under a %s budget = %v, want the budget's own expiry", d.sendBudget, err)
	}
	if got := attempts.Load(); got >= int64(maxAttempts) {
		t.Errorf("delivery attempts = %d, want fewer than %d: the budget bounds the whole delivery, so it cuts the retry loop instead of waiting out every ceiling", got, maxAttempts)
	}
}

// TestNoticesEscapeDiscordMarkdownInConfiguredValues is the oracle for the
// escaping every notice depends on. Without it, dropping escapeMarkdown at any
// of the three call sites leaves the whole suite green.
func TestNoticesEscapeDiscordMarkdownInConfiguredValues(t *testing.T) {
	t.Parallel()

	// The two values a notice interpolates that knell did not write itself: a
	// beat id (config's grammar admits "_") and a node name (arbitrary,
	// byte-capped only and exactly as the operator supplied it, so it can carry
	// Discord's masked-link
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
		Started:     time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
		Recovered:   time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
		Undelivered: true,
	}
	sends := map[string]func(*Discord) error{
		"missing":   func(d *Discord) error { return d.BeatMissing(t.Context(), id, liveSilence(time.Hour)) },
		"recovered": func(d *Discord) error { return d.BeatRecovered(t.Context(), id, liveSilence(time.Hour)) },
		"history one": func(d *Discord) error {
			return d.BeatOutageHistory(t.Context(), id, []watch.Outage{outage})
		},
		"history several": func(d *Discord) error {
			return d.BeatOutageHistory(t.Context(), id, []watch.Outage{outage, outage})
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

// TestBeatMissingEscapesEveryDiscordMarkdownCharacterInNodeName gives each
// markdownEscaper mapping its own named oracle. The test above pins only the
// asterisk, the brackets and the beat-id underscore, so deleting the
// backslash, tilde, backtick or pipe entry leaves the suite green while the
// configured observer identity is rendered as something else — NODE_NAME is
// byte-capped only and arrives exactly as the operator supplied it, so it can
// carry any of them.
func TestBeatMissingEscapesEveryDiscordMarkdownCharacterInNodeName(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		node string
		want string
	}{
		"backslash":       {node: `a\b`, want: `a\\b`},
		"asterisk":        {node: "a*b", want: `a\*b`},
		"underscore":      {node: "a_b", want: `a\_b`},
		"tilde":           {node: "a~b", want: `a\~b`},
		"backtick":        {node: "a`b", want: "a\\`b"},
		"pipe":            {node: "a|b", want: `a\|b`},
		"opening bracket": {node: "a[b", want: `a\[b`},
		"closing bracket": {node: "a]b", want: `a\]b`},

		"CRLF collapses to a space": {node: "a\r\nb", want: "a b"},
		"CR collapses to a space":   {node: "a\rb", want: "a b"},
		"LF collapses to a space":   {node: "a\nb", want: "a b"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rec := newWebhookRecorder(http.StatusNoContent)
			srv := httptest.NewServer(rec.handler(t))
			t.Cleanup(srv.Close)

			d := New(srv.URL, tc.node)
			t.Cleanup(d.Close)

			if err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour)); err != nil {
				t.Fatalf("BeatMissing: %v", err)
			}
			content := <-rec.contents
			if want := "[knell " + tc.want + "]"; !strings.Contains(content, want) {
				t.Errorf("BeatMissing node = %q, want %q", content, want)
			}
		})
	}
}

func TestEscapeMarkdownLeavesEveryOtherCharacterAlone(t *testing.T) {
	t.Parallel()

	// The other half of escapeMarkdown's contract, the half its doc states and
	// nothing measures: ONLY Discord's own markup characters are
	// backslash-escaped (line breaks are collapsed, not escaped, and sit
	// below this loop's 0x20 floor).
	// Discord strips a backslash solely in front of its markup, so an entry
	// added for anything else publishes the backslash itself and the operator
	// can no longer copy the value out of the alert. Every mapping that DOES
	// exist has a named oracle above; nothing rejects a mapping that should
	// not. Only the hyphen is covered incidentally today, by the "node-1"
	// substring every delivery assertion in this file happens to use.
	const markup = "\\*_~`|[]"
	for r := rune(0x20); r < 0x7f; r++ {
		if strings.ContainsRune(markup, r) {
			continue
		}
		in := "a" + string(r) + "b"
		if got := escapeMarkdown(in); got != in {
			t.Errorf("escapeMarkdown(%q) = %q, want it unchanged: %q is not one of Discord's markup characters, so Discord leaves the backslash visible and the configured beat id or node name cannot be read off the notice", in, got, string(r))
		}
	}
	// Whole values a deployment really configures, including the shapes the
	// loop's single-character cases cannot show: a hyphenated beat id (every
	// README example and both homelab beats) and a non-ASCII node name, since
	// NODE_NAME is byte-capped only and arrives exactly as the operator
	// supplied it.
	for _, in := range []string{"cron-backup", "watchdog-mimir", "caf\u00e9", "obs\u00a01"} {
		if got := escapeMarkdown(in); got != in {
			t.Errorf("escapeMarkdown(%q) = %q, want it unchanged: it carries no Discord markup character", in, got)
		}
	}
}

func TestRequestBuildErrorNeverLeaksWebhookURL(t *testing.T) {
	t.Parallel()

	// A control character makes http.NewRequestWithContext reject the URL;
	// the raw parse error embeds the full URL (with its secret path), so
	// the returned error must be reduced to the cause only.
	d := New("http://127.0.0.1:9/api/webhooks/1234567890/verysecrettoken\x00", "node-1")
	defer d.Close()

	err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour))
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

func TestCanceledDeliveryErrorIsCanceled(t *testing.T) {
	t.Parallel()

	// watch.sweep and watch.sendRecovered key their shutdown handling on
	// errors.Is(err, context.Canceled): an abandoned send must not count
	// as failed (KnellNotifyFailing would page on every shutdown). That
	// contract crosses post's whole wrap chain (client error -> safeTransportError,
	// whose Error() renders only knell's phrase and whose Unwrap is the only thing
	// carrying the cause -> %w wrap), so pin it against the real
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

	ctx, cancel := context.WithCancel(t.Context())
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

	err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour))
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

	if err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour)); err == nil {
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

	err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour))
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

	if err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour)); err != nil {
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

	if err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour)); err != nil {
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
	// boundary are pinned. One byte past the cap is READ (statusDetail's +1) so
	// an over-cap body is detectable and dropped rather than decoded; a body of
	// exactly the cap is Discord's object and keeps its code. Neither an
	// off-by-one in the comparison nor a lost +1 in the LimitReader changes any
	// other test in this file.
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
			body: padTo(maxErrorBodyBytes),
			want: []string{"Discord error code 10015"},
		},
		"one byte past the cap publishes no code": {
			body:    padTo(maxErrorBodyBytes + 1),
			want:    []string{"HTTP 404"},
			notWant: []string{"Discord error code"},
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

			err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour))
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

	if err := d.BeatMissing(t.Context(), "api", liveSilence(time.Hour)); err == nil {
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
	if err := d.BeatOutageHistory(t.Context(), "api", outages); err != nil {
		t.Fatalf("BeatOutageHistory: %v", err)
	}
	content := <-rec.contents
	// 18:37 +04:30 is 14:07 UTC; without the .UTC() conversion the notice
	// renders "2026-07-23 18:37 +0430" and this fails.
	if want := "recovered at 2026-07-23 14:07 UTC"; !strings.Contains(content, want) {
		t.Errorf("content %q missing %q: historyMessage must convert the recovery point to UTC before formatting", content, want)
	}
}

// TestNoticesReportWholeSecondDurations pins the .Truncate(time.Second) at
// every site a notice renders a span. Every other fixture in this package
// measures a whole-second silence, so all four truncations are no-ops under
// test and dropping them leaves the suite green - while production spans are
// never whole (Started, Observed and Recovered all come from time.Now), so
// every notice an operator reads would start carrying nanoseconds.
func TestNoticesReportWholeSecondDurations(t *testing.T) {
	t.Parallel()

	const ragged = 21*time.Minute + 30*time.Second + 123456789*time.Nanosecond
	started := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	short := watch.Outage{Started: started, Recovered: started.Add(ragged), Undelivered: true}
	// A ragged span that is also the batch's longest: the summary truncates
	// watch.LongestOutage's result, a fourth call site no singular notice
	// reaches.
	long := watch.Outage{
		Started:     started,
		Recovered:   started.Add(47*time.Minute + 987654321*time.Nanosecond),
		Undelivered: true,
	}
	cases := map[string]struct {
		send func(*Discord) error
		want string
	}{
		"missing": {
			send: func(d *Discord) error { return d.BeatMissing(t.Context(), "api", liveSilence(ragged)) },
			want: "silent for 21m30s.",
		},
		"recovered": {
			send: func(d *Discord) error { return d.BeatRecovered(t.Context(), "api", liveSilence(ragged)) },
			want: "after 21m30s of silence",
		},
		"history one": {
			send: func(d *Discord) error {
				return d.BeatOutageHistory(t.Context(), "api", []watch.Outage{short})
			},
			want: "was missing for 21m30s,",
		},
		"history several": {
			send: func(d *Discord) error {
				return d.BeatOutageHistory(t.Context(), "api", []watch.Outage{short, long})
			},
			want: "longest 47m0s,",
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

			if err := tc.send(d); err != nil {
				t.Fatalf("sending the %s notice: %v", name, err)
			}
			content := <-rec.contents
			if !strings.Contains(content, tc.want) {
				t.Errorf("the %s notice = %q, want it to report %q: an untruncated span renders nanoseconds, and every production span carries them", name, content, tc.want)
			}
		})
	}
}

func TestTransportErrorNamesTheFailedStage(t *testing.T) {
	t.Parallel()

	// net.OpError's Op is one of net's own fixed verbs, so it is printable where
	// the cause's text is not, and it is the only diagnosis a transport failure
	// gets: a stalled dial points at egress or DNS, a stalled read means the host
	// accepted the connection and went quiet. The exact-equality assertion is also
	// the leak oracle: the input carries the credential in its URL field, so any
	// rendering of it fails the comparison.
	rawURL := "https://discord.example/api/webhooks/1234567890/verysecretstagetoken"
	for name, tc := range map[string]struct {
		op   string
		want string
	}{
		"a refused dial names the dial":     {op: "dial", want: "webhook transport failed during dial"},
		"a stalled read names the read":     {op: "read", want: "webhook transport failed during read"},
		"an OpError with no verb adds none": {op: "", want: "webhook transport failed"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			in := &url.Error{Op: "Post", URL: rawURL, Err: &net.OpError{
				Op: tc.op, Net: "tcp", Err: errors.New("connect: connection refused"),
			}}
			if got := safeTransportError(in); got.Error() != tc.want {
				t.Errorf("safeTransportError(*net.OpError{Op: %q}).Error() = %q, want %q", tc.op, got.Error(), tc.want)
			}
		})
	}
}
