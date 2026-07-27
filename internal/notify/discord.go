// Package notify delivers knell's transition notifications to a Discord
// webhook. It is the app's only outbound-network package and retries
// transient delivery failures via httpx.
//
// Wording lives here, not in the state machine: internal/watch decides WHICH
// transition happened and hands over its own types (watch.Outage), and this
// package decides how an operator reads it.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/cplieger/httpx/v4"
	"github.com/cplieger/knell/internal/watch"
)

// attemptTimeout bounds each delivery attempt when the caller's context
// carries no deadline of its own.
const attemptTimeout = 10 * time.Second

// maxAttempts is the total delivery attempts per notification (httpx
// semantics: total, including the first).
const maxAttempts = 3

// maxErrorBodyBytes caps how much of a rejected response's body is carried
// into the delivery error. Discord names the cause there (a deleted webhook
// vs a rejected payload); a few hundred bytes is the whole explanation and
// keeps a hostile or chatty body out of the log.
const maxErrorBodyBytes = 512

// userAgent identifies this client to Discord's edge. Discord's HTTP API
// reference requires a User-Agent naming the client library and version;
// Go sends "Go-http-client/1.1" when the header is unset, and a refusal
// would arrive as a non-transient 4xx the sweep re-posts forever.
const userAgent = "knell (https://github.com/cplieger/knell)"

// Discord posts plain-content messages to one Discord-compatible webhook.
type Discord struct {
	client *http.Client
	url    string
	node   string
	// redact holds every rendering of the webhook URL that an error
	// message can carry (see redactionCandidates); logSafe scrubs all of
	// them, not only the raw configured string.
	redact []string
	// attemptTimeout bounds one delivery attempt. It is a field rather than
	// a direct use of the constant only so a test can shorten it on its own
	// notifier; New always sets it to attemptTimeout.
	attemptTimeout time.Duration
}

// Discord implements the transition contract the state machine consumes;
// the assertion keeps a signature drift a notify-local compile error
// instead of one that first appears in main's wiring.
var _ watch.Notifier = (*Discord)(nil)

// New builds a Discord notifier for the given webhook URL. node names this
// observer instance in every message so multi-node deployments read as
// distinct reports.
func New(webhookURL, node string) *Discord {
	// Client timeout above the per-attempt context timeout so the
	// context is the effective per-attempt bound.
	client := httpx.NewClient(attemptTimeout + 5*time.Second)
	// Redirect policy, declaratively: follow only a same-host hop (the
	// configured webhook URL is the only trusted delivery target; a
	// cross-host hop would post the notice to an origin the operator never
	// named, and Go forwards custom headers across hops), never an
	// https->http downgrade (refused by default, and it matters here
	// because the URL's own path is the credential), and never a hop
	// net/http would rewrite to another method — the webhook POST must not
	// be replayed as a bodyless GET. WithPreserveMethod refuses such a hop
	// by surfacing its 3xx response, which postAttempt then reports as
	// non-delivery; TestMethodChangingRedirectIsNotDelivery pins that, and
	// is also the only test pinning that a policy is installed at all.
	client.CheckRedirect = httpx.RedirectPolicyFunc(httpx.WithSameHost(), httpx.WithPreserveMethod())
	return &Discord{
		client:         client,
		url:            webhookURL,
		node:           node,
		redact:         redactionCandidates(webhookURL),
		attemptTimeout: attemptTimeout,
	}
}

// redactionCandidates lists every rendering of the webhook URL whose presence
// in an error message would leak the credential (a Discord webhook carries it
// in the URL path). The raw configured string is only one of them:
// http.NewRequestWithContext parses the URL and HTTP code renders
// req.URL.String(), which net/url canonicalizes — non-ASCII path bytes come
// back percent-escaped — so the canonical form can carry the credential
// without containing the configured string. The bare path forms cover an
// error that quotes only the path. Duplicates, the empty path and a bare "/"
// are dropped ("/" appears in nearly every message and redacting it would
// mangle unrelated text without hiding a secret). An unparseable URL yields
// the raw value alone, which is exactly the text such an error embeds.
func redactionCandidates(webhookURL string) []string {
	candidates := []string{webhookURL}
	u, parseErr := url.Parse(webhookURL)
	if parseErr != nil {
		return candidates
	}
	for _, c := range []string{u.String(), u.Path, u.EscapedPath()} {
		if c == "" || c == "/" || slices.Contains(candidates, c) {
			continue
		}
		candidates = append(candidates, c)
	}
	return candidates
}

// Close releases idle connections. Call once on shutdown.
func (d *Discord) Close() {
	d.client.CloseIdleConnections()
}

// BeatMissing announces that a beat's deadline of silence has passed.
func (d *Discord) BeatMissing(ctx context.Context, id string, silence time.Duration) error {
	msg := fmt.Sprintf(
		"🚨 [knell %s] beat **%s** MISSING: silent for %s. The sender is down, or nothing on its path can reach this observer.",
		d.node, id, silence.Truncate(time.Second),
	)
	return d.post(ctx, "missing "+id, msg)
}

// BeatRecovered announces the first ping after a missing alert.
func (d *Discord) BeatRecovered(ctx context.Context, id string, downFor time.Duration) error {
	msg := fmt.Sprintf(
		"✅ [knell %s] beat **%s** recovered: pings arriving again after %s of silence.",
		d.node, id, downFor.Truncate(time.Second),
	)
	return d.post(ctx, "recovered "+id, msg)
}

// historyTimeFormat renders a recovery point for an operator reading the
// notice minutes after the fact: wall-clock time plus zone, so it lines up
// with a dashboard without guessing which observer's timezone it came from.
const historyTimeFormat = "15:04 MST"

// BeatOutageHistory announces outages that had already ended before this
// observer could deliver their notices, in one past-tense message so a
// resolved incident never reads as a new live failure. One outage is reported
// on its own; several are summarized, because the point of the message is
// that they are over, not to replay each of them.
func (d *Discord) BeatOutageHistory(ctx context.Context, id string, outages []watch.Outage) error {
	if len(outages) == 0 {
		// watch never sends an empty history notice; guard so a future
		// caller cannot post a message that reports nothing.
		return errors.New("delivering history notification: no ended outages to report")
	}
	return d.post(ctx, "history "+id, d.historyMessage(id, outages))
}

// historyMessage renders the history notice for id. outages is chronological
// and non-empty (BeatOutageHistory's contract), so the last entry is the most
// recent recovery.
func (d *Discord) historyMessage(id string, outages []watch.Outage) string {
	last := outages[len(outages)-1]
	if len(outages) == 1 {
		return fmt.Sprintf(
			"🕓 [knell %s] beat **%s** was missing for %s, recovered at %s. This notice is late: notifications were failing while the outage happened.",
			d.node, id, last.DownFor().Truncate(time.Second), last.Recovered.Format(historyTimeFormat),
		)
	}
	return fmt.Sprintf(
		"🕓 [knell %s] beat **%s** had %d outages while notifications were failing: longest %s, last recovered at %s.",
		d.node, id, len(outages),
		watch.LongestOutage(outages).Truncate(time.Second), last.Recovered.Format(historyTimeFormat),
	)
}

// post delivers one message, retrying transient failures. The webhook URL
// never appears in returned errors or logs: transport errors are reduced by
// logSafe, and a status failure is httpx.CheckHTTPStatus's error (the status
// code only) plus a bounded prefix of the remote body, which postAttempt
// scrubs against the same candidates before wrapping it.
func (d *Discord) post(ctx context.Context, label, content string) error {
	body, err := json.Marshal(map[string]string{"content": content})
	if err != nil {
		return fmt.Errorf("encoding webhook payload: %w", err)
	}
	_, err = httpx.Do(ctx, func(ctx context.Context) (struct{}, error) {
		return d.postAttempt(ctx, body)
	}, httpx.WithLabel("discord webhook "+label), httpx.WithMaxAttempts(maxAttempts),
		httpx.WithRateLimitRetry(30*time.Second),
		// watch publishes the terminal verdict for every failed delivery
		// (sendMissing/sendHistory/sendRecovered log at Error with the beat,
		// the silence and the retry plan), so httpx's own exhaustion WARN is a
		// second, thinner line for one event. Debug keeps it for diagnosis
		// without the alarm, which is what WithExhaustedLevel is for.
		httpx.WithExhaustedLevel(slog.LevelDebug))
	if err != nil {
		return fmt.Errorf("delivering %s notification: %w", label, d.logSafe(err))
	}
	return nil
}

// logSafe reduces err to a form that cannot carry the webhook URL.
// httpx.LogSafeError strips the *url.Error wrapper that embeds it; the
// value-based redaction below is the backstop for any error type whose text
// carries the URL without being a *url.Error (LogSafeError passes those
// through unchanged, so the type-based reduction alone is only as complete as
// httpx's error taxonomy). Every candidate rendering is scrubbed, not just the
// raw configured string, because an unrecognized error commonly formats
// req.URL.String() — net/url's canonical form, which may percent-escape the
// credential-bearing path. Wrapping preserves errors.Is/As, which the sweep
// relies on for context.Canceled and httpx.Do for transient classification —
// httpx.RedactSecret cannot be used here because it returns a bare
// errors.New and would break both.
func (d *Discord) logSafe(err error) error {
	safe := httpx.LogSafeError(err)
	if safe == nil {
		if err == nil {
			return nil
		}
		// LogSafeError returns urlErr.Err verbatim, so a *url.Error whose
		// own Err is nil would reduce a real failure to nil. postAttempt's
		// return IS httpx.Do's success signal, so a nil there would report
		// an undelivered notification as delivered and suppress the alert;
		// fail closed instead.
		return errors.New("webhook delivery failed")
	}
	// Candidates are never empty (config rejects an empty/non-https webhook
	// URL before New is called), and RedactSecretString is a no-op on an
	// empty secret, so this cannot mask an error into an empty message.
	msg := safe.Error()
	scrubbed := d.scrubText(msg)
	if scrubbed != msg {
		return &redactedError{msg: scrubbed, err: safe}
	}
	return safe
}

// scrubText removes every candidate rendering of the webhook URL from text.
// It is the value-based half of logSafe, split out because postAttempt must
// scrub a response body at source (before httpx.Do logs the attempt error)
// rather than through logSafe, which only sees httpx.Do's return.
// RedactSecretString is a plain replacement, so this is a no-op on text that
// does not carry the credential.
func (d *Discord) scrubText(text string) string {
	for _, candidate := range d.redact {
		text = httpx.RedactSecretString(text, candidate)
	}
	return text
}

// redactedError carries a scrubbed message while keeping the original error in
// the chain for errors.Is/As.
type redactedError struct {
	err error
	msg string
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }

// attemptTimeoutError reports that a single delivery attempt exceeded
// postAttempt's private per-attempt deadline while the caller's context was
// still live, which is a retryable condition. It intentionally has no Unwrap
// method: exposing context.DeadlineExceeded would make httpx.Do treat it as a
// terminal caller-cancellation decision before consulting IsTransient. Its
// message carries no URL.
type attemptTimeoutError struct{}

func (attemptTimeoutError) Error() string     { return "webhook attempt timed out" }
func (attemptTimeoutError) IsTransient() bool { return true }

// postAttempt performs one delivery attempt of an already-encoded payload:
// per-attempt deadline, request construction, transport call, response
// cleanup, and strict delivery validation. It is the retry callback post
// hands to httpx.Do, which owns the retry policy and terminal wrapping.
// Every error it returns is URL-free — including the remote body prefix on a
// status failure, which is scrubbed here rather than by post's logSafe,
// because httpx.Do logs the attempt error before post sees it.
func (d *Discord) postAttempt(ctx context.Context, body []byte) (struct{}, error) {
	attemptCtx, cancel := httpx.ContextWithDefaultTimeout(ctx, d.attemptTimeout)
	defer cancel()
	req, reqErr := http.NewRequestWithContext(attemptCtx, http.MethodPost, d.url, bytes.NewReader(body))
	if reqErr != nil {
		// The raw error would embed the URL; report the cause only.
		return struct{}{}, fmt.Errorf("building webhook request: %w", d.logSafe(reqErr))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	resp, doErr := d.client.Do(req) //nolint:bodyclose // closed via deferred httpx.DrainClose below
	if doErr != nil {
		// A child attempt deadline is retryable while the caller's budget is
		// still live. httpx deliberately treats context deadline errors as
		// terminal, so translate only this per-attempt timeout into its
		// Transient contract; caller cancellation/deadlines stay terminal.
		if errors.Is(doErr, context.DeadlineExceeded) && ctx.Err() == nil {
			return struct{}{}, attemptTimeoutError{}
		}
		// *url.Error embeds the full webhook URL; reduce it to its cause
		// (transient classification survives the reduction).
		return struct{}{}, d.logSafe(doErr)
	}
	defer httpx.DrainClose(resp.Body)
	// Success is exactly 2xx here: CheckHTTPStatus rejects every other
	// status, an unfollowed redirect's 3xx included (pinned by
	// TestUnfollowedRedirectIsNotDelivery), so the sweep keeps retrying a
	// non-delivery. Its error is typed, which is what lets httpx.Do
	// classify 502/503/504 as transient and retry within the call.
	if statusErr := httpx.CheckHTTPStatus(resp); statusErr != nil {
		// The code alone cannot tell a deleted webhook from a rejected
		// payload; Discord names the cause in the body. Wrapping keeps the
		// typed error in the chain (httpx.Do still classifies 502/503/504 as
		// transient and still finds *RateLimitError for the 429 wait). The
		// excerpt is scrubbed HERE, not by post's logSafe: httpx.Do logs a
		// transient attempt's error through the type-based LogSafeError only,
		// which passes this wrapped error through unchanged, so a body that
		// echoes the request path (a common proxy error page) would otherwise
		// carry the credential into the retry and exhausted log lines. Scrub
		// precedes Quote because Quote escapes a non-ASCII candidate byte out
		// of reach of the match; Quote then neutralizes control characters
		// before the text reaches a log line.
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		if len(detail) > 0 {
			return struct{}{}, fmt.Errorf("%w: %s", statusErr, strconv.Quote(d.scrubText(string(detail))))
		}
		return struct{}{}, statusErr
	}
	return struct{}{}, nil
}
