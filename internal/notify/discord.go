// Package notify delivers knell's transition notifications to a Discord
// webhook. It is the app's only outbound-network package and retries
// transient delivery failures via httpx.
//
// Wording lives here, not in the state machine: internal/watch decides WHICH
// transition happened and hands over its own types (watch.Transition for a
// live incident, watch.Outage for one that is already over), and this package
// decides how an operator reads it. Every duration a notice reports comes from
// those types' DownFor, so the live and past-tense notices cannot measure the
// same span differently.
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
	"syscall"
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

// maxErrorBodyBytes caps how much of a rejected response's body is READ, and
// nothing about it is ever printed. Discord names the cause in that body as a
// numeric code (a deleted webhook vs a rejected payload), and knell reports
// the code plus its own wording for it — never the body's own text, which is
// authored by the other end and can echo the webhook URL that IS the
// credential. The cap bounds the JSON parse; a body past it is not Discord's
// error object at all (that object is a few hundred bytes), so the size is
// reported as the fact it is and the detail is dropped.
const maxErrorBodyBytes = 512

// userAgent identifies this client to Discord's edge. Go sends
// "Go-http-client/1.1" when the header is unset, which an edge or WAF in
// front of a webhook commonly refuses; that refusal would arrive as a
// non-transient 4xx the sweep re-posts forever. Discord's DiscordBot
// ($url, $version) form is the bot-API rule and is deliberately not
// followed here: webhook execution accepts any identifying agent, and
// knell has no version symbol to render (nothing sets one, and
// debug.ReadBuildInfo plumbing would buy no delivery guarantee).
const userAgent = "knell (https://github.com/cplieger/knell)"

// Discord posts plain-content messages to one Discord-compatible webhook.
type Discord struct {
	client *http.Client
	url    string
	node   string
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
		attemptTimeout: attemptTimeout,
	}
}

// Close releases idle connections. Call once on shutdown.
func (d *Discord) Close() {
	d.client.CloseIdleConnections()
}

// BeatMissing announces that a beat's deadline of silence has passed.
func (d *Discord) BeatMissing(ctx context.Context, id string, live watch.Transition) error {
	msg := fmt.Sprintf(
		"🚨 [knell %s] beat **%s** MISSING: silent for %s. The sender is down, or nothing on its path can reach this observer.",
		d.node, id, live.DownFor().Truncate(time.Second),
	)
	return d.post(ctx, "missing "+id, msg)
}

// BeatRecovered announces the first ping after a missing alert.
func (d *Discord) BeatRecovered(ctx context.Context, id string, live watch.Transition) error {
	msg := fmt.Sprintf(
		"✅ [knell %s] beat **%s** recovered: pings arriving again after %s of silence.",
		d.node, id, live.DownFor().Truncate(time.Second),
	)
	return d.post(ctx, "recovered "+id, msg)
}

// historyTimeFormat renders the recovery point in the HISTORY notice, and it
// carries the DATE because that notice can be arbitrarily old: an outage whose
// alert went undelivered (watch.LateUndelivered) waited in watch's per-beat
// queue for a webhook that was unreachable, and beat deadlines are measured in
// hours, so the recovery point being reported is commonly a different day than
// the message an operator is reading. Discord's own message timestamp only
// bounds it from above, which leaves a bare "14:07" ambiguous between today,
// yesterday and Monday — and reconstructing the missed window is the whole
// point of a late notice. The other late reason (an outage that ended before a
// sweep saw it) reports a recovery point at most one sweep old, but it uses the
// same format: two shapes of the same notice would only invite reading the
// difference as meaning something.
//
// The live notices render no absolute timestamp to widen, deliberately: their
// watch.Transition carries the instants, but BeatMissing and BeatRecovered
// arrive as the transition happens, so their durations are already anchored to
// the message itself and a bare time of day would be unambiguous there anyway.
// Keeping the common message short is worth more than a date nobody has to
// reason about.
//
// The zone stays in both halves of the format: the operator reading the
// notice is not necessarily in the observer's timezone, and the notice is the
// whole record.
const historyTimeFormat = "2006-01-02 15:04 MST"

// BeatOutageHistory announces outages that were already over by the time this
// observer could send anything about them, in one past-tense message so a
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
//
// Every notice is two parts: WHAT happened, then why it is being read after
// the fact. The second part comes from watch's LateReason and is never guessed
// here, because the two reasons send an operator to opposite places: one is a
// webhook to fix, the other is a beat that came back faster than the sweep
// could see it, where a webhook check finds nothing wrong.
func (d *Discord) historyMessage(id string, outages []watch.Outage) string {
	last := outages[len(outages)-1]
	recovered := last.Recovered.Format(historyTimeFormat)
	if len(outages) == 1 {
		return fmt.Sprintf(
			"🕓 [knell %s] beat **%s** was missing for %s, recovered at %s. %s",
			d.node, id, last.DownFor().Truncate(time.Second), recovered, lateClause(last.LateReason),
		)
	}
	return fmt.Sprintf(
		"🕓 [knell %s] beat **%s** had %d outages: longest %s, last recovered at %s. %s",
		d.node, id, len(outages),
		watch.LongestOutage(outages).Truncate(time.Second), recovered, batchLateClause(outages),
	)
}

// lateClause explains why ONE ended outage is reported after the fact, and
// what the operator should do about it. The undelivered case names the webhook,
// because delivery is what lagged; the other case says delivery was fine, so
// nobody spends an evening on a webhook that posted this very message on its
// first try.
func lateClause(reason watch.LateReason) string {
	if reason == watch.LateUndelivered {
		return "This notice is late: its alert was still undelivered when the beat returned - check the webhook."
	}
	return "This notice is late only because the outage ended before a sweep detected it - nothing was wrong with delivery."
}

// batchLateClause explains why a whole run of ended outages is reported after
// the fact. A batch can MIX the two reasons — a webhook outage holds alerts
// back while short outages keep ending between sweeps, which is exactly how a
// flapping beat behaves during a Discord outage — so a mixed batch reports
// BOTH counts instead of picking the majority reason and stating something
// false about the rest. It keeps the webhook pointer: one undelivered alert is
// reason enough to look at delivery, while naming the outages that ended before
// a sweep saw them stops the count from reading as that many webhook failures.
func batchLateClause(outages []watch.Outage) string {
	undelivered := 0
	for _, o := range outages {
		if o.LateReason == watch.LateUndelivered {
			undelivered++
		}
	}
	switch undelivered {
	case len(outages):
		return "Their alerts were still undelivered when it returned - check the webhook."
	case 0:
		return "Each ended before a sweep detected it - nothing was wrong with delivery."
	default:
		return fmt.Sprintf("%d had an undelivered alert (check the webhook), %d ended before a sweep detected it.",
			undelivered, len(outages)-undelivered)
	}
}

// post delivers one message, retrying transient failures. The webhook URL
// cannot appear in returned errors or logs, because no text the OTHER end
// authored is ever printed: every message this package publishes is written
// here (none of them interpolates d.url), and the two places remote text
// could enter are reduced structurally instead of filtered — a transport
// error through logSafe (httpx.LogSafeError unwraps the *url.Error that
// embeds the URL) and a rejected response through statusDetail (Discord's
// numeric error code and knell's own wording for it, never the body's text).
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
		return fmt.Errorf("delivering %s notification: %w", label, logSafe(err))
	}
	return nil
}

// logSafe reduces err to a form that cannot carry the webhook URL. The
// reduction is purely STRUCTURAL: httpx.LogSafeError unwraps the *url.Error
// net/http builds around a transport failure, which is the one error shape
// that embeds the full request URL, and returns everything else untouched.
// There is deliberately no string search-and-replace backstop: text-matching
// redaction can only defend text knell chose to publish, and this package
// publishes none — every message is authored here (never interpolating d.url)
// and a rejected response contributes a number, not its own words (see
// statusDetail). postAttempt applies this before returning, because httpx.Do
// logs each attempt's error itself and reduces it by type only.
//
// The reduced error is returned as-is rather than re-wrapped, which keeps
// errors.Is/As intact: the sweep relies on it for context.Canceled and
// httpx.Do for transient classification. httpx.RedactSecret cannot be used
// here — it returns a bare errors.New and would break both.
func logSafe(err error) error {
	if err == nil {
		return nil
	}
	if safe := httpx.LogSafeError(err); safe != nil {
		return safe
	}
	// LogSafeError returns urlErr.Err verbatim, so a *url.Error whose own Err
	// is nil would reduce a real failure to nil. postAttempt's return IS
	// httpx.Do's success signal, so a nil there would report an undelivered
	// notification as delivered and suppress the alert; fail closed instead.
	return errors.New("webhook delivery failed")
}

// attemptTimeoutError reports that a single delivery attempt exceeded
// postAttempt's private per-attempt deadline while the caller's context was
// still live, which is a retryable condition. It intentionally has no Unwrap
// method: exposing context.DeadlineExceeded would make httpx.Do treat it as a
// terminal caller-cancellation decision before consulting IsTransient. Its
// message carries no URL.
type attemptTimeoutError struct {
	// cause is the text of the STRUCTURALLY REDUCED transport error the
	// expired deadline produced, and after is the bound that expired. A
	// string, not an error: the type must still not unwrap to
	// context.DeadlineExceeded (httpx classifies that terminal before
	// consulting IsTransient), and the text is exactly what the non-timeout
	// transport path already returns through logSafe — a transport error with
	// its URL-bearing *url.Error wrapper removed, never remote body bytes.
	// The zero value stays valid and renders the bare message.
	cause string
	after time.Duration
}

func (e attemptTimeoutError) Error() string {
	msg := "webhook attempt timed out"
	if e.after > 0 {
		msg += " after " + e.after.String()
	}
	if e.cause != "" {
		msg += ": " + e.cause
	}
	return msg
}
func (attemptTimeoutError) IsTransient() bool { return true }

// postAttempt performs one delivery attempt of an already-encoded payload:
// per-attempt deadline, request construction, transport call, response
// cleanup, and strict delivery validation. It is the retry callback post
// hands to httpx.Do, which owns the retry policy and terminal wrapping.
// Every error it returns is URL-free by CONSTRUCTION rather than by
// filtering: a transport error is reduced to its cause by logSafe, and a
// rejected response contributes only statusDetail's numbers and knell's own
// wording for them. The reduction happens here rather than in post's logSafe
// because httpx.Do logs each attempt's error before post ever sees it.
func (d *Discord) postAttempt(ctx context.Context, body []byte) (struct{}, error) {
	attemptCtx, cancel := httpx.ContextWithDefaultTimeout(ctx, d.attemptTimeout)
	defer cancel()
	req, reqErr := http.NewRequestWithContext(attemptCtx, http.MethodPost, d.url, bytes.NewReader(body))
	if reqErr != nil {
		// The raw error would embed the URL; report the cause only.
		return struct{}{}, fmt.Errorf("building webhook request: %w", logSafe(reqErr))
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
			// Carry the structurally reduced cause and the bound that fired:
			// a "dial tcp <host>:443: ..." stall and a bare "context
			// deadline exceeded" after the connection was established are
			// different incidents (egress/DNS blocked vs Discord answering
			// slowly), and this error is the whole incident record — httpx.Do
			// returns it verbatim on exhaustion and watch logs that at Error.
			// logSafe applies the same reduction the non-timeout path below
			// returns, so no rendering of the webhook URL survives.
			return struct{}{}, attemptTimeoutError{cause: logSafe(doErr).Error(), after: d.attemptTimeout}
		}
		// *url.Error embeds the full webhook URL; reduce it to its cause
		// (transient classification survives the reduction).
		return struct{}{}, logSafe(doErr)
	}
	defer httpx.DrainClose(resp.Body)
	// Success is exactly 2xx here: CheckHTTPStatus rejects every other
	// status, an unfollowed redirect's 3xx included (pinned by
	// TestUnfollowedRedirectIsNotDelivery), so the sweep keeps retrying a
	// non-delivery. Its error is typed, which is what lets httpx.Do
	// classify 502/503/504 as transient and retry within the call.
	if statusErr := httpx.CheckHTTPStatus(resp); statusErr != nil {
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			// A 3xx reaches a caller only because the hop was NOT followed:
			// New's policy hands back the 3xx for a method-changing hop
			// (http.ErrUseLastResponse), and net/http does not follow a 3xx
			// with no usable Location (a cross-host refusal never reaches
			// here — CheckRedirect's error surfaces on the doErr path above).
			// "HTTP 302" alone reads like a webhook-side rejection, so say
			// that nothing was delivered and what to point the URL at; the
			// specific reason the hop was not followed is not knowable here,
			// so the text does not claim one. The response body of an
			// unfollowed redirect is not diagnostic, and neither the Location
			// nor the request URL is included: for a webhook the path IS the
			// credential.
			return struct{}{}, fmt.Errorf(
				"%w: redirect or other 3xx response was not followed, nothing was delivered (point DISCORD_WEBHOOK_URL at an endpoint that accepts the POST with a 2xx response)",
				statusErr)
		}
		// The code alone cannot tell a deleted webhook from a rejected
		// payload; Discord names that difference in the body as a numeric
		// code, and statusDetail reports the code plus knell's own wording
		// for it. Wrapping keeps the typed error in the chain, so httpx.Do
		// still classifies 502/503/504 as transient and still finds
		// *RateLimitError for the 429 wait.
		//
		// The detail is built HERE, not by post's logSafe: httpx.Do logs a
		// transient attempt's error through the type-based LogSafeError only,
		// which passes this wrapped error through unchanged, so anything the
		// body's own text contributed would reach the retry and exhausted log
		// lines — and for a webhook whose edge echoes the request URI, that
		// text IS the credential. statusDetail therefore publishes numbers
		// and knell's words only. An empty body has nothing to add, and the
		// typed status error is returned unwrapped.
		if detail := statusDetail(resp.Body); detail != "" {
			return struct{}{}, fmt.Errorf("%w%s", statusErr, detail)
		}
		return struct{}{}, statusErr
	}
	return struct{}{}, nil
}

// statusDetail renders what a rejected response adds to its status code,
// using only numbers this package measured and words this package wrote.
//
// Discord answers a rejected webhook POST with a JSON error object whose
// numeric "code" field names the cause, and that number is the body's whole
// diagnostic value: a number cannot carry a credential. The object's own
// "message" string and nested "errors" object are authored by the other end
// and are never published, in any form — a webhook edge that echoes the
// request URI would otherwise put the credential (for a webhook the URL path
// IS the credential) into this error and into httpx.Do's log lines.
//
// Everything else about the body is reported as a fact ABOUT the body and
// never as its content: a body that is not Discord's error object, one past
// maxErrorBodyBytes, and one that did not arrive whole all report the byte
// count and that the detail was dropped. The empty string means the status is
// the whole verdict (an empty body), which the caller reports as the bare
// typed error.
func statusDetail(body io.Reader) string {
	// One byte past the cap, so an over-cap body is DETECTABLE instead of
	// silently truncated into a partial (and then unattributable) fragment.
	detail, readErr := io.ReadAll(io.LimitReader(body, maxErrorBodyBytes+1))
	switch {
	case readErr != nil:
		// A body that did not arrive whole is worth saying out loud: a bare
		// status reads as "the webhook explained nothing", while a response
		// that broke mid-body points at the path between here and Discord
		// rather than at Discord's verdict, and the status cannot tell them
		// apart. The partial bytes are dropped, and so is the read error's
		// own text — net/textproto renders a malformed trailer as "malformed
		// MIME header line: <remote bytes>", which is remote-authored like
		// any body. readFailure classifies it structurally instead.
		return fmt.Sprintf(" (response body unreadable after %d bytes, detail dropped: %s)",
			len(detail), readFailure(readErr))
	case len(detail) > maxErrorBodyBytes:
		return fmt.Sprintf(" (response body over %d bytes, detail dropped)", maxErrorBodyBytes)
	case len(detail) == 0:
		return ""
	}
	code, ok := discordErrorCode(detail)
	if !ok {
		return fmt.Sprintf(" (response body of %d bytes carried no Discord error code, detail dropped)", len(detail))
	}
	if meaning := discordCodeMeaning(code); meaning != "" {
		return fmt.Sprintf(": Discord error code %d (%s)", code, meaning)
	}
	// An unmapped code is still the one fact worth having — the operator can
	// look it up in Discord's error reference — and knell claims no meaning
	// it does not know.
	return fmt.Sprintf(": Discord error code %d", code)
}

// discordErrorCode reports the numeric error code of a rejected response
// body. Exactly one field is decoded: the surrounding object's text fields
// are remote-authored, so they are never bound to a variable this package
// formats. A body that is not a JSON object, or whose "code" is missing or
// not a number, reports no code, and the decode error is discarded rather
// than reported — encoding/json's message describes the input.
func discordErrorCode(body []byte) (int, bool) {
	var parsed struct {
		Code *int `json:"code"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil || parsed.Code == nil {
		return 0, false
	}
	return *parsed.Code, true
}

// discordCodeMeaning is knell's own wording for the Discord error codes an
// operator can act on; the empty string means knell knows no meaning for the
// code and reports the bare number. The mapped codes split exactly the way
// the response to them splits: 10015 and 50027 mean the webhook this knell
// posts to no longer accepts it, which only an operator can fix, while 50006
// and 50035 mean Discord rejected the payload knell built, which no
// configuration change helps. Values are phrased as that verdict rather than
// as a translation of Discord's message, which is never read.
func discordCodeMeaning(code int) string {
	switch code {
	case 10015:
		return "unknown webhook: it was deleted, or DISCORD_WEBHOOK_URL points at one that never existed - recreate the webhook and update the config"
	case 50027:
		return "invalid webhook token: DISCORD_WEBHOOK_URL's token does not match the webhook - recreate the webhook and update the config"
	case 50006:
		return "cannot send an empty message: knell built a payload with no content, which is a knell bug, not an operator problem"
	case 50035:
		return "invalid request body: Discord rejected the shape of knell's payload, which is a knell bug, not an operator problem"
	}
	return ""
}

// readFailure names why reading a rejected response's body failed, in knell's
// own words. The read error is classified structurally (errors.Is against a
// fixed set) and never rendered: io.ReadAll surfaces net/textproto's
// "malformed MIME header line: <remote bytes>" verbatim, so its text is
// remote-authored input, not a diagnosis. An unrecognized failure reports
// only that the read failed; the caller's byte count carries the rest.
func readFailure(err error) string {
	switch {
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "the connection closed before the body was complete"
	case errors.Is(err, context.DeadlineExceeded):
		return "the attempt deadline expired mid-body"
	case errors.Is(err, syscall.ECONNRESET):
		return "the connection was reset"
	}
	return "the read failed"
}
