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
	"strings"
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
// keeps a hostile or chatty body out of the log. A body that exceeds the cap
// is dropped whole rather than truncated: truncation can cut a webhook URL
// the body echoed, and a partial URL is a credential fragment no exact-value
// redaction can remove (see postAttempt).
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
// in the URL path). The raw configured string is only one of them: HTTP code
// renders req.URL.String(), which net/url canonicalizes — non-ASCII path bytes
// come back percent-escaped — so the canonical form can carry the credential
// without containing the configured string. The bare path forms cover an error
// that quotes only the path; credentialForms' fragments cover a remote body
// that echoes only the credential-bearing tail. The request-URI form
// (path?query), the raw query and each decoded query value cover a
// relay-style webhook whose credential rides in the query string (config
// accepts any https URL, so that shape is deployable): scrubbing only the
// path forms out of an echoed request-URI would leave its query behind.
// Redaction is plain replacement, so order matters: each form group runs
// longest-first. The two path encodings interleave rather than sorting into one
// descending list, so a fragment CAN fire inside text that also carried a
// longer rendering — safe only because every fragment is anchored at the END of
// the path, which makes even an out-of-order match remove the token instead of
// leaving it behind.
// Keep that anchoring when adding a candidate. Duplicates, the empty path, the
// bare "/" and any DERIVED form shorter than minCredentialCandidate are
// dropped: such a fragment appears in unrelated messages and redacting it would
// mangle them without hiding a secret. The complete path renderings are exempt
// from that floor — the path IS the credential, so a short configured path must
// still be scrubbed when a body echoes only it. An unparseable URL yields the
// raw value and its escaped rendering alone, which is all the text such an
// error embeds.
func redactionCandidates(webhookURL string) []string {
	candidates := []string{webhookURL}
	u, parseErr := url.Parse(webhookURL)
	if parseErr != nil {
		// The raw value is the only rendering such an error can embed — plus
		// its JSON slash-escaped form, for the same reason the escaped loop
		// below exists: a body that escapes "/" carries the credential in a
		// form the plain value does not match.
		if escaped := strings.ReplaceAll(webhookURL, "/", `\/`); escaped != webhookURL {
			candidates = append(candidates, escaped)
		}
		return candidates
	}
	// The configured string leads the list so the escaped loop below covers it
	// as well (the dedup loop then drops the plain form as already present).
	// u.RequestURI() is what a proxy error page echoes (path?query); it is
	// listed before the bare path forms so an echoed request-URI is removed
	// whole instead of leaving its query behind. RawQuery and the decoded
	// query values cover a body that echoes only the query; the length floor
	// below keeps a short non-secret value (?wait=true) from mangling log
	// text.
	forms := []string{webhookURL, u.String(), u.RequestURI(), u.Path, u.EscapedPath(), u.RawQuery}
	forms = append(forms, queryValueForms(u)...)
	for _, p := range []string{u.Path, u.EscapedPath()} {
		forms = append(forms, credentialForms(p)...)
	}
	completePaths := completePathForms(u)
	// A JSON error body commonly escapes "/" as "\/" (PHP json_encode does so
	// by default), which no plain-byte candidate matches. Register the escaped
	// rendering of every slash-bearing form; the loop runs over a snapshot so
	// the escaped forms are not re-escaped.
	for _, form := range slices.Clone(forms) {
		if escaped := strings.ReplaceAll(form, "/", `\/`); escaped != form {
			forms = append(forms, escaped)
		}
	}
	for _, c := range forms {
		// The length floor applies to every DERIVED form — the sub-path
		// fragments credentialForms builds and the query forms: a fragment too
		// short to be a usable credential is also too short to replace without
		// mangling unrelated text (the reason a bare "/" was rejected,
		// generalized). It cannot weaken the guarantee for a COMPLETE
		// rendering: the configured string is kept unconditionally above,
		// u.String() is never shorter than "https://h/", and the complete path
		// renderings (u.Path, u.EscapedPath() and their slash-escaped forms)
		// are exempt via completePaths, so a webhook whose whole path is a
		// handful of bytes is still scrubbed when a body echoes only that path.
		if c == "" || c == "/" || slices.Contains(candidates, c) {
			continue
		}
		if len(c) < minCredentialCandidate && !slices.Contains(completePaths, c) {
			continue
		}
		candidates = append(candidates, c)
	}
	return candidates
}

// webhookPathPrefix is the fixed part of a Discord webhook path; everything
// after it is the credential.
const webhookPathPrefix = "/api/webhooks/"

// queryValueForms lists the renderings of the query VALUES a body can echo
// without the surrounding query string: each decoded value, and each value's
// WIRE (percent-encoded) rendering. Both encodings are registered for the same
// reason the path has u.Path and u.EscapedPath(): a body echoing a value with
// escapable bytes carries it in a shape the other form does not match. A
// relay-style webhook can hold its credential in the query (config accepts any
// https URL), and the length floor in redactionCandidates drops the short
// non-secret values (?wait=true) these loops also collect.
func queryValueForms(u *url.URL) []string {
	var forms []string
	for _, values := range u.Query() {
		forms = append(forms, values...)
	}
	for pair := range strings.SplitSeq(u.RawQuery, "&") {
		if _, encValue, ok := strings.Cut(pair, "="); ok {
			forms = append(forms, encValue)
		}
	}
	return forms
}

// completePathForms lists the COMPLETE renderings of the path component —
// u.Path, u.EscapedPath() and their JSON slash-escaped forms. The path IS the
// credential, so these are exempt from redactionCandidates' length floor
// however short the configured path is (a body echoing only "/hook" echoes the
// whole secret). The empty path and the bare "/" are excluded: neither is a
// credential, and redacting "/" would mangle every path in a log line.
func completePathForms(u *url.URL) []string {
	var forms []string
	for _, p := range []string{u.Path, u.EscapedPath()} {
		if p == "" || p == "/" {
			continue
		}
		forms = append(forms, p)
		if escaped := strings.ReplaceAll(p, "/", `\/`); escaped != p {
			forms = append(forms, escaped)
		}
	}
	return forms
}

// minCredentialCandidate is the shortest fragment worth redacting. A handful
// of bytes cannot be a usable webhook credential (a real path carries a long
// channel id and token), while replacing such a fragment would mangle every
// unrelated log line containing it — the same reason a bare "/" is rejected.
const minCredentialCandidate = 8

// credentialForms lists the credential-bearing fragments of a webhook path
// that an error can carry WITHOUT the surrounding URL or the leading slash, in
// descending length: the path minus its leading slash, the suffix after the
// fixed /api/webhooks/ prefix (when present), and the final token segment. A
// proxy or webhook edge commonly reports only one of these (a body reading
// "upstream failed for <id>/<token>" is the whole credential), and an exact
// value-based redaction that knows only the complete renderings passes such
// text through untouched — into httpx.Do's per-attempt and exhaustion log
// lines and into the returned delivery error.
func credentialForms(p string) []string {
	forms := []string{strings.TrimPrefix(p, "/")}
	if suffix, ok := strings.CutPrefix(p, webhookPathPrefix); ok {
		forms = append(forms, suffix)
	}
	if i := strings.LastIndex(p, "/"); i >= 0 {
		forms = append(forms, p[i+1:])
	}
	return forms
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
// code only) plus the remote body when it arrived whole and fits the size
// cap, which postAttempt scrubs against the same candidates before wrapping
// it (an oversized or partially read body is dropped, never truncated).
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
type attemptTimeoutError struct {
	// cause is the SCRUBBED text of the transport error the expired deadline
	// produced, and after is the bound that expired. A string, not an error:
	// the type must still not unwrap to context.DeadlineExceeded (httpx
	// classifies that terminal before consulting IsTransient), and the text is
	// exactly what the non-timeout transport path already returns through
	// logSafe, so it carries no rendering of the webhook URL. The zero value
	// stays valid and renders the bare message.
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
// Every error it returns is URL-free — including the remote body detail on a
// status failure, which is scrubbed here rather than by post's logSafe,
// because httpx.Do logs the attempt error before post sees it. That detail
// is only ever a COMPLETE body under the cap; a truncated one is dropped.
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
			// Carry the scrubbed cause and the bound that fired: a
			// "dial tcp <host>:443: ..." stall and a bare "context deadline
			// exceeded" after the connection was established are different
			// incidents (egress/DNS blocked vs Discord answering slowly), and
			// this error is the whole incident record — httpx.Do returns it
			// verbatim on exhaustion and watch logs that at Error. logSafe
			// applies the same reduction the non-timeout path below returns,
			// so no rendering of the webhook URL survives.
			return struct{}{}, attemptTimeoutError{cause: d.logSafe(doErr).Error(), after: d.attemptTimeout}
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
		// payload; Discord names the cause in the body. Wrapping keeps the
		// typed error in the chain, so httpx.Do still classifies 502/503/504
		// as transient and still finds *RateLimitError for the 429 wait.
		// The excerpt is scrubbed HERE, not by post's logSafe: httpx.Do logs
		// a transient attempt's error through the type-based LogSafeError
		// only, which passes this wrapped error through unchanged, so a body
		// echoing the request path would otherwise carry the credential into
		// the retry and exhausted log lines. Scrub precedes Quote because
		// Quote escapes any byte that is not printable UTF-8, which would put
		// such a byte out of reach of an exact match.
		//
		// Redaction removes only text matching a candidate exactly — a
		// complete rendering or one of credentialForms' path fragments — so a
		// credential cut MID-VALUE matches nothing and must never be
		// published. Both ways to hold one drop the detail instead of
		// publishing it: a body past the cap (read cap+1 to detect it, since
		// a body echoing the request path across the cutoff leaves a
		// truncated path behind), and a read that failed midway (io.ReadAll
		// returns the partial bytes with the error).
		detail, readErr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes+1))
		switch {
		case readErr == nil && len(detail) > 0 && len(detail) <= maxErrorBodyBytes:
			return struct{}{}, fmt.Errorf("%w: %s", statusErr, strconv.Quote(d.scrubText(string(detail))))
		case readErr != nil:
			// The body did not arrive whole, so its bytes are unusable; say
			// so, or a bare status reads as "the webhook explained nothing".
			// Never the partial bytes themselves (a cut path is an
			// unscrubbable credential prefix), but the byte count and the
			// scrubbed read failure: a response that broke mid-body points at
			// the path between here and the webhook, not at the webhook's
			// verdict, and the status code alone cannot tell them apart.
			// Quote for the same reason the whole-body case does: the read
			// error can carry remote bytes verbatim (net/textproto reports a
			// malformed chunked trailer as "malformed MIME header line:
			// <remote bytes>"), and Quote neutralizes control characters
			// before the text reaches a log line.
			return struct{}{}, fmt.Errorf("%w (response body unreadable after %d bytes, detail dropped: %s)",
				statusErr, len(detail), strconv.Quote(d.scrubText(readErr.Error())))
		case len(detail) > maxErrorBodyBytes:
			return struct{}{}, fmt.Errorf("%w (response body over %d bytes, detail dropped)", statusErr, maxErrorBodyBytes)
		}
		return struct{}{}, statusErr
	}
	return struct{}{}, nil
}
