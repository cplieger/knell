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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"time"

	"github.com/cplieger/httpx/v4"
	"github.com/cplieger/knell/internal/watch"
)

// attemptTimeout bounds each delivery attempt. httpx.WithAttemptTimeout
// installs it inside the retry loop, so its expiry is retryable and the
// caller's own budget is never extended (a nearer caller deadline still
// governs).
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

// historyTimeFormat includes the date because queued history notices may arrive
// days after recovery, and the zone because readers may be outside the observer's timezone.
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
	if reason == watch.LateEndedBeforeDetection {
		return "This notice is late only because the outage ended before a sweep detected it - nothing was wrong with delivery."
	}
	return "This notice is late: its alert was still undelivered when the beat returned - check the webhook."
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
	ended := 0
	for _, o := range outages {
		if o.LateReason == watch.LateEndedBeforeDetection {
			ended++
		}
	}
	switch ended {
	case len(outages):
		return "Each ended before a sweep detected it - nothing was wrong with delivery."
	case 0:
		return "Their alerts were still undelivered when it returned - check the webhook."
	default:
		return fmt.Sprintf("%d had an undelivered alert (check the webhook), %d ended before a sweep detected it.",
			len(outages)-ended, ended)
	}
}

// post delivers one message, retrying transient failures. The webhook URL
// cannot appear in returned errors or logs, because no text the OTHER end
// authored is ever printed: every message this package publishes is written
// here (none of them interpolates d.url), and the two places remote text
// could enter are reduced structurally instead of filtered — a transport
// error through safeTransportError (the *url.Error that embeds the URL is
// unwrapped and the cause underneath is classified, never rendered) and a
// rejected response through statusDetail (Discord's numeric error code and
// knell's own wording for it, never the body's text).
func (d *Discord) post(ctx context.Context, label, content string) error {
	body, err := json.Marshal(map[string]string{"content": content})
	if err != nil {
		return fmt.Errorf("encoding webhook payload: %w", err)
	}
	_, err = httpx.Do(ctx, func(ctx context.Context) (struct{}, error) {
		return d.postAttempt(ctx, body)
	}, httpx.WithLabel("discord webhook "+label), httpx.WithMaxAttempts(maxAttempts),
		// Bound EACH attempt and make that bound's expiry retryable. httpx
		// classifies a bare context deadline as terminal (it cannot tell the
		// caller's budget from a per-attempt bound), so the bound has to be
		// installed by the loop that owns the retries: WithAttemptTimeout
		// derives the attempt context itself, marks only an expiry that fired
		// while the caller's context was still live, and keeps the deadline
		// visible to errors.Is — which is what makes the timeout classifiable
		// by httpx AND transparent to knell's own callers.
		httpx.WithAttemptTimeout(d.attemptTimeout),
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
// that embeds the full request URL, and returns everything else untouched. A
// *url.Error carrying NO cause reduces to httpx's own contentless stand-in
// rather than to nil (v4.2.0 fixed that at the source), so a real failure can
// never be reduced to a success signal here — nil in, nil out, and non-nil in,
// non-nil out.
//
// There is deliberately no string search-and-replace backstop: text-matching
// redaction can only defend text knell chose to publish, and this package
// publishes none (see post for that invariant).
//
// Stripping the wrapper leaves the CAUSE's text, which is why the transport
// path must NOT return this error as-is; safeTransportError, immediately
// below, names those causes and adds the classification step. This function
// is the reduction those callers share.
//
// The reduced error is returned as-is rather than re-wrapped, which keeps
// errors.Is/As intact: the sweep relies on it for context.Canceled and
// httpx.Do for transient classification. httpx.RedactSecret cannot be used
// here — it returns a bare errors.New and would break both.
//
// It is a thin wrapper over one library call and stays for two reasons: it is
// the named home of the invariant above (three call sites, one place to read
// why), and safeTransportError's reduction loop is written against its
// "reduce, never nil" contract.
func logSafe(err error) error {
	return httpx.LogSafeError(err)
}

// safeTransportError reports a failed transport call in knell's own words.
// It is what postAttempt returns for every error client.Do produces, and it
// exists because logSafe alone is not enough there: stripping the *url.Error
// wrapper leaves the cause's own TEXT, and two of net/http's causes are
// written from the response's Location header — a malformed one is rendered as
// `failed to parse Location header "<remote bytes>"`, and a refused hop as
// httpx's `refusing redirect to <remote host>`. Both are remote-authored, and
// an endpoint that answers with a redirect echoing the request URI would put
// the webhook path (which IS the credential) into httpx.Do's per-attempt logs
// and into the returned error.
//
// So the cause is CLASSIFIED, never rendered: transportPhrase maps it to one
// of knell's fixed phrases and the cause itself is reachable only through
// Unwrap. That keeps every consumer of the chain intact — watch's
// context.Canceled exemption and httpx's transient classification both use
// errors.Is/As, which traverse Unwrap without ever formatting the error.
func safeTransportError(err error) error {
	if err == nil {
		return nil
	}
	// Reduce until no *url.Error is left ANYWHERE in the chain, not just at
	// the top. httpx.LogSafeError searches the chain with errors.As and
	// RETURNS what it finds, so a url.Error still nested under the cause would
	// let it unwrap past this wrapper and render that error's cause instead of
	// the phrase below — in httpx.Do's attempt lines and in post's own
	// logSafe. The loop terminates: each pass either strips a url.Error or
	// (for one with a nil Err) substitutes httpx's contentless stand-in, which
	// is not a url.Error.
	// maxURLErrorDepth bounds the reduction. Real chains are one or two
	// levels deep (net/http wraps at most a redirect error inside a
	// transport error), and httpx.LogSafeError reduces strictly, so the
	// cap is never reached. It is here because the loop's termination
	// would otherwise rest entirely on the library's behavior: a value
	// that reduces to itself would spin this bare loop forever inside the
	// delivery attempt, which no context can interrupt, and knell's only
	// sender goroutine would stop notifying with /healthz still green.
	const maxURLErrorDepth = 8
	cause := logSafe(err)
	for range maxURLErrorDepth {
		var nested *url.Error
		if !errors.As(cause, &nested) {
			break
		}
		cause = logSafe(cause)
	}
	return transportError{phrase: transportPhrase(cause), cause: cause}
}

// transportError is a transport failure whose message is knell's alone.
// Error() renders the fixed phrase and NOTHING from the cause; Unwrap exposes
// the cause so errors.Is/As keep working. The pair is the whole point: the
// error stays classifiable by machines and unquotable by remote input.
type transportError struct {
	cause  error
	phrase string
}

func (e transportError) Error() string { return e.phrase }
func (e transportError) Unwrap() error { return e.cause }

// transportPhrase names why an attempt produced no response, choosing from a
// finite set of knell's own sentences. err is matched STRUCTURALLY (errors.Is
// against sentinel values, errors.As against types) and its text is never
// read, because a transport cause can be written from a response header — see
// safeTransportError. An unrecognized cause reports only that the transport
// failed; the stage suffix carries what is still knowable about it.
func transportPhrase(err error) string {
	var (
		dnsErr  *net.DNSError
		certErr *tls.CertificateVerificationError
		netErr  net.Error
	)
	switch {
	case errors.Is(err, context.Canceled):
		return "webhook delivery was canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "a deadline expired" + transportStage(err)
	case isProxyConnectError(err):
		return "the egress proxy could not be reached"
	case errors.As(err, &dnsErr):
		return "the webhook host could not be resolved"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "the webhook host refused the connection"
	case errors.Is(err, syscall.ECONNRESET):
		return "the connection to the webhook was reset"
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		// No route, not a refusal: the packet never left this host's
		// network. Named separately because the operator action is the
		// node's egress (routing, firewall, a missing IPv6 route), not
		// Discord and not DNS.
		return "no network route to the webhook host"
	case errors.As(err, &certErr):
		return "the webhook's TLS certificate could not be verified"
	case errors.As(err, &netErr) && netErr.Timeout():
		return "the webhook did not answer in time" + transportStage(err)
	case errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF):
		return "the connection closed before the webhook answered"
	}
	return "webhook transport failed" + transportStage(err)
}

// transportStage names WHICH stage of the attempt failed, from net.OpError's
// Op field. That field is one of net's own fixed verbs ("dial", "read",
// "write", or "proxyconnect" for a proxied attempt), so it is safe to print
// where the surrounding error text is not, and it carries the distinction that
// matters most during an outage: a stalled dial points at egress or DNS, while a
// stalled read means the webhook host accepted the connection and then went
// quiet.
//
// "proxyconnect" reaches here even though transportPhrase has a branch for it:
// a proxy dial that TIMES OUT (rather than being refused) matches the deadline
// branch first, which appends this suffix, so that failure reads "a deadline
// expired during proxyconnect" — the stage still names the proxy, which is why
// the order is left alone.
func transportStage(err error) string {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op != "" {
		return " during " + opErr.Op
	}
	return ""
}

// isProxyConnectError reports whether the failure happened while reaching the
// egress proxy rather than the webhook host. knell keeps
// http.DefaultTransport, which honors HTTPS_PROXY/HTTP_PROXY, and net/http
// wraps a proxy dial failure as *net.OpError{Op: "proxyconnect"} (a fixed
// literal of net/http, safe to match). The errno underneath (ECONNREFUSED, a
// DNS failure) would otherwise satisfy the webhook-host branches in
// transportPhrase and name the wrong endpoint — a confident wrong diagnosis
// sends the operator to Discord while their own proxy is what is down. The
// proxy's HOSTNAME is operator-supplied text and is NOT named, only net's own
// verb for the stage. errors.As finds the OUTERMOST *net.OpError, which for a
// proxied attempt is the proxyconnect wrapper, so an unproxied failure (whose
// first OpError is a "dial"/"read"/"write") is unaffected.
func isProxyConnectError(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "proxyconnect"
}

// postAttempt performs one delivery attempt of an already-encoded payload:
// request construction, transport call, and response cleanup, leaving the
// verdict on the response to deliveryError. It is the retry callback post
// hands to httpx.Do, which owns the retry policy, the per-attempt deadline
// (WithAttemptTimeout, so ctx already carries it) and terminal wrapping.
// Every error it returns is URL-free by CONSTRUCTION rather than by
// filtering: a transport error is reduced and classified by
// safeTransportError, and a rejected response contributes only statusDetail's
// numbers and knell's own wording for them. The reduction happens here rather
// than in post's logSafe because httpx.Do logs each attempt's error before
// post ever sees it.
func (d *Discord) postAttempt(ctx context.Context, body []byte) (struct{}, error) {
	req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(body))
	if reqErr != nil {
		// The raw error would embed the URL; report the cause only.
		return struct{}{}, fmt.Errorf("building webhook request: %w", logSafe(reqErr))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	resp, doErr := d.client.Do(req) //nolint:bodyclose // closed via the deferred httpx.DrainClose below
	if doErr != nil {
		// *url.Error embeds the full webhook URL and its cause can be written
		// from a response's Location header; report knell's own phrase for it.
		// The chain survives Unwrap, which is what both classifications read:
		// httpx's transient check, and its own per-attempt-deadline mark (the
		// expiry of the bound WithAttemptTimeout installed is retried, while
		// the caller's own expired budget stays terminal).
		return struct{}{}, safeTransportError(doErr)
	}
	// Drain up to 64 KiB and close, so the connection can be reused. The
	// library helper is safe to use directly as of httpx v4.2.1: its drain
	// logs a bare Debug line and no longer passes the body-read error VALUE
	// to it. That error's text is remote-authored (net/http renders a
	// malformed chunked trailer as `malformed MIME header: missing colon:
	// "<remote bytes>"`, and a webhook edge echoing the request URI puts the
	// path that IS the credential in those bytes), which is why knell carried
	// a local drain until the library dropped the attribute at the source.
	// TestSuccessfulDeliveryDrainsTheBodyWithoutLoggingItsReadError keeps
	// asserting it here, because a regression in httpx would reopen the leak
	// in knell's own log stream.
	defer httpx.DrainClose(resp.Body)
	return struct{}{}, deliveryError(resp)
}

// deliveryError reports what a response says about delivery: nil for a
// success, otherwise CheckHTTPStatus's typed error carrying whatever knell can
// safely add to it.
//
// Success is exactly 2xx: CheckHTTPStatus rejects every other status, an
// unfollowed redirect's 3xx included (pinned by
// TestUnfollowedRedirectIsNotDelivery), so the sweep keeps retrying a
// non-delivery. Its error is typed, and every return here keeps that type in
// the chain (%w), which is what lets httpx.Do classify 502/503/504 as
// transient and find *RateLimitError for the 429 wait.
//
// Nothing the other end authored is added. The detail is built HERE rather
// than by post's logSafe because httpx.Do logs each attempt's error through
// the type-based LogSafeError only, which passes a wrapped status error
// through unchanged — so anything the body's own text contributed would reach
// the retry and exhausted log lines, and for a webhook whose edge echoes the
// request URI that text IS the credential. statusDetail therefore publishes
// numbers and knell's words only, and an empty body adds nothing.
func deliveryError(resp *http.Response) error {
	statusErr := httpx.CheckHTTPStatus(resp)
	if statusErr == nil {
		return nil
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		// A 3xx reaches a caller only because the hop was NOT followed:
		// New's policy hands back the 3xx for a method-changing hop
		// (http.ErrUseLastResponse), and net/http does not follow a 3xx
		// with no usable Location (a cross-host refusal never reaches
		// here — CheckRedirect's error surfaces on the doErr path in
		// postAttempt). "HTTP 302" alone reads like a webhook-side
		// rejection, so say that nothing was delivered and what to point the
		// URL at; the specific reason the hop was not followed is not knowable
		// here, so the text does not claim one. The response body of an
		// unfollowed redirect is not diagnostic, and neither the Location nor
		// the request URL is included: for a webhook the path IS the
		// credential.
		return fmt.Errorf(
			"%w: redirect or other 3xx response was not followed, nothing was delivered (point DISCORD_WEBHOOK_URL at an endpoint that accepts the POST with a 2xx response)",
			statusErr)
	}
	// The code alone cannot tell a deleted webhook from a rejected payload;
	// Discord names that difference in the body as a numeric code, and
	// statusDetail reports the code plus knell's own wording for it.
	detail := statusDetail(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		// CheckHTTPStatus renders these two as "invalid API key (401)" and
		// "access denied (403)" — wording for a keyed API. knell sends no API
		// key: DISCORD_WEBHOOK_URL's own path and token ARE the credential,
		// which is why config refuses a non-https URL. Discord answers a
		// rotated webhook token with 401 + code 50027, but that code reaches
		// the operator only when the body arrives whole and under
		// maxErrorBodyBytes, so name the credential knell actually has and
		// the verdict stands on its own when the body contributes nothing.
		return fmt.Errorf(
			"%w%s (knell sends no API key - DISCORD_WEBHOOK_URL's own path and token are the credential: recreate the webhook and update the config)",
			statusErr, detail)
	}
	if detail != "" {
		return fmt.Errorf("%w%s", statusErr, detail)
	}
	return statusErr
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
// code and reports the bare number. The mapped codes split by what the
// operator can do about them: 10015 and 50027 mean the webhook this knell
// posts to no longer accepts it, which only an operator can fix; 50006 and
// 50035 mean Discord refused the payload knell built, which no configuration
// change helps — so both are reported as knell bugs, and neither names a
// setting for the operator to re-check: config validates every operator-set
// value at startup, so a rejected payload is knell's own doing and pointing
// at an input startup already accepted would only send the operator to
// inspect the wrong thing. Values are phrased as that verdict rather than as
// a translation of Discord's message, which is never read.
func discordCodeMeaning(code int) string {
	switch code {
	case 10015:
		return "unknown webhook: it was deleted, or DISCORD_WEBHOOK_URL points at one that never existed - recreate the webhook and update the config"
	case 50027:
		return "invalid webhook token: DISCORD_WEBHOOK_URL's token does not match the webhook - recreate the webhook and update the config"
	case 50006:
		return "cannot send an empty message: knell built a payload with no content, which is a knell bug, not an operator problem"
	case 50035:
		return "invalid request body: Discord rejected the payload knell built, which no configuration change helps, so this is a knell bug worth reporting"
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
