// Package notify delivers knell's transition notifications to a Discord
// webhook. It is the app's only outbound-network package and retries transient
// delivery failures via httpx.
//
// Wording lives here, not in the state machine: internal/watch decides WHICH
// transition happened and hands over its own types, and this package decides how
// an operator reads it. Every duration a notice reports comes from those types'
// DownFor, so the live and past-tense notices cannot measure a span differently.
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
	"strings"
	"syscall"
	"time"

	"github.com/cplieger/httpx/v4"
	"github.com/cplieger/knell/internal/watch"
)

// attemptTimeout bounds each delivery attempt. httpx.WithAttemptTimeout
// installs it inside the retry loop, so its expiry is retryable and the
// caller's own budget is never extended.
const attemptTimeout = 10 * time.Second

// MaxNodeNameBytes is the maximum UTF-8 byte length of NODE_NAME, enforced by
// internal/config. The node name is interpolated into EVERY notice, so an
// unbounded value makes Discord reject all of them: knell would start, accept
// beats and detect outages while no notice is ever delivered. Counting BYTES is
// conservative, UTF-8 bytes always being >= the character count.
const MaxNodeNameBytes = 256

// maxAttempts is the total delivery attempts per notification (httpx
// semantics: total, including the first).
const maxAttempts = 3

// rateLimitMaxWait caps how long ONE rate-limited attempt may park the sweep's
// single sender goroutine. httpx waits min(Retry-After, this ceiling) before a
// 429 retry, so this number -- never Discord's hint -- bounds the delay every
// OTHER beat's notice inherits from one rate-limited beat.
const rateLimitMaxWait = 30 * time.Second

// maxErrorBodyBytes caps how much of a rejected response's body is READ, and
// nothing about it is ever printed. Discord names the cause in that body as a
// numeric code, and knell reports the code plus its own wording for it -- never
// the body's own text, which is authored by the other end and can echo the
// webhook URL that IS the credential.
const maxErrorBodyBytes = 512

// userAgent identifies this client to Discord's edge. Go sends
// "Go-http-client/1.1" when the header is unset, which an edge or WAF in front
// of a webhook commonly refuses; that refusal would arrive as a non-transient
// 4xx the sweep re-posts forever. Discord's DiscordBot form is the bot-API rule
// and does not apply: webhook execution accepts any identifying agent.
const userAgent = "knell (https://github.com/cplieger/knell)"

// Discord posts plain-content messages to one Discord-compatible webhook.
type Discord struct {
	client *http.Client
	url    string
	// node is the observer name ALREADY escaped for Discord markdown: New escapes
	// it once because it is constant for the process. Escaping it again at a
	// render site publishes the backslashes instead of the name.
	node string
	// attemptTimeout bounds one delivery attempt, rateLimitMaxWait one rate-limit
	// retry wait. Fields only so a test can shorten them on its own notifier.
	attemptTimeout   time.Duration
	rateLimitMaxWait time.Duration
}

// Discord implements the transition contract the state machine consumes; the
// assertion keeps a signature drift a notify-local compile error.
var _ watch.Notifier = (*Discord)(nil)

// New builds a Discord notifier for the given webhook URL. node names this
// observer instance in every message so multi-node deployments read as
// distinct reports.
func New(webhookURL, node string) *Discord {
	// Client timeout above the per-attempt context timeout so the
	// context is the effective per-attempt bound.
	client := httpx.NewClient(attemptTimeout + 5*time.Second)
	// Redirect policy: follow only a same-host hop, and never one net/http would
	// rewrite to another method -- the webhook POST must not be replayed as a
	// bodyless GET. The Referer deletion is separate: net/http writes the
	// PREVIOUS request's full URL there, and for a webhook the path IS the
	// credential.
	policy := httpx.RedirectPolicyFunc(httpx.WithSameHost(), httpx.WithPreserveMethod())
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		req.Header.Del("Referer")
		return policy(req, via)
	}
	return &Discord{
		client:           client,
		url:              webhookURL,
		node:             escapeMarkdown(node),
		attemptTimeout:   attemptTimeout,
		rateLimitMaxWait: rateLimitMaxWait,
	}
}

// Close releases idle connections. Call once on shutdown.
func (d *Discord) Close() {
	d.client.CloseIdleConnections()
}

// BeatMissing announces that a beat's deadline of silence has passed. The
// wording names what to check without presuming the beat ever pinged: a beat
// configured in BEATS but never wired to a sender fires this notice one deadline
// after start, and notify cannot tell that from a sender that pinged for weeks
// and stopped, watch.Transition's Started collapsing "last accepted ping" and
// the process-start baseline into one field.
func (d *Discord) BeatMissing(ctx context.Context, id string, live watch.Transition) error {
	msg := fmt.Sprintf(
		"🚨 [knell %s] beat **%s** MISSING: silent for %s. Nothing has pinged it in time: check the sender, its path to this observer, and that anything is pinging this beat id at all.",
		d.node, escapeMarkdown(id), live.DownFor().Truncate(time.Second),
	)
	return d.post(ctx, "missing "+id, msg)
}

// BeatRecovered announces the first ping after a missing alert.
func (d *Discord) BeatRecovered(ctx context.Context, id string, live watch.Transition) error {
	msg := fmt.Sprintf(
		"✅ [knell %s] beat **%s** recovered: pings arriving again after %s of silence.",
		d.node, escapeMarkdown(id), live.DownFor().Truncate(time.Second),
	)
	return d.post(ctx, "recovered "+id, msg)
}

// historyTimeFormat includes the date because queued history notices may arrive
// days after recovery, and the zone because readers may be outside the
// observer's timezone. The instant is always converted to UTC before
// formatting, so the recovery point correlates with knell's own log lines and
// knell_beat_last_seen_timestamp_seconds.
const historyTimeFormat = "2006-01-02 15:04 MST"

// BeatOutageHistory announces outages that were already over by the time this
// observer could send anything about them, in one past-tense message so a
// resolved incident never reads as a new live failure. One outage is reported on
// its own; several are summarized. The batch's shape is the caller's contract:
// watch asserts every record ended, has a start, and ascends by recovery point,
// so a refusal here would keep the records queued forever and present a producer
// bug as a webhook one.
func (d *Discord) BeatOutageHistory(ctx context.Context, id string, outages []watch.Outage) error {
	return d.post(ctx, "history "+id, d.historyMessage(id, outages))
}

// historyMessage renders the history notice for id. outages is non-empty and
// ascends by recovery point (both asserted by watch), so the last entry is the
// most recent recovery. Every notice is two parts: WHAT happened, then why it is
// being read after the fact. The second part comes from watch's LateReason and is
// never guessed here, because on two of its three reasons a webhook check finds
// nothing wrong.
func (d *Discord) historyMessage(id string, outages []watch.Outage) string {
	last := outages[len(outages)-1]
	recovered := last.Recovered.UTC().Format(historyTimeFormat)
	name := escapeMarkdown(id)
	if len(outages) == 1 {
		return fmt.Sprintf(
			"🕓 [knell %s] beat **%s** was missing for %s, recovered at %s. %s",
			d.node, name, last.DownFor().Truncate(time.Second), recovered, lateClause(last.LateReason),
		)
	}
	return fmt.Sprintf(
		"🕓 [knell %s] beat **%s** had %d outages: longest %s, last recovered at %s. %s",
		d.node, name, len(outages),
		watch.LongestOutage(outages).Truncate(time.Second), recovered, batchLateClause(outages),
	)
}

// markdownEscaper neutralizes the markup Discord's renderer consumes, in two
// ways. Every ESCAPED entry is a Discord formatting character, which matters:
// Discord strips a backslash only in front of one of its own markup characters,
// so escaping anything else would publish the backslash. Line breaks are
// COLLAPSED for that same reason. The masked-link delimiters are in the set
// because NODE_NAME is only trimmed and byte-capped.
var markdownEscaper = strings.NewReplacer(
	// Line breaks first, and collapsed rather than escaped: Discord's heading,
	// blockquote and list markup is LINE-ANCHORED, so only removing the break can
	// suppress it, and NODE_NAME is the one value that can carry one. A space is
	// never wider than the break it replaces, so MaxNodeNameBytes still holds.
	"\r\n", " ",
	"\r", " ",
	"\n", " ",
	`\`, `\\`,
	"*", `\*`,
	"_", `\_`,
	"~", `\~`,
	"`", "\\`",
	"|", `\|`,
	"[", `\[`,
	"]", `\]`,
)

// escapeMarkdown renders s literally in a Discord message. It is applied to the
// two values a notice interpolates that knell did not write itself, because
// Discord's markdown EATS characters rather than merely styling them: a pair of
// underscores italicizes what sits between them and removes both, so an id
// configured as "db_backup_nightly" arrives as "dbbackupnightly", matching
// nothing in BEATS and nothing on /beat/{id}.
func escapeMarkdown(s string) string {
	return markdownEscaper.Replace(s)
}

// lateClause explains why ONE ended outage is reported after the fact. Each of
// watch's three reasons gets its own sentence, because two of them must steer
// the operator AWAY from the webhook: only LateUndelivered means a send was
// ATTEMPTED and refused. An unrecognized reason falls to that sentence, matching
// watch's zero value: a notice that cannot tell would rather point at a healthy
// webhook than vouch for it.
func lateClause(reason watch.LateReason) string {
	switch reason {
	case watch.LateEndedBeforeDetection:
		return "This notice is late only because the outage ended before a sweep detected it - nothing was wrong with delivery."
	case watch.LateSchedulerDeferred:
		return "This notice is late because this observer deferred the alert to a later sweep and the beat came back first - no delivery was attempted, so the webhook is not the place to look."
	default:
		return "This notice is late because delivery was delayed - check the webhook."
	}
}

// batchLateClause explains why a whole run of ended outages is reported after
// the fact. A batch can MIX all three of watch's reasons -- how a flapping beat
// behaves during a Discord outage -- so a mixed batch reports EVERY non-zero
// count instead of picking a majority and stating something false about the rest.
// One refused delivery is reason enough to look at the webhook; the other counts
// stop that number from reading as that many webhook failures.
func batchLateClause(outages []watch.Outage) string {
	var undelivered, deferred, ended int
	for _, o := range outages {
		switch o.LateReason {
		case watch.LateEndedBeforeDetection:
			ended++
		case watch.LateSchedulerDeferred:
			deferred++
		default:
			// The zero reason counts as undelivered, like watch.LateUndelivered
			// being the zero value: a batch naming no reason blames delivery.
			undelivered++
		}
	}
	// A batch that names ONE reason reads better as a statement about the whole
	// batch than as a count of itself.
	switch total := len(outages); {
	case undelivered == total:
		return "Delivery was delayed for every outage - check the webhook."
	case deferred == total:
		return "Every alert was deferred to a later sweep and each beat came back first - no delivery was attempted, so the webhook is not the place to look."
	case ended == total:
		return "Each ended before a sweep detected it - nothing was wrong with delivery."
	}
	// Mixed, so every non-zero count is named. Delivery leads because it is the
	// only actionable one, which keeps the rest starting with their own digit.
	clauses := make([]string, 0, 3)
	if undelivered > 0 {
		clauses = append(clauses, fmt.Sprintf("Delivery was delayed for %d (check the webhook)", undelivered))
	}
	if deferred > 0 {
		clauses = append(clauses, fmt.Sprintf("%d deferred to a later sweep with nothing attempted", deferred))
	}
	if ended > 0 {
		clauses = append(clauses, fmt.Sprintf("%d ended before a sweep detected it", ended))
	}
	return strings.Join(clauses, "; ") + "."
}

// post delivers one message, retrying transient failures. The webhook URL cannot
// appear in returned errors or logs, because no text the OTHER end authored is
// ever printed: the two places remote text could enter are reduced structurally
// instead of filtered -- a transport error through safeTransportError and a
// rejected response through statusDetail.
func (d *Discord) post(ctx context.Context, label, content string) error {
	// allowed_mentions with an EMPTY parse list is the only structural way to
	// keep a notice from pinging anyone: the interpolated values are not filtered
	// for mention tokens, and escapeMarkdown cannot be -- a backslash before "@"
	// is not one of Discord's escapes.
	body, err := json.Marshal(map[string]any{
		"content":          content,
		"allowed_mentions": map[string][]string{"parse": {}},
	})
	if err != nil {
		return fmt.Errorf("encoding webhook payload: %w", err)
	}
	_, err = httpx.Do(ctx, func(ctx context.Context) (struct{}, error) {
		return d.postAttempt(ctx, body)
	}, httpx.WithLabel("discord webhook "+label), httpx.WithMaxAttempts(maxAttempts),
		// Bound EACH attempt and make that bound's expiry retryable: httpx
		// classifies a bare context deadline as terminal, unable to tell the
		// caller's budget from a per-attempt bound.
		httpx.WithAttemptTimeout(d.attemptTimeout),
		httpx.WithRateLimitRetry(d.rateLimitMaxWait),
		// watch publishes the terminal verdict for every failed delivery, so
		// httpx's own exhaustion WARN is a second, thinner line for one event.
		// Debug keeps it for diagnosis without the alarm.
		httpx.WithExhaustedLevel(slog.LevelDebug))
	if err != nil {
		return fmt.Errorf("delivering %s notification: %w", label, httpx.LogSafeError(err))
	}
	return nil
}

// safeTransportError reports a failed transport call in knell's own words,
// because httpx.LogSafeError alone is not enough: stripping the *url.Error
// wrapper leaves the cause's own TEXT, and two of net/http's causes are written
// from the response's Location header, so an endpoint answering with a redirect
// that echoes the request URI would put the webhook path (the credential) into
// the logs. The cause is CLASSIFIED, never rendered, and reachable only through
// Unwrap, which keeps every errors.Is/As consumer intact.
func safeTransportError(err error) error {
	// Reduce until no *url.Error is left ANYWHERE in the chain: LogSafeError
	// searches with errors.As and RETURNS what it finds, so a nested one would let
	// it unwrap past this wrapper. maxURLErrorDepth bounds the loop because a
	// value that reduced to itself would spin forever inside the attempt,
	// uninterruptible, with knell's only sender silent and /healthz still green.
	const maxURLErrorDepth = 8
	cause := httpx.LogSafeError(err)
	for range maxURLErrorDepth {
		var nested *url.Error
		if !errors.As(cause, &nested) {
			break
		}
		cause = httpx.LogSafeError(cause)
	}
	var unreduced *url.Error
	if errors.As(cause, &unreduced) {
		// The cap was reached with a *url.Error still in the chain, so the URL is
		// still reachable. Fail closed: publish the phrase with no cause, at the
		// cost of the classification httpx and watch read off the chain, so the
		// attempt is terminal and the sweep retries. Logged because that phrase
		// is also transportPhrase's unrecognized-cause phrase.
		slog.Warn("webhook transport error still carried a URL after the reduction cap, "+
			"diagnosis dropped to protect the credential and this attempt is terminal",
			"reductions", maxURLErrorDepth)
		return transportError{phrase: "webhook transport failed", cause: nil}
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
// finite set of knell's own sentences. err is matched STRUCTURALLY and its text
// is never read, because a transport cause can be written from a response header.
// An unrecognized cause reports only that the transport failed.
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
		// No route, not a refusal: the packet never left this host's network.
		// Named separately because the operator action is the node's egress, not
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

// transportStage names WHICH stage of the attempt failed, from net.OpError's Op
// field. That field is one of net's own fixed verbs, so it is safe to print
// where the surrounding error text is not, and it carries the distinction that
// matters during an outage: a stalled dial points at egress or DNS, a stalled
// read means the host accepted the connection and went quiet. "proxyconnect"
// reaches here despite transportPhrase's own branch, a proxy dial that TIMES OUT
// matching the deadline branch first.
func transportStage(err error) string {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op != "" {
		return " during " + opErr.Op
	}
	return ""
}

// isProxyConnectError reports whether the failure happened while reaching the
// egress proxy rather than the webhook host. net/http wraps a proxy dial failure
// as *net.OpError{Op: "proxyconnect"}, and the errno underneath would otherwise
// satisfy transportPhrase's webhook-host branches -- a confident wrong diagnosis
// sending the operator to Discord while their own proxy is down. errors.As finds
// the OUTERMOST *net.OpError, so an unproxied failure is unaffected.
func isProxyConnectError(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "proxyconnect"
}

// postAttempt performs one delivery attempt of an already-encoded payload:
// request construction, transport call, and response cleanup, leaving the
// verdict to deliveryError. It is the retry callback post hands to httpx.Do,
// which owns the retry policy and the per-attempt deadline. Every error it
// returns is URL-free by CONSTRUCTION, and the reduction happens here rather
// than in post because httpx.Do logs each attempt's error first.
func (d *Discord) postAttempt(ctx context.Context, body []byte) (struct{}, error) {
	req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(body))
	if reqErr != nil {
		// The raw error would embed the URL; report the cause only.
		return struct{}{}, fmt.Errorf("building webhook request: %w", httpx.LogSafeError(reqErr))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	resp, doErr := d.client.Do(req) //nolint:bodyclose // closed via the deferred httpx.DrainClose below
	if doErr != nil {
		// *url.Error embeds the full webhook URL and its cause can be written
		// from a response's Location header; report knell's own phrase for it.
		// The chain survives Unwrap, which is what both classifications read.
		return struct{}{}, safeTransportError(doErr)
	}
	// Drain up to 64 KiB and close, so the connection can be reused. The library
	// helper is safe to use directly as of httpx v4.2.1: its drain no longer
	// passes the body-read error VALUE to the log, and that text is
	// remote-authored -- an edge echoing the request URI puts the credential in it.
	defer httpx.DrainClose(resp.Body)
	return struct{}{}, deliveryError(resp)
}

// deliveryError reports what a response says about delivery: nil for a success,
// otherwise CheckHTTPStatus's typed error carrying whatever knell can safely add.
// Success is exactly 2xx, an unfollowed redirect's 3xx included, so the sweep
// keeps retrying a non-delivery. Every return keeps its typed error in the chain
// (%w), which lets httpx.Do classify 502/503/504 as transient and find
// *RateLimitError for the 429 wait. The detail is built HERE because httpx.Do
// logs each attempt's error through the type-based LogSafeError only.
func deliveryError(resp *http.Response) error {
	statusErr := httpx.CheckHTTPStatus(resp)
	if statusErr == nil {
		return nil
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		// A 3xx reaches a caller only because the hop was NOT followed. "HTTP 302"
		// alone reads like a webhook-side rejection, so say that nothing was
		// delivered and what to point the URL at. Neither the Location nor the
		// request URL is included: for a webhook the path IS the credential.
		return fmt.Errorf(
			"%w: redirect or other 3xx response was not followed, nothing was delivered (point DISCORD_WEBHOOK_URL at an endpoint that accepts the POST with a 2xx response)",
			statusErr)
	}
	// The code alone cannot tell a deleted webhook from a rejected payload;
	// Discord names that difference in the body as a numeric code.
	detail := statusDetail(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		// CheckHTTPStatus renders these two as wording for a keyed API. knell
		// sends no API key: DISCORD_WEBHOOK_URL's own path and token ARE the
		// credential. %w cannot be used, since it would carry that text into the
		// message this correction exists to replace.
		return &webhookCredentialError{cause: statusErr, detail: detail, status: resp.StatusCode}
	}
	if detail != "" {
		return fmt.Errorf("%w%s", statusErr, detail)
	}
	return statusErr
}

// webhookCredentialError reports a 401/403 in knell's own words while keeping
// httpx's typed status error reachable for errors.As. Its Error text is
// entirely knell-owned on purpose: the httpx wording it replaces describes a
// keyed API, and knell sends no API key — DISCORD_WEBHOOK_URL's own path and
// token are the credential.
type webhookCredentialError struct {
	cause  error
	detail string
	status int
}

func (e *webhookCredentialError) Error() string {
	return fmt.Sprintf(
		"HTTP %d: DISCORD_WEBHOOK_URL's path/token credential was rejected%s (knell sends no API key: recreate the webhook and update the config)",
		e.status, e.detail)
}

func (e *webhookCredentialError) Unwrap() error { return e.cause }

// statusDetail renders what a rejected response adds to its status code, using
// only numbers this package measured and words this package wrote. Discord
// answers a rejected POST with a JSON error object whose numeric "code" field
// names the cause, and that number is the body's whole diagnostic value: a
// number cannot carry a credential. The object's text fields are authored by the
// other end and never published; everything else is reported as a fact ABOUT the
// body. The empty string means the status is the whole verdict.
func statusDetail(body io.Reader) string {
	// One byte past the cap, so an over-cap body is DETECTABLE instead of
	// silently truncated into a partial (and then unattributable) fragment.
	detail, readErr := io.ReadAll(io.LimitReader(body, maxErrorBodyBytes+1))
	switch {
	case readErr != nil:
		// A body that did not arrive whole is worth saying out loud: a bare status
		// reads as "the webhook explained nothing", while a response that broke
		// mid-body points at the path between here and Discord. The read error's
		// own text is remote-authored, so readFailure classifies it instead.
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

// discordErrorCode reports the numeric error code of a rejected response body.
// Exactly one field is decoded: the surrounding object's text fields are
// remote-authored, so they are never bound to a variable this package formats. A
// body that is not a JSON object, or whose "code" is missing, reports no code,
// and the decode error is discarded: encoding/json's message describes the input.
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
// code and reports the bare number. The mapped codes split by what the operator
// can do: 10015 and 50027 mean the webhook no longer accepts this knell, which
// only an operator can fix, while 50006 and 50035 mean Discord refused the
// payload knell built, so both are reported as knell bugs and neither names a
// setting to re-check.
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
// own words. The read error is classified structurally and never rendered:
// io.ReadAll surfaces net/textproto's "malformed MIME header line: <remote
// bytes>" verbatim, so its text is remote-authored input, not a diagnosis.
func readFailure(err error) string {
	switch {
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "the connection closed before the body was complete"
	case errors.Is(err, context.Canceled):
		// Shutdown, not a fault on the path to Discord: the sweep's context was
		// canceled while the rejected response's body was being read.
		return "delivery was canceled before the body was complete"
	case errors.Is(err, context.DeadlineExceeded):
		return "the attempt deadline expired mid-body"
	case errors.Is(err, syscall.ECONNRESET):
		return "the connection was reset"
	}
	return "the read failed"
}
