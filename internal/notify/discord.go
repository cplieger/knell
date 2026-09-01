// Package notify delivers knell's transition notifications to a Discord
// webhook. It is the app's only outbound-network package and retries transient
// delivery failures via httpx. internal/watch decides which transition
// happened; this package decides how an operator reads it.
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
	"net/url"
	"strings"
	"time"

	"github.com/cplieger/httpx/v5"
	"github.com/cplieger/knell/internal/watch"
)

// attemptTimeout bounds each delivery attempt inside httpx's retry loop, so
// its expiry is retryable rather than extending the caller's own budget.
const attemptTimeout = 10 * time.Second

// MaxNodeNameBytes is the maximum UTF-8 byte length of NODE_NAME, enforced by
// internal/config. The name is interpolated into every notice, so an
// unbounded value would make Discord reject all of them.
const MaxNodeNameBytes = 256

// maxAttempts is the total delivery attempts per notification (httpx
// semantics: total, including the first).
const maxAttempts = 3

// sendBudget is the total wall time one delivery may spend, derived from the
// per-attempt knobs so changing either cannot leave it stale.
const sendBudget = maxAttempts*attemptTimeout + rateLimitMaxWait

// rateLimitMaxWait caps one rate-limited attempt's wait, and is the wait used
// whenever a 429 carries no positive Retry-After header.
const rateLimitMaxWait = 30 * time.Second

// maxErrorBodyBytes caps how much of a rejected response's body is read.
// Nothing from it is ever printed except the numeric "code" field: the rest
// is remote-authored and can echo the webhook URL, which is the credential.
const maxErrorBodyBytes = 512

// userAgent identifies this client to Discord's edge; an unset User-Agent
// (Go's default) is commonly refused by an edge or WAF in front of a webhook.
const userAgent = "knell (https://github.com/cplieger/knell)"

// Discord posts plain-content messages to one Discord-compatible webhook.
type Discord struct {
	client *http.Client
	url    string
	// node is already escaped for Discord markdown; escaping it again at a
	// render site would publish the backslashes instead of the name.
	node string
	// Fields only so a test can shorten them.
	attemptTimeout   time.Duration
	rateLimitMaxWait time.Duration
	sendBudget       time.Duration
}

var _ watch.Notifier = (*Discord)(nil)

// New builds a Discord notifier for the given webhook URL. node names this
// observer instance in every message so multi-node deployments read as
// distinct reports.
func New(webhookURL, node string) *Discord {
	client := httpx.NewClient(attemptTimeout + 5*time.Second)
	// Follow only a same-host hop, never one net/http would rewrite to another
	// method: the webhook POST must not be replayed as a bodyless GET. Referer
	// is deleted separately because net/http writes the previous request's full
	// URL there, and for a webhook the path is the credential.
	policy := httpx.RedirectPolicyFunc(httpx.WithSameHost(true), httpx.WithPreserveMethod(true))
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
		sendBudget:       sendBudget,
	}
}

// Close releases idle connections. Call once on shutdown.
func (d *Discord) Close() {
	d.client.CloseIdleConnections()
}

// BeatMissing announces that a beat's deadline of silence has passed. The
// wording names what to check without presuming the beat ever pinged, since
// watch.Transition cannot distinguish "never wired up" from "pinged for weeks
// and stopped".
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

// historyTimeFormat includes the date because a queued notice may arrive days
// after recovery; times are always converted to UTC first so they correlate
// with knell_beat_last_seen_timestamp_seconds.
const historyTimeFormat = "2006-01-02 15:04 MST"

// BeatOutageHistory announces outages already over by the time this observer
// could send anything, in one past-tense message per call. outages must be
// non-empty and ascend by recovery point (guaranteed by watch).
func (d *Discord) BeatOutageHistory(ctx context.Context, id string, outages []watch.Outage) error {
	return d.post(ctx, "history "+id, d.historyMessage(id, outages))
}

// historyMessage renders the history notice for id. The lateness clause comes
// from watch's delivery blame rather than being guessed here.
func (d *Discord) historyMessage(id string, outages []watch.Outage) string {
	last := outages[len(outages)-1]
	recovered := last.Recovered.UTC().Format(historyTimeFormat)
	name := escapeMarkdown(id)
	if len(outages) == 1 {
		return fmt.Sprintf(
			"🕓 [knell %s] beat **%s** was missing for %s, recovered at %s. %s",
			d.node, name, last.DownFor().Truncate(time.Second), recovered, lateClause(last.Undelivered),
		)
	}
	return fmt.Sprintf(
		"🕓 [knell %s] beat **%s** had %d outages: longest %s, last recovered at %s. %s",
		d.node, name, len(outages),
		watch.LongestOutage(outages).Truncate(time.Second), recovered, batchLateClause(outages),
	)
}

// markdownEscaper neutralizes Discord's markup. Every escaped entry is a
// Discord formatting character; Discord strips a backslash only in front of
// one of its own, so escaping anything else would publish the backslash.
var markdownEscaper = strings.NewReplacer(
	// Line breaks are collapsed rather than escaped: Discord's heading,
	// blockquote and list markup is line-anchored, so only removing the break
	// suppresses it. A space is never wider than the break it replaces, so
	// MaxNodeNameBytes still holds.
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

// escapeMarkdown renders s literally in a Discord message. Discord's markdown
// eats characters rather than merely styling them: a pair of underscores
// italicizes and removes both, so "db_backup_nightly" would otherwise arrive
// as "dbbackupnightly", matching nothing in BEATS.
func escapeMarkdown(s string) string {
	return markdownEscaper.Replace(s)
}

// lateClause explains why one ended outage is reported after the fact.
func lateClause(undelivered bool) string {
	if undelivered {
		return "This notice is late because delivery was delayed - check the webhook."
	}
	return "This notice is late because no delivery was ever attempted for it - the webhook is not the place to look."
}

// batchLateClause explains why a whole run of ended outages is reported after
// the fact. A mixed batch names both counts rather than picking a majority and
// stating something false about the rest.
func batchLateClause(outages []watch.Outage) string {
	var undelivered int
	for _, o := range outages {
		if o.Undelivered {
			undelivered++
		}
	}
	switch total := len(outages); undelivered {
	case total:
		return "Delivery was delayed for every outage - check the webhook."
	case 0:
		return "No delivery was ever attempted for any of them - the webhook is not the place to look."
	default:
		return fmt.Sprintf(
			"Delivery was delayed for %d (check the webhook); %d had nothing attempted.",
			undelivered, total-undelivered,
		)
	}
}

// post delivers one message, retrying transient failures. The webhook URL
// never appears in returned errors or logs: the two places remote text could
// enter are reduced structurally rather than filtered (safeTransportError,
// statusDetail).
func (d *Discord) post(ctx context.Context, label, content string) error {
	// An empty allowed_mentions parse list is the only structural way to keep a
	// notice from pinging anyone, since escapeMarkdown cannot filter mentions.
	body, _ := json.Marshal(map[string]any{
		"content":          content,
		"allowed_mentions": map[string][]string{"parse": {}},
	})
	ctx, cancel := httpx.ContextWithDefaultTimeout(ctx, d.sendBudget)
	defer cancel()
	_, err := httpx.Do(ctx, func(ctx context.Context) (struct{}, error) {
		return d.postAttempt(ctx, body)
	}, httpx.WithLabel("discord webhook "+label), httpx.WithMaxAttempts(maxAttempts),
		// Retryable so httpx does not classify this bound's expiry as terminal.
		httpx.WithAttemptTimeout(d.attemptTimeout),
		httpx.WithRateLimitRetry(d.rateLimitMaxWait),
		// watch already logs the terminal verdict; keep httpx's own line at Debug.
		httpx.WithExhaustedLevel(slog.LevelDebug))
	if err != nil {
		return fmt.Errorf("delivering %s notification: %w", label, err)
	}
	return nil
}

// safeTransportError reports a failed transport call in knell's own words.
// httpx.LogSafeError alone is not enough: two of net/http's causes are written
// from the response's Location header, so a redirecting endpoint could put the
// webhook path into the logs. The cause is classified, never rendered, and
// reachable only through Unwrap.
func safeTransportError(err error) error {
	// Reduce until no *url.Error is left anywhere in the chain: LogSafeError
	// returns what errors.As finds, so a nested one would unwrap past this
	// wrapper otherwise. Bounded so a self-reducing value cannot spin forever.
	const maxURLErrorDepth = 8
	cause := httpx.LogSafeError(err)
	for range maxURLErrorDepth {
		if _, ok := errors.AsType[*url.Error](cause); !ok {
			break
		}
		cause = httpx.LogSafeError(cause)
	}
	// net.OpError's Op is one of net's own fixed verbs, so it is safe to print;
	// it distinguishes a stalled dial (egress/DNS) from a stalled read (host
	// went quiet after accepting).
	phrase := "webhook transport failed"
	if opErr, ok := errors.AsType[*net.OpError](cause); ok && opErr.Op != "" {
		phrase += " during " + opErr.Op
	}
	return transportError{phrase: phrase, cause: cause}
}

// transportError is a transport failure whose message is knell's alone.
// Error() renders the fixed phrase and nothing from the cause; Unwrap exposes
// the cause so errors.Is/As keep working.
type transportError struct {
	cause  error
	phrase string
}

func (e transportError) Error() string { return e.phrase }
func (e transportError) Unwrap() error { return e.cause }

// postAttempt performs one delivery attempt: request construction, transport
// call, and response cleanup, leaving the verdict to deliveryError. Every
// error it returns is URL-free by construction.
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
		return struct{}{}, safeTransportError(doErr)
	}
	// Drain up to 64 KiB and close so the connection can be reused.
	defer httpx.DrainClose(resp.Body)
	return struct{}{}, deliveryError(resp)
}

// deliveryError reports what a response says about delivery: nil for success
// (exactly 2xx), otherwise CheckHTTPStatus's typed error plus whatever knell
// can safely add, so httpx.Do can still classify 502/503/504 and the 429 wait.
func deliveryError(resp *http.Response) error {
	statusErr := httpx.CheckHTTPStatus(resp)
	if statusErr == nil {
		return nil
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		// A 3xx reaches here only because the hop was not followed; neither the
		// Location nor the request URL is included, since for a webhook the
		// path is the credential.
		return fmt.Errorf(
			"%w: redirect or other 3xx response was not followed, nothing was delivered (point DISCORD_WEBHOOK_URL at an endpoint that accepts the POST with a 2xx response)",
			statusErr,
		)
	}
	detail := statusDetail(resp.Body)
	if detail != "" {
		return fmt.Errorf("%w%s", statusErr, detail)
	}
	return statusErr
}

// statusDetail renders what a rejected response adds to its status code:
// Discord's numeric "code" field, never the object's text fields, which are
// remote-authored. The empty string means the status is the whole verdict.
func statusDetail(body io.Reader) string {
	// One byte past the cap, so an over-cap body is detectable and dropped
	// instead of being decoded as a whole one.
	detail, readErr := io.ReadAll(io.LimitReader(body, maxErrorBodyBytes+1))
	if readErr != nil || len(detail) > maxErrorBodyBytes {
		return ""
	}
	code, ok := discordErrorCode(detail)
	if !ok {
		return ""
	}
	return fmt.Sprintf(": Discord error code %d", code)
}

// discordErrorCode reports the numeric error code of a rejected response
// body. Only that one field is decoded; the object's other text fields are
// remote-authored and never bound to a variable this package formats.
func discordErrorCode(body []byte) (int, bool) {
	var parsed struct {
		Code *int `json:"code"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil || parsed.Code == nil {
		return 0, false
	}
	return *parsed.Code, true
}
