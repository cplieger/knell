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
	"net/http"
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

// Discord posts plain-content messages to one Discord-compatible webhook.
type Discord struct {
	client *http.Client
	url    string
	node   string
}

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
	//
	// Two deltas from the hand-rolled wrapper this replaced: ANY
	// method-changing hop is refused, not only one whose original request
	// was a POST (knell only ever POSTs, so nothing here reaches the
	// difference), and a call with an empty via chain is refused instead of
	// allowed — net/http always passes at least the original request, so
	// that path is unreachable in production, and failing closed is the
	// safer direction for a credential-bearing POST.
	client.CheckRedirect = httpx.RedirectPolicyFunc(httpx.WithSameHost(), httpx.WithPreserveMethod())
	return &Discord{
		client: client,
		url:    webhookURL,
		node:   node,
	}
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
// never appears in returned errors or logs (httpx redacts transport errors;
// status failures are rebuilt without the URL).
func (d *Discord) post(ctx context.Context, label, content string) error {
	body, err := json.Marshal(map[string]string{"content": content})
	if err != nil {
		return fmt.Errorf("encoding webhook payload: %w", err)
	}
	_, err = httpx.Do(ctx, func(ctx context.Context) (struct{}, error) {
		return d.postAttempt(ctx, body)
	}, httpx.WithLabel("discord webhook "+label), httpx.WithMaxAttempts(maxAttempts), httpx.WithRateLimitRetry(30*time.Second))
	if err != nil {
		return fmt.Errorf("delivering %s notification: %w", label, httpx.LogSafeError(err))
	}
	return nil
}

// postAttempt performs one delivery attempt of an already-encoded payload:
// per-attempt deadline, request construction, transport call, response
// cleanup, and strict delivery validation. It is the retry callback post
// hands to httpx.Do, which owns the retry policy and terminal wrapping.
// Every error it returns is URL-free, so the webhook secret cannot reach a
// log or a returned error.
func (d *Discord) postAttempt(ctx context.Context, body []byte) (struct{}, error) {
	attemptCtx, cancel := httpx.ContextWithDefaultTimeout(ctx, attemptTimeout)
	defer cancel()
	req, reqErr := http.NewRequestWithContext(attemptCtx, http.MethodPost, d.url, bytes.NewReader(body))
	if reqErr != nil {
		// The raw error would embed the URL; report the cause only.
		return struct{}{}, fmt.Errorf("building webhook request: %w", httpx.LogSafeError(reqErr))
	}
	req.Header.Set("Content-Type", "application/json")
	resp, doErr := d.client.Do(req) //nolint:bodyclose // closed via deferred httpx.DrainClose below
	if doErr != nil {
		// *url.Error embeds the full webhook URL; reduce it to its cause
		// (transient classification survives the reduction).
		return struct{}{}, httpx.LogSafeError(doErr)
	}
	defer httpx.DrainClose(resp.Body)
	// Success is exactly 2xx here: CheckHTTPStatus rejects every other
	// status, an unfollowed redirect's 3xx included (pinned by
	// TestUnfollowedRedirectIsNotDelivery), so the sweep keeps retrying a
	// non-delivery. Its error is typed, which is what lets httpx.Do
	// classify 502/503/504 as transient and retry within the call.
	if statusErr := httpx.CheckHTTPStatus(resp); statusErr != nil {
		return struct{}{}, statusErr
	}
	return struct{}{}, nil
}
