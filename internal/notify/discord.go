// Package notify delivers knell's transition notifications to a Discord
// webhook. It is the only outbound-network code in the app: one POST per
// transition, retried on transient failures via httpx.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/cplieger/httpx/v3"
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
	return &Discord{
		// Client timeout above the per-attempt context timeout so the
		// context is the effective per-attempt bound.
		client: &http.Client{Timeout: attemptTimeout + 5*time.Second},
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
		"🚨 [knell %s] beat **%s** MISSING — silent for %s. The sender is down, or nothing on its path can reach this observer.",
		d.node, id, silence.Truncate(time.Second),
	)
	return d.post(ctx, "missing "+id, msg)
}

// BeatRecovered announces the first ping after a missing alert.
func (d *Discord) BeatRecovered(ctx context.Context, id string, downFor time.Duration) error {
	msg := fmt.Sprintf(
		"✅ [knell %s] beat **%s** recovered — pings arriving again after %s of silence.",
		d.node, id, downFor.Truncate(time.Second),
	)
	return d.post(ctx, "recovered "+id, msg)
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
		if statusErr := httpx.CheckHTTPStatus(resp); statusErr != nil {
			return struct{}{}, statusErr
		}
		return struct{}{}, nil
	}, httpx.WithLabel("discord webhook "+label), httpx.WithMaxAttempts(maxAttempts), httpx.WithRateLimitRetry(30*time.Second))
	if err != nil {
		return fmt.Errorf("delivering %s notification: %w", label, httpx.LogSafeError(err))
	}
	return nil
}
