package notify

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// FuzzLogSafeNeverLeaksWebhookRendering asserts the crown-jewel invariant of
// this package over an unbounded URL space: no rendering of the configured
// webhook URL survives logSafe, and logSafe never breaks the errors.Is chain
// the sweep and httpx.Do classify through. The three renderings are derived
// here independently (raw, url.String, EscapedPath) rather than read from
// redactionCandidates, so a candidate list that stops covering one of them
// fails instead of silently narrowing the property with it.
func FuzzLogSafeNeverLeaksWebhookRendering(f *testing.F) {
	// Seed tails stand in for the credential segment of a webhook URL. They
	// deliberately avoid secret-shaped keywords ("token", "secret", …): a
	// literal that looks like a real credential trips the repo's secret scan
	// even though it is only fuzz seed data.
	for _, tail := range []string{
		"1234567890/plainsegment",
		"1234567890/v\u00e9rylongsegment",
		"1234567890/tok en",
		"1234567890/tok#frag",
		"1234567890/tok?q=1",
		"1234567890/%2Ftok",
		"1234567890/",
		"",
		"/",
		"..",
	} {
		f.Add(tail)
	}

	f.Fuzz(func(t *testing.T, tail string) {
		rawURL := "https://discord.example/api/webhooks/" + tail
		d := New(rawURL, "node-1")
		defer d.Close()

		renderings := []string{rawURL}
		if u, parseErr := url.Parse(rawURL); parseErr == nil {
			renderings = append(renderings, u.String(), u.EscapedPath())
		}
		for _, rendering := range renderings {
			// Skip a degenerate rendering: a handful of bytes can occur in
			// the surrounding error text, where its presence proves nothing.
			if len(rendering) < 8 {
				continue
			}
			// Minimal scaffolding on purpose: the only other text is the
			// wrapped error, so a match can only be the credential itself.
			got := d.logSafe(fmt.Errorf("%s: %w", rendering, context.Canceled))
			if strings.Contains(got.Error(), rendering) {
				t.Errorf("logSafe kept webhook rendering %q in %q", rendering, got)
			}
			if !errors.Is(got, context.Canceled) {
				t.Errorf("logSafe(%q) broke the errors.Is chain: %v", rendering, got)
			}
		}
	})
}
