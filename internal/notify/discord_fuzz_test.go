package notify

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// FuzzLogSafeNeverLeaksWebhookRendering asserts the crown-jewel invariant of
// this package over an unbounded URL space: neither a complete rendering of the
// configured webhook URL nor its credential-bearing suffix survives logSafe,
// and logSafe never breaks the errors.Is chain the sweep and httpx.Do classify
// through. The four renderings are derived here independently (raw, url.String,
// Path, EscapedPath) rather than read from redactionCandidates, so a candidate
// list that stops covering one of them fails instead of silently narrowing the
// property with it. The credential needle is derived independently of the
// full-rendering needle for the same reason: a redaction that removes only the
// harmless scheme+host prefix makes the complete rendering disappear while the
// /api/webhooks/<credential> segment still reaches the log.
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
			renderings = append(renderings, u.String(), u.Path, u.EscapedPath())
		}
		for _, rendering := range renderings {
			// Skip a degenerate rendering: a handful of bytes can occur in
			// the surrounding error text, where its presence proves nothing.
			if len(rendering) < 8 {
				continue
			}
			// The credential-bearing suffix, derived independently of the
			// full-rendering needle above. Prefix-only redaction (drop the
			// scheme+host, keep /api/webhooks/<credential>) satisfies the
			// full-rendering assertion while still leaking the secret, so
			// the suffix is asserted on its own. Guarded by the same
			// minimum length: a few bytes prove nothing.
			credential := strings.TrimPrefix(rendering, "https://discord.example")
			credential = strings.TrimPrefix(credential, "/api/webhooks/")
			// Two error SHAPES, because logSafe has two halves and production
			// takes the second one. A plain wrapError exercises the
			// value-based backstop (scrub the candidates out of the text); a
			// *url.Error exercises the type-based reduction
			// httpx.LogSafeError performs, which is what postAttempt's
			// transport path actually returns.
			// An empty-message sentinel keeps arbitrary fuzz input out of the
			// wrapped cause while still giving errors.Is a stable identity:
			// with a fixed-text cause, a fuzz tail equal to that text would
			// make the credential needle match the scaffolding instead of a
			// leak. So any match below can only be the credential itself.
			sentinel := errors.New("")
			shapes := map[string]error{
				"wrapped text": fmt.Errorf("%s: %w", rendering, sentinel),
				"url error":    &url.Error{Op: "Post", URL: rendering, Err: sentinel},
			}
			for shape, in := range shapes {
				got := d.logSafe(in)
				if strings.Contains(got.Error(), rendering) {
					t.Errorf("logSafe(%s) kept webhook rendering %q in %q", shape, rendering, got)
				}
				if len(credential) >= 8 && strings.Contains(got.Error(), credential) {
					t.Errorf("logSafe(%s) kept webhook credential %q in %q", shape, credential, got)
				}
				if !errors.Is(got, sentinel) {
					t.Errorf("logSafe(%s, %q) broke the errors.Is chain: %v", shape, rendering, got)
				}
			}
		}
	})
}
