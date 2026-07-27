package notify

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
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
// /api/webhooks/<credential> segment still reaches the log. Every needle is
// also fed as an error text in its OWN right, because a remote body that
// echoes only the credential tail is the shape a complete-rendering-only
// candidate list passes through untouched.
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
		// A fragment-only tail: the path carries no credential at all, so
		// nothing here is a needle. Found by a fuzz run against an earlier
		// oracle that derived the credential by trimming a RENDERING and so
		// demanded redaction of fragment text the request never sends.
		"#0000000",
	} {
		f.Add(tail)
	}

	f.Fuzz(func(t *testing.T, tail string) {
		rawURL := "https://discord.example/api/webhooks/" + tail
		d := New(rawURL, "node-1")
		defer d.Close()

		renderings := []string{rawURL}
		// The credential needles, derived from the PARSED PATH rather than by
		// trimming a rendering: the credential is the path tail after the
		// fixed /api/webhooks/ prefix, so a query or fragment in the fuzz tail
		// is not part of it (net/http never sends a fragment, and the delivery
		// path cannot leak what it never carries). Derived independently of
		// the renderings above so a candidate list that stops covering the
		// suffix fails instead of silently narrowing the property.
		var credentials []string
		if u, parseErr := url.Parse(rawURL); parseErr == nil {
			renderings = append(renderings, u.String(), u.Path, u.EscapedPath())
			for _, p := range []string{u.Path, u.EscapedPath()} {
				if suffix, ok := strings.CutPrefix(p, "/api/webhooks/"); ok {
					credentials = append(credentials, suffix)
				}
			}
		}
		// Carriers are the texts an error can be built from; needles are the
		// texts that must never survive one. Both are length-guarded: a
		// handful of bytes can occur in the surrounding error text, where its
		// presence proves nothing.
		//
		// A credential is a needle AND a carrier of its own, not only a
		// substring of a rendering. Prefix-only redaction (drop the
		// scheme+host, keep /api/webhooks/<credential>) satisfies the
		// full-rendering assertion while still leaking the secret, and a
		// remote body that echoes ONLY the credential tail is the shape that
		// exposed it: scrubbing a complete rendering out of such a body
		// removes nothing.
		needles := append(slices.Clone(renderings), credentials...)
		for _, carrier := range needles {
			if len(carrier) < 8 {
				continue
			}
			// An empty-message sentinel keeps arbitrary fuzz input out of the
			// wrapped cause while still giving errors.Is a stable identity:
			// with a fixed-text cause, a fuzz tail equal to that text would
			// make a needle match the scaffolding instead of a leak. So any
			// match below can only be the secret itself.
			sentinel := errors.New("")
			// Two error SHAPES, because logSafe has two halves and production
			// takes the second one. A plain wrapError exercises the
			// value-based backstop (scrub the candidates out of the text); a
			// *url.Error exercises the type-based reduction
			// httpx.LogSafeError performs, which is what postAttempt's
			// transport path actually returns.
			shapes := map[string]error{
				"wrapped text": fmt.Errorf("%s: %w", carrier, sentinel),
				"url error":    &url.Error{Op: "Post", URL: carrier, Err: sentinel},
			}
			for shape, in := range shapes {
				got := d.logSafe(in)
				for _, needle := range needles {
					// Only a needle the input actually rendered can prove a
					// leak; absence of one that was never there is vacuous.
					if len(needle) < 8 || !strings.Contains(in.Error(), needle) {
						continue
					}
					if strings.Contains(got.Error(), needle) {
						t.Errorf("logSafe(%s of %q) kept %q in %q", shape, carrier, needle, got)
					}
				}
				if !errors.Is(got, sentinel) {
					t.Errorf("logSafe(%s, %q) broke the errors.Is chain: %v", shape, carrier, got)
				}
			}
		}
	})
}
