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
// configured webhook URL nor any credential-bearing fragment of its path
// survives logSafe, and logSafe never breaks the errors.Is chain the sweep and
// httpx.Do classify through. Each fuzz tail is checked against two webhook
// path SHAPES, because the shape decides which candidate is the only
// protection: with Discord's fixed /api/webhooks/ prefix the suffix after it
// covers the body, while a Discord-compatible edge on its own path (knell
// accepts any https URL) is covered only by the path minus its leading slash.
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
		// A credential carried in the QUERY instead of the path: knell
		// accepts any https URL, so a relay-style webhook authenticated by
		// ?key=… is a deployable shape, and the query candidates are its
		// only protection.
		"1234567890/plainsegment?key=queryborneexample",
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
		for _, base := range []string{
			"https://discord.example/api/webhooks/",
			"https://relay.example/hooks/",
		} {
			assertLogSafeHidesEveryCredentialForm(t, base+tail)
		}
	})
}

// assertLogSafeHidesEveryCredentialForm checks the leak-and-chain invariant for
// one configured webhook URL. The four complete renderings (raw, url.String,
// Path, EscapedPath) and the three credential fragments are derived here
// independently rather than read from redactionCandidates, so a candidate list
// that stops covering one of them fails instead of silently narrowing the
// property with it. Every needle is also fed as an error text in its OWN right,
// because a remote body that echoes only one fragment is the shape a
// longer-form-only candidate list passes through untouched: scrubbing a
// complete rendering out of such a body removes nothing.
func assertLogSafeHidesEveryCredentialForm(t *testing.T, rawURL string) {
	t.Helper()

	d := New(rawURL, "node-1")
	defer d.Close()

	renderings := []string{rawURL}
	// The credential needles, derived from the PARSED PATH rather than by
	// trimming a rendering: a query or fragment in the fuzz tail is not part
	// of the path (net/http never sends a fragment, and the delivery path
	// cannot leak what it never carries). All three fragment shapes an error
	// can carry without the surrounding URL are needles, because each is the
	// only protection for a different remote-body shape: the path minus its
	// leading slash, the suffix after Discord's fixed prefix when the URL has
	// one, and the final segment on its own (a proxy reporting "upstream
	// failed for <token>"). The query forms below and the JSON slash-escaped
	// rendering of every form are covered for the same reason: each is the
	// only protection for one remote-body shape.
	var credentials []string
	if u, parseErr := url.Parse(rawURL); parseErr == nil {
		renderings = append(renderings, u.String(), u.Path, u.EscapedPath())
		for _, p := range []string{u.Path, u.EscapedPath()} {
			credentials = append(credentials, strings.TrimPrefix(p, "/"))
			if suffix, ok := strings.CutPrefix(p, "/api/webhooks/"); ok {
				credentials = append(credentials, suffix)
			}
			if i := strings.LastIndex(p, "/"); i >= 0 {
				credentials = append(credentials, p[i+1:])
			}
		}
		// The query forms too: a relay-style webhook can carry its
		// credential as ?key=…, and then the raw query and the decoded
		// values are the only candidates covering a body that echoes the
		// request-URI or the query alone.
		credentials = append(credentials, u.RawQuery)
		for _, values := range u.Query() {
			credentials = append(credentials, values...)
		}
	}
	// Carriers are the texts an error can be built from; needles are the
	// texts that must never survive one. Both are length-guarded: a handful
	// of bytes can occur in the surrounding error text, where its presence
	// proves nothing.
	needles := append(slices.Clone(renderings), credentials...)
	// A JSON error body commonly escapes "/" as "\/" (PHP json_encode does so
	// by default), which carries the very same credential in a form no
	// plain-byte candidate matches. Each escaped rendering is therefore both a
	// carrier (the remote body shape) and a needle (what must not survive it).
	for _, form := range slices.Clone(needles) {
		if escaped := strings.ReplaceAll(form, "/", `\/`); escaped != form {
			needles = append(needles, escaped)
		}
	}
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
}
