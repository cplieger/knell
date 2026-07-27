package notify

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/cplieger/httpx/v4"
)

// FuzzDeliveryErrorNeverCarriesWebhookURL checks that arbitrary response
// statuses and bodies cannot place the configured webhook URL or a
// credential-bearing path fragment in a delivery error. It invokes
// postAttempt directly because that is the exact error httpx.Do logs, while
// avoiding retry backoff and rate-limit waits. It also checks that non-2xx
// responses retain the typed errors used for retry classification.
func FuzzDeliveryErrorNeverCarriesWebhookURL(f *testing.F) {
	// Seed bodies stand in for what the other end can answer with; seed tails
	// for the credential segment of a webhook URL. They deliberately avoid
	// secret-shaped keywords ("token", "secret", …): a literal that looks like
	// a real credential trips the repo's secret scan even as fuzz seed data.
	for _, seed := range []struct {
		tail   string
		status uint16
		body   string
	}{
		{"1234567890/plainsegment", 404, `{"message": "Unknown Webhook", "code": 10015}`},
		{"1234567890/plainsegment", 400, `{"message": "Invalid Form Body", "code": 50035, "errors": {"content": {}}}`},
		// The leak shape behind both real leaks: the body echoes the request
		// URI, which for a webhook IS the credential.
		{"1234567890/plainsegment", 503, "502 Bad Gateway: upstream failed for /api/webhooks/1234567890/plainsegment"},
		// The same echo, JSON-escaped ("\/"), a form no byte-for-byte
		// filtering of the body ever matched.
		{"1234567890/plainsegment", 500, `{"message": "failed for \/api\/webhooks\/1234567890\/plainsegment"}`},
		// The credential-bearing tail alone, with no surrounding URL.
		{"1234567890/plainsegment", 502, "upstream rejected 1234567890/plainsegment"},
		{"1234567890/plainsegment", 400, strings.Repeat("x", maxErrorBodyBytes+64)},
		{"1234567890/plainsegment", 204, ""},
		{"1234567890/plainsegment", 302, ""},
		{"1234567890/v\u00e9rylongsegment", 400, "for /api/webhooks/1234567890/v\u00e9rylongsegment"},
		// A relay-style webhook whose whole path is the credential, and one
		// carrying it in the query: knell accepts any https URL.
		{"1234567890/plainsegment?key=queryborneexample", 401, `{"code": 50027, "q": "key=queryborneexample"}`},
		{"", 400, "/api/webhooks/"},
		{"/", 429, `{"message": "You are being rate limited.", "retry_after": 0.5}`},
		{"..", 400, `{"code": null}`},
		{"1234567890/tok en", 400, `{"code": "10015"}`},
		{"1234567890/%2Ftok", 400, `{"code": 40333}`},
	} {
		f.Add(seed.tail, seed.status, seed.body)
	}

	f.Fuzz(func(t *testing.T, tail string, statusSeed uint16, body string) {
		// The whole HTTP status space including the informational and
		// redirect bands, so the 3xx branch and CheckHTTPStatus's 1xx
		// rejection are fuzzed alongside the body branches.
		status := 100 + int(statusSeed%600)
		for _, base := range []string{
			"https://discord.example/api/webhooks/",
			"https://relay.example/hooks/",
		} {
			assertDeliveryErrorHidesWebhookURL(t, base+tail, status, body)
		}
	})
}

// assertDeliveryErrorHidesWebhookURL runs one delivery attempt against a stub
// transport that answers with the given status and body, and checks that the
// resulting error carries nothing derived from the configured webhook URL.
//
// A needle that also appears in the error built for a DIFFERENT (control)
// webhook URL is skipped: that text came from knell's own message template, so
// a fuzz tail that happens to contain a phrase like "detail dropped" cannot
// masquerade as a leak. Everything else is a leak by definition — nothing in
// the delivery path is supposed to depend on the URL at all.
func assertDeliveryErrorHidesWebhookURL(t *testing.T, rawURL string, status int, body string) {
	t.Helper()

	const controlURL = "https://control.example/hooks/controlsegment"
	gotErr, requested := attemptAgainstStub(t, rawURL, status, body)
	controlErr, _ := attemptAgainstStub(t, controlURL, status, body)
	if gotErr == nil {
		// A nil error is only correct when nothing was delivered-checked (an
		// unusable fuzzed URL never reached the transport) or the status was
		// a 2xx. Returning unconditionally here would let the more serious
		// regression — a non-2xx reported as a successful delivery, which
		// silently disarms the switch — pass this target.
		if requested && (status < 200 || status >= 300) {
			t.Errorf("postAttempt status %d returned nil, want a typed status error", status)
		}
		return
	}
	for _, needle := range webhookNeedles(rawURL) {
		// A handful of bytes can occur in the surrounding message text, where
		// its presence proves nothing.
		if len(needle) < 8 {
			continue
		}
		if controlErr != nil && strings.Contains(controlErr.Error(), needle) {
			continue
		}
		if strings.Contains(gotErr.Error(), needle) {
			t.Errorf("delivery error for %q (status %d) kept %q: %v", rawURL, status, needle, gotErr)
		}
	}
	assertTypedStatusError(t, gotErr, requested, status)
}

// assertTypedStatusError checks the other half of the contract the status
// branch must keep: every non-2xx stays %w-wrapped around CheckHTTPStatus's
// typed error, which is what lets httpx.Do classify 502/503/504 as transient
// and find the 429's *RateLimitError. A run where the request was never made
// (an unusable fuzzed URL) reaches no status at all, and a 2xx is a delivery.
//
// Split out of assertDeliveryErrorHidesWebhookURL so the leak oracle and the
// typed-chain oracle are one assertion each.
func assertTypedStatusError(t *testing.T, gotErr error, requested bool, status int) {
	t.Helper()

	if !requested || (status >= 200 && status < 300) {
		return
	}
	var (
		statusErr *httpx.HTTPStatusError
		authErr   *httpx.AuthError
		rateErr   *httpx.RateLimitError
	)
	if !errors.As(gotErr, &statusErr) && !errors.As(gotErr, &authErr) && !errors.As(gotErr, &rateErr) {
		t.Errorf("delivery error for status %d = %v, want CheckHTTPStatus's typed error still in the chain", status, gotErr)
	}
}

// attemptAgainstStub performs one postAttempt against a stub transport that
// answers with status and body, reporting the resulting error and whether the
// transport was reached at all (a fuzzed URL that no request can be built from
// fails earlier, and then no status branch ran).
func attemptAgainstStub(t *testing.T, rawURL string, status int, body string) (err error, requested bool) {
	t.Helper()

	d := New(rawURL, "node-1")
	defer d.Close()
	d.client.Transport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		requested = true
		if _, copyErr := io.Copy(io.Discard, r.Body); copyErr != nil {
			t.Errorf("reading request body: %v", copyErr)
		}
		return &http.Response{
			StatusCode: status,
			Status:     fmt.Sprintf("%d Fuzzed", status),
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    r,
		}, nil
	})
	_, err = d.postAttempt(context.Background(), []byte(`{"content":"fuzz"}`))
	return err, requested
}

// webhookNeedles lists every text whose presence in a delivery error would be
// a credential leak: the complete renderings of the configured URL, the
// credential-bearing fragments of its path (the path itself, the suffix after
// Discord's fixed prefix, the final segment — each is the only evidence of a
// leak for a different remote-body shape), its query forms, and the
// slash-escaped rendering of each, since a JSON error body commonly escapes
// "/" as "\/".
func webhookNeedles(rawURL string) []string {
	needles := []string{rawURL}
	u, parseErr := url.Parse(rawURL)
	if parseErr != nil {
		return needles
	}
	needles = append(needles, u.String(), u.Path, u.EscapedPath())
	for _, p := range []string{u.Path, u.EscapedPath()} {
		needles = append(needles, strings.TrimPrefix(p, "/"))
		if suffix, ok := strings.CutPrefix(p, "/api/webhooks/"); ok {
			needles = append(needles, suffix)
		}
		if i := strings.LastIndex(p, "/"); i >= 0 {
			needles = append(needles, p[i+1:])
		}
	}
	needles = append(needles, u.RawQuery)
	for _, values := range u.Query() {
		needles = append(needles, values...)
	}
	for _, form := range needles[:len(needles):len(needles)] {
		if escaped := strings.ReplaceAll(form, "/", `\/`); escaped != form {
			needles = append(needles, escaped)
		}
	}
	return needles
}

// FuzzRedirectResponsesNeverCarryLocationText fuzzes the third channel remote
// text can enter a delivery error by: a response HEADER. net/http writes two
// of its transport causes from the Location header (a malformed one as `failed
// to parse Location header "<header>"`, a policy refusal as httpx's `refusing
// redirect to <host>`), so an endpoint answering a webhook POST with a
// redirect that echoes the request URI would put the credential into the
// returned error and into httpx.Do's attempt lines without ever sending a
// body. TestRedirectDerivedTransportErrorsCarryNoRemoteText pins two
// hand-picked shapes; this target explores the whole header space and the
// whole 3xx band, so the seed corpus committed here is the durable coverage of
// the class (the weekly run's generated corpus is discarded).
func FuzzRedirectResponsesNeverCarryLocationText(f *testing.F) {
	// Seed Locations cover the shapes that reach a different cause: an
	// unparseable header, a cross-host method-preserving hop (refused by
	// policy, and the shape whose refusal text names the remote host), a
	// scheme downgrade, a followed same-host hop, a host past any sane
	// length, a non-HTTP scheme, a query-borne credential, and an empty
	// header (a 3xx net/http cannot follow). They avoid secret-shaped
	// keywords: a literal that looks like a credential trips the repo's
	// secret scan even as fuzz seed data.
	for _, seed := range []struct {
		location string
		status   uint8
	}{
		{"/api/webhooks/1234567890/plainsegment%zz", 2},
		{"https://plainsegment.redirect.example/hooks/1", 7},
		{"", 0},
		{"/finish", 7},
		{`\/api\/webhooks\/1234567890\/plainsegment`, 1},
		{"http://discord.example/api/webhooks/1234567890/plainsegment", 2},
		{"https://discord.example:99999/hooks", 8},
		{"//" + strings.Repeat("p", 600) + ".example/hooks", 7},
		{"h ttp://plainsegment.example/\x7f\x01", 3},
		{"/hooks/v\u00e9rylongsegment", 4},
		{"mailto:plainsegment@example.test", 2},
		{"?key=queryborneexample", 7},
	} {
		f.Add(seed.location, seed.status)
	}

	f.Fuzz(func(t *testing.T, location string, statusSeed uint8) {
		// The whole redirect band: 301/302/303 are the method-changing hops
		// the policy surfaces as a 3xx, 307/308 the method-preserving ones it
		// refuses with an error, and 300/304/305/306 reach net/http's
		// no-usable-Location path.
		status := 300 + int(statusSeed%9)
		const rawURL = "https://discord.example/api/webhooks/1234567890/plainsegment"
		// A fixed control Location isolates knell's own message template: a
		// fuzzed header that happens to contain a phrase like "nothing was
		// delivered" cannot masquerade as a leak.
		const controlLocation = "/control-hop"

		gotErr := attemptAgainstRedirectStub(t, rawURL, status, location)
		controlErr := attemptAgainstRedirectStub(t, rawURL, status, controlLocation)
		if gotErr == nil {
			// Every status here is a non-2xx, so a nil would report an
			// undelivered notification as delivered.
			t.Fatalf("postAttempt against a %d response returned nil, want a non-delivery error", status)
		}
		needles := append(webhookNeedles(rawURL), locationNeedles(location)...)
		for _, needle := range needles {
			// A handful of bytes can occur in the surrounding message text,
			// where its presence proves nothing.
			if len(needle) < 8 {
				continue
			}
			if controlErr != nil && strings.Contains(controlErr.Error(), needle) {
				continue
			}
			if strings.Contains(gotErr.Error(), needle) {
				t.Errorf("delivery error for Location %q (status %d) kept %q: %v", location, status, needle, gotErr)
			}
		}
	})
}

// locationNeedles lists every text whose presence in a delivery error would
// mean the Location header reached it: the header whole, its slash-escaped
// rendering (a JSON error body commonly escapes "/" as "\/"), and each part a
// parse of it exposes, since net/http's causes are built from parsed pieces as
// well as from the raw header.
func locationNeedles(location string) []string {
	needles := []string{location, strings.ReplaceAll(location, "/", `\/`)}
	if u, err := url.Parse(location); err == nil {
		needles = append(needles, u.Host, u.Hostname(), u.Path, u.EscapedPath(), u.RawQuery)
	}
	if i := strings.LastIndex(location, "/"); i >= 0 {
		needles = append(needles, location[i+1:])
	}
	return needles
}

// attemptAgainstRedirectStub performs one postAttempt against a stub transport
// that answers every request with status and the given Location header,
// reporting the resulting error. A stub rather than a listener because the
// redirect machinery lives in http.Client, above the transport: this exercises
// the real policy and the real Location parsing with no socket, no dial and no
// clock in the oracle.
func attemptAgainstRedirectStub(t *testing.T, rawURL string, status int, location string) error {
	t.Helper()

	d := New(rawURL, "node-1")
	defer d.Close()
	d.client.Transport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		if _, copyErr := io.Copy(io.Discard, r.Body); copyErr != nil {
			t.Errorf("reading request body: %v", copyErr)
		}
		header := make(http.Header)
		header.Set("Location", location)
		return &http.Response{
			StatusCode: status,
			Status:     fmt.Sprintf("%d Fuzzed", status),
			Header:     header,
			Body:       http.NoBody,
			Request:    r,
		}, nil
	})
	_, err := d.postAttempt(context.Background(), []byte(`{"content":"fuzz"}`))
	return err
}
