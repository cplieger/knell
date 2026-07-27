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

// FuzzDeliveryErrorNeverCarriesWebhookURL asserts the crown-jewel invariant of
// this package over an unbounded space of REMOTE input: for an arbitrary
// response status and body, the error the delivery path returns carries no
// rendering of the configured webhook URL and no fragment of its
// credential-bearing path.
//
// The invariant used to be defended by scrubbing candidate renderings out of
// the published body text, and this target used to fuzz that candidate
// machinery. It is now structural — the body's own text is never published at
// all, only Discord's numeric error code and knell's own wording — so the
// property holds by construction and this target should pass trivially. That
// is exactly what makes it worth keeping: it fails the moment a future change
// puts remote-authored text back into a delivery error.
//
// postAttempt is the subject rather than post, for two reasons: it is the
// function that turns a response into an error, and its return value is
// verbatim what httpx.Do logs on each attempt (through the type-based
// LogSafeError, which can only shrink it), so the log surface is covered by
// the same assertion. Calling it directly also keeps one fuzz iteration to one
// attempt, with no retry backoff and no 429 Retry-After wait.
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
	// The other half of the contract the status branch must keep: every
	// non-2xx stays %w-wrapped around CheckHTTPStatus's typed error, which is
	// what lets httpx.Do classify 502/503/504 as transient and find the 429's
	// *RateLimitError. A run where the request was never made (an unusable
	// fuzzed URL) reaches no status at all.
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
