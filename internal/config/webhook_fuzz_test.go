package config

import (
	"testing"
)

// FuzzParseWebhookURL pins the webhook validator's safety and secret-hygiene
// invariants: it never panics, every accepted URL is an absolute https URL
// with a host (plain http is rejected — the URL's path is a credential), and
// every rejection message is one of a fixed set of constants, so no error can
// embed any part of the operator-supplied value (startup errors are logged).
func FuzzParseWebhookURL(f *testing.F) {
	f.Add("https://discord.com/api/webhooks/1/abc")
	f.Add("http://127.0.0.1:9/hook")
	f.Add("ftp://discord.com/api/webhooks/1234567890/verysecrettoken")
	f.Add("discord.com/api/webhooks/1/abc")
	f.Add("https:///hook")
	f.Add("://")
	f.Add("https://host/secret\x00token")
	f.Add("credentialmaterial:rest")
	f.Fuzz(func(t *testing.T, raw string) {
		u, err := parseWebhookURL(raw)
		if err == nil {
			if u.Scheme != "https" {
				t.Fatalf("accepted scheme %q, want https only", u.Scheme)
			}
			if u.Host == "" {
				t.Fatal("accepted URL without host")
			}
			return
		}
		// Secret hygiene: every rejection message is a fixed constant, so no
		// error can embed ANY part of the operator-supplied value — not just
		// its slash-bearing path, but also a slash-free secret that url.Parse
		// reads as a scheme ("credentialmaterial:rest"). Pinning the exact
		// message set is what a path substring check cannot do: a future wrap
		// that leaks a slash-free secret fails here too.
		switch err.Error() {
		case "not a valid URL",
			"scheme must be https (the webhook URL's own path is the credential, so plain http would send it in cleartext)",
			"missing host",
			"missing path (the webhook URL's own path carries the credential, so a host-only URL cannot deliver a notification)",
			"contains a space (a space is percent-encoded on every request, so the webhook path that reaches the other end is not the configured one)":
		default:
			t.Fatalf("unexpected rejection message %q: a new message must be a fixed constant that cannot embed the operator-supplied URL", err)
		}
	})
}
