package config

import (
	"net/url"
	"strings"
	"testing"
)

// FuzzParseWebhookURL pins the webhook validator's safety and secret-hygiene
// invariants: it never panics, every accepted URL is an absolute http(s) URL
// with a host, and no rejection error ever embeds the raw URL or its path
// (the path carries the webhook credential, and startup errors are logged).
func FuzzParseWebhookURL(f *testing.F) {
	f.Add("https://discord.com/api/webhooks/1/abc")
	f.Add("http://127.0.0.1:9/hook")
	f.Add("ftp://discord.com/api/webhooks/1234567890/verysecrettoken")
	f.Add("discord.com/api/webhooks/1/abc")
	f.Add("https:///hook")
	f.Add("://")
	f.Add("https://host/secret\x00token")
	f.Fuzz(func(t *testing.T, raw string) {
		u, err := parseWebhookURL(raw)
		if err == nil {
			if u.Scheme != "http" && u.Scheme != "https" {
				t.Fatalf("accepted scheme %q", u.Scheme)
			}
			if u.Host == "" {
				t.Fatal("accepted URL without host")
			}
			return
		}
		// Secret hygiene: every current rejection message is "/"-free, so
		// an error may never contain the slash-bearing raw URL or its
		// hierarchical path (where the webhook credential lives). Any
		// future wrap that embeds the URL fails here.
		if strings.Contains(raw, "/") && strings.Contains(err.Error(), raw) {
			t.Fatalf("rejection error %q embeds the raw URL", err)
		}
		if parsed, perr := url.Parse(raw); perr == nil &&
			strings.HasPrefix(parsed.Path, "/") && len(parsed.Path) > 1 &&
			strings.Contains(err.Error(), parsed.Path) {
			t.Fatalf("rejection error %q embeds the URL path", err)
		}
	})
}
