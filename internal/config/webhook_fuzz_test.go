package config

import (
	"strconv"
	"strings"
	"testing"
	"unicode"
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
	f.Add("https://:443/api/webhooks/1/abc")
	f.Add("https://discord.com")
	f.Add("https://discord.com/")
	f.Add("https://discord.com?token=abc")
	f.Add("https://discord.com/api/webhooks/1/ab c")
	f.Add("https://discord.com:99999/api/webhooks/1/abc")
	f.Add("https://discord.com/api/webhooks/1/ab\u00a0c")
	f.Add("https://discord.\u200bcom/api/webhooks/1/abc")
	f.Add("https://discord.com/api/webhooks/1/ab\u00adc")
	f.Add("https://discord.com/api/webhooks/1/ab\ufeffc")
	f.Fuzz(func(t *testing.T, raw string) {
		u, err := parseWebhookURL(raw)
		if err == nil {
			if u.Scheme != "https" {
				t.Fatalf("accepted scheme %q, want https only", u.Scheme)
			}
			if u.Hostname() == "" {
				// Hostname(), not Host: an authority of nothing but a port
				// (":443") has a NON-EMPTY Host, so a Host-based assertion
				// cannot catch a revert of parseWebhookURL's own gate.
				t.Fatal("accepted URL without a hostname")
			}
			if port := u.Port(); port != "" {
				// url.Parse only checks that a port is digits; an out-of-range
				// one makes net/http refuse every POST, so an accepted URL must
				// carry a port the transport can actually dial.
				n, convErr := strconv.Atoi(port)
				if convErr != nil || n < 1 || n > 65535 {
					t.Fatalf("accepted port %q: net/http refuses it on every request, so this URL can never deliver a notice", port)
				}
			}
			// DELIVERABILITY, not just transportability: the two remaining gates
			// are the ones whose failure is invisible until an outage. A path-less
			// URL posts every notice to the origin's root, while a space or any
			// other non-printable rune is percent-encoded on the wire, so the
			// host or path that reaches Discord is not the configured one - in
			// both cases startup succeeds, /healthz reports ready, and the bell
			// never rings. Asserting them on the ACCEPTED side is what makes
			// dropping either guard fail here as well as in the hand-written
			// table.
			if u.Path == "" || u.Path == "/" {
				t.Fatalf("accepted a URL whose path is %q: the webhook path IS the Discord credential, so this URL delivers nothing", u.Path)
			}
			// Keep this oracle independent of invisibleInURL: using the
			// production predicate here would let a narrowed predicate relax the
			// parser and its test together. unicode.IsPrint states the configured
			// contract without calling the implementation helper.
			if strings.IndexFunc(raw, func(r rune) bool { return r == ' ' || !unicode.IsPrint(r) }) >= 0 {
				t.Fatal("accepted a URL containing a space or non-printable rune: it is percent-encoded on every request, so the host or path that reaches the other end is not the configured one")
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
			"port must be between 1 and 65535",
			"missing path (the webhook URL's own path carries the credential, so a host-only URL cannot deliver a notification)",
			"contains a space or an invisible character (it is percent-encoded on every request, so the webhook host and path that reach the other end are not the configured ones; remove it, or percent-encode it yourself if it really belongs to the credential)":
		default:
			t.Fatalf("unexpected rejection message %q: a new message must be a fixed constant that cannot embed the operator-supplied URL", err)
		}
	})
}
