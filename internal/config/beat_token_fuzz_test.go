package config

import (
	"bufio"
	"bytes"
	"net/http"
	"net/textproto"
	"testing"

	"github.com/cplieger/slogx/capture"
)

// FuzzCheckBeatToken pins the invariant the BEAT_TOKEN gate rests on: every
// token knell ACCEPTS must reach webapi's verifier as the operator configured
// it. webapi compares the Authorization header against "Bearer "+token, so a
// token the transport alters or refuses arms a gate that 401s every ping while
// startup reports itself gated — and one deadline later every configured beat
// posts a false MISSING notice.
//
// The oracle is the transport, not this package's own constants:
// TestBeatTokenFitsHeaderAgreesWithTheTransportForEveryByte proves
// beatTokenFitsHeader agrees with the wire for the INTERIOR bytes of one fixed
// shape, while the edge cutset (asciiWhitespace) and the empty, blank and
// invisible-edge verdicts are asserted only against hand-picked values. Fuzzing
// the whole validator against a header round trip covers the combinations no
// table enumerates: mixed edges, multi-byte edges, interior padding beside an
// invisible edge.
func FuzzCheckBeatToken(f *testing.F) {
	for _, seed := range []string{
		"unit-test-beat-token",
		"",
		" ",
		"\t",
		" secret",
		"secret ",
		"  secret  ",
		"secret\n",
		"alpha\nbeta",
		"alpha\rbeta",
		"alpha\vbeta",
		"alpha\fbeta",
		"alpha\x00beta",
		"alpha\x7fbeta",
		"alpha\tbeta",
		"\u00a0",
		" \u00a0 ",
		"\u00a0secret",
		"secret\u200b",
		"\ufeffsecret",
		"t\u00f6k\u00e9n-with-h\u00efgh-bytes",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, token string) {
		// checkBeatToken warns on two of the shapes it accepts; capture keeps
		// the fuzz output readable and restores the default logger per input.
		_ = capture.Default(t)

		if err := checkBeatToken(token); err != nil {
			return
		}
		if token == "" {
			t.Fatal("accepted an empty token: BEAT_TOKEN is required and an empty value is no credential at all, so accepting it would leave webapi's gate with nothing to verify against")
		}
		if len(token) < minTokenLength {
			t.Fatalf("accepted a %d-byte token: the gate is /beat/{id}'s only defense, so a value under the %d-byte floor is guessable and must fail startup", len(token), minTokenLength)
		}
		if len(token) > maxTokenLength {
			t.Fatalf("accepted a %d-byte token: knell reads at most %d bytes of request headers, so a value past the %d-byte maximum cannot travel and every ping would be answered 431 by an endpoint reporting itself gated", len(token), MaxRequestHeaderBytes, maxTokenLength)
		}
		got, err := wireAuthorizationValue(t, token)
		if err != nil {
			t.Fatalf("accepted token %q that cannot be carried in an HTTP header value (%v): no sender could present it, so every ping 401s against an endpoint that reports itself gated", token, err)
		}
		if want := "Bearer " + token; got != want {
			t.Fatalf("accepted token %q: the wire delivers %q while webapi compares against %q; the configured value and the authenticating value differ, so every ping 401s and one deadline later every beat posts a false MISSING notice", token, got, want)
		}
	})
}

// wireAuthorizationValue reports the Authorization value a verifier reads after
// "Bearer "+token has made the round trip through net/http's header writer and
// net/textproto's header reader — the two normalizations that decide whether a
// configured credential reaches webapi unchanged. It deliberately restates
// neither asciiWhitespace nor beatTokenFitsHeader: it asks the transport, so
// narrowing either one cannot relax the assertion along with the code.
func wireAuthorizationValue(t *testing.T, token string) (string, error) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://knell.invalid/beat/api", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	var wire bytes.Buffer
	if err := req.Write(&wire); err != nil {
		return "", err
	}
	reader := bufio.NewReader(&wire)
	if _, err := reader.ReadString('\n'); err != nil {
		return "", err
	}
	header, err := textproto.NewReader(reader).ReadMIMEHeader()
	if err != nil {
		return "", err
	}
	return header.Get("Authorization"), nil
}
