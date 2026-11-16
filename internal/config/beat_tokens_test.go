package config

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/go/slogx/capture"
)

// tokenBeats is the BEATS every grammar case parses against.
var tokenBeats = []Beat{
	{ID: "backup", Deadline: time.Hour},
	{ID: "offsite", Deadline: time.Hour},
	{ID: "watchdog", Deadline: time.Hour},
}

// Two distinct tokens that clear every checkBeatToken rule.
const (
	tokenA = "aaaaaaaaaaaaaaaa-token-a"
	tokenB = "bbbbbbbbbbbbbbbb-token-b"
)

// covering returns the beat ids token covers in tokens, or nil when no entry
// holds it.
func covering(tokens BeatTokens, token string) []string {
	i, ok := tokens.Set.Match(token)
	if !ok {
		return nil
	}
	return tokens.Covers[i]
}

// TestParseBeatTokensGivesEachTokenItsOwnBeats pins the grammar: one entry per
// line, split at the first colon, a "+" set of ids, blank lines skipped, the
// token kept byte for byte, commas and colons included.
func TestParseBeatTokensGivesEachTokenItsOwnBeats(t *testing.T) {
	t.Parallel()
	raw := "\n  backup + offsite :" + tokenA + "\n\n watchdog:" + tokenB + ",with:comma\n"
	tokens, err := parseBeatTokens(raw, tokenBeats)
	if err != nil {
		t.Fatalf("parseBeatTokens: %v", err)
	}
	if got := covering(tokens, tokenA); !slices.Equal(got, []string{"backup", "offsite"}) {
		t.Errorf("token A covers %v, want [backup offsite]", got)
	}
	if got := covering(tokens, tokenB+",with:comma"); !slices.Equal(got, []string{"watchdog"}) {
		t.Errorf("token B, with a comma and a colon inside, covers %v, want [watchdog]: only a line break separates entries and only the first colon splits one", got)
	}
	if got := covering(tokens, tokenB); got != nil {
		t.Errorf("a prefix of token B covers %v, want nothing: the token is compared byte for byte", got)
	}
	if tokens.Set.Len() != 2 {
		t.Errorf("Set.Len() = %d, want 2: blank lines are not entries", tokens.Set.Len())
	}
}

// TestParseBeatTokensRefusals pins every refusal, and that none quotes an
// entry: an entry written token-first puts the secret where the ids belong,
// and a hex token matches the beat-id grammar.
func TestParseBeatTokensRefusals(t *testing.T) {
	t.Parallel()
	many := strings.Builder{}
	for i := range maxBeatTokenEntries + 1 {
		many.WriteString("backup+offsite+watchdog:" + tokenA + strconv.Itoa(i) + "\n")
	}
	cases := map[string]struct {
		raw  string
		want string
	}{
		"no colon":              {raw: "backup:" + tokenA + "\n" + tokenB, want: "line 2 has no colon"},
		"token first":           {raw: "swapped-token-placed-first:backup\noffsite+watchdog:" + tokenB, want: "line 1 names a beat that is not in BEATS"},
		"an all-beats wildcard": {raw: "*:" + tokenA, want: "line 1 names a beat that is not in BEATS"},
		"an empty beat set":     {raw: " :" + tokenA, want: "line 1 names a beat that is not in BEATS"},
		"an empty id in a set":  {raw: "backup++offsite+watchdog:" + tokenA, want: "line 1 names a beat that is not in BEATS"},
		"a beat left uncovered": {raw: "backup+offsite:" + tokenA, want: "gives no token to watchdog"},
		"a repeated token":      {raw: "backup:" + tokenA + "\n\noffsite+watchdog:" + tokenA, want: "line 3 repeats the token of line 1"},
		"a short token":         {raw: "backup+offsite+watchdog:shorty", want: "line 1 has a token shorter than the 16-byte minimum"},
		"a CRLF file":           {raw: "backup:" + tokenA + "\r\noffsite+watchdog:" + tokenB + "\r\n", want: "line 1 has a token with leading or trailing ASCII whitespace"},
		"too many entries":      {raw: many.String(), want: "more than 128 entries"},
		"nothing but blanks":    {raw: "\n \n\t\n", want: "BEAT_TOKENS is set but empty"},
	}
	secrets := []string{tokenA, tokenB, "swapped-token-placed-first", "shorty"}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := parseBeatTokens(tc.raw, tokenBeats)
			if err == nil {
				t.Fatal("parseBeatTokens accepted it, want a startup refusal")
			}
			msg := err.Error()
			if !strings.Contains(msg, tc.want) {
				t.Errorf("error = %q, want it to say %q", msg, tc.want)
			}
			for _, secret := range secrets {
				if strings.Contains(msg, secret) {
					t.Errorf("error = %q quotes %q from the entries: the startup log ships wider than the token file", msg, secret)
				}
			}
			assertTokenMessageShape(t, msg)
		})
	}
}

// removedTokenName matches BEAT_TOKEN anywhere but as the prefix of
// BEAT_TOKENS.
var removedTokenName = regexp.MustCompile(`BEAT_TOKEN(?:[^S]|$)`)

// assertTokenMessageShape pins the v4 wording of a token refusal: it names
// BEAT_TOKENS and never the removed shared token, and never v3's claim that
// one token guards every beat.
func assertTokenMessageShape(t *testing.T, msg string) {
	t.Helper()
	if !strings.HasPrefix(msg, "BEAT_TOKENS") {
		t.Errorf("error = %q, want it to start with BEAT_TOKENS", msg)
	}
	if removedTokenName.MatchString(msg) {
		t.Errorf("error = %q names the removed BEAT_TOKEN", msg)
	}
	if strings.Contains(msg, "every beat") {
		t.Errorf("error = %q describes v3's single token: a v4 token exposes only the beats its entry names", msg)
	}
}

// TestCheckBeatTokenMessagesNameTheirLine pins each token rule's message to
// its BEAT_TOKENS line, with the short-token one stating what a v4 token
// exposes.
func TestCheckBeatTokenMessagesNameTheirLine(t *testing.T) {
	t.Parallel()
	for name, token := range map[string]string{
		"padded":          " " + tokenA,
		"a control byte":  "aaaaaaaa\x01bbbbbbbb",
		"too short":       "shorty",
		"too long":        strings.Repeat("a", maxTokenLength+1),
		"a lone carriage": tokenA + "\r",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := checkBeatToken(token, 7)
			if err == nil {
				t.Fatal("checkBeatToken accepted it")
			}
			if !strings.HasPrefix(err.Error(), "BEAT_TOKENS line 7 ") {
				t.Errorf("error = %q, want it to start with \"BEAT_TOKENS line 7 \"", err)
			}
			assertTokenMessageShape(t, err.Error())
		})
	}
	if err := checkBeatToken("shorty", 1); !strings.Contains(err.Error(), "the beats on that line") {
		t.Errorf("short-token error = %q, want it to say the token exposes the beats on its line", err)
	}
}

// TestLoadNamesTheRemovedBeatToken pins the v3 upgrade diagnostics: a v3
// environment with only BEAT_TOKEN is refused with the move named, and a left
// over BEAT_TOKEN beside BEAT_TOKENS draws a warning to unset it.
func TestLoadNamesTheRemovedBeatToken(t *testing.T) {
	t.Run("only the removed variable", func(t *testing.T) {
		setValidLoadEnv(t)
		unsetEnv(t, "BEAT_TOKENS")
		t.Setenv("BEAT_TOKEN", validBeatToken)
		_, err := Load(maxNodeNameBytes)
		if err == nil || !strings.Contains(err.Error(), "BEAT_TOKEN was removed in v4") {
			t.Fatalf("Load() = %v, want the refusal to name the removed BEAT_TOKEN and its move", err)
		}
		if strings.Contains(err.Error(), validBeatToken) {
			t.Errorf("refusal quotes the old token: %q", err)
		}
	})
	t.Run("both, the removed one left over", func(t *testing.T) {
		setValidLoadEnv(t)
		t.Setenv("BEAT_TOKEN", "old-v3-shared-token")
		rec := capture.Default(t)
		if _, err := Load(maxNodeNameBytes); err != nil {
			t.Fatalf("Load() = %v, want nil: BEAT_TOKENS is set", err)
		}
		if !rec.Contains("BEAT_TOKEN was removed in v4 and is ignored") {
			t.Errorf("no warning to unset the removed BEAT_TOKEN; log %v", rec.Messages())
		}
		if strings.Contains(strings.Join(rec.Messages(), "\n"), "old-v3-shared-token") {
			t.Error("the warning quotes the old token")
		}
	})
	t.Run("neither", func(t *testing.T) {
		setValidLoadEnv(t)
		unsetEnv(t, "BEAT_TOKENS")
		_, err := Load(maxNodeNameBytes)
		if err == nil || strings.Contains(err.Error(), "removed in v4") {
			t.Fatalf("Load() = %v, want the plain required refusal with no v3 sentence", err)
		}
	})
}

// TestLoadReadsAOneEntryCRLFTokenFile pins that a one-line token file saved
// with CRLF loads: envx strips one trailing line ending, CRLF included.
func TestLoadReadsAOneEntryCRLFTokenFile(t *testing.T) {
	setValidLoadEnv(t)
	path := filepath.Join(t.TempDir(), "beat_tokens")
	if err := os.WriteFile(path, []byte("api:"+validBeatToken+"\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	unsetEnv(t, "BEAT_TOKENS")
	t.Setenv("BEAT_TOKENS_FILE", path)
	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}
	if !hasToken(cfg, validBeatToken) {
		t.Error("the token from a one-line CRLF file does not match")
	}
}

// FuzzParseBeatTokens pins two properties over arbitrary BEAT_TOKENS: an
// accepted parse matches every entry's token for exactly its beats, and a
// refusal never quotes a token-sized run of either side of any entry.
func FuzzParseBeatTokens(f *testing.F) {
	for _, seed := range []string{
		"backup+offsite+watchdog:" + tokenA,
		"backup:" + tokenA + "\noffsite+watchdog:" + tokenB,
		tokenA + ":backup",
		"*:" + tokenA,
		"backup:" + tokenA + "\r\noffsite+watchdog:" + tokenB,
		"backup+offsite+watchdog:" + tokenA + "\nbackup:" + tokenA,
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		tokens, err := parseBeatTokens(raw, tokenBeats)
		if err != nil {
			for line := range strings.SplitSeq(raw, "\n") {
				left, right, _ := strings.Cut(line, ":")
				for _, side := range []string{left, right} {
					if len(side) >= minTokenLength && strings.Contains(err.Error(), side) {
						t.Fatalf("refusal %q quotes %q from the entries", err, side)
					}
				}
			}
			return
		}
		for line := range strings.SplitSeq(raw, "\n") {
			left, token, found := strings.Cut(line, ":")
			if !found {
				continue
			}
			got := covering(tokens, token)
			for id := range strings.SplitSeq(left, beatSetSeparator) {
				if id = strings.Trim(id, asciiWhitespace); !slices.Contains(got, id) {
					t.Fatalf("token of line %q covers %v, missing %q", line, got, id)
				}
			}
		}
	})
}
