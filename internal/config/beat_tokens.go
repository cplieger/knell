package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/cplieger/go/envx"
	"github.com/cplieger/go/httpserver/v2"
)

// maxBeatTokenEntries bounds BEAT_TOKENS: two entries per beat at the
// maxBeats cap, so every beat can overlap an old and a new token during a
// rotation. It also bounds the comparisons TokenSet.Match makes per ping.
const maxBeatTokenEntries = 2 * maxBeats

// beatSetSeparator joins the beat ids one entry covers.
const beatSetSeparator = "+"

// BeatTokens is the parsed BEAT_TOKENS: the token set every ping is matched
// against, and for each entry, in the set's order, the beat ids its token may
// ping. It keeps no token after Load: TokenSet holds digests only.
type BeatTokens struct {
	Set    httpserver.TokenSet
	Covers [][]string
}

// errBeatTokensSetButEmpty and errBeatTokensRequired are the two diagnoses an
// absent BEAT_TOKENS gets, kept apart because the operator's next move differs.
var (
	errBeatTokensSetButEmpty = errors.New("BEAT_TOKENS is set but empty. Give each beat its own token, one <beats>:<token> entry per line, such as backup:<the output of `openssl rand -hex 16`>, or point BEAT_TOKENS_FILE at a file holding them")
	errBeatTokensRequired    = errors.New("BEAT_TOKENS is required. Give each beat its own token, one <beats>:<token> entry per line, such as backup:<the output of `openssl rand -hex 16`>, or point BEAT_TOKENS_FILE at a file holding them")
)

// removedBeatTokenVars are v3's shared-token settings, which v4 no longer
// reads.
var removedBeatTokenVars = []string{"BEAT_TOKEN", "BEAT_TOKEN_FILE"}

// removedBeatTokenSet reports whether a removed shared-token variable is
// still present in the environment.
func removedBeatTokenSet() bool {
	return slices.ContainsFunc(removedBeatTokenVars, func(key string) bool {
		_, ok := os.LookupEnv(key)
		return ok
	})
}

// loadBeatTokens reads the required BEAT_TOKENS, or its _FILE form, and
// parses it against the configured beats.
func loadBeatTokens(beats []Beat) (BeatTokens, envx.SecretSource, error) {
	raw, src, err := resolveSecret("BEAT_TOKENS", errBeatTokensSetButEmpty, errBeatTokensRequired)
	if err != nil {
		// A diagnostic for the v3 upgrade, not an accepted spelling.
		if src == envx.SecretSourceNone && removedBeatTokenSet() {
			err = fmt.Errorf("%w. BEAT_TOKEN was removed in v4: move its token into BEAT_TOKENS as <beat>+<beat>:<token>, listing every beat it pinged", err)
		}
		return BeatTokens{}, src, err
	}
	tokens, err := parseBeatTokens(raw, beats)
	if err != nil {
		return BeatTokens{}, src, fileSourcedValueError("BEAT_TOKENS", src, err)
	}
	if removedBeatTokenSet() {
		slog.Warn("BEAT_TOKEN was removed in v4 and is ignored; unset it to keep the old credential out of the process environment",
			"variables", strings.Join(removedBeatTokenVars, ","))
	}
	return tokens, src, nil
}

// parseBeatTokens parses BEAT_TOKENS: one <beats>:<token> entry per line,
// split at the first colon, where <beats> is one beat id or several joined
// with "+". Blank lines are skipped. A line break is the only separator,
// because checkBeatToken refuses CR and LF inside a token. No error quotes a
// byte of an entry, since an entry written token-first puts the secret where
// the beat ids belong: errors name the line number and ids from BEATS only.
func parseBeatTokens(raw string, beats []Beat) (BeatTokens, error) {
	configured := make(map[string]bool, len(beats))
	for _, b := range beats {
		configured[b.ID] = true
	}
	var (
		tokens []string
		lines  []int
		out    BeatTokens
	)
	covered := make(map[string]bool, len(beats))
	for i, line := range strings.Split(raw, "\n") {
		n := i + 1
		if strings.Trim(line, asciiWhitespace) == "" {
			continue
		}
		ids, token, err := parseBeatTokenEntry(line, configured, n)
		if err != nil {
			return BeatTokens{}, err
		}
		if len(tokens) == maxBeatTokenEntries {
			return BeatTokens{}, fmt.Errorf("BEAT_TOKENS has more than %d entries, two per beat at the %d-beat maximum", maxBeatTokenEntries, maxBeats)
		}
		for _, id := range ids {
			covered[id] = true
		}
		tokens = append(tokens, token)
		lines = append(lines, n)
		out.Covers = append(out.Covers, ids)
	}
	if len(tokens) == 0 {
		return BeatTokens{}, errBeatTokensSetButEmpty
	}
	if uncovered := uncoveredBeats(beats, covered); len(uncovered) > 0 {
		return BeatTokens{}, fmt.Errorf("BEAT_TOKENS gives no token to %s, so no job could ever ping it and it would be reported missing at every deadline. Add an entry for each", strings.Join(uncovered, ", "))
	}
	set, err := httpserver.NewTokenSet(tokens)
	if err != nil {
		return BeatTokens{}, tokenSetError(err, lines)
	}
	out.Set = set
	return out, nil
}

// parseBeatTokenEntry parses line n, one <beats>:<token> entry, into its
// beat ids and its token.
func parseBeatTokenEntry(line string, configured map[string]bool, n int) (ids []string, token string, err error) {
	left, token, found := strings.Cut(line, ":")
	if !found {
		return nil, "", fmt.Errorf("BEAT_TOKENS line %d has no colon. Each line is <beats>:<token>, such as backup:<token> or backup+offsite:<token>", n)
	}
	ids, err = parseBeatSet(left, configured, n)
	if err != nil {
		return nil, "", err
	}
	if tokenErr := checkBeatToken(token, n); tokenErr != nil {
		return nil, "", tokenErr
	}
	return ids, token, nil
}

// uncoveredBeats lists, in BEATS order, the beats no entry covers.
func uncoveredBeats(beats []Beat, covered map[string]bool) []string {
	var uncovered []string
	for _, b := range beats {
		if !covered[b.ID] {
			uncovered = append(uncovered, b.ID)
		}
	}
	return uncovered
}

// parseBeatSet parses an entry's <beats>: one or more beat ids joined with
// "+", each in BEATS. An id given twice is inert and kept once.
func parseBeatSet(left string, configured map[string]bool, line int) ([]string, error) {
	var ids []string
	for id := range strings.SplitSeq(left, beatSetSeparator) {
		id = strings.Trim(id, asciiWhitespace)
		if !beatIDPattern.MatchString(id) || !configured[id] {
			return nil, fmt.Errorf("BEAT_TOKENS line %d names a beat that is not in BEATS. Each line is <beats>:<token>, the beat ids first, joined with +", line)
		}
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// tokenSetError maps NewTokenSet's refusal of a repeated token, the one its
// earlier checks leave, to BEAT_TOKENS line numbers.
func tokenSetError(err error, lines []int) error {
	var tse *httpserver.TokenSetError
	if errors.As(err, &tse) && errors.Is(err, httpserver.ErrTokenRepeated) {
		return fmt.Errorf("BEAT_TOKENS line %d repeats the token of line %d, so one token would ping the beats of both. Give each its own token, or join the beats on one line with +", lines[tse.Index], lines[tse.First])
	}
	return fmt.Errorf("BEAT_TOKENS could not be loaded: %w", err)
}

// beatTokenFieldError words httpserver.CheckFieldValue's refusal of line's token.
func beatTokenFieldError(err error, line int) error {
	switch {
	case errors.Is(err, httpserver.ErrFieldInvisible):
		return fmt.Errorf("BEAT_TOKENS line %d has a token with an invisible character, such as a zero-width space or a bidi control, so the token you read is not the one a sender must present. Retype it, or use `openssl rand -hex 16`", line)
	case errors.Is(err, httpserver.ErrFieldInvalidUTF8):
		return fmt.Errorf("BEAT_TOKENS line %d has a token that is not valid UTF-8, so it cannot be read or retyped as written. Use `openssl rand -hex 16`", line)
	default:
		return fmt.Errorf("BEAT_TOKENS line %d has a token with a control character that HTTP forbids in a header value, so no sender can present it. Use a token of at least %d printable characters", line, minTokenLength)
	}
}

// checkBeatToken validates one entry's token as the exact credential senders
// present, or refuses it naming its line. It never rewrites the value: edge
// ASCII whitespace and interior forbidden bytes are refused rather than
// trimmed, since such a token is unsendable as written or unreadable in the
// value the operator holds.
func checkBeatToken(token string, line int) error {
	if strings.Trim(token, asciiWhitespace) != token {
		return fmt.Errorf("BEAT_TOKENS line %d has a token with leading or trailing ASCII whitespace. A trailing space or tab is stripped from the header on the wire, CR, LF, VT and FF cannot be sent in one, and a leading one is invisible in the value you read, so the token would never match. Remove the whitespace, and save the file with LF line endings", line)
	}
	if err := httpserver.CheckFieldValue(token); err != nil {
		return beatTokenFieldError(err, line)
	}
	if len(token) < minTokenLength {
		return fmt.Errorf("BEAT_TOKENS line %d has a token shorter than the %d-byte minimum. A token short enough to guess lets a stranger who can reach this port keep the beats on that line reading fresh while their jobs are dead. Use `openssl rand -hex 16`", line, minTokenLength)
	}
	if len(token) > maxTokenLength {
		return fmt.Errorf("BEAT_TOKENS line %d has a token longer than the %d-byte maximum, the token's share of the %d-byte header block knell reads. A real token is far shorter, so check that BEAT_TOKENS_FILE names the token file and not a bundle the mount picked up", line, maxTokenLength, MaxRequestHeaderBytes)
	}
	return nil
}
