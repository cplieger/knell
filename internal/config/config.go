// Package config reads and validates knell's environment configuration.
// All environment reads live here; the rest of the app receives the parsed
// Config. Malformed required values fail startup with a clear error rather
// than falling back: a dead-man watcher silently running with the wrong
// beats or webhook is worse than one that refuses to start.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/cplieger/envx"
	"github.com/cplieger/slogx"
)

// maxBeats caps how many beats one instance will watch. The cap keeps the
// metric label space and the notification fan-out operator-bounded; a config
// past it is almost certainly a generator bug.
const maxBeats = 64

// minDeadline is the smallest accepted beat deadline. Anything shorter turns
// transient sender hiccups into alert spam; a sender that beats more often
// than every 30 seconds still works with a longer deadline.
const minDeadline = 30 * time.Second

// minTokenLength is the shortest BEAT_TOKEN that does not draw a startup
// warning: anything shorter is realistically guessable, so operators are
// nudged toward a long random value (the check stays warn-only; the gate
// still arms).
const minTokenLength = 16

// asciiWhitespace is the cutset of characters that can never carry a bearer
// token through an HTTP header, and therefore the definition of "whitespace
// only" loadBeatToken refuses: net/textproto strips leading and trailing
// SPACE and TAB from every header value, and CR, LF, VT and FF are illegal
// bytes in a field value, so a token built only from these characters reaches
// the verifier as the empty string (or not at all) no matter what the sender
// puts on the wire.
//
// Non-ASCII spaces (NBSP U+00A0, NEL U+0085, U+2000…) are deliberately NOT in
// the set: textproto keeps them, so a token made of them IS presented verbatim
// and DOES authenticate. strings.TrimSpace (unicode.IsSpace) would treat them
// as blank, so using it as the refusal test would fail startup on a working
// configuration.
const asciiWhitespace = " \t\r\n\v\f"

// maxNodeNameBytes caps NODE_NAME so every notification stays inside
// Discord's 2000-character `content` limit. The node name is interpolated
// into EVERY notice (missing, recovered, history), so an unbounded value
// makes Discord reject all of them: knell would start, accept beats and
// detect outages while no notice is ever delivered — a dead-man switch that
// delivers nothing.
//
// The budget, worst case over notify's templates: the longest fixed text is
// the single-outage history notice (54 chars) plus its longest late clause
// (111) = 165, plus a beat id (<= 64 by beatIDPattern), a truncated duration
// (<= 32) and a "2006-01-02 15:04 MST" recovery timestamp (20) = 281
// characters. That leaves ~1719 for the node name, so 256 keeps a wide
// margin while admitting every DNS-legal hostname (253 max). Counting BYTES
// is conservative against Discord's character limit: UTF-8 bytes are always
// >= the character count and >= the UTF-16 code-unit count.
const maxNodeNameBytes = 256

// defaultListenAddr is the listener address used when LISTEN_ADDR is unset or
// blank.
const defaultListenAddr = ":9190"

// beatIDPattern is the accepted beat-id grammar: URL-path and metric-label
// safe, human-readable, bounded.
var beatIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

// Beat is one watched heartbeat: an id senders ping and the silence deadline
// after which the beat is declared missing.
type Beat struct {
	ID       string
	Deadline time.Duration
}

// Config is the fully parsed runtime configuration.
type Config struct {
	WebhookURL string
	Node       string
	ListenAddr string
	BeatToken  string
	Beats      []Beat
	LogLevel   slog.Level
}

// LogValue implements slog.LogValuer so a Config can never publish its own
// secrets. DISCORD_WEBHOOK_URL's path IS the Discord credential and BeatToken
// is the /beat/{id} gate, so both are reported by PRESENCE only: logging a
// Config (or a *Config, whose method set includes this value receiver) stays
// leak-free even from a call site that logs the whole struct instead of the
// hand-picked fields main.go's logConfig chooses today.
//
// The receiver is deliberately a VALUE, not a pointer: a method set on *Config
// leaves a bare Config value (what Load returns, and what run() holds) outside
// slog.LogValuer, so the one call site the redaction exists to survive - a
// future slog call that logs the whole struct by value - would reflection-render
// both secrets. The copy is a startup-frequency 96 bytes.
//
//nolint:gocritic // hugeParam: slog.LogValuer must sit on the value receiver so a bare Config redacts too; the copy happens at most once per config log line.
func (c Config) LogValue() slog.Value {
	webhook := "unset"
	if c.WebhookURL != "" {
		webhook = "configured"
	}
	beatAuth := "open"
	if c.BeatToken != "" {
		beatAuth = "required"
	}
	return slog.GroupValue(
		slog.Int("beats", len(c.Beats)),
		slog.String("node", c.Node),
		slog.String("listen_addr", c.ListenAddr),
		slog.String("webhook", webhook),
		slog.String("beat_auth", beatAuth),
		slog.String("log_level", c.LogLevel.String()),
	)
}

// Load reads the environment and returns the validated configuration.
// BEATS and DISCORD_WEBHOOK_URL are required; everything else has a default.
func Load() (Config, error) {
	var cfg Config

	rawBeats, err := envx.Require("BEATS")
	if err != nil {
		return cfg, fmt.Errorf("BEATS is required (e.g. \"api:20m,backup:26h\"): %w", err)
	}
	beats, err := parseBeats(rawBeats)
	if err != nil {
		return cfg, fmt.Errorf("parsing BEATS: %w", err)
	}
	cfg.Beats = beats

	webhook, err := loadWebhook()
	if err != nil {
		return cfg, err
	}
	cfg.WebhookURL = webhook

	node, err := nodeName()
	if err != nil {
		return cfg, err
	}
	cfg.Node = node

	cfg.ListenAddr = listenAddr()

	beatToken, err := loadBeatToken()
	if err != nil {
		return cfg, err
	}
	cfg.BeatToken = beatToken

	rawLevel := envx.String("LOG_LEVEL", "")
	level, ok := slogx.ParseLevel(rawLevel, slog.LevelInfo)
	if !ok {
		slog.Warn("invalid LOG_LEVEL, using info", "value", rawLevel)
	}
	cfg.LogLevel = level

	return cfg, nil
}

// nodeName resolves the observer name: NODE_NAME when set, else the
// hostname, else "unknown". A NODE_NAME past maxNodeNameBytes fails startup
// like any other malformed required value: the cap (see maxNodeNameBytes for
// the budget) is what guarantees no name can push a notification past
// Discord's content limit, where the switch would arm and never ring. The
// hostname fallback is not length-checked because the kernel
// already bounds it far below the cap (HOST_NAME_MAX is 64 on Linux, 255 by
// POSIX), and refusing to start over the machine's own hostname would trade a
// deliverable notice for no notice at all.
func nodeName() (string, error) {
	node := strings.TrimSpace(envx.String("NODE_NAME", ""))
	if node == "" {
		return hostnameNode(), nil
	}
	if len(node) > maxNodeNameBytes {
		return "", fmt.Errorf("NODE_NAME is %d bytes, maximum is %d: the node name prefixes every Discord notification, and the cap keeps every notice far inside Discord's 2000-character content limit (an unbounded name would make Discord reject them all)", len(node), maxNodeNameBytes)
	}
	return node, nil
}

// hostnameNode is the NODE_NAME fallback: the hostname, else "unknown". A
// missing or blank hostname is a warning rather than a startup failure — the
// notices stay deliverable and attributable to something, which beats not
// arming the switch at all.
func hostnameNode() string {
	host, err := os.Hostname()
	if err != nil {
		slog.Warn("failed to determine hostname, using fallback node name", "node", "unknown", "error", err)
		return "unknown"
	}
	if host = strings.TrimSpace(host); host == "" {
		slog.Warn("hostname is blank, using fallback node name", "node", "unknown")
		return "unknown"
	}
	return host
}

// listenAddr resolves the listener address: LISTEN_ADDR when set, else
// defaultListenAddr. A padded LISTEN_ADDR copied from a deployment file is not
// a usable address (net.Listen resolves " :9190" as a hostname lookup and
// fails), and the padding is invisible in the resulting crash-loop log line.
// Trimmed for the same reason the plain webhook and token values are trimmed; a
// value that is entirely whitespace falls back to the default rather than to
// "", which would bind an ephemeral port and hide the listener from scrapes.
func listenAddr() string {
	if addr := strings.TrimSpace(envx.String("LISTEN_ADDR", "")); addr != "" {
		return addr
	}
	return defaultListenAddr
}

// rejectBlankFileVar fails startup when a `_FILE` variable is PRESENT but
// empty. envx gates its file channel on a non-empty value, so an empty
// `_FILE` is indistinguishable from unset and silently falls back to the
// plain variable — which for BEAT_TOKEN is fail-OPEN (an unauthenticated
// /beat/{id}). Compose interpolation of an unset variable produces exactly
// this shape, so it is checked here rather than delegated to envx.
func rejectBlankFileVar(key string) error {
	if path, ok := os.LookupEnv(key + "_FILE"); ok && strings.TrimSpace(path) == "" {
		return fmt.Errorf("%s_FILE is set but empty: unset it to configure %s directly, or point it at a secret file", key, key)
	}
	return nil
}

// warnPlainVarIgnored reports that KEY_FILE supplied the secret while the
// plain KEY was also set, so the plain variable was ignored. envx documents
// this composition as the caller's policy (SecretWithSource reports the
// source on its error paths too); subject names the credential in the
// operator's own vocabulary. Call it only once the secret read SUCCEEDED — on
// an error path the file supplied nothing, so there is no winner to report and
// the plain variable was never consulted. The message is static and the
// variable names ride attributes, so one Loki query covers both credentials
// and can filter on which variable was ignored.
func warnPlainVarIgnored(key, subject string, src envx.SecretSource) {
	if src != envx.SourceFile || os.Getenv(key) == "" {
		return
	}
	slog.Warn("both the plain variable and its _FILE companion are set; the file wins and the plain variable is ignored, so unset it to keep the credential out of the process environment",
		"variable", key, "file_variable", key+"_FILE", "credential", subject)
}

// loadBeatToken reads the optional BEAT_TOKEN bearer gate for
// POST/GET /beat/{id}; an empty return disables the check. Optional means
// ABSENT, not blank: a present-but-empty BEAT_TOKEN fails startup, and so
// does one that is only ASCII whitespace (a token no sender could ever
// present), like an empty BEAT_TOKEN_FILE. BEAT_TOKEN_FILE points at a
// mounted secret file instead (the same convention DISCORD_WEBHOOK_URL uses),
// keeping the credential out of `docker inspect` output.
func loadBeatToken() (string, error) {
	if err := rejectBlankFileVar("BEAT_TOKEN"); err != nil {
		return "", err
	}
	token, tokenSrc, err := envx.SecretWithSource("BEAT_TOKEN")
	switch {
	case err == nil:
		warnPlainVarIgnored("BEAT_TOKEN", "token", tokenSrc)
		// Same reason as the webhook: a padded token makes every sender 401
		// and every beat cross its deadline, so padding is trimmed. A value
		// that is ENTIRELY ASCII whitespace cannot be presented at all (see
		// asciiWhitespace), so it fails startup like a present-but-empty
		// BEAT_TOKEN and a blank BEAT_TOKEN_FILE: keeping it armed reported
		// a gated endpoint that rejected every ping.
		switch trimmed := strings.TrimSpace(token); {
		case trimmed != "":
			token = trimmed
		case strings.Trim(token, asciiWhitespace) == "":
			return "", errors.New("BEAT_TOKEN is set but contains only whitespace: HTTP strips the leading and trailing spaces and tabs from a header value, so no sender can present this token and POST /beat/{id} would reject every ping while the endpoint reports itself gated; set it to a long random token, or unset the variable entirely to serve /beat/{id} open on purpose")
		default:
			// All whitespace by Unicode rules, but at least one rune
			// survives the header (a non-ASCII space): the token IS
			// presentable, so it is kept verbatim and the gate stays armed.
			// Say so, because the only other signal is the length hint
			// below, which reads as "your token is short" for a value the
			// operator cannot see in `docker inspect` output.
			slog.Warn("BEAT_TOKEN is whitespace only; the /beat/{id} gate is armed with a whitespace token every sender must send verbatim, so set a long random token, or unset the variable to serve the endpoint open")
		}
	case errors.As(err, new(*envx.MissingError)):
		// Neither BEAT_TOKEN nor BEAT_TOKEN_FILE is set: the documented
		// open-endpoint case, so the empty token stands and webapi's gate
		// never arms. A PRESENT-but-empty BEAT_TOKEN lands here too (envx
		// Require cannot tell it from unset) and that is exactly the shape
		// compose interpolation of an undefined variable produces — so it is
		// REFUSED rather than read as consent to an open endpoint, matching
		// rejectBlankFileVar on the _FILE channel, whose own consequence is
		// the same unauthenticated /beat/{id}. Only an ABSENT variable means
		// "serve it open"; an operator who typed the name is asking for the
		// gate, and a startup failure is the one signal an accident cannot
		// be mistaken for intent (the INFO beat_auth=open line looks
		// identical either way).
		if v, ok := os.LookupEnv("BEAT_TOKEN"); ok && v == "" {
			return "", errors.New("BEAT_TOKEN is set but empty: unset the variable entirely to serve /beat/{id} open on purpose, or set it to a long random token")
		}
		token = ""
	default:
		// Any other error means the variable WAS provided and could not be
		// used (unreadable or blank _FILE): fail closed rather than serving
		// an open endpoint the operator meant to gate.
		return "", fmt.Errorf("BEAT_TOKEN: %w", err)
	}
	if token != "" && len(token) < minTokenLength {
		// The exact length is deliberately NOT logged: it is an attribute of a
		// live credential, and knell's startup log is shipped to Loki where its
		// audience is far wider than the age-encrypted file the token itself
		// lives in. The operator set the token, so the number tells them
		// nothing they do not know, while it tells a log reader exactly how
		// large the guess space for an unrate-limited POST /beat/{id} is.
		slog.Warn("BEAT_TOKEN is shorter than the recommended minimum; a short token is guessable, prefer a long random value", "minimum", minTokenLength)
	}
	return token, nil
}

// loadWebhook reads and shape-checks DISCORD_WEBHOOK_URL. The URL is a
// secret: errors never embed it, and only https is accepted — the URL's own
// path carries the webhook credential, so a plain-http webhook would put the
// secret on the wire in cleartext on every notification.
func loadWebhook() (string, error) {
	if err := rejectBlankFileVar("DISCORD_WEBHOOK_URL"); err != nil {
		return "", err
	}
	webhook, src, err := envx.SecretWithSource("DISCORD_WEBHOOK_URL")
	if err != nil {
		if errors.As(err, new(*envx.MissingError)) {
			return "", fmt.Errorf("DISCORD_WEBHOOK_URL is required: %w", err)
		}
		// Provided via _FILE but unreadable/empty: not a missing-variable
		// case, and never a fallback to the plain variable — a webhook the
		// operator meant to configure must fail startup, not go unset.
		return "", fmt.Errorf("DISCORD_WEBHOOK_URL: %w", err)
	}

	warnPlainVarIgnored("DISCORD_WEBHOOK_URL", "webhook URL", src)

	// envx trims the _FILE branch only; a plain variable copied from a
	// deployment file can carry padding. A trailing space survives
	// url.Parse and is escaped as %20 on every POST, so Discord answers
	// 404 forever and the switch can never ring.
	webhook = strings.TrimSpace(webhook)
	if webhook == "" {
		// Whitespace-only: the variable WAS provided, so this is a broken
		// secret pipeline, not a missing setting. Reported as such instead
		// of falling through to the shape check, which would answer
		// "scheme must be https" for a value that carries no scheme.
		return "", errors.New("DISCORD_WEBHOOK_URL is set but empty (whitespace only): point it at the https webhook URL, or use DISCORD_WEBHOOK_URL_FILE")
	}
	if _, err := parseWebhookURL(webhook); err != nil {
		return "", fmt.Errorf("DISCORD_WEBHOOK_URL: %w", err)
	}
	return webhook, nil
}

// parseBeats parses the BEATS spec list: comma-separated "id:deadline"
// entries, e.g. "watchdog-mimir:20m,watchdog-loki:20m". IDs must match
// [A-Za-z0-9][A-Za-z0-9_-]{0,63} and be unique; deadlines are Go durations
// of at least minDeadline.
func parseBeats(raw string) ([]Beat, error) {
	entries := strings.Split(raw, ",")
	beats := make([]Beat, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		b, err := parseBeatEntry(entry, seen)
		if err != nil {
			return nil, err
		}
		beats = append(beats, b)
	}
	if len(beats) == 0 {
		return nil, errors.New("no beats configured")
	}
	if len(beats) > maxBeats {
		return nil, fmt.Errorf("%d beats configured, maximum is %d", len(beats), maxBeats)
	}
	return beats, nil
}

// parseBeatEntry validates one trimmed "id:deadline" entry and records the
// id in seen. Checks run in the documented order: grammar, duplicate,
// duration syntax, minimum deadline.
func parseBeatEntry(entry string, seen map[string]struct{}) (Beat, error) {
	id, rawDeadline, found := strings.Cut(entry, ":")
	if !found {
		return Beat{}, fmt.Errorf("entry %q: expected \"id:deadline\"", entry)
	}
	id = strings.TrimSpace(id)
	if !beatIDPattern.MatchString(id) {
		return Beat{}, fmt.Errorf("entry %q: id must match %s", entry, beatIDPattern)
	}
	if _, dup := seen[id]; dup {
		return Beat{}, fmt.Errorf("entry %q: duplicate id %q", entry, id)
	}
	deadline, err := time.ParseDuration(strings.TrimSpace(rawDeadline))
	if err != nil {
		return Beat{}, fmt.Errorf("entry %q: invalid deadline: %w", entry, err)
	}
	if deadline < minDeadline {
		return Beat{}, fmt.Errorf("entry %q: deadline below minimum %s", entry, minDeadline)
	}
	seen[id] = struct{}{}
	return Beat{ID: id, Deadline: deadline}, nil
}

// parseWebhookURL validates that raw is an absolute HTTPS URL with a host.
// Errors intentionally exclude operator-supplied text because the URL path
// contains the webhook credential. This is a configuration shape check, not
// an SSRF guard.
func parseWebhookURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		// Deliberately not wrapped: a url.Error embeds the raw URL, and the
		// webhook URL is a secret that must not reach the startup error log.
		return nil, errors.New("not a valid URL")
	}
	if u.Scheme != "https" {
		// Deliberately omits the provided scheme: a malformed secret value
		// like "credentialmaterial:rest" parses with the secret prefix as its
		// scheme, and this error reaches the startup log. Naming only the
		// required scheme keeps the message actionable and leak-free.
		return nil, errors.New("scheme must be https (the webhook URL's own path is the credential, so plain http would send it in cleartext)")
	}
	if u.Host == "" {
		return nil, errors.New("missing host")
	}
	return u, nil
}
