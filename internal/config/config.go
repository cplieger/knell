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

// MaxBeats caps how many beats one instance will watch. The cap keeps the
// metric label space and the notification fan-out operator-bounded; a config
// past it is almost certainly a generator bug.
const MaxBeats = 64

// minDeadline is the smallest accepted beat deadline. Anything shorter turns
// transient sender hiccups into alert spam; a sender that beats more often
// than every 30 seconds still works with a longer deadline.
const minDeadline = 30 * time.Second

// minTokenLength is the shortest BEAT_TOKEN that does not draw a startup
// warning: anything shorter is realistically guessable, so operators are
// nudged toward a long random value (the check stays warn-only; the gate
// still arms).
const minTokenLength = 16

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

// Load reads the environment and returns the validated configuration.
// BEATS and DISCORD_WEBHOOK_URL are required; everything else has a default.
func Load() (Config, error) {
	var cfg Config

	rawBeats, err := envx.Require("BEATS")
	if err != nil {
		return cfg, fmt.Errorf("BEATS is required (e.g. \"api:20m,backup:26h\"): %w", err)
	}
	beats, err := ParseBeats(rawBeats)
	if err != nil {
		return cfg, fmt.Errorf("parsing BEATS: %w", err)
	}
	cfg.Beats = beats

	webhook, err := loadWebhook()
	if err != nil {
		return cfg, err
	}
	cfg.WebhookURL = webhook

	cfg.Node = nodeName()

	cfg.ListenAddr = envx.String("LISTEN_ADDR", ":9190")

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
// hostname, else "unknown".
func nodeName() string {
	if node := envx.String("NODE_NAME", ""); node != "" {
		return node
	}
	host, err := os.Hostname()
	if err != nil {
		slog.Warn("failed to determine hostname, using fallback node name", "node", "unknown", "error", err)
		return "unknown"
	}
	return host
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

// loadBeatToken reads the optional BEAT_TOKEN bearer gate for
// POST/GET /beat/{id}; an empty return disables the check. BEAT_TOKEN_FILE
// points at a mounted secret file instead (the same convention
// DISCORD_WEBHOOK_URL uses), keeping the credential out of `docker inspect`
// output.
func loadBeatToken() (string, error) {
	if blankErr := rejectBlankFileVar("BEAT_TOKEN"); blankErr != nil {
		return "", blankErr
	}
	token, tokenSrc, err := envx.SecretWithSource("BEAT_TOKEN")
	if tokenSrc == envx.SourceFile && os.Getenv("BEAT_TOKEN") != "" {
		slog.Warn("BEAT_TOKEN and BEAT_TOKEN_FILE are both set; the file wins and the plain variable is ignored, so unset it to keep the token out of the process environment")
	}
	switch {
	case err == nil:
		// Same reason as the webhook: a padded token makes every sender 401
		// and every beat cross its deadline. A value that is ENTIRELY
		// whitespace is kept verbatim instead: an empty token is the
		// documented open-endpoint sentinel (webapi builds no verifier for
		// it), so trimming would silently disarm the gate the operator did
		// set — while the same value via BEAT_TOKEN_FILE fails startup.
		if trimmed := strings.TrimSpace(token); trimmed != "" {
			token = trimmed
		}
	case errors.As(err, new(*envx.MissingError)):
		// Neither BEAT_TOKEN nor BEAT_TOKEN_FILE is set: the documented
		// open-endpoint case, so the empty token stands and webapi's gate
		// never arms.
		token = ""
	default:
		// Any other error means the variable WAS provided and could not be
		// used (unreadable or blank _FILE): fail closed rather than serving
		// an open endpoint the operator meant to gate.
		return "", fmt.Errorf("BEAT_TOKEN: %w", err)
	}
	if token != "" && len(token) < minTokenLength {
		slog.Warn("BEAT_TOKEN is shorter than the recommended minimum; a short token is guessable, prefer a long random value", "length", len(token), "minimum", minTokenLength)
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
	if src == envx.SourceFile && os.Getenv("DISCORD_WEBHOOK_URL") != "" {
		slog.Warn("DISCORD_WEBHOOK_URL and DISCORD_WEBHOOK_URL_FILE are both set; the file wins and the plain variable is ignored, so unset it to keep the webhook URL out of the process environment")
	}
	if err == nil {
		// envx trims the _FILE branch only; a plain variable copied from a
		// deployment file can carry padding. A trailing space survives
		// url.Parse and is escaped as %20 on every POST, so Discord answers
		// 404 forever and the switch can never ring.
		webhook = strings.TrimSpace(webhook)
	}
	if err != nil {
		if errors.As(err, new(*envx.MissingError)) {
			return "", fmt.Errorf("DISCORD_WEBHOOK_URL is required: %w", err)
		}
		// Provided via _FILE but unreadable/empty: not a missing-variable case.
		return "", fmt.Errorf("DISCORD_WEBHOOK_URL: %w", err)
	}
	if _, err := parseWebhookURL(webhook); err != nil {
		return "", fmt.Errorf("DISCORD_WEBHOOK_URL: %w", err)
	}
	return webhook, nil
}

// ParseBeats parses the BEATS spec list: comma-separated "id:deadline"
// entries, e.g. "watchdog-mimir:20m,watchdog-loki:20m". IDs must match
// [A-Za-z0-9][A-Za-z0-9_-]{0,63} and be unique; deadlines are Go durations
// of at least minDeadline.
func ParseBeats(raw string) ([]Beat, error) {
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
	if len(beats) > MaxBeats {
		return nil, fmt.Errorf("%d beats configured, maximum is %d", len(beats), MaxBeats)
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
