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

// MinDeadline is the smallest accepted beat deadline. Anything shorter turns
// transient sender hiccups into alert spam; a sender that beats more often
// than every 30 seconds still works with a longer deadline.
const MinDeadline = 30 * time.Second

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

	// BEAT_TOKEN optionally gates POST/GET /beat/{id}; empty disables the
	// check. BEAT_TOKEN_FILE points at a mounted secret file instead (the
	// same convention DISCORD_WEBHOOK_URL uses), keeping the credential out
	// of `docker inspect` output.
	if os.Getenv("BEAT_TOKEN") != "" || os.Getenv("BEAT_TOKEN_FILE") != "" {
		token, err := envx.Secret("BEAT_TOKEN")
		if err != nil {
			return cfg, fmt.Errorf("BEAT_TOKEN: %w", err)
		}
		cfg.BeatToken = token
	}
	if cfg.BeatToken != "" && len(cfg.BeatToken) < 16 {
		slog.Warn("BEAT_TOKEN is shorter than 16 bytes; a short token is guessable, prefer a long random value", "length", len(cfg.BeatToken))
	}

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

// loadWebhook reads and shape-checks DISCORD_WEBHOOK_URL. The URL is a
// secret: errors never embed it, and plain http only warns on the scheme.
func loadWebhook() (string, error) {
	webhook, err := envx.Secret("DISCORD_WEBHOOK_URL")
	if err != nil {
		return "", fmt.Errorf("DISCORD_WEBHOOK_URL is required: %w", err)
	}
	if err := validateWebhookURL(webhook); err != nil {
		return "", fmt.Errorf("DISCORD_WEBHOOK_URL: %w", err)
	}
	if strings.HasPrefix(strings.ToLower(webhook), "http://") {
		slog.Warn("DISCORD_WEBHOOK_URL uses plain http; the webhook URL is a secret and will transit unencrypted")
	}
	return webhook, nil
}

// ParseBeats parses the BEATS spec list: comma-separated "id:deadline"
// entries, e.g. "watchdog-mimir:20m,watchdog-loki:20m". IDs must match
// [A-Za-z0-9][A-Za-z0-9_-]{0,63} and be unique; deadlines are Go durations
// of at least MinDeadline.
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
	if deadline < MinDeadline {
		return Beat{}, fmt.Errorf("entry %q: deadline below minimum %s", entry, MinDeadline)
	}
	seen[id] = struct{}{}
	return Beat{ID: id, Deadline: deadline}, nil
}

// validateWebhookURL checks the webhook is an absolute http(s) URL with a
// host. The value is operator-supplied config, so this is a shape check
// against paste accidents, not an SSRF guard.
func validateWebhookURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		// Deliberately not wrapped: a url.Error embeds the raw URL, and the
		// webhook URL is a secret that must not reach the startup error log.
		return errors.New("not a valid URL")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return fmt.Errorf("scheme must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("missing host")
	}
	return nil
}
