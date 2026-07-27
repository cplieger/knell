package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/slogx/capture"
)

func TestMain(m *testing.M) {
	for _, key := range []string{
		"DISCORD_WEBHOOK_URL_FILE",
		"BEAT_TOKEN",
		"BEAT_TOKEN_FILE",
	} {
		if err := os.Unsetenv(key); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}

// setValidLoadEnv sets the minimal environment Load accepts. Tests that
// exercise a variant override individual keys with t.Setenv afterwards.
func setValidLoadEnv(t *testing.T) {
	t.Helper()
	t.Setenv("BEATS", "api:20m")
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/hook")
	t.Setenv("NODE_NAME", "node-1")
}

// unsetEnv removes key for the duration of the test. t.Setenv registers the
// restore of the original value, so the following os.Unsetenv leaves the
// variable absent inside the test and restored afterwards. A plain
// t.Setenv(key, "") would leave a PRESENT-but-empty variable, which Load
// rejects for `_FILE` keys (an empty _FILE is a broken mount, not a fallback).
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unsetting %s: %v", key, err)
	}
}

func TestParseBeats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    []Beat
		wantErr string
	}{
		{
			name: "single beat",
			raw:  "api:20m",
			want: []Beat{{ID: "api", Deadline: 20 * time.Minute}},
		},
		{
			name: "multiple beats with spaces",
			raw:  " watchdog-mimir:20m , watchdog-loki : 20m ,backup:26h",
			want: []Beat{
				{ID: "watchdog-mimir", Deadline: 20 * time.Minute},
				{ID: "watchdog-loki", Deadline: 20 * time.Minute},
				{ID: "backup", Deadline: 26 * time.Hour},
			},
		},
		{
			name: "trailing comma tolerated",
			raw:  "api:20m,",
			want: []Beat{{ID: "api", Deadline: 20 * time.Minute}},
		},
		{
			name: "exact minimum deadline accepted",
			raw:  "api:30s",
			want: []Beat{{ID: "api", Deadline: 30 * time.Second}},
		},
		{
			name: "max length id accepted",
			raw:  strings.Repeat("a", 64) + ":20m",
			want: []Beat{{ID: strings.Repeat("a", 64), Deadline: 20 * time.Minute}},
		},
		{name: "empty", raw: "", wantErr: "no beats"},
		{name: "only commas", raw: ",,,", wantErr: "no beats"},
		{name: "missing deadline", raw: "api", wantErr: "expected"},
		{name: "empty deadline", raw: "api:", wantErr: "invalid deadline"},
		{name: "bare number deadline", raw: "api:30", wantErr: "invalid deadline"},
		{name: "negative deadline", raw: "api:-20m", wantErr: "below minimum"},
		{name: "below minimum deadline", raw: "api:5s", wantErr: "below minimum"},
		{name: "duplicate id", raw: "api:20m,api:30m", wantErr: "duplicate"},
		{name: "invalid id chars", raw: "api beat:20m", wantErr: "id must match"},
		{name: "leading dash id", raw: "-api:20m", wantErr: "id must match"},
		{name: "empty id", raw: ":20m", wantErr: "id must match"},
		{name: "id too long", raw: strings.Repeat("a", 65) + ":20m", wantErr: "id must match"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseBeats(tt.raw)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseBeats(%q) = %v, want error containing %q", tt.raw, got, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseBeats(%q) error = %q, want containing %q", tt.raw, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseBeats(%q) unexpected error: %v", tt.raw, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parseBeats(%q) = %v, want %v", tt.raw, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("beat[%d] = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestParseBeatsMaxCap(t *testing.T) {
	t.Parallel()

	var entries []string
	for r := 'a'; r <= 'z'; r++ {
		for s := 'a'; s <= 'c'; s++ {
			entries = append(entries, string(r)+string(s)+":20m")
		}
	}
	if len(entries) <= maxBeats {
		t.Fatalf("test needs more than %d entries, built %d", maxBeats, len(entries))
	}
	_, err := parseBeats(strings.Join(entries, ","))
	if err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("expected maximum-cap error, got %v", err)
	}
}

func TestParseBeatsAcceptsExactlyMaxCap(t *testing.T) {
	t.Parallel()

	entries := make([]string, 0, maxBeats)
	for r := 'a'; r <= 'h'; r++ {
		for s := 'a'; s <= 'h'; s++ {
			entries = append(entries, string(r)+string(s)+":20m")
		}
	}
	if len(entries) != maxBeats {
		t.Fatalf("test built %d entries, want exactly %d", len(entries), maxBeats)
	}
	beats, err := parseBeats(strings.Join(entries, ","))
	if err != nil {
		t.Fatalf("parseBeats with exactly %d beats = %v, want accepted (the cap is inclusive)", maxBeats, err)
	}
	if len(beats) != maxBeats {
		t.Errorf("len(beats) = %d, want %d", len(beats), maxBeats)
	}
}

func TestLoad(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("LISTEN_ADDR", ":9999")
	t.Setenv("LOG_LEVEL", "debug")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if len(cfg.Beats) != 1 || cfg.Beats[0].ID != "api" {
		t.Errorf("Beats = %+v", cfg.Beats)
	}
	if cfg.WebhookURL != "https://discord.example/hook" {
		t.Errorf("WebhookURL = %q", cfg.WebhookURL)
	}
	if cfg.Node != "node-1" {
		t.Errorf("Node = %q", cfg.Node)
	}
	if cfg.ListenAddr != ":9999" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
	if cfg.LogLevel.String() != "DEBUG" {
		t.Errorf("LogLevel = %v", cfg.LogLevel)
	}
}

func TestLoadDefaultsAndFailures(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("NODE_NAME", "")
	t.Setenv("LISTEN_ADDR", "")
	t.Setenv("LOG_LEVEL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.ListenAddr != ":9190" {
		t.Errorf("ListenAddr default = %q, want :9190", cfg.ListenAddr)
	}

	t.Setenv("BEATS", "")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "BEATS is required") {
		t.Errorf("Load() with empty BEATS error = %v, want it to name BEATS as required", err)
	}

	t.Setenv("BEATS", "api:20m")
	t.Setenv("DISCORD_WEBHOOK_URL", "")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "DISCORD_WEBHOOK_URL is required") {
		t.Errorf("Load() with empty DISCORD_WEBHOOK_URL error = %v, want it to name DISCORD_WEBHOOK_URL as required", err)
	}

	t.Setenv("DISCORD_WEBHOOK_URL", "not-a-url")
	_, err = Load()
	if err == nil || !strings.Contains(err.Error(), "scheme must be https") {
		t.Errorf("Load() with a schemeless DISCORD_WEBHOOK_URL error = %v, want the https-scheme rejection", err)
	}
	if err != nil && strings.Contains(err.Error(), "not-a-url") {
		t.Errorf("error leaks the rejected webhook value: %v", err)
	}
}

func TestLoadInvalidLogLevelFallsBackToInfo(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("LOG_LEVEL", "chatty")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.LogLevel.String() != "INFO" {
		t.Errorf("LogLevel = %v, want INFO (fallback for unknown value)", cfg.LogLevel)
	}
}

func TestLoadRejectsMalformedBeats(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("BEATS", "api:1s")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with below-minimum deadline = nil, want error")
	}
	if !strings.Contains(err.Error(), "parsing BEATS") {
		t.Errorf("error = %q, want it wrapped with \"parsing BEATS\"", err)
	}
}

func TestLoadRejectsPlainHTTPWebhook(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL", "http://127.0.0.1:9/hook")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with a plain-http webhook = nil, want error (the webhook URL is a secret and must not transit in cleartext)")
	}
	if !strings.Contains(err.Error(), "DISCORD_WEBHOOK_URL") {
		t.Errorf("error = %q, want DISCORD_WEBHOOK_URL context", err)
	}
	if strings.Contains(err.Error(), "127.0.0.1") || strings.Contains(err.Error(), "/hook") {
		t.Errorf("error leaks the rejected webhook URL: %v", err)
	}
}

func TestLoadRejectsPlainHTTPWebhookFromFile(t *testing.T) {
	hookFile := filepath.Join(t.TempDir(), "webhook-url")
	if err := os.WriteFile(hookFile, []byte("http://discord.example/file-borne-hook\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL", "")
	t.Setenv("DISCORD_WEBHOOK_URL_FILE", hookFile)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with a plain-http DISCORD_WEBHOOK_URL_FILE = nil, want error: the https gate must apply to the file channel too, or a mounted secret ships the credential in cleartext")
	}
	if !strings.Contains(err.Error(), "scheme must be https") {
		t.Errorf("error = %q, want the https-scheme rejection", err)
	}
	if strings.Contains(err.Error(), "discord.example") || strings.Contains(err.Error(), "file-borne-hook") {
		t.Errorf("error leaks the rejected webhook URL: %v", err)
	}
}

func TestLoadBeatToken(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "unit-test-beat-token")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken != "unit-test-beat-token" {
		t.Errorf("BeatToken = %q, want the configured token (webapi's gate arms only when config carries it)", cfg.BeatToken)
	}
}

func TestLoadBeatTokenDefaultsEmpty(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken != "" {
		t.Errorf("BeatToken = %q, want empty (open endpoint) when BEAT_TOKEN is unset", cfg.BeatToken)
	}
}

func TestLoadShortBeatTokenWarnsWithoutLeakingIt(t *testing.T) {
	// Serial (t.Setenv forbids t.Parallel anyway): swaps the process-global
	// slog default to capture the short-token warning.
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "shorty")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	rec := capture.Default(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() with short BEAT_TOKEN = %v, want accepted (warn, not fail)", err)
	}
	if cfg.BeatToken != "shorty" {
		t.Errorf("BeatToken = %q, want the configured token (short tokens warn but still arm the gate)", cfg.BeatToken)
	}
	if !rec.Contains("BEAT_TOKEN is shorter") {
		t.Errorf("log output %v missing the short-token warning", rec.Messages())
	}
	if rec.Contains("shorty") || rec.AttrContains("", "", "shorty") {
		t.Errorf("log output leaks the token value: %v", rec.Messages())
	}
}

func TestLoadBeatTokenFromFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "beat-token")
	if err := os.WriteFile(tokenFile, []byte("file-borne-beat-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "")
	t.Setenv("BEAT_TOKEN_FILE", tokenFile)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken != "file-borne-beat-token" {
		t.Errorf("BeatToken = %q, want the file-borne token (BEAT_TOKEN_FILE alone must arm the gate, trimmed)", cfg.BeatToken)
	}
}

func TestLoadBeatTokenFileWinsOverPlainVar(t *testing.T) {
	// Serial (t.Setenv forbids t.Parallel anyway): swaps the process-global
	// slog default to assert the both-channels-set warning.
	tokenFile := filepath.Join(t.TempDir(), "beat-token")
	if err := os.WriteFile(tokenFile, []byte("file-borne-beat-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "stale-env-beat-token")
	t.Setenv("BEAT_TOKEN_FILE", tokenFile)

	rec := capture.Default(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken != "file-borne-beat-token" {
		t.Errorf("BeatToken = %q, want the file-borne token: BEAT_TOKEN_FILE wins over BEAT_TOKEN, so a rotated secret file must not be shadowed by a stale plain variable that would 401 every sender", cfg.BeatToken)
	}
	if !rec.Contains("BEAT_TOKEN and BEAT_TOKEN_FILE are both set") {
		t.Errorf("log output %v missing the both-channels-set warning that tells the operator the plain variable is ignored", rec.Messages())
	}
	if rec.Contains("stale-env-beat-token") || rec.AttrContains("", "", "stale-env-beat-token") ||
		rec.Contains("file-borne-beat-token") || rec.AttrContains("", "", "file-borne-beat-token") {
		t.Errorf("log output leaks a token value: %v", rec.Messages())
	}
}

func TestLoadWebhookFromFile(t *testing.T) {
	hookFile := filepath.Join(t.TempDir(), "webhook-url")
	if err := os.WriteFile(hookFile, []byte("https://discord.example/file-borne-hook\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL", "")
	t.Setenv("DISCORD_WEBHOOK_URL_FILE", hookFile)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.WebhookURL != "https://discord.example/file-borne-hook" {
		t.Errorf("WebhookURL = %q, want the file-borne URL (DISCORD_WEBHOOK_URL_FILE is the documented secret-file convention, trimmed)", cfg.WebhookURL)
	}
}

func TestLoadWebhookFileWinsOverPlainVar(t *testing.T) {
	// Serial (t.Setenv forbids t.Parallel anyway): swaps the process-global
	// slog default to assert the both-channels-set warning.
	hookFile := filepath.Join(t.TempDir(), "webhook-url")
	if err := os.WriteFile(hookFile, []byte("https://discord.example/file-borne-hook\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/stale-env-hook")
	t.Setenv("DISCORD_WEBHOOK_URL_FILE", hookFile)

	rec := capture.Default(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.WebhookURL != "https://discord.example/file-borne-hook" {
		t.Errorf("WebhookURL = %q, want the file-borne URL: DISCORD_WEBHOOK_URL_FILE wins, so a rotated webhook must not be shadowed by a stale plain variable every notice would 404 against", cfg.WebhookURL)
	}
	if !rec.Contains("DISCORD_WEBHOOK_URL and DISCORD_WEBHOOK_URL_FILE are both set") {
		t.Errorf("log output %v missing the both-channels-set warning that tells the operator the plain variable is ignored", rec.Messages())
	}
	if rec.Contains("stale-env-hook") || rec.AttrContains("", "", "stale-env-hook") ||
		rec.Contains("file-borne-hook") || rec.AttrContains("", "", "file-borne-hook") {
		t.Errorf("log output leaks a webhook URL: %v", rec.Messages())
	}
}

func TestLoadRejectsUnreadableWebhookFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-webhook")
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/fallback")
	t.Setenv("DISCORD_WEBHOOK_URL_FILE", missing)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with unreadable DISCORD_WEBHOOK_URL_FILE = nil, want error (the secret file must not silently fall back to the environment value)")
	}
	if !strings.Contains(err.Error(), "DISCORD_WEBHOOK_URL") {
		t.Errorf("error = %q, want DISCORD_WEBHOOK_URL context", err)
	}
	if strings.Contains(err.Error(), "discord.example") {
		t.Errorf("error leaks the fallback webhook URL: %v", err)
	}
}

func TestLoadRejectsUnreadableBeatTokenFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-beat-token")
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "env-fallback-token")
	t.Setenv("BEAT_TOKEN_FILE", missing)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with unreadable BEAT_TOKEN_FILE = nil, want error (the secret file must not silently fall back to the environment value, which would arm the gate with the wrong token)")
	}
	if !strings.Contains(err.Error(), "BEAT_TOKEN") {
		t.Errorf("error = %q, want BEAT_TOKEN context", err)
	}
	if strings.Contains(err.Error(), "env-fallback-token") {
		t.Errorf("error leaks the fallback token value: %v", err)
	}
}

func TestLoadRejectsEmptyBeatTokenFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "beat-token")
	// envx trims before its empty check, so a whitespace-only file is the
	// same condition as a zero-byte one.
	if err := os.WriteFile(tokenFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "env-fallback-token")
	t.Setenv("BEAT_TOKEN_FILE", tokenFile)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with an empty BEAT_TOKEN_FILE = nil, want error (an empty secret file is a broken mount, not an open endpoint)")
	}
	if !strings.Contains(err.Error(), "BEAT_TOKEN") {
		t.Errorf("error = %q, want BEAT_TOKEN context", err)
	}
	if strings.Contains(err.Error(), "env-fallback-token") {
		t.Errorf("error leaks the fallback token value: %v", err)
	}
}

func TestLoadRejectsBlankBeatTokenFileVar(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "env-fallback-token")
	t.Setenv("BEAT_TOKEN_FILE", "")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with a present-but-empty BEAT_TOKEN_FILE = nil, want error; envx cannot tell it from unset, so falling back would serve an unauthenticated /beat/{id} the operator meant to gate")
	}
	if !strings.Contains(err.Error(), "BEAT_TOKEN_FILE") {
		t.Errorf("error = %q, want BEAT_TOKEN_FILE context", err)
	}
	if strings.Contains(err.Error(), "env-fallback-token") {
		t.Errorf("error leaks the fallback token value: %v", err)
	}
}

func TestLoadRejectsBlankWebhookFileVar(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL_FILE", "   ")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with a present-but-empty DISCORD_WEBHOOK_URL_FILE = nil, want error rather than a silent fallback to the plain variable")
	}
	if !strings.Contains(err.Error(), "DISCORD_WEBHOOK_URL_FILE") {
		t.Errorf("error = %q, want DISCORD_WEBHOOK_URL_FILE context", err)
	}
}

func TestLoadTrimsPaddedPlainSecrets(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/hook ")
	t.Setenv("BEAT_TOKEN", "  unit-test-beat-token  ")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	// A trailing space survives url.Parse and is escaped as %20 on every
	// POST, so an untrimmed webhook 404s forever; a padded token 401s every
	// sender. envx trims only its _FILE branch, so both are trimmed here.
	if cfg.WebhookURL != "https://discord.example/hook" {
		t.Errorf("WebhookURL = %q, want the padding trimmed", cfg.WebhookURL)
	}
	if cfg.BeatToken != "unit-test-beat-token" {
		t.Errorf("BeatToken = %q, want the padding trimmed", cfg.BeatToken)
	}
}

func TestLoadKeepsAWhitespaceOnlyBeatTokenArmed(t *testing.T) {
	// A whitespace-only BEAT_TOKEN is a misconfigured credential, but an
	// EMPTY BeatToken is webapi's open-endpoint sentinel: trimming this
	// value to "" would silently disarm the /beat/{id} gate the operator
	// set (and skip the short-token warning too), while the same value via
	// BEAT_TOKEN_FILE fails startup. Keep it non-empty so the gate arms.
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "   ")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken == "" {
		t.Fatal("BeatToken is empty for a present whitespace-only BEAT_TOKEN: webapi would serve /beat/{id} ungated (fail-open)")
	}
	if cfg.BeatToken != "   " {
		t.Errorf("BeatToken = %q, want the value preserved verbatim", cfg.BeatToken)
	}
}

func TestLoadBeatTokenAtWarnBoundaryDoesNotWarn(t *testing.T) {
	// Serial (t.Setenv forbids t.Parallel): swaps the process-global slog
	// default to assert the absence of the short-token warning.
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", strings.Repeat("x", 16))
	unsetEnv(t, "BEAT_TOKEN_FILE")

	rec := capture.Default(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken != strings.Repeat("x", 16) {
		t.Errorf("BeatToken = %q, want the configured 16-byte token", cfg.BeatToken)
	}
	if rec.Contains("BEAT_TOKEN is shorter") {
		t.Errorf("16-byte token triggered the short-token warning (warn only below 16 bytes): %v", rec.Messages())
	}
}

func TestLoadFallsBackToTheHostnameWhenNodeNameIsUnset(t *testing.T) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		t.Skipf("cannot determine the hostname: %v", err)
	}
	setValidLoadEnv(t)
	t.Setenv("NODE_NAME", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Node != host {
		t.Errorf("Node = %q, want the hostname %q; the node name prefixes every Discord notice, so a fallback that reports a constant makes a three-observer set unattributable", cfg.Node, host)
	}
}

func TestLoadTrimsPaddedListenAddr(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("LISTEN_ADDR", "  0.0.0.0:9999  ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.ListenAddr != "0.0.0.0:9999" {
		t.Errorf("ListenAddr = %q, want the trimmed address: net.Listen resolves a padded address as a hostname lookup, so the container crash-loops with the padding invisible in the log line", cfg.ListenAddr)
	}
}

func TestLoadFallsBackToTheDefaultListenAddrWhenBlank(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("LISTEN_ADDR", "   ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.ListenAddr != ":9190" {
		t.Errorf("ListenAddr = %q, want :9190: an empty address makes net.Listen bind an EPHEMERAL port, hiding /metrics from Alloy and /beat/{id} from every sender", cfg.ListenAddr)
	}
}

func TestLoadTrimsPaddedNodeName(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("NODE_NAME", "  node-1  ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Node != "node-1" {
		t.Errorf("Node = %q, want \"node-1\": the node name prefixes every Discord notice, so padding misattributes which observer reported the outage", cfg.Node)
	}
}

func TestLoadRejectsWhitespaceOnlyWebhook(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL", "   ")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with a whitespace-only DISCORD_WEBHOOK_URL = nil, want error: a broken secret pipeline must fail startup rather than arm a switch that can never ring")
	}
	if !strings.Contains(err.Error(), "set but empty") {
		t.Errorf("error = %q, want the set-but-empty diagnosis rather than the misleading https-scheme rejection", err)
	}
}

func TestLoadWarnsWhenBeatTokenIsPresentButEmpty(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	rec := capture.Default(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken != "" {
		t.Errorf("BeatToken = %q, want empty: envx.Require cannot tell a present-but-empty BEAT_TOKEN from an unset one", cfg.BeatToken)
	}
	if !rec.Contains("BEAT_TOKEN is set but empty") {
		t.Errorf("log output %v missing the warning that /beat/{id} is served ungated, the only signal that separates the compose-interpolation accident from a deliberately open endpoint", rec.Messages())
	}
}
