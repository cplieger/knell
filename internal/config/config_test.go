package config

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/slogx/capture"
)

// Both forms must satisfy slog.LogValuer: the redaction seam only covers a
// call site that logs the whole struct if the VALUE implements it too (Load
// returns Config by value, and a pointer-receiver method set would leave that
// value reflection-rendered, secrets included).
var (
	_ slog.LogValuer = Config{}
	_ slog.LogValuer = (*Config)(nil)
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
// t.Setenv(key, "") would leave it present-but-empty, which is not equivalent
// for the keys this helper serves: `_FILE` keys reject a broken mount, and
// BEAT_TOKEN rejects an accidental empty gate while absence deliberately serves
// /beat/{id} open.
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
	unsetEnv(t, "BEAT_TOKEN")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken != "" {
		t.Errorf("BeatToken = %q, want empty (open endpoint) when BEAT_TOKEN is unset", cfg.BeatToken)
	}
}

// TestLoadDoesNotWarnAboutTokenLengthWhenNoTokenIsSet pins the non-empty
// guard on the short-token warning. An absent BEAT_TOKEN is the documented
// open-endpoint case, and the length warning must not speak for it: without
// the token != "" guard, every ungated deployment logs "BEAT_TOKEN is shorter
// than the recommended minimum" at startup, which reads as "a weak token is
// armed" for a configuration that has no gate at all — so the one line an
// operator would act on describes a token that does not exist while the real
// state, an unauthenticated /beat/{id}, goes unnamed.
// TestLoadBeatTokenDefaultsEmpty pins the returned value for this case but
// captures no log, so the warning is free to fire.
func TestLoadDoesNotWarnAboutTokenLengthWhenNoTokenIsSet(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	setValidLoadEnv(t)
	unsetEnv(t, "BEAT_TOKEN")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	rec := capture.Default(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken != "" {
		t.Fatalf("BeatToken = %q, want empty: an absent BEAT_TOKEN is the documented open-endpoint case", cfg.BeatToken)
	}
	if rec.Contains("BEAT_TOKEN is shorter") {
		t.Errorf("an absent BEAT_TOKEN drew the short-token warning: %v; it reads as \"a weak token is armed\" for a configuration that has no gate at all", rec.Messages())
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
	// The actionable content is the recommended minimum, and it is the ONLY
	// number the warning may carry: the token's own length is an attribute of a
	// live credential, and this line is shipped to a log store whose audience is
	// far wider than the encrypted file the token lives in.
	if !rec.HasAttr("BEAT_TOKEN is shorter", "minimum", strconv.Itoa(minTokenLength)) {
		t.Errorf("short-token warning does not report the recommended minimum %d: %v", minTokenLength, rec.Messages())
	}
	if _, found := rec.AttrValue("BEAT_TOKEN is shorter", "length"); found {
		t.Errorf("short-token warning publishes the token's exact length: it bounds the guess space of an already-weak credential on an unrate-limited POST /beat/{id} for every reader of the log store")
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
	if !rec.Contains("the plain variable is ignored") || !rec.HasAttr("", "variable", "BEAT_TOKEN") {
		t.Errorf("log output %v missing the both-channels-set warning that tells the operator the plain variable is ignored, carrying the ignored variable as a queryable attribute", rec.Messages())
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
	if !rec.Contains("the plain variable is ignored") || !rec.HasAttr("", "variable", "DISCORD_WEBHOOK_URL") {
		t.Errorf("log output %v missing the both-channels-set warning that tells the operator the plain variable is ignored, carrying the ignored variable as a queryable attribute", rec.Messages())
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

// TestLoadDoesNotWarnAboutTheIgnoredPlainVarWhenTheFileReadFails pins the
// POSITION of warnPlainVarIgnored's two call sites: both must sit PAST their
// loader's error gate. envx reports SourceFile together with its error on a
// failed file read, so a call made before the gate warns "the file wins and
// the plain variable is ignored" on a startup that is aborting because the
// file supplied nothing — and its "unset it" advice then deletes the one
// credential still present in the environment. Both loaders are pinned here:
// they are structurally identical uses of the same helper, and the fatal-path
// warning is invisible to every other test in this file (the two
// FileWinsOverPlainVar tests supply a readable file, and the
// RejectsUnreadable* tests capture no log).
func TestLoadDoesNotWarnAboutTheIgnoredPlainVarWhenTheFileReadFails(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	t.Run("webhook file unreadable", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing-webhook")
		setValidLoadEnv(t)
		t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/fallback")
		t.Setenv("DISCORD_WEBHOOK_URL_FILE", missing)

		rec := capture.Default(t)

		if _, err := Load(); err == nil {
			t.Fatal("Load() with an unreadable DISCORD_WEBHOOK_URL_FILE = nil, want error")
		}
		if rec.Contains("the plain variable is ignored") {
			t.Errorf("a failed webhook file read warned that the plain variable is ignored: %v; the file supplied nothing, and following the advice deletes the only webhook URL left in the environment", rec.Messages())
		}
	})

	t.Run("beat token file unreadable", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing-beat-token")
		setValidLoadEnv(t)
		t.Setenv("BEAT_TOKEN", "fallback-beat-token")
		t.Setenv("BEAT_TOKEN_FILE", missing)

		rec := capture.Default(t)

		if _, err := Load(); err == nil {
			t.Fatal("Load() with an unreadable BEAT_TOKEN_FILE = nil, want error")
		}
		if rec.Contains("the plain variable is ignored") {
			t.Errorf("a failed beat-token file read warned that the plain variable is ignored: %v; it reads as \"the gate is armed from the file\" while the process is refusing to start", rec.Messages())
		}
	})
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

func TestLoadRejectsAnASCIIWhitespaceOnlyBeatToken(t *testing.T) {
	// Where this shape comes from: a compose quoting accident or a padded
	// interpolation (BEAT_TOKEN="${TOKEN} " with TOKEN undefined) hands the
	// process a token made only of spaces and tabs.
	//
	// Such a token can never be PRESENTED: net/textproto strips leading and
	// trailing spaces and tabs from every header value, so "Bearer   " is
	// read back as "Bearer" and webhttp's sha256 + ConstantTimeCompare
	// verifier (no normalization) rejects it. Keeping it armed was the worst
	// outcome available for a dead-man switch: knell started, reported
	// itself gated, 401'd every ping, and one deadline later posted a
	// MISSING notice for every configured beat. So it fails startup, like
	// the two adjacent accidents this package already refuses (a
	// present-but-empty BEAT_TOKEN and a blank BEAT_TOKEN_FILE).
	tests := map[string]string{
		"spaces":             "   ",
		"tab":                "\t",
		"spaces and tabs":    " \t \t ",
		"carriage return lf": "\r\n",
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv("BEAT_TOKEN", token)
			unsetEnv(t, "BEAT_TOKEN_FILE")

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() with BEAT_TOKEN=%q = nil, want error: the token cannot survive into a header value, so every ping 401s against an endpoint that reports itself gated", token)
			}
			if !strings.Contains(err.Error(), "BEAT_TOKEN") {
				t.Errorf("error = %q, want BEAT_TOKEN named so the operator knows which variable to fix", err)
			}
		})
	}
}

func TestLoadKeepsANonASCIISpaceBeatTokenArmed(t *testing.T) {
	// The boundary of the refusal above, and the one place a naive
	// strings.TrimSpace check would be wrong. TrimSpace follows
	// unicode.IsSpace and treats NBSP (U+00A0) as blank, but net/textproto
	// strips only spaces and tabs: an NBSP-only token IS presented verbatim
	// and DOES authenticate, so refusing it would fail startup on a working
	// configuration, and trimming it to "" would silently serve /beat/{id}
	// open. Accepted verbatim, gate armed, with the warning — it is still
	// almost certainly an accident.
	//
	// Serial (t.Setenv forbids t.Parallel): swaps the process-global slog
	// default to assert the warning.
	const token = "\u00a0"
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", token)
	unsetEnv(t, "BEAT_TOKEN_FILE")

	rec := capture.Default(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() with an NBSP-only BEAT_TOKEN = %v, want accepted: textproto keeps a non-ASCII space, so the token is presentable and the gate must stay armed", err)
	}
	if cfg.BeatToken != token {
		t.Errorf("BeatToken = %q, want %q preserved verbatim: an empty BeatToken is webapi's open-endpoint sentinel, so trimming would disarm the gate the operator set", cfg.BeatToken, token)
	}
	// The shape, not just the length: the generic short-token warning fires
	// for a 2-byte token too, so without this assertion the one warning that
	// names the actual misconfiguration can be dropped and the log still
	// looks populated.
	if !rec.Contains("whitespace only") {
		t.Errorf("log output %v never says the token is whitespace only; the only other signal is the length hint, which reads as \"your token is short\" while senders must reproduce an invisible character", rec.Messages())
	}
}

func TestLoadNormalizesASCIIPaddingAroundANonASCIISpaceBeatToken(t *testing.T) {
	// The mixed shape between the two rules above, and the reason the ASCII
	// trim must run BEFORE any classification. net/textproto strips the outer
	// spaces from the header value but keeps the NBSP, so the sender presents
	// "Bearer \u00a0". Storing the padded value verbatim would leave the
	// verifier holding "Bearer \u00a0 " with its padding: startup succeeds,
	// the log reports the gate armed, and every ping 401s until each beat
	// crosses its deadline and posts a false MISSING notice. Store what the
	// wire delivers.
	const presented = "\u00a0"
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", " \u00a0 ")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() with an ASCII-padded NBSP BEAT_TOKEN = %v, want accepted: the NBSP survives the header, so the token is presentable once the padding is trimmed", err)
	}
	if cfg.BeatToken != presented {
		t.Errorf("BeatToken = %q, want %q: HTTP strips the outer spaces, so keeping them would make the verifier compare a padded token against the unpadded value every sender can actually send", cfg.BeatToken, presented)
	}
}

func TestLoadRejectsABeatTokenHTTPCannotCarry(t *testing.T) {
	// Distinct from the whitespace-only refusal: these values are non-empty
	// after the ASCII trim, so trimming cannot rescue them, yet HTTP forbids
	// the byte they carry in a field value. Go's client refuses to write the
	// header and Go's server rejects a handcrafted one before beatHandler, so
	// the gate would be armed with a token no sender could ever present —
	// knell starts, reports healthy, records no ping, and one deadline later
	// declares every configured beat missing.
	tests := map[string]string{
		"interior newline":        "alpha\nbeta",
		"interior carriage":       "alpha\rbeta",
		"trailing newline inside": "alpha\nbeta\n",
		"delete byte":             "alpha\x7fbeta",
		"vertical tab interior":   "alpha\vbeta",
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv("BEAT_TOKEN", token)
			unsetEnv(t, "BEAT_TOKEN_FILE")

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() with BEAT_TOKEN=%q = nil, want error: HTTP forbids that byte in a field value, so every ping 401s against an endpoint that reports itself gated", token)
			}
			if !strings.Contains(err.Error(), "BEAT_TOKEN") {
				t.Errorf("error = %q, want BEAT_TOKEN named so the operator knows which variable to fix", err)
			}
			if strings.Contains(err.Error(), token) {
				t.Errorf("error = %q embeds the token value; the startup error is shipped to Loki, so it must describe the shape and never echo the credential", err)
			}
		})
	}
}

func TestBeatTokenFitsHeaderMatchesWhatHTTPActuallyCarries(t *testing.T) {
	// The oracle for the refusal above: the predicate is only worth anything
	// if it agrees with the transport it claims to model. Every accepted value
	// ALREADY TRIMMED OF EDGE ASCII WHITESPACE (the predicate's precondition,
	// and all loadBeatToken ever hands it) must survive a real request verbatim
	// (a value the wire alters is just as unpresentable as one it rejects), and
	// every rejected value must be one Go's HTTP client refuses to send.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Echo", r.Header.Get("Authorization"))
	}))
	t.Cleanup(srv.Close)

	tests := map[string]string{
		"printable":       "plain-token-1234567",
		"non-ascii space": "\u00a0",
		"obs-text":        "tökén-with-hïgh-bytes",
		"interior tab":    "alpha\tbeta",
		"interior spaces": "alpha  beta",
		"interior nl":     "alpha\nbeta",
		"interior cr":     "alpha\rbeta",
		"nul":             "alpha\x00beta",
		"del":             "alpha\x7fbeta",
		"vertical tab":    "alpha\vbeta",
		"form feed":       "alpha\fbeta",
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
			if err != nil {
				t.Fatalf("NewRequestWithContext: %v", err)
			}
			req.Header.Set("Authorization", "Bearer "+token)

			resp, doErr := srv.Client().Do(req)
			echoed := ""
			if doErr == nil {
				echoed = resp.Header.Get("X-Echo")
				if err := resp.Body.Close(); err != nil {
					t.Errorf("closing body: %v", err)
				}
			}

			if beatTokenFitsHeader(token) {
				if doErr != nil {
					t.Fatalf("beatTokenFitsHeader(%q) = true but the HTTP client refused to send it: %v — the predicate accepts a token startup would arm and no sender could present", token, doErr)
				}
				if echoed != "Bearer "+token {
					t.Errorf("server read %q, want %q: the predicate accepts a token the wire alters, so the exact-match verifier would reject every ping", echoed, "Bearer "+token)
				}
				return
			}
			if doErr == nil {
				t.Errorf("beatTokenFitsHeader(%q) = false but the client sent it fine (server read %q); the refusal would fail startup on a working configuration", token, echoed)
			}
		})
	}
}

// TestBeatTokenFitsHeaderAgreesWithTheTransportForEveryByte sweeps the
// whole byte space through the same oracle
// TestBeatTokenFitsHeaderMatchesWhatHTTPActuallyCarries applies to eleven
// hand-picked values. The predicate claims to model exactly what an HTTP field
// value can carry, and both directions of a divergence are silent: a byte it
// wrongly ACCEPTS arms a gate no sender can present (every ping 401s and every
// beat goes falsely missing one deadline later), and a byte it wrongly REJECTS
// fails startup on a working configuration. Only the interior position is
// swept, because the predicate's precondition is that edge ASCII whitespace was
// already trimmed.
func TestBeatTokenFitsHeaderAgreesWithTheTransportForEveryByte(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Echo", r.Header.Get("Authorization"))
	}))
	t.Cleanup(srv.Close)

	for b := range 256 {
		// INTERIOR placement only: a trailing SP or HTAB is legal in a field
		// value yet stripped by the wire, and the predicate answers the
		// byte-legality question for an already-trimmed value (see
		// asciiWhitespace).
		token := "alpha" + string([]byte{byte(b)}) + "beta"
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatalf("NewRequestWithContext: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)

		resp, doErr := srv.Client().Do(req)
		echoed := ""
		if doErr == nil {
			echoed = resp.Header.Get("X-Echo")
			if err := resp.Body.Close(); err != nil {
				t.Errorf("closing body: %v", err)
			}
		}

		carried := doErr == nil && echoed == "Bearer "+token
		if got := beatTokenFitsHeader(token); got != carried {
			t.Errorf("interior byte 0x%02x: beatTokenFitsHeader = %v, but the transport carried it verbatim = %v (err=%v echo=%q); the predicate and the wire must agree or startup either arms an unpresentable token or refuses a working one", b, got, carried, doErr, echoed)
		}
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
	// ABSENT, not present-but-empty: an unset NODE_NAME is what every
	// deployment ships (the compose example sets BEATS and the webhook only),
	// and this package already treats the two states differently elsewhere
	// (loadBeatToken refuses a present-but-empty BEAT_TOKEN while an absent one
	// serves /beat/{id} open).
	unsetEnv(t, "NODE_NAME")

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

func TestLoadAcceptsANodeNameAtTheLimit(t *testing.T) {
	setValidLoadEnv(t)
	node := strings.Repeat("n", maxNodeNameBytes)
	t.Setenv("NODE_NAME", node)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() with a %d-byte NODE_NAME error = %v, want it accepted: the cap is the last accepted value, and it still leaves every notice far inside Discord's 2000-character limit", maxNodeNameBytes, err)
	}
	if cfg.Node != node {
		t.Errorf("Node = %q, want the configured %d-byte name verbatim", cfg.Node, maxNodeNameBytes)
	}
}

func TestLoadRejectsANodeNamePastTheLimit(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("NODE_NAME", strings.Repeat("n", maxNodeNameBytes+1))

	_, err := Load()
	if err == nil {
		t.Fatalf("Load() with a %d-byte NODE_NAME = nil, want error: the name prefixes every notice, so an oversized one makes Discord reject missing, recovered and history alike - the switch arms and never rings", maxNodeNameBytes+1)
	}
	if !strings.Contains(err.Error(), "NODE_NAME") {
		t.Errorf("Load() error = %q, want it to name NODE_NAME so the operator knows which variable to shorten", err)
	}
	if !strings.Contains(err.Error(), strconv.Itoa(maxNodeNameBytes)) {
		t.Errorf("Load() error = %q, want it to state the %d-byte limit", err, maxNodeNameBytes)
	}
}

// TestLoadCountsNodeNameLengthInBytesNotRunes pins the side of the boundary
// maxNodeNameBytes is measured on. The cap exists so the node name cannot push
// a notice past Discord's character limit, and counting BYTES is the
// deliberately conservative direction (UTF-8 bytes are always >= both the
// character count and the UTF-16 code-unit count). A multi-byte name is the
// only input that can tell the two counts apart, so without this test a
// change to utf8.RuneCountInString would silently relax the documented bound.
func TestLoadCountsNodeNameLengthInBytesNotRunes(t *testing.T) {
	t.Run("accepts a multi-byte name at exactly the byte limit", func(t *testing.T) {
		setValidLoadEnv(t)
		// "é" is 2 UTF-8 bytes, so this is maxNodeNameBytes bytes in half as
		// many runes: accepted, because the cap is the last accepted value.
		node := strings.Repeat("é", maxNodeNameBytes/2)
		t.Setenv("NODE_NAME", node)

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() with a %d-byte (%d-rune) NODE_NAME error = %v, want it accepted", len(node), maxNodeNameBytes/2, err)
		}
		if cfg.Node != node {
			t.Errorf("Node = %q, want the configured name verbatim", cfg.Node)
		}
	})

	t.Run("rejects a multi-byte name over the byte limit but under it in runes", func(t *testing.T) {
		setValidLoadEnv(t)
		// Fewer runes than maxNodeNameBytes, but twice as many bytes: this is
		// the input a rune-counting implementation would wrongly accept.
		node := strings.Repeat("é", maxNodeNameBytes-10)
		t.Setenv("NODE_NAME", node)

		if _, err := Load(); err == nil {
			t.Fatalf("Load() with a %d-byte (%d-rune) NODE_NAME = nil, want error: the bound is counted in bytes, which is the conservative direction against Discord's character limit", len(node), maxNodeNameBytes-10)
		}
	})
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

func TestLoadRejectsAPresentButEmptyBeatToken(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with a present-but-empty BEAT_TOKEN = nil, want error; envx.Require cannot tell it from unset, and it is exactly what compose interpolation of an undefined variable produces, so accepting it would serve /beat/{id} unauthenticated by accident")
	}
	if !strings.Contains(err.Error(), "BEAT_TOKEN") {
		t.Errorf("error = %q, want BEAT_TOKEN context: the operator has to know which variable to unset or fill in", err)
	}
}

// TestLoadDoesNotWarnWhenOnlyTheSecretFilesAreSet pins the negative half of
// the both-channels-set warning: with only BEAT_TOKEN_FILE and
// DISCORD_WEBHOOK_URL_FILE set, no plain variable is being shadowed, so there
// is nothing to report. The positive half is pinned by
// TestLoadBeatTokenFileWinsOverPlainVar and
// TestLoadWebhookFileWinsOverPlainVar; without this half, dropping
// warnPlainVarIgnored's plain-variable guard tells every _FILE-only operator
// to unset a variable they never set, on the one startup line that is
// supposed to mean a rotated secret is being ignored.
func TestLoadDoesNotWarnWhenOnlyTheSecretFilesAreSet(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "beat-token")
	if err := os.WriteFile(tokenFile, []byte("file-borne-beat-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	hookFile := filepath.Join(dir, "webhook-url")
	if err := os.WriteFile(hookFile, []byte("https://discord.example/file-borne-hook\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	unsetEnv(t, "BEAT_TOKEN")
	unsetEnv(t, "DISCORD_WEBHOOK_URL")
	t.Setenv("BEAT_TOKEN_FILE", tokenFile)
	t.Setenv("DISCORD_WEBHOOK_URL_FILE", hookFile)

	rec := capture.Default(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken != "file-borne-beat-token" || cfg.WebhookURL != "https://discord.example/file-borne-hook" {
		t.Fatalf("BeatToken = %q, WebhookURL = %q, want both from their _FILE channels", cfg.BeatToken, cfg.WebhookURL)
	}
	if rec.Contains("the plain variable is ignored") {
		t.Errorf("_FILE-only configuration warned that the plain variable is ignored: %v", rec.Messages())
	}
}

// TestLoadDoesNotWarnWhenOnlyThePlainVarsAreSet pins the plain-only half of
// warnPlainVarIgnored's source guard. The warning means "a _FILE channel
// supplied this secret, so the plain variable you also set was ignored" — it
// is only true when src is envx.SourceFile. TestLoadBeatTokenFileWinsOverPlainVar
// and TestLoadWebhookFileWinsOverPlainVar pin the positive case, and
// TestLoadDoesNotWarnWhenOnlyTheSecretFilesAreSet pins the _FILE-only case;
// neither exercises the ordinary plain-variable-only deployment, so dropping
// the src check tells that operator, on every startup, to unset the variable
// that is actually supplying the credential — advice that leaves /beat/{id}
// ungated and the next start failing on a missing webhook.
func TestLoadDoesNotWarnWhenOnlyThePlainVarsAreSet(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "plain-only-beat-token")
	unsetEnv(t, "BEAT_TOKEN_FILE")
	unsetEnv(t, "DISCORD_WEBHOOK_URL_FILE")

	rec := capture.Default(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken != "plain-only-beat-token" || cfg.WebhookURL != "https://discord.example/hook" {
		t.Fatalf("BeatToken = %q, WebhookURL = %q, want both read from their plain variables", cfg.BeatToken, cfg.WebhookURL)
	}
	if rec.Contains("the plain variable is ignored") {
		t.Errorf("plain-variable-only configuration was told its plain variable is ignored: %v; following that advice unsets the variable that is actually supplying the credential", rec.Messages())
	}
}

// TestConfigLogValueReportsBothSecretsByPresenceOnly pins the redaction seam:
// LogValue is the reason a call site can log a whole Config without leaking,
// so it must report DISCORD_WEBHOOK_URL and BEAT_TOKEN by presence and never
// by value. The receiver under test is a VALUE, not a pointer: Load returns
// Config by value and that is the form a future slog call would hand a
// logger, so a seam that only covers *Config would not cover the leak.
func TestConfigLogValueReportsBothSecretsByPresenceOnly(t *testing.T) {
	t.Parallel()

	const (
		secretHook  = "https://discord.example/api/webhooks/1/leak-me-if-you-can"
		secretToken = "leak-me-if-you-can-token"
	)
	cfg := Config{
		WebhookURL: secretHook,
		Node:       "node-a",
		ListenAddr: ":9190",
		BeatToken:  secretToken,
		Beats:      []Beat{{ID: "api", Deadline: time.Hour}},
		LogLevel:   slog.LevelInfo,
	}

	got := map[string]string{}
	for _, attr := range cfg.LogValue().Group() {
		got[attr.Key] = attr.Value.String()
	}
	for key, value := range got {
		if strings.Contains(value, secretHook) || strings.Contains(value, secretToken) {
			t.Errorf("LogValue attr %s = %q carries a secret verbatim: logging a Config would publish the Discord credential and the /beat/{id} gate into the log store", key, value)
		}
	}
	if got["webhook"] != "configured" || got["beat_auth"] != "required" {
		t.Errorf("webhook = %q, beat_auth = %q, want \"configured\" and \"required\": presence is the only thing these two attrs may report", got["webhook"], got["beat_auth"])
	}

	empty := Config{LogLevel: slog.LevelInfo}
	got = map[string]string{}
	for _, attr := range empty.LogValue().Group() {
		got[attr.Key] = attr.Value.String()
	}
	if got["webhook"] != "unset" || got["beat_auth"] != "open" {
		t.Errorf("unconfigured: webhook = %q, beat_auth = %q, want \"unset\" and \"open\"; beat_auth must not read \"required\" for an ungated endpoint", got["webhook"], got["beat_auth"])
	}
}

// TestConfigLogValueReportsEveryNonSecretField pins the accuracy half of
// LogValue's contract; TestConfigLogValueReportsBothSecretsByPresenceOnly
// pins the hygiene half. LogValue exists so a call site can hand a whole
// Config to slog, and those six attrs are then the entire rendering of a
// configuration that is env-only, with no reload and no readback endpoint.
// Every value below differs from any plausible default and from every sibling
// field, so an attr rewired to a literal or to the wrong field fails here
// instead of publishing a line that contradicts the configuration running.
func TestConfigLogValueReportsEveryNonSecretField(t *testing.T) {
	t.Parallel()

	cfg := Config{
		WebhookURL: "https://discord.example/hook",
		Node:       "observer-borgcube",
		ListenAddr: "127.0.0.1:19190",
		BeatToken:  "unit-test-beat-token",
		Beats: []Beat{
			{ID: "watchdog-mimir", Deadline: 20 * time.Minute},
			{ID: "watchdog-loki", Deadline: 26 * time.Hour},
		},
		LogLevel: slog.LevelDebug,
	}

	got := map[string]string{}
	for _, attr := range cfg.LogValue().Group() {
		got[attr.Key] = attr.Value.String()
	}

	want := map[string]string{
		"beats":       "2",
		"node":        "observer-borgcube",
		"listen_addr": "127.0.0.1:19190",
		"webhook":     "configured",
		"beat_auth":   "required",
		"log_level":   "DEBUG",
	}
	if len(got) != len(want) {
		t.Errorf("LogValue() rendered %d attrs %v, want exactly %d: an added attr has to be reviewed for secret content, and a dropped one silently removes a field from the only rendering of an env-only configuration", len(got), got, len(want))
	}
	for key, wantValue := range want {
		if got[key] != wantValue {
			t.Errorf("LogValue attr %s = %q, want %q", key, got[key], wantValue)
		}
	}
}

// TestParseWebhookURLRejectsUndeliverableShapes pins the two refusals that
// separate a TRANSPORTABLE URL from a DELIVERABLE one. Both are invisible at
// runtime: startup succeeds, /healthz is healthy, outages are detected, and
// only the notice fails — forever. Nothing else asserts them
// (FuzzParseWebhookURL only constrains what a rejection message may SAY), so
// dropping either guard leaves the whole suite green.
func TestParseWebhookURLRejectsUndeliverableShapes(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		raw     string
		wantErr string
	}{
		"host only":              {raw: "https://discord.example", wantErr: "missing path"},
		"root path only":         {raw: "https://discord.example/", wantErr: "missing path"},
		"interior space in path": {raw: "https://discord.example/api/webhooks/1/ab c", wantErr: "contains a space"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := parseWebhookURL(tt.raw)
			if err == nil {
				t.Fatalf("parseWebhookURL(%q) = nil, want an error: the webhook path is the credential, so this value can never deliver a notice and startup is the only moment the operator is watching", tt.raw)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to name %q", err, tt.wantErr)
			}
			if strings.Contains(err.Error(), "discord.example") {
				t.Errorf("error = %q embeds the URL; the webhook URL is a secret and startup errors are shipped to Loki", err)
			}
		})
	}

	if _, err := parseWebhookURL("https://discord.example/api/webhooks/1/abc"); err != nil {
		t.Errorf("parseWebhookURL(a well-formed webhook) = %v, want nil: the deliverability checks must not refuse a working configuration", err)
	}
}

// TestLoadWarnsOnlyWhenLogLevelIsUnparseable pins the ONLY signal that a
// mistyped LOG_LEVEL was ignored. slogx.ParseLevel returns ok=true for an unset
// value and ok=false only for a non-empty unparseable one, so this WARN is what
// separates "the operator asked for debug and got debug" from "the operator
// typo'd and is silently running at info" — a distinction that matters exactly
// when someone is turning up logging to diagnose a live outage.
// TestLoadInvalidLogLevelFallsBackToInfo pins the resulting level but captures
// no log, so inverting or dropping the !ok guard keeps every existing test
// green while every default deployment either loses the warning or gains a
// spurious one on a perfectly valid level.
func TestLoadWarnsOnlyWhenLogLevelIsUnparseable(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	tests := map[string]struct {
		raw      string
		unset    bool
		want     string
		wantWarn bool
	}{
		"unparseable value warns":         {raw: "chatty", want: "INFO", wantWarn: true},
		"present but blank does not warn": {raw: "", want: "INFO", wantWarn: false},
		"unset does not warn":             {unset: true, want: "INFO", wantWarn: false},
		"valid value does not warn":       {raw: "debug", want: "DEBUG", wantWarn: false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			if tt.unset {
				unsetEnv(t, "LOG_LEVEL")
			} else {
				t.Setenv("LOG_LEVEL", tt.raw)
			}

			rec := capture.Default(t)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if cfg.LogLevel.String() != tt.want {
				t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, tt.want)
			}
			if got := rec.Contains("invalid LOG_LEVEL"); got != tt.wantWarn {
				t.Errorf("LOG_LEVEL=%q: warned = %v, want %v; the warning is the only signal that a typo was ignored, and a warning on a valid level trains the operator to ignore it: %v", tt.raw, got, tt.wantWarn, rec.Messages())
			}
		})
	}
}

// TestLoadRejectsAFileBorneBeatTokenHTTPCannotCarry pins that the
// control-byte refusal covers the _FILE channel too, the same way
// TestLoadRejectsPlainHTTPWebhookFromFile pins the https gate for the webhook's
// file channel. envx trims only the file's edges, so a two-line secret file (a
// stray second line, a copy-pasted pair of lines) hands loadBeatToken a token
// with an INTERIOR newline that no trim can rescue and no sender can present.
// TestLoadRejectsABeatTokenHTTPCannotCarry covers the plain variable only, so
// scoping the check to the plain channel — the shape the cycle-8 normalization
// work moved code toward — leaves the mounted-secret path arming a gate that
// 401s every ping while every existing test stays green.
func TestLoadRejectsAFileBorneBeatTokenHTTPCannotCarry(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "beat-token")
	if err := os.WriteFile(tokenFile, []byte("alpha\nbeta\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	unsetEnv(t, "BEAT_TOKEN")
	t.Setenv("BEAT_TOKEN_FILE", tokenFile)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with a two-line BEAT_TOKEN_FILE = nil, want error: the interior newline is illegal in a header value, so the gate would be armed with a token no sender can present and every configured beat goes falsely missing one deadline later")
	}
	if !strings.Contains(err.Error(), "BEAT_TOKEN") {
		t.Errorf("error = %q, want BEAT_TOKEN named so the operator knows which secret to fix", err)
	}
	if strings.Contains(err.Error(), "alpha") || strings.Contains(err.Error(), "beta") {
		t.Errorf("error = %q embeds the token value; the startup error is shipped to Loki, so it must describe the shape and never echo the credential", err)
	}
}
