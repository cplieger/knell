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

	"github.com/cplieger/knell/internal/notify"
	"github.com/cplieger/slogx/capture"
	"github.com/cplieger/webhttp"
)

// maxNodeNameBytes is the NODE_NAME bound the composition root passes to Load
// in production. Load takes it as a parameter so the environment boundary does
// not depend on the Discord notifier (notify owns the budget and the templates
// it is measured over); the tests read the real value here, from the same
// source main does, so the cap they pin is the cap that ships.
const maxNodeNameBytes = notify.MaxNodeNameBytes

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

// validBeatToken is a token that satisfies every checkBeatToken rule, including
// the minTokenLength floor. Tests that are not about the token itself use it so
// the required credential is present and valid.
const validBeatToken = "unit-test-beat-token"

// setValidLoadEnv sets the minimal environment Load accepts. Tests that
// exercise a variant override individual keys with t.Setenv afterwards.
func setValidLoadEnv(t *testing.T) {
	t.Helper()
	t.Setenv("BEATS", "api:20m")
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/hook")
	t.Setenv("NODE_NAME", "node-1")
	// BEAT_TOKEN is required: the bearer gate is /beat/{id}'s only gate, so Load
	// refuses to start without it and every variant test needs one.
	t.Setenv("BEAT_TOKEN", validBeatToken)
}

// unsetEnv removes key for the duration of the test. t.Setenv registers the
// restore of the original value, so the following os.Unsetenv leaves the
// variable absent inside the test and restored afterwards. A plain
// t.Setenv(key, "") would leave it present-but-empty, which is not equivalent
// for the keys this helper serves: a `_FILE` key rejects a broken mount with its
// own message, and BEAT_TOKEN distinguishes "you set it to nothing" from "you
// set nothing at all" — both fail startup, with different remedies.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unsetting %s: %v", key, err)
	}
}

// authEchoServer starts a throwaway server that echoes the Authorization header
// it received back in X-Echo, so a test can compare what the wire carried
// against what beatTokenFitsHeader claims about it.
func authEchoServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Echo", r.Header.Get("Authorization"))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// echoAuthHeader sends "Bearer "+token to srv and reports the Authorization
// value the server actually read, or the error the client refused to send it
// with. One definition of "what the transport carries" for both
// beatTokenFitsHeader oracles, so the byte sweep and the hand-picked table
// cannot come to measure different things.
func echoAuthHeader(t *testing.T, srv *httptest.Server, token string) (string, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, doErr := srv.Client().Do(req)
	if doErr != nil {
		return "", doErr
	}
	echoed := resp.Header.Get("X-Echo")
	if closeErr := resp.Body.Close(); closeErr != nil {
		t.Errorf("closing body: %v", closeErr)
	}
	return echoed, nil
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

	cfg, err := Load(maxNodeNameBytes)
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

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.ListenAddr != ":9190" {
		t.Errorf("ListenAddr default = %q, want :9190", cfg.ListenAddr)
	}

	t.Setenv("BEATS", "")
	if _, err := Load(maxNodeNameBytes); err == nil || !strings.Contains(err.Error(), "BEATS is required") {
		t.Errorf("Load() with empty BEATS error = %v, want it to name BEATS as required", err)
	}

	t.Setenv("BEATS", "api:20m")
	t.Setenv("DISCORD_WEBHOOK_URL", "")
	if _, err := Load(maxNodeNameBytes); err == nil || !strings.Contains(err.Error(), "DISCORD_WEBHOOK_URL is required") {
		t.Errorf("Load() with empty DISCORD_WEBHOOK_URL error = %v, want it to name DISCORD_WEBHOOK_URL as required", err)
	}

	t.Setenv("DISCORD_WEBHOOK_URL", "not-a-url")
	_, err = Load(maxNodeNameBytes)
	if err == nil || !strings.Contains(err.Error(), "scheme must be https") {
		t.Errorf("Load() with a schemeless DISCORD_WEBHOOK_URL error = %v, want the https-scheme rejection", err)
	}
	if err != nil && strings.Contains(err.Error(), "not-a-url") {
		t.Errorf("error leaks the rejected webhook value: %v", err)
	}
}

func TestLoadRejectsMalformedBeats(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("BEATS", "api:1s")

	_, err := Load(maxNodeNameBytes)
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

	_, err := Load(maxNodeNameBytes)
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

	_, err := Load(maxNodeNameBytes)
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

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken != "unit-test-beat-token" {
		t.Errorf("BeatToken = %q, want the configured token (webapi's gate arms only when config carries it)", cfg.BeatToken)
	}
}

// TestLoadRefusesWithoutABeatToken pins the required credential. The bearer
// gate is /beat/{id}'s ONLY gate: with no token, any client that can reach the
// port keeps every beat reading fresh, so the switch is silently disarmed while
// /healthz reports ready and nothing in the log says otherwise. A startup
// refusal is the one signal that cannot be mistaken for a working observer, so
// neither an absent variable nor a present-but-empty one may start.
func TestLoadRefusesWithoutABeatToken(t *testing.T) {
	tests := map[string]func(t *testing.T){
		"absent": func(t *testing.T) {
			unsetEnv(t, "BEAT_TOKEN")
			unsetEnv(t, "BEAT_TOKEN_FILE")
		},
		"present but empty": func(t *testing.T) {
			t.Setenv("BEAT_TOKEN", "")
			unsetEnv(t, "BEAT_TOKEN_FILE")
		},
	}
	for name, setup := range tests {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			setup(t)

			_, err := Load(maxNodeNameBytes)
			if err == nil {
				t.Fatalf("Load() with %s BEAT_TOKEN succeeded, want a startup refusal: an ungated /beat/{id} lets anything that reaches the port keep the switch armed", name)
			}
			// The message has to name the variable and what to do next: this is
			// the refusal a first deployment meets, and the container
			// crash-loops on it.
			for _, want := range []string{"BEAT_TOKEN", "16"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Load() error = %q, want it to mention %q so the operator knows what to set", err, want)
				}
			}
		})
	}
}

// TestLoadRefusesAShortBeatTokenWithoutLeakingIt pins the length floor as a
// REFUSAL. A guessable token is not a weaker gate, it is a bypassable one, and
// the failed-auth throttle only slows a guessing run down; since the token is
// the endpoint's only gate, a short one is refused at startup, where the
// operator is watching, rather than warned about in a line nobody reads.
//
// The message may name the MINIMUM and nothing else about the value: the startup
// log ships to a store whose audience is far wider than the encrypted file the
// token lives in, so the token itself, its exact length and its character class
// all stay out.
func TestLoadRefusesAShortBeatTokenWithoutLeakingIt(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "shorty")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with a 6-byte BEAT_TOKEN succeeded, want a startup refusal: the token is the only gate, so a guessable one is a bypassable one")
	}
	if !strings.Contains(err.Error(), strconv.Itoa(minTokenLength)) {
		t.Errorf("refusal = %q, want it to name the %d-byte minimum: it is the only actionable number", err, minTokenLength)
	}
	if strings.Contains(err.Error(), "shorty") {
		t.Errorf("refusal leaks the token value: %q", err)
	}
	// Every number in the message must be the minimum: the token's own length is
	// an attribute of a live credential, and this text ships to the log store.
	for _, run := range digitRuns(err.Error()) {
		if run != strconv.Itoa(minTokenLength) {
			t.Errorf("refusal carries the number %q: %q; only the %d-byte minimum may appear, or the line bounds the guess space of an already-weak credential for every reader of the log store",
				run, err, minTokenLength)
		}
	}
}

// digitRuns returns every maximal run of digits in s, so a test can assert which
// numbers a message is allowed to carry.
func digitRuns(s string) []string {
	var runs []string
	var cur strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			cur.WriteRune(r)
			continue
		}
		if cur.Len() > 0 {
			runs = append(runs, cur.String())
			cur.Reset()
		}
	}
	if cur.Len() > 0 {
		runs = append(runs, cur.String())
	}
	return runs
}

func TestLoadBeatTokenFromFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "beat-token")
	if err := os.WriteFile(tokenFile, []byte("file-borne-beat-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "")
	t.Setenv("BEAT_TOKEN_FILE", tokenFile)

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.BeatToken != "file-borne-beat-token" {
		t.Errorf("BeatToken = %q, want the file-borne token (BEAT_TOKEN_FILE alone must arm the gate, with the file's single trailing newline removed)", cfg.BeatToken)
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

	cfg, err := Load(maxNodeNameBytes)
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

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.WebhookURL != "https://discord.example/file-borne-hook" {
		t.Errorf("WebhookURL = %q, want the file-borne URL (DISCORD_WEBHOOK_URL_FILE is the documented secret-file convention, with the file's single trailing newline removed)", cfg.WebhookURL)
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

	cfg, err := Load(maxNodeNameBytes)
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

	_, err := Load(maxNodeNameBytes)
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

		if _, err := Load(maxNodeNameBytes); err == nil {
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

		if _, err := Load(maxNodeNameBytes); err == nil {
			t.Fatal("Load() with an unreadable BEAT_TOKEN_FILE = nil, want error")
		}
		if rec.Contains("the plain variable is ignored") {
			t.Errorf("a failed beat-token file read warned that the plain variable is ignored: %v; it reads as \"the gate is armed from the file\" while the process is refusing to start", rec.Messages())
		}
	})
}

// TestLoadDoesNotWarnAboutTheIgnoredPlainVarWhenTheFileTokenIsInvalid pins the
// remaining fatal path after the file read SUCCEEDS: the advisory must not
// advise unsetting the plain variable and then exit over the winning value.
// The reachable case is a file-sourced token carrying an interior control byte
// while the plain BEAT_TOKEN is also set — the file wins, the advisory used to
// fire, and startup then failed on the token, so the operator was told to
// delete the only other token in the environment by a process that never ran.
// (An edge-padded file token is a second instance of this same shape, pinned by
// TestLoadRejectsAPaddedFileBorneBeatToken: envx no longer trims the file's
// bytes, so the padding refusal is reachable through the _FILE channel too. A
// BLANK file still errors earlier, inside envx.)
func TestLoadDoesNotWarnAboutTheIgnoredPlainVarWhenTheFileTokenIsInvalid(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	tokenFile := filepath.Join(t.TempDir(), "beat-token")
	if err := os.WriteFile(tokenFile, []byte("alpha\nbeta\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "fallback-beat-token")
	t.Setenv("BEAT_TOKEN_FILE", tokenFile)

	rec := capture.Default(t)

	if _, err := Load(maxNodeNameBytes); err == nil {
		t.Fatal("Load() with a two-line BEAT_TOKEN_FILE = nil, want error: the interior newline is illegal in a header value")
	}
	if rec.Contains("the plain variable is ignored") {
		t.Errorf("a fatal token validation warned that the plain variable is ignored: %v; the advice describes a configuration that never ran", rec.Messages())
	}
}

// TestLoadDoesNotWarnAboutTheIgnoredPlainVarWhenTheFileWebhookIsInvalid pins
// the post-VALIDATION position of loadWebhook's advisory, the webhook half of
// the pin TestLoadDoesNotWarnAboutTheIgnoredPlainVarWhenTheFileTokenIsInvalid
// gives loadBeatToken: a readable DISCORD_WEBHOOK_URL_FILE whose content fails
// the shape check must abort startup WITHOUT advising the operator to unset
// the plain variable — the one webhook credential still in the environment.
// Moving warnPlainVarIgnored back above the validation reintroduces exactly
// that with every other test green: TestLoadRejectsPlainHTTPWebhookFromFile
// sets the plain variable blank, so the advisory stays silent there either way.
func TestLoadDoesNotWarnAboutTheIgnoredPlainVarWhenTheFileWebhookIsInvalid(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	hookFile := filepath.Join(t.TempDir(), "webhook-url")
	if err := os.WriteFile(hookFile, []byte("http://discord.example/file-borne-hook\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/fallback")
	t.Setenv("DISCORD_WEBHOOK_URL_FILE", hookFile)

	rec := capture.Default(t)

	if _, err := Load(maxNodeNameBytes); err == nil {
		t.Fatal("Load() with a plain-http DISCORD_WEBHOOK_URL_FILE = nil, want error")
	}
	if rec.Contains("the plain variable is ignored") {
		t.Errorf("a fatal webhook validation warned that the plain variable is ignored: %v; the advice tells the operator to delete the only webhook credential left in the environment, for a configuration that never ran", rec.Messages())
	}
}

func TestLoadRejectsUnreadableBeatTokenFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-beat-token")
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "env-fallback-token")
	t.Setenv("BEAT_TOKEN_FILE", missing)

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with unreadable BEAT_TOKEN_FILE = nil, want error (the secret file must not silently fall back to the environment value, which would arm the gate with the wrong token)")
	}
	if !strings.Contains(err.Error(), "could not be read (open failed)") {
		t.Errorf("error = %q, want the read-failure diagnosis naming the failed operation: envx embeds the path in its os.PathError and this sanitizer replaces it, so the operation plus the OS reason is all the operator has left to tell a missing mount from a permission problem", err)
	}
	if !strings.Contains(err.Error(), "BEAT_TOKEN") {
		t.Errorf("error = %q, want BEAT_TOKEN context", err)
	}
	if strings.Contains(err.Error(), missing) {
		t.Errorf("error = %q embeds the BEAT_TOKEN_FILE value; os.PathError carries it and the sanitizer must drop it, because that value is the bearer token itself whenever the operator pasted the credential into the file variable", err)
	}
	if strings.Contains(err.Error(), "env-fallback-token") {
		t.Errorf("error leaks the fallback token value: %v", err)
	}
}

func TestLoadRejectsEmptyBeatTokenFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "beat-token")
	// envx judges blankness on the whitespace-trimmed content (even though it
	// returns the value itself untrimmed), so a whitespace-only file is the
	// same condition as a zero-byte one.
	if err := os.WriteFile(tokenFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "env-fallback-token")
	t.Setenv("BEAT_TOKEN_FILE", tokenFile)

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with an empty BEAT_TOKEN_FILE = nil, want error (an empty secret file is a broken mount, and the token it should have carried is required)")
	}
	if !strings.Contains(err.Error(), "blank secret file") {
		t.Errorf("error = %q, want the blank-secret-file diagnosis: secretFileError names each envx failure class so the operator is told WHICH failure happened, and the generic fallback sends someone whose mount produced an empty file to check the path instead of the content", err)
	}
	if !strings.Contains(err.Error(), "BEAT_TOKEN") {
		t.Errorf("error = %q, want BEAT_TOKEN context", err)
	}
	if strings.Contains(err.Error(), tokenFile) {
		t.Errorf("error = %q embeds the BEAT_TOKEN_FILE value; envx names the path in its own blank-file error, and this sanitizer exists to drop it because that value is the credential itself whenever the operator pasted the secret into the file variable", err)
	}
	if strings.Contains(err.Error(), "env-fallback-token") {
		t.Errorf("error leaks the fallback token value: %v", err)
	}
}

// TestAllowedHostsGate pins the ALLOWED_HOSTS states knell itself owns, none of
// which is asserted anywhere in this package: unset must stay INACTIVE (the
// documented default that accepts every Host - an accidental deny-all here 403s
// every beat in every default deployment and turns the whole switch silent),
// blank entries must not engage the gate, and an allowlist whose entries are all
// unusable must stay ACTIVE (fail closed) and SAY so, because that configuration
// rejects every non-loopback sender. webapi's tests cover the policy's
// request-time behaviour; this covers the env-to-policy mapping.
func TestAllowedHostsGate(t *testing.T) {
	tests := map[string]struct {
		raw        string
		set        bool
		wantActive bool
		wantSize   int
		wantWarn   string
	}{
		"unset accepts every host":          {wantActive: false},
		"one hostname engages the gate":     {set: true, raw: "knell.internal", wantActive: true, wantSize: 1},
		"blank entries are skipped":         {set: true, raw: "knell.internal, ,10.0.0.5", wantActive: true, wantSize: 2},
		"present but blank is reported":     {set: true, raw: "", wantActive: false, wantWarn: "is set but blank"},
		"malformed entries are dropped":     {set: true, raw: "knell.internal,http://x/y", wantActive: true, wantSize: 1, wantWarn: "dropping malformed ALLOWED_HOSTS entries"},
		"all entries unusable fails closed": {set: true, raw: ":9190", wantActive: true, wantSize: 0, wantWarn: "rejecting every non-loopback request"},
		// A PADDED blank is the same compose accident as the empty one above
		// (ALLOWED_HOSTS="${HOSTS} " with HOSTS undefined), and which of the two
		// outcomes it lands on is decided inside webhttp, not here: if
		// ParseHostList ever stopped trimming an entry, "   " would become a
		// non-blank unusable entry, the gate would ENGAGE with nothing in it, and
		// every non-loopback ping would 403 until every beat posted a false
		// MISSING notice. Pinned so a webhttp bump has to fail here rather than
		// in production.
		"padded blank never engages": {set: true, raw: "   ", wantActive: false, wantWarn: "is set but blank"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			// Serial (no t.Parallel): capture.Default swaps the process-global
			// slog default, and t.Setenv forbids parallel tests anyway.
			if tt.set {
				t.Setenv("ALLOWED_HOSTS", tt.raw)
			} else {
				unsetEnv(t, "ALLOWED_HOSTS")
			}

			rec := capture.Default(t)

			policy := allowedHosts()
			if policy.Active() != tt.wantActive {
				t.Errorf("Active() = %v, want %v: an inactive policy accepts every Host and an active one rejects every Host it does not list, so this is the difference between the documented default and a deny-all", policy.Active(), tt.wantActive)
			}
			if policy.Size() != tt.wantSize {
				t.Errorf("Size() = %d, want %d: every entry silently dropped is a hostname senders and browsers can no longer reach knell by", policy.Size(), tt.wantSize)
			}
			if tt.wantWarn == "" {
				if rec.Contains("ALLOWED_HOSTS") {
					t.Errorf("log output %v warns about ALLOWED_HOSTS for a usable configuration; a warning on the documented default trains operators to ignore the ones that matter", rec.Messages())
				}
				return
			}
			if !rec.Contains(tt.wantWarn) {
				t.Errorf("log output %v never says %q; an allowlist knell could not use is invisible otherwise - every non-loopback ping 403s and one deadline later every beat posts a false MISSING notice", rec.Messages(), tt.wantWarn)
			}
		})
	}
}

func TestLoadRejectsBlankBeatTokenFileVar(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "env-fallback-token")
	t.Setenv("BEAT_TOKEN_FILE", "")

	_, err := Load(maxNodeNameBytes)
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

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with a present-but-empty DISCORD_WEBHOOK_URL_FILE = nil, want error rather than a silent fallback to the plain variable")
	}
	if !strings.Contains(err.Error(), "DISCORD_WEBHOOK_URL_FILE") {
		t.Errorf("error = %q, want DISCORD_WEBHOOK_URL_FILE context", err)
	}
}

func TestLoadTrimsPaddedPlainWebhook(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/hook ")
	t.Setenv("BEAT_TOKEN", "unit-test-beat-token")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	// A trailing space survives url.Parse and is escaped as %20 on every
	// POST, so an untrimmed webhook 404s forever. envx trims neither channel
	// (the file channel only drops one trailing newline), so this trim is
	// knell's own. The webhook is trimmed
	// rather than refused because knell is its only sender: trimming makes it
	// deliverable, and there is no second party that must reproduce the value.
	// BEAT_TOKEN is the opposite case (see the test below) — senders must
	// reproduce it byte for byte, so rewriting it is what breaks the gate.
	if cfg.WebhookURL != "https://discord.example/hook" {
		t.Errorf("WebhookURL = %q, want the padding trimmed", cfg.WebhookURL)
	}
	if cfg.BeatToken != "unit-test-beat-token" {
		t.Errorf("BeatToken = %q, want the configured token verbatim", cfg.BeatToken)
	}
}

// TestLoadRejectsAPaddedPlainBeatToken pins that a BEAT_TOKEN carrying edge
// ASCII whitespace FAILS STARTUP instead of being silently trimmed. Trimming
// (the behaviour through cycle 8) was justified as "HTTP strips leading and
// trailing spaces and tabs, so the trimmed form is what arrives on the wire" —
// which is false for the LEADING edge: webapi verifies "Bearer "+token, so a
// leading run is INTERIOR to the Authorization value and is delivered intact.
// Configuring " secret" armed the gate for "secret", so a sender using the
// configured value sent "Bearer  secret" and got 401 while startup reported the
// gate armed and every beat went falsely missing one deadline later. Refusing
// keeps "the value you configured" and "the value that authenticates" the same
// string: a dead-man switch must not silently rewrite a credential.
func TestLoadRejectsAPaddedPlainBeatToken(t *testing.T) {
	tests := map[string]string{
		"leading space":  " unit-test-beat-token",
		"trailing space": "unit-test-beat-token ",
		"both edges":     "  unit-test-beat-token  ",
		"leading tab":    "\tunit-test-beat-token",
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv("BEAT_TOKEN", token)
			unsetEnv(t, "BEAT_TOKEN_FILE")

			_, err := Load(maxNodeNameBytes)
			if err == nil {
				t.Fatalf("Load() with BEAT_TOKEN=%q = nil, want error: rewriting the credential would arm the gate for a value the sender that uses the configured one cannot present", token)
			}
			if !strings.Contains(err.Error(), "BEAT_TOKEN") {
				t.Errorf("error = %q, want BEAT_TOKEN named so the operator knows which variable to fix", err)
			}
			if strings.Contains(err.Error(), "unit-test-beat-token") {
				t.Errorf("error = %q embeds the token value; the startup error is shipped to Loki, so it must describe the shape and never echo the credential", err)
			}
		})
	}
}

// TestLoadRejectsAPaddedFileBorneBeatToken pins the _FILE half of the refusal
// TestLoadRejectsAPaddedPlainBeatToken gives the plain variable: a secret file
// whose CONTENT carries edge ASCII whitespace FAILS STARTUP.
//
// envx returns the file's content as written apart from at most ONE trailing
// line ending, so the padded bytes reach checkBeatToken and both channels now
// refuse the same value — which is what keeps "the token you configured" and
// "the token that authenticates" the same string on the mounted-secret path too.
// The earlier envx contract ran strings.TrimSpace over the file's bytes, so the
// same file armed the gate with a REWRITTEN credential and nothing in this
// package could observe it; without this test the next envx bump can restore
// that silently. The trailing-newline cases are here to prove the one line
// ending envx still removes does not rescue a padded value: `printf '%s\n'
// token > file` stays unaffected, `printf '%s \n' token > file` does not.
func TestLoadRejectsAPaddedFileBorneBeatToken(t *testing.T) {
	tests := map[string]string{
		"leading space":               " unit-test-beat-token",
		"trailing space":              "unit-test-beat-token ",
		"trailing space then newline": "unit-test-beat-token \n",
		"leading tab then newline":    "\tunit-test-beat-token\n",
	}
	for name, content := range tests {
		t.Run(name, func(t *testing.T) {
			tokenFile := filepath.Join(t.TempDir(), "beat-token")
			if err := os.WriteFile(tokenFile, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			setValidLoadEnv(t)
			unsetEnv(t, "BEAT_TOKEN")
			t.Setenv("BEAT_TOKEN_FILE", tokenFile)

			_, err := Load(maxNodeNameBytes)
			if err == nil {
				t.Fatalf("Load() with a BEAT_TOKEN_FILE holding %q = nil, want error: the padding is part of the configured credential, so arming the gate with a trimmed copy 401s every sender that presents the file's token and turns every configured beat falsely missing one deadline later", content)
			}
			if !strings.Contains(err.Error(), "leading or trailing ASCII whitespace") {
				t.Errorf("error = %q, want the padding refusal: any other failure would mean the file channel was rejected for the wrong reason, or rewritten before checkBeatToken saw it", err)
			}
			if !strings.Contains(err.Error(), "BEAT_TOKEN") {
				t.Errorf("error = %q, want BEAT_TOKEN named so the operator knows which secret to fix", err)
			}
			if strings.Contains(err.Error(), "unit-test-beat-token") {
				t.Errorf("error = %q embeds the token value; the startup error is shipped to Loki, so it must describe the shape and never echo the credential", err)
			}
		})
	}
}

func TestLoadRejectsAnASCIIWhitespaceOnlyBeatToken(t *testing.T) {
	// Where this shape comes from: a compose quoting accident or a padded
	// interpolation (BEAT_TOKEN="${TOKEN} " with TOKEN undefined) hands the
	// process a token made only of spaces and tabs.
	//
	// Such a token has nothing but edge whitespace, so it is refused by the
	// padding rule above: its every byte is either stripped on the wire
	// (trailing SP/HTAB), illegal in a field value (CR, LF, VT, FF), or
	// delivered as an invisible leading run inside "Bearer <token>" that the
	// sender using the configured value cannot reproduce. Keeping it armed was
	// the worst outcome available for a dead-man switch: knell started,
	// reported itself gated, 401'd every ping, and one deadline later posted a
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

			_, err := Load(maxNodeNameBytes)
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
	// configuration, and trimming it to "" would leave webapi's gate with no
	// credential to verify. Accepted verbatim, gate armed, with the warning —
	// it is still almost certainly an accident.
	//
	// Eight NBSPs, because the token has to clear the minTokenLength floor to
	// reach the warning at all: NBSP encodes as two bytes, so this is exactly a
	// 16-byte token. That is also why the floor does NOT make this warning
	// unreachable — a value can be long enough and still read as blank.
	//
	// Serial (t.Setenv forbids t.Parallel): swaps the process-global slog
	// default to assert the warning.
	const token = "\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0"
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", token)
	unsetEnv(t, "BEAT_TOKEN_FILE")

	rec := capture.Default(t)

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() with an NBSP-only BEAT_TOKEN = %v, want accepted: textproto keeps a non-ASCII space, so the token is presentable and the gate must stay armed", err)
	}
	if len(token) != minTokenLength {
		t.Fatalf("fixture is %d bytes, want exactly %d: the case only reaches the warning past the length floor", len(token), minTokenLength)
	}
	if cfg.BeatToken != token {
		t.Errorf("BeatToken = %q, want %q preserved verbatim: trimming it would leave webapi's gate with a credential no sender presents", cfg.BeatToken, token)
	}
	// The shape, not just the length: without this assertion the one warning
	// that names the actual misconfiguration can be dropped and the log still
	// looks populated. The asserted text deliberately does not describe the
	// token's character class — the startup log ships to Loki, so it must not
	// narrow a guess at a live credential's alphabet.
	if !rec.Contains("mistake for absent") {
		t.Errorf("log output %v never says the gate is armed with a value that looks absent; nothing else in the startup log distinguishes this token from one the operator can read, while senders must reproduce an invisible character", rec.Messages())
	}
	if rec.Contains("whitespace") {
		t.Errorf("log output %v describes the token's character class; the startup log is shipped to Loki, so it must say the gate is armed without disclosing the credential's alphabet", rec.Messages())
	}
}

// TestLoadWarnsWhenBeatTokenCarriesAnInvisibleEdgeCharacter pins the diagnostic
// for the one unpresentable-in-practice token shape this package accepts. ASCII
// edge padding is refused, and an all-invisible token draws the
// mistake-for-absent warning, but a REAL token carrying a non-ASCII space at an
// edge (an NBSP pasted along with the value out of a rendered page) is
// presentable, so the gate arms for a string one character longer than the one
// the operator reads: every sender presenting the visible token gets 401, and
// one deadline later every configured beat posts a false MISSING notice. Without
// this warning that configuration starts, reports itself gated, and says
// nothing.
func TestLoadWarnsWhenBeatTokenCarriesAnInvisibleEdgeCharacter(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	// Every shape is a rune HTTP carries verbatim and the operator cannot see,
	// so each arms the gate for a value longer than the one they read.
	for name, token := range map[string]string{
		"leading non-ASCII space":   "\u00a0unit-test-beat-token",
		"trailing zero-width space": "unit-test-beat-token\u200b",
		"leading byte-order mark":   "\ufeffunit-test-beat-token",
	} {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv("BEAT_TOKEN", token)
			unsetEnv(t, "BEAT_TOKEN_FILE")

			rec := capture.Default(t)

			cfg, err := Load(maxNodeNameBytes)
			if err != nil {
				t.Fatalf("Load() with an invisible-edge BEAT_TOKEN = %v, want accepted: HTTP carries every byte >= 0x80, so the token is presentable and the gate must stay armed", err)
			}
			if cfg.BeatToken != token {
				t.Errorf("BeatToken = %q, want the configured value verbatim", cfg.BeatToken)
			}
			if !rec.Contains("invisible but part of the credential") {
				t.Errorf("log output %v never says the armed token carries an invisible edge character; the operator reads a visible token, configures senders with it, and every ping 401s until every beat goes falsely missing", rec.Messages())
			}
			if rec.Contains("mistake for absent") {
				t.Errorf("log output %v used the all-invisible wording for a token that has visible content: %q", rec.Messages(), token)
			}
			if rec.Contains("unit-test-beat-token") || rec.AttrContains("", "", "unit-test-beat-token") {
				t.Errorf("log output leaks the token value: %v", rec.Messages())
			}
		})
	}
}

// TestLoadRejectsASCIIPaddingAroundANonASCIISpaceBeatToken is the mixed shape
// between the two rules above. Trimming the outer spaces (the cycle-8
// behaviour) armed the gate for "\u00a0" while the operator configured
// " \u00a0 ": startup succeeded, the log reported the gate armed, and a sender
// presenting the configured value sent "Bearer  \u00a0 " and got 401 until every
// beat crossed its deadline and posted a false MISSING notice. The padding is
// refused instead, so the configured value and the verified value are the same
// string or knell does not start.
func TestLoadRejectsASCIIPaddingAroundANonASCIISpaceBeatToken(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", " \u00a0 ")
	unsetEnv(t, "BEAT_TOKEN_FILE")

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with an ASCII-padded NBSP BEAT_TOKEN = nil, want error: silently trimming the padding arms the gate for a value the sender using the configured one cannot present")
	}
	if !strings.Contains(err.Error(), "BEAT_TOKEN") {
		t.Errorf("error = %q, want BEAT_TOKEN named so the operator knows which variable to fix", err)
	}
}

func TestLoadRejectsABeatTokenHTTPCannotCarry(t *testing.T) {
	// Distinct from the whitespace-only refusal: these values are non-empty
	// after an ASCII trim, so trimming cannot rescue them, yet HTTP forbids
	// the byte they carry in a field value. Go's client refuses to write the
	// header and Go's server rejects a handcrafted one before beatHandler, so
	// the gate would be armed with a token no sender could ever present —
	// knell starts, reports healthy, records no ping, and one deadline later
	// declares every configured beat missing. The interior spellings are caught
	// by beatTokenFitsHeader and the edge ones by the padding cutset; the
	// classification is one class to the operator, which is why they share this
	// table.
	tests := map[string]string{
		"interior newline":        "alpha\nbeta",
		"interior carriage":       "alpha\rbeta",
		"trailing newline inside": "alpha\nbeta\n",
		"delete byte":             "alpha\x7fbeta",
		"vertical tab interior":   "alpha\vbeta",
		// A single trailing newline (the shape a pasted value or a here-doc
		// carries) is the EDGE spelling of the same class: CR/LF/VT/FF are
		// forbidden field-value bytes, so no sender can present it, and the edge
		// cutset refuses it before the interior check runs. Classified here
		// rather than beside the SP/HTAB padding cases, which HTTP normalizes
		// rather than forbids.
		"trailing newline": "unit-test-beat-token\n",
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv("BEAT_TOKEN", token)
			unsetEnv(t, "BEAT_TOKEN_FILE")

			_, err := Load(maxNodeNameBytes)
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
	srv := authEchoServer(t)

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
			echoed, doErr := echoAuthHeader(t, srv, token)
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
	srv := authEchoServer(t)

	for b := range 256 {
		// INTERIOR placement only: a trailing SP or HTAB is legal in a field
		// value yet stripped by the wire, and the predicate answers the
		// byte-legality question for an already-trimmed value (see
		// asciiWhitespace).
		token := "alpha" + string([]byte{byte(b)}) + "beta"
		echoed, doErr := echoAuthHeader(t, srv, token)

		carried := doErr == nil && echoed == "Bearer "+token
		if got := beatTokenFitsHeader(token); got != carried {
			t.Errorf("interior byte 0x%02x: beatTokenFitsHeader = %v, but the transport carried it verbatim = %v (err=%v echo=%q); the predicate and the wire must agree or startup either arms an unpresentable token or refuses a working one", b, got, carried, doErr, echoed)
		}
	}
}

// TestLoadAcceptsABeatTokenAtTheLengthFloor pins the inclusive edge of the
// refusal: minTokenLength bytes is the SHORTEST accepted token, so an
// off-by-one would fail startup on a value the README tells the operator to use.
func TestLoadAcceptsABeatTokenAtTheLengthFloor(t *testing.T) {
	token := strings.Repeat("x", minTokenLength)
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", token)
	unsetEnv(t, "BEAT_TOKEN_FILE")

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() with a %d-byte BEAT_TOKEN = %v, want accepted: the floor is inclusive", minTokenLength, err)
	}
	if cfg.BeatToken != token {
		t.Errorf("BeatToken = %q, want the configured %d-byte token", cfg.BeatToken, minTokenLength)
	}
}

func TestLoadFallsBackToTheHostnameWhenNodeNameIsUnset(t *testing.T) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		t.Skipf("cannot determine the hostname: %v", err)
	}
	setValidLoadEnv(t)
	// ABSENT, not present-but-empty: an unset NODE_NAME is what a deployment
	// that accepts the hostname default ships, and this package treats the two
	// states differently throughout (a present-but-blank NODE_NAME is warned
	// about and ignored; an unset one is silent).
	unsetEnv(t, "NODE_NAME")

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Node != host {
		t.Errorf("Node = %q, want the hostname %q; the node name prefixes every Discord notice, so a fallback that reports a constant makes a three-observer set unattributable", cfg.Node, host)
	}
}

// TestHostnameNodeFallsBackWhenTheOSCannotAnswer drives the two branches the
// osHostname seam exists to reach: neither is reachable through the
// environment, and both decide the name that prefixes every Discord notice, so
// a regression that returned "" or dropped the warning would leave a
// three-observer set unattributable with nothing failing.
func TestHostnameNodeFallsBackWhenTheOSCannotAnswer(t *testing.T) {
	// Serial (no t.Parallel): the osHostname seam and capture.Default are both
	// process-global.
	tests := map[string]struct {
		host string
		err  error
		warn string
	}{
		"hostname error": {err: os.ErrPermission, warn: "failed to determine hostname"},
		"blank hostname": {host: "   ", warn: "hostname is blank"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			orig := osHostname
			t.Cleanup(func() { osHostname = orig })
			osHostname = func() (string, error) { return tt.host, tt.err }

			rec := capture.Default(t)

			if got := hostnameNode(); got != "unknown" {
				t.Errorf("hostnameNode() = %q, want \"unknown\": every notice is prefixed with this name, so an empty one names no observer at all", got)
			}
			if !rec.Contains(tt.warn) {
				t.Errorf("hostnameNode() did not warn %q; the warning is the only signal that the notices name a fallback rather than this host: %v", tt.warn, rec.Messages())
			}
		})
	}
}

func TestLoadTrimsPaddedListenAddr(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("LISTEN_ADDR", "  0.0.0.0:9999  ")

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.ListenAddr != "0.0.0.0:9999" {
		t.Errorf("ListenAddr = %q, want the trimmed address: net.Listen resolves a padded address as a hostname lookup, so the container crash-loops with the padding invisible in the log line", cfg.ListenAddr)
	}
}

func TestLoadTrimsPaddedNodeName(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("NODE_NAME", "  node-1  ")

	cfg, err := Load(maxNodeNameBytes)
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

	cfg, err := Load(maxNodeNameBytes)
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

	_, err := Load(maxNodeNameBytes)
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

func TestNodeNameCapCoversTheHostnameFallback(t *testing.T) {
	t.Parallel()

	// POSIX's HOST_NAME_MAX. Linux bounds a hostname at 64, other kernels allow
	// up to this, and hostnameNode's value is the DEFAULT node name, so this is
	// the widest value that path can hand notify.
	const posixHostNameMax = 255
	if maxNodeNameBytes < posixHostNameMax {
		t.Fatalf("maxNodeNameBytes = %d, want at least %d: hostnameNode is deliberately not length-checked, so a cap below the OS hostname bound lets the DEFAULT node name render a notice notify's budget test never measured - Discord answers 400 for an over-limit content and no notice is ever delivered", maxNodeNameBytes, posixHostNameMax)
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

		cfg, err := Load(maxNodeNameBytes)
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

		if _, err := Load(maxNodeNameBytes); err == nil {
			t.Fatalf("Load() with a %d-byte (%d-rune) NODE_NAME = nil, want error: the bound is counted in bytes, which is the conservative direction against Discord's character limit", len(node), maxNodeNameBytes-10)
		}
	})
}

func TestLoadRejectsWhitespaceOnlyWebhook(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL", "   ")

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with a whitespace-only DISCORD_WEBHOOK_URL = nil, want error: a broken secret pipeline must fail startup rather than arm a switch that can never ring")
	}
	if !strings.Contains(err.Error(), "set but empty") {
		t.Errorf("error = %q, want the set-but-empty diagnosis rather than the misleading https-scheme rejection", err)
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

	cfg, err := Load(maxNodeNameBytes)
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
// ungated and the next start failing on a missing credential entirely.
func TestLoadDoesNotWarnWhenOnlyThePlainVarsAreSet(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "plain-only-beat-token")
	unsetEnv(t, "BEAT_TOKEN_FILE")
	unsetEnv(t, "DISCORD_WEBHOOK_URL_FILE")

	rec := capture.Default(t)

	cfg, err := Load(maxNodeNameBytes)
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

// TestConfigLogValueNeverRendersASecret pins the redaction seam:
// LogValue is the reason a call site can log a whole Config without leaking, so
// DISCORD_WEBHOOK_URL is reported by presence and BEAT_TOKEN is not reported at
// all — it is required, so its presence is a constant and an attr for it would
// report no state while giving a future edit somewhere to render the value. The
// receiver under test is a VALUE, not a pointer: Load returns
// Config by value and that is the form a future slog call would hand a
// logger, so a seam that only covers *Config would not cover the leak.
func TestConfigLogValueNeverRendersASecret(t *testing.T) {
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
	if got["webhook"] != "configured" {
		t.Errorf("webhook = %q, want \"configured\": presence is the only thing this attr may report", got["webhook"])
	}
	if _, reported := got["beat_auth"]; reported {
		t.Errorf("LogValue renders beat_auth = %q; the token is required, so the attr can only ever say \"required\" and reports no state", got["beat_auth"])
	}

	empty := Config{LogLevel: slog.LevelInfo}
	got = map[string]string{}
	for _, attr := range empty.LogValue().Group() {
		got[attr.Key] = attr.Value.String()
	}
	if got["webhook"] != "unset" {
		t.Errorf("unconfigured: webhook = %q, want \"unset\"", got["webhook"])
	}
}

// TestConfigLogValueReportsEveryNonSecretField pins the accuracy half of
// LogValue's contract; TestConfigLogValueNeverRendersASecret
// pins the hygiene half. LogValue exists so a call site can hand a whole
// Config to slog, and those six attrs are then the entire rendering of a
// configuration that is env-only, with no reload and no readback endpoint.
// Every value below differs from any plausible default and from every sibling
// field, so an attr rewired to a literal or to the wrong field fails here
// instead of publishing a line that contradicts the configuration running.
func TestConfigLogValueReportsEveryNonSecretField(t *testing.T) {
	t.Parallel()

	// Two entries, so allowed_hosts pins the SIZE rather than merely "active":
	// an allowlist state rendered from the wrong side of the policy (a hardcoded
	// 1, or Active() read as the count) still reads plausible in a log line.
	policy, invalid := webhttp.ParseHostList([]string{"knell.internal", "10.0.0.5"})
	if len(invalid) > 0 {
		t.Fatalf("ParseHostList rejected %v; the fixture must be a valid allowlist", invalid)
	}

	cfg := Config{
		AllowedHosts: policy,
		WebhookURL:   "https://discord.example/hook",
		Node:         "observer-borgcube",
		ListenAddr:   "127.0.0.1:19190",
		BeatToken:    "unit-test-beat-token",
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
		"beats":         "2",
		"node":          "observer-borgcube",
		"listen_addr":   "127.0.0.1:19190",
		"webhook":       "configured",
		"allowed_hosts": "allowlist(2)",
		"log_level":     "DEBUG",
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

// TestConfigLogValueReportsTheHostAllowlistState pins all three states the
// allowed_hosts attr distinguishes, because the attr exists for the state the
// rest of the process is SILENT about: a misspelled ALLOWED_HOSTS is
// indistinguishable from unset, draws no parse warning, and leaves the
// DNS-rebinding guard off while the operator believes it is armed.
//
// The zero-size case is why the attr carries a count instead of a boolean:
// malformed entries are warned-and-dropped, so an ENGAGED allowlist can hold no
// usable host and then rejects every non-loopback request — no sender can record
// a beat — and a bare on/off value would render that identically to a working
// allowlist. The nil case is the second reason: Config values built without
// Load (every other test here, and a zero Config) must render rather than panic.
func TestConfigLogValueReportsTheHostAllowlistState(t *testing.T) {
	t.Parallel()

	twoHosts, invalid := webhttp.ParseHostList([]string{"knell.internal", "10.0.0.5"})
	if len(invalid) > 0 {
		t.Fatalf("ParseHostList rejected %v; the fixture must be a valid allowlist", invalid)
	}
	// Non-blank but uncanonicalizable (a scheme and path, the shape the parse
	// warning names): the gate ENGAGES fail-closed with nothing in it.
	failClosed, invalid := webhttp.ParseHostList([]string{"https://knell.internal/"})
	if len(invalid) != 1 {
		t.Fatalf("ParseHostList reported %v invalid entries, want exactly 1: the fixture must engage the gate with no usable host", invalid)
	}
	blank, _ := webhttp.ParseHostList([]string{""})

	for name, tc := range map[string]struct {
		policy *webhttp.HostPolicy
		want   string
	}{
		"unset leaves every Host accepted":      {policy: nil, want: "any"},
		"blank value never engages the gate":    {policy: blank, want: "any"},
		"an allowlist reports its size":         {policy: twoHosts, want: "allowlist(2)"},
		"an unusable allowlist is still a gate": {policy: failClosed, want: "allowlist(0)"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{AllowedHosts: tc.policy, LogLevel: slog.LevelInfo}
			got := ""
			for _, attr := range cfg.LogValue().Group() {
				if attr.Key == "allowed_hosts" {
					got = attr.Value.String()
				}
			}
			if got != tc.want {
				t.Errorf("allowed_hosts = %q, want %q: the startup summary is the only rendering of an env-only configuration, so a wrong value here is an operator believing the rebinding guard is in a state it is not", got, tc.want)
			}
		})
	}
}

// TestParseWebhookURLRejectsUndeliverableShapes pins the two refusals that
// separate a TRANSPORTABLE URL from a DELIVERABLE one. Both are invisible at
// runtime: startup succeeds, /healthz is healthy, outages are detected, and
// only the notice fails — forever. This table pins the representative exact
// rejection shapes and their messages, while FuzzParseWebhookURL independently
// asserts the same two invariants from the accepted side (an accepted URL
// carries a non-root path and no invisible rune).
func TestParseWebhookURLRejectsUndeliverableShapes(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		raw     string
		wantErr string
	}{
		"host only":              {raw: "https://discord.example", wantErr: "missing path"},
		"root path only":         {raw: "https://discord.example/", wantErr: "missing path"},
		"interior space in path": {raw: "https://discord.example/api/webhooks/1/ab c", wantErr: "contains a space"},
		// An authority made of nothing but a port has a NON-EMPTY url.Host
		// (":443"), so a Host-based gate admits it: startup succeeds, the
		// health marker goes ready, and every notice then fails as a transport
		// error for lack of any destination host. Hostname() is what tells the
		// two apart.
		"port-only authority": {raw: "https://:443/api/webhooks/1/abc", wantErr: "missing host"},
		// Deliberate scope decision, not an oversight: knell posts to Discord
		// only, and a Discord webhook always carries its credential in the
		// path (/api/webhooks/{id}/{token}). A query-carried credential is out
		// of scope, so a URL whose only credential-bearing part is its query
		// string is refused by the same missing-path rule rather than being
		// admitted as a second supported shape. Pins the decision so a later
		// "relax the path check, the query has the token" change has to argue
		// with a test instead of a silent guard.
		"query-only credential": {raw: "https://relay.example?token=abc", wantErr: "missing path"},
		// The same defect class as the interior space above, one the ASCII-only
		// check missed: url.Parse accepts a non-ASCII space or a zero-width
		// rune and percent-encodes it on every request, in the host as well as
		// the path, so startup succeeds and no notice can ever be delivered.
		"non-breaking space in path": {raw: "https://discord.example/api/webhooks/1/ab\u00a0c", wantErr: "invisible character"},
		"zero-width space in host":   {raw: "https://discord.\u200bexample/api/webhooks/1/abc", wantErr: "invisible character"},
		// The byte-level twin of the two rows above: an invalid UTF-8 byte
		// decodes to U+FFFD, which unicode.IsPrint accepts, so only the
		// utf8.ValidString gate refuses it — and the committed fuzz seeds are
		// all valid UTF-8, so without this row no per-PR test fails when that
		// gate is reverted (the weekly fuzz corpus does not persist).
		"invalid UTF-8 byte in path": {raw: "https://discord.example/api/webhooks/1/ab\x80c", wantErr: "invisible character"},
		// url.Parse checks a port's syntax, not its range: an out-of-range port
		// starts the process healthy and then fails EVERY POST inside net/http
		// ("invalid port"), so the switch arms and can never ring. Both edges of
		// the unusable range are pinned because each is a distinct spelling of
		// the same operator typo.
		"port zero":        {raw: "https://discord.example:0/api/webhooks/1/abc", wantErr: "port must be between 1 and 65535"},
		"port above range": {raw: "https://discord.example:65536/api/webhooks/1/abc", wantErr: "port must be between 1 and 65535"},
		"port far above":   {raw: "https://discord.example:99999/api/webhooks/1/abc", wantErr: "port must be between 1 and 65535"},
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
			for _, secret := range []string{"discord.example", "relay.example", "token=abc"} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("error = %q embeds %q from the URL; the webhook URL is a secret and startup errors are shipped to Loki", err, secret)
				}
			}
		})
	}

	// The usable port edges stay accepted: the range check must refuse the
	// unusable spellings only, never a working non-default relay port.
	for _, raw := range []string{
		"https://discord.example:1/api/webhooks/1/abc",
		"https://discord.example:65535/api/webhooks/1/abc",
	} {
		if _, err := parseWebhookURL(raw); err != nil {
			t.Errorf("parseWebhookURL(%q) = %v, want nil: a port inside 1..65535 is usable", raw, err)
		}
	}

	if _, err := parseWebhookURL("https://discord.example/api/webhooks/1/abc"); err != nil {
		t.Errorf("parseWebhookURL(a well-formed webhook) = %v, want nil: the deliverability checks must not refuse a working configuration", err)
	}
}

// TestLoadRejectsAWebhookWithoutAHostname is the startup half of the port-only
// authority case: an operator whose secret pipeline dropped the host must be
// told at boot, not one outage later. Without it the process starts, /healthz
// reports ready, and every notice fails for lack of a destination — the exact
// failure mode a dead-man switch cannot afford.
func TestLoadRejectsAWebhookWithoutAHostname(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL", "https://:443/api/webhooks/1/canary-webhook-token")

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with a port-only webhook authority = nil, want error: startup is the only moment the operator is watching")
	}
	if !strings.Contains(err.Error(), "missing host") {
		t.Errorf("error = %q, want it to name the missing host", err)
	}
	if strings.Contains(err.Error(), "canary-webhook-token") {
		t.Errorf("error = %q leaks the webhook credential; the startup error is shipped to Loki", err)
	}
}

// TestLoadDoesNotLeakACredentialPastedIntoASecretFileVariable pins the
// sanitizer on both _FILE channels. envx embeds the KEY_FILE VALUE in its
// blank-file, path-policy and os.PathError messages — correct for a path, and a
// credential leak for the most common misconfiguration of this convention: the
// operator pastes the secret itself into DISCORD_WEBHOOK_URL_FILE (or
// BEAT_TOKEN_FILE) instead of a path to it. Wrapping envx's error verbatim
// copied that live credential into the startup ERROR line, from where Loki
// keeps it long after the value is rotated.
func TestLoadDoesNotLeakACredentialPastedIntoASecretFileVariable(t *testing.T) {
	tests := map[string]struct {
		key    string
		canary string
	}{
		"webhook url pasted into the file variable": {
			key:    "DISCORD_WEBHOOK_URL",
			canary: "https://discord.example/api/webhooks/1/canary-webhook-token",
		},
		"bearer token pasted into the file variable": {
			key:    "BEAT_TOKEN",
			canary: "canary-bearer-token-value",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv(tt.key+"_FILE", tt.canary)

			_, err := Load(maxNodeNameBytes)
			if err == nil {
				t.Fatalf("Load() with %s_FILE holding the credential itself = nil, want error: the file channel must fail closed", tt.key)
			}
			if !strings.Contains(err.Error(), tt.key+"_FILE") {
				t.Errorf("error = %q, want it to name %s_FILE so the operator knows which variable is broken", err, tt.key)
			}
			if strings.Contains(err.Error(), tt.canary) {
				t.Errorf("error = %q embeds the pasted credential; startup errors are shipped to Loki, where they outlive the secret", err)
			}
		})
	}
}

// TestLoadRefusesAHeaderIllegalTokenWithoutCallingItArmed pins the ORDER of
// checkBeatToken's two remaining checks. A plain BEAT_TOKEN of Unicode spaces
// around a control byte passes the ASCII-edge refusal and reads blank to
// strings.TrimSpace, so warning first logged "the gate is armed" for a value
// the very next check refuses to start on — two contradictory startup signals
// for one value, of which only the failure ever happened.
// TestLoadKeepsANonASCIISpaceBeatTokenArmed pins the warning itself for a valid
// NBSP-only token, so the two together fix the order.
func TestLoadRefusesAHeaderIllegalTokenWithoutCallingItArmed(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "\u00a0\n\u00a0")

	rec := capture.Default(t)

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with a BEAT_TOKEN carrying an interior newline = nil, want error: no sender can present a control byte in a header value")
	}
	if !strings.Contains(err.Error(), "control character") {
		t.Errorf("error = %q, want the control-character refusal", err)
	}
	if rec.Contains("mistake for absent") {
		t.Errorf("startup reported the gate armed for a token it then refused: %v", rec.Messages())
	}
}

// TestLoadWarnsOnlyWhenLogLevelIsUnparseable pins the ONLY signal that a
// mistyped LOG_LEVEL was ignored. slogx.ParseLevel returns ok=true for an unset
// value and ok=false only for a non-empty unparseable one, so this WARN is what
// separates "the operator asked for debug and got debug" from "the operator
// typo'd and is silently running at info" — a distinction that matters exactly
// when someone is turning up logging to diagnose a live outage.
// The table pins both the resulting level and the warning's presence, so a
// regression in either half — an inverted or dropped !ok guard that loses the
// warning on a typo, or gains a spurious one on a perfectly valid level — fails
// this one behavior-focused test.
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

			cfg, err := Load(maxNodeNameBytes)
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

// TestLoadWarnsWhenLogLevelIsPresentButBlank pins the OTHER half of the
// LOG_LEVEL diagnostic, the one TestLoadWarnsOnlyWhenLogLevelIsUnparseable
// deliberately does not cover: slogx.ParseLevel returns ok=true for a blank
// value, so without this line a LOG_LEVEL that resolved empty (compose
// interpolation of an undefined variable produces exactly that shape) falls
// back to info in total silence — on the one knob an operator turns while
// diagnosing a live outage. The message is distinct from the typo warning on
// purpose, so the sibling test's "present but blank does not warn" case keeps
// pinning the typo-vs-accident distinction.
func TestLoadWarnsWhenLogLevelIsPresentButBlank(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	setValidLoadEnv(t)
	t.Setenv("LOG_LEVEL", "   ")

	rec := capture.Default(t)

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want INFO: a blank LOG_LEVEL must land on the documented default", cfg.LogLevel)
	}
	if !rec.Contains("LOG_LEVEL is set but blank") {
		t.Errorf("no warning that a blank LOG_LEVEL was ignored; the operator set the variable and this process threw the value away, so the level they turned up to diagnose an outage silently never applies: %v", rec.Messages())
	}
}

// TestLoadRejectsAFileBorneBeatTokenHTTPCannotCarry pins that the
// control-byte refusal covers the _FILE channel too, the same way
// TestLoadRejectsPlainHTTPWebhookFromFile pins the https gate for the webhook's
// file channel. envx removes at most one trailing line ending, so a two-line
// secret file (a stray second line, a copy-pasted pair of lines) hands
// loadBeatToken a token with an INTERIOR newline that no trim can rescue and no
// sender can present.
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

	_, err := Load(maxNodeNameBytes)
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

// TestLoadWarnsOnlyWhenAnOptionalVariableIsPresentButBlank pins the
// present-versus-unset distinction on the two optional variables that fall
// back silently: NODE_NAME and LISTEN_ADDR. Both warn ONLY when the operator
// set the variable and this process threw the value away; an unset variable is
// the documented default and must stay silent. Neither half is asserted
// anywhere else - TestLoadFallsBackToTheHostnameWhenNodeNameIsUnset pins the
// hostname fallback and captures no log, and the :9190 fallback VALUE is also
// pinned by TestLoadDefaultsAndFailures while the warning that NAMES it is
// asserted only by this test's own 'names the default' subtest - so
// dropping the `if present` guard makes every default deployment log two
// warnings about variables nobody set, and dropping the warning lets a value
// the operator set be discarded with no signal at all, on the two settings
// that decide which observer a Discord notice names and whether /metrics
// answers at the scraped address.
func TestLoadWarnsOnlyWhenAnOptionalVariableIsPresentButBlank(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	tests := map[string]struct {
		key      string
		value    string
		unset    bool
		warnSub  string
		wantWarn bool
	}{
		"node name blank warns": {key: "NODE_NAME", value: "  ", warnSub: "NODE_NAME is set but blank", wantWarn: true},
		// TrimSpace does not see these: an all-invisible NODE_NAME (zero-width
		// space, BOM) is blank to the operator reading it, so it takes the same
		// warn-and-fall-back-to-hostname path. Without this row nothing pins that,
		// and the notices silently go back to reading "[knell ]".
		"node name invisible warns":   {key: "NODE_NAME", value: "\u200b\ufeff", warnSub: "NODE_NAME is set but blank", wantWarn: true},
		"node name unset stays quiet": {key: "NODE_NAME", unset: true, warnSub: "NODE_NAME is set but blank", wantWarn: false},
		"node name set stays quiet":   {key: "NODE_NAME", value: "observer-1", warnSub: "NODE_NAME is set but blank", wantWarn: false},
		"listen addr blank warns":     {key: "LISTEN_ADDR", value: "   ", warnSub: "LISTEN_ADDR is set but blank", wantWarn: true},
		"listen addr unset is quiet":  {key: "LISTEN_ADDR", unset: true, warnSub: "LISTEN_ADDR is set but blank", wantWarn: false},
		"listen addr set is quiet":    {key: "LISTEN_ADDR", value: "127.0.0.1:9999", warnSub: "LISTEN_ADDR is set but blank", wantWarn: false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			if tt.unset {
				unsetEnv(t, tt.key)
			} else {
				t.Setenv(tt.key, tt.value)
			}

			rec := capture.Default(t)

			if _, err := Load(maxNodeNameBytes); err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if got := rec.Contains(tt.warnSub); got != tt.wantWarn {
				t.Errorf("%s=%q (unset=%v): warned = %v, want %v; the warning is the only signal that a value the operator set was discarded, and a warning for an unset variable trains the operator to ignore it: %v",
					tt.key, tt.value, tt.unset, got, tt.wantWarn, rec.Messages())
			}
		})
	}

	// The blank-LISTEN_ADDR warning must name the address the listener falls
	// back to: without it the operator reads "ignored" with no way to tell which
	// port the scrape should target.
	t.Run("blank listen addr warning names the default", func(t *testing.T) {
		setValidLoadEnv(t)
		t.Setenv("LISTEN_ADDR", " ")

		rec := capture.Default(t)

		cfg, err := Load(maxNodeNameBytes)
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.ListenAddr != defaultListenAddr {
			t.Fatalf("ListenAddr = %q, want %q", cfg.ListenAddr, defaultListenAddr)
		}
		if !rec.HasAttr("LISTEN_ADDR is set but blank", "listen_addr", defaultListenAddr) {
			t.Errorf("blank-LISTEN_ADDR warning does not report the fallback address %q: %v", defaultListenAddr, rec.Records())
		}
	})

	// The blank-NODE_NAME warning has to name the way out, because the operator
	// who set the variable is the one reading it.
	t.Run("blank node name warning tells the operator both options", func(t *testing.T) {
		setValidLoadEnv(t)
		t.Setenv("NODE_NAME", "\t")

		rec := capture.Default(t)

		if _, err := Load(maxNodeNameBytes); err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if !strings.Contains(strings.Join(rec.Messages(), "\n"), "unset the variable") {
			t.Errorf("blank-NODE_NAME warning does not name the unset-for-hostname option: %v", rec.Messages())
		}
	})
}
