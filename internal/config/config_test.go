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

	"github.com/cplieger/envx"
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
		"ALLOWED_HOSTS",
		"BEATS",
		"BEAT_TOKEN",
		"BEAT_TOKEN_FILE",
		"DISCORD_WEBHOOK_URL",
		"DISCORD_WEBHOOK_URL_FILE",
		"LISTEN_ADDR",
		"LOG_LEVEL",
		"NODE_NAME",
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
	if _, err := Load(maxNodeNameBytes); err == nil || !strings.Contains(err.Error(), "DISCORD_WEBHOOK_URL is set but empty") {
		t.Errorf("Load() with a present-but-empty DISCORD_WEBHOOK_URL error = %v, want the set-but-empty diagnosis: the operator DID set the variable, so \"is required\" sends them to look for a missing key instead of the secret pipeline that delivered nothing", err)
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

// TestLoadRefusesWithoutAWebhook pins the required webhook for the truly-ABSENT
// case, the half TestLoadDefaultsAndFailures does not cover (it pins
// present-but-empty). With neither DISCORD_WEBHOOK_URL nor
// DISCORD_WEBHOOK_URL_FILE set, Load must refuse startup with the is-required
// diagnosis: a regression that returned an empty webhook instead would start a
// switch that can never ring while /healthz reports ready, and the
// set-but-empty wording would send a fresh deployment hunting a secret
// pipeline it never configured.
func TestLoadRefusesWithoutAWebhook(t *testing.T) {
	setValidLoadEnv(t)
	unsetEnv(t, "DISCORD_WEBHOOK_URL")
	unsetEnv(t, "DISCORD_WEBHOOK_URL_FILE")

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with no webhook configured = nil, want a startup refusal: a dead-man switch with no webhook can never ring, and startup is the only moment the operator is watching")
	}
	if !strings.Contains(err.Error(), "DISCORD_WEBHOOK_URL is required") {
		t.Errorf("error = %q, want the is-required diagnosis: the operator has to supply a webhook, and any other wording sends them to the wrong next move", err)
	}
	if strings.Contains(err.Error(), "set but empty") {
		t.Errorf("error = %q uses the set-but-empty diagnosis for an unset variable; the two refusals deliberately ask for different next moves (supply a value vs fix the pipeline that delivered nothing)", err)
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

// TestBeatTokenLengthCeiling pins both edges of the token maximum. The bound is
// not about credential strength — a 512-byte token is absurd, not weak — but
// about the header budget the credential TRAVELS in: maxTokenLength is the
// token's share of MaxRequestHeaderBytes, so an accepted token always leaves
// headerOverheadAllowance bytes for the request line, Host and the "Bearer "
// prefix. One byte past the maximum still travels — 431 arrives only once the
// WHOLE header block exceeds MaxRequestHeaderBytes — so what the bound protects
// is that reserve. Both edges are asserted because either mistake is silent in
// production: a maximum that refused a token the budget reserves room for would
// stop deployments that work today, and one that admitted an unbounded token
// would let a BEAT_TOKEN_FILE pointing at the wrong file eat the reserve until
// every ping is answered 431 by an endpoint reporting itself gated and one
// deadline later every configured beat posts a false MISSING notice.
func TestBeatTokenLengthCeiling(t *testing.T) {
	atMax := strings.Repeat("a", maxTokenLength)

	t.Run("a token at the maximum starts", func(t *testing.T) {
		setValidLoadEnv(t)
		t.Setenv("BEAT_TOKEN", atMax)
		unsetEnv(t, "BEAT_TOKEN_FILE")

		cfg, err := Load(maxNodeNameBytes)
		if err != nil {
			t.Fatalf("Load() with a %d-byte BEAT_TOKEN error = %v, want nil: the maximum is a value knell accepts, and the header budget reserves headerOverheadAllowance bytes on top of it", maxTokenLength, err)
		}
		if cfg.BeatToken != atMax {
			t.Error("BeatToken differs from the configured value: the credential is verified verbatim, so any rewrite 401s every ping")
		}
	})

	t.Run("one byte past the maximum refuses startup", func(t *testing.T) {
		setValidLoadEnv(t)
		t.Setenv("BEAT_TOKEN", atMax+"a")
		unsetEnv(t, "BEAT_TOKEN_FILE")

		_, err := Load(maxNodeNameBytes)
		if err == nil {
			t.Fatalf("Load() with a %d-byte BEAT_TOKEN = nil, want a startup refusal: the maximum is the token's share of the %d-byte header block knell reads, so anything past it eats the %d-byte reserve held for the request line, Host and the \"Bearer \" prefix", maxTokenLength+1, MaxRequestHeaderBytes, headerOverheadAllowance)
		}
		if !strings.Contains(err.Error(), strconv.Itoa(maxTokenLength)) {
			t.Errorf("refusal = %q, want it to name the %d-byte maximum: it is the number the operator has to act on", err, maxTokenLength)
		}
		if strings.Contains(err.Error(), atMax) {
			t.Error("refusal echoes the token value into the startup log")
		}
	})
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
// blank entries must not engage the gate, and any entry knell cannot use must
// REFUSE STARTUP, because a dropped entry leaves an allowlist that is not the one
// configured: every non-loopback request under that hostname is refused 403,
// including the pings, which then surface as missing-beat alerts for healthy
// services. webapi's tests cover the policy's request-time behaviour; this covers
// the env-to-policy mapping.
func TestAllowedHostsGate(t *testing.T) {
	tests := map[string]struct {
		raw        string
		set        bool
		wantActive bool
		wantSize   int
		wantWarn   string
		wantErr    string
	}{
		"unset accepts every host":      {wantActive: false},
		"one hostname engages the gate": {set: true, raw: "knell.internal", wantActive: true, wantSize: 1},
		"blank entries are skipped":     {set: true, raw: "knell.internal, ,10.0.0.5", wantActive: true, wantSize: 2},
		"present but blank is reported": {set: true, raw: "", wantActive: false, wantWarn: "is set but blank"},
		"one unusable entry refuses":    {set: true, raw: "knell.internal,http://x/y", wantErr: "http://x/y"},
		// Formerly pinned as active-size-zero, warned about and STARTED: the
		// oracle is unchanged (this configuration rejects every non-loopback
		// sender, so no beat can be recorded), and the verdict is now the
		// refusal that makes it impossible to reach production.
		"all entries unusable refuses": {set: true, raw: ":9190", wantErr: ":9190"},
		// A PADDED blank is the same compose accident as the empty one above
		// (ALLOWED_HOSTS="${HOSTS} " with HOSTS undefined), and which of the two
		// outcomes it lands on is decided inside webhttp, not here: if
		// ParseHostList ever stopped trimming an entry, "   " would become a
		// non-blank unusable entry and startup would refuse instead of accepting
		// every Host. Pinned so a webhttp bump has to fail here rather than in
		// production.
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

			// nil options: this table asserts the env-to-policy mapping only
			// (active, size, the blank warning, the startup refusal), none of
			// which the serving-side options affect — webapi's tests cover the
			// shipped envelope and the loopback exemption through
			// webapi.HostPolicyOptions.
			policy, err := allowedHosts(nil)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("allowedHosts() with %q = nil error, want a startup refusal: knell would serve an allowlist the operator never configured, and every non-loopback ping it should have admitted 403s until one deadline later every beat posts a false MISSING notice", tt.raw)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("refusal = %q, want it to name the unusable entry %q: it is the only part of the value the operator can act on", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("allowedHosts() with %q error = %v, want nil: a usable configuration must start", tt.raw, err)
			}
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

// TestLoadThreadsTheHostPolicyOptionsThrough pins the hop the cycle-7 refactor
// introduced. The loopback exemption and the ALLOWED_HOSTS-naming 403 envelope
// are no longer applied inside allowedHosts; they arrive as a Load PARAMETER
// (internal/webapi's HostPolicyOptions, passed by the composition root). Every
// other Load call in this suite passes none, webapi's tests build the policy
// from HostPolicyOptions directly, and the lifecycle test's host_not_allowed
// assertion matches webhttp's DEFAULT code — so without this, a Load that
// forwards nothing keeps the whole suite green while the shipped allowlist 403s
// the in-container `curl http://127.0.0.1:9190/healthz` the README promises
// keeps working.
func TestLoadThreadsTheHostPolicyOptionsThrough(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv.
	setValidLoadEnv(t)
	t.Setenv("ALLOWED_HOSTS", "knell.internal")

	// A genuinely local caller: loopback socket peer AND loopback Host, the only
	// shape WithLoopbackExempt admits. It is NOT in the allowlist, so it is
	// admitted by the option or by nothing.
	loopback := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9190/healthz", nil)
		r.RemoteAddr = "127.0.0.1:54321"
		return r
	}

	exempt, err := Load(maxNodeNameBytes, webhttp.WithLoopbackExempt())
	if err != nil {
		t.Fatalf("Load() with the loopback exemption: %v", err)
	}
	if !exempt.AllowedHosts.Allows(loopback()) {
		t.Error("Load did not forward its host-policy options: a loopback peer under a loopback Host is refused, so an in-container curl http://127.0.0.1:9190/healthz 403s under any allowlist and an operator's own probe stops working")
	}

	bare, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() with no options: %v", err)
	}
	if bare.AllowedHosts.Allows(loopback()) {
		t.Error("the same request is admitted with NO options passed, so the assertion above pins webhttp's default rather than the parameter Load forwards; pick an option whose effect the default does not already give")
	}
}

// TestLoadRejectsAnUnusableAllowedHostsEntry pins that allowedHosts' refusal
// reaches STARTUP. TestAllowedHostsGate calls allowedHosts directly, so
// nothing asserts that Load propagates its error: dropping that check leaves
// cfg.AllowedHosts nil, an inactive policy accepts every Host, and knell
// starts happily with the DNS-rebinding guard OFF while the operator reads
// their allowlist in the compose file. /metrics enumerates every beat and its
// freshness, and the bearer gate does not cover it, so that is the one
// endpoint the allowlist exists to protect.
func TestLoadRejectsAnUnusableAllowedHostsEntry(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("ALLOWED_HOSTS", "knell.internal,http://x/y")

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with an unusable ALLOWED_HOSTS entry = nil, want the refusal propagated: an unpropagated error leaves the policy nil, so every Host is accepted and the rebinding guard is off while the operator believes the allowlist is armed")
	}
	if !strings.Contains(err.Error(), "ALLOWED_HOSTS") {
		t.Errorf("Load() error = %q, want ALLOWED_HOSTS named so the operator knows which variable to fix", err)
	}
	// %q, not %v: the entries are rendered quoted so an invisible rune pasted in
	// with a hostname is escaped rather than printed invisibly. Asserting the
	// QUOTES (not the bare entry, which both verbs print) is what fails if the
	// rendering is reverted.
	if !strings.Contains(err.Error(), `"http://x/y"`) {
		t.Errorf("Load() error = %q, want the unusable entry named and QUOTED: it is the only part of the value the operator can act on, and %%v prints an invisible rune invisibly, so the refusal would name a hostname that looks correct", err)
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
			// The mirror of the file-borne case: the clause fires only on
			// envx.SourceFile, so a plain-variable refusal must not send the
			// operator to a mount they never configured.
			if strings.Contains(err.Error(), "came from BEAT_TOKEN_FILE") {
				t.Errorf("error = %q blames BEAT_TOKEN_FILE for a value the plain variable supplied", err)
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
			// The CHANNEL, not just the variable: a BEAT_TOKEN_FILE pointing at
			// the wrong file crash-loops the observer, and a refusal naming only
			// BEAT_TOKEN describes a variable the operator may never have set.
			if !strings.Contains(err.Error(), "came from BEAT_TOKEN_FILE") {
				t.Errorf("error = %q, want it to name the channel the refused value arrived through; the padding refusal itself never mentions the _FILE variable, so only fileSourcedValueError can supply it", err)
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
	// credential to verify. Accepted verbatim, gate armed.
	//
	// Eight NBSPs, because the token has to clear the minTokenLength floor to
	// be accepted at all: NBSP encodes as two bytes, so this is exactly a
	// 16-byte token — a value can read as blank and still be long enough.
	const token = "\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0"
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", token)
	unsetEnv(t, "BEAT_TOKEN_FILE")

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() with an NBSP-only BEAT_TOKEN = %v, want accepted: textproto keeps a non-ASCII space, so the token is presentable and the gate must stay armed", err)
	}
	if len(token) != minTokenLength {
		t.Fatalf("fixture is %d bytes, want exactly %d: the case only clears the length floor at that size", len(token), minTokenLength)
	}
	if cfg.BeatToken != token {
		t.Errorf("BeatToken = %q, want %q preserved verbatim: trimming it would leave webapi's gate with a credential no sender presents", cfg.BeatToken, token)
	}
}

// TestLoadKeepsABeatTokenWithInteriorASCIIWhitespaceArmed pins the SCOPE of the
// padding refusal: it is an EDGE rule, and a space or tab INSIDE the credential
// is accepted and stored verbatim. asciiWhitespace's two mechanisms both act on
// the edges only -- the wire strips a TRAILING SP/HTAB run and a LEADING run is
// invisible in the value the operator reads -- while an interior SP or HTAB is a
// legal field-value byte that net/textproto delivers untouched, so an
// interior-whitespace token authenticates exactly as configured.
//
// Nothing else in the package pins the ACCEPTING side of that scope.
// checkBeatToken's refusals are pinned by value, FuzzCheckBeatToken asserts only
// on tokens it ACCEPTS (a rejection returns early, so an over-strict validator
// satisfies it vacuously), and every OTHER interior-whitespace fixture in the
// package is too short to be ACCEPTED: "alpha\tbeta" and "alpha  beta" in the
// beatTokenFitsHeader tables never reach checkBeatToken at all, and
// "alpha\tbeta" is also a committed FuzzCheckBeatToken seed, where it does reach
// it but the 16-byte floor refuses its 10 bytes before any acceptance assertion
// runs. So tightening the edge test to refuse SP/HTAB anywhere -- the
// shape a "harden the credential" edit reaches for -- leaves every existing test
// green while a passphrase-style BEAT_TOKEN stops the observer from starting at
// all, which for a dead-man switch is the one refusal nobody is watching for.
func TestLoadKeepsABeatTokenWithInteriorASCIIWhitespaceArmed(t *testing.T) {
	// Serial (t.Setenv forbids t.Parallel). A space AND a tab, both interior,
	// and 20 bytes so the value clears the minTokenLength floor on its own.
	const token = "unit test beat\ttoken"
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", token)
	unsetEnv(t, "BEAT_TOKEN_FILE")

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() with BEAT_TOKEN=%q = %v, want accepted: HTTP normalizes only the EDGES of a field value, so an interior space or tab reaches the verifier untouched and refusing it fails startup on a working credential", token, err)
	}
	if cfg.BeatToken != token {
		t.Errorf("BeatToken = %q, want %q verbatim: webapi compares against \"Bearer \"+token, so collapsing or stripping an interior byte arms the gate for a value no sender presents and one deadline later every beat posts a false MISSING notice", cfg.BeatToken, token)
	}
	// The transport is the oracle, not this package's own cutset: the accepted
	// token has to come back off the wire byte for byte, or accepting it was the
	// mistake rather than refusing it.
	srv := authEchoServer(t)
	echoed, doErr := echoAuthHeader(t, srv, token)
	if doErr != nil {
		t.Fatalf("the accepted token could not be sent at all: %v", doErr)
	}
	if want := "Bearer " + token; echoed != want {
		t.Errorf("the wire delivered %q, want %q: an accepted token the transport alters is as unpresentable as one it refuses", echoed, want)
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

// TestLoadWarnsWhenListenAddrAsksForAnEphemeralPort pins the port-0
// diagnostic, the one LISTEN_ADDR signal nothing else in this package
// asserts. Port 0 BINDS, so net.Listen never refuses it and startup reports
// itself healthy while the kernel hands out a fresh random port on every
// boot: no sender's POST /beat/{id} URL and no scrape target can name it, so
// every configured beat goes missing one deadline after start while the
// observer looks up. Both directions are pinned because both are silent - a
// dropped or inverted zero check loses the only signal that says so, and a
// warning on a usable address trains the operator to ignore the ones that
// matter.
func TestLoadWarnsWhenListenAddrAsksForAnEphemeralPort(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	tests := map[string]struct {
		addr     string
		wantWarn bool
	}{
		"port zero on every interface": {addr: ":0", wantWarn: true},
		"port zero on one interface":   {addr: "127.0.0.1:0", wantWarn: true},
		"an explicit port stays quiet": {addr: "127.0.0.1:9999", wantWarn: false},
		// A service NAME is resolved by net.Listen and is never zero, and a
		// value that is not a host:port at all is refused at bind time and
		// named by main's classifyBindError, so neither is this warning's job.
		"a service name stays quiet": {addr: "127.0.0.1:http", wantWarn: false},
		"a value net.Listen refuses": {addr: "9190", wantWarn: false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv("LISTEN_ADDR", tt.addr)

			rec := capture.Default(t)

			cfg, err := Load(maxNodeNameBytes)
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if cfg.ListenAddr != tt.addr {
				t.Errorf("ListenAddr = %q, want %q kept verbatim: the address is handed to net.Listen as configured, so a rewrite here binds a port nobody scrapes", cfg.ListenAddr, tt.addr)
			}
			// Counted per LEVEL, not merely present: capture.Default records
			// every level, so a Contains check stays green if the diagnostic
			// is demoted to Debug or Info — and a demoted line is invisible at
			// the WARN-and-above level a deployment runs at, which is the whole
			// signal. The exact count also closes the other direction: a
			// spurious second record at any level is a warning the operator
			// learns to ignore.
			wantCount := 0
			if tt.wantWarn {
				wantCount = 1
			}
			gotAny := rec.Count("asks for port 0")
			gotWarn := rec.CountLevel(slog.LevelWarn, "asks for port 0")
			if gotAny != wantCount || gotWarn != wantCount {
				t.Errorf("LISTEN_ADDR=%q: matching records = %d, WARN records = %d, want %d; the diagnostic is the only signal that the listener moves to a fresh random port on every boot, so it must be absent or emitted exactly once at WARN: %v",
					tt.addr, gotAny, gotWarn, wantCount, rec.Records())
			}
			if !tt.wantWarn {
				return
			}
			// The hint carries the way out. Without it the operator reads that
			// the port is random with nothing telling them what to set instead.
			if !rec.HasAttr("asks for port 0", "hint", "set an explicit port, e.g. "+defaultListenAddr) {
				t.Errorf("the port-0 warning does not carry the %q hint: %v", defaultListenAddr, rec.Records())
			}
		})
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

// TestLoadTrimsInvisibleConfigPadding pins the part of NODE_NAME's,
// LISTEN_ADDR's and DISCORD_WEBHOOK_URL's trim that strings.TrimSpace does NOT
// do. The padded tests above use ASCII spaces, so they stay green against
// either predicate and cannot tell them apart; every value here survives
// TrimSpace (a zero-width space, a soft hyphen and a BOM are Cf format runes,
// not Unicode White_Space), so reverting any of the three
// strings.TrimFunc(…, invisibleInURL) calls to TrimSpace fails exactly one case
// below. Without this test the invisible-padding behavior is unpinned in all
// three places: a padded LISTEN_ADDR goes back to
// crash-looping on a bind error whose cause cannot be seen in the log line, a
// padded NODE_NAME goes back to prefixing every notice with a character the
// operator cannot see, and a padded DISCORD_WEBHOOK_URL goes back to refusing
// startup through parseWebhookURL over a rune nobody can see in the value.
func TestLoadTrimsInvisibleConfigPadding(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, and the last subtest swaps the
	// process-global slog default via capture.Default.
	const (
		zeroWidthSpace = "\u200b"
		softHyphen     = "\u00ad"
		byteOrderMark  = "\ufeff"
	)

	t.Run("a NODE_NAME padded with invisible runes is trimmed", func(t *testing.T) {
		setValidLoadEnv(t)
		t.Setenv("NODE_NAME", zeroWidthSpace+"node-1"+byteOrderMark)

		cfg, err := Load(maxNodeNameBytes)
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.Node != "node-1" {
			t.Errorf("Node = %q, want \"node-1\": TrimSpace keeps these runes, so every Discord notice would read as this observer's name plus a character nobody can see", cfg.Node)
		}
	})

	t.Run("a LISTEN_ADDR padded with invisible runes is trimmed", func(t *testing.T) {
		setValidLoadEnv(t)
		t.Setenv("LISTEN_ADDR", zeroWidthSpace+"0.0.0.0:9999"+softHyphen)

		cfg, err := Load(maxNodeNameBytes)
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.ListenAddr != "0.0.0.0:9999" {
			t.Errorf("ListenAddr = %q, want the trimmed address: net.Listen resolves a value carrying these runes as a hostname lookup and fails, and the padding is invisible in the resulting crash-loop log line", cfg.ListenAddr)
		}
	})

	t.Run("a DISCORD_WEBHOOK_URL padded with invisible runes is trimmed", func(t *testing.T) {
		setValidLoadEnv(t)
		t.Setenv("DISCORD_WEBHOOK_URL", zeroWidthSpace+"https://discord.example/api/webhooks/1/abc"+byteOrderMark)

		cfg, err := Load(maxNodeNameBytes)
		if err != nil {
			t.Fatalf("Load() error: %v: TrimSpace keeps these runes, so parseWebhookURL refuses a URL pasted out of a rendered page and the observer never starts", err)
		}
		if cfg.WebhookURL != "https://discord.example/api/webhooks/1/abc" {
			t.Errorf("WebhookURL = %q, want the invisible padding trimmed: an untrimmed rune is percent-encoded on every POST, so Discord answers 404 forever and the switch can never ring", cfg.WebhookURL)
		}
	})

	t.Run("an entirely invisible LISTEN_ADDR falls back to the default", func(t *testing.T) {
		setValidLoadEnv(t)
		t.Setenv("LISTEN_ADDR", zeroWidthSpace+byteOrderMark)

		rec := capture.Default(t)

		cfg, err := Load(maxNodeNameBytes)
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.ListenAddr != defaultListenAddr {
			t.Errorf("ListenAddr = %q, want the default %q: a value with nothing visible in it reaches the documented blank path, not a bind failure", cfg.ListenAddr, defaultListenAddr)
		}
		if !rec.Contains("LISTEN_ADDR is set but blank") {
			t.Errorf("no blank-LISTEN_ADDR warning for a value that is entirely invisible: the operator set a value this process threw away and nothing says so: %v", rec.Messages())
		}
	})
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
// DISCORD_WEBHOOK_URL and BEAT_TOKEN are both reported by their SOURCE and
// neither by its value — and neither gets a presence attr, because both are
// required, so presence reports no state while giving a future edit somewhere to
// render the credential. Each source is state and each value is not: no
// channel's name can carry a byte of the credential, and the plain channel is
// the one worth naming because that credential is then also in `docker inspect`
// output. The
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
		WebhookURL:      secretHook,
		WebhookSource:   envx.SourceEnv,
		Node:            "node-a",
		ListenAddr:      ":9190",
		BeatToken:       secretToken,
		BeatTokenSource: envx.SourceFile,
		Beats:           []Beat{{ID: "api", Deadline: time.Hour}},
		LogLevel:        slog.LevelInfo,
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
	if got["webhook"] != string(envx.SourceEnv) {
		t.Errorf("webhook = %q, want %q: the attr reports the channel that supplied the credential, never the credential", got["webhook"], envx.SourceEnv)
	}
	if got["beat_token"] != string(envx.SourceFile) {
		t.Errorf("beat_token = %q, want %q: the attr reports the channel that supplied the token, never the token", got["beat_token"], envx.SourceFile)
	}
	if _, reported := got["beat_auth"]; reported {
		t.Errorf("LogValue renders beat_auth = %q; the token is required, so a presence attr can only ever say \"required\" and reports no state — its CHANNEL is the state, and beat_token publishes that", got["beat_auth"])
	}
}

// TestConfigLogValueReportsTheWebhookCredentialSource pins the attr's whole
// value domain against Load, which is the only producer of it. The point of the
// attr is that it reports STATE: the file channel keeps the credential out of
// the process environment and out of `docker inspect`, the plain variable does
// not, and warnPlainVarIgnored only speaks up when BOTH variables are set — so
// this line is the only signal a plain-only deployment ever gets. A rendering
// keyed off presence instead of source passes every assertion above and fails
// here, because presence is identical in both cases.
func TestConfigLogValueReportsTheWebhookCredentialSource(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv.
	const url = "https://discord.example/api/webhooks/1/source-probe"

	for name, tc := range map[string]struct {
		file bool
		want envx.SecretSource
	}{
		"the plain variable leaves the credential in the environment": {want: envx.SourceEnv},
		"the _FILE companion keeps it out of the environment":         {file: true, want: envx.SourceFile},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("BEATS", "api:1h")
			t.Setenv("BEAT_TOKEN", "source-probe-beat-token")
			if tc.file {
				path := filepath.Join(t.TempDir(), "webhook")
				if err := os.WriteFile(path, []byte(url+"\n"), 0o600); err != nil {
					t.Fatalf("writing the webhook secret file: %v", err)
				}
				t.Setenv("DISCORD_WEBHOOK_URL_FILE", path)
			} else {
				t.Setenv("DISCORD_WEBHOOK_URL", url)
			}

			cfg, err := Load(maxNodeNameBytes)
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if cfg.WebhookURL != url {
				t.Fatalf("WebhookURL = %q, want %q", cfg.WebhookURL, url)
			}
			if cfg.WebhookSource != tc.want {
				t.Errorf("WebhookSource = %q, want %q: Load must carry the channel through, or LogValue has nothing to report", cfg.WebhookSource, tc.want)
			}
			var rendered string
			for _, attr := range cfg.LogValue().Group() {
				if attr.Key == "webhook" {
					rendered = attr.Value.String()
				}
			}
			if rendered != string(tc.want) {
				t.Errorf("startup line reports webhook = %q, want %q", rendered, tc.want)
			}
			if strings.Contains(rendered, url) {
				t.Errorf("startup line reports webhook = %q, which carries the credential itself", rendered)
			}
		})
	}
}

// TestConfigLogValueReportsTheBeatTokenSource mirrors the webhook's
// source-flow test for the /beat/{id} credential, against Load, which is the
// only producer of the field. The token carries the same exposure the webhook
// does — envx.SourceEnv means it is also in the process environment and in
// `docker inspect` output, the _FILE channel keeps it out — and
// warnPlainVarIgnored speaks up only when BOTH variables are set, so for a
// plain-only deployment this attr is the whole signal. Publishing the webhook's
// channel and not the token's would tell an operator half of where their
// secrets live, which is why the two are pinned the same way.
func TestConfigLogValueReportsTheBeatTokenSource(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv.
	const token = "source-probe-beat-token"

	for name, tc := range map[string]struct {
		file bool
		want envx.SecretSource
	}{
		"the plain variable leaves the token in the environment": {want: envx.SourceEnv},
		"the _FILE companion keeps it out of the environment":    {file: true, want: envx.SourceFile},
	} {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			if tc.file {
				path := filepath.Join(t.TempDir(), "beat-token")
				if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
					t.Fatalf("writing the beat token secret file: %v", err)
				}
				unsetEnv(t, "BEAT_TOKEN")
				t.Setenv("BEAT_TOKEN_FILE", path)
			} else {
				t.Setenv("BEAT_TOKEN", token)
				unsetEnv(t, "BEAT_TOKEN_FILE")
			}

			cfg, err := Load(maxNodeNameBytes)
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if cfg.BeatToken != token {
				t.Fatalf("BeatToken = %q, want %q", cfg.BeatToken, token)
			}
			if cfg.BeatTokenSource != tc.want {
				t.Errorf("BeatTokenSource = %q, want %q: Load must carry the channel through, or LogValue has nothing to report", cfg.BeatTokenSource, tc.want)
			}
			var rendered string
			for _, attr := range cfg.LogValue().Group() {
				if attr.Key == "beat_token" {
					rendered = attr.Value.String()
				}
			}
			if rendered != string(tc.want) {
				t.Errorf("startup line reports beat_token = %q, want %q", rendered, tc.want)
			}
			if strings.Contains(rendered, token) {
				t.Errorf("startup line reports beat_token = %q, which carries the credential itself", rendered)
			}
		})
	}
}

// TestConfigLogValueReportsEveryNonSecretField pins the accuracy half of
// LogValue's contract; TestConfigLogValueNeverRendersASecret
// pins the hygiene half. LogValue exists so a call site can hand a whole
// Config to slog, and those seven attrs are then the entire rendering of a
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
		AllowedHosts:  policy,
		WebhookURL:    "https://discord.example/hook",
		WebhookSource: envx.SourceFile,
		Node:          "observer-borgcube",
		ListenAddr:    "127.0.0.1:19190",
		BeatToken:     "unit-test-beat-token",
		// The OTHER channel than the webhook's, deliberately: the two source
		// attrs are the only pair in this fixture that share a value domain, so
		// an attr wired to the wrong source field would otherwise render exactly
		// what the right one does and pass.
		BeatTokenSource: envx.SourceEnv,
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
		// The rendered WORD, not string(envx.SourceFile): restating the constant
		// would assert nothing about what an operator reads, and this attr's
		// whole job is to name a channel legibly in the startup line.
		"webhook": "file",
		// The token's channel, reported symmetrically with the webhook's and from
		// its own field: both credentials are exposed by the plain variable in the
		// same way, so publishing one channel and not the other tells an operator
		// half of where their secrets live.
		"beat_token":    "env",
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

// TestConfigLogValueReportsTheHostAllowlistState pins the two inactive
// allowed_hosts states. A nil policy covers a zero Config built without Load;
// a blank policy is the runtime shape of a present-but-blank ALLOWED_HOSTS.
// Both must render as "any" rather than panic or claim the gate is active.
func TestConfigLogValueReportsTheHostAllowlistState(t *testing.T) {
	t.Parallel()

	blank, invalid := webhttp.ParseHostList([]string{""})
	if len(invalid) > 0 {
		t.Fatalf("ParseHostList rejected blank input: %v", invalid)
	}

	for name, policy := range map[string]*webhttp.HostPolicy{
		"unset leaves every Host accepted":   nil,
		"blank value never engages the gate": blank,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{AllowedHosts: policy, LogLevel: slog.LevelInfo}
			got := ""
			for _, attr := range cfg.LogValue().Group() {
				if attr.Key == "allowed_hosts" {
					got = attr.Value.String()
				}
			}
			if got != "any" {
				t.Errorf("allowed_hosts = %q, want %q: an inactive policy accepts every Host", got, "any")
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

// TestLoadRefusesAHeaderIllegalTokenBeforeTheLengthFloor pins the ORDER of
// checkBeatToken's header-legality and minimum-length refusals. A plain
// BEAT_TOKEN of Unicode spaces around a control byte passes the ASCII-edge
// refusal and is BOTH header-illegal and under the 16-byte floor, so it is the
// one fixture whose diagnosis reveals which check ran first: the operator has
// to be told the token carries a byte HTTP forbids, not that it is too short,
// because lengthening it would not make it presentable.
func TestLoadRefusesAHeaderIllegalTokenBeforeTheLengthFloor(t *testing.T) {
	// Serial (t.Setenv forbids t.Parallel).
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKEN", "\u00a0\n\u00a0")

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with a BEAT_TOKEN carrying an interior newline = nil, want error: no sender can present a control byte in a header value")
	}
	if !strings.Contains(err.Error(), "control character") {
		t.Errorf("error = %q, want the control-character refusal", err)
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
