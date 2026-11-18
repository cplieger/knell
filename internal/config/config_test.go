package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/go/envx"
	"github.com/cplieger/go/httpserver/v2"
	"github.com/cplieger/go/slogx/capture"
	"github.com/cplieger/knell/internal/notify"
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
		"BEAT_TOKENS",
		"BEAT_TOKENS_FILE",
		"DISCORD_WEBHOOK_URL",
		"DISCORD_WEBHOOK_URL_FILE",
		"LISTEN_ADDR",
		"LOG_LEVEL",
		"MISSING_REPEAT",
		"NODE_NAME",
		"OPS_LISTEN_ADDR",
		"TRUSTED_PROXIES",
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
	// BEAT_TOKENS is required: the bearer gate is /beat/{id}'s only gate, so Load
	// refuses to start without it and every variant test needs one.
	t.Setenv("BEAT_TOKENS", "api:"+validBeatToken)
}

// unsetEnv removes key for the duration of the test. t.Setenv registers the
// restore of the original value, so the following os.Unsetenv leaves the
// variable absent inside the test and restored afterwards. A plain
// t.Setenv(key, "") would leave it present-but-empty, which is not equivalent
// for the keys this helper serves: a `_FILE` key rejects a broken mount with its
// own message, and BEAT_TOKENS distinguishes "you set it to nothing" from "you
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
// against what checkBeatToken claims about it.
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
// with. One definition of "what the transport carries" for every test that uses
// the wire itself as its oracle, so they cannot come to measure different
// things.
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
	if cfg.OpsListenAddr != ":9191" {
		t.Errorf("OpsListenAddr default = %q, want :9191", cfg.OpsListenAddr)
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

func TestLoadReadsMissingRepeat(t *testing.T) {
	accepted := map[string]time.Duration{
		"":       0,
		"0":      0,
		"0s":     0,
		"6h":     6 * time.Hour,
		" 6h ":   6 * time.Hour,
		"10m":    10 * time.Minute,
		"90m30s": 90*time.Minute + 30*time.Second,
	}
	for raw, want := range accepted {
		t.Run("accepts "+raw, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv("MISSING_REPEAT", raw)
			cfg, err := Load(maxNodeNameBytes)
			if err != nil {
				t.Fatalf("Load() with MISSING_REPEAT=%q error: %v", raw, err)
			}
			if cfg.MissingRepeat != want {
				t.Errorf("MissingRepeat = %s, want %s", cfg.MissingRepeat, want)
			}
		})
	}
	t.Run("unset sends one missing notice per outage", func(t *testing.T) {
		setValidLoadEnv(t)
		unsetEnv(t, "MISSING_REPEAT")
		cfg, err := Load(maxNodeNameBytes)
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.MissingRepeat != 0 {
			t.Errorf("MissingRepeat = %s, want 0", cfg.MissingRepeat)
		}
	})

	refused := map[string]string{
		"6":    "not a Go duration",
		"6d":   "not a Go duration",
		"soon": "not a Go duration",
		"-1h":  "below the minimum of 10m0s",
		"9m":   "below the minimum of 10m0s",
	}
	for raw, wantMsg := range refused {
		t.Run("refuses "+raw, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv("MISSING_REPEAT", raw)
			_, err := Load(maxNodeNameBytes)
			if err == nil {
				t.Fatalf("Load() with MISSING_REPEAT=%q = nil, want a startup refusal", raw)
			}
			if !strings.Contains(err.Error(), "MISSING_REPEAT") || !strings.Contains(err.Error(), wantMsg) {
				t.Errorf("error = %q, want it to name MISSING_REPEAT and say %q", err, wantMsg)
			}
		})
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
	// The mirror of the file-borne case below: the channel clause fires only on
	// envx.SecretSourceFile, so a plain-variable refusal must not send the operator to
	// a mount they never configured.
	if strings.Contains(err.Error(), "came from DISCORD_WEBHOOK_URL_FILE") {
		t.Errorf("error = %q blames DISCORD_WEBHOOK_URL_FILE for a value the plain variable supplied", err)
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
	// The CHANNEL, not just the variable: DISCORD_WEBHOOK_URL is required, so a
	// _FILE pointing at the wrong file crash-loops the observer, and a refusal
	// naming only DISCORD_WEBHOOK_URL sends the operator to a variable they set
	// to "" on purpose. Only fileSourcedValueError can supply this clause -- the
	// scheme refusal itself never mentions the _FILE variable.
	if !strings.Contains(err.Error(), "came from DISCORD_WEBHOOK_URL_FILE") {
		t.Errorf("error = %q, want it to name the channel the refused value arrived through", err)
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
			unsetEnv(t, "BEAT_TOKENS")
			unsetEnv(t, "BEAT_TOKENS_FILE")
		},
		"present but empty": func(t *testing.T) {
			t.Setenv("BEAT_TOKENS", "")
			unsetEnv(t, "BEAT_TOKENS_FILE")
		},
	}
	for name, setup := range tests {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			setup(t)

			_, err := Load(maxNodeNameBytes)
			if err == nil {
				t.Fatalf("Load() with %s BEAT_TOKENS succeeded, want a startup refusal: an ungated /beat/{id} lets anything that reaches the port keep the switch armed", name)
			}
			// The message has to name the variable and what to do next: this is
			// the refusal a first deployment meets, and the container
			// crash-loops on it.
			for _, want := range []string{"BEAT_TOKENS", "16"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Load() error = %q, want it to mention %q so the operator knows what to set", err, want)
				}
			}
		})
	}
}

// TestLoadRefusesAShortBeatTokenWithoutLeakingIt pins the length floor as a
// startup refusal: the token is the endpoint's only gate and the throttle only
// slows a guessing run, so a guessable token is refused where the operator is
// watching. The message names the minimum and nothing about the value, since
// the startup log reaches a wider audience than the token's file.
func TestLoadRefusesAShortBeatTokenWithoutLeakingIt(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKENS", "api:"+"shorty")
	unsetEnv(t, "BEAT_TOKENS_FILE")

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with a 6-byte BEAT_TOKENS succeeded, want a startup refusal: the token is the only gate, so a guessable one is a bypassable one")
	}
	if !strings.Contains(err.Error(), strconv.Itoa(minTokenLength)) {
		t.Errorf("refusal = %q, want it to name the %d-byte minimum: it is the only actionable number", err, minTokenLength)
	}
	if strings.Contains(err.Error(), "shorty") {
		t.Errorf("refusal leaks the token value: %q", err)
	}
	// Every number in the message must be the minimum: the token's own length is
	// an attribute of a live credential, and this text ships to the log store.
	// "1" is the line number, a position in the file and no attribute of the
	// token.
	for _, run := range digitRuns(err.Error()) {
		if run != strconv.Itoa(minTokenLength) && run != "1" {
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

// TestBeatTokenLengthCeiling pins both edges of the token maximum, which guards
// the header budget rather than credential strength: maxTokenLength is the
// token's share of MaxRequestHeaderBytes and leaves headerOverheadAllowance for
// the request line, Host and "Bearer ". A maximum too low stops working
// deployments; one too high lets a BEAT_TOKENS_FILE naming the wrong file eat
// the reserve until every ping is answered 431.
func TestBeatTokenLengthCeiling(t *testing.T) {
	atMax := strings.Repeat("a", maxTokenLength)

	t.Run("a token at the maximum starts", func(t *testing.T) {
		setValidLoadEnv(t)
		t.Setenv("BEAT_TOKENS", "api:"+atMax)
		unsetEnv(t, "BEAT_TOKENS_FILE")

		cfg, err := Load(maxNodeNameBytes)
		if err != nil {
			t.Fatalf("Load() with a %d-byte BEAT_TOKENS error = %v, want nil: the maximum is a value knell accepts, and the header budget reserves headerOverheadAllowance bytes on top of it", maxTokenLength, err)
		}
		if !hasToken(cfg, atMax) {
			t.Error("BeatToken differs from the configured value: the credential is verified verbatim, so any rewrite 401s every ping")
		}
	})

	t.Run("the header cap carries a token at the maximum", func(t *testing.T) {
		// The reserve asserted from the wire rather than restated from the constants:
		// the Authorization line of a maximum-length token plus a generous ceiling for
		// the rest of a ping's header block (the request line, Host, Content-Length and
		// what a proxy appends). A cap short of that admits the token at startup and
		// then answers 431 to every ping presenting it.
		const restOfTheHeaderBlock = 1 << 10
		authorization := len("Authorization: Bearer " + atMax + "\r\n")

		if MaxRequestHeaderBytes < authorization+restOfTheHeaderBlock {
			t.Errorf("MaxRequestHeaderBytes = %d, want at least %d: a %d-byte Authorization line plus %d bytes for the request line, Host and anything a proxy adds, because a cap under that 431s every ping carrying a token Load just accepted",
				MaxRequestHeaderBytes, authorization+restOfTheHeaderBlock, authorization, restOfTheHeaderBlock)
		}
	})

	t.Run("one byte past the maximum refuses startup", func(t *testing.T) {
		setValidLoadEnv(t)
		t.Setenv("BEAT_TOKENS", "api:"+atMax+"a")
		unsetEnv(t, "BEAT_TOKENS_FILE")

		_, err := Load(maxNodeNameBytes)
		if err == nil {
			t.Fatalf("Load() with a %d-byte BEAT_TOKENS = nil, want a startup refusal: the maximum is the token's share of the %d-byte header block knell reads, so anything past it eats the %d-byte reserve held for the request line, Host and the \"Bearer \" prefix", maxTokenLength+1, MaxRequestHeaderBytes, headerOverheadAllowance)
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
	if err := os.WriteFile(tokenFile, []byte("api:file-borne-beat-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKENS", "api:"+"stale-env-beat-token")
	t.Setenv("BEAT_TOKENS_FILE", tokenFile)

	rec := capture.Default(t)

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if !hasToken(cfg, "file-borne-beat-token") {
		t.Errorf("BeatToken = %q, want the file-borne token: BEAT_TOKENS_FILE wins over BEAT_TOKENS, so a rotated secret file must not be shadowed by a stale plain variable that would 401 every sender", tokenSummary(cfg))
	}
	if !rec.Contains("the plain variable is ignored") || !rec.HasAttr("", "variable", "BEAT_TOKENS") {
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

// TestLoadNeverAdvisesUnsettingThePlainVarWrongly pins both of
// warnPlainVarIgnored's guards and its position after validation: the advisory
// means "a _FILE channel supplied this secret, so the plain variable you also
// set was ignored", so it must stay silent whenever that is not what happened —
// including on every startup that then fails, where the advice would tell the
// operator to delete the only credential left in the environment for a
// configuration that never ran.
func TestLoadNeverAdvisesUnsettingThePlainVarWrongly(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and t.Setenv forbids parallel tests anyway.
	for name, tc := range map[string]struct {
		setup   func(t *testing.T)
		wantErr bool
		check   func(t *testing.T, cfg Config)
	}{
		// envx reports SecretSourceFile together with its error on a failed file read,
		// so a call made before the loader's error gate warns "the file wins and
		// the plain variable is ignored" on a startup that is aborting because the
		// file supplied nothing. Both loaders are pinned because they are
		// structurally identical uses of the same helper, and the fatal-path
		// warning is invisible to every other test in this file (the two
		// FileWinsOverPlainVar tests supply a readable file, and the
		// RejectsUnreadable* tests capture no log).
		"webhook file unreadable": {
			setup: func(t *testing.T) {
				t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/fallback")
				t.Setenv("DISCORD_WEBHOOK_URL_FILE", filepath.Join(t.TempDir(), "missing-webhook"))
			},
			wantErr: true,
		},
		"beat token file unreadable": {
			setup: func(t *testing.T) {
				t.Setenv("BEAT_TOKENS", "api:"+"fallback-beat-token")
				t.Setenv("BEAT_TOKENS_FILE", filepath.Join(t.TempDir(), "missing-beat-token"))
			},
			wantErr: true,
		},
		// The remaining fatal path after the file read succeeds: a file-sourced
		// token with an interior control byte while plain BEAT_TOKENS is also
		// set. The file wins, so no advisory may tell the operator to delete the
		// plain variable before startup fails on the file token. (An edge-padded
		// file token is pinned by TestLoadRejectsAPaddedFileBorneBeatToken; a
		// blank file errors earlier, inside envx.)
		"two-line token file": {
			setup: func(t *testing.T) {
				tokenFile := filepath.Join(t.TempDir(), "beat-token")
				if err := os.WriteFile(tokenFile, []byte("alpha\nbeta\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("BEAT_TOKENS", "api:"+"fallback-beat-token")
				t.Setenv("BEAT_TOKENS_FILE", tokenFile)
			},
			wantErr: true,
		},
		// The webhook half of that post-VALIDATION pin. Moving warnPlainVarIgnored
		// back above loadWebhook's shape check reintroduces exactly this with every
		// other test green: TestLoadRejectsPlainHTTPWebhookFromFile sets the plain
		// variable blank, so the advisory stays silent there either way.
		"plain-http webhook file": {
			setup: func(t *testing.T) {
				hookFile := filepath.Join(t.TempDir(), "webhook-url")
				if err := os.WriteFile(hookFile, []byte("http://discord.example/file-borne-hook\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/fallback")
				t.Setenv("DISCORD_WEBHOOK_URL_FILE", hookFile)
			},
			wantErr: true,
		},
		// The CROSS-loader path, which no row above reaches: the webhook's own
		// channels are both fine, so its advisory fires on a startup a LATER
		// refusal aborts. Only a position past every refusal keeps it silent.
		"webhook file wins, BEAT_TOKENS missing": {
			setup: func(t *testing.T) {
				hookFile := filepath.Join(t.TempDir(), "webhook-url")
				if err := os.WriteFile(hookFile, []byte("https://discord.example/file-borne-hook\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/fallback")
				t.Setenv("DISCORD_WEBHOOK_URL_FILE", hookFile)
				unsetEnv(t, "BEAT_TOKENS")
				unsetEnv(t, "BEAT_TOKENS_FILE")
			},
			wantErr: true,
		},
		// The _FILE-only deployment: no plain variable is being shadowed, so there
		// is nothing to report. Without this row, dropping warnPlainVarIgnored's
		// plain-variable guard tells every _FILE-only operator to unset a variable
		// they never set, on the one startup line that is supposed to mean a
		// rotated secret is being ignored.
		"_FILE channels only": {
			setup: func(t *testing.T) {
				dir := t.TempDir()
				tokenFile := filepath.Join(dir, "beat-token")
				if err := os.WriteFile(tokenFile, []byte("api:file-borne-beat-token\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				hookFile := filepath.Join(dir, "webhook-url")
				if err := os.WriteFile(hookFile, []byte("https://discord.example/file-borne-hook\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				unsetEnv(t, "BEAT_TOKENS")
				unsetEnv(t, "DISCORD_WEBHOOK_URL")
				t.Setenv("BEAT_TOKENS_FILE", tokenFile)
				t.Setenv("DISCORD_WEBHOOK_URL_FILE", hookFile)
			},
			check: func(t *testing.T, cfg Config) {
				if !hasToken(cfg, "file-borne-beat-token") || cfg.WebhookURL != "https://discord.example/file-borne-hook" {
					t.Fatalf("BeatToken = %q, WebhookURL = %q, want both from their _FILE channels", tokenSummary(cfg), cfg.WebhookURL)
				}
			},
		},
		// The ordinary plain-variable-only deployment, which no other row reaches:
		// src is not envx.SecretSourceFile, so dropping the src check tells that operator,
		// on every startup, to unset the variable that is actually supplying the
		// credential — advice that leaves /beat/{id} ungated and the next start
		// failing on a missing credential entirely.
		"plain vars only": {
			setup: func(t *testing.T) {
				t.Setenv("BEAT_TOKENS", "api:"+"plain-only-beat-token")
				unsetEnv(t, "BEAT_TOKENS_FILE")
				unsetEnv(t, "DISCORD_WEBHOOK_URL_FILE")
			},
			check: func(t *testing.T, cfg Config) {
				if !hasToken(cfg, "plain-only-beat-token") || cfg.WebhookURL != "https://discord.example/hook" {
					t.Fatalf("BeatToken = %q, WebhookURL = %q, want both read from their plain variables", tokenSummary(cfg), cfg.WebhookURL)
				}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			tc.setup(t)

			rec := capture.Default(t)

			cfg, err := Load(maxNodeNameBytes)
			if tc.wantErr != (err != nil) {
				t.Fatalf("Load() error = %v, want an error: %v", err, tc.wantErr)
			}
			if rec.Contains("the plain variable is ignored") {
				t.Errorf("Load() advised unsetting the plain variable: %v; the advice is only true when a _FILE channel supplied the credential and the configuration then ran, so here it tells the operator to delete a credential still in use", rec.Messages())
			}
			if tc.check != nil {
				tc.check(t, cfg)
			}
		})
	}
}

func TestLoadRejectsUnreadableBeatTokenFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-beat-token")
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKENS", "api:"+"env-fallback-token")
	t.Setenv("BEAT_TOKENS_FILE", missing)

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with unreadable BEAT_TOKENS_FILE = nil, want error (the secret file must not silently fall back to the environment value, which would arm the gate with the wrong token)")
	}
	if !strings.Contains(err.Error(), "could not be read because the open call failed") {
		t.Errorf("error = %q, want the read-failure diagnosis naming the failed operation: envx embeds the path in its os.PathError and this sanitizer replaces it, so the operation plus the OS reason is all the operator has left to tell a missing mount from a permission problem", err)
	}
	if !strings.Contains(err.Error(), "BEAT_TOKENS") {
		t.Errorf("error = %q, want BEAT_TOKENS context", err)
	}
	if strings.Contains(err.Error(), missing) {
		t.Errorf("error = %q embeds the BEAT_TOKENS_FILE value; os.PathError carries it and the sanitizer must drop it, because that value is the bearer token itself whenever the operator pasted the credential into the file variable", err)
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
	t.Setenv("BEAT_TOKENS", "api:"+"env-fallback-token")
	t.Setenv("BEAT_TOKENS_FILE", tokenFile)

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with an empty BEAT_TOKENS_FILE = nil, want error (an empty secret file is a broken mount, and the token it should have carried is required)")
	}
	if !strings.Contains(err.Error(), "blank secret file") {
		t.Errorf("error = %q, want the blank-secret-file diagnosis: secretFileError names each envx failure class so the operator is told WHICH failure happened, and the generic fallback sends someone whose mount produced an empty file to check the path instead of the content", err)
	}
	if !strings.Contains(err.Error(), "BEAT_TOKENS") {
		t.Errorf("error = %q, want BEAT_TOKENS context", err)
	}
	if strings.Contains(err.Error(), tokenFile) {
		t.Errorf("error = %q embeds the BEAT_TOKENS_FILE value; envx names the path in its own blank-file error, and this sanitizer exists to drop it because that value is the credential itself whenever the operator pasted the secret into the file variable", err)
	}
	if strings.Contains(err.Error(), "env-fallback-token") {
		t.Errorf("error leaks the fallback token value: %v", err)
	}
}

// TestAllowedHostsGate pins the env-to-policy mapping of ALLOWED_HOSTS; webapi's
// tests cover request time. Unset stays inactive, since an accidental deny-all
// 403s every beat in a default deployment; blank entries do not engage the
// gate; and an entry knell cannot use refuses startup, since dropping it would
// 403 the pings sent under that name and page them as missing beats.
func TestAllowedHostsGate(t *testing.T) {
	tests := map[string]struct {
		raw        string
		set        bool
		wantActive bool
		wantSize   int
		wantErr    string
	}{
		"unset accepts every host":        {wantActive: false},
		"one hostname engages the gate":   {set: true, raw: "knell.internal", wantActive: true, wantSize: 1},
		"blank entries are skipped":       {set: true, raw: "knell.internal, ,10.0.0.5", wantActive: true, wantSize: 2},
		"present but blank never engages": {set: true, raw: "", wantActive: false},
		"one unusable entry refuses":      {set: true, raw: "knell.internal,http://x/y", wantErr: "http://x/y"},
		// Formerly pinned as active-size-zero, warned about and STARTED: the
		// oracle is unchanged (this configuration rejects every non-loopback
		// sender, so no beat can be recorded), and the verdict is now the
		// refusal that makes it impossible to reach production.
		"all entries unusable refuses": {set: true, raw: ":9190", wantErr: ":9190"},
		// A PADDED blank is the same compose accident as the empty one above
		// (ALLOWED_HOSTS="${HOSTS} " with HOSTS undefined), and which of the two
		// outcomes it lands on is decided inside httpserver, not here: if
		// ParseHostList ever stopped trimming an entry, "   " would become a
		// non-blank unusable entry and startup would refuse instead of accepting
		// every Host. Pinned so a httpserver bump has to fail here rather than in
		// production.
		"padded blank never engages": {set: true, raw: "   ", wantActive: false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			// Serial (no t.Parallel): t.Setenv forbids parallel tests.
			if tt.set {
				t.Setenv("ALLOWED_HOSTS", tt.raw)
			} else {
				unsetEnv(t, "ALLOWED_HOSTS")
			}

			// nil options: this table asserts the env-to-policy mapping only
			// (active, size, the startup refusal), none of
			// which the serving-side options affect — webapi's tests cover the
			// shipped envelope and the loopback exemption through
			// webapi.HostPolicyOptions.
			policy, _, err := allowedHosts(nil)
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
		})
	}
}

// TestLoadTrustedProxiesDegradesRestrictively pins the TRUSTED_PROXIES contract
// through Load, so a Load that stops assigning the field fails too. Unset, blank
// and all-entries-invalid each yield an EMPTY set, so httpserver.WithClientIP gets
// no proxies and client_ip stays on the socket peer. A malformed entry is warned
// about and DROPPED rather than failing startup, legal only because dropping one
// NARROWS whose X-Forwarded-For is believed; a fallback that trusted everything
// would make client_ip spoofable. TestAllowedHostsGate is the contrast: a
// dropped entry there would refuse a wanted caller, so that list refuses startup.
func TestLoadTrustedProxiesDegradesRestrictively(t *testing.T) {
	tests := map[string]struct {
		raw         string
		set         bool
		wantCount   int
		wantDropped string
	}{
		"unset honors no forwarded header":    {},
		"blank honors no forwarded header":    {set: true},
		"padded blank honors none":            {set: true, raw: "   "},
		"a CIDR and a bare IP both parse":     {set: true, raw: "10.0.0.0/24,192.168.1.5", wantCount: 2},
		"spaces around entries are tolerated": {set: true, raw: " 10.0.0.0/24 , 192.168.1.5 ", wantCount: 2},
		"one malformed entry is dropped":      {set: true, raw: "10.0.0.0/24,not-an-ip", wantCount: 1, wantDropped: "not-an-ip"},
		"all malformed leaves the set empty":  {set: true, raw: "not-an-ip", wantDropped: "not-an-ip"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			// Serial (no t.Parallel): capture.Default swaps the process-global
			// slog default, and t.Setenv forbids parallel tests anyway.
			setValidLoadEnv(t)
			if tt.set {
				t.Setenv("TRUSTED_PROXIES", tt.raw)
			} else {
				unsetEnv(t, "TRUSTED_PROXIES")
			}

			rec := capture.Default(t)

			cfg, err := Load(maxNodeNameBytes)
			if err != nil {
				t.Fatalf("Load() with TRUSTED_PROXIES=%q error = %v, want nil: a malformed restriction list degrades, it does not refuse startup", tt.raw, err)
			}
			if len(cfg.TrustedProxies) != tt.wantCount {
				t.Errorf("cfg.TrustedProxies holds %d ranges %v, want %d: webapi spreads this field into httpserver.WithClientIP, so a range lost here is a proxy whose X-Forwarded-For stops being believed, and one gained is a caller that can choose the client_ip knell attributes its 401s to", len(cfg.TrustedProxies), cfg.TrustedProxies, tt.wantCount)
			}
			if tt.wantDropped == "" {
				if rec.Contains("TRUSTED_PROXIES") {
					t.Errorf("log output %v warns about TRUSTED_PROXIES for a usable configuration; a warning on the documented default trains operators to ignore the ones that matter", rec.Messages())
				}
				return
			}
			if got := rec.CountLevel(slog.LevelWarn, "TRUSTED_PROXIES entries ignored"); got != 1 {
				t.Errorf("the dropped-entry line was logged at WARN %d times, want 1 (records %v); at DEBUG it is out of the log at knell's default LOG_LEVEL, so the operator never learns which proxy stopped being trusted", got, rec.Records())
			}
			if !rec.AttrContains("TRUSTED_PROXIES", "ignored", tt.wantDropped) {
				t.Errorf("warning %v never names the dropped entry %q; the startup line reports only a COUNT, so this warning is the only place an operator learns WHICH proxy is not trusted", rec.Records(), tt.wantDropped)
			}
		})
	}
}

// TestLoadThreadsTheHostPolicyOptionsThrough pins that Load forwards its host
// policy options (webapi's HostPolicyOptions: the loopback exemption and the
// 403 envelope naming ALLOWED_HOSTS). Every other Load call here passes none,
// and webapi's tests build the policy directly, so a Load that forwarded nothing
// would pass the suite while the allowlist 403ed the in-container `knell health`
// probe.
func TestLoadThreadsTheHostPolicyOptionsThrough(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv.
	setValidLoadEnv(t)
	t.Setenv("ALLOWED_HOSTS", "knell.internal")

	// A genuinely local caller: loopback socket peer AND loopback Host, the only
	// shape WithLoopbackExempt admits. It is NOT in the allowlist, so it is
	// admitted by the option or by nothing.
	loopback := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9191/healthz", nil)
		r.RemoteAddr = "127.0.0.1:54321"
		return r
	}

	exempt, err := Load(maxNodeNameBytes, httpserver.WithLoopbackExempt(true))
	if err != nil {
		t.Fatalf("Load() with the loopback exemption: %v", err)
	}
	if !exempt.AllowedHosts.Allows(loopback()) {
		t.Error("Load did not forward its host-policy options: a loopback peer under a loopback Host is refused, so the in-container knell health probe of http://127.0.0.1:9191/healthz 403s under any allowlist and the container turns unhealthy")
	}

	bare, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() with no options: %v", err)
	}
	if bare.AllowedHosts.Allows(loopback()) {
		t.Error("the same request is admitted with NO options passed, so the assertion above pins httpserver's default rather than the parameter Load forwards; pick an option whose effect the default does not already give")
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
	t.Setenv("BEAT_TOKENS", "api:"+"env-fallback-token")
	t.Setenv("BEAT_TOKENS_FILE", "")

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with a present-but-empty BEAT_TOKENS_FILE = nil, want error; envx cannot tell it from unset, so falling back would serve an unauthenticated /beat/{id} the operator meant to gate")
	}
	if !strings.Contains(err.Error(), "BEAT_TOKENS_FILE") {
		t.Errorf("error = %q, want BEAT_TOKENS_FILE context", err)
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

// TestLoadRejectsAWebhookWithNothingVisible pins the third webhook-absence refusal.
// envx judges a secret file blank with strings.TrimSpace, which does not count U+200B or
// U+FEFF as space, so such a file arrives as a VALUE and only knell's own trim empties
// it; without this arm it reaches parseWebhookURL and a broken secret pipeline is
// answered "scheme must be https", the wording that belongs to a schemeless URL.
func TestLoadRejectsAWebhookWithNothingVisible(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		fromFile bool
	}{
		{name: "plain variable holding only spaces", value: "  "},
		{name: "secret file holding only a zero-width space", value: "\u200b\n", fromFile: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setValidLoadEnv(t)
			unsetEnv(t, "DISCORD_WEBHOOK_URL_FILE")
			if tt.fromFile {
				hookFile := filepath.Join(t.TempDir(), "webhook-url")
				if err := os.WriteFile(hookFile, []byte(tt.value), 0o600); err != nil {
					t.Fatal(err)
				}
				unsetEnv(t, "DISCORD_WEBHOOK_URL")
				t.Setenv("DISCORD_WEBHOOK_URL_FILE", hookFile)
			} else {
				t.Setenv("DISCORD_WEBHOOK_URL", tt.value)
			}

			_, err := Load(maxNodeNameBytes)
			if err == nil {
				t.Fatal("Load() with a webhook holding nothing visible = nil, want a startup refusal: every notice would POST to a URL with no host at all and the bell never rings")
			}
			if !errors.Is(err, errWebhookSetButEmpty) {
				t.Errorf("error = %v, want errWebhookSetButEmpty: the variable WAS supplied, so the operator's next move is the secret pipeline that delivered nothing", err)
			}
			if tt.fromFile && !strings.Contains(err.Error(), "DISCORD_WEBHOOK_URL_FILE") {
				t.Errorf("error = %q does not name the channel that supplied the value, so the crash loop points at a variable the operator may never have set", err)
			}
		})
	}
}

func TestLoadTrimsPaddedPlainWebhook(t *testing.T) {
	setValidLoadEnv(t)
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/hook ")
	t.Setenv("BEAT_TOKENS", "api:"+"unit-test-beat-token")
	unsetEnv(t, "BEAT_TOKENS_FILE")

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
	// BEAT_TOKENS is the opposite case (see the test below) — senders must
	// reproduce it byte for byte, so rewriting it is what breaks the gate.
	if cfg.WebhookURL != "https://discord.example/hook" {
		t.Errorf("WebhookURL = %q, want the padding trimmed", cfg.WebhookURL)
	}
	if !hasToken(cfg, "unit-test-beat-token") {
		t.Errorf("BeatToken = %q, want the configured token verbatim", tokenSummary(cfg))
	}
}

// TestLoadRejectsAPaddedPlainBeatToken pins that a BEAT_TOKENS token with edge
// ASCII whitespace fails startup rather than being trimmed. webapi verifies
// "Bearer "+token, so a leading run is interior to the header and arrives
// intact: trimming " secret" to "secret" would arm the gate for a value no
// sender presents, and every beat would go falsely missing one deadline later.
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
			t.Setenv("BEAT_TOKENS", "api:"+token)
			unsetEnv(t, "BEAT_TOKENS_FILE")

			_, err := Load(maxNodeNameBytes)
			if err == nil {
				t.Fatalf("Load() with BEAT_TOKENS=%q = nil, want error: rewriting the credential would arm the gate for a value the sender that uses the configured one cannot present", token)
			}
			if !strings.Contains(err.Error(), "BEAT_TOKENS") {
				t.Errorf("error = %q, want BEAT_TOKENS named so the operator knows which variable to fix", err)
			}
			// The mirror of the file-borne case: the clause fires only on
			// envx.SecretSourceFile, so a plain-variable refusal must not send the
			// operator to a mount they never configured.
			if strings.Contains(err.Error(), "came from BEAT_TOKENS_FILE") {
				t.Errorf("error = %q blames BEAT_TOKENS_FILE for a value the plain variable supplied", err)
			}
			if strings.Contains(err.Error(), "unit-test-beat-token") {
				t.Errorf("error = %q embeds the token value; the startup error is shipped to Loki, so it must describe the shape and never echo the credential", err)
			}
		})
	}
}

// TestLoadRejectsAPaddedFileBorneBeatToken pins the _FILE half of the padded
// token refusal: envx returns a file's content as written apart from one
// trailing line ending, so padded bytes reach checkBeatToken and both channels
// refuse the same value. The trailing-newline cases prove that line ending does
// not rescue a padded value: `printf '%s\n'` passes, `printf '%s \n'` does not.
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
			if err := os.WriteFile(tokenFile, []byte("api:"+content), 0o600); err != nil {
				t.Fatal(err)
			}
			setValidLoadEnv(t)
			unsetEnv(t, "BEAT_TOKENS")
			t.Setenv("BEAT_TOKENS_FILE", tokenFile)

			_, err := Load(maxNodeNameBytes)
			if err == nil {
				t.Fatalf("Load() with a BEAT_TOKENS_FILE holding %q = nil, want error: the padding is part of the configured credential, so arming the gate with a trimmed copy 401s every sender that presents the file's token and turns every configured beat falsely missing one deadline later", content)
			}
			if !strings.Contains(err.Error(), "leading or trailing ASCII whitespace") {
				t.Errorf("error = %q, want the padding refusal: any other failure would mean the file channel was rejected for the wrong reason, or rewritten before checkBeatToken saw it", err)
			}
			if !strings.Contains(err.Error(), "BEAT_TOKENS") {
				t.Errorf("error = %q, want BEAT_TOKENS named so the operator knows which secret to fix", err)
			}
			// The CHANNEL, not just the variable: a BEAT_TOKENS_FILE pointing at
			// the wrong file crash-loops the observer, and a refusal naming only
			// BEAT_TOKENS describes a variable the operator may never have set.
			if !strings.Contains(err.Error(), "came from BEAT_TOKENS_FILE") {
				t.Errorf("error = %q, want it to name the channel the refused value arrived through; the padding refusal itself never mentions the _FILE variable, so only fileSourcedValueError can supply it", err)
			}
			if strings.Contains(err.Error(), "unit-test-beat-token") {
				t.Errorf("error = %q embeds the token value; the startup error is shipped to Loki, so it must describe the shape and never echo the credential", err)
			}
		})
	}
}

func TestLoadKeepsANonASCIISpaceBeatTokenArmed(t *testing.T) {
	// The boundary of the refusal above: net/textproto strips only spaces and
	// tabs, so an NBSP-only token arrives verbatim and authenticates, although
	// strings.TrimSpace calls it blank. Refusing or trimming it would break a
	// working configuration. Eight NBSPs make exactly 16 bytes, clearing the
	// length floor.
	const token = "\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0\u00a0"
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKENS", "api:"+token)
	unsetEnv(t, "BEAT_TOKENS_FILE")

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() with an NBSP-only BEAT_TOKENS = %v, want accepted: textproto keeps a non-ASCII space, so the token is presentable and the gate must stay armed", err)
	}
	if len(token) != minTokenLength {
		t.Fatalf("fixture is %d bytes, want exactly %d: the case only clears the length floor at that size", len(token), minTokenLength)
	}
	if !hasToken(cfg, token) {
		t.Errorf("BeatToken = %q, want %q preserved verbatim: trimming it would leave webapi's gate with a credential no sender presents", tokenSummary(cfg), token)
	}
}

// TestLoadKeepsABeatTokenWithInteriorASCIIWhitespaceArmed pins the SCOPE of the
// padding refusal through Load: it is an EDGE rule, and a space or tab INSIDE the
// credential is accepted and stored verbatim, since net/textproto delivers an
// interior SP or HTAB untouched. Refusing them anywhere, the shape a "harden the
// credential" edit reaches for, stops an observer with a passphrase-style token
// from starting at all.
func TestLoadKeepsABeatTokenWithInteriorASCIIWhitespaceArmed(t *testing.T) {
	// Serial (t.Setenv forbids t.Parallel). A space AND a tab, both interior,
	// and 20 bytes so the value clears the minTokenLength floor on its own.
	const token = "unit test beat\ttoken"
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKENS", "api:"+token)
	unsetEnv(t, "BEAT_TOKENS_FILE")

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() with BEAT_TOKENS=%q = %v, want accepted: HTTP normalizes only the EDGES of a field value, so an interior space or tab reaches the verifier untouched and refusing it fails startup on a working credential", token, err)
	}
	if !hasToken(cfg, token) {
		t.Errorf("BeatToken = %q, want %q verbatim: webapi compares against \"Bearer \"+token, so collapsing or stripping an interior byte arms the gate for a value no sender presents and one deadline later every beat posts a false MISSING notice", tokenSummary(cfg), token)
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

func TestLoadRejectsABeatTokenNoSenderCanPresentAsRead(t *testing.T) {
	// Each value survives an ASCII trim, yet HTTP forbids its byte in a field
	// value or the operator cannot read the token as configured. Armed with it,
	// knell reports healthy, records no ping, and one deadline later declares
	// every beat missing. Each row names the diagnosis it must draw and clears
	// the length floor, or the length refusal answers for the byte rule and
	// deleting the field-value check leaves this table green.
	tests := map[string]struct {
		token   string
		wantErr string
	}{
		"interior carriage": {token: "unit-test\rbeat-token", wantErr: "control character"},
		"delete byte":       {token: "unit-test\x7fbeat-token", wantErr: "control character"},
		// An interior VT is inside asciiWhitespace's cutset, but that cutset is an
		// EDGE rule, so the byte rule is the only one that can refuse this value.
		"vertical tab interior": {token: "unit-test\vbeat-token", wantErr: "control character"},
		// EDGE position, for the bytes asciiWhitespace does NOT cover: DEL and the
		// C0 controls other than HTAB/CR/LF/VT/FF survive the edge trim, so the
		// field-value check is the only rule that can refuse them there, and a
		// check that skipped either end would arm a token no sender can write.
		"leading control byte": {token: "\x01unit-test-beat-token", wantErr: "control character"},
		"trailing delete byte": {token: "unit-test-beat-token\x7f", wantErr: "control character"},
		// The EDGE spelling of CR: a forbidden field-value byte that the padding
		// cutset refuses first, so this row draws the whitespace diagnosis rather
		// than the byte one. LF never reaches a token: it separates entries.
		"trailing carriage return": {token: "unit-test-beat-token\r", wantErr: "leading or trailing ASCII whitespace"},
		// HTTP carries these bytes, so the wire alone accepts them, but the operator
		// cannot read them in the token they hold: a pasted zero-width space or bidi
		// control makes the value the sender types differ from the configured one.
		"zero-width space":    {token: "unit-test\u200bbeat-token", wantErr: "invisible character"},
		"bidi override":       {token: "unit-test\u202ebeat-token", wantErr: "invisible character"},
		"leading byte order":  {token: "\ufeffunit-test-beat-token", wantErr: "invisible character"},
		"invalid UTF-8 byte":  {token: "unit-test\xffbeat-token", wantErr: "not valid UTF-8"},
		"C1 control in UTF-8": {token: "unit-test\u0085beat-token", wantErr: "invisible character"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv("BEAT_TOKENS", "api:"+tt.token)
			unsetEnv(t, "BEAT_TOKENS_FILE")

			_, err := Load(maxNodeNameBytes)
			if err == nil {
				t.Fatalf("Load() with BEAT_TOKENS=%q = nil, want the %q refusal: the gate would arm for a token no sender can present as the operator reads it, so every ping 401s against an endpoint that reports itself gated", tt.token, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want the %q diagnosis: every fixture here also clears the length floor, so any other refusal means the rule that owns this byte no longer runs", err, tt.wantErr)
			}
			if !strings.Contains(err.Error(), "BEAT_TOKENS") {
				t.Errorf("error = %q, want BEAT_TOKENS named so the operator knows which variable to fix", err)
			}
			if strings.Contains(err.Error(), tt.token) {
				t.Errorf("error = %q embeds the token value; the startup error is shipped to Loki, so it must describe the shape and never echo the credential", err)
			}
		})
	}
}

// TestCheckBeatTokenAgreesWithTheTransportForEveryASCIIByte sweeps every ASCII
// byte in the interior of a token against the transport, the only oracle for
// it. Both directions of a divergence are silent: a byte wrongly ACCEPTED arms
// a gate no sender can present (every ping 401s and every beat goes falsely
// missing one deadline later), and a byte wrongly REFUSED fails startup on a
// working configuration. A byte from 0x80 up is refused by design whatever the
// wire carries: alone it is invalid UTF-8.
func TestCheckBeatTokenAgreesWithTheTransportForEveryASCIIByte(t *testing.T) {
	srv := authEchoServer(t)
	for b := range 0x80 {
		token := "unit-test" + string([]byte{byte(b)}) + "beat-token"
		echoed, doErr := echoAuthHeader(t, srv, token)
		carried := doErr == nil && echoed == "Bearer "+token
		if accepted := checkBeatToken(token, 1) == nil; accepted != carried {
			t.Errorf("interior byte 0x%02x: checkBeatToken accepts = %v, but the transport carried it verbatim = %v (err=%v echo=%q); the two must agree or startup either arms an unpresentable token or refuses a working one", b, accepted, carried, doErr, echoed)
		}
	}
}

// TestLoadAcceptsABeatTokenAtTheLengthFloor pins the inclusive edge of the
// refusal: minTokenLength bytes is the SHORTEST accepted token, so an
// off-by-one would fail startup on a value the README tells the operator to use.
func TestLoadAcceptsABeatTokenAtTheLengthFloor(t *testing.T) {
	token := strings.Repeat("x", minTokenLength)
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKENS", "api:"+token)
	unsetEnv(t, "BEAT_TOKENS_FILE")

	cfg, err := Load(maxNodeNameBytes)
	if err != nil {
		t.Fatalf("Load() with a %d-byte BEAT_TOKENS = %v, want accepted: the floor is inclusive", minTokenLength, err)
	}
	if !hasToken(cfg, token) {
		t.Errorf("BeatToken = %q, want the configured %d-byte token", tokenSummary(cfg), minTokenLength)
	}
}

func TestLoadFallsBackToTheHostnameWhenNodeNameIsUnset(t *testing.T) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		t.Skipf("cannot determine the hostname: %v", err)
	}
	setValidLoadEnv(t)
	// ABSENT, not present-but-empty: an unset NODE_NAME is what a deployment
	// that accepts the hostname default ships. For NODE_NAME the two states are
	// indistinguishable to the code (os.Getenv returns "" for both) and both
	// silently fall back to the hostname; unsetting here documents the shipped
	// shape rather than a behavioral difference.
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

// TestLoadReadsTheOpsListenAddr pins OPS_LISTEN_ADDR's trim, its default for a
// value with nothing visible in it, and that Load carries it into Config.
func TestLoadReadsTheOpsListenAddr(t *testing.T) {
	cases := map[string]struct{ raw, want string }{
		"set":             {"127.0.0.1:19191", "127.0.0.1:19191"},
		"padded":          {"  0.0.0.0:19191\u200b", "0.0.0.0:19191"},
		"blank":           {"", defaultOpsListenAddr},
		"nothing visible": {"\u200b\ufeff", defaultOpsListenAddr},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv("OPS_LISTEN_ADDR", tc.raw)
			cfg, err := Load(maxNodeNameBytes)
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if cfg.OpsListenAddr != tc.want {
				t.Errorf("OpsListenAddr = %q, want %q", cfg.OpsListenAddr, tc.want)
			}
			if got := OpsListenAddr(); got != tc.want {
				t.Errorf("OpsListenAddr() = %q, want %q: the health probe and Load must read one value", got, tc.want)
			}
		})
	}
}

// TestLoadRefusesTheOpsListenerOnThePingPort pins that /healthz and /metrics
// never share LISTEN_ADDR's port: ports compare as the
// numbers net.Listen resolves, and the refusal names both settings and the
// default so an operator whose LISTEN_ADDR is :9191 sees where 9191 came from.
func TestLoadRefusesTheOpsListenerOnThePingPort(t *testing.T) {
	refused := map[string]struct{ listen, ops string }{
		"the same address":                    {":9300", ":9300"},
		"the same port on another host":       {"127.0.0.1:9300", "0.0.0.0:9300"},
		"a leading zero":                      {":9300", ":09300"},
		"a v3 LISTEN_ADDR on the new default": {":9191", ""},
	}
	for name, tc := range refused {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv("LISTEN_ADDR", tc.listen)
			t.Setenv("OPS_LISTEN_ADDR", tc.ops)
			_, err := Load(maxNodeNameBytes)
			if err == nil {
				t.Fatal("Load() accepted an OPS_LISTEN_ADDR on LISTEN_ADDR's port, so /metrics would be served wherever pings are")
			}
			for _, want := range []string{"OPS_LISTEN_ADDR", "LISTEN_ADDR"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %s", err, want)
				}
			}
		})
	}
	accepted := map[string]struct{ listen, ops string }{
		"two ports":           {":9300", ":9301"},
		"two ephemeral ports": {"127.0.0.1:0", "127.0.0.1:0"},
		"a port that does not resolve, refused by the bind instead": {":9300", ":nosuchservice"},
	}
	for name, tc := range accepted {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			t.Setenv("LISTEN_ADDR", tc.listen)
			t.Setenv("OPS_LISTEN_ADDR", tc.ops)
			if _, err := Load(maxNodeNameBytes); err != nil {
				t.Errorf("Load() = %v, want nil", err)
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

// TestLoadTrimsInvisibleConfigPadding pins the part of LISTEN_ADDR's and
// DISCORD_WEBHOOK_URL's trim that strings.TrimSpace does not do: a zero-width
// space, a soft hyphen and a BOM are format runes, not White_Space, so
// reverting either TrimFunc(…, invisibleInURL) to TrimSpace fails a case here.
// Unpinned, a padded LISTEN_ADDR crash-loops on a bind error whose cause the
// log line cannot show.
func TestLoadTrimsInvisibleConfigPadding(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv.
	const (
		zeroWidthSpace = "\u200b"
		softHyphen     = "\u00ad"
		byteOrderMark  = "\ufeff"
	)

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

		cfg, err := Load(maxNodeNameBytes)
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.ListenAddr != defaultListenAddr {
			t.Errorf("ListenAddr = %q, want the default %q: a value with nothing visible in it reaches the documented blank path, not a bind failure", cfg.ListenAddr, defaultListenAddr)
		}
	})
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

// TestConfigLogValueNeverRendersASecret pins the redaction seam that lets a
// call site log a whole Config: both credentials are reported by source, never
// by value, and neither gets a presence attr, since both are required and
// presence reports no state. The receiver is a value, not a pointer, because
// Load returns Config by value and that is what a slog call would receive.
func TestConfigLogValueNeverRendersASecret(t *testing.T) {
	t.Parallel()

	const (
		secretHook  = "https://discord.example/api/webhooks/1/leak-me-if-you-can"
		secretToken = "leak-me-if-you-can-token"
	)
	cfg := Config{
		WebhookURL:       secretHook,
		WebhookSource:    envx.SecretSourceEnv,
		Node:             "node-a",
		ListenAddr:       ":9190",
		BeatTokens:       mustBeatTokens(t, "api:"+secretToken),
		BeatTokensSource: envx.SecretSourceFile,
		Beats:            []Beat{{ID: "api", Deadline: time.Hour}},
		LogLevel:         slog.LevelInfo,
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
	if got["webhook"] != string(envx.SecretSourceEnv) {
		t.Errorf("webhook = %q, want %q: the attr reports the channel that supplied the credential, never the credential", got["webhook"], envx.SecretSourceEnv)
	}
	if got["beat_tokens"] != string(envx.SecretSourceFile) {
		t.Errorf("beat_tokens = %q, want %q: the attr reports the channel that supplied the tokens, never a token", got["beat_tokens"], envx.SecretSourceFile)
	}
	if _, reported := got["beat_auth"]; reported {
		t.Errorf("LogValue renders beat_auth = %q; the token is required, so a presence attr can only ever say \"required\" and reports no state — its CHANNEL is the state, and beat_tokens publishes that", got["beat_auth"])
	}
}

// TestConfigLogValueReportsEachCredentialSource pins both source attrs against
// Load. Each reports state: the _FILE channel keeps a credential out of the
// environment and `docker inspect`, the plain variable does not, and for a
// plain-only deployment this line is the whole signal. A rendering keyed off
// presence instead of source fails only here.
func TestConfigLogValueReportsEachCredentialSource(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv.
	const (
		url   = "https://discord.example/api/webhooks/1/source-probe"
		token = "source-probe-beat-token"
	)

	for name, tc := range map[string]struct {
		key   string
		value string
		file  bool
		attr  string
		field func(Config) string
		want  envx.SecretSource
	}{
		"the plain webhook variable leaves the credential in the environment": {
			key: "DISCORD_WEBHOOK_URL", value: url, attr: "webhook",
			field: func(cfg Config) string { return cfg.WebhookURL }, want: envx.SecretSourceEnv,
		},
		"the webhook _FILE companion keeps it out of the environment": {
			key: "DISCORD_WEBHOOK_URL", value: url, file: true, attr: "webhook",
			field: func(cfg Config) string { return cfg.WebhookURL }, want: envx.SecretSourceFile,
		},
		"the plain token variable leaves the token in the environment": {
			key: "BEAT_TOKENS", value: "api:" + token, attr: "beat_tokens",
			field: func(cfg Config) string { return tokenLine(cfg, token) }, want: envx.SecretSourceEnv,
		},
		"the token _FILE companion keeps it out of the environment": {
			key: "BEAT_TOKENS", value: "api:" + token, file: true, attr: "beat_tokens",
			field: func(cfg Config) string { return tokenLine(cfg, token) }, want: envx.SecretSourceFile,
		},
	} {
		t.Run(name, func(t *testing.T) {
			setValidLoadEnv(t)
			if tc.file {
				path := filepath.Join(t.TempDir(), "secret")
				if err := os.WriteFile(path, []byte(tc.value+"\n"), 0o600); err != nil {
					t.Fatalf("writing the secret file: %v", err)
				}
				unsetEnv(t, tc.key)
				t.Setenv(tc.key+"_FILE", path)
			} else {
				t.Setenv(tc.key, tc.value)
				unsetEnv(t, tc.key+"_FILE")
			}

			cfg, err := Load(maxNodeNameBytes)
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if got := tc.field(cfg); got != tc.value {
				t.Fatalf("%s = %q, want %q", tc.key, got, tc.value)
			}
			var rendered string
			for _, attr := range cfg.LogValue().Group() {
				if attr.Key == tc.attr {
					rendered = attr.Value.String()
				}
			}
			if rendered != string(tc.want) {
				t.Errorf("startup line reports %s = %q, want %q: Load must carry the channel through, or LogValue has nothing to report", tc.attr, rendered, tc.want)
			}
			if strings.Contains(rendered, tc.value) {
				t.Errorf("startup line reports %s = %q, which carries the credential itself", tc.attr, rendered)
			}
		})
	}
}

// TestConfigLogValueReportsEveryNonSecretField pins the accuracy half of
// LogValue's contract; TestConfigLogValueNeverRendersASecret
// pins the hygiene half. LogValue exists so a call site can hand a whole
// Config to slog, and those attrs are then the entire rendering of a
// configuration that is env-only, with no reload and no readback endpoint.
// Every value below differs from any plausible default and from every sibling
// field, so an attr rewired to a literal or to the wrong field fails here
// instead of publishing a line that contradicts the configuration running.
func TestConfigLogValueReportsEveryNonSecretField(t *testing.T) {
	t.Parallel()

	// Two entries, so allowed_hosts pins the SIZE rather than merely "active":
	// an allowlist state rendered from the wrong side of the policy (a hardcoded
	// 1, or Active() read as the count) still reads plausible in a log line.
	policy, invalid := httpserver.ParseHostList([]string{"knell.internal", "10.0.0.5"})
	if len(invalid) > 0 {
		t.Fatalf("ParseHostList rejected %v; the fixture must be a valid allowlist", invalid)
	}
	proxies, invalidCIDRs := httpserver.ParseCIDRs([]string{"10.0.0.0/24", "192.168.1.5"})
	if len(invalidCIDRs) > 0 {
		t.Fatalf("ParseCIDRs rejected %v; the fixture must be a valid trusted-proxy set", invalidCIDRs)
	}

	cfg := Config{
		AllowedHosts: policy,
		// Two ranges rather than zero or one: the attr publishes a COUNT, so a
		// zero fixture renders the same "0" an unset TRUSTED_PROXIES does and
		// would assert nothing, and 1 cannot be told apart from a boolean.
		TrustedProxies: proxies,
		WebhookURL:     "https://discord.example/hook",
		WebhookSource:  envx.SecretSourceFile,
		Node:           "observer-1",
		ListenAddr:     "127.0.0.1:19190",
		OpsListenAddr:  "127.0.0.1:19191",
		BeatTokens:     mustBeatTokens(t, "api:unit-test-beat-token"),
		// The OTHER channel than the webhook's, deliberately: the two source
		// attrs are the only pair in this fixture that share a value domain, so
		// an attr wired to the wrong source field would otherwise render exactly
		// what the right one does and pass.
		BeatTokensSource: envx.SecretSourceEnv,
		Beats: []Beat{
			{ID: "watchdog-mimir", Deadline: 20 * time.Minute},
			{ID: "watchdog-loki", Deadline: 26 * time.Hour},
		},
		MissingRepeat: 6 * time.Hour,
		LogLevel:      slog.LevelDebug,
	}

	got := map[string]string{}
	for _, attr := range cfg.LogValue().Group() {
		got[attr.Key] = attr.Value.String()
	}

	want := map[string]string{
		"beats":           "2",
		"node":            "observer-1",
		"listen_addr":     "127.0.0.1:19190",
		"ops_listen_addr": "127.0.0.1:19191",
		// The rendered WORD, not string(envx.SecretSourceFile): restating the constant
		// would assert nothing about what an operator reads, and this attr's
		// whole job is to name a channel legibly in the startup line.
		"webhook": "file",
		// The token's channel, reported symmetrically with the webhook's and from
		// its own field: both credentials are exposed by the plain variable in the
		// same way, so publishing one channel and not the other tells an operator
		// half of where their secrets live.
		"beat_tokens":        "env",
		"beat_token_entries": "1",
		"allowed_hosts":      "allowlist(2)",
		// A COUNT, for the reason allowed_hosts publishes one: a misspelled
		// TRUSTED_PROXIES is indistinguishable from an unset one, so the number
		// of ranges actually held is the state an operator checks this line for.
		"trusted_proxies": "2",
		"missing_repeat":  "6h0m0s",
		"log_level":       "DEBUG",
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

	blank, invalid := httpserver.ParseHostList([]string{""})
	if len(invalid) > 0 {
		t.Fatalf("ParseHostList rejected blank input: %v", invalid)
	}

	for name, policy := range map[string]*httpserver.HostPolicy{
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
		// health verdict turns healthy, and every notice then fails as a transport
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
// BEAT_TOKENS_FILE) instead of a path to it. Wrapping envx's error verbatim
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
			key:    "BEAT_TOKENS",
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
// BEAT_TOKENS of Unicode spaces around a control byte passes the ASCII-edge
// refusal and is BOTH header-illegal and under the 16-byte floor, so it is the
// one fixture whose diagnosis reveals which check ran first: the operator has
// to be told the token carries a byte HTTP forbids, not that it is too short,
// because lengthening it would not make it presentable.
func TestLoadRefusesAHeaderIllegalTokenBeforeTheLengthFloor(t *testing.T) {
	// Serial (t.Setenv forbids t.Parallel).
	setValidLoadEnv(t)
	t.Setenv("BEAT_TOKENS", "api:"+"\u00a0\x01\u00a0")

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with a BEAT_TOKENS carrying an interior control byte = nil, want error: no sender can present a control byte in a header value")
	}
	if !strings.Contains(err.Error(), "control character") {
		t.Errorf("error = %q, want the control-character refusal", err)
	}
}

// TestLoadWarnsOnlyWhenLogLevelIsUnparseable pins the only signal that a
// mistyped LOG_LEVEL was ignored: slogx.ParseLevel reports !ok only for a
// non-empty unparseable value, and this WARN separates "asked for debug, got
// debug" from a typo running silently at info. The table pins the level and the
// warning together, so losing the warning on a typo or adding one on a valid
// level both fail.
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
			wantCount := 0
			if tt.wantWarn {
				wantCount = 1
			}
			gotAny := rec.Count("invalid LOG_LEVEL")
			gotWarn := rec.CountLevel(slog.LevelWarn, "invalid LOG_LEVEL")
			if gotAny != wantCount || gotWarn != wantCount {
				t.Errorf("LOG_LEVEL=%q: matching records = %d, WARN records = %d, want %d; the warning is the only signal that a typo was ignored, it is invisible below WARN at the default level, and a warning on a valid level trains the operator to ignore it: %v", tt.raw, gotAny, gotWarn, wantCount, rec.Records())
			}
		})
	}
}

// hasToken reports whether token is one of cfg's beat tokens.
func hasToken(cfg Config, token string) bool {
	_, ok := cfg.BeatTokens.Set.Match(token)
	return ok
}

// tokenSummary renders cfg's token set for a failure message, without a token.
func tokenSummary(cfg Config) string {
	return fmt.Sprintf("%d token(s) covering %v", cfg.BeatTokens.Set.Len(), cfg.BeatTokens.Covers)
}

// tokenLine is the BEAT_TOKENS line "api:"+token when cfg holds token for api,
// else "".
func tokenLine(cfg Config, token string) string {
	if i, ok := cfg.BeatTokens.Set.Match(token); ok && slices.Contains(cfg.BeatTokens.Covers[i], "api") {
		return "api:" + token
	}
	return ""
}

// mustBeatTokens parses raw against the one beat api, failing the test on an
// error.
func mustBeatTokens(t *testing.T, raw string) BeatTokens {
	t.Helper()
	tokens, err := parseBeatTokens(raw, []Beat{{ID: "api", Deadline: time.Hour}})
	if err != nil {
		t.Fatalf("parseBeatTokens: %v", err)
	}
	return tokens
}
