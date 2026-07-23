package config

import (
	"strings"
	"testing"
	"time"
)

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
			got, err := ParseBeats(tt.raw)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("ParseBeats(%q) = %v, want error containing %q", tt.raw, got, tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ParseBeats(%q) error = %q, want containing %q", tt.raw, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseBeats(%q) unexpected error: %v", tt.raw, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("ParseBeats(%q) = %v, want %v", tt.raw, got, tt.want)
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
	if len(entries) <= MaxBeats {
		t.Fatalf("test needs more than %d entries, built %d", MaxBeats, len(entries))
	}
	_, err := ParseBeats(strings.Join(entries, ","))
	if err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("expected maximum-cap error, got %v", err)
	}
}

func TestValidateWebhookURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw  string
		ok   bool
		name string
	}{
		{name: "https", raw: "https://discord.com/api/webhooks/1/abc", ok: true},
		{name: "http", raw: "http://127.0.0.1:9/hook", ok: true},
		{name: "no scheme", raw: "discord.com/api/webhooks/1/abc", ok: false},
		{name: "wrong scheme", raw: "ftp://discord.com/hook", ok: false},
		{name: "no host", raw: "https:///hook", ok: false},
		{name: "garbage", raw: "://", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateWebhookURL(tt.raw)
			if tt.ok && err != nil {
				t.Fatalf("validateWebhookURL(%q) = %v, want nil", tt.raw, err)
			}
			if !tt.ok && err == nil {
				t.Fatalf("validateWebhookURL(%q) = nil, want error", tt.raw)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	t.Setenv("BEATS", "api:20m")
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/hook")
	t.Setenv("NODE_NAME", "node-1")
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
	t.Setenv("BEATS", "api:20m")
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/hook")
	t.Setenv("NODE_NAME", "")
	t.Setenv("LISTEN_ADDR", "")
	t.Setenv("LOG_LEVEL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Node == "" {
		t.Error("Node should fall back to hostname, got empty")
	}
	if cfg.ListenAddr != ":9190" {
		t.Errorf("ListenAddr default = %q, want :9190", cfg.ListenAddr)
	}

	t.Setenv("BEATS", "")
	if _, err := Load(); err == nil {
		t.Error("Load() with empty BEATS should fail")
	}

	t.Setenv("BEATS", "api:20m")
	t.Setenv("DISCORD_WEBHOOK_URL", "")
	if _, err := Load(); err == nil {
		t.Error("Load() with empty DISCORD_WEBHOOK_URL should fail")
	}

	t.Setenv("DISCORD_WEBHOOK_URL", "not-a-url")
	if _, err := Load(); err == nil {
		t.Error("Load() with malformed DISCORD_WEBHOOK_URL should fail")
	}
}

// FuzzParseBeats pins the parser's safety invariants: it never panics, and
// every accepted result respects the documented grammar and caps.
func FuzzParseBeats(f *testing.F) {
	f.Add("api:20m")
	f.Add("a:30s,b:26h")
	f.Add("")
	f.Add(",,,")
	f.Add("api")
	f.Add("api:")
	f.Add(":20m")
	f.Add("api:20m,api:20m")
	f.Add("api beat:20m")
	f.Add("api:1ns")
	f.Add("🚨:20m")
	f.Fuzz(func(t *testing.T, raw string) {
		beats, err := ParseBeats(raw)
		if err != nil {
			return
		}
		if len(beats) == 0 || len(beats) > MaxBeats {
			t.Fatalf("accepted result has %d beats", len(beats))
		}
		seen := make(map[string]struct{}, len(beats))
		for _, b := range beats {
			if !beatIDPattern.MatchString(b.ID) {
				t.Fatalf("accepted id %q violates grammar", b.ID)
			}
			if b.Deadline < MinDeadline {
				t.Fatalf("accepted deadline %s below minimum", b.Deadline)
			}
			if _, dup := seen[b.ID]; dup {
				t.Fatalf("accepted duplicate id %q", b.ID)
			}
			seen[b.ID] = struct{}{}
		}
	})
}

func TestLoadInvalidLogLevelFallsBackToInfo(t *testing.T) {
	t.Setenv("BEATS", "api:20m")
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/hook")
	t.Setenv("NODE_NAME", "node-1")
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
	t.Setenv("BEATS", "api:1s")
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/hook")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with below-minimum deadline = nil, want error")
	}
	if !strings.Contains(err.Error(), "parsing BEATS") {
		t.Errorf("error = %q, want it wrapped with \"parsing BEATS\"", err)
	}
}

func TestLoadAcceptsPlainHTTPWebhook(t *testing.T) {
	t.Setenv("BEATS", "api:20m")
	t.Setenv("DISCORD_WEBHOOK_URL", "http://127.0.0.1:9/hook")
	t.Setenv("NODE_NAME", "node-1")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() with plain-http webhook = %v, want accepted (warn, not fail)", err)
	}
	if cfg.WebhookURL != "http://127.0.0.1:9/hook" {
		t.Errorf("WebhookURL = %q", cfg.WebhookURL)
	}
}
