package main

import (
	"errors"
	"io/fs"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cplieger/health"
	"github.com/cplieger/knell/internal/config"
	"github.com/cplieger/slogx/capture"
)

// unsetEnv removes key for the duration of the test. t.Setenv registers the
// restore of the original value, so the following os.Unsetenv leaves the
// variable absent inside the test and restored afterwards. A plain
// t.Setenv(key, "") would leave a PRESENT-but-empty variable, which
// config.Load rejects for `_FILE` keys.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("unsetting %s: %v", key, err)
	}
}

// TestRunClearsStaleHealthMarkerBeforeTheConfigGate pins the boot ordering a
// crash-looping container depends on: the marker is cleared before the first
// exit path, so a fail-fast config gate cannot leave the previous process's
// marker on disk reporting the restart loop healthy. Both cases fail config
// before run() reaches the listener bind, which is what makes run() reachable
// from a test at all.
func TestRunClearsStaleHealthMarkerBeforeTheConfigGate(t *testing.T) {
	cases := map[string]map[string]string{
		"malformed BEATS": {
			"BEATS": "api-without-a-deadline",
		},
		// The live upgrade case: a deployment whose webhook URL is plain http
		// now fails the https-only gate at boot. DISCORD_WEBHOOK_URL_FILE is
		// unset in the subtest so an ambient _FILE secret cannot satisfy the
		// gate and let run() proceed to bind and block.
		"plain-http webhook": {
			"BEATS":               "api:1m",
			"DISCORD_WEBHOOK_URL": "http://discord.example/api/webhooks/1234567890/verysecrettoken",
		},
	}

	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			// Serial (no t.Parallel): t.Setenv, plus run() installs a
			// process-global slog default of its own.
			for k, v := range env {
				t.Setenv(k, v)
			}
			// Unset, not blanked: a present-but-empty _FILE variable now
			// fails startup, and this only needs the ambient secret gone so
			// it cannot satisfy the webhook gate.
			unsetEnv(t, "DISCORD_WEBHOOK_URL_FILE")
			// Installs a fresh recorder and restores the previous default at
			// test end; run() replaces the default with its own handler.
			capture.Default(t)

			// Plant the marker a previous run would have left behind.
			// O_EXCL|O_NOFOLLOW: health.DefaultPath is a fixed path in a
			// world-writable directory, so a plain os.WriteFile would follow a
			// pre-planted symlink and truncate its target as the test user.
			f, plantErr := os.OpenFile(health.DefaultPath,
				os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
			if errors.Is(plantErr, fs.ErrExist) {
				// A marker left behind by an earlier test (or by a knell
				// running on this host) must not silently disable this
				// contract: drop it and retry with the same refusing flags,
				// so a symlink planted in the window is still refused.
				if info, lerr := os.Lstat(health.DefaultPath); lerr == nil && info.Mode().IsRegular() {
					if rerr := os.Remove(health.DefaultPath); rerr != nil {
						t.Skipf("cannot clear a stale marker at %s: %v", health.DefaultPath, rerr)
					}
					f, plantErr = os.OpenFile(health.DefaultPath,
						os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
				}
			}
			if plantErr != nil {
				t.Skipf("cannot plant a health marker at %s: %v", health.DefaultPath, plantErr)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("closing planted marker: %v", err)
			}
			t.Cleanup(func() { _ = os.Remove(health.DefaultPath) })

			err := run()
			if err == nil {
				t.Fatal("run() = nil, want a configuration error")
			}
			if !strings.Contains(err.Error(), "configuration") {
				t.Fatalf("run() = %v, want the configuration gate to reject the boot", err)
			}
			if _, statErr := os.Stat(health.DefaultPath); !errors.Is(statErr, fs.ErrNotExist) {
				t.Errorf("marker %s after a rejected boot: stat = %v, want it gone; a stale marker reports a crash-looping container healthy",
					health.DefaultPath, statErr)
			}
		})
	}
}

func TestLogConfigNeverLeaksWebhookURL(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global
	// slog default to inspect the startup summary.
	cfg := config.Config{
		WebhookURL: "https://discord.example/api/webhooks/1234567890/verysecrettoken",
		Node:       "node-1",
		ListenAddr: ":9190",
		Beats:      []config.Beat{{ID: "api", Deadline: 20 * time.Minute}},
	}

	rec := capture.Default(t)
	logConfig(&cfg)

	if !rec.Contains("configuration loaded") {
		t.Fatalf("messages = %v, want the startup summary", rec.Messages())
	}
	if !rec.HasAttr("configuration loaded", "webhook", "configured") {
		t.Error(`webhook attr must render as the literal presence marker "configured"`)
	}
	if rec.Contains("verysecrettoken") || rec.AttrContains("", "", "verysecrettoken") {
		t.Errorf("startup log leaks the webhook URL: %v", rec.Messages())
	}
	if !rec.HasAttr("configuration loaded", "beat_auth", "open") {
		t.Errorf("beat_auth should report open when BeatToken is empty: %v", rec.Messages())
	}
	if !rec.Contains("watching beat") || !rec.AttrContains("watching beat", "beat", "api") {
		t.Errorf("per-beat startup line missing: %v", rec.Messages())
	}
}

func TestLogConfigReportsBeatAuthRequiredWithoutLeakingToken(t *testing.T) {
	// Serial (no t.Parallel): swaps the process-global slog default.
	cfg := config.Config{
		WebhookURL: "https://discord.example/hook",
		Node:       "node-1",
		ListenAddr: ":9190",
		BeatToken:  "unit-test-beat-token",
		Beats:      []config.Beat{{ID: "api", Deadline: 20 * time.Minute}},
	}

	rec := capture.Default(t)
	logConfig(&cfg)

	if !rec.HasAttr("configuration loaded", "beat_auth", "required") {
		t.Errorf("beat_auth should report required when BeatToken is set: %v", rec.Messages())
	}
	if rec.Contains("unit-test-beat-token") || rec.AttrContains("", "", "unit-test-beat-token") {
		t.Errorf("startup log leaks the beat token: %v", rec.Messages())
	}
}

func TestRunFailsFastWhenTheListenAddressIsAlreadyBound(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, plus run() installs a process-global
	// slog default and touches the shared health-marker path.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind a probe listener: %v", err)
	}
	t.Cleanup(func() { _ = occupied.Close() })

	t.Setenv("BEATS", "api:20m")
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/api/webhooks/1234567890/verysecrettoken")
	t.Setenv("BEAT_TOKEN", "")
	unsetEnv(t, "DISCORD_WEBHOOK_URL_FILE")
	unsetEnv(t, "BEAT_TOKEN_FILE")
	t.Setenv("LISTEN_ADDR", occupied.Addr().String())
	capture.Default(t)

	err = run()
	if err == nil {
		t.Fatal("run() = nil, want a bind error; an address already in use must fail the boot instead of leaving the watcher alerting behind a listener nothing can reach")
	}
	if !strings.Contains(err.Error(), "binding") {
		t.Fatalf("run() = %v, want the bind failure surfaced to the caller", err)
	}
	if strings.Contains(err.Error(), "verysecrettoken") {
		t.Errorf("bind error leaks the webhook URL: %v", err)
	}
}
