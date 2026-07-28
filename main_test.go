package main

import (
	"context"
	"errors"
	"fmt"
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

// plantHealthMarker creates the marker a previous knell run would have left
// behind, or skips the test when the fixed path cannot be planted.
// O_EXCL|O_NOFOLLOW: health.DefaultPath is a fixed path in a world-writable
// directory, so a plain os.WriteFile would follow a pre-planted symlink and
// truncate its target as the test user. A marker left behind by an earlier
// test (or by a knell running on this host) must not silently disable the
// contract under test: drop it and retry with the same refusing flags, so a
// symlink planted in the window is still refused.
func plantHealthMarker(t *testing.T) {
	t.Helper()
	plant := func() (*os.File, error) {
		return os.OpenFile(health.DefaultPath,
			os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	}
	f, plantErr := plant()
	if errors.Is(plantErr, fs.ErrExist) {
		if info, lerr := os.Lstat(health.DefaultPath); lerr == nil && info.Mode().IsRegular() {
			if rerr := os.Remove(health.DefaultPath); rerr != nil {
				t.Skipf("cannot clear a stale marker at %s: %v", health.DefaultPath, rerr)
			}
			f, plantErr = plant()
		}
	}
	if plantErr != nil {
		t.Skipf("cannot plant a health marker at %s: %v", health.DefaultPath, plantErr)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("closing planted marker: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(health.DefaultPath) })
}

// TestRunClearsStaleHealthMarkerBeforeTheConfigGate pins the boot ordering a
// crash-looping container depends on: the marker is cleared before the first
// exit path, so a fail-fast config gate cannot leave the previous process's
// marker on disk reporting the restart loop healthy. The case fails config
// before run() reaches the listener bind, which is what makes run() reachable
// from a test at all. The https-scheme rejection itself is pinned end to end
// by TestRejectedConfigExitsOneWithoutLeakingTheWebhook (main_cli_test.go).
func TestRunClearsStaleHealthMarkerBeforeTheConfigGate(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, plus run() installs a
	// process-global slog default of its own.
	t.Setenv("BEATS", "api-without-a-deadline")
	// Unset, not blanked: a present-but-empty _FILE variable now
	// fails startup, and this only needs the ambient secret gone so
	// it cannot satisfy the webhook gate.
	unsetEnv(t, "DISCORD_WEBHOOK_URL_FILE")
	// Installs a fresh recorder and restores the previous default at
	// test end; run() replaces the default with its own handler.
	capture.Default(t)

	// Plant the marker a previous run would have left behind.
	plantHealthMarker(t)

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
	unsetEnv(t, "BEAT_TOKEN")
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

// TestClassifyServeErrorNamesADrainThatOutlivedTheGrace pins the one ERROR
// line a container emits when its drain runs out of budget. webhttp.Run
// reports the expired shutdown deadline as a bare context.DeadlineExceeded,
// which on its own tells an operator nothing about WHICH deadline expired;
// classifyServeError is what turns it into a line naming the drain and the
// grace constant that bounds it. If the branch were dropped, a container
// SIGKILLed mid-drain would exit 1 with "context deadline exceeded" and the
// operator would have no way to tell a stuck drain from any other expired
// context; if the classification went the other way, an accept failure would
// be reported as a drain overrun that never happened.
func TestClassifyServeErrorNamesADrainThatOutlivedTheGrace(t *testing.T) {
	t.Parallel()

	serveFailure := errors.New("accept tcp [::]:9190: use of closed network connection")
	tests := map[string]struct {
		in        error
		wantNamed bool
	}{
		"clean shutdown":               {in: nil, wantNamed: false},
		"bare drain deadline":          {in: context.DeadlineExceeded, wantNamed: true},
		"wrapped drain deadline":       {in: fmt.Errorf("shutting down: %w", context.DeadlineExceeded), wantNamed: true},
		"serve failure":                {in: serveFailure, wantNamed: false},
		"cancellation, not a deadline": {in: context.Canceled, wantNamed: false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := classifyServeError(tt.in)
			if !tt.wantNamed {
				if got != tt.in {
					t.Errorf("classifyServeError(%v) = %v, want it returned unchanged: only the drain overrun may be renamed, or an accept failure is reported as a shutdown that never happened", tt.in, got)
				}
				return
			}
			if got == nil {
				t.Fatalf("classifyServeError(%v) = nil, want the drain overrun surfaced so the process exits 1", tt.in)
			}
			if !errors.Is(got, context.DeadlineExceeded) {
				t.Errorf("classifyServeError(%v) = %v, want the deadline still unwrappable", tt.in, got)
			}
			if !strings.Contains(got.Error(), "shutdown grace") {
				t.Errorf("classifyServeError(%v) = %q, want the drain named", tt.in, got)
			}
			if !strings.Contains(got.Error(), shutdownGrace.String()) {
				t.Errorf("classifyServeError(%v) = %q, want the grace budget %s named so the ERROR line points at the constant to raise", tt.in, got, shutdownGrace)
			}
		})
	}
}
