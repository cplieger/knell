package main

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/cplieger/health"
)

// TestMain re-executes the test binary as knell itself when the re-exec
// variable is set, so main()'s argv dispatch — which ends in os.Exit on every
// branch — can be observed from a child process. Without the variable it just
// runs the package's tests.
func TestMain(m *testing.M) {
	if os.Getenv("KNELL_TEST_REEXEC_MAIN") == "1" {
		main()
		return
	}
	os.Exit(m.Run())
}

// runMain re-executes this test binary as knell with args and env, returning
// the child's exit code and its combined output.
func runMain(t *testing.T, env []string, args ...string) (int, string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "KNELL_TEST_REEXEC_MAIN=1")
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), string(out)
	default:
		t.Fatalf("re-exec %v: %v", args, err)
		return -1, ""
	}
}

// TestHealthSubcommandReportsTheMarkerVerdictWithoutBooting pins the Docker
// healthcheck contract: `knell health` stats the marker and exits with the
// probe's verdict, and never falls through into the server.
func TestHealthSubcommandReportsTheMarkerVerdictWithoutBooting(t *testing.T) {
	t.Run("marker absent", func(t *testing.T) {
		_ = os.Remove(health.DefaultPath)
		code, out := runMain(t, nil, "health")
		if code != 1 {
			t.Errorf("knell health without a marker = exit %d, want 1; the healthcheck must fail closed: %s", code, out)
		}
		if strings.Contains(out, "listening") {
			t.Errorf("knell health booted the server: %s", out)
		}
	})

	t.Run("marker present", func(t *testing.T) {
		if err := os.WriteFile(health.DefaultPath, nil, 0o600); err != nil {
			t.Skipf("cannot plant a health marker at %s: %v", health.DefaultPath, err)
		}
		t.Cleanup(func() { _ = os.Remove(health.DefaultPath) })
		code, out := runMain(t, nil, "health")
		if code != 0 {
			t.Errorf("knell health with a marker = exit %d, want 0: %s", code, out)
		}
	})
}

// TestUnknownSubcommandExitsTwoWithoutBooting pins that an unrecognized argv
// is a usage error, not an accidental boot: a mistyped healthcheck command
// must not start a second switch behind the same marker.
func TestUnknownSubcommandExitsTwoWithoutBooting(t *testing.T) {
	code, out := runMain(t, nil, "serve")
	if code != 2 {
		t.Errorf("knell serve = exit %d, want 2: %s", code, out)
	}
	if !strings.Contains(out, "unknown command") {
		t.Errorf("output = %q, want it to name the unknown command", out)
	}
	if strings.Contains(out, "listening") {
		t.Errorf("an unknown subcommand booted the server: %s", out)
	}
}

// TestRejectedConfigExitsOneWithoutLeakingTheWebhook pins the boot-failure
// contract end to end: a rejected configuration exits 1 (so the container
// restarts instead of idling as a dead switch), says so, and never echoes the
// webhook URL — the credential is in its path.
func TestRejectedConfigExitsOneWithoutLeakingTheWebhook(t *testing.T) {
	code, out := runMain(t, []string{
		"BEATS=api:20m",
		"DISCORD_WEBHOOK_URL=http://discord.example/api/webhooks/1234567890/verysecrettoken",
		"DISCORD_WEBHOOK_URL_FILE=",
		"BEAT_TOKEN=",
		"BEAT_TOKEN_FILE=",
	})
	if code != 1 {
		t.Errorf("knell with a plain-http webhook = exit %d, want 1: %s", code, out)
	}
	if !strings.Contains(out, "knell exited with error") {
		t.Errorf("output = %q, want the boot failure reported", out)
	}
	if strings.Contains(out, "verysecrettoken") {
		t.Errorf("the boot-failure output leaks the webhook URL: %s", out)
	}
}
