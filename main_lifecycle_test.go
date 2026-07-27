package main

import (
	"errors"
	"io/fs"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/cplieger/health"
	"github.com/cplieger/slogx/capture"
)

// waitForMarker blocks until the health marker exists, or fails the test.
func waitForMarker(t *testing.T, present bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, err := os.Stat(health.DefaultPath)
		if errors.Is(err, fs.ErrNotExist) != present {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("marker %s presence never became %v", health.DefaultPath, present)
}

// TestRunArmsTheMarkerWhileServingAndReturnsCleanlyOnSignal pins the boot and
// shutdown halves of the switch's lifecycle: the marker — the only thing the
// baked `knell health` probe looks at — becomes present once the listener is
// serving (a dropped marker.Set(true) would leave the container unhealthy
// forever while serving correctly), a SIGTERM returns run() with a nil error
// so the container exits 0 instead of looking like a crash, and the marker is
// gone afterwards. The ORDER of bind and arm is not observable from here: on a
// failed bind the deferred marker.Cleanup removes the marker either way, so
// this test cannot distinguish arm-before-bind from arm-after-bind.
func TestRunArmsTheMarkerWhileServingAndReturnsCleanlyOnSignal(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, a process-global slog default, a
	// process-wide signal, and the shared health-marker path.
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot reserve a port: %v", err)
	}
	addr := free.Addr().String()
	if err := free.Close(); err != nil {
		t.Fatalf("releasing the reserved port: %v", err)
	}

	t.Setenv("BEATS", "api:1m")
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.example/api/webhooks/1234567890/verysecrettoken")
	unsetEnv(t, "DISCORD_WEBHOOK_URL_FILE")
	unsetEnv(t, "BEAT_TOKEN")
	unsetEnv(t, "BEAT_TOKEN_FILE")
	t.Setenv("LISTEN_ADDR", addr)
	// Installs a fresh recorder and restores the previous default at test
	// end; run() replaces the default with its own handler.
	capture.Default(t)
	t.Cleanup(func() { _ = os.Remove(health.DefaultPath) })

	done := make(chan error, 1)
	go func() { done <- run() }()

	waitForMarker(t, true)

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() = %v, want nil after a shutdown signal", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run() did not return within the shutdown grace after SIGTERM")
	}

	if _, err := os.Stat(health.DefaultPath); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("marker %s after shutdown: stat = %v, want it gone; a stopped switch must not report healthy",
			health.DefaultPath, err)
	}
}
