package main

import (
	"bufio"
	"errors"
	"io"
	"io/fs"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/cplieger/health"
	"github.com/cplieger/slogx/capture"
)

// waitForMarkerWithin blocks until the health marker reaches the wanted
// presence, or fails the test. A Stat error other than fs.ErrNotExist is a
// hard failure, never evidence of presence: a marker the probe cannot stat
// is not a healthy switch, and swallowing the error turned the oracle into
// "any error means absent".
func waitForMarkerWithin(t *testing.T, present bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := os.Stat(health.DefaultPath)
		switch {
		case err == nil && present:
			return
		case errors.Is(err, fs.ErrNotExist) && !present:
			return
		case err != nil && !errors.Is(err, fs.ErrNotExist):
			t.Fatalf("stat marker %s: %v", health.DefaultPath, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("marker %s presence never became %v within %s", health.DefaultPath, present, timeout)
}

// TestRunTracksHealthMarkerAcrossServeAndDrain pins the full lifecycle of the
// marker — the only thing the baked `knell health` probe looks at — including
// the ordering the deferred marker.Cleanup hides: the marker becomes present
// once the listener is serving (a dropped marker.Set(true) would leave the
// container unhealthy forever while serving correctly), it becomes ABSENT
// while run() is still draining a real in-flight /beat request (a deleted or
// post-Shutdown marker.Set(false) would call a draining container healthy for
// the whole stop window), and a SIGTERM returns run() with a nil error so the
// container exits 0 instead of looking like a crash.
func TestRunTracksHealthMarkerAcrossServeAndDrain(t *testing.T) {
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
	// Establish absence first: a marker left by a previous failed run or
	// another knell process would satisfy the presence wait immediately and
	// let the test signal itself before run() installed its signal context.
	if err := os.Remove(health.DefaultPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Skipf("cannot clear health marker at %s: %v", health.DefaultPath, err)
	}
	t.Cleanup(func() { _ = os.Remove(health.DefaultPath) })

	done := make(chan error, 1)
	go func() { done <- run() }()
	waitForMarkerWithin(t, true, 10*time.Second)

	// Hold one real request open across the shutdown so the interval between
	// pre-drain and the deferred cleanup becomes observable. Expect:
	// 100-continue proves the server accepted the request and is inside the
	// handler before the signal, without sending the body.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial serving knell: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := io.WriteString(conn, "POST /beat/api HTTP/1.1\r\nHost: "+addr+"\r\nContent-Length: 1048576\r\nExpect: 100-continue\r\n\r\n"); err != nil {
		t.Fatalf("start slow beat request: %v", err)
	}
	reader := bufio.NewReader(conn)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set continue deadline: %v", err)
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read 100-continue response: %v", err)
	}
	if line != "HTTP/1.1 100 Continue\r\n" {
		t.Fatalf("continue status line = %q, want HTTP/1.1 100 Continue", line)
	}
	if line, err = reader.ReadString('\n'); err != nil || line != "\r\n" {
		t.Fatalf("continue terminator = %q, %v, want blank line", line, err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear continue deadline: %v", err)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signalling self: %v", err)
	}
	waitForMarkerWithin(t, false, time.Second)
	select {
	case err := <-done:
		t.Fatalf("run() returned before its active request drained: %v", err)
	default:
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("release slow beat request: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() = %v, want nil after the active request drained", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after the active request drained")
	}

	if _, err := os.Stat(health.DefaultPath); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("marker %s after shutdown: stat = %v, want it gone; a stopped switch must not report healthy",
			health.DefaultPath, err)
	}
}
