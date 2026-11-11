package main

import (
	"net"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/cplieger/knell/internal/config"
)

// dockerfileInstructions returns the Dockerfile's instructions as fields, one
// slice per line that starts an instruction.
func dockerfileInstructions(t *testing.T) [][]string {
	t.Helper()
	b, err := os.ReadFile("Dockerfile")
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	var out [][]string
	for line := range strings.SplitSeq(string(b), "\n") {
		if fields := strings.Fields(line); len(fields) > 0 && !strings.HasPrefix(fields[0], "#") {
			out = append(out, fields)
		}
	}
	return out
}

// TestDockerfileExposesTheDefaultListenPort pins EXPOSE to the port knell
// binds when LISTEN_ADDR is unset, so `docker run -P` and tools that read the
// image's ports find the listener.
func TestDockerfileExposesTheDefaultListenPort(t *testing.T) {
	t.Setenv("LISTEN_ADDR", "")
	_, port, err := net.SplitHostPort(config.ListenAddr())
	if err != nil {
		t.Fatalf("default LISTEN_ADDR: %v", err)
	}
	for _, f := range dockerfileInstructions(t) {
		if f[0] == "EXPOSE" {
			if !slices.Contains(f[1:], port) {
				t.Errorf("EXPOSE lists %v, want the default listen port %s", f[1:], port)
			}
			return
		}
	}
	t.Errorf("Dockerfile has no EXPOSE, want EXPOSE %s", port)
}

// TestDockerfileShipsNoTmp pins that the image writes nothing to disk: the
// health verdict is in memory, so a /tmp in the image would only invite a
// tmpfs nobody needs.
func TestDockerfileShipsNoTmp(t *testing.T) {
	t.Parallel()
	for _, f := range dockerfileInstructions(t) {
		if f[0] == "COPY" && f[len(f)-1] == "/tmp" {
			t.Errorf("Dockerfile copies a /tmp into the image: %q", strings.Join(f, " "))
		}
	}
}
