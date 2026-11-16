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

// TestDockerfileExposesTheDefaultListenPorts pins EXPOSE to the two ports
// knell binds when LISTEN_ADDR and OPS_LISTEN_ADDR are unset, so `docker run
// -P` and tools that read the image's ports find both listeners.
func TestDockerfileExposesTheDefaultListenPorts(t *testing.T) {
	t.Setenv("LISTEN_ADDR", "")
	t.Setenv("OPS_LISTEN_ADDR", "")
	var want []string
	for _, addr := range []string{config.ListenAddr(), config.OpsListenAddr()} {
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("default address %q: %v", addr, err)
		}
		want = append(want, port)
	}
	for _, f := range dockerfileInstructions(t) {
		if f[0] == "EXPOSE" {
			if !slices.Equal(f[1:], want) {
				t.Errorf("EXPOSE lists %v, want the default ping and operations ports %v", f[1:], want)
			}
			return
		}
	}
	t.Errorf("Dockerfile has no EXPOSE, want EXPOSE %v", want)
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
