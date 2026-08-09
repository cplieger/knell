package main

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// TestDockerfile_bakes_tmp_marker_mode pins the /tmp mode bake the health
// marker depends on. A plain COPY recreates the destination directory 0755, and
// engines that replicate the IMAGE dir's mode onto a tmpfs mount (Docker 24 on
// DSM) then leave /tmp unwritable for USER 65534: health enters degraded mode,
// where Set is a no-op and the probe still exits 0, so the container reports
// healthy with a dead marker channel. Nothing else catches a dropped --chmod -
// the build succeeds and the image-smoke run mounts its own mode=1777 tmpfs -
// so this assertion is the guard.
func TestDockerfile_bakes_tmp_marker_mode(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile("Dockerfile")
	if err != nil {
		t.Fatalf("read Dockerfile: %v", err)
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "COPY" || fields[len(fields)-1] != "/tmp" {
			continue
		}
		if !slices.Contains(fields, "--chmod=1777") {
			t.Fatalf("COPY into /tmp must bake mode 1777, got: %q", strings.TrimSpace(line))
		}
		return
	}
	t.Fatal("Dockerfile has no COPY instruction targeting /tmp: the health marker directory must be created with --chmod=1777")
}
