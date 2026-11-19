package obs

import (
	"flag"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	expositionGolden = "testdata/exposition.golden"
	// expositionChild, as the first argument after the test flags, names
	// the child scrape; an argument, so no ambient environment value can turn
	// a test run into a child that skips the golden compare.
	expositionChild = "knell-obs-exposition-child"
	expositionRegen = "regenerate with UPDATE_GOLDEN=1 go test ./internal/obs/ -run '^TestExpositionMatchesGolden$'"
)

// TestExpositionMatchesGolden pins every byte of knell's own /metrics families
// (names, HELP, TYPE, label order, seeded series, value formatting) against a
// committed golden page, so dashboards and alert rules keep reading the same
// series. The registry is a process-wide singleton that other tests in this
// package move, so the page is taken in a fresh child process of this test
// binary, from the init() cold start plus fixedState.
func TestExpositionMatchesGolden(t *testing.T) {
	if args := flag.Args(); len(args) == 2 && args[0] == expositionChild {
		path := args[1]
		fixedState()
		if err := os.WriteFile(path, []byte(ownFamilies(exposition(t))), 0o600); err != nil {
			t.Fatalf("write the child scrape: %v", err)
		}
		return
	}

	path := filepath.Join(t.TempDir(), "exposition")
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestExpositionMatchesGolden$", "-test.count=1", expositionChild, path)
	// atexit_sleep_ms=0: under -race the child would otherwise sleep a second
	// at exit.
	cmd.Env = append(os.Environ(), "GORACE=atexit_sleep_ms=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child scrape: %v\n%s", err, out)
	}
	page, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the child scrape: %v", err)
	}
	got := string(page)

	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(expositionGolden, []byte(got), 0o600); err != nil {
			t.Fatalf("write %s: %v", expositionGolden, err)
		}
		return
	}
	want, err := os.ReadFile(expositionGolden)
	if err != nil {
		t.Fatalf("read %s: %v; %s", expositionGolden, err, expositionRegen)
	}
	if got != string(want) {
		t.Errorf("/metrics differs from %s; a changed family breaks every dashboard and rule reading it. Review the change, then %s\ngot:\n%s", expositionGolden, expositionRegen, got)
	}
}

// fixedState records at least one value on every instrument through the
// exported recording functions, with fixed inputs, on top of init()'s seeds.
func fixedState() {
	start := time.Unix(1700000000, 0)
	SetBuildInfo("v1.2.3-dev.4")
	InitBeat("backup", 20*time.Minute, start)
	InitBeat("cron-nightly", 26*time.Hour+1500*time.Millisecond, start)
	SetBeatFresh("backup", true)
	SetBeatFresh("cron-nightly", false)
	RecordBeat("backup", start.Add(10*time.Minute))
	RecordBeat("backup", start.Add(25*time.Minute+500*time.Millisecond))
	RecordOutage("cron-nightly")
	RecordOutageRecordDropped("cron-nightly")
	RecordNotificationSent(KindMissing)
	RecordNotificationSent(KindRecovered)
	RecordNotificationFailed(KindHistory)
	RecordNotificationDropped(KindRecovered)
	RecordPreRouteRefusal(RefusalHostNotAllowed)
	RecordAuthFailure(AuthInvalidToken)
	RecordAuthFailure(AuthThrottled)
	RecordAuthFailure(AuthThrottled)
	RecordBeatRecordedWhileThrottled("backup")
	RecordHTTP(http.MethodPost, "/beat/{id}", http.StatusNoContent, 12*time.Millisecond)
	RecordHTTP(http.MethodGet, "/metrics", http.StatusOK, 3*time.Millisecond)
	RecordHTTP(http.MethodGet, "unmatched", http.StatusNotFound, 7500*time.Millisecond)
	RecordAcceptError()
}

// ownFamilies drops the Go and process families, whose values move between
// runs, and keeps every other line of the page in order.
func ownFamilies(page string) string {
	var b strings.Builder
	for line := range strings.Lines(page) {
		name := line
		for _, p := range []string{"# HELP ", "# TYPE "} {
			name = strings.TrimPrefix(name, p)
		}
		if strings.HasPrefix(name, "go_") || strings.HasPrefix(name, "process_") {
			continue
		}
		b.WriteString(line)
	}
	return b.String()
}
