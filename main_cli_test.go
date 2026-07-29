package main

import (
	"bufio"
	"context"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

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

// childEnv builds the environment a re-executed knell child runs with: the
// parent environment, the re-exec marker, and a pinned level -- several cases
// use an INFO line ("listening") as the no-boot oracle, and an inherited
// LOG_LEVEL=warn would filter it out whether or not the child booted. Each
// entry from env is then applied: "KEY=" removes KEY from the child
// environment entirely (config.Load rejects a PRESENT-but-empty _FILE
// variable, so blanking one would fail the boot at the blank-_FILE gate
// instead of the gate under test), anything else overrides the parent's value.
// A caller's own LOG_LEVEL entry is appended after the pinned one and os/exec
// keeps the last duplicate, so an explicit per-test level still wins.
func childEnv(env []string) []string {
	out := append(os.Environ(), "KNELL_TEST_REEXEC_MAIN=1", "LOG_LEVEL=info")
	for _, e := range env {
		if key, val, _ := strings.Cut(e, "="); val == "" {
			out = slices.DeleteFunc(out, func(entry string) bool {
				return strings.HasPrefix(entry, key+"=")
			})
			continue
		}
		out = append(out, e)
	}
	return out
}

// bootEnv is one configuration knell boots from, as childEnv entries: the
// beats spec, the webhook, the listen address, and every secret variable
// CLEARED. Single home, so a newly required _FILE gate cannot leave one boot
// test failing at the config gate while its sibling reaches the gate it means
// to exercise -- the CLI half of main_test.go's installRunEnv.
func bootEnv(beats, addr string) []string {
	env := []string{
		"BEATS=" + beats,
		"DISCORD_WEBHOOK_URL=" + testWebhookURL,
		"LISTEN_ADDR=" + addr,
	}
	for _, key := range secretEnvKeys {
		env = append(env, key+"=")
	}
	return env
}

// runMain re-executes this test binary as knell with args and env, returning
// the child's exit code and its combined output. An env entry "KEY=" removes
// KEY from the child environment rather than blanking it.
func runMain(t *testing.T, env []string, args ...string) (int, string) {
	t.Helper()
	// Bounded: every case asserts the child EXITS (a probe verdict, a usage
	// error, a rejected boot). If a regression makes it boot instead, the
	// child would otherwise hold CombinedOutput open until the package-wide
	// test timeout panics; the kill turns that into this test's failure.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], args...)
	cmd.Env = childEnv(env)
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
		// Skip rather than assert on a marker this process could not clear: a
		// failed remove would otherwise be reported as a probe that did not fail
		// closed, sending the reader after a healthcheck bug that is not there.
		if err := os.Remove(health.DefaultPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Skipf("cannot clear the marker at %s: %v", health.DefaultPath, err)
		}
		code, out := runMain(t, nil, "health")
		if code != 1 {
			t.Errorf("knell health without a marker = exit %d, want 1; the healthcheck must fail closed: %s", code, out)
		}
		if strings.Contains(out, "listening") {
			t.Errorf("knell health booted the server: %s", out)
		}
	})

	t.Run("marker present", func(t *testing.T) {
		plantHealthMarker(t)
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
	if !strings.Contains(out, `level=ERROR msg="unknown command"`) {
		t.Errorf("output = %q, want the usage failure as a level-carrying STRUCTURED line: a mistyped container command crash-loops while publishing no metrics at all, so this line is the only trace of the cause, and a bare stderr print still contains the words while carrying no level for the Loki rules that match every other knell failure", out)
	}
	if !strings.Contains(out, "command=serve") {
		t.Errorf("output = %q, want the rejected command carried as an attribute; without it the line says a command was unknown but not which one, and there is no metric to identify it from", out)
	}
	if strings.Contains(out, "listening") {
		t.Errorf("an unknown subcommand booted the server: %s", out)
	}
}

// TestSignalledKnellLogsTheCleanStopAsItsLastLineAndExitsZero pins the
// graceful stop as an operator reads it: a signalled knell ends its log with
// "stopped" and exits 0. Nothing else covers this. The in-process lifecycle
// tests assert run() returns nil, but main()'s final line is what
// distinguishes a stop that finished under its own power from one SIGKILLed at
// the container stop timeout - both log "shutting down" and then go quiet -
// and the exit status is what keeps a clean stop from being read as a crash.
//
// The oracle is the LAST line, not merely its presence: a "stopped" logged
// before the teardown finished would prove nothing about the drain, and
// "watch loop stopped" already contains the word.
func TestSignalledKnellLogsTheCleanStopAsItsLastLineAndExitsZero(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0])
	// Port 0: the child picks its own port, so this test never races another
	// test for a reserved one. childEnv pins LOG_LEVEL=info, which this test's
	// oracle needs: run() applies the parsed level before the listener is bound,
	// so an inherited LOG_LEVEL=warn would silence "listening", "shutting down"
	// and "stopped" alike and report an ambient environment as a missing
	// clean-stop contract.
	cmd.Env = childEnv(bootEnv("cli-clean-stop:1m", "127.0.0.1:0"))
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start knell: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	var lines []string
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if strings.Contains(scanner.Text(), "msg=listening") {
			break
		}
	}
	if len(lines) == 0 || !strings.Contains(lines[len(lines)-1], "msg=listening") {
		t.Fatalf("knell never reported a bound listener; log:\n%s", strings.Join(lines, "\n"))
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signalling knell: %v", err)
	}
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	log := strings.Join(lines, "\n")

	if err := cmd.Wait(); err != nil {
		t.Errorf("knell after an interrupt = %v, want exit 0; a clean stop reported as a failure makes an ordinary restart look like a crash. log:\n%s", err, log)
	}
	if !strings.Contains(log, `msg="shutting down"`) {
		t.Errorf("log never announced the shutdown:\n%s", log)
	}
	if last := lines[len(lines)-1]; !strings.Contains(last, "msg=stopped") {
		t.Errorf("last log line = %q, want the clean-stop line; without it a stop that finished under its own power and one killed at the container stop timeout leave identical logs. log:\n%s", last, log)
	}
}

// TestConfiguredLogLevelReachesTheRunningProcess pins the one configuration
// field nothing else in the suite can see APPLIED. run() installs the slog
// handler before config parsing (so config warnings render through it) and
// applies the parsed level to the handler afterwards; drop that one call and
// the process keeps slogx's INFO default while the startup summary still
// reports log_level=DEBUG -- an operator who raises the level to diagnose a
// missing beat gets the summary's confirmation and none of the detail. The
// config suite pins env-to-level PARSING only, and the in-process lifecycle
// tests cannot inspect run()'s handler because run() replaces the default they
// captured, so nothing outside a child process can catch it.
//
// The oracle is the /healthz access line, which webhttp logs at DEBUG for a
// healthy probe: absent at the INFO default, present only once the parsed level
// has actually reached the handler.
func TestConfiguredLogLevelReachesTheRunningProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0])
	// Port 0: the child picks its own port, so this test never races another
	// for a reserved one, and the bound address is read back off the listening
	// line. LOG_LEVEL=debug is appended AFTER childEnv's pinned info and
	// os/exec keeps the last duplicate, so it is the variable under test.
	cmd.Env = childEnv(append(bootEnv("cli-log-level:1m", "127.0.0.1:0"), "LOG_LEVEL=debug"))
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start knell: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	var lines []string
	var addr string
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if _, rest, found := strings.Cut(scanner.Text(), "msg=listening addr="); found {
			if fields := strings.Fields(rest); len(fields) > 0 {
				addr = fields[0]
			}
			break
		}
	}
	if addr == "" {
		t.Fatalf("knell never reported a bound listener; log:\n%s", strings.Join(lines, "\n"))
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/healthz", nil)
	if err != nil {
		t.Fatalf("building the healthz request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("probe /healthz: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close /healthz body: %v", err)
	}
	// A healthy probe is what logs at DEBUG; a 4xx/5xx one logs at Warn/Error
	// and would fail the level assertion for an unrelated reason.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz on a serving switch = %d, want 200", resp.StatusCode)
	}

	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signalling knell: %v", err)
	}
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := cmd.Wait(); err != nil {
		t.Errorf("knell after an interrupt = %v, want exit 0. log:\n%s", err, strings.Join(lines, "\n"))
	}
	if !slices.ContainsFunc(lines, func(line string) bool {
		return strings.Contains(line, "level=DEBUG") && strings.Contains(line, "/healthz")
	}) {
		t.Errorf("no DEBUG line for the /healthz probe, so the parsed LOG_LEVEL never reached the handler: the level an operator turns up to diagnose an outage is reported as applied in the startup summary and silently discarded. log:\n%s",
			strings.Join(lines, "\n"))
	}
}

// TestRejectedConfigExitsOneWithoutLeakingTheWebhook pins the boot-failure
// contract end to end: a rejected configuration exits 1 (so the container
// restarts instead of idling as a dead switch), says so, and never echoes the
// webhook URL — the credential is in its path.
func TestRejectedConfigExitsOneWithoutLeakingTheWebhook(t *testing.T) {
	code, out := runMain(t, []string{
		"BEATS=api:20m",
		"DISCORD_WEBHOOK_URL=" + testPlainHTTPWebhookURL,
		"DISCORD_WEBHOOK_URL_FILE=",
		"BEAT_TOKEN=",
		"BEAT_TOKEN_FILE=",
	})
	if code != 1 {
		t.Errorf("knell with a plain-http webhook = exit %d, want 1: %s", code, out)
	}
	if !strings.Contains(out, `level=ERROR msg="knell exited with error"`) {
		t.Errorf("output = %q, want the boot failure as a level-carrying STRUCTURED line: a rejected configuration crash-loops publishing no metrics, so this line is the operator's only trace, and a bare stderr print keeps the words while losing the level a Loki rule keys on", out)
	}
	if !strings.Contains(out, "scheme must be https") {
		t.Errorf("output = %q, want the https-only webhook gate to be the rejection", out)
	}
	if strings.Contains(out, testWebhookSecret) {
		t.Errorf("the boot-failure output leaks the webhook URL: %s", out)
	}
}
