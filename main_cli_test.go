package main

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

// reexecMain is the first argument that turns this test binary into knell
// itself. An argument, not an environment variable: `go test` never passes it,
// so an ambient value cannot replace a test run with a boot.
const reexecMain = "knell-test-reexec-main"

// TestMain re-executes the test binary as knell itself when its first
// argument is reexecMain, so main()'s argv dispatch, which ends in os.Exit on
// every branch, can be observed from a child process. The sentinel is removed
// before main() reads os.Args. Otherwise it just runs the package's tests.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == reexecMain {
		os.Args = append(os.Args[:1], os.Args[2:]...)
		main()
		return
	}
	os.Exit(m.Run())
}

// childEnv builds a re-executed knell child's environment: the parent's, with
// LOG_LEVEL pinned to info because several cases use an INFO line as the
// no-boot oracle. Each env entry is then applied: "KEY=" removes KEY entirely
// (config.Load refuses a present but empty _FILE variable, which would fail the
// wrong gate), anything else overrides. os/exec keeps the last duplicate, so a
// caller's own LOG_LEVEL still wins.
func childEnv(env []string) []string {
	// atexit_sleep_ms=0: under -race each child would otherwise sleep a
	// second at exit.
	out := append(os.Environ(), "LOG_LEVEL=info", "GORACE=atexit_sleep_ms=0")
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
	return bootEnvOps(beats, addr, "127.0.0.1:0")
}

// bootEnvOps is bootEnv with the operations address chosen by the caller.
func bootEnvOps(beats, addr, opsAddr string) []string {
	env := []string{
		"BEATS=" + beats,
		"DISCORD_WEBHOOK_URL=" + testWebhookURL,
		"LISTEN_ADDR=" + addr,
		"OPS_LISTEN_ADDR=" + opsAddr,
	}
	for _, key := range secretEnvKeys {
		env = append(env, key+"=")
	}
	// After the clearing sweep, because BEAT_TOKENS is in it: the tokens are
	// REQUIRED, so a child that cleared them exits at the configuration gate
	// instead of booting.
	env = append(env, "BEAT_TOKENS="+beatTokensFor(beats, testBeatToken))
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
	// Background, not t.Context(): cancel is registered as a Cleanup, and
	// t.Context() is already cancelled by the time Cleanups run.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], append([]string{reexecMain}, args...)...)
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

// servingKnell is one knell child that has reported its bound listeners: the
// process, the ping and operations addresses it bound, the lines read so far,
// and the scanner the caller drains after signalling it.
type servingKnell struct {
	cmd     *exec.Cmd
	scanner *bufio.Scanner
	lines   []string
	addr    string
	opsAddr string
}

// startServingKnell re-executes this test binary as knell with env and blocks
// until it logs its bound listeners, recording their addresses. Both CLI boot
// tests share it so the boot and the "msg=listening addr=" parse cannot drift
// apart and hang one of them. Port 0 keeps the child off reserved ports, and
// childEnv's LOG_LEVEL=info keeps the lines the oracles read.
func startServingKnell(t *testing.T, env []string) *servingKnell {
	t.Helper()
	// Background, not t.Context(): this child must outlive the test body so the
	// caller can signal it and drain its shutdown lines, and its teardown is
	// registered as a Cleanup, by which point t.Context() is already cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], reexecMain)
	cmd.Env = childEnv(env)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start knell: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	child := &servingKnell{cmd: cmd, scanner: bufio.NewScanner(stderr)}
	for child.scanner.Scan() {
		line := child.scanner.Text()
		child.lines = append(child.lines, line)
		if _, rest, found := strings.Cut(line, "msg=listening addr="); found {
			if fields := strings.Fields(rest); len(fields) > 1 {
				if ops, ok := strings.CutPrefix(fields[1], "ops_addr="); ok {
					child.addr, child.opsAddr = fields[0], ops
				}
			}
			break
		}
	}
	if child.addr == "" || child.opsAddr == "" {
		t.Fatalf("knell never reported a bound listener; log:\n%s", strings.Join(child.lines, "\n"))
	}
	return child
}

// TestHealthSubcommandAsksTheListenerWithoutBooting pins the Docker
// healthcheck contract: `knell health` GETs /healthz on OPS_LISTEN_ADDR's
// port, on loopback or on the one address OPS_LISTEN_ADDR names, exits with
// that verdict, and never falls through into the server. LISTEN_ADDR's port
// serves no /healthz, so a probe aimed there fails.
func TestHealthSubcommandAsksTheListenerWithoutBooting(t *testing.T) {
	t.Run("nothing listening", func(t *testing.T) {
		code, out := runMain(t, []string{"OPS_LISTEN_ADDR=" + unusedLoopbackAddr(t)}, "health")
		if code != 1 {
			t.Errorf("knell health with no listener = exit %d, want 1: %s", code, out)
		}
		if strings.Contains(out, "listening") {
			t.Errorf("knell health booted the server: %s", out)
		}
	})

	t.Run("serving knell", func(t *testing.T) {
		addr, opsAddr := unusedLoopbackAddr(t), unusedLoopbackAddr(t)
		startServingKnell(t, bootEnvOps("cli-health-probe:1m", addr, opsAddr))
		_, port, err := net.SplitHostPort(opsAddr)
		if err != nil {
			t.Fatalf("split %q: %v", opsAddr, err)
		}
		code, out := runMain(t, []string{"OPS_LISTEN_ADDR=:" + port}, "health")
		if code != 0 {
			t.Errorf("knell health against a serving knell = exit %d, want 0: %s", code, out)
		}
		_, pingPort, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("split %q: %v", addr, err)
		}
		if code, out := runMain(t, []string{"OPS_LISTEN_ADDR=:" + pingPort}, "health"); code != 1 {
			t.Errorf("knell health aimed at the ping port = exit %d, want 1: /healthz is served only on the operations port: %s", code, out)
		}
	})
}

// unusedLoopbackAddr reserves and releases a loopback port, for a child that
// must be told its address before it binds.
func unusedLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot reserve a port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("releasing the reserved port: %v", err)
	}
	return addr
}

// TestUnknownSubcommandExitsTwoWithoutBooting pins that an unrecognized argv
// is a usage error, not an accidental boot: a mistyped healthcheck command
// must not start a second switch on the same port. It exits through the
// one exit line every other failure uses, which KnellExitedWithError reads.
func TestUnknownSubcommandExitsTwoWithoutBooting(t *testing.T) {
	code, out := runMain(t, nil, "serve")
	if code != 2 {
		t.Errorf("knell serve = exit %d, want 2: %s", code, out)
	}
	if !strings.Contains(out, `level=ERROR msg="knell exited with error"`) {
		t.Errorf("output = %q, want the usage failure on the one exit line: a mistyped container command crash-loops while publishing no metrics at all, so this line is the only trace of the cause", out)
	}
	if strings.Contains(out, `msg="unknown command"`) {
		t.Errorf("output = %q, want no second exit message beside the one exit line", out)
	}
	if !strings.Contains(out, `unknown command \"serve\"`) {
		t.Errorf("output = %q, want the error to name the rejected command; there is no metric to identify it from", out)
	}
	if strings.Contains(out, "listening") {
		t.Errorf("an unknown subcommand booted the server: %s", out)
	}
}

// TestSignalledKnellLogsTheCleanStopAsItsLastLineAndExitsZero pins the graceful
// stop as an operator reads it: "stopped" as the last line and exit 0 tell a
// stop that finished apart from one killed at the container stop timeout, which
// also logs "shutting down" and goes quiet. The oracle is the last line, since
// "watch loop stopped" also contains the word.
func TestSignalledKnellLogsTheCleanStopAsItsLastLineAndExitsZero(t *testing.T) {
	child := startServingKnell(t, bootEnv("cli-clean-stop:1m", "127.0.0.1:0"))
	cmd, lines, scanner := child.cmd, child.lines, child.scanner
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

// TestConfiguredLogLevelReachesTheRunningProcess pins that run() applies the
// parsed level to the handler it installed before config parsing. Without that
// call the process stays at INFO while the startup summary reports DEBUG; the
// config suite pins parsing only and in-process tests cannot see run()'s
// handler, so a child process is the oracle, reading the /healthz access line
// httpserver logs at DEBUG.
func TestConfiguredLogLevelReachesTheRunningProcess(t *testing.T) {
	// LOG_LEVEL=debug is appended AFTER childEnv's pinned info and os/exec keeps
	// the last duplicate, so it is the variable under test.
	child := startServingKnell(t, append(bootEnv("cli-log-level:1m", "127.0.0.1:0"), "LOG_LEVEL=debug"))
	cmd, opsAddr, lines, scanner := child.cmd, child.opsAddr, child.lines, child.scanner

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+opsAddr+"/healthz", nil)
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
		"BEAT_TOKENS=",
		"BEAT_TOKENS_FILE=",
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
