package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cplieger/envx"
	"github.com/cplieger/health"
	"github.com/cplieger/knell/internal/config"
	"github.com/cplieger/knell/internal/notify"
	"github.com/cplieger/knell/internal/watch"
	"github.com/cplieger/slogx/capture"
	"github.com/cplieger/webhttp"
)

// testWebhookSecret is the credential half of the webhook every boot test
// configures, and testWebhookURL is the URL that carries it -- the string every
// leak assertion in this package greps for. They are ONE declaration on
// purpose: a URL edited in one boot helper while an assertion kept the old
// token leaves a secret-leak guard that still passes and can never fail again.
const (
	testWebhookSecret = "verysecrettoken"
	testWebhookURL    = "https://discord.example/api/webhooks/1234567890/" + testWebhookSecret
	// testPlainHTTPWebhookURL is the same webhook over the one scheme the
	// config gate must refuse, so the rejection test cannot drift off the
	// secret above.
	testPlainHTTPWebhookURL = "http://discord.example/api/webhooks/1234567890/" + testWebhookSecret
	// testBeatToken is the required beat credential every boot test configures.
	// It clears config's minTokenLength floor, so a boot test fails on the gate
	// it means to exercise rather than on a too-short token.
	testBeatToken = "unit-test-beat-token"
)

// secretEnvKeys are the variables a boot test must CLEAR rather than blank:
// config.Load rejects a PRESENT-but-empty _FILE variable, and one leaked from
// another test (or the ambient environment) changes what run() reads. One home,
// so a newly added _FILE gate is cleared everywhere at once.
var secretEnvKeys = []string{"DISCORD_WEBHOOK_URL_FILE", "BEAT_TOKEN", "BEAT_TOKEN_FILE"}

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

// installRunEnv installs a complete environment config.Load accepts: the beats
// spec, the listen address, the webhook (whose token half every leak assertion
// greps for), and the _FILE and token variables explicitly CLEARED, since one
// leaked from another test changes what run() reads. It is the single home of
// "a configuration run() gets past", so a newly required variable cannot leave
// one boot test failing at the config gate while its sibling still reaches the
// gate it means to exercise.
func installRunEnv(t *testing.T, beats, addr string) {
	t.Helper()
	t.Setenv("BEATS", beats)
	t.Setenv("DISCORD_WEBHOOK_URL", testWebhookURL)
	for _, key := range secretEnvKeys {
		unsetEnv(t, key)
	}
	// After the clearing sweep, because BEAT_TOKEN is in it: the token is
	// REQUIRED, so a boot test that cleared it would fail at the configuration
	// gate instead of reaching the one it means to exercise.
	t.Setenv("BEAT_TOKEN", testBeatToken)
	t.Setenv("LISTEN_ADDR", addr)
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

// TestWatchBeatsCarriesEveryIDAndDeadlineIntoTheStateMachine pins the one
// translation main owns between parsed configuration and the state machine.
// The watch tests build their own []watch.Beat and the config tests stop at
// []config.Beat, so a field dropped or crossed HERE is invisible to both: a
// lost deadline arms every beat at zero and fires a false missing notice on
// every restart, and a lost id makes the configured beat 404 forever.
func TestWatchBeatsCarriesEveryIDAndDeadlineIntoTheStateMachine(t *testing.T) {
	t.Parallel()

	got := watchBeats([]config.Beat{
		{ID: "api", Deadline: 20 * time.Minute},
		{ID: "cron-backup", Deadline: 26 * time.Hour},
	})
	want := []watch.Beat{
		{ID: "api", Deadline: 20 * time.Minute},
		{ID: "cron-backup", Deadline: 26 * time.Hour},
	}
	if !slices.Equal(got, want) {
		t.Errorf("watchBeats = %v, want %v: a dropped deadline arms every beat at zero, so the first freshness refresh reports it overdue and the next sweep fires a false missing notice on every restart; a dropped id makes the beat unpingable and 404 forever", got, want)
	}
	if beats := watchBeats(nil); len(beats) != 0 {
		t.Errorf("watchBeats(nil) = %v, want no beats", beats)
	}
}

func TestLogConfigNeverLeaksWebhookURL(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global
	// slog default to inspect the startup summary.
	cfg := config.Config{
		WebhookURL:    testWebhookURL,
		WebhookSource: envx.SourceEnv,
		Node:          "node-1",
		ListenAddr:    ":9190",
		Beats:         []config.Beat{{ID: "api", Deadline: 20 * time.Minute}},
	}

	rec := capture.Default(t)
	logConfig(&cfg)

	if !rec.Contains("configuration loaded") {
		t.Fatalf("messages = %v, want the startup summary", rec.Messages())
	}
	// The channel that supplied the credential, never the credential: "env"
	// means it is also in the process environment and in `docker inspect`
	// output, which is the one thing about a required webhook worth publishing.
	if !rec.HasAttr("configuration loaded", "webhook", "env") {
		t.Error(`webhook attr must name the credential's source channel ("env" for the plain variable)`)
	}
	if rec.Contains(testWebhookSecret) || rec.AttrContains("", "", testWebhookSecret) {
		t.Errorf("startup log leaks the webhook URL: %v", rec.Messages())
	}
	// Every attribute config.LogValue publishes must reach the shipped line:
	// the hand-picked copy this call site used to build had already dropped
	// log_level, so an operator diagnosing a level that "did not apply" had
	// nothing in the configuration line to confirm it against.
	if !rec.HasAttr("configuration loaded", "log_level", "INFO") {
		t.Errorf("startup summary omits the effective log_level: %v", rec.Messages())
	}
	if !rec.HasAttr("configuration loaded", "beats", "1") {
		t.Errorf("startup summary omits the beat count: %v", rec.Messages())
	}
	// The Host allowlist's OFF state has to reach the shipped line, because it
	// is the one knell is otherwise silent about: a misspelled ALLOWED_HOSTS
	// reads as unset, draws no parse warning, and leaves the DNS-rebinding guard
	// off while the operator believes it is armed. cfg carries no policy here,
	// which is also the nil-safety check for the attr.
	if !rec.HasAttr("configuration loaded", "allowed_hosts", "any") {
		t.Errorf("allowed_hosts should report any when no allowlist is configured: %v", rec.Messages())
	}
	if !rec.Contains("watching beat") || !rec.AttrContains("watching beat", "beat", "api") {
		t.Errorf("per-beat startup line missing: %v", rec.Messages())
	}
}

// TestLogConfigNeverPublishesTheBeatToken pins the startup summary's silence
// about the credential. BEAT_TOKEN is required, so a presence attr could only
// ever read "required" and would report no state at all — and the summary is the
// one line that renders a whole Config, so it is where a rendering of the value
// itself would surface. Neither the token nor an attr naming it may appear.
func TestLogConfigNeverPublishesTheBeatToken(t *testing.T) {
	// Serial (no t.Parallel): swaps the process-global slog default.
	cfg := config.Config{
		WebhookURL: "https://discord.example/hook",
		Node:       "node-1",
		ListenAddr: ":9190",
		BeatToken:  testBeatToken,
		Beats:      []config.Beat{{ID: "api", Deadline: 20 * time.Minute}},
	}

	rec := capture.Default(t)
	logConfig(&cfg)

	if rec.Contains(testBeatToken) || rec.AttrContains("", "", testBeatToken) {
		t.Errorf("startup log leaks the beat token: %v", rec.Messages())
	}
	if _, reported := rec.AttrValue("configuration loaded", "beat_auth"); reported {
		t.Errorf("startup summary carries a beat_auth attr: %v; the token is required, so the attr reports no state and only gives a future edit a place to render the value", rec.Records())
	}
}

// TestRunEnforcesTheNodeNameCapNotifyOwns pins the second value main mediates
// between two internal packages: notify owns the NODE_NAME bound (it renders
// every template the Discord budget is measured over), config ENFORCES
// whatever bound it is handed, and main is the only place the two meet.
// notify's budget test proves the templates fit at MaxNodeNameBytes and the
// config tests alias the same constant into their own local copy, so neither
// can see what the composition root actually passes: a larger bound admits a
// node name whose notices exceed Discord's content limit, where the switch
// arms and every notice is refused with 400 -- the exact failure the constant
// exists to prevent, and one that is invisible until an outage.
//
// The listen address is an OCCUPIED one so a bound passed too LARGE fails this
// test instead of hanging it: the boot would then clear the configuration gate
// and go on to serve, and the bind is the next thing run() does.
func TestRunEnforcesTheNodeNameCapNotifyOwns(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, plus run() installs a process-global
	// slog default of its own.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind a probe listener: %v", err)
	}
	t.Cleanup(func() { _ = occupied.Close() })

	installRunEnv(t, "api:20m", occupied.Addr().String())
	t.Setenv("NODE_NAME", strings.Repeat("n", notify.MaxNodeNameBytes+1))
	capture.Default(t)

	err = run()
	if err == nil {
		t.Fatal("run() = nil, want a NODE_NAME over notify's cap to fail the boot; a name past the cap renders notices over Discord's content limit, so the switch arms and every notice is refused")
	}
	if !strings.Contains(err.Error(), "configuration") || !strings.Contains(err.Error(), "NODE_NAME") {
		t.Fatalf("run() = %v, want the configuration gate to reject an over-cap NODE_NAME; a composition root passing a LARGER bound than the render budget was measured against admits a name whose notices Discord refuses", err)
	}
	if want := strconv.Itoa(notify.MaxNodeNameBytes); !strings.Contains(err.Error(), want) {
		t.Errorf("run() = %v, want the enforced cap %s named: config enforces whatever main passes, so a different constant here silently decouples the cap from the render budget it protects", err, want)
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

	installRunEnv(t, "api:20m", occupied.Addr().String())
	capture.Default(t)

	err = run()
	if err == nil {
		t.Fatal("run() = nil, want a bind error; an address already in use must fail the boot instead of leaving the watcher alerting behind a listener nothing can reach")
	}
	if !strings.Contains(err.Error(), "binding") {
		t.Fatalf("run() = %v, want the bind failure surfaced to the caller", err)
	}
	if strings.Contains(err.Error(), testWebhookSecret) {
		t.Errorf("bind error leaks the webhook URL: %v", err)
	}
}

// TestClassifyBindErrorSeparatesAPreBindStopFromAPortConflict pins the boot
// path a container stopped during startup takes. A signal that arrives before
// the bind cancels the Listen itself, so the resulting error IS the shutdown:
// reported as a bind failure it would exit 1 (a restart loop) and name an
// address that was never the problem. The inverse matters just as much: a real
// port conflict must still surface, or knell idles as a dead switch behind a
// listener nothing can reach — including when it coincides with a signal, which
// is why the classification turns on webhttp.CausedByCancellation PROVING the
// error is the cancellation rather than on the context merely being cancelled.
func TestClassifyBindErrorSeparatesAPreBindStopFromAPortConflict(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default.
	rec := capture.Default(t)

	bindFailure := errors.New("listen tcp :9190: bind: address already in use")

	got := classifyBindError(t.Context(), ":9190", bindFailure)
	if got == nil {
		t.Fatal("classifyBindError(live ctx) = nil, want the bind failure surfaced so the boot fails instead of running as a switch nothing can reach")
	}
	if !errors.Is(got, bindFailure) {
		t.Errorf("classifyBindError = %v, want the bind failure still unwrappable", got)
	}
	if !strings.Contains(got.Error(), ":9190") {
		t.Errorf("classifyBindError = %q, want the address named so the operator knows which port to free", got)
	}
	if rec.Contains("shutting down before the listener was bound") {
		t.Errorf("a port conflict reported itself as a clean stop: %v", rec.Messages())
	}

	stopped, cancel := context.WithCancel(context.Background())
	cancel()

	// A bind the signal itself cancelled: the error carries the cancellation, so
	// this is the clean stop.
	cancelledListen := fmt.Errorf("listen tcp :9190: %w", context.Canceled)
	if got := classifyBindError(stopped, ":9190", cancelledListen); got != nil {
		t.Errorf("classifyBindError(cancelled listen) = %v, want nil: a signal arriving before the bind is a clean stop, not a boot failure", got)
	}
	if !rec.Contains("shutting down before the listener was bound") {
		t.Errorf("messages = %v, want the pre-bind stop reported; otherwise a container stopped mid-boot leaves no trace of why it never served", rec.Messages())
	}

	// A port conflict that merely COINCIDED with the signal is still a port
	// conflict: the cancelled context is not evidence about this error, and
	// swallowing it would exit 0 having never bound the listener a quorum
	// depends on.
	if got := classifyBindError(stopped, ":9190", bindFailure); got == nil {
		t.Error("classifyBindError(cancelled ctx, genuine bind failure) = nil, want the failure surfaced: a cancelled context is not evidence that THIS error was the shutdown")
	}
}

// TestClassifyServeErrorNamesADrainThatOutlivedTheGrace pins the one ERROR
// line a container emits when its drain runs out of budget. webhttp.Run wraps
// that outcome in ErrShutdownGraceExpired, which is the only thing that
// identifies WHICH deadline expired; classifyServeError is what turns it into a
// line naming the drain and the grace constant that bounds it. If the branch
// were dropped, a container SIGKILLed mid-drain would exit 1 with "context
// deadline exceeded" and the operator would have no way to tell a stuck drain
// from any other expired context; if the classification keyed on a bare
// context.DeadlineExceeded instead, a deadline of the caller's own making would
// be reported as a drain overrun that never happened.
func TestClassifyServeErrorNamesADrainThatOutlivedTheGrace(t *testing.T) {
	t.Parallel()

	serveFailure := errors.New("accept tcp [::]:9190: use of closed network connection")
	// The shape webhttp.Run returns for a grace expiry: the origin sentinel
	// wrapping the deadline that produced it.
	graceExpired := fmt.Errorf("%w: %w", webhttp.ErrShutdownGraceExpired, context.DeadlineExceeded)
	tests := map[string]struct {
		in        error
		wantNamed bool
	}{
		"clean shutdown":                {in: nil, wantNamed: false},
		"grace expiry":                  {in: graceExpired, wantNamed: true},
		"wrapped grace expiry":          {in: fmt.Errorf("shutting down: %w", graceExpired), wantNamed: true},
		"deadline of another origin":    {in: context.DeadlineExceeded, wantNamed: false},
		"serve failure":                 {in: serveFailure, wantNamed: false},
		"cancellation, not a grace run": {in: context.Canceled, wantNamed: false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := classifyServeError(tt.in)
			if !tt.wantNamed {
				if got != tt.in {
					t.Errorf("classifyServeError(%v) = %v, want it returned unchanged: only a webhttp-reported grace expiry may be renamed, or an accept failure or a caller's own deadline is reported as a shutdown that never happened", tt.in, got)
				}
				return
			}
			if got == nil {
				t.Fatalf("classifyServeError(%v) = nil, want the drain overrun surfaced so the process exits 1", tt.in)
			}
			if !errors.Is(got, context.DeadlineExceeded) {
				t.Errorf("classifyServeError(%v) = %v, want the deadline still unwrappable", tt.in, got)
			}
			if !errors.Is(got, webhttp.ErrShutdownGraceExpired) {
				t.Errorf("classifyServeError(%v) = %v, want the grace-expiry origin still unwrappable", tt.in, got)
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
