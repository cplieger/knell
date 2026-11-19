package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/go/envx/v2"
	"github.com/cplieger/go/httpserver/v2"
	"github.com/cplieger/go/masked"
	"github.com/cplieger/go/slogx/capture"
	"github.com/cplieger/knell/internal/config"
	"github.com/cplieger/knell/internal/notify"
	"github.com/cplieger/knell/internal/watch"
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
var secretEnvKeys = []string{"DISCORD_WEBHOOK_URL_FILE", "BEAT_TOKENS", "BEAT_TOKENS_FILE", "BEAT_TOKEN", "BEAT_TOKEN_FILE"}

// beatTokensFor is a BEAT_TOKENS giving token every beat in a BEATS spec.
func beatTokensFor(beats, token string) string {
	var ids []string
	for entry := range strings.SplitSeq(beats, ",") {
		if id, _, ok := strings.Cut(strings.TrimSpace(entry), ":"); ok {
			ids = append(ids, strings.TrimSpace(id))
		}
	}
	return strings.Join(ids, "+") + ":" + token
}

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
// spec, the two listen addresses (ops on any free loopback port), the webhook (whose token half every leak assertion
// greps for), and the _FILE and token variables explicitly CLEARED, since one
// leaked from another test changes what run() reads. It is the single home of
// "a configuration run() gets past", so a newly required variable cannot leave
// one boot test failing at the config gate while its sibling still reaches the
// gate it means to exercise.
func installRunEnv(t *testing.T, beats, addr string) {
	t.Helper()
	installRunEnvOps(t, beats, addr, "127.0.0.1:0")
}

// installRunEnvOps is installRunEnv with the operations address chosen by the
// caller.
func installRunEnvOps(t *testing.T, beats, addr, opsAddr string) {
	t.Helper()
	t.Setenv("BEATS", beats)
	t.Setenv("DISCORD_WEBHOOK_URL", testWebhookURL)
	for _, key := range secretEnvKeys {
		unsetEnv(t, key)
	}
	// After the clearing sweep, because BEAT_TOKENS is in it: the tokens are
	// REQUIRED, so a boot test that cleared them would fail at the
	// configuration gate instead of reaching the one it means to exercise.
	t.Setenv("BEAT_TOKENS", beatTokensFor(beats, testBeatToken))
	t.Setenv("LISTEN_ADDR", addr)
	t.Setenv("OPS_LISTEN_ADDR", opsAddr)
}

// TestWatchBeatsCarriesEveryFieldIntoTheStateMachine pins the one
// translation main owns between parsed configuration and the state machine.
// The watch tests build their own []watch.Beat and the config tests stop at
// config.Config, so a field dropped or crossed HERE is invisible to both: a
// lost deadline arms every beat at zero and fires a false missing notice on
// every restart, a lost id makes the configured beat 404 forever, and a lost
// repeat interval silently turns MISSING_REPEAT off.
func TestWatchBeatsCarriesEveryFieldIntoTheStateMachine(t *testing.T) {
	t.Parallel()

	got := watchBeats([]config.Beat{
		{ID: "api", Deadline: 20 * time.Minute},
		{ID: "cron-backup", Deadline: 26 * time.Hour},
	}, 6*time.Hour)
	want := []watch.Beat{
		{ID: "api", Deadline: 20 * time.Minute, Repeat: 6 * time.Hour},
		{ID: "cron-backup", Deadline: 26 * time.Hour, Repeat: 6 * time.Hour},
	}
	if !slices.Equal(got, want) {
		t.Errorf("watchBeats = %v, want %v: a dropped deadline arms every beat at zero, so the first freshness refresh reports it overdue and the next sweep fires a false missing notice on every restart; a dropped id makes the beat unpingable and 404 forever; a dropped repeat sends one notice per outage whatever MISSING_REPEAT says", got, want)
	}
	if beats := watchBeats(nil, 0); len(beats) != 0 {
		t.Errorf("watchBeats(nil) = %v, want no beats", beats)
	}
}

func TestLogConfigNeverLeaksWebhookURL(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global
	// slog default to inspect the startup summary.
	cfg := config.Config{
		WebhookURL:    masked.NewSecret(testWebhookURL),
		WebhookSource: envx.SecretSourceEnv,
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
	// Every attribute config.LogValue publishes must reach the shipped line, or
	// an operator diagnosing a level that "did not apply" has nothing in the
	// configuration line to confirm it against.
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
// about the credential. BEAT_TOKENS is required, so a presence attr could only
// ever read "required" and would report no state at all — and the summary is the
// one line that renders a whole Config, so it is where a rendering of the value
// itself would surface. The token value and a redundant beat_auth presence attr
// must not appear; the beat_tokens attr reports only its source channel.
func TestLogConfigNeverPublishesTheBeatToken(t *testing.T) {
	// Serial (no t.Parallel): swaps the process-global slog default.
	cfg := config.Config{
		WebhookURL: masked.NewSecret("https://discord.example/hook"),
		Node:       "node-1",
		ListenAddr: ":9190",
		Beats:      []config.Beat{{ID: "api", Deadline: 20 * time.Minute}},
	}
	tokens, err := httpserver.NewTokenSet([]string{testBeatToken})
	if err != nil {
		t.Fatal(err)
	}
	cfg.BeatTokens = config.BeatTokens{Set: tokens, Covers: [][]string{{"api"}}}

	rec := capture.Default(t)
	logConfig(&cfg)

	if rec.Contains(testBeatToken) || rec.AttrContains("", "", testBeatToken) {
		t.Errorf("startup log leaks the beat token: %v", rec.Messages())
	}
	if _, reported := rec.AttrValue("configuration loaded", "beat_auth"); reported {
		t.Errorf("startup summary carries a beat_auth attr: %v; the token is required, so the attr reports no state and only gives a future edit a place to render the value", rec.Records())
	}
}

// TestRunEnforcesTheNodeNameCapNotifyOwns pins the bound main passes from
// notify, which owns the NODE_NAME limit its templates fit Discord under, to
// config, which enforces it; neither package sees what main passes. A larger
// bound admits a name whose notices Discord refuses with 400. The listen
// address is occupied, so a too-large bound fails at the bind instead of
// serving.
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

	err = run(nil)
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
	// slog default.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind a probe listener: %v", err)
	}
	t.Cleanup(func() { _ = occupied.Close() })

	installRunEnv(t, "api:20m", occupied.Addr().String())
	capture.Default(t)

	err = run(nil)
	if err == nil {
		t.Fatal("run() = nil, want a bind error; an address already in use must fail the boot instead of leaving the watcher alerting behind a listener nothing can reach")
	}
	if !strings.Contains(err.Error(), "LISTEN_ADDR \""+occupied.Addr().String()+"\"") {
		t.Fatalf("run() = %v, want the bind failure surfaced to the caller, naming LISTEN_ADDR", err)
	}
	if strings.Contains(err.Error(), testWebhookSecret) {
		t.Errorf("bind error leaks the webhook URL: %v", err)
	}
}

// TestRunFailsFastWhenTheOpsListenAddressIsAlreadyBound pins that a held
// operations port fails the boot naming OPS_LISTEN_ADDR, instead of serving
// pings with no /healthz or /metrics anywhere.
func TestRunFailsFastWhenTheOpsListenAddressIsAlreadyBound(t *testing.T) {
	// Serial (no t.Parallel): t.Setenv, plus run() installs a process-global
	// slog default.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot bind a probe listener: %v", err)
	}
	t.Cleanup(func() { _ = occupied.Close() })

	installRunEnvOps(t, "api:20m", "127.0.0.1:0", occupied.Addr().String())
	capture.Default(t)

	err = run(nil)
	if err == nil {
		t.Fatal("run() = nil, want a bind error for the operations port")
	}
	if !strings.Contains(err.Error(), "OPS_LISTEN_ADDR \""+occupied.Addr().String()+"\"") {
		t.Fatalf("run() = %v, want the bind failure naming OPS_LISTEN_ADDR and its address", err)
	}
}

// TestClassifyBindErrorSeparatesAPreBindStopFromAPortConflict pins the boot of a
// container stopped during startup: a signal before the bind cancels Listen, so
// the error is the shutdown, and reporting it as a bind failure would exit 1
// into a restart loop. A real port conflict must still surface, even beside a
// signal, which is why httpserver.CausedByCancellation must prove the error is
// the cancellation.
func TestClassifyBindErrorSeparatesAPreBindStopFromAPortConflict(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default.
	rec := capture.Default(t)

	bindFailure := errors.New("listen tcp :9190: bind: address already in use")

	got := classifyBindError(t.Context(), bindFailure)
	if got == nil {
		t.Fatal("classifyBindError(live ctx) = nil, want the bind failure surfaced so the boot fails instead of running as a switch nothing can reach")
	}
	if !errors.Is(got, bindFailure) {
		t.Errorf("classifyBindError = %v, want the bind failure still unwrappable", got)
	}
	if rec.Contains("shutting down before the listener was bound") {
		t.Errorf("a port conflict reported itself as a clean stop: %v", rec.Messages())
	}

	stopped, cancel := context.WithCancel(context.Background())
	cancel()

	// A bind the signal itself cancelled: the error carries the cancellation, so
	// this is the clean stop.
	cancelledListen := fmt.Errorf("listen tcp :9190: %w", context.Canceled)
	if got := classifyBindError(stopped, cancelledListen); got != nil {
		t.Errorf("classifyBindError(cancelled listen) = %v, want nil: a signal arriving before the bind is a clean stop, not a boot failure", got)
	}
	if !rec.Contains("shutting down before the listener was bound") {
		t.Errorf("messages = %v, want the pre-bind stop reported; otherwise a container stopped mid-boot leaves no trace of why it never served", rec.Messages())
	}

	// A port conflict that merely COINCIDED with the signal is still a port
	// conflict: the cancelled context is not evidence about this error, and
	// swallowing it would exit 0 having never bound the listener a quorum
	// depends on.
	if got := classifyBindError(stopped, bindFailure); got == nil {
		t.Error("classifyBindError(cancelled ctx, genuine bind failure) = nil, want the failure surfaced: a cancelled context is not evidence that THIS error was the shutdown")
	}
}

// TestClassifyServeErrorNamesADrainThatOutlivedTheGrace pins the ERROR line a
// drain out of budget emits: httpserver.Run's ErrShutdownGraceExpired is the only
// mark of which deadline expired, and classifyServeError names the drain and its
// grace constant. Dropped, a SIGKILLed drain reads as a bare "context deadline
// exceeded"; keyed on context.DeadlineExceeded instead, a caller's own deadline
// would be reported as a drain overrun.
func TestClassifyServeErrorNamesADrainThatOutlivedTheGrace(t *testing.T) {
	t.Parallel()

	serveFailure := errors.New("accept tcp [::]:9190: use of closed network connection")
	// The shape httpserver.Run returns for a grace expiry: the origin sentinel
	// wrapping the deadline that produced it.
	graceExpired := fmt.Errorf("%w: %w", httpserver.ErrShutdownGraceExpired, context.DeadlineExceeded)
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
					t.Errorf("classifyServeError(%v) = %v, want it returned unchanged: only an httpserver-reported grace expiry may be renamed, or an accept failure or a caller's own deadline is reported as a shutdown that never happened", tt.in, got)
				}
				return
			}
			if got == nil {
				t.Fatalf("classifyServeError(%v) = nil, want the drain overrun surfaced so the process exits 1", tt.in)
			}
			if !errors.Is(got, context.DeadlineExceeded) {
				t.Errorf("classifyServeError(%v) = %v, want the deadline still unwrappable", tt.in, got)
			}
			if !errors.Is(got, httpserver.ErrShutdownGraceExpired) {
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

// TestLogConfigWarnsWhileEveryHostIsServed pins the startup WARN a wide
// posture owes: ALLOWED_HOSTS unset answers every Host, so the summary says so
// at WARN, and a set allowlist draws no warning.
func TestLogConfigWarnsWhileEveryHostIsServed(t *testing.T) {
	// Serial (no t.Parallel): swaps the process-global slog default.
	const warning = "ALLOWED_HOSTS is unset, so knell answers every Host; set it to the names your jobs, probes and scraper use"
	rec := capture.Default(t)
	logConfig(&config.Config{})
	if got := rec.CountLevel(slog.LevelWarn, warning); got != 1 {
		t.Errorf("warnings with ALLOWED_HOSTS unset = %d, want 1", got)
	}
	policy, invalid := httpserver.ParseHostList([]string{"knell.example"})
	if len(invalid) > 0 {
		t.Fatal(invalid)
	}
	rec = capture.Default(t)
	logConfig(&config.Config{AllowedHosts: policy})
	if got := rec.CountExact(warning); got != 0 {
		t.Errorf("warnings with ALLOWED_HOSTS set = %d, want 0", got)
	}
}

// TestTrustWindowIsTwoOfTheLongestDeadlines pins how long a sender stays
// exempt from the global failure window: twice the longest deadline, so a
// beat's own sender stays trusted between its pings.
func TestTrustWindowIsTwoOfTheLongestDeadlines(t *testing.T) {
	t.Parallel()
	got := trustWindow([]config.Beat{{ID: "a", Deadline: 20 * time.Minute}, {ID: "b", Deadline: 26 * time.Hour}})
	if got != 52*time.Hour {
		t.Errorf("trustWindow = %s, want 52h", got)
	}
}
