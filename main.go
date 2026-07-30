// Package main is the knell entry point: a dead-man switch that listens for
// heartbeat pings and rings a Discord webhook when a beat falls silent.
//
// main.go is the composition root: it wires config → notifier → watcher →
// HTTP server and drives the signal-driven lifecycle. All behavior lives in
// internal/*.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cplieger/health"
	"github.com/cplieger/knell/internal/config"
	"github.com/cplieger/knell/internal/notify"
	"github.com/cplieger/knell/internal/watch"
	"github.com/cplieger/knell/internal/webapi"
	"github.com/cplieger/slogx"
	"github.com/cplieger/webhttp"
)

// shutdownGrace bounds the whole stop sequence: pre-drain, the request
// drain, and the watch-loop teardown share this single budget.
//
// 8s, not 10s: Docker's default stop timeout is 10s, so a budget of 10s puts
// the end of the teardown phase at the same instant as SIGKILL and the
// "watch loop still running" WARN never flushes. 8s leaves the process room
// to finish and exit under its own power.
const shutdownGrace = 8 * time.Second

// maxHeaderBytes bounds the request line plus headers of every request,
// tightening net/http's 1 MiB default (which webhttp.NewServer restates and
// deliberately leaves to the app to choose). knell's largest legitimate
// request is POST /beat/<id> with an id capped at 64 bytes plus Host,
// Authorization, Content-Type, Content-Length and a User-Agent -- under 1 KiB
// even behind a proxy adding X-Forwarded-* -- so 16 KiB is ~16x headroom for
// every documented sender while cutting the pre-auth memory a single
// connection can make knell hold by 64x. The bound matters because it is
// charged BEFORE any gate: the failed-auth throttle, the ALLOWED_HOSTS policy
// and the beat token all run after net/http has read the headers, so on a
// memory-limited deployment (the hardened profile in the README provisions
// knell small) a handful of connections sending maximal headers can OOM the
// process -- and every restart re-arms all boot-armed deadlines, which is the
// one failure that silences the missing notice instead of raising it (see the
// README's KnellRestartChurn rule). Over-limit requests get net/http's 431.
const maxHeaderBytes = 16 << 10

// requestTimeout bounds a whole request, read and write alike. No route
// streams, so both bounds are safe: the read bound stops a slow-trickled
// body from holding a handler goroutine forever (the 1 MiB drain cap bounds
// bytes, not time), and the write bound stops a client that requests
// /metrics and never reads the response from pinning the goroutine in Write.
//
// It is LARGER than shutdownGrace, and that pairing is load-bearing: webhttp
// spends one grace budget across pre-drain, srv.Shutdown and the teardown,
// and srv.Shutdown waits for in-flight requests -- so a single request still
// inside this bound when the signal arrives can spend the whole grace, leave
// awaitWatchLoop no budget, and turn an ordinary stop into the exit
// classifyServeError names. Read the two constants together before changing
// either.
const requestTimeout = 30 * time.Second

func main() {
	// CLI liveness probe for the Docker healthcheck (scratch image: no
	// shell, no curl). The marker is level-based boot state — set once the
	// listener is bound, removed on shutdown — so no freshness deadline.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "health":
			// RunProbe exits with the probe's verdict (0 healthy, 1 not) and
			// never returns. The explicit exit keeps this case terminal: a
			// future health release that returned instead of exiting would
			// otherwise leave the switch and boot the server from a probe
			// invocation; exiting 1 instead fails the probe closed.
			health.RunProbe(health.DefaultPath)
			os.Exit(1)
		default:
			// slog, not fmt: a mistyped container command crash-loops publishing no
			// metrics, so this line is the only trace of the cause, and a level-less
			// line matches none of the Loki rules that catch every other knell
			// failure. Installing a handler here is safe because the branch exits;
			// run() installs its own.
			slogx.Setup(slogx.Options{})
			slog.Error("unknown command", "command", os.Args[1], "supported", "health")
			os.Exit(2)
		}
	}

	if err := run(); err != nil {
		slog.Error("knell exited with error", "error", err)
		os.Exit(1)
	}
	// "shutting down" is logged BEFORE the drain, so without this line a stop
	// that finished under its own power and one SIGKILLed at the container stop
	// timeout leave identical logs (the exit code lives only in Docker's inspect
	// state). Emitted after run's defers have flushed the teardown.
	slog.Info("stopped")
}

// run wires the app and blocks until a shutdown signal or a serve error.
// It returns nil on a clean signal-driven shutdown.
func run() error {
	// The boot-armed clock's baseline is process start, not the instant
	// wiring reaches watch.New: marker probing and mounted-secret reads can
	// delay that point, and every beat's first deadline must still count
	// from here (README "boot-armed clock").
	processStart := time.Now()

	// Install the handler before config parsing so config warnings render
	// through the configured handler; apply the parsed level after.
	levelVar := slogx.Setup(slogx.Options{})

	// Clear any stale marker from a previous run BEFORE the first exit path.
	// config.Load fails fast (a plain-http webhook URL, a malformed BEATS),
	// and a marker left on the tmpfs by the previous process would report a
	// crash-looping container healthy for the whole restart loop. The marker
	// depends on no configuration; it does depend on the installed handler,
	// since NewMarker warns when the marker directory is not writable — that
	// warning now renders at the default level, the parsed one not being
	// known yet.
	marker := health.NewMarker(health.DefaultPath)
	// Cleanup, not Set(false): both remove the marker, but Set treats the first
	// call as a health TRANSITION and would WARN on every boot for a state that
	// is only 'not booted yet'. Cleanup warns only if the remove fails.
	marker.Cleanup()
	defer marker.Cleanup()

	// The NODE_NAME cap is notify's to own (it renders every template the
	// budget is measured over) and config's to enforce, so the composition root
	// mediates the value between them — the same translation it performs for
	// config.Beat -> watch.Beat.
	cfg, err := config.Load(notify.MaxNodeNameBytes)
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}
	levelVar.Set(cfg.LogLevel)
	logConfig(&cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	notifier := notify.New(cfg.WebhookURL, cfg.Node)
	defer notifier.Close()

	watcher := watch.New(watchBeats(cfg.Beats), notifier, time.Now, processStart)

	// The app context goes into webapi so the beat endpoint starts REFUSING at
	// the same instant the watch loop begins stopping: watcher.Run returns on
	// ctx.Done() after snapshotting its undelivered work, while the HTTP surface
	// stays live for up to the shutdown grace below. The pre-drain hook is the
	// wrong signal for that: it runs on webhttp.Run's goroutine while Run
	// returns on its own, so the two race, and a flag one of them owns leaves
	// /beat/{id} fully live for the rest of the drain.
	//
	// This is an early refusal, NOT the admission boundary. A ping that passes
	// webapi's context check can still win the watcher mutex before Run calls
	// stopAccepting, and it is watch.Watcher.Beat that makes that safe: Beat,
	// stopAccepting and the undelivered-work snapshot serialize on the one
	// mutex, so such a ping completes before admission closes and IS counted in
	// the tally. Do not read this wiring as the guarantee and drop that
	// mutex-ordered `accepting` flag as redundant — it is what keeps a recorded
	// ping from landing behind a tally already taken.
	handler := webapi.New(ctx, watcher, webapi.Deps{
		Healthz:   health.Handler(marker),
		BeatToken: cfg.BeatToken,
		Hosts:     cfg.AllowedHosts,
	})
	srv := newServer(handler)

	// Bind up front so a port-in-use error surfaces synchronously.
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return classifyBindError(ctx, cfg.ListenAddr, err)
	}
	slog.Info("listening", "addr", ln.Addr().String())
	marker.Set(true)

	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		watcher.Run(ctx, watch.DefaultTick)
	}()

	// The marker flip rides the PRE-DRAIN phase, not onShutdown: webhttp.Run
	// spends ONE shutdown budget on pre-drain -> srv.Shutdown -> onShutdown, so
	// a flip in onShutdown lands only after the drain has finished, and the
	// baked `knell health` probe — which stats the marker FILE, something
	// listener closure does not cover — would call a draining container healthy
	// for the whole window.
	preDrain := webhttp.WithPreDrain(func(context.Context) {
		// No cause attribute: signal.NotifyContext cancels through a plain
		// context.WithCancel and nothing here uses context.WithCancelCause, so
		// context.Cause(ctx) can only ever render "context canceled". webhttp
		// invokes this hook only after ctx is cancelled, and on that path the
		// cancellation is always the signal, so the message carries the whole
		// trigger on its own.
		beginTeardown(marker, "shutting down")
	})
	// Wait for the single sender to finish abandoning its in-flight
	// delivery, so its "abandoned, shutting down" log lines actually land
	// instead of racing process exit. Bounded by the shutdown grace: the
	// teardown context webhttp.Run passes shares that deadline.
	// Both teardown paths report whether the watch loop actually finished, so
	// the exit cannot claim a clean stop over a loop abandoned while it still
	// held undelivered notices. Written and read on webhttp.Run's own calling
	// goroutine -- Run invokes both hooks inline and run blocks inside it --
	// so no synchronization is needed.
	watchLoopStopped := true
	onShutdown := func(teardownCtx context.Context) {
		watchLoopStopped = awaitWatchLoop(teardownCtx, watcherDone)
	}
	// Handle the non-graceful exit where Serve returns before a signal.
	// webhttp.Run invokes either this hook or the graceful shutdown hooks,
	// never both; teardownAfterServeExit owns what that path must do.
	serveExit := webhttp.WithServeExit(func(exitCtx context.Context) {
		watchLoopStopped = teardownAfterServeExit(exitCtx, marker, stop, watcherDone)
	})
	err = webhttp.Run(ctx, srv, ln, onShutdown, webhttp.WithShutdownGrace(shutdownGrace), preDrain, serveExit)
	return classifyAbandonedWatchLoop(classifyServeError(err), watchLoopStopped)
}

// newServer builds knell's HTTP server: the request bounds above plus the
// error-log bridge, in one place a test can build without a listener.
//
// WithSlogErrorLog resolves slog.Default() as NewServer applies it, so this
// must run after the process handler is installed -- which it does, since
// run() installs the handler before any of the wiring.
func newServer(handler http.Handler) *http.Server {
	return webhttp.NewServer(handler,
		webhttp.WithReadTimeout(requestTimeout),
		webhttp.WithWriteTimeout(requestTimeout),
		webhttp.WithMaxHeaderBytes(maxHeaderBytes),
		// Connection-level errors net/http reports itself -- above all
		// "http: Accept error: ...; retrying", the trace of an exhausted fd
		// budget that stops every beat from being received -- default to the
		// standard logger, i.e. an unstructured, level-less line in an
		// otherwise structured stderr stream. Level-based Loki rules cannot
		// match it. ERROR because knell's only job is answering pings: an
		// accept failure is a whole-service outage here, not a degradation.
		webhttp.WithSlogErrorLog(slog.LevelError))
}

// teardownAfterServeExit runs the teardown for the non-graceful exit where
// Serve returns before a signal: webhttp skips the drain hooks on that path,
// so this is the only place that marks the process unhealthy, cancels the
// watcher and waits for it, all under the one grace budget carried by exitCtx.
//
// The stop() is this path's own, not a duplicate of main's defer: the app
// context is still live here (no signal arrived), so without it awaitWatchLoop
// would wait out the whole grace for a watch loop nobody asked to stop.
func teardownAfterServeExit(exitCtx context.Context, marker *health.Marker, stop context.CancelFunc, watcherDone <-chan struct{}) bool {
	// The graceful path's "shutting down" does not run here (webhttp skips
	// pre-drain when Serve returns on its own), and the serve error itself is
	// logged only after this teardown returns. Without this line the watch
	// loop's abandoned-delivery lines are read before anything says why.
	beginTeardown(marker, "serve loop exited, tearing down")
	stop()
	return awaitWatchLoop(exitCtx, watcherDone)
}

// classifyBindError turns a failed listener bind into run's own contract.
//
// A signal that arrives before the bind cancels the Listen itself, so that
// error IS the shutdown, not a port conflict: report the stop and return nil
// (run's contract for a clean signal-driven shutdown) instead of an ERROR line
// naming an address that was never the problem.
//
// webhttp.CausedByCancellation PROVES that reading instead of assuming it: the
// error must actually carry ctx's cancellation (or its cause). A cancelled
// context alone is not evidence — a port conflict that happens to coincide with
// a SIGTERM is still a port conflict, and swallowing it would leave knell
// exiting 0 having never bound the listener a whole quorum depends on.
func classifyBindError(ctx context.Context, addr string, err error) error {
	if webhttp.CausedByCancellation(ctx, err) {
		slog.Info("shutting down before the listener was bound")
		return nil
	}
	return fmt.Errorf("binding %s: %w", addr, err)
}

// watchBeats translates the config DTOs into the watch package's own input
// type. The composition root owns this boundary, so the state machine never
// depends on how configuration was parsed.
func watchBeats(cfg []config.Beat) []watch.Beat {
	beats := make([]watch.Beat, len(cfg))
	for i, b := range cfg {
		beats[i] = watch.Beat{ID: b.ID, Deadline: b.Deadline}
	}
	return beats
}

// classifyServeError turns webhttp.Run's outcome into run's own contract.
//
// webhttp wraps the grace-expiry outcome in ErrShutdownGraceExpired, so this
// names the drain from the origin webhttp reported rather than from a bare
// context.DeadlineExceeded — a value a caller-supplied deadline produces just
// as readily. Name it, so the one ERROR line points at the drain and at the
// constant that bounds it instead of at an anonymous expired context. Every
// other outcome, nil included, passes through untouched.
func classifyServeError(err error) error {
	if errors.Is(err, webhttp.ErrShutdownGraceExpired) {
		return fmt.Errorf("the shutdown sequence outlived the %s shutdown grace (in-flight requests still draining, or a stalled pre-drain hook): %w", shutdownGrace, err)
	}
	return err
}

// classifyAbandonedWatchLoop turns a teardown that ran out of budget into
// run's own contract. awaitWatchLoop's WARN is the only trace of the notices
// an abandoned loop was still holding, and that loop never reached the
// ctx.Done arm that logs its own shutdown tally, so returning nil here would
// print "stopped" and exit 0 on top of that warning -- the clean-stop reading
// classifyServeError already denies a mere drain overrun, which loses nothing.
// An error already in hand wins: it names an earlier failure of the same
// sequence.
func classifyAbandonedWatchLoop(err error, watchLoopStopped bool) error {
	if err != nil || watchLoopStopped {
		return err
	}
	return fmt.Errorf("the watch loop was still running at the end of the %s shutdown grace, so the notices it still held were abandoned without its shutdown tally being logged", shutdownGrace)
}

// beginTeardown marks the process unhealthy and then announces the teardown.
// The ORDER is load-bearing and shared by both stop paths: slogx installs a
// synchronous stderr handler, so a blocked container log driver can stall
// inside slog.Info, and the baked `knell health` probe stats the marker FILE,
// something neither listener closure nor a stalled log covers. Flipping first
// makes the probe fail closed even if the announcement then blocks.
func beginTeardown(marker *health.Marker, msg string) {
	marker.Set(false)
	slog.Info(msg)
}

// awaitWatchLoop blocks until the watch loop has finished, or until the
// teardown budget runs out — at which point it says so, since the loop's
// abandoned deliveries are then the operator's only trace of the notices this
// process will never send.
//
// webhttp.AwaitDone, not a two-case select: teardownCtx carries the SAME
// deadline srv.Shutdown just spent, so it is routinely already expired, and a
// select with both cases ready would report a stopped loop as hung about half
// the time. AwaitDone rechecks completion after ctx fires; the logging policy
// (what, at which level, naming which constant) stays here.
func awaitWatchLoop(teardownCtx context.Context, watcherDone <-chan struct{}) bool {
	start := time.Now()
	if webhttp.AwaitDone(teardownCtx, watcherDone) {
		return true
	}
	// Report how much of the SHARED budget this phase actually got:
	// pre-drain -> srv.Shutdown -> here all spend one shutdownGrace, so a
	// waited value near zero says the drain consumed the budget and the loop
	// was never given a chance, while one near the grace says the loop
	// itself is wedged. Without both figures the line reads as an
	// accusation against the loop either way, and nothing names the
	// constant to raise — the same reason classifyServeError names it.
	slog.Warn("watch loop still running at the end of the shutdown grace",
		"waited", time.Since(start).Truncate(time.Millisecond).String(),
		"grace", shutdownGrace.String())
	return false
}

// logConfig logs the active configuration at startup. The webhook URL is a
// secret and never logged; only its presence is reported.
func logConfig(cfg *config.Config) {
	for _, b := range cfg.Beats {
		slog.Info("watching beat", "beat", b.ID, "deadline", b.Deadline.String())
	}
	// Config owns secret redaction and startup-field selection; Group spreads
	// those fields into the flat configuration record. No signal context exists
	// yet because configuration loads before signal registration.
	slog.LogAttrs(context.Background(), slog.LevelInfo, "configuration loaded", cfg.LogValue().Group()...)
}
