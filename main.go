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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cplieger/health"
	"github.com/cplieger/knell/internal/config"
	"github.com/cplieger/knell/internal/metrics"
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
			// Structured and level-carrying, on the same stderr stream every other
			// knell line uses. A mistyped container command is a boot failure that
			// crash-loops under `restart: unless-stopped` while publishing no
			// metrics at all, so this line is the only trace of the cause -- and a
			// bare fmt line carries no level, so the Loki rules that match every
			// other knell failure cannot match it. Installing the handler here is
			// safe because this branch exits: run() installs its own, and the two
			// paths never both run.
			slogx.Setup(slogx.Options{})
			slog.Error("unknown command", "command", os.Args[1], "supported", "health")
			os.Exit(2)
		}
	}

	if err := run(); err != nil {
		slog.Error("knell exited with error", "error", err)
		os.Exit(1)
	}
	// Last line of a clean stop, emitted once run's defers have flushed the
	// marker, the notifier and the watch-loop teardown. "shutting down" is
	// logged BEFORE the drain, so without this line a stop that finished under
	// its own power and one killed at the container stop timeout leave identical
	// logs, and the exit code lives in Docker's inspect state rather than in the
	// stream Loki keeps.
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
	// Cleanup, not Set(false): both remove the marker (and both are no-ops in
	// degraded mode), but Set treats the first call as a health TRANSITION and
	// logs a WARN on every boot for a state that is only 'not booted yet'.
	// Cleanup logs only when the remove itself fails, which is the outcome
	// worth a warn here.
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

	// The app context goes into webapi so beat ACCEPTANCE closes at the same
	// instant the watch loop stops: watcher.Run returns on ctx.Done() after
	// snapshotting its undelivered work, while the HTTP surface stays live for
	// up to the shutdown grace below. A ping accepted in that window would be
	// recorded behind a sender that no longer exists — the recovered notice it
	// queues has no reader left — and would make the snapshot stale after the
	// fact. Gating on the shared ctx rather than on the pre-drain hook keeps
	// that deterministic: pre-drain and Run's exit race each other, ctx
	// cancellation is one instant both see.
	handler := webapi.New(ctx, watcher, cfg.BeatToken, webapi.Routes{
		Healthz: health.Handler(marker),
		Metrics: metrics.Handler(),
		Hosts:   cfg.AllowedHosts,
	})
	srv := webhttp.NewServer(handler,
		webhttp.WithReadTimeout(requestTimeout),
		webhttp.WithWriteTimeout(requestTimeout),
		// Connection-level errors net/http reports itself -- above all
		// "http: Accept error: ...; retrying", the trace of an exhausted fd
		// budget that stops every beat from being received -- default to the
		// standard logger, i.e. an unstructured, level-less line in an
		// otherwise structured stderr stream. Level-based Loki rules cannot
		// match it.
		webhttp.WithErrorLog(slog.NewLogLogger(slog.Default().Handler(), slog.LevelError)))

	// Bind up front so a port-in-use error surfaces synchronously.
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return classifyBindError(ctx.Err(), cfg.ListenAddr, err)
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
	// The flip goes FIRST inside the hook: slogx installs a synchronous stderr
	// handler, so a blocked container log driver can stall inside slog.Info,
	// and webhttp cannot enforce the shutdown budget on an inline callback.
	// Flipping before logging makes the probe fail closed even if either
	// shutdown log then blocks.
	preDrain := webhttp.WithPreDrain(func(context.Context) {
		marker.Set(false)
		// No cause attribute: signal.NotifyContext cancels through a plain
		// context.WithCancel and nothing here uses context.WithCancelCause, so
		// context.Cause(ctx) can only ever render "context canceled". webhttp
		// invokes this hook only after ctx is cancelled, and on that path the
		// cancellation is always the signal, so the message carries the whole
		// trigger on its own.
		slog.Info("shutting down")
	})
	// Wait for the single sender to finish abandoning its in-flight
	// delivery, so its "abandoned, shutting down" log lines actually land
	// instead of racing process exit. Bounded by the shutdown grace: the
	// teardown context webhttp.Run passes shares that deadline.
	onShutdown := func(teardownCtx context.Context) {
		awaitWatchLoop(teardownCtx, watcherDone)
	}
	// Handle the non-graceful exit where Serve returns before a signal.
	// webhttp.Run invokes either this hook or the graceful shutdown hooks,
	// never both; teardownAfterServeExit owns what that path must do.
	serveExit := webhttp.WithServeExit(func(exitCtx context.Context) {
		teardownAfterServeExit(exitCtx, marker, stop, watcherDone)
	})
	err = webhttp.Run(ctx, srv, ln, onShutdown, webhttp.WithShutdownGrace(shutdownGrace), preDrain, serveExit)
	return classifyServeError(err)
}

// teardownAfterServeExit runs the teardown for the non-graceful exit where
// Serve returns before a signal: webhttp skips the drain hooks on that path,
// so this is the only place that marks the process unhealthy, cancels the
// watcher and waits for it, all under the one grace budget carried by exitCtx.
//
// The stop() is this path's own, not a duplicate of main's defer: the app
// context is still live here (no signal arrived), so without it awaitWatchLoop
// would wait out the whole grace for a watch loop nobody asked to stop.
func teardownAfterServeExit(exitCtx context.Context, marker *health.Marker, stop context.CancelFunc, watcherDone <-chan struct{}) {
	marker.Set(false)
	// The graceful path's "shutting down" does not run here (webhttp skips
	// pre-drain when Serve returns on its own), and the serve error itself is
	// logged only after this teardown returns. Without this line the watch
	// loop's abandoned-delivery lines are read before anything says why.
	slog.Info("serve loop exited, tearing down")
	stop()
	awaitWatchLoop(exitCtx, watcherDone)
}

// classifyBindError turns a failed listener bind into run's own contract.
//
// A signal that arrives before the bind cancels the Listen itself, so that
// error IS the shutdown, not a port conflict: report the stop and return nil
// (run's contract for a clean signal-driven shutdown) instead of an ERROR line
// naming an address that was never the problem. ctxErr is the app context's
// error at the moment of the failure, so a nil ctxErr means the bind itself
// failed and the caller must see it.
func classifyBindError(ctxErr error, addr string, err error) error {
	if ctxErr != nil {
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
// A shutdown sequence that ran and reported its own deadline means in-flight
// requests outlived the single grace budget. Name that, so the one ERROR line
// points at the drain and at the constant that bounds it instead of at an
// anonymous expired context. Only the graceful path can produce this error:
// the other one returns srv.Serve's own failure, an accept error that carries
// no deadline. Every other outcome, nil included, passes through untouched.
func classifyServeError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("the shutdown sequence outlived the %s shutdown grace (in-flight requests still draining, or a stalled pre-drain hook): %w", shutdownGrace, err)
	}
	return err
}

// awaitWatchLoop blocks until the watch loop has finished, or until the
// teardown budget runs out — at which point it says so, since the loop's
// abandoned deliveries are then the operator's only trace of the notices this
// process will never send.
func awaitWatchLoop(teardownCtx context.Context, watcherDone <-chan struct{}) {
	start := time.Now()
	select {
	case <-watcherDone:
	case <-teardownCtx.Done():
		// webhttp.Run derives teardownCtx from the SAME deadline srv.Shutdown
		// just spent, so a drain that used the whole grace hands this function an
		// already-expired context, and a select with both cases ready picks
		// pseudo-randomly. Re-check before declaring the loop still running,
		// or a watch loop that DID stop is reported as hung half the time.
		select {
		case <-watcherDone:
		default:
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
		}
	}
}

// logConfig logs the active configuration at startup. The webhook URL is a
// secret and never logged; only its presence is reported.
func logConfig(cfg *config.Config) {
	for _, b := range cfg.Beats {
		slog.Info("watching beat", "beat", b.ID, "deadline", b.Deadline.String())
	}
	// The summary's attributes come from config.Config.LogValue, the package
	// that owns which of them are secrets and how each renders. Hand-picking
	// them here published a rendering that had already drifted from it: the
	// effective LOG_LEVEL was absent, and the webhook attribute was a literal
	// rather than the presence LogValue reports. Group() spreads the SAME flat
	// attribute names the line emitted before, so nothing in the shipped stream
	// is renamed. No context exists at this call site yet — the signal context
	// is installed after configuration loads — hence Background.
	slog.LogAttrs(context.Background(), slog.LevelInfo, "configuration loaded", cfg.LogValue().Group()...)
}
