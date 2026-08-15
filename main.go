// Package main is the knell entry point: a dead-man switch that listens for
// heartbeat pings and rings a Discord webhook when a beat falls silent.
// main.go is the composition root; all behavior lives in internal/*.
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

// shutdownGrace bounds the whole stop sequence: pre-drain, request drain and
// watch-loop teardown share this one budget.
//
// 8s, not 10s: Docker's default stop timeout is 10s, so a 10s budget puts the
// end of teardown at the same instant as SIGKILL and the "watch loop still
// running" WARN never flushes.
const shutdownGrace = 8 * time.Second

// INVARIANT for the three request bounds: an in-flight request must reach its
// own deadline INSIDE shutdownGrace, with margin left for teardown, because
// webhttp spends ONE grace budget across pre-drain, srv.Shutdown and
// onShutdown. The write deadline is armed once the headers are read while the
// read bound runs from the header bound's t0, so the worst-case active request
// is 2s + 3s = 5s and at least 3s of the grace survives for awaitWatchLoop.
// None of these may approach shutdownGrace.
const (
	requestHeaderTimeout = 2 * time.Second
	requestReadTimeout   = 3 * time.Second
	requestWriteTimeout  = 3 * time.Second
)

func main() {
	// CLI liveness probe for the Docker healthcheck (scratch image: no shell,
	// no curl). The marker is level-based boot state, so no freshness deadline.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "health":
			health.RunProbe(health.DefaultPath)
		default:
			// slog, not fmt: a mistyped container command crash-loops
			// publishing no metrics, and a level-less line matches none of the
			// Loki rules. Installing a handler is safe because this exits.
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
	// timeout leave identical logs.
	slog.Info("stopped")
}

// run wires the app and blocks until a shutdown signal or a serve error.
// It returns nil on a clean signal-driven shutdown.
func run() error {
	// The boot-armed clock's baseline is process start, not the instant wiring
	// reaches watch.New: marker probing and mounted-secret reads delay that
	// point, and every beat's first deadline must count from here.
	processStart := time.Now()

	// Install the handler before config parsing so config warnings render
	// through the configured handler; apply the parsed level after.
	levelVar := slogx.Setup(slogx.Options{})

	// Clear any stale marker BEFORE the first exit path: config.Load fails
	// fast, and a marker left on the tmpfs by the previous process would
	// report a crash-looping container healthy for the whole restart loop.
	marker := health.NewMarker(health.DefaultPath)
	// Cleanup, not Set(false): Set treats the first call as a health
	// TRANSITION and would WARN on every boot for a state that is only
	// "not booted yet".
	marker.Cleanup()
	defer marker.Cleanup()

	// The NODE_NAME cap is notify's to own and config's to enforce, so the
	// composition root mediates the value between them -- as it does for the
	// ALLOWED_HOSTS policy shape, owned by webapi and parsed by config.
	cfg, err := config.Load(notify.MaxNodeNameBytes, webapi.HostPolicyOptions()...)
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

	// The watcher is the single owner of beat admission, so the endpoint holds
	// no lifecycle state and no context of its own.
	handler := webapi.New(watcher, webapi.Deps{
		Healthz:        health.Handler(marker),
		BeatToken:      cfg.BeatToken,
		Hosts:          cfg.AllowedHosts,
		TrustedProxies: cfg.TrustedProxies,
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

	// The marker flip rides the PRE-DRAIN phase, not onShutdown: one budget
	// covers pre-drain -> Shutdown -> onShutdown, so a later flip would let
	// the `knell health` probe call a draining container healthy.
	preDrain := webhttp.WithPreDrain(func(context.Context) {
		// Close beat admission FIRST: this hook is the one place that runs
		// strictly before the HTTP drain, and a ping answered 200 after
		// cancellation is a heartbeat with no sender behind it. It precedes
		// beginTeardown because that can block on a stalled log driver.
		watcher.StopAccepting()
		beginTeardown(marker, "shutting down")
	})
	// Both teardown paths report whether the watch loop actually finished, so
	// the exit cannot claim a clean stop over an abandoned loop. Read on
	// webhttp.Run's own goroutine, so no synchronization is needed.
	watchLoopStopped := true
	onShutdown := func(teardownCtx context.Context) {
		watchLoopStopped = awaitWatchLoop(teardownCtx, watcherDone)
	}
	// The non-graceful exit where Serve returns before a signal: webhttp
	// invokes either this hook or the graceful ones, never both.
	serveExit := webhttp.WithServeExit(func(exitCtx context.Context) {
		watchLoopStopped = teardownAfterServeExit(exitCtx, srv, marker, watcher.StopAccepting, stop, watcherDone)
	})
	err = webhttp.Run(ctx, srv, ln, onShutdown, webhttp.WithShutdownGrace(shutdownGrace), preDrain, serveExit)
	return classifyAbandonedWatchLoop(classifyServeError(err), watchLoopStopped)
}

// newServer builds knell's HTTP server in one place a test can build without a
// listener. WithSlogErrorLog resolves slog.Default() as NewServer applies it,
// so this must run after run() installs the process handler.
func newServer(handler http.Handler) *http.Server {
	return webhttp.NewServer(handler,
		// LOAD-BEARING for the shutdown-grace invariant, and not redundant
		// with the read bound: while the headers are being read net/http has
		// armed ONLY the header deadline, so webhttp's 10s default is a live
		// slowloris window the 3s read bound never cuts short.
		webhttp.WithReadHeaderTimeout(requestHeaderTimeout),
		webhttp.WithReadTimeout(requestReadTimeout),
		webhttp.WithWriteTimeout(requestWriteTimeout),
		// The header ceiling is config's: it is derived from the BEAT_TOKEN
		// maximum enforced at startup, so the bound a token is checked against
		// and the bound the server reads cannot drift apart.
		webhttp.WithMaxHeaderBytes(config.MaxRequestHeaderBytes),
		// Connection-level errors net/http reports itself default to an
		// unstructured, level-less line that level-based Loki rules cannot
		// match. ERROR because knell's only job is answering pings: an accept
		// failure is a whole-service outage here.
		webhttp.WithSlogErrorLog(slog.LevelError))
}

// teardownAfterServeExit runs the teardown for the non-graceful exit where
// Serve returns before a signal: webhttp skips the drain hooks there, so this
// is the only place that closes beat admission, marks the process unhealthy,
// drains accepted handlers, cancels the watcher and waits for it, all under
// exitCtx's one grace budget. stop() is this path's own -- the app context is
// still live, so without it awaitWatchLoop would wait out the whole grace for
// a loop nobody asked to stop -- and srv.Shutdown runs after it so the loop
// tallies concurrently rather than behind the drain.
func teardownAfterServeExit(exitCtx context.Context, srv *http.Server, marker *health.Marker, closeAdmission func(), stop context.CancelFunc, watcherDone <-chan struct{}) bool {
	closeAdmission()
	// The graceful path's "shutting down" does not run here, and the serve
	// error is logged only after this returns, so without this line the watch
	// loop's abandoned-delivery lines are read before anything says why.
	beginTeardown(marker, "serve loop exited, tearing down")
	stop()
	if err := srv.Shutdown(exitCtx); err != nil {
		// retryable=false for the same reason every loss line in
		// internal/watch carries it: a handler cut off here never emitted
		// whatever loss diagnostic it owed, and nothing retries.
		slog.Warn("accepted requests were still in flight at the end of the shutdown grace, so a handler's own loss diagnostic may never have been logged",
			"grace", shutdownGrace.String(), "error", err, "retryable", false)
	}
	return awaitWatchLoop(exitCtx, watcherDone)
}

// classifyBindError turns a failed listener bind into run's own contract.
//
// A signal that arrives before the bind cancels the Listen itself, so that
// error IS the shutdown. CausedByCancellation proves that rather than assuming
// it: a port conflict coinciding with a SIGTERM is still a port conflict, and
// swallowing it would exit 0 having never bound the listener.
func classifyBindError(ctx context.Context, addr string, err error) error {
	if webhttp.CausedByCancellation(ctx, err) {
		slog.Info("shutting down before the listener was bound")
		return nil
	}
	return fmt.Errorf("binding %s: %w", addr, err)
}

// watchBeats translates the config DTOs into the watch package's own input
// type, so the state machine never depends on how configuration was parsed.
func watchBeats(cfg []config.Beat) []watch.Beat {
	beats := make([]watch.Beat, len(cfg))
	for i, b := range cfg {
		beats[i] = watch.Beat{ID: b.ID, Deadline: b.Deadline}
	}
	return beats
}

// classifyServeError turns webhttp.Run's outcome into run's own contract, so
// the one ERROR line names the drain and the constant that bounds it instead
// of a bare expired context. Every other outcome passes through untouched.
func classifyServeError(err error) error {
	if errors.Is(err, webhttp.ErrShutdownGraceExpired) {
		return fmt.Errorf("the shutdown sequence outlived the %s shutdown grace (in-flight requests still draining, or a stalled pre-drain hook): %w", shutdownGrace, err)
	}
	return err
}

// classifyAbandonedWatchLoop turns a teardown that ran out of budget into
// run's own contract: an abandoned loop never reached the arm that logs its
// shutdown tally, so returning nil would print "stopped" and exit 0 on top of
// awaitWatchLoop's WARN. An error already in hand wins.
func classifyAbandonedWatchLoop(err error, watchLoopStopped bool) error {
	if err != nil || watchLoopStopped {
		return err
	}
	return fmt.Errorf("the watch loop was still running at the end of the %s shutdown grace, so the notices it still held were abandoned without its shutdown tally being logged", shutdownGrace)
}

// beginTeardown marks the process unhealthy and then announces the teardown.
// The ORDER is load-bearing and shared by both stop paths: a blocked container
// log driver can stall inside slog.Info, so flipping first makes the `knell
// health` probe fail closed even if the announcement then blocks.
func beginTeardown(marker *health.Marker, msg string) {
	marker.Set(false)
	slog.Info(msg)
}

// awaitWatchLoop blocks until the watch loop has finished, or until the
// teardown budget runs out -- at which point it says so, since the loop's
// abandoned deliveries are the operator's only trace of the notices this
// process will never send.
//
// AwaitDone, not a two-case select: teardownCtx carries the SAME deadline
// srv.Shutdown just spent, so an expired context would make a select report a
// stopped loop as hung about half the time.
func awaitWatchLoop(teardownCtx context.Context, watcherDone <-chan struct{}) bool {
	start := time.Now()
	if webhttp.AwaitDone(teardownCtx, watcherDone) {
		return true
	}
	// Report how much of the SHARED budget this phase got: near zero says the
	// drain consumed it and the loop was never given a chance, near the grace
	// says the loop itself is wedged.
	slog.Warn("watch loop still running at the end of the shutdown grace",
		"waited", time.Since(start).Truncate(time.Millisecond).String(),
		"grace", shutdownGrace.String(),
		"retryable", false)
	return false
}

// logConfig logs the active configuration at startup. The webhook URL is a
// secret and never logged; only which channel supplied it is reported.
func logConfig(cfg *config.Config) {
	for _, b := range cfg.Beats {
		slog.Info("watching beat", "beat", b.ID, "deadline", b.Deadline.String())
	}
	// Config owns secret redaction and startup-field selection; Group spreads
	// those fields into the flat record. No signal context exists yet.
	slog.LogAttrs(context.Background(), slog.LevelInfo, "configuration loaded", cfg.LogValue().Group()...)
}
