// Package main is the knell entry point: a dead-man switch that listens for
// heartbeat pings and rings a Discord webhook when a beat falls silent.
// main.go is the composition root plus what only the process boundary owns:
// the `knell health` probe dispatch, the health marker lifecycle, and
// shutdown/exit-code classification; everything else lives in internal/*.
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
	"github.com/cplieger/webhttp/v2"
)

// shutdownGrace bounds the whole stop sequence: pre-drain, request drain and
// watch-loop teardown share this one budget. 8s rather than Docker's default
// 10s stop timeout, so the "watch loop still running" WARN has time to flush
// before SIGKILL.
const shutdownGrace = 8 * time.Second

// The three request bounds must each fit inside shutdownGrace with margin for
// teardown: webhttp spends one grace budget across pre-drain, srv.Shutdown and
// onShutdown, so the worst-case active request (2s header + 3s read = 5s)
// leaves at least 3s of the grace for awaitWatchLoop.
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
			// slog, not fmt: a level-less line matches none of the Loki rules.
			slogx.Setup(slogx.Options{})
			slog.Error("unknown command", "command", os.Args[1], "supported", "health")
			os.Exit(2)
		}
	}

	if err := run(); err != nil {
		slog.Error("knell exited with error", "error", err)
		os.Exit(1)
	}
	// Logged before the drain, so a clean stop and a SIGKILLed one leave
	// identical logs without this line.
	slog.Info("stopped")
}

// run wires the app and blocks until a shutdown signal or a serve error.
// It returns nil on a clean signal-driven shutdown.
func run() error {
	// The boot-armed clock's baseline is process start: marker probing and
	// mounted-secret reads delay the point watch.New is reached, and every
	// beat's first deadline must count from here.
	processStart := time.Now()

	// Install the handler before config parsing so config warnings render
	// through the configured handler; apply the parsed level after.
	levelVar := slogx.Setup(slogx.Options{})

	// Clear any stale marker before the first exit path: config.Load fails
	// fast, and a marker left on the tmpfs by the previous process would
	// report a crash-looping container healthy for the whole restart loop.
	marker := health.NewMarker(health.DefaultPath)
	// Cleanup, not Set(false): Set treats the first call as a health
	// transition and would WARN on every boot for a state that is only
	// "not booted yet".
	marker.Cleanup()
	defer marker.Cleanup()

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

	// The marker flip rides the pre-drain phase, not onShutdown: one budget
	// covers pre-drain -> Shutdown -> onShutdown, so a later flip would let
	// the `knell health` probe call a draining container healthy.
	preDrain := webhttp.WithPreDrain(func(context.Context) {
		// Close beat admission first: a ping answered 200 after cancellation
		// is a heartbeat with no sender behind it.
		watcher.StopAccepting()
		beginTeardown(marker, "shutting down")
	})
	// Both teardown paths report whether the watch loop actually finished, so
	// the exit cannot claim a clean stop over an abandoned loop.
	var watchLoopStopped bool
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
		// Load-bearing for the shutdown-grace invariant: while headers are
		// being read net/http has armed only the header deadline, so
		// webhttp's 10s default would be a live slowloris window otherwise.
		webhttp.WithReadHeaderTimeout(requestHeaderTimeout),
		webhttp.WithReadTimeout(requestReadTimeout),
		webhttp.WithWriteTimeout(requestWriteTimeout),
		webhttp.WithMaxHeaderBytes(config.MaxRequestHeaderBytes),
		webhttp.WithSlogErrorLog(slog.LevelError))
}

// teardownAfterServeExit runs the teardown for the non-graceful exit where
// Serve returns before a signal: webhttp skips the drain hooks there, so this
// is the only place that closes beat admission, marks the process unhealthy,
// drains accepted handlers, cancels the watcher and waits for it. stop() runs
// before srv.Shutdown so the loop tallies concurrently rather than behind the
// drain.
func teardownAfterServeExit(exitCtx context.Context, srv *http.Server, marker *health.Marker, closeAdmission func(), stop context.CancelFunc, watcherDone <-chan struct{}) bool {
	closeAdmission()
	beginTeardown(marker, "serve loop exited, tearing down")
	stop()
	if err := srv.Shutdown(exitCtx); err != nil {
		slog.Warn("accepted requests were still in flight at the end of the shutdown grace, so a handler's own loss diagnostic may never have been logged",
			"grace", shutdownGrace.String(), "error", err, "retryable", false)
	}
	return awaitWatchLoop(exitCtx, watcherDone)
}

// classifyBindError turns a failed listener bind into run's own contract. A
// signal that arrives before the bind cancels the Listen itself, so
// CausedByCancellation distinguishes that from a genuine bind failure (a port
// conflict coinciding with a SIGTERM is still a port conflict).
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
// the error names the grace constant instead of a bare expired context.
func classifyServeError(err error) error {
	if errors.Is(err, webhttp.ErrShutdownGraceExpired) {
		return fmt.Errorf("the shutdown sequence outlived the %s shutdown grace (in-flight requests still draining, or a stalled pre-drain hook): %w", shutdownGrace, err)
	}
	return err
}

// classifyAbandonedWatchLoop turns a teardown that ran out of budget into
// run's own contract: an abandoned loop never logged its shutdown tally, so
// returning nil here would print "stopped" on top of awaitWatchLoop's WARN.
func classifyAbandonedWatchLoop(err error, watchLoopStopped bool) error {
	if err != nil || watchLoopStopped {
		return err
	}
	return fmt.Errorf("the watch loop was still running at the end of the %s shutdown grace, so the notices it still held were abandoned without its shutdown tally being logged", shutdownGrace)
}

// beginTeardown marks the process unhealthy and then announces the teardown.
// Order is load-bearing: a blocked container log driver can stall inside
// slog.Info, so flipping first makes the `knell health` probe fail closed
// even if the announcement then blocks.
func beginTeardown(marker *health.Marker, msg string) {
	marker.Set(false)
	slog.Info(msg)
}

// awaitWatchLoop blocks until the watch loop has finished, or until the
// teardown budget runs out.
//
// AwaitDone, not a two-case select: teardownCtx carries the same deadline
// srv.Shutdown just spent, so an expired context would make a select report a
// stopped loop as hung about half the time.
func awaitWatchLoop(teardownCtx context.Context, watcherDone <-chan struct{}) bool {
	start := time.Now()
	if webhttp.AwaitDone(teardownCtx, watcherDone) {
		return true
	}
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
	slog.LogAttrs(context.Background(), slog.LevelInfo, "configuration loaded", cfg.LogValue().Group()...)
}
