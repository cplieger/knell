// Package main is the knell entry point: a dead-man switch that listens for
// heartbeat pings and rings a Discord webhook when a beat falls silent.
//
// main.go is the composition root: it wires config → notifier → watcher →
// HTTP server and drives the signal-driven lifecycle. All behavior lives in
// internal/*.
package main

import (
	"context"
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
			fmt.Fprintf(os.Stderr, "unknown command %q (the only subcommand is \"health\")\n", os.Args[1])
			os.Exit(2)
		}
	}

	if err := run(); err != nil {
		slog.Error("knell exited with error", "error", err)
		os.Exit(1)
	}
}

// run wires the app and blocks until a shutdown signal or a serve error.
// It returns nil on a clean signal-driven shutdown.
func run() error {
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
	marker.Set(false)
	defer marker.Cleanup()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}
	levelVar.Set(cfg.LogLevel)
	logConfig(&cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	notifier := notify.New(cfg.WebhookURL, cfg.Node)
	defer notifier.Close()

	// Translate the config DTO into the watch package's own input type: the
	// composition root owns the boundary, so the state machine never
	// depends on how configuration was parsed.
	beats := make([]watch.Beat, len(cfg.Beats))
	for i, b := range cfg.Beats {
		beats[i] = watch.Beat{ID: b.ID, Deadline: b.Deadline}
	}
	watcher := watch.New(beats, notifier, time.Now)

	handler := webapi.New(watcher, cfg.BeatToken, webapi.Routes{
		Healthz: health.Handler(marker),
		Metrics: metrics.Registry.Handler(),
	})
	// No route streams, so whole-request read and write bounds are safe
	// here: the read bound stops a slow-trickled body from holding a
	// handler goroutine forever (the 1 MiB drain cap bounds bytes, not
	// time), and the write bound stops a client that requests /metrics
	// and never reads the response from pinning the goroutine in Write.
	srv := webhttp.NewServer(handler,
		webhttp.WithReadTimeout(30*time.Second),
		webhttp.WithWriteTimeout(30*time.Second))

	// Bind up front so a port-in-use error surfaces synchronously.
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("binding %s: %w", cfg.ListenAddr, err)
	}
	slog.Info("listening", "addr", ln.Addr().String())
	marker.Set(true)

	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		watcher.Run(ctx, watch.DefaultTick)
	}()

	// The marker flip rides the pre-drain phase: webhttp.Run spends ONE
	// shutdown budget on pre-drain -> srv.Shutdown -> onShutdown, so a flip in
	// onShutdown lands only after the drain has finished, and the baked
	// `knell health` probe — which stats the marker FILE, something listener
	// closure does not cover — would call a draining container healthy for the
	// whole window. A serve error returns from Run without invoking either
	// hook; the deferred marker.Cleanup covers that path.
	preDrain := webhttp.WithPreDrain(func(context.Context) {
		slog.Info("shutting down", "cause", context.Cause(ctx))
		marker.Set(false)
	})
	// Wait for the single sender to finish abandoning its in-flight
	// delivery, so its "abandoned, shutting down" log lines actually land
	// instead of racing process exit. Bounded by the shutdown grace: the
	// teardown context webhttp.Run passes shares that deadline.
	onShutdown := func(teardownCtx context.Context) {
		select {
		case <-watcherDone:
		case <-teardownCtx.Done():
			slog.Warn("watch loop still running at the end of the shutdown grace")
		}
	}
	return webhttp.Run(ctx, srv, ln, onShutdown, webhttp.WithShutdownGrace(10*time.Second), preDrain)
}

// logConfig logs the active configuration at startup. The webhook URL is a
// secret and never logged; only its presence is reported.
func logConfig(cfg *config.Config) {
	for _, b := range cfg.Beats {
		slog.Info("watching beat", "beat", b.ID, "deadline", b.Deadline.String())
	}
	beatAuth := "open"
	if cfg.BeatToken != "" {
		beatAuth = "required"
	}
	slog.Info("configuration loaded",
		"beats", len(cfg.Beats),
		"node", cfg.Node,
		"listen_addr", cfg.ListenAddr,
		"webhook", "configured",
		"beat_auth", beatAuth)
}
