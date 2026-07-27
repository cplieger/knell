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

// shutdownGrace bounds the whole stop sequence: pre-drain, the request
// drain, and the watch-loop teardown share this single budget.
//
// 8s, not 10s: Docker's default stop timeout is 10s, so a budget of 10s puts
// the end of the teardown phase at the same instant as SIGKILL and the
// "watch loop still running" WARN never flushes. 8s leaves the process room
// to finish and exit under its own power.
const shutdownGrace = 8 * time.Second

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
	watcher := watch.New(beats, notifier, time.Now, processStart)

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
	})
	// No route streams, so whole-request read and write bounds are safe
	// here: the read bound stops a slow-trickled body from holding a
	// handler goroutine forever (the 1 MiB drain cap bounds bytes, not
	// time), and the write bound stops a client that requests /metrics
	// and never reads the response from pinning the goroutine in Write.
	srv := webhttp.NewServer(handler,
		webhttp.WithReadTimeout(30*time.Second),
		webhttp.WithWriteTimeout(30*time.Second),
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
	// The flip goes FIRST inside the hook: slogx installs a synchronous
	// stderr handler, so a blocked container log driver can stall inside
	// slog.Info, and webhttp cannot enforce the shutdown budget on an inline
	// callback. Flipping before logging makes the probe fail closed even if
	// either shutdown log then blocks.
	// shutdownHooksRan separates "Run never reached the shutdown sequence"
	// (a fatal serve error) from "the sequence ran and Shutdown reported an
	// error" (a drain that outlived the grace). webhttp calls preDrain
	// inline on Run's own goroutine, and only after ctx is cancelled, so
	// this is a plain same-goroutine write.
	shutdownHooksRan := false
	preDrain := webhttp.WithPreDrain(func(context.Context) {
		shutdownHooksRan = true
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
		// Check the loop first, on its own: webhttp.Run derives teardownCtx from
		// the SAME deadline srv.Shutdown just spent, so a drain that used the
		// whole grace hands this hook an already-expired context. With both
		// cases ready a single select picks pseudo-randomly, so a watch loop
		// that DID stop would warn that it is still running half the time.
		select {
		case <-watcherDone:
			return
		default:
		}
		select {
		case <-watcherDone:
		case <-teardownCtx.Done():
			// The watcher may have closed after the preliminary check while
			// this already-expired context was competing in the select, so
			// re-check before declaring the loop still running.
			select {
			case <-watcherDone:
			default:
				slog.Warn("watch loop still running at the end of the shutdown grace")
			}
		}
	}
	err = webhttp.Run(ctx, srv, ln, onShutdown, webhttp.WithShutdownGrace(shutdownGrace), preDrain)
	if err != nil && !shutdownHooksRan {
		// A fatal serve error returns from Run without invoking either hook, so
		// the marker still reads healthy and the watch loop has not logged the
		// notices this process will never deliver -- the operator's only trace of
		// them. Flip the marker, cancel the loop and wait for it, bounded by the
		// same grace. The graceful path is excluded by shutdownHooksRan: there
		// Run already spent the single budget on pre-drain -> Shutdown ->
		// onShutdown, and a second full grace here would push the process past
		// Docker's SIGKILL - the exact failure the 8s budget exists to avoid.
		marker.Set(false)
		stop()
		teardownCtx, cancelTeardown := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancelTeardown()
		onShutdown(teardownCtx)
	}
	return err
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
