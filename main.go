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
	if len(os.Args) > 1 && os.Args[1] == "health" {
		health.RunProbe(health.DefaultPath)
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
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}
	levelVar.Set(cfg.LogLevel)
	logConfig(&cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Clear any stale marker from a previous run before declaring health.
	marker := health.NewMarker(health.DefaultPath)
	marker.Set(false)
	defer marker.Cleanup()

	notifier := notify.New(cfg.WebhookURL, cfg.Node)
	defer notifier.Close()

	watcher := watch.New(cfg.Beats, notifier, time.Now)

	handler := webapi.New(watcher, health.Handler(marker), metrics.Registry.Handler())
	srv := webhttp.NewServer(handler)

	// Bind up front so a port-in-use error surfaces synchronously.
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("binding %s: %w", cfg.ListenAddr, err)
	}
	slog.Info("listening", "addr", ln.Addr().String())
	marker.Set(true)

	go watcher.Run(ctx, watch.DefaultTick)

	onShutdown := func(context.Context) {
		slog.Info("shutting down", "cause", context.Cause(ctx))
		marker.Set(false)
	}
	return webhttp.Run(ctx, srv, ln, onShutdown, webhttp.WithShutdownGrace(10*time.Second))
}

// logConfig logs the active configuration at startup. The webhook URL is a
// secret and never logged; only its presence is reported.
func logConfig(cfg *config.Config) {
	for _, b := range cfg.Beats {
		slog.Info("watching beat", "beat", b.ID, "deadline", b.Deadline.String())
	}
	slog.Info("configuration loaded",
		"beats", len(cfg.Beats),
		"node", cfg.Node,
		"listen_addr", cfg.ListenAddr,
		"webhook", "configured")
}
