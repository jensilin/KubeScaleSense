// Command normalizer is the demonstration workload for KubeScaleSense.
//
// It normalizes PM records over HTTP, with a configurable CPU cost per record
// and a bounded number of processing slots, so that a rising input rate
// produces genuine queueing and genuine CPU load. It is the Deployment
// KubeScaleSense observes and — from P3 — scales.
//
// It is a separate program from the controller on purpose and shares no
// configuration, no packages, and no assumptions with it.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/normalizer"
)

// version is set at build time with -ldflags.
var version = "dev"

func main() {
	code := run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	os.Exit(code)
}

// run is main with its dependencies injected, so the startup and shutdown paths
// are testable without spawning a process.
func run(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	opts, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, errFlagHelp) {
			return 0
		}
		printf(stderr, "normalizer: %v\n", err)
		return 2
	}

	if opts.showVersion {
		printf(stdout, "%s\n", version)
		return 0
	}

	cfg, err := normalizer.LoadConfig(getenv)
	if err != nil {
		// Configuration errors go to stderr as plain text, not as a log record:
		// this happens before the logger's format is even settled, and a
		// crash-looping pod's first line should be readable by a human reading
		// `kubectl logs`.
		printf(stderr, "normalizer: %v\n", err)
		return 2
	}

	log := slog.New(slog.NewJSONHandler(stdout, nil)).With(slog.String("app", "normalizer"))

	if opts.validateOnly {
		log.Info("configuration is valid", configAttrs(cfg)...)
		return 0
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := serve(ctx, log, cfg); err != nil {
		log.Error("normalizer stopped", slog.Any("error", err))
		return 1
	}
	return 0
}

func serve(ctx context.Context, log *slog.Logger, cfg normalizer.Config) error {
	metrics := normalizer.NewMetrics()

	server, err := normalizer.NewServer(cfg, log, metrics, time.Now)
	if err != nil {
		return err
	}

	data := &http.Server{
		Addr:              cfg.Addr,
		Handler:           server.DataHandler(),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
	admin := &http.Server{
		Addr:              cfg.AdminAddr,
		Handler:           server.AdminHandler(),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}

	// A request may legitimately occupy a slot for QueueTimeout plus the
	// processing time, so there is deliberately no per-request write timeout
	// here: it would cut off exactly the slow requests the queue exists to
	// absorb. The bound on a request's lifetime is the queue timeout, which is
	// enforced where the waiting happens.

	failed := make(chan error, 2)
	for _, srv := range []*http.Server{data, admin} {
		go func(srv *http.Server) {
			log.Info("listening", slog.String("addr", srv.Addr))
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				failed <- fmt.Errorf("serving %s: %w", srv.Addr, err)
				return
			}
			failed <- nil
		}(srv)
	}

	log.Info("normalizer started", append(configAttrs(cfg), slog.String("version", version))...)

	select {
	case err := <-failed:
		// A bind failure is fatal and immediate. A Normalizer that is running
		// but not listening would pass its liveness probe while silently
		// serving nothing.
		if err != nil {
			return err
		}
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	return shutdown(log, server, cfg, data, admin)
}

// shutdown implements the documented drain order: fail readiness, let the
// Service withdraw the pod, then stop accepting and finish what is in flight
// (WR-06, D-05).
func shutdown(log *slog.Logger, server *normalizer.Server, cfg normalizer.Config, data, admin *http.Server) error {
	budget, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	server.Drain(budget)

	// The data plane closes first and the admin plane last, so /metrics and
	// /healthz keep answering while in-flight records finish. A drain that
	// cannot be observed is a drain nobody will trust.
	var firstErr error
	if err := data.Shutdown(budget); err != nil {
		firstErr = fmt.Errorf("draining the data plane: %w", err)
		log.Warn("the drain did not finish within the budget; in-flight records were cut off",
			slog.String("shutdown_timeout", cfg.ShutdownTimeout.String()),
			slog.Int("in_flight", server.InFlight()),
			slog.Int64("queued", server.Queued()),
			slog.Any("error", err))
	}
	if err := admin.Shutdown(budget); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("stopping the admin plane: %w", err)
	}

	log.Info("shutdown complete")
	return firstErr
}

// printf writes a user-facing line to the console. The write error is dropped
// deliberately, as in the controller: if stderr is unwritable there is nowhere
// left to report the fact.
func printf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func configAttrs(cfg normalizer.Config) []any {
	return []any{
		slog.String("addr", cfg.Addr),
		slog.String("admin_addr", cfg.AdminAddr),
		slog.Int("cost_rounds", cfg.CostRounds),
		slog.String("processing_delay", cfg.ProcessingDelay.String()),
		slog.Int("max_concurrent", cfg.MaxConcurrent),
		slog.Int("queue_limit", cfg.QueueLimit),
		slog.String("queue_timeout", cfg.QueueTimeout.String()),
		slog.String("drain_delay", cfg.DrainDelay.String()),
		slog.String("shutdown_timeout", cfg.ShutdownTimeout.String()),
	}
}
