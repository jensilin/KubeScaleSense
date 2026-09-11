// Command kubescalesense is the KubeScaleSense controller entrypoint.
//
// Phase 0 scope: load configuration, validate it, initialise structured
// logging, handle SIGINT/SIGTERM, and shut down cleanly. There is deliberately
// no Kubernetes client, no metrics collection, and no reconcile loop — the
// phase order in docs/implementation-plan.md front-loads the resource
// estimator and keeps actuation until P3, so this binary is the scaffold those
// phases are built on and nothing more.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/config"
)

// AppName is the canonical application name used in logs and metrics.
const AppName = "kubescalesense"

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

// shutdownTimeout bounds the graceful stop. Nothing in Phase 0 needs draining,
// so this budget is currently never spent; it exists because the informers,
// listeners, and leader-election lease added in P1 and P4 will need it, and
// retrofitting a shutdown deadline is how a controller ends up being SIGKILLed
// mid-write.
const shutdownTimeout = 15 * time.Second

// Exit codes are distinct so that a crash-looping pod can be diagnosed from
// `kubectl describe` without reading logs. Configuration failures are the
// common case (FS-19) and are worth their own code.
const (
	exitOK            = 0
	exitConfigInvalid = 1
	exitRuntimeError  = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run holds everything main does, with I/O and arguments injected so that the
// startup and shutdown paths are testable without spawning a process.
func run(args []string, stdout, stderr io.Writer) int {
	opts, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		printf(stderr, "%s: %v\n", AppName, err)
		return exitConfigInvalid
	}

	switch {
	case opts.showVersion:
		printf(stdout, "%s %s\n", AppName, version)
		return exitOK
	case opts.printEnv:
		printEnv(stdout)
		return exitOK
	}

	cfg, src, err := config.Load(opts.configPath)
	if err != nil {
		// Deliberately plain text on stderr rather than a structured log line:
		// the logger is configured *by* the config that just failed to load,
		// and a human is reading this in `kubectl logs` of a CrashLoopBackOff.
		printf(stderr, "%s: %v\n", AppName, err)
		return exitConfigInvalid
	}

	log := cfg.NewLogger(stdout).With(slog.String("app", AppName))

	if opts.validateOnly {
		log.Info("configuration is valid",
			slog.String("version", version),
			slog.String("config_source", src.String()),
		)
		return exitOK
	}

	logStartup(log, cfg, src)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := serve(ctx, log); err != nil {
		log.Error("shutdown failed", slog.Any("error", err))
		return exitRuntimeError
	}
	return exitOK
}

type options struct {
	configPath   string
	showVersion  bool
	printEnv     bool
	validateOnly bool
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	var opts options

	fs := flag.NewFlagSet(AppName, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&opts.configPath, "config", "", "path to the YAML configuration file; defaults and KSS_* overrides apply when empty")
	fs.BoolVar(&opts.showVersion, "version", false, "print the version and exit")
	fs.BoolVar(&opts.printEnv, "print-env", false, "print the recognised KSS_* environment overrides and exit")
	fs.BoolVar(&opts.validateOnly, "validate", false, "validate the configuration and exit without starting")

	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if fs.NArg() > 0 {
		return opts, fmt.Errorf("unexpected argument %q; this command takes flags only", fs.Arg(0))
	}
	return opts, nil
}

// printf writes a user-facing line to the console. The write error is
// deliberately dropped at every call site: if stdout or stderr is unwritable
// there is nowhere left to report the fact, and the alternative is six
// unchecked-error suppressions saying the same thing.
func printf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func printEnv(w io.Writer) {
	printf(w, "Recognised %s configuration overrides:\n", AppName)
	keys := config.Keys()
	for _, key := range keys {
		printf(w, "  %-44s %s\n", config.EnvVarFor(key), key)
	}
	printf(w, "\n%d override(s). Values set to the empty string are ignored.\n", len(keys))
}

// logStartup emits the facts needed to reconstruct what this process is doing,
// per the startup logging requirement. No configuration *values* beyond these
// are logged: the config holds no credentials by design (CR-5), but logging a
// whole struct is a habit that stops being safe the moment one is added.
func logStartup(log *slog.Logger, cfg *config.Config, src config.Source) {
	log.Info("KubeScaleSense foundation started",
		slog.String("version", version),
		slog.String("config_source", src.String()),
		slog.Bool("dry_run", cfg.Controller.DryRun),
		slog.String("phase", "P0"),
	)

	log.Info("target configured, not yet observed",
		slog.String("namespace", cfg.Target.Namespace),
		slog.String("deployment", cfg.Target.Deployment),
		slog.Int("min_replicas", int(cfg.Target.MinReplicas)),
		slog.Int("max_replicas", int(cfg.Target.MaxReplicas)),
		slog.String("signal_source", cfg.Workload.Signal.Source),
	)

	if len(src.EnvOverrides) > 0 {
		log.Info("environment overrides applied", slog.Any("variables", src.EnvOverrides))
	}

	// Said once, loudly, so that nobody reading these logs concludes the
	// controller is scaling anything.
	log.Warn("no scaling is performed in this phase",
		slog.String("detail", "Phase 0 provides configuration, logging, and lifecycle only; observation arrives in P1 and actuation in P3"),
	)
}

// serve blocks until the context is cancelled by SIGINT or SIGTERM, then
// performs a bounded shutdown.
//
// Phase 0 runs no background goroutines at all, which is the reason this
// function is as short as it is: there is genuinely nothing to supervise yet,
// and inventing a worker pool to demonstrate lifecycle handling would be
// scaffolding that later phases would have to unpick.
func serve(ctx context.Context, log *slog.Logger) error {
	log.Info("waiting for shutdown signal", slog.String("signals", "SIGINT, SIGTERM"))

	<-ctx.Done()

	// Rendered as a string rather than with slog.Duration, which JSON-encodes a
	// duration as a raw nanosecond count that nobody reads correctly at a
	// glance.
	log.Info("shutdown signal received, stopping",
		slog.String("timeout", shutdownTimeout.String()),
	)

	// The shutdown context is derived from Background, not from ctx: ctx is
	// already cancelled by the time we get here, so deriving from it would
	// leave every shutdown step with an expired deadline.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := shutdown(shutdownCtx); err != nil {
		return err
	}

	log.Info("shutdown complete")
	return nil
}

// shutdown releases resources in reverse order of acquisition. Phase 0 acquires
// none, so this is a no-op that returns immediately rather than waiting out the
// timeout.
func shutdown(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("shutdown budget already expired: %w", err)
	}
	return nil
}
