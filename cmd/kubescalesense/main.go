// Command kubescalesense is the KubeScaleSense controller entrypoint.
//
// It observes the cluster, computes how many more target pods it could actually
// place, collects the workload-pressure signal, decides a replica count — and,
// from P3, writes it.
//
// What the binary can do to a cluster is decided at startup and is narrow by
// construction. Observation runs on a client whose transport refuses every
// mutating method, exactly as it did in P1 and P2. Actuation, when enabled, runs
// on a second client whose transport permits two methods against one URL: the
// scale subresource of the one configured Deployment. In dry-run — still the
// default — no actuation client is built at all, so the process holds nothing
// that could write.
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
	"github.com/jensilin/KubeScaleSense/internal/controller"
	kubernetesaccess "github.com/jensilin/KubeScaleSense/internal/kubernetes"
	kssmetrics "github.com/jensilin/KubeScaleSense/internal/metrics"
	"github.com/jensilin/KubeScaleSense/internal/observability"
	"github.com/jensilin/KubeScaleSense/internal/resources"
)

// AppName is the canonical application name used in logs and metrics.
const AppName = "kubescalesense"

// Phase names the implementation phase in logs and in kss_config_info, so that
// a stray log file is self-describing about what the process could and could not
// do.
const Phase = "P3"

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

// shutdownTimeout bounds the graceful stop: draining the HTTP listeners and
// letting the reconcile loop finish its current pass.
const shutdownTimeout = 15 * time.Second

// cacheSyncTimeout bounds the initial informer list.
//
// Generous, because it covers a cold cluster-wide pod list, and fatal on expiry,
// because a controller that proceeds with empty caches would compute the fit
// capacity of an apparently empty cluster — the most dangerous possible wrong
// answer for a resource estimator.
const cacheSyncTimeout = 60 * time.Second

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

	if err := serve(ctx, log, cfg, opts); err != nil {
		log.Error("controller stopped with an error", slog.Any("error", err))
		return exitRuntimeError
	}
	return exitOK
}

type options struct {
	configPath     string
	kubeconfigPath string
	showVersion    bool
	printEnv       bool
	validateOnly   bool
	once           bool
}

func parseFlags(args []string, stderr io.Writer) (options, error) {
	var opts options

	fs := flag.NewFlagSet(AppName, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&opts.configPath, "config", "", "path to the YAML configuration file; defaults and KSS_* overrides apply when empty")
	fs.StringVar(&opts.kubeconfigPath, "kubeconfig", "", "path to a kubeconfig for observing a cluster from outside it; in-cluster credentials are used when empty")
	fs.BoolVar(&opts.showVersion, "version", false, "print the version and exit")
	fs.BoolVar(&opts.printEnv, "print-env", false, "print the recognised KSS_* environment overrides and exit")
	fs.BoolVar(&opts.validateOnly, "validate", false, "validate the configuration and exit without starting")
	fs.BoolVar(&opts.once, "once", false, "perform a single observation and decision, print it, and exit; read-only, and intended for inspecting a cluster by hand")

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

// logStartup emits the facts needed to reconstruct what this process is doing.
//
// No configuration *values* beyond these are logged: the config holds no
// credentials by design (CR-5), but logging a whole struct is a habit that stops
// being safe the moment one is added.
func logStartup(log *slog.Logger, cfg *config.Config, src config.Source) {
	log.Info("KubeScaleSense started",
		slog.String("version", version),
		slog.String("config_source", src.String()),
		slog.Bool("dry_run", cfg.Controller.DryRun),
		slog.String("phase", Phase),
	)

	log.Info("target configured",
		slog.String("namespace", cfg.Target.Namespace),
		slog.String("deployment", cfg.Target.Deployment),
		slog.Int("min_replicas", int(cfg.Target.MinReplicas)),
		slog.Int("max_replicas", int(cfg.Target.MaxReplicas)),
		slog.String("signal_source", cfg.Workload.Signal.Source),
		slog.String("interval", cfg.Controller.Interval.Duration().String()),
	)

	if len(src.EnvOverrides) > 0 {
		log.Info("environment overrides applied", slog.Any("variables", src.EnvOverrides))
	}

	// Said once, loudly, in whichever direction is true. A log file should be
	// self-describing about whether the process that wrote it could change a
	// replica count, and the reader of a stray log has no other way to tell.
	if cfg.Controller.DryRun {
		log.Warn("no scaling will be performed: this process is in dry-run",
			slog.String("detail", "the controller reports the decision it would make; no actuation client is built, "+
				"the observation client refuses mutating requests, and no replica count is ever written"),
		)
		return
	}

	log.Warn("live actuation is enabled: this process will change the replica count of the target Deployment",
		slog.String("target", cfg.Target.Namespace+"/"+cfg.Target.Deployment),
		slog.Int("bounded_between", int(cfg.Target.MinReplicas)),
		slog.Int("and", int(cfg.Target.MaxReplicas)),
		slog.String("detail", "writes go to the deployments/scale subresource only, with a resourceVersion "+
			"precondition; the pod template cannot be reached and no pod is ever deleted"),
	)
}

// serve builds the observation stack and runs it until the context is
// cancelled.
//
// The order matters: the caches must be synced before the target can be
// verified, and the target must be verified before any decision is reported,
// because a target with no resource requests makes every fit capacity
// meaningless.
func serve(ctx context.Context, log *slog.Logger, cfg *config.Config, opts options) error {
	restConfig, err := kubernetesaccess.NewReadOnlyRESTConfig(opts.kubeconfigPath)
	if err != nil {
		return fmt.Errorf("building the Kubernetes client configuration: %w", err)
	}
	clients, err := kubernetesaccess.NewClients(restConfig)
	if err != nil {
		return err
	}

	reader := kubernetesaccess.NewInformerReader(clients, cfg.Target.Namespace)

	log.Info("starting informers", slog.String("cache_sync_timeout", cacheSyncTimeout.String()))
	if err = reader.Start(ctx, cacheSyncTimeout); err != nil {
		return err
	}
	log.Info("informer caches synced")

	signalSource, err := kssmetrics.NewWorkloadSignal(cfg.Workload.Signal, time.Now)
	if err != nil {
		return fmt.Errorf("building the workload signal source: %w", err)
	}
	defer func() {
		if closeErr := signalSource.Close(); closeErr != nil {
			log.Warn("closing the workload signal source", slog.Any("error", closeErr))
		}
	}()

	resourceOpts, err := resources.OptionsFrom(cfg.Resources)
	if err != nil {
		return err
	}

	metrics := observability.NewMetrics()

	actuator, err := buildActuator(cfg, opts, metrics, log)
	if err != nil {
		return err
	}

	ctrl, err := controller.New(controller.Options{
		Config:      cfg,
		Reader:      reader,
		Signal:      signalSource,
		Utilization: kssmetrics.NewUtilizationCollector(kubernetesaccess.NewMetricsReader(clients.Metrics)),
		Resources:   resourceOpts,
		Metrics:     metrics,
		Actuator:    actuator,
		Log:         log,
	})
	if err != nil {
		return err
	}

	// The two checks config validation could not perform without a cluster.
	// Both are fatal: see controller.VerifyTarget.
	podRequest, err := ctrl.VerifyTarget(ctx)
	if err != nil {
		return err
	}
	metrics.SetConfigInfo(cfg, Phase, podRequest)
	log.Info("target verified",
		slog.Int64("pod_request_cpu_milli", podRequest.CPUMilli),
		slog.Int64("pod_request_memory_mib", podRequest.MemoryMiB()),
		slog.String("checks", "pod template declares CPU and memory requests; no HorizontalPodAutoscaler targets it"),
	)

	server := observability.NewServer(cfg, metrics, ctrl.Ready, log)
	server.Start()

	if opts.once {
		// A single observation, for inspecting a cluster by hand. Exercises the
		// entire read path and the whole decision, and writes nothing.
		if _, err := ctrl.Reconcile(ctx); err != nil {
			return fmt.Errorf("single reconcile: %w", err)
		}
		return shutdownServer(log, server)
	}

	loopDone := make(chan error, 1)
	go func() { loopDone <- ctrl.Run(ctx) }()

	log.Info("waiting for shutdown signal", slog.String("signals", "SIGINT, SIGTERM"))
	<-ctx.Done()

	if err := shutdownServer(log, server); err != nil {
		return err
	}
	return <-loopDone
}

// buildActuator selects the actuator for this process's configuration.
//
// The branch is the whole safety story of P3 and it is deliberately the only
// one: in dry-run no writable client is constructed, so the capability to
// change the cluster is absent from the process rather than present and
// unused. A flag that is checked at each write site can be checked wrongly; a
// client that was never built cannot be called at all.
//
// Note that the actuation client is separate from the observation client, and
// the observation client keeps its read-only transport in both modes. Enabling
// actuation adds a narrow write path; it does not relax the read path.
func buildActuator(
	cfg *config.Config,
	opts options,
	metrics *observability.Metrics,
	log *slog.Logger,
) (controller.Actuator, error) {
	if cfg.Controller.DryRun {
		return controller.NewDryRunActuator(log), nil
	}

	namespace, deployment := cfg.Target.Namespace, cfg.Target.Deployment

	scaleConfig, err := kubernetesaccess.NewScaleRESTConfig(opts.kubeconfigPath, namespace, deployment)
	if err != nil {
		return nil, fmt.Errorf("building the actuation client configuration: %w", err)
	}
	scaleClients, err := kubernetesaccess.NewClients(scaleConfig)
	if err != nil {
		return nil, fmt.Errorf("building the actuation client: %w", err)
	}

	actuator, err := controller.NewScaleActuator(controller.ScaleActuatorOptions{
		Target:      kubernetesaccess.NewScaleWriter(scaleClients.Core, namespace, deployment),
		MinReplicas: cfg.Target.MinReplicas,
		MaxReplicas: cfg.Target.MaxReplicas,
		// Bounded by the reconcile interval, so retries of a transient failure
		// cannot outlive the reconcile that started them and overlap the next
		// one (ADR-14).
		RetryBudget: cfg.Controller.Interval.Duration(),
		Metrics:     metrics,
		Log:         log,
	})
	if err != nil {
		return nil, err
	}

	log.Info("actuation client built",
		slog.String("writable_path", kubernetesaccess.ScalePath(namespace, deployment)),
		slog.String("retry_budget", cfg.Controller.Interval.Duration().String()),
		slog.String("detail", "the transport permits PUT and PATCH to this one path and refuses every other "+
			"mutating request, including any write to the Deployment itself"),
	)

	return actuator, nil
}

// shutdownServer stops the HTTP listeners within the shutdown budget.
func shutdownServer(log *slog.Logger, server *observability.Server) error {
	// Rendered as a string rather than with slog.Duration, which JSON-encodes a
	// duration as a raw nanosecond count that nobody reads correctly at a
	// glance.
	log.Info("shutting down", slog.String("timeout", shutdownTimeout.String()))

	// The shutdown context is derived from Background, not from the cancelled
	// serve context: deriving from it would leave every step with an expired
	// deadline.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := server.Err(); err != nil {
		return err
	}

	log.Info("shutdown complete")
	return nil
}

// shutdown checks that there is budget left to shut down in.
func shutdown(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("shutdown budget already expired: %w", err)
	}
	return nil
}

// awaitShutdown blocks until the context is cancelled and then reports whether
// there was budget left to stop cleanly.
//
// Separated from serve so that the lifecycle can be tested without a cluster:
// the cancellation and goroutine-leak tests exercise this, while serve's own
// path needs an API server.
func awaitShutdown(ctx context.Context, log *slog.Logger) error {
	<-ctx.Done()

	log.Info("shutdown signal received, stopping",
		slog.String("timeout", shutdownTimeout.String()),
	)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := shutdown(shutdownCtx); err != nil {
		return err
	}

	log.Info("shutdown complete")
	return nil
}
