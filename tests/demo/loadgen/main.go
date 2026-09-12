// Command loadgen drives the demonstration pipeline's input.
//
// It is not part of KubeScaleSense and the controller has no knowledge of it.
// It exists to make demand deterministic: a spike whose record count, rate,
// burst size, and payload size are all stated, so that a scenario can be run
// twice and compared.
//
// Two modes:
//
//	-mode=file  writes input files for NiFi's ListFile, which is the full
//	            pipeline: files → NiFi → HTTP → Normalizer.
//	-mode=http  posts records straight at normalizer-service, which measures
//	            the workload with NiFi removed from the path. This is the mode
//	            DI-08 uses, because a flat throughput curve must not be
//	            attributable to the client's concurrency (FS-28).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

func parseFloat(raw string) (float64, error) { return strconv.ParseFloat(raw, 64) }
func parseInt(raw string) (int, error)       { return strconv.Atoi(raw) }

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type settings struct {
	mode        string
	dir         string
	url         string
	serve       string
	concurrency int
	timeout     time.Duration
	plan        Plan
}

func run(args []string, stdout, stderr io.Writer) int {
	cfg, err := parse(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		printf(stderr, "loadgen: %v\n", err)
		return 2
	}

	log := slog.New(slog.NewJSONHandler(stdout, nil)).With(slog.String("app", "loadgen"))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Serve mode keeps the process alive so it can sit as a sidecar next to
	// NiFi and be driven on demand. The demo needs a quiet baseline before its
	// first spike, so a generator that started producing at startup would
	// remove step 1 of the narrative.
	if cfg.serve != "" {
		return serveControl(ctx, log, cfg)
	}

	stats := &Stats{}
	target, err := newSink(cfg, stats)
	if err != nil {
		printf(stderr, "loadgen: %v\n", err)
		return 1
	}

	start := time.Now()
	runErr := Run(ctx, log, cfg.plan, target, stats)
	closeErr := target.close()

	log.Info("run finished",
		slog.Int64("records", stats.Records.Load()),
		slog.Int64("files", stats.Files.Load()),
		slog.Int64("failures", stats.Failures.Load()),
		slog.String("elapsed", time.Since(start).Round(time.Millisecond).String()))

	if runErr != nil {
		printf(stderr, "loadgen: %v\n", runErr)
		return 1
	}
	if closeErr != nil {
		printf(stderr, "loadgen: %v\n", closeErr)
		return 1
	}
	return 0
}

// printf writes one line to a console or a response. The write error is
// dropped deliberately: for stderr there is nowhere left to report it, and for
// a control-endpoint response a failed write is already visible to the caller.
func printf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func newSink(cfg settings, stats *Stats) (sink, error) {
	switch cfg.mode {
	case "file":
		return newFileSink(cfg.dir, cfg.plan, stats)
	case "http":
		return newHTTPSink(cfg.url, cfg.plan, cfg.concurrency, cfg.timeout, stats), nil
	default:
		return nil, fmt.Errorf("mode %q must be \"file\" or \"http\"", cfg.mode)
	}
}

func parse(args []string, stderr io.Writer) (settings, error) {
	cfg := settings{plan: DefaultPlan()}

	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(stderr)

	fs.StringVar(&cfg.mode, "mode", "http", `"file" to write input files for NiFi, "http" to post records directly`)
	fs.StringVar(&cfg.dir, "dir", "/data/input", "input directory, for -mode=file")
	fs.StringVar(&cfg.url, "url", "http://normalizer-service.data-pipeline.svc/normalize",
		"normalize endpoint, for -mode=http")
	fs.StringVar(&cfg.serve, "serve", "", "stay alive on this address and run on request, instead of running once")
	fs.IntVar(&cfg.concurrency, "concurrency", 16,
		"in-flight requests, for -mode=http; hold this above maxReplicas or throughput cannot scale (A-13)")
	fs.DurationVar(&cfg.timeout, "timeout", 30*time.Second, "per-request timeout, for -mode=http")

	fs.StringVar(&cfg.plan.Profile, "profile", cfg.plan.Profile,
		`"constant" or "low-high-low"`)
	fs.Float64Var(&cfg.plan.LowRate, "rate", cfg.plan.LowRate,
		"records per second; the baseline rate under -profile=low-high-low")
	fs.Float64Var(&cfg.plan.HighRate, "high-rate", cfg.plan.HighRate,
		"records per second during the spike, under -profile=low-high-low")
	fs.DurationVar(&cfg.plan.Phase, "phase", cfg.plan.Phase, "duration of each phase under -profile=low-high-low")
	fs.IntVar(&cfg.plan.Records, "records", cfg.plan.Records, "stop after this many records; 0 means run until stopped")
	fs.IntVar(&cfg.plan.Burst, "burst", cfg.plan.Burst, "records sent back to back per tick; bursts are what make a queue form")
	fs.IntVar(&cfg.plan.Pad, "pad", cfg.plan.Pad, "extra bytes per record, for payload-size sensitivity")
	fs.IntVar(&cfg.plan.RecordsPerFile, "records-per-file", cfg.plan.RecordsPerFile, "records per input file, for -mode=file")

	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}
	if cfg.mode != "file" && cfg.mode != "http" {
		return cfg, fmt.Errorf("mode %q must be \"file\" or \"http\"", cfg.mode)
	}
	if cfg.concurrency < 1 {
		return cfg, fmt.Errorf("concurrency must be at least 1, got %d", cfg.concurrency)
	}
	if cfg.timeout <= 0 {
		return cfg, fmt.Errorf("timeout must be greater than 0, got %s", cfg.timeout)
	}
	return cfg, cfg.plan.Validate()
}

// serveControl exposes the minimum needed to drive a scenario: start a run,
// see what it did. Deliberately not an API — one POST and one GET, no
// authentication, and reachable only inside the cluster.
func serveControl(ctx context.Context, log *slog.Logger, cfg settings) int {
	stats := &Stats{}
	// Atomic because the run goroutine writes it while a /status request may be
	// reading it.
	var running atomic.Bool

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		printf(w, "ok\n")
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		printf(w, "running=%t records=%d files=%d failures=%d\n",
			running.Load(), stats.Records.Load(), stats.Files.Load(), stats.Failures.Load())
	})
	mux.HandleFunc("POST /spike", func(w http.ResponseWriter, r *http.Request) {
		plan, err := planFromQuery(cfg.plan, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		target, err := newSink(cfg, stats)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		running.Store(true)
		go func() {
			defer running.Store(false)
			if err := Run(ctx, log, plan, target, stats); err != nil {
				log.Error("run failed", slog.Any("error", err))
			}
			if err := target.close(); err != nil {
				log.Error("closing the sink failed", slog.Any("error", err))
			}
		}()

		printf(w, "started: %+v\n", plan)
	})

	server := &http.Server{Addr: cfg.serve, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	failed := make(chan error, 1)
	go func() {
		log.Info("control endpoint listening", slog.String("addr", cfg.serve))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			failed <- err
			return
		}
		failed <- nil
	}()

	select {
	case err := <-failed:
		if err != nil {
			log.Error("the control endpoint failed", slog.Any("error", err))
			return 1
		}
	case <-ctx.Done():
	}

	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		log.Warn("the control endpoint did not stop cleanly", slog.Any("error", err))
	}
	return 0
}

// planFromQuery overrides the baked-in plan from query parameters, so a
// scenario can be driven with curl without redeploying the sidecar.
func planFromQuery(base Plan, r *http.Request) (Plan, error) {
	plan := base
	query := r.URL.Query()

	floats := map[string]*float64{"rate": &plan.LowRate, "high-rate": &plan.HighRate}
	for name, target := range floats {
		raw := query.Get(name)
		if raw == "" {
			continue
		}
		parsed, err := parseFloat(raw)
		if err != nil {
			return plan, fmt.Errorf("%s=%q is not a number", name, raw)
		}
		*target = parsed
	}

	ints := map[string]*int{
		"records":          &plan.Records,
		"burst":            &plan.Burst,
		"pad":              &plan.Pad,
		"records-per-file": &plan.RecordsPerFile,
	}
	for name, target := range ints {
		raw := query.Get(name)
		if raw == "" {
			continue
		}
		parsed, err := parseInt(raw)
		if err != nil {
			return plan, fmt.Errorf("%s=%q is not an integer", name, raw)
		}
		*target = parsed
	}

	if raw := query.Get("profile"); raw != "" {
		plan.Profile = raw
	}
	if raw := query.Get("phase"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return plan, fmt.Errorf("phase=%q is not a duration", raw)
		}
		plan.Phase = parsed
	}

	return plan, plan.Validate()
}
