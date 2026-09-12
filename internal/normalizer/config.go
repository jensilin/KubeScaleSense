package normalizer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Defaults. Small enough to run several replicas on a laptop kind cluster, and
// chosen so that the shipped Deployment's requests are honest: 4 slots of
// mostly-CPU work against a 500m request is a pod that can actually saturate
// what it asked for.
const (
	DefaultAddr              = ":8080"
	DefaultAdminAddr         = ":8081"
	DefaultCostRounds        = 20000
	DefaultProcessingDelay   = 0
	DefaultMaxConcurrent     = 4
	DefaultQueueLimit        = 64
	DefaultQueueTimeout      = 10 * time.Second
	DefaultMaxBodyBytes      = 64 * 1024
	DefaultReadHeaderTimeout = 5 * time.Second
	DefaultDrainDelay        = 3 * time.Second
	DefaultShutdownTimeout   = 25 * time.Second
)

// EnvPrefix namespaces the Normalizer's environment variables.
//
// A separate prefix from the controller's KSS_ on purpose: these are two
// programs, and sharing a prefix would invite sharing a config struct, which is
// the coupling this package exists without.
const EnvPrefix = "NORMALIZER_"

// Config is the Normalizer's complete configuration.
//
// Environment variables rather than a YAML file: there are eleven settings, all
// scalars, and a ConfigMap consumed with envFrom is both less code and more
// idiomatic for a workload than a parser and a mounted volume. The controller's
// configuration is a file because it has a hundred keys and nested structure.
type Config struct {
	// Addr serves POST /normalize. Separate from AdminAddr so that a
	// NetworkPolicy can expose the data plane to NiFi and the admin plane to the
	// controller and Prometheus, without either being able to reach the other.
	Addr      string
	AdminAddr string

	// CostRounds is the number of SHA-256 rounds per record: the dial that turns
	// records into CPU time. Zero is legal and makes the service a pure
	// normalizer, which is what the fast unit tests use.
	CostRounds int

	// ProcessingDelay adds a fixed sleep per record. It is *not* the CPU dial —
	// a sleeping pod has no utilization — and exists only to place processing
	// time deliberately either side of terminationGracePeriodSeconds for the
	// graceful-shutdown boundary test (DI-05).
	ProcessingDelay time.Duration

	// MaxConcurrent is the number of records processed simultaneously per pod.
	// This is what makes queueing real: the (MaxConcurrent + 1)-th concurrent
	// request waits, and waiting requests are what the pressure signal counts.
	MaxConcurrent int

	// QueueLimit bounds the waiters. Beyond it the answer is 503, which is
	// back-pressure: NiFi keeps the record in its own durable queue and retries,
	// which is where a buffer belongs (D-02). An unbounded queue inside a pod
	// would convert a load spike into an OOM kill and lose the in-flight work.
	QueueLimit int

	// QueueTimeout is how long a record may wait for a slot before it is shed
	// with a 503. Without it, a record can sit behind a long queue until NiFi's
	// response timeout fires, which retries a request that is still queued and
	// doubles the load exactly when the pool is already saturated (FS-29).
	QueueTimeout time.Duration

	MaxBodyBytes      int64
	ReadHeaderTimeout time.Duration

	// DrainDelay is how long after SIGTERM the server keeps accepting requests
	// while already failing readiness.
	//
	// It is not politeness: endpoint removal is eventually consistent, so
	// kube-proxy on other nodes keeps sending traffic for a moment after the pod
	// is marked not-Ready. Closing the listener immediately turns that race into
	// failed requests, which is the most common cause of "scale-down dropped
	// traffic" (WR-06, FS-14).
	DrainDelay time.Duration

	// ShutdownTimeout bounds the drain of in-flight requests. It must stay below
	// the Deployment's terminationGracePeriodSeconds, or SIGKILL arrives first
	// and the drain is a fiction (A-07).
	ShutdownTimeout time.Duration
}

// DefaultConfig returns the defaults.
func DefaultConfig() Config {
	return Config{
		Addr:              DefaultAddr,
		AdminAddr:         DefaultAdminAddr,
		CostRounds:        DefaultCostRounds,
		ProcessingDelay:   DefaultProcessingDelay,
		MaxConcurrent:     DefaultMaxConcurrent,
		QueueLimit:        DefaultQueueLimit,
		QueueTimeout:      DefaultQueueTimeout,
		MaxBodyBytes:      DefaultMaxBodyBytes,
		ReadHeaderTimeout: DefaultReadHeaderTimeout,
		DrainDelay:        DefaultDrainDelay,
		ShutdownTimeout:   DefaultShutdownTimeout,
	}
}

// LoadConfig applies NORMALIZER_* environment overrides to the defaults.
//
// getenv is injected so the loader is testable without mutating the process
// environment, which is shared state that makes parallel tests flaky.
func LoadConfig(getenv func(string) string) (Config, error) {
	cfg := DefaultConfig()
	var problems []string

	str := func(name string, target *string) {
		if raw, ok := lookup(getenv, name); ok {
			*target = raw
		}
	}
	num := func(name string, target *int) {
		raw, ok := lookup(getenv, name)
		if !ok {
			return
		}
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s%s=%q is not an integer", EnvPrefix, name, raw))
			return
		}
		*target = parsed
	}
	num64 := func(name string, target *int64) {
		raw, ok := lookup(getenv, name)
		if !ok {
			return
		}
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s%s=%q is not an integer", EnvPrefix, name, raw))
			return
		}
		*target = parsed
	}
	dur := func(name string, target *time.Duration) {
		raw, ok := lookup(getenv, name)
		if !ok {
			return
		}
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			problems = append(problems, fmt.Sprintf(
				"%s%s=%q is not a duration; use a unit, e.g. %q", EnvPrefix, name, raw, "250ms"))
			return
		}
		*target = parsed
	}

	str("ADDR", &cfg.Addr)
	str("ADMIN_ADDR", &cfg.AdminAddr)
	num("COST_ROUNDS", &cfg.CostRounds)
	dur("PROCESSING_DELAY", &cfg.ProcessingDelay)
	num("MAX_CONCURRENT", &cfg.MaxConcurrent)
	num("QUEUE_LIMIT", &cfg.QueueLimit)
	dur("QUEUE_TIMEOUT", &cfg.QueueTimeout)
	num64("MAX_BODY_BYTES", &cfg.MaxBodyBytes)
	dur("READ_HEADER_TIMEOUT", &cfg.ReadHeaderTimeout)
	dur("DRAIN_DELAY", &cfg.DrainDelay)
	dur("SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout)

	if len(problems) > 0 {
		return Config{}, errors.New("invalid configuration: " + strings.Join(problems, "; "))
	}
	return cfg, cfg.Validate()
}

func lookup(getenv func(string) string, name string) (string, bool) {
	raw := strings.TrimSpace(getenv(EnvPrefix + name))
	return raw, raw != ""
}

// Validate rejects a configuration the Normalizer cannot honour.
//
// Refusing to start is the right response: every one of these mistakes produces
// a service that looks healthy and behaves wrongly, and a wrongly-behaving
// workload invalidates every measurement taken against it.
func (c Config) Validate() error {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	if strings.TrimSpace(c.Addr) == "" {
		add("%sADDR must not be empty", EnvPrefix)
	}
	if strings.TrimSpace(c.AdminAddr) == "" {
		add("%sADMIN_ADDR must not be empty", EnvPrefix)
	}
	if c.Addr == c.AdminAddr {
		add("%sADDR and %sADMIN_ADDR are both %q; the data and admin planes need separate ports so they can be exposed separately",
			EnvPrefix, EnvPrefix, c.Addr)
	}
	if c.CostRounds < 0 {
		add("%sCOST_ROUNDS must not be negative, got %d", EnvPrefix, c.CostRounds)
	}
	if c.ProcessingDelay < 0 {
		add("%sPROCESSING_DELAY must not be negative, got %s", EnvPrefix, c.ProcessingDelay)
	}
	if c.MaxConcurrent < 1 {
		add("%sMAX_CONCURRENT must be at least 1, got %d; a pod with no processing slots accepts records it can never process",
			EnvPrefix, c.MaxConcurrent)
	}
	if c.QueueLimit < 0 {
		add("%sQUEUE_LIMIT must not be negative, got %d", EnvPrefix, c.QueueLimit)
	}
	if c.QueueTimeout <= 0 {
		add("%sQUEUE_TIMEOUT must be greater than 0, got %s; a record that waits forever is retried by NiFi while still queued",
			EnvPrefix, c.QueueTimeout)
	}
	if c.MaxBodyBytes < 1 {
		add("%sMAX_BODY_BYTES must be at least 1, got %d", EnvPrefix, c.MaxBodyBytes)
	}
	if c.ReadHeaderTimeout <= 0 {
		add("%sREAD_HEADER_TIMEOUT must be greater than 0, got %s", EnvPrefix, c.ReadHeaderTimeout)
	}
	if c.DrainDelay < 0 {
		add("%sDRAIN_DELAY must not be negative, got %s", EnvPrefix, c.DrainDelay)
	}
	if c.ShutdownTimeout <= 0 {
		add("%sSHUTDOWN_TIMEOUT must be greater than 0, got %s", EnvPrefix, c.ShutdownTimeout)
	}
	if c.DrainDelay >= c.ShutdownTimeout {
		add("%sDRAIN_DELAY (%s) must be shorter than %sSHUTDOWN_TIMEOUT (%s), or the drain has no time left to finish in-flight records",
			EnvPrefix, c.DrainDelay, EnvPrefix, c.ShutdownTimeout)
	}

	if len(problems) > 0 {
		return errors.New("invalid configuration: " + strings.Join(problems, "; "))
	}
	return nil
}
