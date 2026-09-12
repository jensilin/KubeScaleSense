// Package config owns the KubeScaleSense configuration schema, its defaults,
// its KSS_* environment overrides, and its validation.
//
// The schema here is the executable form of the canonical parameter list in
// docs/requirements.md § 8; the YAML keys, the defaults, and the environment
// override naming all match that document verbatim. Keeping one canonical list
// matters because six other design documents reference these key names.
//
// Nothing in this package reads the Kubernetes API. The checks that need a
// cluster — that the target pod template declares CPU and memory requests
// (A-03), and that no HPA already manages the target (FR-19) — run in the
// Phase 1 startup path and are listed in ClusterChecks.
package config

import "time"

// Default* values are the documented defaults from docs/requirements.md § 8.
// They are exported so that tests and later phases can assert against them
// rather than re-typing literals that would then drift.
const (
	DefaultInterval       = 15 * time.Second
	DefaultLeaderElection = true

	// DefaultDryRun is true for Phase 1, which is a deliberate departure from
	// the false shown in docs/requirements.md § 8.
	//
	// The two documents disagreed: the parameter table lists the eventual
	// production default, while docs/implementation-plan.md § P1 requires dryRun
	// "hard-defaulted to true for this phase". The plan wins, because it is the
	// document that describes what this code is allowed to do — and because the
	// safe default is the one that cannot surprise anyone. Validation goes
	// further and refuses a false value outright: Phase 1 has no write path, so
	// asking for live mode is asking for something the binary cannot do.
	DefaultDryRun = true

	DefaultMetricsAddr = ":8080"
	DefaultHealthAddr  = ":8081"
	DefaultLogLevel    = LogLevelInfo
	DefaultLogFormat   = LogFormatJSON

	DefaultMinReplicas int32 = 1
	DefaultMaxReplicas int32 = 12

	DefaultItemsPerReplica             = 50
	DefaultTargetCPUUtilizationPercent = 70
	DefaultMetricsStaleAfter           = 60 * time.Second
	DefaultPodWarmupPeriod             = 60 * time.Second
	DefaultSmoothingMode               = SmoothingModeEWMA
	DefaultSmoothingAlpha              = 0.4
	DefaultSignalSource                = SignalSourceSynthetic
	DefaultSignalEndpoint              = ""
	DefaultSignalTimeout               = 3 * time.Second
	DefaultSignalSyntheticPath         = "/etc/kubescalesense/pressure.yaml"

	DefaultPerNodeReserveCPUMilli  int64 = 200
	DefaultPerNodeReserveMemoryMiB int64 = 256
	DefaultFitCapacityMarginPods         = 1
	DefaultRespectNodeSelector           = true
	DefaultRespectNodeAffinity           = true
	DefaultRespectTaints                 = true
	DefaultNodeLabelSelector             = ""

	DefaultScaleUpCooldown                    = 60 * time.Second
	DefaultScaleDownCooldown                  = 300 * time.Second
	DefaultScaleDownStabilizationWindow       = 300 * time.Second
	DefaultScaleDownWindowCoverage            = 0.8
	DefaultTolerancePercent                   = 10
	DefaultMaxScaleUpStep               int32 = 4
	DefaultMaxScaleDownStep             int32 = 1
	DefaultAllowPartialScaleUp                = true
	DefaultExternalChangeTolerance            = 3
	DefaultHoldBackoffInitial                 = 30 * time.Second
	DefaultHoldBackoffMax                     = 15 * time.Minute
	DefaultHoldBackoffFactor                  = 2.0

	DefaultPendingPodTimeout = 120 * time.Second
	DefaultOnPendingTimeout  = OnPendingRevert
	DefaultPodStartupTimeout = 300 * time.Second
)

// Enumerated configuration values. Each set is closed: validation rejects
// anything outside it rather than falling back to a default, because silently
// substituting a value an operator did not ask for is how an autoscaler ends up
// behaving in a way nobody configured.
const (
	LogLevelDebug = "debug"
	LogLevelInfo  = "info"
	LogLevelWarn  = "warn"
	LogLevelError = "error"

	LogFormatJSON = "json"
	LogFormatText = "text"

	SmoothingModeNone = "none"
	SmoothingModeEWMA = "ewma"

	// SignalSourceSynthetic replays a scripted pressure series from a file.
	// SignalSourceHTTP scrapes the Normalizer's own metrics endpoint.
	// SignalSourceNone reports the signal as permanently unavailable.
	// See docs/architecture.md ADR-22 and requirements FR-36.
	SignalSourceSynthetic = "synthetic"
	SignalSourceHTTP      = "http"
	SignalSourceNone      = "none"

	OnPendingRevert = "revert"
	OnPendingFreeze = "freeze"
	OnPendingNone   = "none"
)

// MinInterval is the floor on controller.interval required by CR-2. Below this,
// informer churn and metrics-server load outweigh any gain in reaction time.
const MinInterval = 5 * time.Second

// Config is the root of the configuration schema.
type Config struct {
	Controller ControllerConfig `yaml:"controller"`
	Target     TargetConfig     `yaml:"target"`
	Workload   WorkloadConfig   `yaml:"workload"`
	Resources  ResourcesConfig  `yaml:"resources"`
	Scaling    ScalingConfig    `yaml:"scaling"`
	Pending    PendingConfig    `yaml:"pending"`
}

// ControllerConfig holds process-level settings.
type ControllerConfig struct {
	Interval       Duration `yaml:"interval"`
	LeaderElection bool     `yaml:"leaderElection"`
	DryRun         bool     `yaml:"dryRun"`
	MetricsAddr    string   `yaml:"metricsAddr"`
	HealthAddr     string   `yaml:"healthAddr"`
	LogLevel       string   `yaml:"logLevel"`
	LogFormat      string   `yaml:"logFormat"`
}

// TargetConfig identifies the scaled workload. Namespace and Deployment have no
// defaults: CR-1 states that a minimal config specifies only the target, so
// there is nothing safe to guess here. Scaling the wrong Deployment because a
// default pointed somewhere plausible is a worse outcome than refusing to start.
type TargetConfig struct {
	Namespace   string `yaml:"namespace"`
	Deployment  string `yaml:"deployment"`
	MinReplicas int32  `yaml:"minReplicas"`
	MaxReplicas int32  `yaml:"maxReplicas"`
}

// WorkloadConfig describes the demand half of the decision.
type WorkloadConfig struct {
	ItemsPerReplica             int             `yaml:"itemsPerReplica"`
	TargetCPUUtilizationPercent int             `yaml:"targetCPUUtilizationPercent"`
	MetricsStaleAfter           Duration        `yaml:"metricsStaleAfter"`
	PodWarmupPeriod             Duration        `yaml:"podWarmupPeriod"`
	BacklogSmoothing            SmoothingConfig `yaml:"backlogSmoothing"`
	Signal                      SignalConfig    `yaml:"signal"`
}

// SmoothingConfig damps single-sample pressure spikes.
type SmoothingConfig struct {
	Mode  string  `yaml:"mode"`
	Alpha float64 `yaml:"alpha"`
}

// SignalConfig selects the replaceable workload-pressure source (FR-36).
// There is deliberately no credential field of any kind: the v0.2 design reads
// pressure either from a local file or from an in-cluster HTTP endpoint, so
// there is no database role, broker user, or API token to hold (CR-5, FR-37).
type SignalConfig struct {
	Source        string   `yaml:"source"`
	Endpoint      string   `yaml:"endpoint"`
	Timeout       Duration `yaml:"timeout"`
	SyntheticPath string   `yaml:"syntheticPath"`
}

// ResourcesConfig tunes the feasibility half of the decision.
type ResourcesConfig struct {
	PerNodeReserveCPUMilli  int64  `yaml:"perNodeReserveCPUMilli"`
	PerNodeReserveMemoryMiB int64  `yaml:"perNodeReserveMemoryMiB"`
	FitCapacityMarginPods   int    `yaml:"fitCapacityMarginPods"`
	RespectNodeSelector     bool   `yaml:"respectNodeSelector"`
	RespectNodeAffinity     bool   `yaml:"respectNodeAffinity"`
	RespectTaints           bool   `yaml:"respectTaints"`
	NodeLabelSelector       string `yaml:"nodeLabelSelector"`
}

// ScalingConfig holds the hysteresis and rate-limiting parameters.
type ScalingConfig struct {
	ScaleUpCooldown              Duration          `yaml:"scaleUpCooldown"`
	ScaleDownCooldown            Duration          `yaml:"scaleDownCooldown"`
	ScaleDownStabilizationWindow Duration          `yaml:"scaleDownStabilizationWindow"`
	ScaleDownWindowCoverage      float64           `yaml:"scaleDownWindowCoverage"`
	TolerancePercent             int               `yaml:"tolerancePercent"`
	MaxScaleUpStep               int32             `yaml:"maxScaleUpStep"`
	MaxScaleDownStep             int32             `yaml:"maxScaleDownStep"`
	AllowPartialScaleUp          bool              `yaml:"allowPartialScaleUp"`
	ExternalChangeTolerance      int               `yaml:"externalChangeTolerance"`
	HoldBackoff                  HoldBackoffConfig `yaml:"holdBackoff"`
}

// HoldBackoffConfig bounds retries of a scale-up that keeps proving infeasible.
type HoldBackoffConfig struct {
	Initial Duration `yaml:"initial"`
	Max     Duration `yaml:"max"`
	Factor  float64  `yaml:"factor"`
}

// PendingConfig governs the Pending and unhealthy-pod watchdogs.
type PendingConfig struct {
	PendingPodTimeout Duration `yaml:"pendingPodTimeout"`
	OnPendingTimeout  string   `yaml:"onPendingTimeout"`
	PodStartupTimeout Duration `yaml:"podStartupTimeout"`
}

// Default returns a Config populated with every documented default. Target is
// left zero-valued on purpose so that Validate reports it as missing.
func Default() *Config {
	return &Config{
		Controller: ControllerConfig{
			Interval:       Duration(DefaultInterval),
			LeaderElection: DefaultLeaderElection,
			DryRun:         DefaultDryRun,
			MetricsAddr:    DefaultMetricsAddr,
			HealthAddr:     DefaultHealthAddr,
			LogLevel:       DefaultLogLevel,
			LogFormat:      DefaultLogFormat,
		},
		Target: TargetConfig{
			MinReplicas: DefaultMinReplicas,
			MaxReplicas: DefaultMaxReplicas,
		},
		Workload: WorkloadConfig{
			ItemsPerReplica:             DefaultItemsPerReplica,
			TargetCPUUtilizationPercent: DefaultTargetCPUUtilizationPercent,
			MetricsStaleAfter:           Duration(DefaultMetricsStaleAfter),
			PodWarmupPeriod:             Duration(DefaultPodWarmupPeriod),
			BacklogSmoothing: SmoothingConfig{
				Mode:  DefaultSmoothingMode,
				Alpha: DefaultSmoothingAlpha,
			},
			Signal: SignalConfig{
				Source:        DefaultSignalSource,
				Endpoint:      DefaultSignalEndpoint,
				Timeout:       Duration(DefaultSignalTimeout),
				SyntheticPath: DefaultSignalSyntheticPath,
			},
		},
		Resources: ResourcesConfig{
			PerNodeReserveCPUMilli:  DefaultPerNodeReserveCPUMilli,
			PerNodeReserveMemoryMiB: DefaultPerNodeReserveMemoryMiB,
			FitCapacityMarginPods:   DefaultFitCapacityMarginPods,
			RespectNodeSelector:     DefaultRespectNodeSelector,
			RespectNodeAffinity:     DefaultRespectNodeAffinity,
			RespectTaints:           DefaultRespectTaints,
			NodeLabelSelector:       DefaultNodeLabelSelector,
		},
		Scaling: ScalingConfig{
			ScaleUpCooldown:              Duration(DefaultScaleUpCooldown),
			ScaleDownCooldown:            Duration(DefaultScaleDownCooldown),
			ScaleDownStabilizationWindow: Duration(DefaultScaleDownStabilizationWindow),
			ScaleDownWindowCoverage:      DefaultScaleDownWindowCoverage,
			TolerancePercent:             DefaultTolerancePercent,
			MaxScaleUpStep:               DefaultMaxScaleUpStep,
			MaxScaleDownStep:             DefaultMaxScaleDownStep,
			AllowPartialScaleUp:          DefaultAllowPartialScaleUp,
			ExternalChangeTolerance:      DefaultExternalChangeTolerance,
			HoldBackoff: HoldBackoffConfig{
				Initial: Duration(DefaultHoldBackoffInitial),
				Max:     Duration(DefaultHoldBackoffMax),
				Factor:  DefaultHoldBackoffFactor,
			},
		},
		Pending: PendingConfig{
			PendingPodTimeout: Duration(DefaultPendingPodTimeout),
			OnPendingTimeout:  DefaultOnPendingTimeout,
			PodStartupTimeout: Duration(DefaultPodStartupTimeout),
		},
	}
}

// ClusterChecks lists the validation that docs/architecture.md § 4.1 assigns to
// internal/config but which cannot run here, because it needs a Kubernetes
// client.
//
// Phase 0 recorded these as deferred. Phase 1 implements both in
// controller.VerifyTarget, which runs once after the informer caches sync and
// fails startup on either, so the list now documents where they live rather
// than that they are missing.
var ClusterChecks = []string{
	"target pod template declares CPU and memory requests (A-03, FS-19) — controller.VerifyTarget",
	"no HorizontalPodAutoscaler already manages the target (FR-19, FS-16) — controller.VerifyTarget",
}
