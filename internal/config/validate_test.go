package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// validConfig returns a Config that passes validation: the documented defaults
// plus the target, which has no default by design (CR-1).
func validConfig() *Config {
	cfg := Default()
	cfg.Target.Namespace = "data-pipeline"
	cfg.Target.Deployment = "normalizer"
	return cfg
}

func TestValidate_ValidDefaults(t *testing.T) {
	t.Parallel()

	if err := validConfig().Validate(); err != nil {
		t.Fatalf("documented defaults plus a target must validate, got: %v", err)
	}
}

// The defaults are the contract six other documents quote, so they are asserted
// literally rather than via Default(), which would make the test tautological.
func TestDefault_MatchesDocumentedValues(t *testing.T) {
	t.Parallel()

	cfg := Default()

	checks := []struct {
		key  string
		got  any
		want any
	}{
		{"controller.interval", cfg.Controller.Interval.Duration(), 15 * time.Second},
		{"controller.leaderElection", cfg.Controller.LeaderElection, true},
		// The one default that deviates from the requirements § 8 table, which
		// documents the eventual production value. Phase 1 has no write path,
		// so it defaults to dry-run and rejects the alternative (CR-6).
		{"controller.dryRun", cfg.Controller.DryRun, true},
		{"controller.metricsAddr", cfg.Controller.MetricsAddr, ":8080"},
		{"controller.healthAddr", cfg.Controller.HealthAddr, ":8081"},
		{"controller.logLevel", cfg.Controller.LogLevel, "info"},
		{"controller.logFormat", cfg.Controller.LogFormat, "json"},
		{"target.minReplicas", cfg.Target.MinReplicas, int32(1)},
		{"target.maxReplicas", cfg.Target.MaxReplicas, int32(12)},
		{"workload.itemsPerReplica", cfg.Workload.ItemsPerReplica, 50},
		{"workload.targetCPUUtilizationPercent", cfg.Workload.TargetCPUUtilizationPercent, 70},
		{"workload.metricsStaleAfter", cfg.Workload.MetricsStaleAfter.Duration(), 60 * time.Second},
		{"workload.podWarmupPeriod", cfg.Workload.PodWarmupPeriod.Duration(), 60 * time.Second},
		{"workload.backlogSmoothing.mode", cfg.Workload.BacklogSmoothing.Mode, "ewma"},
		{"workload.backlogSmoothing.alpha", cfg.Workload.BacklogSmoothing.Alpha, 0.4},
		{"workload.signal.source", cfg.Workload.Signal.Source, "synthetic"},
		{"workload.signal.endpoint", cfg.Workload.Signal.Endpoint, ""},
		{"workload.signal.timeout", cfg.Workload.Signal.Timeout.Duration(), 3 * time.Second},
		{"workload.signal.syntheticPath", cfg.Workload.Signal.SyntheticPath, "/etc/kubescalesense/pressure.yaml"},
		{"resources.perNodeReserveCPUMilli", cfg.Resources.PerNodeReserveCPUMilli, int64(200)},
		{"resources.perNodeReserveMemoryMiB", cfg.Resources.PerNodeReserveMemoryMiB, int64(256)},
		{"resources.fitCapacityMarginPods", cfg.Resources.FitCapacityMarginPods, 1},
		{"resources.respectNodeSelector", cfg.Resources.RespectNodeSelector, true},
		{"resources.respectNodeAffinity", cfg.Resources.RespectNodeAffinity, true},
		{"resources.respectTaints", cfg.Resources.RespectTaints, true},
		{"resources.nodeLabelSelector", cfg.Resources.NodeLabelSelector, ""},
		{"scaling.scaleUpCooldown", cfg.Scaling.ScaleUpCooldown.Duration(), 60 * time.Second},
		{"scaling.scaleDownCooldown", cfg.Scaling.ScaleDownCooldown.Duration(), 300 * time.Second},
		{"scaling.scaleDownStabilizationWindow", cfg.Scaling.ScaleDownStabilizationWindow.Duration(), 300 * time.Second},
		{"scaling.scaleDownWindowCoverage", cfg.Scaling.ScaleDownWindowCoverage, 0.8},
		{"scaling.tolerancePercent", cfg.Scaling.TolerancePercent, 10},
		{"scaling.maxScaleUpStep", cfg.Scaling.MaxScaleUpStep, int32(4)},
		{"scaling.maxScaleDownStep", cfg.Scaling.MaxScaleDownStep, int32(1)},
		{"scaling.allowPartialScaleUp", cfg.Scaling.AllowPartialScaleUp, true},
		{"scaling.externalChangeTolerance", cfg.Scaling.ExternalChangeTolerance, 3},
		{"scaling.holdBackoff.initial", cfg.Scaling.HoldBackoff.Initial.Duration(), 30 * time.Second},
		{"scaling.holdBackoff.max", cfg.Scaling.HoldBackoff.Max.Duration(), 15 * time.Minute},
		{"scaling.holdBackoff.factor", cfg.Scaling.HoldBackoff.Factor, 2.0},
		{"pending.pendingPodTimeout", cfg.Pending.PendingPodTimeout.Duration(), 120 * time.Second},
		{"pending.onPendingTimeout", cfg.Pending.OnPendingTimeout, "revert"},
		{"pending.podStartupTimeout", cfg.Pending.PodStartupTimeout.Duration(), 300 * time.Second},
	}

	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, documented default is %v", c.key, c.got, c.want)
		}
	}

	// Target identity must stay unset so that Validate reports it as missing.
	if cfg.Target.Namespace != "" || cfg.Target.Deployment != "" {
		t.Errorf("target.namespace/deployment must have no default (CR-1), got %q/%q",
			cfg.Target.Namespace, cfg.Target.Deployment)
	}
}

func TestValidate_ValidCustomConfig(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Controller.Interval = Duration(30 * time.Second)
	cfg.Controller.LogLevel = LogLevelDebug
	cfg.Controller.LogFormat = LogFormatText
	cfg.Controller.DryRun = true
	cfg.Target.MinReplicas = 2
	cfg.Target.MaxReplicas = 40
	cfg.Workload.ItemsPerReplica = 250
	cfg.Workload.TargetCPUUtilizationPercent = 55
	cfg.Workload.BacklogSmoothing.Mode = SmoothingModeNone
	cfg.Workload.Signal.Source = SignalSourceHTTP
	cfg.Workload.Signal.Endpoint = "http://normalizer-service.data-pipeline.svc.cluster.local:9090/metrics"
	cfg.Resources.NodeLabelSelector = "node-pool=workers,tier=compute"
	cfg.Scaling.ScaleDownWindowCoverage = 1.0
	cfg.Scaling.HoldBackoff.Factor = 1.5
	cfg.Pending.OnPendingTimeout = OnPendingFreeze

	if err := cfg.Validate(); err != nil {
		t.Fatalf("custom but legal configuration must validate, got: %v", err)
	}
}

// TestValidate_Rejects is the table behind UT-21: every invalid case must be
// rejected, and must be rejected by name. A validator that says "invalid
// config" and stops is not usable from a CrashLoopBackOff.
func TestValidate_Rejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantKey string
		// wantMsg is a substring the message must contain, asserting the error
		// is actionable rather than merely present.
		wantMsg string
	}{
		// --- missing target (FS-19) ---
		{
			name:    "missing target namespace",
			mutate:  func(c *Config) { c.Target.Namespace = "" },
			wantKey: "target.namespace",
			wantMsg: "required",
		},
		{
			name:    "missing target deployment",
			mutate:  func(c *Config) { c.Target.Deployment = "" },
			wantKey: "target.deployment",
			wantMsg: "required",
		},
		{
			name:    "whitespace-only target deployment",
			mutate:  func(c *Config) { c.Target.Deployment = "   " },
			wantKey: "target.deployment",
			wantMsg: "required",
		},

		// --- invalid replica bounds ---
		{
			name:    "minReplicas exceeds maxReplicas",
			mutate:  func(c *Config) { c.Target.MinReplicas, c.Target.MaxReplicas = 20, 12 },
			wantKey: "target.minReplicas",
			wantMsg: "must not exceed target.maxReplicas (12)",
		},
		{
			name:    "minReplicas zero rejects scale-to-zero",
			mutate:  func(c *Config) { c.Target.MinReplicas = 0 },
			wantKey: "target.minReplicas",
			wantMsg: "at least 1",
		},
		{
			name:    "negative minReplicas",
			mutate:  func(c *Config) { c.Target.MinReplicas = -3 },
			wantKey: "target.minReplicas",
			wantMsg: "at least 1",
		},
		{
			name:    "maxReplicas zero",
			mutate:  func(c *Config) { c.Target.MaxReplicas = 0 },
			wantKey: "target.maxReplicas",
			wantMsg: "at least 1",
		},

		// --- invalid numeric values ---
		{
			name:    "itemsPerReplica zero would divide by zero",
			mutate:  func(c *Config) { c.Workload.ItemsPerReplica = 0 },
			wantKey: "workload.itemsPerReplica",
			wantMsg: "greater than 0",
		},
		{
			name:    "negative itemsPerReplica",
			mutate:  func(c *Config) { c.Workload.ItemsPerReplica = -50 },
			wantKey: "workload.itemsPerReplica",
			wantMsg: "greater than 0",
		},
		{
			name:    "negative per-node CPU reserve",
			mutate:  func(c *Config) { c.Resources.PerNodeReserveCPUMilli = -1 },
			wantKey: "resources.perNodeReserveCPUMilli",
			wantMsg: "negative",
		},
		{
			name:    "negative per-node memory reserve",
			mutate:  func(c *Config) { c.Resources.PerNodeReserveMemoryMiB = -256 },
			wantKey: "resources.perNodeReserveMemoryMiB",
			wantMsg: "negative",
		},
		{
			name:    "negative fit capacity margin would over-claim capacity",
			mutate:  func(c *Config) { c.Resources.FitCapacityMarginPods = -2 },
			wantKey: "resources.fitCapacityMarginPods",
			wantMsg: "negative",
		},
		{
			name:    "maxScaleUpStep zero can never scale up",
			mutate:  func(c *Config) { c.Scaling.MaxScaleUpStep = 0 },
			wantKey: "scaling.maxScaleUpStep",
			wantMsg: "never scale up",
		},
		{
			name:    "maxScaleDownStep zero can never scale down",
			mutate:  func(c *Config) { c.Scaling.MaxScaleDownStep = 0 },
			wantKey: "scaling.maxScaleDownStep",
			wantMsg: "never scale down",
		},
		{
			name:    "negative externalChangeTolerance",
			mutate:  func(c *Config) { c.Scaling.ExternalChangeTolerance = -1 },
			wantKey: "scaling.externalChangeTolerance",
			wantMsg: "negative",
		},

		// --- invalid threshold configuration ---
		{
			name:    "cpu target above 100 percent",
			mutate:  func(c *Config) { c.Workload.TargetCPUUtilizationPercent = 120 },
			wantKey: "workload.targetCPUUtilizationPercent",
			wantMsg: "between 1 and 100",
		},
		{
			name:    "cpu target zero",
			mutate:  func(c *Config) { c.Workload.TargetCPUUtilizationPercent = 0 },
			wantKey: "workload.targetCPUUtilizationPercent",
			wantMsg: "between 1 and 100",
		},
		{
			name:    "tolerance above 100 percent",
			mutate:  func(c *Config) { c.Scaling.TolerancePercent = 101 },
			wantKey: "scaling.tolerancePercent",
			wantMsg: "between 0 and 100",
		},
		{
			name:    "negative tolerance",
			mutate:  func(c *Config) { c.Scaling.TolerancePercent = -5 },
			wantKey: "scaling.tolerancePercent",
			wantMsg: "between 0 and 100",
		},
		{
			name:    "ewma alpha above 1",
			mutate:  func(c *Config) { c.Workload.BacklogSmoothing.Alpha = 1.5 },
			wantKey: "workload.backlogSmoothing.alpha",
			wantMsg: "at most 1",
		},
		{
			name:    "ewma alpha zero",
			mutate:  func(c *Config) { c.Workload.BacklogSmoothing.Alpha = 0 },
			wantKey: "workload.backlogSmoothing.alpha",
			wantMsg: "greater than 0",
		},
		{
			name:    "window coverage above 1",
			mutate:  func(c *Config) { c.Scaling.ScaleDownWindowCoverage = 1.2 },
			wantKey: "scaling.scaleDownWindowCoverage",
			wantMsg: "at most 1",
		},
		{
			name:    "window coverage zero",
			mutate:  func(c *Config) { c.Scaling.ScaleDownWindowCoverage = 0 },
			wantKey: "scaling.scaleDownWindowCoverage",
			wantMsg: "greater than 0",
		},
		{
			name:    "hold backoff factor of 1 never backs off",
			mutate:  func(c *Config) { c.Scaling.HoldBackoff.Factor = 1.0 },
			wantKey: "scaling.holdBackoff.factor",
			wantMsg: "never backs off",
		},

		// --- invalid duration / combination ---
		{
			name:    "interval below the 5s floor",
			mutate:  func(c *Config) { c.Controller.Interval = Duration(time.Second) },
			wantKey: "controller.interval",
			wantMsg: "at least 5s",
		},
		{
			name:    "zero interval",
			mutate:  func(c *Config) { c.Controller.Interval = 0 },
			wantKey: "controller.interval",
			wantMsg: "at least 5s",
		},
		{
			name: "metricsStaleAfter below interval means every sample is stale",
			mutate: func(c *Config) {
				c.Controller.Interval = Duration(30 * time.Second)
				c.Workload.MetricsStaleAfter = Duration(10 * time.Second)
			},
			wantKey: "workload.metricsStaleAfter",
			wantMsg: "at least controller.interval (30s)",
		},
		{
			name: "stabilization window shorter than one sample period",
			mutate: func(c *Config) {
				c.Controller.Interval = Duration(60 * time.Second)
				c.Scaling.ScaleDownStabilizationWindow = Duration(30 * time.Second)
			},
			wantKey: "scaling.scaleDownStabilizationWindow",
			wantMsg: "cannot be covered",
		},
		{
			name: "pendingPodTimeout below interval",
			mutate: func(c *Config) {
				c.Controller.Interval = Duration(60 * time.Second)
				c.Pending.PendingPodTimeout = Duration(10 * time.Second)
			},
			wantKey: "pending.pendingPodTimeout",
			wantMsg: "at least controller.interval",
		},
		{
			name:    "hold backoff max below initial",
			mutate:  func(c *Config) { c.Scaling.HoldBackoff.Max = Duration(time.Second) },
			wantKey: "scaling.holdBackoff.max",
			wantMsg: "at least scaling.holdBackoff.initial (30s)",
		},
		{
			name:    "zero stabilization window",
			mutate:  func(c *Config) { c.Scaling.ScaleDownStabilizationWindow = 0 },
			wantKey: "scaling.scaleDownStabilizationWindow",
			wantMsg: "greater than 0",
		},
		{
			name:    "zero podStartupTimeout",
			mutate:  func(c *Config) { c.Pending.PodStartupTimeout = 0 },
			wantKey: "pending.podStartupTimeout",
			wantMsg: "greater than 0",
		},

		// --- enumerations ---
		{
			name:    "unknown log level",
			mutate:  func(c *Config) { c.Controller.LogLevel = "verbose" },
			wantKey: "controller.logLevel",
			wantMsg: `"debug", "info", "warn", "error"`,
		},
		{
			name:    "unknown log format",
			mutate:  func(c *Config) { c.Controller.LogFormat = "logfmt" },
			wantKey: "controller.logFormat",
			wantMsg: `"json", "text"`,
		},
		{
			name:    "unknown smoothing mode",
			mutate:  func(c *Config) { c.Workload.BacklogSmoothing.Mode = "kalman" },
			wantKey: "workload.backlogSmoothing.mode",
			wantMsg: `"none", "ewma"`,
		},
		{
			name:    "unknown pending action",
			mutate:  func(c *Config) { c.Pending.OnPendingTimeout = "delete" },
			wantKey: "pending.onPendingTimeout",
			wantMsg: `"revert", "freeze", "none"`,
		},

		// --- signal source (FR-36) ---
		{
			name:    "unknown signal source",
			mutate:  func(c *Config) { c.Workload.Signal.Source = "postgres" },
			wantKey: "workload.signal.source",
			wantMsg: `"synthetic", "http", "none"`,
		},
		{
			name: "http source with empty endpoint",
			mutate: func(c *Config) {
				c.Workload.Signal.Source = SignalSourceHTTP
				c.Workload.Signal.Endpoint = ""
			},
			wantKey: "workload.signal.endpoint",
			wantMsg: "required when workload.signal.source is \"http\"",
		},
		{
			name: "http source with malformed endpoint",
			mutate: func(c *Config) {
				c.Workload.Signal.Source = SignalSourceHTTP
				c.Workload.Signal.Endpoint = "normalizer-service:9090/metrics"
			},
			wantKey: "workload.signal.endpoint",
			wantMsg: "http or https scheme",
		},
		{
			name: "http source with scheme but no host",
			mutate: func(c *Config) {
				c.Workload.Signal.Source = SignalSourceHTTP
				c.Workload.Signal.Endpoint = "http:///metrics"
			},
			wantKey: "workload.signal.endpoint",
			wantMsg: "must include a host",
		},
		{
			name: "synthetic source with empty path",
			mutate: func(c *Config) {
				c.Workload.Signal.Source = SignalSourceSynthetic
				c.Workload.Signal.SyntheticPath = ""
			},
			wantKey: "workload.signal.syntheticPath",
			wantMsg: "required when workload.signal.source is \"synthetic\"",
		},
		{
			name: "endpoint set on a source that never scrapes",
			mutate: func(c *Config) {
				c.Workload.Signal.Source = SignalSourceNone
				c.Workload.Signal.Endpoint = "http://normalizer-service:9090/metrics"
			},
			wantKey: "workload.signal.endpoint",
			wantMsg: "would never be read",
		},
		{
			name: "zero signal timeout cannot report unavailability",
			mutate: func(c *Config) {
				c.Workload.Signal.Timeout = 0
			},
			wantKey: "workload.signal.timeout",
			wantMsg: "greater than 0",
		},

		// --- listeners and selectors ---
		{
			name:    "metrics address without a port",
			mutate:  func(c *Config) { c.Controller.MetricsAddr = "8080" },
			wantKey: "controller.metricsAddr",
			wantMsg: "host:port",
		},
		{
			name:    "metrics port out of range",
			mutate:  func(c *Config) { c.Controller.MetricsAddr = ":99999" },
			wantKey: "controller.metricsAddr",
			wantMsg: "between 1 and 65535",
		},
		{
			name:    "metrics and health on the same address",
			mutate:  func(c *Config) { c.Controller.HealthAddr = c.Controller.MetricsAddr },
			wantKey: "controller.healthAddr",
			wantMsg: "must differ from controller.metricsAddr",
		},
		{
			name:    "node label selector without a key",
			mutate:  func(c *Config) { c.Resources.NodeLabelSelector = "=workers" },
			wantKey: "resources.nodeLabelSelector",
			wantMsg: "not a valid label selector",
		},
		{
			name:    "node label selector with an empty term",
			mutate:  func(c *Config) { c.Resources.NodeLabelSelector = "a=b,,c=d" },
			wantKey: "resources.nodeLabelSelector",
			wantMsg: "not a valid label selector",
		},
		{
			name:    "node label selector with an illegal key character",
			mutate:  func(c *Config) { c.Resources.NodeLabelSelector = "node pool=workers" },
			wantKey: "resources.nodeLabelSelector",
			wantMsg: "not a valid label selector",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := validConfig()
			tt.mutate(cfg)

			err := cfg.Validate()
			if err == nil {
				t.Fatalf("expected %s to be rejected, got no error", tt.wantKey)
			}

			var verr *ValidationError
			if !errors.As(err, &verr) {
				t.Fatalf("expected *ValidationError, got %T: %v", err, err)
			}

			if !hasKey(verr, tt.wantKey) {
				t.Errorf("expected a problem on %q, got keys %v\nfull error: %v",
					tt.wantKey, verr.Keys(), err)
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("message must explain the constraint: expected it to contain %q\ngot: %v",
					tt.wantMsg, err)
			}
		})
	}
}

// The node label selector is handed to the same parser the candidate-node
// filter uses, so anything Kubernetes accepts must pass validation. Phase 0
// hand-rolled an equality-only check that rejected most of these.
// Live mode is a legal configuration from P3 onwards, and the default is still
// dry-run.
//
// Both halves matter. Before P3 this validator refused dryRun: false, because
// the binary had no write path and a controller that looks live while changing
// nothing is the more dangerous failure. Now that the write path exists the
// refusal would be wrong — but the default staying true is what keeps live
// actuation something an operator chooses rather than something they inherit.
func TestValidate_LiveModeIsAllowedAndDryRunIsTheDefault(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Controller.DryRun = false

	if err := cfg.Validate(); err != nil {
		t.Errorf("dryRun: false was rejected, but P3 implements actuation: %v", err)
	}

	if !Default().Controller.DryRun {
		t.Error("the default is live actuation; it must remain dry-run so that omitting the field is safe")
	}
}

func TestValidate_AcceptsEveryValidLabelSelector(t *testing.T) {
	t.Parallel()

	selectors := []string{
		"",
		"node-pool=workers",
		"node-pool==workers",
		"node-pool!=control-plane",
		"node-pool in (workers,spot)",
		"node-pool notin (control-plane)",
		"kubescalesense.io/schedulable",
		"!node.kubernetes.io/exclude",
		"node-pool=workers,zone in (eu-north-1a,eu-north-1b)",
		// An empty value is a legal label value, and means something different
		// from an absent label.
		"node-pool=",
	}

	for _, selector := range selectors {
		t.Run(selector, func(t *testing.T) {
			t.Parallel()

			cfg := validConfig()
			cfg.Resources.NodeLabelSelector = selector

			if err := cfg.Validate(); err != nil {
				t.Errorf("selector %q must be accepted, got: %v", selector, err)
			}
		})
	}
}

// Reporting one problem per restart turns a quick ConfigMap edit into a long
// loop, so the validator must surface all of them at once.
func TestValidate_ReportsEveryProblemAtOnce(t *testing.T) {
	t.Parallel()

	cfg := Default() // no target at all: two problems
	cfg.Controller.Interval = Duration(time.Second)
	cfg.Workload.ItemsPerReplica = 0
	cfg.Controller.LogLevel = "chatty"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected rejection")
	}

	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}

	for _, want := range []string{
		"target.namespace",
		"target.deployment",
		"controller.interval",
		"workload.itemsPerReplica",
		"controller.logLevel",
	} {
		if !hasKey(verr, want) {
			t.Errorf("expected %q among reported problems, got %v", want, verr.Keys())
		}
	}

	if !strings.Contains(err.Error(), "5 problems") {
		t.Errorf("summary should count the problems, got: %v", err)
	}
}

// Every message must name the key, the offending value, the override variable,
// and the requirement. That combination is what makes the error actionable
// without opening the source.
func TestValidationError_MessageIsActionable(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Controller.Interval = Duration(2 * time.Second)

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected rejection")
	}

	msg := err.Error()
	for _, want := range []string{
		"controller.interval",     // the key to edit
		"2s",                      // the value that was rejected
		"must be at least 5s",     // the constraint
		"KSS_CONTROLLER_INTERVAL", // how to override it
		"[CR-2]",                  // the requirement behind the rule
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q\ngot: %s", want, msg)
		}
	}
}

func TestValidate_SmoothingAlphaIgnoredWhenModeIsNone(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Workload.BacklogSmoothing.Mode = SmoothingModeNone
	cfg.Workload.BacklogSmoothing.Alpha = 0 // irrelevant when smoothing is off

	if err := cfg.Validate(); err != nil {
		t.Fatalf("alpha must not be validated when mode is none, got: %v", err)
	}
}

func TestValidate_EmptyListenAddrDisablesListener(t *testing.T) {
	t.Parallel()

	cfg := validConfig()
	cfg.Controller.MetricsAddr = ""
	cfg.Controller.HealthAddr = ""

	if err := cfg.Validate(); err != nil {
		t.Fatalf("empty listen addresses must be allowed, got: %v", err)
	}
}

func hasKey(verr *ValidationError, key string) bool {
	for _, fe := range verr.Errors {
		if fe.Key == key {
			return true
		}
	}
	return false
}
