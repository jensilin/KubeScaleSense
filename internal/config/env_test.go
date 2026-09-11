package config

import (
	"slices"
	"strings"
	"testing"
)

// The naming rule is quoted verbatim in docs/requirements.md § 8, so it is
// pinned by example rather than only by construction.
func TestEnvVarFor(t *testing.T) {
	t.Parallel()

	tests := []struct{ key, want string }{
		{"scaling.scaleUpCooldown", "KSS_SCALING_SCALEUPCOOLDOWN"}, // the documented example
		{"controller.interval", "KSS_CONTROLLER_INTERVAL"},
		{"target.minReplicas", "KSS_TARGET_MINREPLICAS"},
		{"workload.signal.syntheticPath", "KSS_WORKLOAD_SIGNAL_SYNTHETICPATH"},
		{"workload.backlogSmoothing.alpha", "KSS_WORKLOAD_BACKLOGSMOOTHING_ALPHA"},
		{"scaling.holdBackoff.factor", "KSS_SCALING_HOLDBACKOFF_FACTOR"},
		{"", ""},
	}

	for _, tt := range tests {
		if got := EnvVarFor(tt.key); got != tt.want {
			t.Errorf("EnvVarFor(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

// Every documented key must be overridable. Deriving the list by reflection is
// what stops the override surface from drifting as fields are added, so the
// derived set is checked against the documented parameter list.
func TestKeys_CoversDocumentedSchema(t *testing.T) {
	t.Parallel()

	keys := Keys()

	documented := []string{
		"controller.interval", "controller.leaderElection", "controller.dryRun",
		"controller.metricsAddr", "controller.healthAddr", "controller.logLevel",
		"controller.logFormat",
		"target.namespace", "target.deployment", "target.minReplicas", "target.maxReplicas",
		"workload.itemsPerReplica", "workload.targetCPUUtilizationPercent",
		"workload.metricsStaleAfter", "workload.podWarmupPeriod",
		"workload.backlogSmoothing.mode", "workload.backlogSmoothing.alpha",
		"workload.signal.source", "workload.signal.endpoint",
		"workload.signal.timeout", "workload.signal.syntheticPath",
		"resources.perNodeReserveCPUMilli", "resources.perNodeReserveMemoryMiB",
		"resources.fitCapacityMarginPods", "resources.respectNodeSelector",
		"resources.respectNodeAffinity", "resources.respectTaints",
		"resources.nodeLabelSelector",
		"scaling.scaleUpCooldown", "scaling.scaleDownCooldown",
		"scaling.scaleDownStabilizationWindow", "scaling.scaleDownWindowCoverage",
		"scaling.tolerancePercent", "scaling.maxScaleUpStep", "scaling.maxScaleDownStep",
		"scaling.allowPartialScaleUp", "scaling.externalChangeTolerance",
		"scaling.holdBackoff.initial", "scaling.holdBackoff.max", "scaling.holdBackoff.factor",
		"pending.pendingPodTimeout", "pending.onPendingTimeout", "pending.podStartupTimeout",
	}

	for _, key := range documented {
		if !slices.Contains(keys, key) {
			t.Errorf("documented key %q is not in the schema", key)
		}
	}
	if len(keys) != len(documented) {
		t.Errorf("schema has %d keys but %d are documented; the two lists must match\nschema: %v",
			len(keys), len(documented), keys)
	}
}

func TestEnvVars_AllPrefixed(t *testing.T) {
	t.Parallel()

	vars := EnvVars()
	if len(vars) == 0 {
		t.Fatal("expected a non-empty override list")
	}
	for _, name := range vars {
		if !strings.HasPrefix(name, EnvPrefix) {
			t.Errorf("%q is missing the %s prefix", name, EnvPrefix)
		}
		if strings.ToUpper(name) != name {
			t.Errorf("%q should be upper-case", name)
		}
	}
	if !slices.IsSorted(vars) {
		t.Error("EnvVars() should be sorted for stable output")
	}
}

// An empty override is treated as unset. Otherwise a Kubernetes manifest that
// declares `value: ""` for an optional variable would blank out a default.
func TestApplyEnvOverrides_EmptyValueIsIgnored(t *testing.T) {
	t.Parallel()

	cfg := Default()
	env := map[string]string{"KSS_CONTROLLER_LOGLEVEL": ""}

	err := applyEnvOverrides(cfg,
		func(k string) string { return env[k] },
		func() []string { return []string{"KSS_CONTROLLER_LOGLEVEL="} },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Controller.LogLevel != DefaultLogLevel {
		t.Errorf("logLevel = %q, want the default %q to be preserved", cfg.Controller.LogLevel, DefaultLogLevel)
	}
}

// Non-KSS_ variables must be left alone; the controller runs in a pod full of
// unrelated environment entries.
func TestApplyEnvOverrides_IgnoresForeignVariables(t *testing.T) {
	t.Parallel()

	cfg := Default()
	err := applyEnvOverrides(cfg,
		func(string) string { return "" },
		func() []string { return []string{"PATH=/usr/bin", "HOME=/root", "KUBERNETES_SERVICE_HOST=10.0.0.1"} },
	)
	if err != nil {
		t.Fatalf("foreign environment variables must be ignored, got: %v", err)
	}
}
