package observability

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/jensilin/KubeScaleSense/internal/config"
	kssmetrics "github.com/jensilin/KubeScaleSense/internal/metrics"
	"github.com/jensilin/KubeScaleSense/internal/resources"
	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

var now = time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)

// documentedMetrics is the metric set of architecture § 8.1.
//
// kss_leader is deliberately absent: there is no Lease until P4, and a gauge
// reporting leadership in a binary that has no leader election would be a
// fabricated signal.
var documentedMetrics = []string{
	"kss_reconcile_total",
	"kss_reconcile_duration_seconds",
	"kss_last_reconcile_timestamp_seconds",
	"kss_scale_actions_total",
	"kss_current_replicas",
	"kss_desired_replicas",
	"kss_desired_replicas_uncapped",
	"kss_backlog_items",
	"kss_backlog_items_smoothed",
	"kss_workload_in_flight_requests",
	"kss_workload_request_rate",
	"kss_workload_processing_rate",
	"kss_workload_processing_latency_seconds",
	"kss_workload_signal_source_up",
	"kss_avg_cpu_utilization_percent",
	"kss_avg_memory_utilization_percent",
	"kss_metric_sample_age_seconds",
	"kss_fit_capacity_pods",
	"kss_fit_capacity_blocking_dimension",
	"kss_candidate_nodes",
	"kss_excluded_nodes",
	"kss_free_requestable_cpu_millicores",
	"kss_free_requestable_memory_bytes",
	"kss_node_free_requestable_cpu_millicores",
	"kss_insufficient_resource_holds_total",
	"kss_hold_backoff_seconds",
	"kss_pending_target_pods",
	"kss_unhealthy_target_pods",
	"kss_pending_pod_remediations_total",
	"kss_external_scale_changes_total",
	"kss_backoff_fit_capacity_at_arm",
	"kss_api_errors_total",
	"kss_config_info",
}

func TestNewMetrics_RegistersTheDocumentedSet(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.RecordReconcile(scaleUpObservation())
	m.SetConfigInfo(testConfig(), "P1", resources.Request{CPUMilli: 500, MemoryBytes: 512 * 1024 * 1024})
	m.RecordAPIError("nodes", "list", "503")
	m.RecordExternalScaleChange()

	present := gatheredNames(t, m)

	for _, name := range documentedMetrics {
		if !present[name] {
			t.Errorf("%s is missing from the exposition", name)
		}
	}
}

// A reason code that has not occurred yet must report zero rather than being
// absent, because a dashboard panel for an absent series renders "no data" at
// exactly the moment an operator is trying to establish that something has
// *not* been happening.
func TestNewMetrics_PreCreatesEveryLabelValue(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	families := gatherByName(t, m)

	t.Run("every reason code", func(t *testing.T) {
		labels := labelValues(families["kss_reconcile_total"], "reason")
		for _, reason := range scaling.Reasons {
			if !labels[string(reason)] {
				t.Errorf("kss_reconcile_total has no series for reason %q", reason)
			}
		}
		if len(labels) != len(scaling.Reasons) {
			t.Errorf("kss_reconcile_total has %d series, want %d — one per reason code", len(labels), len(scaling.Reasons))
		}
	})

	t.Run("every exclusion reason", func(t *testing.T) {
		labels := labelValues(families["kss_excluded_nodes"], "exclusion_reason")
		for _, reason := range resources.ExclusionReasons {
			if !labels[reason] {
				t.Errorf("kss_excluded_nodes has no series for %q", reason)
			}
		}
	})

	t.Run("every blocking dimension", func(t *testing.T) {
		labels := labelValues(families["kss_fit_capacity_blocking_dimension"], "dimension")
		for _, dim := range []resources.Dimension{
			resources.DimensionCPU, resources.DimensionMemory, resources.DimensionPodSlots, resources.DimensionNone,
		} {
			if !labels[string(dim)] {
				t.Errorf("kss_fit_capacity_blocking_dimension has no series for %q", dim)
			}
		}
	})
}

// Phase 1 performs no mutation, so the "applied" series must stay honestly at
// zero while the decision is still counted as a dry run. A dry-run trace that
// incremented "applied" would be indistinguishable from a live one.
func TestRecordReconcile_ScaleActionsAreCountedAsDryRun(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.RecordReconcile(scaleUpObservation())

	families := gatherByName(t, m)

	if got := counterWith(families["kss_scale_actions_total"], map[string]string{"direction": "up", "outcome": "dry_run"}); got != 1 {
		t.Errorf("dry_run count = %v, want 1", got)
	}
	if got := counterWith(families["kss_scale_actions_total"], map[string]string{"direction": "up", "outcome": "applied"}); got != 0 {
		t.Errorf("applied count = %v, want 0: Phase 1 applies nothing", got)
	}
}

func TestRecordReconcile_PublishesTheDecision(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.RecordReconcile(scaleUpObservation())

	families := gatherByName(t, m)

	for _, tc := range []struct {
		metric string
		want   float64
	}{
		{metric: "kss_current_replicas", want: 2},
		{metric: "kss_desired_replicas", want: 8},
		{metric: "kss_desired_replicas_uncapped", want: 8},
		{metric: "kss_fit_capacity_pods", want: 4},
		{metric: "kss_candidate_nodes", want: 3},
		{metric: "kss_free_requestable_cpu_millicores", want: 2600},
		{metric: "kss_backlog_items_smoothed", want: 400},
		{metric: "kss_pending_target_pods", want: 0},
		// Milli-percent is converted back to the natural unit for exposition:
		// the one place the integer discipline is relaxed, after every decision
		// has already been made.
		{metric: "kss_avg_cpu_utilization_percent", want: 90},
	} {
		if got := gaugeValue(families[tc.metric]); got != tc.want {
			t.Errorf("%s = %v, want %v", tc.metric, got, tc.want)
		}
	}

	// The blocking dimension is a one-hot gauge, so exactly one series is 1.
	if got := gaugeWith(families["kss_fit_capacity_blocking_dimension"], map[string]string{"dimension": "cpu"}); got != 1 {
		t.Errorf("blocking dimension cpu = %v, want 1", got)
	}
	if got := gaugeWith(families["kss_fit_capacity_blocking_dimension"], map[string]string{"dimension": "memory"}); got != 0 {
		t.Errorf("blocking dimension memory = %v, want 0", got)
	}

	// Per-node free CPU is where fragmentation becomes visible.
	if got := gaugeWith(families["kss_node_free_requestable_cpu_millicores"], map[string]string{"node": "w-1"}); got != 600 {
		t.Errorf("w-1 free CPU = %v, want 600", got)
	}
}

// An unavailable signal must not publish a backlog of zero: the gauge would
// then be indistinguishable from a genuinely idle pipeline.
func TestRecordReconcile_UnavailableSignalPublishesNoBacklog(t *testing.T) {
	t.Parallel()

	m := NewMetrics()

	obs := scaleUpObservation()
	obs.Sample = kssmetrics.Unavailable()
	obs.Snapshot.Backlog = scaling.Signal[int64]{}
	obs.Decision = scaling.Decision{Reason: scaling.ReasonHoldStaleMetrics, Action: scaling.ActionNone}
	m.RecordReconcile(obs)

	families := gatherByName(t, m)

	if got := gaugeWith(families["kss_workload_signal_source_up"], map[string]string{"source": "synthetic"}); got != 0 {
		t.Errorf("signal_source_up = %v, want 0", got)
	}
	if family := families["kss_backlog_items"]; family != nil && len(family.GetMetric()) != 0 {
		t.Error("kss_backlog_items must have no series while the source is unavailable")
	}
}

func TestRecordReconcile_BackoffGauges(t *testing.T) {
	t.Parallel()

	t.Run("armed", func(t *testing.T) {
		t.Parallel()

		m := NewMetrics()
		obs := scaleUpObservation()
		obs.Decision.Reason = scaling.ReasonHoldInsufficientResources
		obs.Decision.Action = scaling.ActionNone
		obs.Decision.Backoff = scaling.BackoffState{Attempts: 2, FitCapacityAtArm: 3}
		obs.Decision.BackoffInterval = time.Minute
		m.RecordReconcile(obs)

		families := gatherByName(t, m)
		if got := gaugeValue(families["kss_hold_backoff_seconds"]); got != 60 {
			t.Errorf("kss_hold_backoff_seconds = %v, want 60", got)
		}
		if got := gaugeValue(families["kss_backoff_fit_capacity_at_arm"]); got != 3 {
			t.Errorf("kss_backoff_fit_capacity_at_arm = %v, want 3", got)
		}
		if got := counterWith(families["kss_insufficient_resource_holds_total"], map[string]string{"dimension": "cpu"}); got != 1 {
			t.Errorf("insufficient holds for cpu = %v, want 1", got)
		}
	})

	// -1, not 0: "no backoff" must be distinguishable from "armed when the
	// cluster genuinely had no room".
	t.Run("cleared", func(t *testing.T) {
		t.Parallel()

		m := NewMetrics()
		m.RecordReconcile(scaleUpObservation())

		families := gatherByName(t, m)
		if got := gaugeValue(families["kss_hold_backoff_seconds"]); got != 0 {
			t.Errorf("kss_hold_backoff_seconds = %v, want 0", got)
		}
		if got := gaugeValue(families["kss_backoff_fit_capacity_at_arm"]); got != -1 {
			t.Errorf("kss_backoff_fit_capacity_at_arm = %v, want -1", got)
		}
	})
}

// A node that left the cluster must stop reporting a stale free-capacity
// figure forever.
func TestRecordReconcile_ForgetsDepartedNodes(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.RecordReconcile(scaleUpObservation())

	shrunk := scaleUpObservation()
	shrunk.Feasibility.Nodes = shrunk.Feasibility.Nodes[:1]
	m.RecordReconcile(shrunk)

	families := gatherByName(t, m)
	labels := labelValues(families["kss_node_free_requestable_cpu_millicores"], "node")

	if labels["w-2"] {
		t.Error("w-2 left the cluster but is still reporting free capacity")
	}
	if !labels["w-1"] {
		t.Error("w-1 should still be reported")
	}
}

func TestSetConfigInfo_CarriesTheSettingsNeededToCorrelateBehaviour(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.SetConfigInfo(testConfig(), "P1", resources.Request{CPUMilli: 500, MemoryBytes: 512 * 1024 * 1024})

	families := gatherByName(t, m)
	family := families["kss_config_info"]
	if family == nil || len(family.GetMetric()) != 1 {
		t.Fatalf("kss_config_info should have exactly one series, got %v", family)
	}

	labels := map[string]string{}
	for _, pair := range family.GetMetric()[0].GetLabel() {
		labels[pair.GetName()] = pair.GetValue()
	}

	for key, want := range map[string]string{
		"phase":                  "P1",
		"dry_run":                "true",
		"interval":               "15s",
		"min_replicas":           "1",
		"max_replicas":           "12",
		"items_per_replica":      "50",
		"target_cpu_percent":     "70",
		"signal_source":          "synthetic",
		"smoothing":              "ewma",
		"pod_request_cpu_milli":  "500",
		"pod_request_memory_mib": "512",
	} {
		if labels[key] != want {
			t.Errorf("label %s = %q, want %q", key, labels[key], want)
		}
	}
}

// The pod request is a label, and it changes when the target's template
// changes. Without a reset the old series would linger and report a request the
// target no longer has.
func TestSetConfigInfo_ReplacesTheOldSeries(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.SetConfigInfo(testConfig(), "P1", resources.Request{CPUMilli: 500, MemoryBytes: 512 * 1024 * 1024})
	m.SetConfigInfo(testConfig(), "P1", resources.Request{CPUMilli: 750, MemoryBytes: 512 * 1024 * 1024})

	families := gatherByName(t, m)
	if got := len(families["kss_config_info"].GetMetric()); got != 1 {
		t.Errorf("kss_config_info has %d series, want 1: the old request must not linger", got)
	}
}

// --- the HTTP surface -----------------------------------------------------

func TestServer_Endpoints(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	metrics := NewMetrics()

	notReady := "caches have not synced"
	ready := func() error {
		if notReady != "" {
			return errors.New(notReady)
		}
		return nil
	}

	server := NewServer(cfg, metrics, ready, discardLogger())
	if len(server.servers) != 2 {
		t.Fatalf("expected two listeners for two addresses, got %d", len(server.servers))
	}

	metricsHandler := server.servers[0].Handler
	healthHandler := server.servers[1].Handler

	t.Run("metrics", func(t *testing.T) {
		rec := get(metricsHandler, "/metrics")
		if rec.Code != http.StatusOK {
			t.Fatalf("/metrics = %d, want 200", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "kss_reconcile_total") {
			t.Error("/metrics should expose the kss series")
		}
	})

	// /healthz answers "is the process alive?" and performs no dependency
	// checks: failing liveness because metrics-server is down would restart a
	// controller that is behaving exactly as designed.
	t.Run("healthz ignores dependencies", func(t *testing.T) {
		rec := get(healthHandler, "/healthz")
		if rec.Code != http.StatusOK {
			t.Errorf("/healthz = %d, want 200 even while not ready", rec.Code)
		}
	})

	t.Run("readyz reports why", func(t *testing.T) {
		rec := get(healthHandler, "/readyz")
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("/readyz = %d, want 503", rec.Code)
		}
		// The reason matters: the readiness body is often the first place an
		// operator looks when a pod will not come up.
		if !strings.Contains(rec.Body.String(), notReady) {
			t.Errorf("/readyz body = %q, want it to explain the reason", rec.Body.String())
		}

		notReady = ""
		if rec := get(healthHandler, "/readyz"); rec.Code != http.StatusOK {
			t.Errorf("/readyz = %d after becoming ready, want 200", rec.Code)
		}
	})
}

// Binding the same address twice would fail, so identical addresses fold onto
// one listener.
func TestServer_FoldsRoutesWhenAddressesMatch(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Controller.HealthAddr = cfg.Controller.MetricsAddr

	server := NewServer(cfg, NewMetrics(), func() error { return nil }, discardLogger())
	if len(server.servers) != 1 {
		t.Fatalf("expected one listener for one address, got %d", len(server.servers))
	}

	handler := server.servers[0].Handler
	for _, path := range []string{"/metrics", "/healthz", "/readyz"} {
		if rec := get(handler, path); rec.Code != http.StatusOK {
			t.Errorf("%s = %d on the folded listener, want 200", path, rec.Code)
		}
	}
}

func TestServer_StartAndShutdown(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	// Port 0 lets the OS choose, so the test cannot collide with anything.
	cfg.Controller.MetricsAddr = "127.0.0.1:0"
	cfg.Controller.HealthAddr = "127.0.0.1:0"

	server := NewServer(cfg, NewMetrics(), func() error { return nil }, discardLogger())
	server.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	if err := server.Err(); err != nil {
		t.Errorf("Err = %v, want nil", err)
	}
}

// --- the decision log -----------------------------------------------------

// The log line must reconstruct the whole derivation: an operator asking "why 4
// and not 8?" should never have to re-run the controller to find out.
func TestLogDecision_CarriesTheDocumentedFieldSet(t *testing.T) {
	t.Parallel()

	var buf strings.Builder
	log := slog.New(slog.NewJSONHandler(&buf, nil))

	LogDecision(log, scaleUpObservation(), true)

	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &record); err != nil {
		t.Fatalf("the decision log must be valid JSON: %v\n%s", err, buf.String())
	}

	// The whole derivation chain, from the raw signal to the replica count that
	// would have been written.
	for _, field := range []string{
		"reason", "direction", "dry_run", "blocked",
		"current", "ready", "would_target",
		"desiredBacklog", "desiredUtilization", "desiredRaw", "desiredClamped",
		"stepLimited", "requestedDelta", "deficit",
		"backlogAvailable", "backlog", "backlogSmoothed", "sampleAgeSec",
		"cpuAvailable", "avgCpuPct",
		"podRequestCpuMilli", "podRequestMemMiB",
		"candidateNodes", "totalNodes", "excludedNodes",
		"fitCapacity", "rawFitSum", "blocking", "freeCpuMilli", "freeMemMiB",
		"pendingPods", "unhealthyPods",
		"backoffAttempts", "backoffSec", "backoffFitCapacityAtArm",
		"rolloutInProgress", "hpaPresent", "externalDrifts", "reconcileSec",
	} {
		if _, ok := record[field]; !ok {
			t.Errorf("the decision log is missing %q\nrecord: %v", field, record)
		}
	}

	if record["reason"] != string(scaling.ReasonScaleUp) {
		t.Errorf("reason = %v, want %v", record["reason"], scaling.ReasonScaleUp)
	}
	if record["dry_run"] != true {
		t.Errorf("dry_run = %v, want true", record["dry_run"])
	}
	// would_target, not target: the name itself has to say that nothing was
	// written.
	if record["would_target"] != float64(6) {
		t.Errorf("would_target = %v, want 6", record["would_target"])
	}
	// Eight zeroes would bury the one exclusion that happened.
	if exclusions, ok := record["excludedNodes"].(map[string]any); !ok || len(exclusions) != 1 {
		t.Errorf("excludedNodes = %v, want only the non-zero reasons", record["excludedNodes"])
	}
}

// A hold that blocks demand is a state an operator may need to act on — add
// capacity, raise a limit, fix an image — so it is a warning. The steady state
// and the actions are info, or a quiet cluster would log warnings forever.
func TestLogDecision_LevelsMatchOperatorAction(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		reason  scaling.Reason
		blocked bool
		want    string
	}{
		{name: "scale up", reason: scaling.ReasonScaleUp, want: "INFO"},
		{name: "insufficient resources", reason: scaling.ReasonHoldInsufficientResources, blocked: true, want: "WARN"},
		{name: "stale metrics", reason: scaling.ReasonHoldStaleMetrics, blocked: true, want: "WARN"},
		{name: "within tolerance", reason: scaling.ReasonNoChangeWithinTolerance, blocked: true, want: "INFO"},
		{name: "cooldown", reason: scaling.ReasonHoldCooldown, blocked: true, want: "WARN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var buf strings.Builder
			obs := scaleUpObservation()
			obs.Decision.Reason = tc.reason
			obs.Decision.Blocked = tc.blocked

			LogDecision(slog.New(slog.NewJSONHandler(&buf, nil)), obs, true)

			var record map[string]any
			if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &record); err != nil {
				t.Fatalf("unmarshalling the log line: %v", err)
			}
			if record["level"] != tc.want {
				t.Errorf("level = %v, want %v", record["level"], tc.want)
			}
			if record["msg"] != "decision" {
				t.Errorf("msg = %v, want %q — the message is the stable hook for log queries", record["msg"], "decision")
			}
		})
	}
}

// The per-node breakdown is debug-level: it is the fragmentation evidence, and
// it is one line per node on every reconcile.
func TestLogNodeDetail_IsDebugLevel(t *testing.T) {
	t.Parallel()

	var atInfo strings.Builder
	LogNodeDetail(slog.New(slog.NewJSONHandler(&atInfo, &slog.HandlerOptions{Level: slog.LevelInfo})), scaleUpObservation())
	if atInfo.Len() != 0 {
		t.Errorf("per-node detail must not be emitted at info level, got: %s", atInfo.String())
	}

	var atDebug strings.Builder
	LogNodeDetail(slog.New(slog.NewJSONHandler(&atDebug, &slog.HandlerOptions{Level: slog.LevelDebug})), scaleUpObservation())
	if !strings.Contains(atDebug.String(), "w-1") {
		t.Errorf("per-node detail should name the nodes at debug level, got: %s", atDebug.String())
	}
}

// --- helpers ---------------------------------------------------------------

// scaleUpObservation is the reference example of resource-calculation § 7
// feeding a ScaleUp: three candidate nodes, F = 4, demand for 8 replicas.
func scaleUpObservation() Observation {
	feasibility := resources.Feasibility{
		PodRequest:      resources.Request{CPUMilli: 500, MemoryBytes: 512 * 1024 * 1024},
		FitCapacity:     4,
		RawFitSum:       5,
		CandidateNodes:  3,
		TotalNodes:      4,
		Exclusions:      map[string]int32{resources.ExclusionTaint: 1},
		Blocking:        resources.DimensionCPU,
		FreeCPUMilli:    2600,
		FreeMemoryBytes: 7680 * 1024 * 1024,
		Nodes: []resources.NodeFit{
			{Name: "w-1", FreeCPUMilli: 600, Fit: 1, Binding: resources.DimensionCPU},
			{Name: "w-2", FreeCPUMilli: 2000, Fit: 4, Binding: resources.DimensionCPU},
		},
	}

	snapshot := scaling.Snapshot{
		Valid:              true,
		CurrentReplicas:    2,
		ReadyReplicas:      2,
		PodRequest:         feasibility.PodRequest,
		Backlog:            scaling.Signal[int64]{Value: 400, SampledAt: now, Available: true},
		BacklogRaw:         400,
		AvgCPUMilliPercent: scaling.Signal[int64]{Value: 90_000, SampledAt: now, Available: true},
		AvgMemMilliPercent: scaling.Signal[int64]{Value: 40_000, SampledAt: now, Available: true},
		FitCapacity:        4,
		CandidateNodes:     3,
		Blocking:           resources.DimensionCPU,
		FreeCPUMilli:       2600,
		HoldBackoff:        scaling.ClearBackoff(),
	}

	return Observation{
		Decision: scaling.Decision{
			Reason:          scaling.ReasonScaleUp,
			Direction:       scaling.DirectionUp,
			Action:          scaling.ActionWrite,
			CurrentReplicas: 2,
			TargetReplicas:  6,
			DesiredBacklog:  8,
			DesiredRaw:      8,
			DesiredClamped:  8,
			StepLimited:     6,
			RequestedDelta:  4,
			FitCapacity:     4,
			Blocking:        resources.DimensionCPU,
			Backoff:         scaling.ClearBackoff(),
			DemandComputed:  true,
		},
		Snapshot:    snapshot,
		Feasibility: feasibility,
		Sample: kssmetrics.Sample{
			PressureItems:             400,
			InFlightRequests:          12,
			RequestRateMilliPerSec:    6000,
			ProcessingRateMilliPerSec: 4500,
			ProcessingLatencyMillis:   250,
			SampledAt:                 now,
			Available:                 true,
		},
		SignalSource:    config.SignalSourceSynthetic,
		RawBacklog:      400,
		Now:             now,
		ObserveDuration: 3 * time.Millisecond,
		DecideDuration:  100 * time.Microsecond,
		TotalDuration:   4 * time.Millisecond,
	}
}

func testConfig() *config.Config {
	cfg := config.Default()
	cfg.Target.Namespace = "data-pipeline"
	cfg.Target.Deployment = "normalizer"
	return cfg
}

func gatherByName(t *testing.T, m *Metrics) map[string]*dto.MetricFamily {
	t.Helper()

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}

	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		byName[family.GetName()] = family
	}
	return byName
}

func gatheredNames(t *testing.T, m *Metrics) map[string]bool {
	t.Helper()

	names := make(map[string]bool)
	for name := range gatherByName(t, m) {
		names[name] = true
	}
	return names
}

func labelValues(family *dto.MetricFamily, label string) map[string]bool {
	values := make(map[string]bool)
	if family == nil {
		return values
	}
	for _, metric := range family.GetMetric() {
		for _, pair := range metric.GetLabel() {
			if pair.GetName() == label {
				values[pair.GetValue()] = true
			}
		}
	}
	return values
}

func gaugeValue(family *dto.MetricFamily) float64 {
	if family == nil || len(family.GetMetric()) == 0 {
		return 0
	}
	return family.GetMetric()[0].GetGauge().GetValue()
}

func gaugeWith(family *dto.MetricFamily, want map[string]string) float64 {
	metric := find(family, want)
	if metric == nil {
		return 0
	}
	return metric.GetGauge().GetValue()
}

func counterWith(family *dto.MetricFamily, want map[string]string) float64 {
	metric := find(family, want)
	if metric == nil {
		return -1 // distinguishable from a series that exists and reads zero
	}
	return metric.GetCounter().GetValue()
}

func find(family *dto.MetricFamily, want map[string]string) *dto.Metric {
	if family == nil {
		return nil
	}
	for _, metric := range family.GetMetric() {
		matches := 0
		for _, pair := range metric.GetLabel() {
			if value, ok := want[pair.GetName()]; ok && value == pair.GetValue() {
				matches++
			}
		}
		if matches == len(want) {
			return metric
		}
	}
	return nil
}

func get(handler http.Handler, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}
