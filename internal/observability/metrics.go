package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jensilin/KubeScaleSense/internal/config"
	kssmetrics "github.com/jensilin/KubeScaleSense/internal/metrics"
	"github.com/jensilin/KubeScaleSense/internal/resources"
	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// Signal label values for kss_metric_sample_age_seconds.
const (
	SignalBacklog     = "backlog"
	SignalUtilization = "utilization"
)

// Phase label values for kss_reconcile_duration_seconds. Splitting the budget
// by phase is what makes an NFR-02 regression attributable: "reconciles got
// slower" is not actionable, "the node walk got slower" is.
const (
	PhaseObserve = "observe"
	PhaseDecide  = "decide"
	PhaseTotal   = "total"
)

// Metrics holds the registry and every exported series.
//
// A private registry rather than the default one, so that the exposition
// contains exactly the documented set plus the standard process and Go
// collectors — and so that a test can assert on the whole surface without
// interference from whatever another package registered globally.
type Metrics struct {
	registry *prometheus.Registry

	reconcileTotal       *prometheus.CounterVec
	reconcileDuration    *prometheus.HistogramVec
	lastReconcileTime    prometheus.Gauge
	scaleActionsTotal    *prometheus.CounterVec
	currentReplicas      prometheus.Gauge
	desiredReplicas      prometheus.Gauge
	desiredReplicasUncap prometheus.Gauge
	backlogItems         *prometheus.GaugeVec
	backlogItemsSmoothed prometheus.Gauge
	inFlightRequests     prometheus.Gauge
	requestRate          prometheus.Gauge
	processingRate       prometheus.Gauge
	processingLatency    *prometheus.GaugeVec
	signalSourceUp       *prometheus.GaugeVec
	avgCPUUtilization    prometheus.Gauge
	avgMemoryUtilization prometheus.Gauge
	sampleAge            *prometheus.GaugeVec
	fitCapacity          prometheus.Gauge
	blockingDimension    *prometheus.GaugeVec
	candidateNodes       prometheus.Gauge
	excludedNodes        *prometheus.GaugeVec
	freeCPU              prometheus.Gauge
	freeMemory           prometheus.Gauge
	nodeFreeCPU          *prometheus.GaugeVec
	insufficientHolds    *prometheus.CounterVec
	holdBackoffSeconds   prometheus.Gauge
	pendingTargetPods    prometheus.Gauge
	unhealthyTargetPods  prometheus.Gauge
	remediationsTotal    *prometheus.CounterVec
	externalScaleChanges prometheus.Counter
	backoffFitCapacity   prometheus.Gauge
	apiErrorsTotal       *prometheus.CounterVec
	configInfo           *prometheus.GaugeVec
}

// NewMetrics registers the documented metric set.
func NewMetrics() *Metrics {
	m := &Metrics{registry: prometheus.NewRegistry()}

	m.reconcileTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kss_reconcile_total",
		Help: "Reconciles by decision reason code. The primary behavioural signal.",
	}, []string{"reason"})

	m.reconcileDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kss_reconcile_duration_seconds",
		Help:    "Reconcile duration by phase.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
	}, []string{"phase"})

	m.lastReconcileTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_last_reconcile_timestamp_seconds",
		Help: "Unix time of the last completed reconcile. Alert if older than three intervals.",
	})

	m.scaleActionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kss_scale_actions_total",
		Help: "Replica-count mutations by direction and outcome. The applied series counts writes that reached the cluster; dry_run counts decisions deliberately not acted on.",
	}, []string{"direction", "outcome"})

	m.currentReplicas = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_current_replicas",
		Help: "Observed spec.replicas of the target.",
	})

	m.desiredReplicas = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_desired_replicas",
		Help: "Desired replicas after clamping. Sustained divergence from kss_current_replicas is the unmet-demand alert.",
	})

	m.desiredReplicasUncap = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_desired_replicas_uncapped",
		Help: "Desired replicas before clamping, so saturation against maxReplicas is visible rather than hidden.",
	})

	m.backlogItems = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kss_backlog_items",
		Help: "Raw outstanding work items reported by the configured signal source.",
	}, []string{"source"})

	m.backlogItemsSmoothed = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_backlog_items_smoothed",
		Help: "Outstanding work items after smoothing: the value the engine actually used.",
	})

	m.inFlightRequests = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_workload_in_flight_requests",
		Help: "Requests being served now. Distinguishes a busy pool from a queued one.",
	})

	m.requestRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_workload_request_rate",
		Help: "Arrivals per second. Observability only in v0.2.",
	})

	m.processingRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_workload_processing_rate",
		Help: "Completions per second. Observability only in v0.2.",
	})

	m.processingLatency = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kss_workload_processing_latency_seconds",
		Help: "Per-request processing latency, recorded so a latency-driven demand model can later be derived from data.",
	}, []string{"quantile"})

	m.signalSourceUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kss_workload_signal_source_up",
		Help: "1 when the configured source answered the last collection. Distinguishes no work from no answer.",
	}, []string{"source"})

	m.avgCPUUtilization = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_avg_cpu_utilization_percent",
		Help: "Mean CPU utilization over Ready and warm pods, relative to the CPU request.",
	})

	m.avgMemoryUtilization = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_avg_memory_utilization_percent",
		Help: "Mean memory utilization over Ready and warm pods, relative to the memory request. Alerting signal only.",
	})

	m.sampleAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kss_metric_sample_age_seconds",
		Help: "Age of the newest sample per signal. Drives HoldStaleMetrics.",
	}, []string{"signal"})

	m.fitCapacity = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_fit_capacity_pods",
		Help: "Additional target pods placeable right now. The headline resource-awareness metric.",
	})

	m.blockingDimension = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kss_fit_capacity_blocking_dimension",
		Help: "1 for the dimension that bound the fit count. Tells the operator which resource to add.",
	}, []string{"dimension"})

	m.candidateNodes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_candidate_nodes",
		Help: "Nodes that passed every modelled scheduling predicate.",
	})

	m.excludedNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kss_excluded_nodes",
		Help: "Nodes excluded from the candidate set, by reason. Explains a surprising fit capacity.",
	}, []string{"exclusion_reason"})

	m.freeCPU = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_free_requestable_cpu_millicores",
		Help: "Aggregate free requestable CPU over candidate nodes. Reporting only: deliberately not a decision input.",
	})

	m.freeMemory = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_free_requestable_memory_bytes",
		Help: "Aggregate free requestable memory over candidate nodes. Reporting only.",
	})

	m.nodeFreeCPU = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kss_node_free_requestable_cpu_millicores",
		Help: "Per-node free requestable CPU. This is where fragmentation becomes visible.",
	}, []string{"node"})

	m.insufficientHolds = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kss_insufficient_resource_holds_total",
		Help: "Times demand exceeded placeable capacity, by blocking dimension.",
	}, []string{"dimension"})

	m.holdBackoffSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_hold_backoff_seconds",
		Help: "Current backoff interval for repeatedly infeasible scale-ups.",
	})

	m.pendingTargetPods = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_pending_target_pods",
		Help: "Pods of the target that are unschedulable. The L3 check on the L2 estimate.",
	})

	m.unhealthyTargetPods = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_unhealthy_target_pods",
		Help: "Pods of the target scheduled but not Ready.",
	})

	m.remediationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kss_pending_pod_remediations_total",
		Help: "Watchdog interventions by action. Zero throughout Phase 1: the watchdog actuates from P3.",
	}, []string{"action"})

	m.externalScaleChanges = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kss_external_scale_changes_total",
		Help: "Replica-count changes observed that this controller did not make.",
	})

	m.backoffFitCapacity = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kss_backoff_fit_capacity_at_arm",
		Help: "Capacity level the active backoff is a statement about; the reset compares current capacity against it.",
	})

	m.apiErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kss_api_errors_total",
		Help: "Kubernetes API errors by resource, verb, and status code.",
	}, []string{"resource", "verb", "code"})

	m.configInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kss_config_info",
		Help: "Always 1. Labels carry the configuration values needed to correlate behaviour with settings.",
	}, []string{
		"phase", "dry_run", "interval", "min_replicas", "max_replicas",
		"items_per_replica", "target_cpu_percent", "signal_source", "smoothing",
		"pod_request_cpu_milli", "pod_request_memory_mib",
	})

	m.registry.MustRegister(
		m.reconcileTotal, m.reconcileDuration, m.lastReconcileTime, m.scaleActionsTotal,
		m.currentReplicas, m.desiredReplicas, m.desiredReplicasUncap,
		m.backlogItems, m.backlogItemsSmoothed, m.inFlightRequests,
		m.requestRate, m.processingRate, m.processingLatency, m.signalSourceUp,
		m.avgCPUUtilization, m.avgMemoryUtilization, m.sampleAge,
		m.fitCapacity, m.blockingDimension, m.candidateNodes, m.excludedNodes,
		m.freeCPU, m.freeMemory, m.nodeFreeCPU,
		m.insufficientHolds, m.holdBackoffSeconds,
		m.pendingTargetPods, m.unhealthyTargetPods, m.remediationsTotal,
		m.externalScaleChanges, m.backoffFitCapacity, m.apiErrorsTotal, m.configInfo,
	)

	m.initialiseLabelValues()
	return m
}

// initialiseLabelValues pre-creates every known label value at zero.
//
// Without this, a reason code that has not occurred yet is simply absent from
// the exposition, and a dashboard panel for it renders "no data" rather than
// zero — at exactly the moment an operator is trying to establish that
// something has *not* been happening.
func (m *Metrics) initialiseLabelValues() {
	for _, reason := range scaling.Reasons {
		m.reconcileTotal.WithLabelValues(string(reason)).Add(0)
	}
	for _, reason := range resources.ExclusionReasons {
		m.excludedNodes.WithLabelValues(reason).Set(0)
	}
	for _, dim := range []resources.Dimension{
		resources.DimensionCPU, resources.DimensionMemory, resources.DimensionPodSlots, resources.DimensionNone,
	} {
		m.blockingDimension.WithLabelValues(string(dim)).Set(0)
	}
	for _, dim := range []resources.Dimension{
		resources.DimensionCPU, resources.DimensionMemory, resources.DimensionPodSlots,
	} {
		m.insufficientHolds.WithLabelValues(string(dim)).Add(0)
	}
	for _, direction := range []string{"up", "down"} {
		for _, outcome := range []string{"applied", "conflict", "error", "dry_run"} {
			m.scaleActionsTotal.WithLabelValues(direction, outcome).Add(0)
		}
	}
	// The watchdog does not actuate until P3, so this counter is documented as
	// zero for the whole of Phase 1 — which only means anything if the series
	// is actually present and reads zero.
	for _, action := range []string{config.OnPendingRevert, config.OnPendingFreeze, config.OnPendingNone} {
		m.remediationsTotal.WithLabelValues(action).Add(0)
	}
}

// Registry exposes the registry so that tests can gather the exposition
// directly rather than parsing an HTTP response.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the exposition.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// SetConfigInfo publishes the configuration label set.
//
// The gauge is reset first because the effective pod request is one of the
// labels and it changes when the target's template changes; without the reset
// the old series would linger and report a request the target no longer has.
func (m *Metrics) SetConfigInfo(cfg *config.Config, phase string, request resources.Request) {
	m.configInfo.Reset()
	m.configInfo.WithLabelValues(
		phase,
		strconv.FormatBool(cfg.Controller.DryRun),
		cfg.Controller.Interval.Duration().String(),
		strconv.Itoa(int(cfg.Target.MinReplicas)),
		strconv.Itoa(int(cfg.Target.MaxReplicas)),
		strconv.Itoa(cfg.Workload.ItemsPerReplica),
		strconv.Itoa(cfg.Workload.TargetCPUUtilizationPercent),
		cfg.Workload.Signal.Source,
		cfg.Workload.BacklogSmoothing.Mode,
		strconv.FormatInt(request.CPUMilli, 10),
		strconv.FormatInt(request.MemoryMiB(), 10),
	).Set(1)
}

// Observation is everything one reconcile produced, gathered so that the
// metrics update happens in one place instead of being scattered through the
// control flow.
type Observation struct {
	Decision    scaling.Decision
	Snapshot    scaling.Snapshot
	Feasibility resources.Feasibility

	Sample       kssmetrics.Sample
	SignalSource string
	RawBacklog   int64

	// DryRun reports whether this reconcile could have written. It decides
	// which series a scale action lands in: a dry-run decision is recorded as
	// dry_run here, while a live one is recorded by the actuator once the write
	// has actually happened — so "applied" counts writes, not intentions.
	DryRun bool

	Now             time.Time
	ObserveDuration time.Duration
	DecideDuration  time.Duration
	TotalDuration   time.Duration
}

// RecordReconcile publishes one reconcile's worth of state.
func (m *Metrics) RecordReconcile(obs Observation) {
	d := obs.Decision
	s := obs.Snapshot

	m.reconcileTotal.WithLabelValues(string(d.Reason)).Inc()
	m.reconcileDuration.WithLabelValues(PhaseObserve).Observe(obs.ObserveDuration.Seconds())
	m.reconcileDuration.WithLabelValues(PhaseDecide).Observe(obs.DecideDuration.Seconds())
	m.reconcileDuration.WithLabelValues(PhaseTotal).Observe(obs.TotalDuration.Seconds())
	m.lastReconcileTime.Set(float64(obs.Now.Unix()))

	m.currentReplicas.Set(float64(s.CurrentReplicas))
	m.desiredReplicas.Set(float64(d.DesiredClamped))
	m.desiredReplicasUncap.Set(float64(d.DesiredRaw))

	m.signalSourceUp.WithLabelValues(obs.SignalSource).Set(boolToFloat(obs.Sample.Available))
	if obs.Sample.Available {
		m.backlogItems.WithLabelValues(obs.SignalSource).Set(float64(obs.RawBacklog))
		m.backlogItemsSmoothed.Set(float64(s.Backlog.Value))
		m.inFlightRequests.Set(float64(obs.Sample.InFlightRequests))
		m.requestRate.Set(milliToUnit(obs.Sample.RequestRateMilliPerSec))
		m.processingRate.Set(milliToUnit(obs.Sample.ProcessingRateMilliPerSec))
		m.processingLatency.WithLabelValues("mean").Set(milliToUnit(obs.Sample.ProcessingLatencyMillis))
		m.sampleAge.WithLabelValues(SignalBacklog).Set(s.Backlog.Age(obs.Now).Seconds())
	}

	if s.AvgCPUMilliPercent.Available {
		m.avgCPUUtilization.Set(milliToUnit(s.AvgCPUMilliPercent.Value))
		m.sampleAge.WithLabelValues(SignalUtilization).Set(s.AvgCPUMilliPercent.Age(obs.Now).Seconds())
	}
	if s.AvgMemMilliPercent.Available {
		m.avgMemoryUtilization.Set(milliToUnit(s.AvgMemMilliPercent.Value))
	}

	m.recordFeasibility(obs.Feasibility)

	m.pendingTargetPods.Set(float64(s.PendingOurPods))
	m.unhealthyTargetPods.Set(float64(s.UnhealthyOurPods))

	if d.Reason == scaling.ReasonHoldInsufficientResources {
		m.insufficientHolds.WithLabelValues(string(obs.Feasibility.Blocking)).Inc()
	}

	if d.Backoff.Armed() {
		m.holdBackoffSeconds.Set(d.BackoffInterval.Seconds())
		m.backoffFitCapacity.Set(float64(d.Backoff.FitCapacityAtArm))
	} else {
		m.holdBackoffSeconds.Set(0)
		m.backoffFitCapacity.Set(-1)
	}

	// In dry-run an action decision is recorded here, as an intention. In live
	// mode it is not recorded here at all: the actuator increments the counter
	// itself, after the write, with the outcome the write actually had. That
	// split is the whole point — "applied" counts replica counts that reached
	// the cluster, not decisions that hoped to.
	if obs.DryRun && d.Action == scaling.ActionWrite {
		m.scaleActionsTotal.WithLabelValues(string(d.Direction), "dry_run").Inc()
	}
}

// RecordScaleAction counts one attempt to change the replica count, labelled
// with what became of it.
//
// Called by the live actuator rather than by the reconcile loop, because only
// the actuator knows whether the write landed, conflicted, or failed.
func (m *Metrics) RecordScaleAction(direction, outcome string) {
	m.scaleActionsTotal.WithLabelValues(direction, outcome).Inc()
}

func (m *Metrics) recordFeasibility(f resources.Feasibility) {
	m.fitCapacity.Set(float64(f.FitCapacity))
	m.candidateNodes.Set(float64(f.CandidateNodes))
	m.freeCPU.Set(float64(f.FreeCPUMilli))
	m.freeMemory.Set(float64(f.FreeMemoryBytes))

	for _, reason := range resources.ExclusionReasons {
		m.excludedNodes.WithLabelValues(reason).Set(float64(f.Exclusions[reason]))
	}

	for _, dim := range []resources.Dimension{
		resources.DimensionCPU, resources.DimensionMemory, resources.DimensionPodSlots, resources.DimensionNone,
	} {
		m.blockingDimension.WithLabelValues(string(dim)).Set(boolToFloat(f.Blocking == dim))
	}

	// Reset before repopulating so that a node which left the cluster stops
	// reporting a stale free-capacity figure forever.
	m.nodeFreeCPU.Reset()
	for i := range f.Nodes {
		m.nodeFreeCPU.WithLabelValues(f.Nodes[i].Name).Set(float64(f.Nodes[i].FreeCPUMilli))
	}
}

// RecordExternalScaleChange counts a replica change this controller did not
// make.
func (m *Metrics) RecordExternalScaleChange() {
	m.externalScaleChanges.Inc()
}

// RecordAPIError counts a failed Kubernetes read.
func (m *Metrics) RecordAPIError(resource, verb, code string) {
	m.apiErrorsTotal.WithLabelValues(resource, verb, code).Inc()
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// milliToUnit converts the engine's integer milli-units back to the natural
// unit for exposition. Metrics are float64 by protocol, so this is the one
// place the integer discipline is deliberately relaxed — after every decision
// has already been made.
func milliToUnit(milli int64) float64 {
	return float64(milli) / 1000
}
