package metrics

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"

	"github.com/jensilin/KubeScaleSense/internal/resources"
)

// percentMilliScale converts a usage/request ratio into milli-percent, the unit
// the decision engine consumes.
const percentMilliScale int64 = 100 * 1000

// PodMetricsLister reads pod utilization from the Kubernetes Metrics API.
//
// A one-method interface, so the utilization arithmetic is testable with a
// handful of fixtures and no cluster. There is no watch support on
// metrics.k8s.io, so this is a live read each reconcile rather than an informer.
type PodMetricsLister interface {
	ListPodMetrics(ctx context.Context, namespace string) ([]metricsv1beta1.PodMetrics, error)
}

// Utilization is the aggregated pod utilization signal.
type Utilization struct {
	// AvgCPUMilliPercent is mean CPU usage as milli-percent of the pod's CPU
	// *request* — not of its limit, and not of the node.
	//
	// The request is what the pod is entitled to and what the fit calculation
	// reserves, so both halves of the system speak the same unit. Measuring
	// against the node would make the signal depend on which node the scheduler
	// happened to choose.
	AvgCPUMilliPercent int64

	// AvgMemMilliPercent is collected and exported but never raises the desired
	// replica count in v0.1: for this workload high memory usage indicates
	// buffer sizing rather than a throughput deficit, and adding replicas does
	// not reduce per-pod memory.
	AvgMemMilliPercent int64

	EligiblePods int32
	SampledAt    time.Time
	Available    bool
}

// UtilizationCollector averages utilization over the target's Ready and warm
// pods.
type UtilizationCollector struct {
	lister PodMetricsLister
}

// NewUtilizationCollector wires a collector to a metrics reader.
func NewUtilizationCollector(lister PodMetricsLister) *UtilizationCollector {
	return &UtilizationCollector{lister: lister}
}

// Collect averages CPU and memory utilization over the eligible pods of the
// target.
//
// Eligibility is Ready *and* warm, which is two corrections to the obvious
// implementation rather than one:
//
// Including starting pods would drag the average down exactly when a scale-up is
// in progress, suppressing the next one — a self-defeating feedback path.
//
// Readiness alone is not enough either. A pod that has passed its probe but has
// not yet been sent a request also reports near-zero CPU, so pods Ready for less
// than workload.podWarmupPeriod are excluded too. Upstream HPA carries two
// dedicated knobs for the same effect, which is good evidence the problem is
// real rather than theoretical (FR-28, DR-05).
//
// If that leaves no eligible pod, the signal is reported unavailable rather than
// fabricated. The engine then decides what a missing signal means.
func (u *UtilizationCollector) Collect(
	ctx context.Context,
	namespace string,
	pods []*corev1.Pod,
	request resources.Request,
	warmup time.Duration,
	now time.Time,
) (Utilization, error) {
	if request.CPUMilli <= 0 || request.MemoryBytes <= 0 {
		// Without a request there is no denominator. Startup validation rejects
		// this, so reaching it means the template changed underneath us.
		return Utilization{}, nil
	}

	eligible := make(map[string]struct{}, len(pods))
	for _, pod := range pods {
		if isReadyAndWarm(pod, warmup, now) {
			eligible[pod.Name] = struct{}{}
		}
	}
	if len(eligible) == 0 {
		return Utilization{Available: false}, nil
	}

	podMetrics, err := u.lister.ListPodMetrics(ctx, namespace)
	if err != nil {
		// metrics.k8s.io being absent is an expected state, not a fatal one:
		// pressure-only scaling continues and only the utilization safety net
		// is lost (IT-10).
		return Utilization{Available: false}, err
	}

	var (
		cpuMilliSum  int64
		memorySum    int64
		counted      int32
		oldestSample time.Time
	)
	for i := range podMetrics {
		pm := &podMetrics[i]
		if _, ok := eligible[pm.Name]; !ok {
			continue
		}

		cpu, memory := sumContainerUsage(pm)
		cpuMilliSum += cpu
		memorySum += memory
		counted++

		// The oldest sample sets the age of the aggregate: the average is only
		// as fresh as its stalest contributor.
		if ts := pm.Timestamp.Time; !ts.IsZero() && (oldestSample.IsZero() || ts.Before(oldestSample)) {
			oldestSample = ts
		}
	}

	if counted == 0 {
		// Pods are eligible but metrics-server has no samples for them yet.
		return Utilization{Available: false}, nil
	}
	if oldestSample.IsZero() {
		oldestSample = now
	}

	return Utilization{
		AvgCPUMilliPercent: cpuMilliSum * percentMilliScale / (int64(counted) * request.CPUMilli),
		AvgMemMilliPercent: memorySum * percentMilliScale / (int64(counted) * request.MemoryBytes),
		EligiblePods:       counted,
		SampledAt:          oldestSample,
		Available:          true,
	}, nil
}

// sumContainerUsage totals usage across a pod's containers, matching the way
// the effective request totals its containers' requests.
func sumContainerUsage(pm *metricsv1beta1.PodMetrics) (cpuMilli, memoryBytes int64) {
	for i := range pm.Containers {
		usage := pm.Containers[i].Usage
		if q, ok := usage[corev1.ResourceCPU]; ok {
			cpuMilli += q.MilliValue()
		}
		if q, ok := usage[corev1.ResourceMemory]; ok {
			memoryBytes += q.Value()
		}
	}
	return cpuMilli, memoryBytes
}

// isReadyAndWarm reports whether a pod may contribute to the average.
func isReadyAndWarm(pod *corev1.Pod, warmup time.Duration, now time.Time) bool {
	if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
		return false
	}
	for i := range pod.Status.Conditions {
		cond := &pod.Status.Conditions[i]
		if cond.Type != corev1.PodReady {
			continue
		}
		if cond.Status != corev1.ConditionTrue {
			return false
		}
		readySince := cond.LastTransitionTime.Time
		if readySince.IsZero() {
			// No transition time to judge warmth by. Counting the pod would
			// risk the dilution DR-05 describes, so it waits for the next
			// sample.
			return false
		}
		return now.Sub(readySince) >= warmup
	}
	return false
}
