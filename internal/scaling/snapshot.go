package scaling

import (
	"time"

	"github.com/jensilin/KubeScaleSense/internal/resources"
)

// Signal carries a measured value together with whether it was measured at all
// and when.
//
// This is the single most important shape in the package. It exists so that the
// *engine*, not the collector, decides what a missing or stale input means — the
// difference between a controller that "does something reasonable" and one whose
// behaviour on partial failure is specified and tested. A collector that
// returned a bare 0 for "could not reach the source" would make a busy system
// scale to the floor during a monitoring outage.
type Signal[T any] struct {
	Value     T
	SampledAt time.Time
	Available bool
}

// Fresh reports whether the signal may be used for a decision.
//
// Age is measured from SampledAt, which the collector sets to the *older* of the
// local receive time and the source-reported sample time. That two-sided
// measurement is what catches a source which is reachable but frozen, serving
// the same stale number forever (FR-34, DR-14).
//
// A SampledAt in the future is treated as age zero rather than as an error:
// small clock skew between the controller and a metrics source is normal, and
// flapping into HoldStaleMetrics because of it would be worse than tolerating
// it.
func (s Signal[T]) Fresh(staleAfter time.Duration, now time.Time) bool {
	if !s.Available || s.SampledAt.IsZero() {
		return false
	}
	age := now.Sub(s.SampledAt)
	if age < 0 {
		age = 0
	}
	return age <= staleAfter
}

// Age returns the sample age, floored at zero. Reported as
// kss_metric_sample_age_seconds.
func (s Signal[T]) Age(now time.Time) time.Duration {
	if !s.Available || s.SampledAt.IsZero() {
		return 0
	}
	if age := now.Sub(s.SampledAt); age > 0 {
		return age
	}
	return 0
}

// BackoffState is the controller's memory of a scale-up that proved infeasible.
//
// FitCapacityAtArm is the field that makes the reset condition expressible at
// all: "capacity increased" is meaningless without recording the level it
// increased from. Keeping it in the snapshot is what keeps the reset a pure
// function rather than a side effect (I-10, FR-33).
type BackoffState struct {
	Attempts         int
	NextEligible     time.Time
	FitCapacityAtArm int32
}

// ClearBackoff is the zero state. FitCapacityAtArm is -1 rather than 0 so that
// "cleared" is distinguishable from "armed at a genuinely full cluster", where
// the capacity at arm really was zero.
func ClearBackoff() BackoffState {
	return BackoffState{FitCapacityAtArm: -1}
}

// Armed reports whether a backoff has been armed and not yet cleared.
func (b BackoffState) Armed() bool {
	return b.Attempts > 0
}

// DesiredSample is one entry of the scale-down stabilization window.
type DesiredSample struct {
	At      time.Time
	Desired int32
}

// Snapshot is the contract between the I/O layers and the pure engine, and the
// unit of every test fixture (architecture § 6).
//
// Two deliberate differences from the struct sketched in architecture § 6:
//
// The current time is a parameter of Decide rather than a Snapshot field. The
// canonical pseudocode in scaling-algorithm § 10 takes `now` explicitly, and
// carrying it in both places would create two sources of truth for the one input
// most likely to be mocked incorrectly.
//
// Utilization is an integer in milli-percent, not a float64. Exact
// reproducibility at boundary values is a stated requirement, and float
// arithmetic makes it depend on evaluation order and platform rounding at
// precisely the values table-driven tests choose (NFR-07, DR-15).
type Snapshot struct {
	// Valid is false when the snapshot could not be assembled — caches
	// unsynced, target unreadable, or the pod template missing its requests.
	// It produces ErrorAPIFailure and, per I-7, no action in either direction.
	Valid bool

	// Target.
	CurrentReplicas int32
	ReadyReplicas   int32
	PodRequest      resources.Request

	// Demand.
	Backlog            Signal[int64]
	BacklogRaw         int64
	AvgCPUMilliPercent Signal[int64]
	AvgMemMilliPercent Signal[int64]

	// Feasibility, computed by internal/resources before Decide is called.
	FitCapacity     int32
	CandidateNodes  int32
	Blocking        resources.Dimension
	FreeCPUMilli    int64
	FreeMemoryBytes int64

	// Target state read from the Deployment.
	RolloutInProgress bool
	HPAPresent        bool
	ExternalDrifts    int32

	// History owned by the controller and intentionally not persisted across
	// restarts: after a restart the controller re-derives everything from the
	// cluster and behaves as if freshly cooled down (NFR-08).
	LastScaleUp      time.Time
	LastScaleDown    time.Time
	DesiredHistory   []DesiredSample
	HoldBackoff      BackoffState
	LastGoodReplicas int32

	// Pod health of the target's current ReplicaSet.
	PendingOurPods   int32
	OldestPendingAge time.Duration

	// UnhealthyOurPods is the count of scheduled-but-not-Ready pods, and
	// UnhealthyPodAge the longest such duration. The guard keys on the age
	// rather than the count, because a pod that has been starting for two
	// seconds is not a failure — only one that has been starting for longer
	// than podStartupTimeout is.
	UnhealthyOurPods int32
	UnhealthyPodAge  time.Duration
}

// MaxDesiredInWindow returns the highest desired value sampled inside the
// window, and whether any sample was found.
//
// This is the strongest scale-down guard: a single busy sample anywhere in the
// window pins the replica count, so the workload must be *sustainably* quiet
// rather than momentarily quiet.
func (s Snapshot) MaxDesiredInWindow(window time.Duration, now time.Time) (int32, bool) {
	maxDesired := int32(0)
	found := false
	for _, sample := range s.DesiredHistory {
		if now.Sub(sample.At) > window {
			continue
		}
		if !found || sample.Desired > maxDesired {
			maxDesired = sample.Desired
			found = true
		}
	}
	return maxDesired, found
}

// WindowCovered reports whether enough of the window has actually been observed
// to justify a scale-down.
//
// Without this check a five-minute metrics outage would leave a nearly-empty
// window whose max() is trivially low, permitting a scale-down justified by
// *missing* data — the same class of error as reading "no backlog data" as "no
// backlog", re-entering through the history buffer (DR-04, FS-27).
//
// Two conditions, both required:
//
//	enough samples   at least coverage x ceil(window/interval) of them
//	long enough      the oldest retained sample is at least a window old
//
// The second is what makes a freshly started controller wait rather than scale
// down on its first quiet minute.
func (s Snapshot) WindowCovered(window, interval time.Duration, coverage float64, now time.Time) bool {
	if interval <= 0 || window <= 0 {
		return false
	}

	expected := int((window + interval - 1) / interval)
	if expected < 1 {
		expected = 1
	}

	present := 0
	oldest := time.Time{}
	for _, sample := range s.DesiredHistory {
		if now.Sub(sample.At) <= window {
			present++
		}
		if oldest.IsZero() || sample.At.Before(oldest) {
			oldest = sample.At
		}
	}

	// Integer comparison of present/expected >= coverage, done by
	// cross-multiplication in thousandths so the gate does not depend on float
	// rounding at exact boundaries.
	coverageMilli := int64(coverage * 1000)
	if int64(present)*1000 < coverageMilli*int64(expected) {
		return false
	}
	return !oldest.IsZero() && now.Sub(oldest) >= window
}
