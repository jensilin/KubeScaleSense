package scaling

// Reason is the single outcome code of a reconcile.
//
// The same string appears in the log line, the Kubernetes Event, and the
// `reason` label of kss_reconcile_total. That makes it the project's audit
// contract, so these values are a stable API: renaming one breaks dashboards
// and the assertions of every E2E scenario (I-6, scaling-algorithm § 2).
type Reason string

// The complete set of reason codes. There is deliberately no "other" or
// "unknown" member: a decision the engine cannot classify is a bug in the
// engine, not a state to report.
const (
	// Actions. In P1 these are computed and reported but never applied.
	ReasonScaleUp        Reason = "ScaleUp"
	ReasonScaleUpPartial Reason = "ScaleUpPartial"
	ReasonScaleDown      Reason = "ScaleDown"

	// The steady state.
	ReasonNoChangeWithinTolerance Reason = "NoChangeWithinTolerance"

	// Holds. Each names a different thing for an operator to do, which is the
	// reason there are this many of them rather than one "Hold".
	ReasonHoldInsufficientResources Reason = "HoldInsufficientResources"
	ReasonHoldCooldown              Reason = "HoldCooldown"
	ReasonHoldStabilizationWindow   Reason = "HoldStabilizationWindow"
	ReasonHoldBackoff               Reason = "HoldBackoff"
	ReasonHoldPendingPods           Reason = "HoldPendingPods"
	ReasonHoldUnhealthyPods         Reason = "HoldUnhealthyPods"
	ReasonHoldReplicasSettling      Reason = "HoldReplicasSettling"
	ReasonHoldRolloutInProgress     Reason = "HoldRolloutInProgress"
	ReasonHoldExternalChange        Reason = "HoldExternalChange"
	ReasonHoldAtMaxReplicas         Reason = "HoldAtMaxReplicas"
	ReasonHoldAtMinReplicas         Reason = "HoldAtMinReplicas"
	ReasonHoldScalingConflict       Reason = "HoldScalingConflict"
	ReasonHoldStaleMetrics          Reason = "HoldStaleMetrics"

	// Failure to build a usable snapshot.
	ReasonErrorAPIFailure Reason = "ErrorAPIFailure"
)

// Reasons lists every code, used to pre-create the kss_reconcile_total label
// values so that a reason which has not occurred yet still reports zero rather
// than being absent from the metrics endpoint (IT-05).
var Reasons = []Reason{
	ReasonScaleUp,
	ReasonScaleUpPartial,
	ReasonScaleDown,
	ReasonNoChangeWithinTolerance,
	ReasonHoldInsufficientResources,
	ReasonHoldCooldown,
	ReasonHoldStabilizationWindow,
	ReasonHoldBackoff,
	ReasonHoldPendingPods,
	ReasonHoldUnhealthyPods,
	ReasonHoldReplicasSettling,
	ReasonHoldRolloutInProgress,
	ReasonHoldExternalChange,
	ReasonHoldAtMaxReplicas,
	ReasonHoldAtMinReplicas,
	ReasonHoldScalingConflict,
	ReasonHoldStaleMetrics,
	ReasonErrorAPIFailure,
}

// Action says whether a reason would change the replica count. In P1 nothing
// acts on this — the only actuator is a logger — but the engine still computes
// it, because the whole point of the phase is that the dry-run trace is
// directly comparable with a live one.
type Action string

const (
	ActionNone  Action = "none"
	ActionWrite Action = "write"
)

// Action reports whether this reason corresponds to a replica-count write.
func (r Reason) Action() Action {
	switch r {
	case ReasonScaleUp, ReasonScaleUpPartial, ReasonScaleDown:
		return ActionWrite
	default:
		return ActionNone
	}
}

// IsHold reports whether the reason blocked a change that demand asked for.
//
// NoChangeWithinTolerance is not a hold: nothing was blocked, because nothing
// was wanted. Conflating the two would make "how often are we blocked?" read as
// permanently saturated in the steady state.
func (r Reason) IsHold() bool {
	switch r {
	case ReasonScaleUp, ReasonScaleUpPartial, ReasonScaleDown, ReasonNoChangeWithinTolerance:
		return false
	default:
		return true
	}
}

// Direction is the direction demand pointed in, independent of whether a guard
// then blocked the move. It is reported separately from the reason so that a
// HoldCooldown on the way up is distinguishable from one on the way down.
type Direction string

const (
	DirectionNone Direction = "none"
	DirectionUp   Direction = "up"
	DirectionDown Direction = "down"
)

// BackoffAction is what the decision instructs the controller to do with the
// backoff state. Returned rather than applied, because applying it would make
// Decide stateful.
type BackoffAction string

const (
	// BackoffUnchanged is used by the guards that return before demand is
	// known. A reconcile that could not read the cluster says nothing about
	// whether capacity has recovered, so it must not disturb the backoff.
	BackoffUnchanged BackoffAction = "unchanged"
	BackoffArm       BackoffAction = "arm"
	BackoffReset     BackoffAction = "reset"
)
