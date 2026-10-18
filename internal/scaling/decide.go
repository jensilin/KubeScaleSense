package scaling

import (
	"time"

	"github.com/jensilin/KubeScaleSense/internal/config"
	"github.com/jensilin/KubeScaleSense/internal/resources"
)

// Decision is the complete outcome of one reconcile.
//
// Every intermediate value is carried, not just the answer, so that a single log
// line reconstructs the whole derivation: an operator asking "why 4 and not 8?"
// should never have to re-run the controller to find out (NFR-10).
type Decision struct {
	Reason    Reason
	Direction Direction
	Action    Action

	// Blocked is true when a guard prevented a change that demand asked for.
	// The steady state is not blocked; a cooldown is.
	Blocked bool

	CurrentReplicas int32

	// TargetReplicas is what the replica count would be set to. For every hold
	// it equals CurrentReplicas, which is what makes "a hold never changes
	// anything" checkable by a property test rather than by inspection.
	TargetReplicas int32

	DesiredBacklog     int32
	DesiredUtilization int32
	DesiredRaw         int32
	DesiredClamped     int32
	StepLimited        int32
	RequestedDelta     int32

	// Deficit is the shortfall between what demand asked for and what fits.
	// Exported because desired-minus-current sustained over time is the
	// recommended alert.
	Deficit int32

	FitCapacity int32
	Blocking    resources.Dimension

	Backoff         BackoffState
	BackoffAction   BackoffAction
	BackoffInterval time.Duration

	// DemandComputed reports whether DesiredRaw is meaningful. The controller
	// appends to the stabilization window only when it is, which is precisely
	// what stops a metrics outage from emptying the window and permitting a
	// blind scale-down (DR-04).
	DemandComputed bool

	// PendingTimedOut reports that replicas of the target have been stuck —
	// unschedulable, or never materialised into pods — for longer than
	// pendingPodTimeout. Set under every onPendingTimeout mode, including the
	// ones that do not write, because freeze and none are remediations too and
	// the controller has to count and announce them.
	PendingTimedOut bool

	RequeueAfter time.Duration
}

// Decide computes the decision for one reconcile.
//
// Pure: the only inputs are the snapshot, the configuration, and the current
// time. Fit capacity has already been computed for this snapshot by
// internal/resources, including on reconciles that will return HoldBackoff.
//
// Guards are evaluated in the fixed documented order and the first match wins.
func Decide(s Snapshot, c *config.Config, now time.Time) Decision {
	d := Decision{
		Direction:       DirectionNone,
		CurrentReplicas: s.CurrentReplicas,
		TargetReplicas:  s.CurrentReplicas,
		FitCapacity:     s.FitCapacity,
		Blocking:        s.Blocking,
		Backoff:         s.HoldBackoff,
		BackoffAction:   BackoffUnchanged,
		RequeueAfter:    c.Controller.Interval.Duration(),
	}

	staleAfter := c.Workload.MetricsStaleAfter.Duration()

	// ── G0: is the snapshot usable at all?
	if !s.Valid {
		return d.with(ReasonErrorAPIFailure)
	}

	// ── G1: are we the sole writer? Competing with another controller is
	// unwinnable — whoever writes last wins — so the response is refusal
	// rather than competition (I-12).
	if s.HPAPresent {
		return d.with(ReasonHoldScalingConflict)
	}
	if s.ExternalDrifts > int32(c.Scaling.ExternalChangeTolerance) {
		return d.with(ReasonHoldExternalChange)
	}

	// ── G1': is the target quiet? A scale-up requested mid-rollout is itself
	// surged, so the fit estimate we would gate on is not the resource cost the
	// Deployment controller will actually incur (DR-08).
	if s.RolloutInProgress {
		return d.with(ReasonHoldRolloutInProgress)
	}

	// ── W: the pending-pod watchdog (FR-20, ADR-13). Before the freshness gate,
	// because a stuck replica processes nothing whatever the demand is, so
	// taking it back needs no demand data — and holding a Pending pod through
	// a metrics outage would break "never Pending past the timeout" on exactly
	// the day it is hardest to diagnose.
	var reverted bool
	if d, reverted = d.watchdog(s, c, now); reverted {
		return d
	}

	// A complete scale-up carried the attempt count instead of clearing it.
	// Every replica existing and scheduled is the success that clears it, and
	// it needs no demand data, so it is not lost to a metrics outage.
	if s.HoldBackoff.AwaitingPlacement && s.PendingOurPods == 0 && s.UnmaterialisedReplicas == 0 {
		d.Backoff = ClearBackoff()
		d.BackoffAction = BackoffReset
	}

	// ── G2: never compute a decision from data known to be stale. This is the
	// fail-safe that makes "unknown" mean freeze rather than zero (I-7).
	if !s.Backlog.Fresh(staleAfter, now) {
		return d.with(ReasonHoldStaleMetrics)
	}

	// ── Step 1: demand.
	d.DesiredBacklog = int32(ceilDiv(s.Backlog.Value, int64(c.Workload.ItemsPerReplica)))

	// The utilization signal is a safety net for the case where itemsPerReplica
	// is wrong because items turned out to be unexpectedly expensive. Unlike the
	// backlog it is optional: missing utilization degrades the decision rather
	// than freezing it (UT-12).
	if s.AvgCPUMilliPercent.Fresh(staleAfter, now) && s.ReadyReplicas > 0 {
		d.DesiredUtilization = int32(ceilDiv(
			int64(s.ReadyReplicas)*s.AvgCPUMilliPercent.Value,
			int64(c.Workload.TargetCPUUtilizationPercent)*1000,
		))
	}

	// max() is the conservative combiner: either signal may request capacity,
	// but a scale-down requires both to be low. This is also what makes "never
	// scale down while a backlog exists" fall out without a separate guard,
	// because the backlog target keeps the floor up on its own (I-8, FR-22).
	d.DesiredRaw = maxInt32(d.DesiredBacklog, d.DesiredUtilization)
	d.DesiredClamped = clampInt32(d.DesiredRaw, c.Target.MinReplicas, c.Target.MaxReplicas)
	d.DemandComputed = true

	// Demand falling to or below the current count means the shortfall the
	// backoff was a statement about has gone away, so the backoff is cleared
	// even though no scale-up happened. Compared against the *unclamped*
	// desired, so that sitting at maxReplicas under genuinely high demand does
	// not look like demand having fallen.
	if s.HoldBackoff.Armed() && d.DesiredRaw <= s.CurrentReplicas {
		d.Backoff = ClearBackoff()
		d.BackoffAction = BackoffReset
	}

	// ── G3/G3': is the clamp the thing that is binding? Reported separately
	// from a resource shortfall because the operator action differs: raise the
	// policy limit, versus add cluster capacity.
	if d.DesiredRaw > c.Target.MaxReplicas && s.CurrentReplicas == c.Target.MaxReplicas {
		return d.with(ReasonHoldAtMaxReplicas)
	}
	if d.DesiredRaw < c.Target.MinReplicas && s.CurrentReplicas == c.Target.MinReplicas {
		return d.with(ReasonHoldAtMinReplicas)
	}

	// ── G4: deadband.
	if withinTolerance(d.DesiredClamped, s.CurrentReplicas, c.Scaling.TolerancePercent) {
		return d.with(ReasonNoChangeWithinTolerance)
	}

	if d.DesiredClamped > s.CurrentReplicas {
		return d.scaleUpPath(s, c, now)
	}
	return d.scaleDownPath(s, c, now)
}

// scaleUpPath implements the up-direction gates G5..G8 and the feasibility gate.
func (d Decision) scaleUpPath(s Snapshot, c *config.Config, now time.Time) Decision {
	d.Direction = DirectionUp

	// Fit capacity was computed before these gates, which is what allows the
	// backoff below to be level-triggered on capacity (DR-01).
	backoffActive := now.Before(s.HoldBackoff.NextEligible) &&
		s.FitCapacity <= s.HoldBackoff.FitCapacityAtArm

	// G5: our pods are already unschedulable, or replicas we asked for have not
	// become pods at all. Stacking another request on top would compound an
	// estimation error across reconciles. The unmaterialised half is the quota
	// case, which has no Pending pod to see (FS-10).
	if s.PendingOurPods > 0 || s.UnmaterialisedReplicas > 0 {
		return d.with(ReasonHoldPendingPods)
	}

	// G5': pods are scheduled but never becoming useful. They hold real
	// capacity while processing nothing, so the backlog keeps rising and a
	// naive controller would keep adding more broken replicas — starving other
	// workloads to no benefit. There is deliberately no auto-revert: a broken
	// image may recover, and removing replicas from a partly-broken pool can
	// remove the working ones (DR-06).
	if s.UnhealthyPodAge > c.Pending.PodStartupTimeout.Duration() {
		return d.with(ReasonHoldUnhealthyPods)
	}

	// G6: before the cooldown, because a scale-up just proven infeasible makes
	// the cooldown state irrelevant and reporting it would mislead.
	if backoffActive {
		return d.with(ReasonHoldBackoff)
	}

	// G7: cooldown.
	if now.Sub(s.LastScaleUp) < c.Scaling.ScaleUpCooldown.Duration() {
		return d.with(ReasonHoldCooldown)
	}

	// Step limits bound the blast radius of a bad input: a corrupt backlog
	// reading of ten thousand cannot jump the Deployment to maxReplicas in one
	// write, and the several intervals it would take instead give the
	// feasibility gate, the watchdog, and a human a chance to intervene.
	d.StepLimited = minInt32(d.DesiredClamped, s.CurrentReplicas+c.Scaling.MaxScaleUpStep)
	d.RequestedDelta = d.StepLimited - s.CurrentReplicas

	// ── G8: the feasibility gate. This is the one thing that distinguishes
	// KubeScaleSense from a standard HPA: a scale-up is issued only for pods
	// believed placeable, so Pending becomes an estimation error rather than the
	// normal outcome of a shortfall (I-4).
	switch {
	case s.FitCapacity >= d.RequestedDelta:
		d.TargetReplicas = d.StepLimited
		// Complete, but only as an estimate: the attempt count is carried
		// until the new replicas are placed. Clearing it here would let a
		// fit estimate the watchdog keeps disproving retry at the initial
		// interval forever, which is the retry storm by another route.
		if d.Backoff.Armed() {
			d.Backoff = BackoffState{
				Attempts:          d.Backoff.Attempts,
				FitCapacityAtArm:  -1,
				AwaitingPlacement: true,
			}
			d.BackoffAction = BackoffCarry
		} else {
			d.Backoff = ClearBackoff()
			d.BackoffAction = BackoffReset
		}
		return d.with(ReasonScaleUp)

	case s.FitCapacity > 0 && c.Scaling.AllowPartialScaleUp:
		// Partial progress beats none: placing 2 of 4 needed pods drains the
		// backlog more slowly but it does drain, using capacity the cluster
		// genuinely has.
		d.TargetReplicas = s.CurrentReplicas + s.FitCapacity
		d.Deficit = d.RequestedDelta - s.FitCapacity
		// A partial scale-up is evidence of a shortfall, not of recovery, so it
		// arms the backoff rather than resetting it. Treating it as a success
		// would clear the backoff on the same reconcile that armed it and
		// restore the retry storm backoff exists to prevent (DR-02).
		d.Backoff, d.BackoffInterval = armBackoff(d.Backoff, s.FitCapacity, c.Scaling.HoldBackoff, now)
		d.BackoffAction = BackoffArm
		return d.with(ReasonScaleUpPartial)

	default:
		d.Deficit = d.RequestedDelta
		d.Backoff, d.BackoffInterval = armBackoff(d.Backoff, s.FitCapacity, c.Scaling.HoldBackoff, now)
		d.BackoffAction = BackoffArm
		return d.with(ReasonHoldInsufficientResources)
	}
}

// scaleDownPath implements the down-direction gates G9..G12.
//
// Scale-down is treated as the more dangerous direction throughout, because
// removing a pod terminates work in progress (I-9).
func (d Decision) scaleDownPath(s Snapshot, c *config.Config, now time.Time) Decision {
	d.Direction = DirectionDown

	// G9: all replicas must have settled. Otherwise a scale-up still in
	// progress depresses the utilization target through its ReadyReplicas
	// numerator, and the controller can remove pods it added seconds earlier
	// (DR-03).
	if s.ReadyReplicas != s.CurrentReplicas {
		return d.with(ReasonHoldReplicasSettling)
	}

	// G10: five times the scale-up cooldown, deliberately. Reacting quickly to
	// load is cheap and reversible; retreating quickly is expensive and
	// disruptive.
	if now.Sub(s.LastScaleDown) < c.Scaling.ScaleDownCooldown.Duration() {
		return d.with(ReasonHoldCooldown)
	}

	// G11: the workload must have been sustainably quiet for the whole window,
	// and the window must actually have been observed.
	window := c.Scaling.ScaleDownStabilizationWindow.Duration()
	covered := s.WindowCovered(window, c.Controller.Interval.Duration(), c.Scaling.ScaleDownWindowCoverage, now)
	maxDesired, found := s.MaxDesiredInWindow(window, now)
	if !covered || !found || maxDesired >= s.CurrentReplicas {
		return d.with(ReasonHoldStabilizationWindow)
	}

	// One replica per cooldown by default: each removed pod terminates work in
	// progress, so retreat is strictly gradual.
	d.StepLimited = maxInt32(d.DesiredClamped, s.CurrentReplicas-c.Scaling.MaxScaleDownStep)

	// The clamp is a hard bound and the step limit only rate-limits movement
	// inside it, so the clamp is applied last. It matters when someone else has
	// set the replica count above maxReplicas: stepping down one replica at a
	// time from 40 to 12 would leave the workload outside its own policy for
	// half an hour, so returning into range is immediate. FR-02 says every
	// decision is clamped, without an exception for the ones we arrived at
	// gradually.
	d.StepLimited = clampInt32(d.StepLimited, c.Target.MinReplicas, c.Target.MaxReplicas)
	d.TargetReplicas = d.StepLimited
	d.RequestedDelta = d.TargetReplicas - s.CurrentReplicas
	return d.with(ReasonScaleDown)
}

// watchdog evaluates the stuck-replica remediation and reports whether it
// settles the decision.
//
// Only `revert` writes. `freeze` and `none` mark the timeout and fall through
// to the ordinary guards, where G5 already refuses to stack a scale-up on top
// of a stuck one — so the two differ in what is counted and announced, not in
// what the engine decides.
//
// The revert removes the stuck replicas and nothing else:
//
//	target = max(lastGoodReplicas, current − stuck), clamped to [min, max]
//
// The ReplicaSet controller deletes unscheduled pods before scheduled ones and
// a quota-rejected replica has no pod to delete, so lowering the count by the
// stuck number removes exactly the stuck replicas. When every pod of the last
// scale-up is stuck that is lastGoodReplicas, as ADR-13 specifies. When some of
// them did schedule, those are kept: they are serving traffic, and the stuck
// ones are the only liability. lastGoodReplicas remains the floor, so a revert
// can never undo more than our own last scale-up.
//
// Nothing to revert — an external actor added the replicas, or the floor
// already equals the current count — is not a write. G5 still holds every
// scale-up, which is the freeze behaviour, and is the safe fallback.
func (d Decision) watchdog(s Snapshot, c *config.Config, now time.Time) (Decision, bool) {
	timeout := c.Pending.PendingPodTimeout.Duration()
	stuck := s.PendingOurPods + s.UnmaterialisedReplicas

	timedOut := (s.PendingOurPods > 0 && s.OldestPendingAge > timeout) ||
		(s.UnmaterialisedReplicas > 0 && s.UnmaterialisedAge > timeout)
	if !timedOut {
		return d, false
	}
	d.PendingTimedOut = true

	if c.Pending.OnPendingTimeout != config.OnPendingRevert {
		return d, false
	}

	target := maxInt32(s.LastGoodReplicas, s.CurrentReplicas-stuck)
	target = clampInt32(target, c.Target.MinReplicas, c.Target.MaxReplicas)
	if target >= s.CurrentReplicas {
		return d, false
	}

	d.Direction = DirectionDown
	d.TargetReplicas = target
	d.RequestedDelta = target - s.CurrentReplicas

	// Armed, not reset: a stuck pod is proof that the fit estimate was wrong
	// at the level it currently reports, so the next scale-up must wait for
	// capacity to rise above that level or for the backoff to expire, rather
	// than reissue the request that just failed (ADR-13).
	d.Backoff, d.BackoffInterval = armBackoff(d.Backoff, s.FitCapacity, c.Scaling.HoldBackoff, now)
	d.BackoffAction = BackoffArm
	return d.with(ReasonRevertPendingPods), true
}

// with finalises the decision with its single reason code.
func (d Decision) with(r Reason) Decision {
	d.Reason = r
	d.Action = r.Action()
	d.Blocked = r.IsHold()
	return d
}

// armBackoff advances the backoff, doubling by default from 30 s to a 15 m cap.
//
// The growth curve matters far less than the reset conditions: the backoff
// exists to suppress futile retries, not to delay recovery, which is why a
// capacity increase clears it immediately rather than waiting out the interval
// (I-10, § 9.2).
func armBackoff(prev BackoffState, fitCapacity int32, cfg config.HoldBackoffConfig, now time.Time) (BackoffState, time.Duration) {
	attempts := prev.Attempts + 1
	interval := cfg.Initial.Duration()
	maxInterval := cfg.Max.Duration()

	for i := 1; i < attempts; i++ {
		grown := time.Duration(float64(interval) * cfg.Factor)
		if grown <= interval {
			// A factor of 1 or less would make the backoff never grow and this
			// loop never terminate usefully. Configuration validation rejects
			// it; this is the belt to that braces.
			break
		}
		interval = grown
		if interval >= maxInterval {
			break
		}
	}
	if interval > maxInterval {
		interval = maxInterval
	}

	return BackoffState{
		Attempts:         attempts,
		NextEligible:     now.Add(interval),
		FitCapacityAtArm: fitCapacity,
	}, interval
}

// withinTolerance evaluates the relative deadband.
//
// Cross-multiplied rather than divided, so the boundary is exact: a change of
// exactly tolerancePercent is outside the band and acts. Being relative, the
// band is effectively inert at small replica counts — at two replicas a
// one-replica change is 50 % — which is intentional, because at low counts every
// change is significant and hysteresis comes from the cooldowns instead.
func withinTolerance(desired, current int32, tolerancePercent int) bool {
	if current <= 0 {
		return false
	}
	diff := int64(desired) - int64(current)
	if diff < 0 {
		diff = -diff
	}
	return diff*100 < int64(tolerancePercent)*int64(current)
}

// ceilDiv is integer ceiling division for non-negative inputs.
func ceilDiv(a, b int64) int64 {
	if a <= 0 || b <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

func clampInt32(v, lo, hi int32) int32 {
	if v < lo {
		v = lo
	}
	if v > hi {
		v = hi
	}
	return v
}

func minInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func maxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}
