package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/config"
	kubernetesaccess "github.com/jensilin/KubeScaleSense/internal/kubernetes"
	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// EventRecorder announces decisions on the target Deployment, so that
// `kubectl describe deployment` explains what the autoscaler did and why
// (FR-24, architecture § 8.2).
//
// One method, and the method can only add an announcement: there is no way
// through it to read, edit, or retract anything. Nil in dry-run, where no
// writable client of any kind is built.
type EventRecorder interface {
	Record(ctx context.Context, event kubernetesaccess.Event) error
}

// The architecture § 8.2 event reasons. Holds that table does not name are
// announced under their decision reason, so every event reason is either one
// of these or a stable scaling.Reason.
const (
	eventScaledUp              = "ScaledUp"
	eventScaledUpPartial       = "ScaledUpPartial"
	eventScaledDown            = "ScaledDown"
	eventInsufficientResources = "InsufficientClusterResources"
	eventPodPendingTimeout     = "PodPendingTimeout"
	eventPodStartupFailure     = "PodStartupFailure"
	eventScalingConflict       = "ScalingConflict"
	eventExternalScaleDetected = "ExternalScaleDetected"
	eventRolloutInProgress     = "RolloutInProgress"
	eventMetricsUnavailable    = "MetricsUnavailable"
)

// actionAdoptBaseline is the event action for an external change being
// adopted. Every other event's action is the decision reason, which is what
// ties an event to the matching log line and kss_reconcile_total series.
const actionAdoptBaseline = "AdoptBaseline"

// externalChange is a replica count change made by another actor, held until
// the reconcile that observed it can announce it.
type externalChange struct {
	from, to int32
}

// announce emits this reconcile's events. Best-effort by design: an event is a
// note for a human, and failing to write one must never block, delay, or fail
// the reconcile whose decision it describes.
func (c *Controller) announce(ctx context.Context, decision scaling.Decision, snapshot scaling.Snapshot,
	actuateErr error, now time.Time,
) {
	c.mu.Lock()
	change := c.state.pendingExternalChange
	c.state.pendingExternalChange = nil
	c.mu.Unlock()

	if c.events == nil {
		return
	}

	if change != nil {
		c.emit(ctx, kubernetesaccess.Event{
			Type:   kubernetesaccess.EventWarning,
			Reason: eventExternalScaleDetected,
			Action: actionAdoptBaseline,
			Note: fmt.Sprintf("replicas changed %d -> %d by another actor; adopting it as the baseline, "+
				"stabilization history reset", change.from, change.to),
		})
	}

	event, ok := describeDecision(decision, snapshot, c.cfg, now)
	if !ok {
		return
	}

	if decision.Action == scaling.ActionWrite {
		// A write is announced once it has happened. A conflicted, stale, or
		// refused write changed nothing, and its failure is already in the log
		// and in kss_scale_actions_total.
		if actuateErr == nil {
			c.emit(ctx, event)
		}
		return
	}

	key := string(decision.Reason)
	if decision.PendingTimedOut {
		key = eventPodPendingTimeout
	}
	c.mu.Lock()
	due := c.state.holdEvents.due(key, now, c.cfg.Scaling.HoldBackoff)
	c.mu.Unlock()
	if due {
		c.emit(ctx, event)
	}
}

func (c *Controller) emit(ctx context.Context, event kubernetesaccess.Event) {
	if err := c.events.Record(ctx, event); err != nil {
		c.log.Warn("could not record a Kubernetes event; the decision is unaffected",
			slog.String("event_reason", event.Reason),
			slog.String("action", event.Action),
			slog.Any("error", err),
		)
	}
}

// describeDecision renders a decision as an event, and reports false for the
// decisions that announce nothing: the steady state, which is not news, and a
// failed observation, which cannot be trusted to describe the target at all.
func describeDecision(d scaling.Decision, s scaling.Snapshot, c *config.Config, now time.Time) (kubernetesaccess.Event, bool) {
	normal := func(reason, note string) (kubernetesaccess.Event, bool) {
		return kubernetesaccess.Event{
			Type: kubernetesaccess.EventNormal, Reason: reason, Action: string(d.Reason), Note: note,
		}, true
	}
	warning := func(reason, note string) (kubernetesaccess.Event, bool) {
		return kubernetesaccess.Event{
			Type: kubernetesaccess.EventWarning, Reason: reason, Action: string(d.Reason), Note: note,
		}, true
	}

	// Checked before the reason because a timed-out pod is the most important
	// thing to say whatever else held the reconcile, and under freeze or none
	// the reason alone (often HoldPendingPods) does not say a timeout passed.
	if d.PendingTimedOut || d.Reason == scaling.ReasonRevertPendingPods {
		return warning(eventPodPendingTimeout, pendingTimeoutNote(d, s, c))
	}

	switch d.Reason {
	case scaling.ReasonScaleUp:
		return normal(eventScaledUp, fmt.Sprintf("%d -> %d (backlog %d, itemsPerReplica %d, desired %d, fitCapacity %d)",
			d.CurrentReplicas, d.TargetReplicas, s.Backlog.Value, c.Workload.ItemsPerReplica, d.DesiredClamped,
			d.FitCapacity))

	case scaling.ReasonScaleUpPartial:
		return warning(eventScaledUpPartial, fmt.Sprintf("%d -> %d of %d wanted; fitCapacity %d, blocking dimension %s",
			d.CurrentReplicas, d.TargetReplicas, d.StepLimited, d.FitCapacity, d.Blocking))

	case scaling.ReasonScaleDown:
		return normal(eventScaledDown, fmt.Sprintf("%d -> %d (backlog %d, desired %d, stabilization window of %s satisfied)",
			d.CurrentReplicas, d.TargetReplicas, s.Backlog.Value, d.DesiredClamped,
			c.Scaling.ScaleDownStabilizationWindow.Duration()))

	case scaling.ReasonHoldInsufficientResources:
		return warning(eventInsufficientResources, fmt.Sprintf(
			"need %d more pods of %s, can place %d; candidate nodes %d, blocking %s; retry in %s",
			d.StepLimited-d.CurrentReplicas, s.PodRequest, d.FitCapacity, s.CandidateNodes, d.Blocking,
			d.BackoffInterval))

	case scaling.ReasonHoldUnhealthyPods:
		return warning(eventPodStartupFailure, fmt.Sprintf(
			"%d pods scheduled but not Ready for %s (podStartupTimeout %s); blocking scale-ups",
			s.UnhealthyOurPods, s.UnhealthyPodAge.Round(time.Second), c.Pending.PodStartupTimeout))

	case scaling.ReasonHoldScalingConflict:
		return warning(eventScalingConflict,
			"a HorizontalPodAutoscaler targets the same Deployment; refusing to scale (FR-19)")

	case scaling.ReasonHoldExternalChange:
		return warning(eventExternalScaleDetected, fmt.Sprintf(
			"replicas changed by another actor %d times within %s; holding until those changes age out",
			s.ExternalDrifts, c.Scaling.ScaleDownStabilizationWindow.Duration()))

	case scaling.ReasonHoldRolloutInProgress:
		return normal(eventRolloutInProgress, "a rollout of the target is in progress; holding both directions until it completes")

	case scaling.ReasonHoldStaleMetrics:
		note := "workload signal unavailable; holding"
		if s.Backlog.Available {
			note = fmt.Sprintf("workload signal stale (sample age %s, metricsStaleAfter %s); holding",
				s.Backlog.Age(now).Round(time.Second), c.Workload.MetricsStaleAfter)
		}
		return warning(eventMetricsUnavailable, note)

	case scaling.ReasonHoldCooldown:
		return normal(string(d.Reason), fmt.Sprintf("scale-%s wanted (%d -> %d) but the cooldown since the last one has not elapsed",
			d.Direction, d.CurrentReplicas, d.DesiredClamped))

	case scaling.ReasonHoldStabilizationWindow:
		return normal(string(d.Reason), fmt.Sprintf(
			"demand is down to %d replicas from %d; waiting for the %s stabilization window to show it is sustained",
			d.DesiredClamped, d.CurrentReplicas, c.Scaling.ScaleDownStabilizationWindow.Duration()))

	case scaling.ReasonHoldBackoff:
		return normal(string(d.Reason), fmt.Sprintf(
			"scale-up to %d held after an infeasible attempt; next attempt at %s unless capacity grows first",
			d.DesiredClamped, d.Backoff.NextEligible.UTC().Format(time.RFC3339)))

	case scaling.ReasonHoldPendingPods:
		return normal(string(d.Reason), fmt.Sprintf(
			"%d pods Pending and %d replicas not yet created; holding scale-ups until they are placed",
			s.PendingOurPods, s.UnmaterialisedReplicas))

	case scaling.ReasonHoldReplicasSettling:
		return normal(string(d.Reason), fmt.Sprintf(
			"%d of %d replicas Ready; holding scale-down until the last change settles", s.ReadyReplicas, d.CurrentReplicas))

	case scaling.ReasonHoldAtMaxReplicas:
		return warning(string(d.Reason), fmt.Sprintf("demand wants %d replicas but maxReplicas is %d",
			d.DesiredRaw, c.Target.MaxReplicas))

	case scaling.ReasonHoldAtMinReplicas:
		return normal(string(d.Reason), fmt.Sprintf("demand wants %d replicas but minReplicas is %d",
			d.DesiredRaw, c.Target.MinReplicas))

	default:
		// NoChangeWithinTolerance and ErrorAPIFailure.
		return kubernetesaccess.Event{}, false
	}
}

func pendingTimeoutNote(d scaling.Decision, s scaling.Snapshot, c *config.Config) string {
	stuck := ""
	switch {
	case s.PendingOurPods > 0 && s.UnmaterialisedReplicas > 0:
		stuck = fmt.Sprintf("%d pods unschedulable for %s and %d replicas never created for %s",
			s.PendingOurPods, s.OldestPendingAge.Round(time.Second),
			s.UnmaterialisedReplicas, s.UnmaterialisedAge.Round(time.Second))
	case s.UnmaterialisedReplicas > 0:
		stuck = fmt.Sprintf("%d replicas never created for %s (a ResourceQuota or admission rejection)",
			s.UnmaterialisedReplicas, s.UnmaterialisedAge.Round(time.Second))
	default:
		stuck = fmt.Sprintf("%d pods unschedulable for %s", s.PendingOurPods, s.OldestPendingAge.Round(time.Second))
	}

	switch {
	case d.Reason == scaling.ReasonRevertPendingPods:
		return fmt.Sprintf("%s; reverting %d -> %d", stuck, d.CurrentReplicas, d.TargetReplicas)
	case c.Pending.OnPendingTimeout == config.OnPendingRevert:
		return stuck + "; none of them are ours to revert, so scale-ups are held"
	default:
		return fmt.Sprintf("%s; holding scale-ups (onPendingTimeout %s)", stuck, c.Pending.OnPendingTimeout)
	}
}

// holdCadence is when one hold reason was last announced, and how many times
// in a row.
type holdCadence struct {
	lastSeen    time.Time
	lastEmitted time.Time
	emitted     int
}

// holdEventLimiter rate-limits repeating hold events, once per backoff cycle.
//
// Each hold reason keeps its own cadence on the hold-backoff schedule: the
// first occurrence is announced at once, and repeats after initial, then
// initial x factor, and so on up to max. A reason that has not been seen for
// longer than max is a new occurrence and is announced at once again. Two
// reasons that alternate every reconcile each still follow their own cadence,
// so flapping cannot turn into a stream of events.
type holdEventLimiter map[string]*holdCadence

func (l holdEventLimiter) due(key string, now time.Time, backoff config.HoldBackoffConfig) bool {
	cadence, seen := l[key]
	if !seen || now.Sub(cadence.lastSeen) > backoff.Max.Duration() {
		l[key] = &holdCadence{lastSeen: now, lastEmitted: now, emitted: 1}
		return true
	}

	cadence.lastSeen = now
	if now.Sub(cadence.lastEmitted) < cycle(cadence.emitted, backoff) {
		return false
	}
	cadence.lastEmitted = now
	cadence.emitted++
	return true
}

// cycle is the wait before the next repeat after n announcements.
func cycle(n int, backoff config.HoldBackoffConfig) time.Duration {
	interval := backoff.Initial.Duration()
	limit := backoff.Max.Duration()
	for i := 1; i < n && interval < limit; i++ {
		grown := time.Duration(float64(interval) * backoff.Factor)
		if grown <= interval {
			break
		}
		interval = grown
	}
	return min(interval, limit)
}
