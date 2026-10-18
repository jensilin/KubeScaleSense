package scaling

import (
	"testing"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/config"
)

// The pending-pod watchdog at the engine level: IT-07 and IT-08 restated as
// pure decisions, so every mode and both stuck signatures are pinned down
// before a cluster is involved (FR-20, ADR-13, FS-06, FS-10).

const pastTimeout = 121 * time.Second

func TestWatchdog_RevertTakesBackTheStuckReplicas(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                       string
		lastGood, current, pending int32
		want                       int32
		why                        string
	}{
		{"every new pod is stuck", 2, 6, 4, 2,
			"the whole scale-up failed, so the revert restores lastGoodReplicas exactly (ADR-13)"},
		{"one new pod is stuck", 2, 6, 1, 5,
			"three of the four new pods scheduled and are serving; only the stuck one is a liability"},
		{"more stuck than the last scale-up added", 5, 6, 3, 5,
			"lastGoodReplicas is the floor, so a revert never undoes more than our own last scale-up"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := Decide(stuckAfterScaleUp(tc.lastGood, tc.current, tc.pending), baseConfig(), now)

			if d.Reason != ReasonRevertPendingPods {
				t.Fatalf("Reason = %s, want %s", d.Reason, ReasonRevertPendingPods)
			}
			if d.TargetReplicas != tc.want {
				t.Errorf("TargetReplicas = %d, want %d: %s", d.TargetReplicas, tc.want, tc.why)
			}
			if d.Action != ActionWrite || d.Direction != DirectionDown {
				t.Errorf("Action/Direction = %s/%s, want write/down", d.Action, d.Direction)
			}
			if !d.PendingTimedOut {
				t.Error("PendingTimedOut is false on a revert")
			}
			if d.BackoffAction != BackoffArm || !d.Backoff.Armed() {
				t.Errorf("the revert did not arm the backoff (%s); the next scale-up would reissue the request "+
					"that just failed", d.BackoffAction)
			}
		})
	}
}

// Not a write: the floor is already the current count. An external actor
// scaled up and its pods are stuck, which is not ours to undo — and G5 still
// refuses to stack a scale-up on top, which is the safe fallback.
func TestWatchdog_NothingToRevertFallsBackToHolding(t *testing.T) {
	t.Parallel()

	s := stuckAfterScaleUp(6, 6, 2)
	s.Backlog = fresh(800) // demand points up

	d := Decide(s, baseConfig(), now)

	if d.Action == ActionWrite {
		t.Fatalf("wrote %d -> %d with nothing of ours to revert", d.CurrentReplicas, d.TargetReplicas)
	}
	if d.Reason != ReasonHoldPendingPods {
		t.Errorf("Reason = %s, want %s", d.Reason, ReasonHoldPendingPods)
	}
	if !d.PendingTimedOut {
		t.Error("the timeout passed but PendingTimedOut is false, so it would go uncounted")
	}
}

// freeze and none do not write. Both still hold scale-ups, because G5 is
// independent of the mode: a stuck pod is never stacked on, whatever the
// remediation policy says about removing it.
func TestWatchdog_FreezeAndNoneNeverWrite(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{config.OnPendingFreeze, config.OnPendingNone} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			cfg := baseConfig()
			cfg.Pending.OnPendingTimeout = mode

			s := stuckAfterScaleUp(2, 6, 4)
			s.Backlog = fresh(800)

			d := Decide(s, cfg, now)

			if d.Action == ActionWrite {
				t.Fatalf("%s wrote %d -> %d", mode, d.CurrentReplicas, d.TargetReplicas)
			}
			if d.Reason != ReasonHoldPendingPods {
				t.Errorf("Reason = %s, want %s", d.Reason, ReasonHoldPendingPods)
			}
			if !d.PendingTimedOut {
				t.Errorf("%s must still mark the timeout so it is counted and announced", mode)
			}
		})
	}
}

// The timeout is strict and measured on the oldest stuck pod. A pod that has
// been Pending for less than pendingPodTimeout may still schedule — a node
// rebooting, a volume attaching — which is why ADR-13 picked a generous value.
func TestWatchdog_DoesNotActBeforeTheTimeout(t *testing.T) {
	t.Parallel()

	for _, age := range []time.Duration{0, 30 * time.Second, 120 * time.Second} {
		s := stuckAfterScaleUp(2, 6, 4)
		s.OldestPendingAge = age

		d := Decide(s, baseConfig(), now)

		if d.Reason == ReasonRevertPendingPods {
			t.Errorf("reverted at a pending age of %s; the timeout is 120s and strict", age)
		}
		if d.PendingTimedOut {
			t.Errorf("PendingTimedOut at a pending age of %s", age)
		}
	}
}

// A quota rejects pod creation outright, so there is no Pending pod to see —
// spec.replicas rises and the pods never appear. That gap is the signature.
func TestWatchdog_UnmaterialisedReplicasAreTheQuotaSignature(t *testing.T) {
	t.Parallel()

	t.Run("blocks scale-ups at once", func(t *testing.T) {
		t.Parallel()

		s := scaleUpWanted()
		s.UnmaterialisedReplicas = 2
		s.UnmaterialisedAge = 5 * time.Second

		if d := Decide(s, baseConfig(), now); d.Reason != ReasonHoldPendingPods {
			t.Errorf("Reason = %s, want %s: stacking replicas on a quota-blocked request compounds it",
				d.Reason, ReasonHoldPendingPods)
		}
	})

	t.Run("is reverted past the timeout", func(t *testing.T) {
		t.Parallel()

		s := stuckAfterScaleUp(2, 6, 0)
		s.UnmaterialisedReplicas = 4
		s.UnmaterialisedAge = pastTimeout

		d := Decide(s, baseConfig(), now)
		if d.Reason != ReasonRevertPendingPods || d.TargetReplicas != 2 {
			t.Errorf("got %s to %d, want %s to 2 (IT-08)", d.Reason, d.TargetReplicas, ReasonRevertPendingPods)
		}
	})
}

// The revert is not a statement about demand, so it runs without demand data
// and bypasses the scale-down gates. A stuck pod held through a metrics outage
// or a cooldown would break "never Pending past the timeout".
func TestWatchdog_ActsWithoutDemandAndBypassesTheScaleDownGates(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		mutate func(*Snapshot)
	}{
		{"backlog unavailable", func(s *Snapshot) { s.Backlog = Signal[int64]{} }},
		{"backlog stale", func(s *Snapshot) { s.Backlog.SampledAt = now.Add(-time.Hour) }},
		{"replicas not settled (G9)", func(s *Snapshot) { s.ReadyReplicas = 2 }},
		{"just scaled down (G10)", func(s *Snapshot) { s.LastScaleDown = now }},
		{"window not covered (G11)", func(s *Snapshot) { s.DesiredHistory = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := stuckAfterScaleUp(2, 6, 4)
			tc.mutate(&s)

			if d := Decide(s, baseConfig(), now); d.Reason != ReasonRevertPendingPods {
				t.Errorf("Reason = %s, want %s", d.Reason, ReasonRevertPendingPods)
			}
		})
	}
}

// The guards above the watchdog still win. Each is a reason not to write at all:
// an unreadable cluster, a competing controller, or a rollout whose surge pods
// belong to the Deployment controller rather than to our scale-up.
func TestWatchdog_TheSoleOwnershipGuardsStillWin(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		mutate func(*Snapshot)
		want   Reason
	}{
		{"invalid snapshot", func(s *Snapshot) { s.Valid = false }, ReasonErrorAPIFailure},
		{"HPA present", func(s *Snapshot) { s.HPAPresent = true }, ReasonHoldScalingConflict},
		{"external writer", func(s *Snapshot) { s.ExternalDrifts = 4 }, ReasonHoldExternalChange},
		{"rollout", func(s *Snapshot) { s.RolloutInProgress = true }, ReasonHoldRolloutInProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := stuckAfterScaleUp(2, 6, 4)
			tc.mutate(&s)

			if d := Decide(s, baseConfig(), now); d.Reason != tc.want {
				t.Errorf("Reason = %s, want %s", d.Reason, tc.want)
			}
		})
	}
}

// Found by the live IT-07 run on kind: the revert armed the backoff, the
// scale-up after it cleared the backoff on writing, and the next revert armed
// it at the initial interval again — so a fit estimate the watchdog kept
// disproving was retried every pendingPodTimeout + 30 s indefinitely. With the
// count carried through the scale-up, each episode waits longer (FR-18).
func TestWatchdog_RepeatedEpisodesGrowTheBackoff(t *testing.T) {
	t.Parallel()

	cfg := baseConfig()
	backoff := ClearBackoff()

	for episode, wantInterval := range []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute} {
		stuck := stuckAfterScaleUp(3, 7, 4)
		stuck.HoldBackoff = backoff

		d := Decide(stuck, cfg, now)
		if d.Reason != ReasonRevertPendingPods {
			t.Fatalf("episode %d: Reason = %s, want %s", episode+1, d.Reason, ReasonRevertPendingPods)
		}
		if d.Backoff.Attempts != episode+1 || d.BackoffInterval != wantInterval {
			t.Fatalf("episode %d: revert armed attempt %d for %s, want attempt %d for %s",
				episode+1, d.Backoff.Attempts, d.BackoffInterval, episode+1, wantInterval)
		}

		// The interval runs out with demand still high and the same estimate,
		// so one retry is allowed — and it must not forget the count.
		expired := d.Backoff
		expired.NextEligible = now.Add(-time.Second)
		retry := scaleUpWanted()
		retry.CurrentReplicas, retry.ReadyReplicas = 3, 3
		retry.HoldBackoff = expired

		d = Decide(retry, cfg, now)
		if d.Reason != ReasonScaleUp || d.BackoffAction != BackoffCarry {
			t.Fatalf("episode %d: retry = %s with backoff %s, want %s with %s",
				episode+1, d.Reason, d.BackoffAction, ReasonScaleUp, BackoffCarry)
		}
		backoff = d.Backoff
	}
}

func TestWatchdog_RevertRespectsMinReplicas(t *testing.T) {
	t.Parallel()

	cfg := baseConfig()
	cfg.Target.MinReplicas = 2

	// lastGoodReplicas below the floor can only come from a configuration
	// change after it was recorded; the clamp still wins (FR-02).
	s := stuckAfterScaleUp(1, 3, 3)

	d := Decide(s, cfg, now)
	if d.Reason != ReasonRevertPendingPods || d.TargetReplicas != 2 {
		t.Errorf("got %s to %d, want %s to minReplicas (2)", d.Reason, d.TargetReplicas, ReasonRevertPendingPods)
	}
}

// stuckAfterScaleUp is the state ADR-13 is about: we scaled from lastGood to
// current, and pending of the new pods have been unschedulable past the
// timeout. Demand sits exactly at the current count, so no other guard has a
// reason to act.
func stuckAfterScaleUp(lastGood, current, pending int32) Snapshot {
	s := baseSnapshot()
	s.CurrentReplicas = current
	s.ReadyReplicas = current - pending
	s.LastGoodReplicas = lastGood
	s.Backlog = fresh(int64(current) * int64(config.DefaultItemsPerReplica))
	s.PendingOurPods = pending
	if pending > 0 {
		s.OldestPendingAge = pastTimeout
	}
	s.LastScaleUp = now.Add(-pastTimeout)
	return s
}
