package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/jensilin/KubeScaleSense/internal/config"
	"github.com/jensilin/KubeScaleSense/internal/observability"
	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// The pending-pod watchdog through the reconcile loop: IT-07 and IT-08 against
// the controller's own state — lastGoodReplicas, the unmaterialised clock, and
// the once-per-episode accounting — rather than against a hand-built snapshot.

const pastPendingTimeout = 121 * time.Second

// IT-07, revert: our scale-up leaves pods unschedulable, and past the timeout
// the stuck replicas are taken back through the scale subresource.
func TestWatchdog_RevertsOurOwnStuckScaleUp(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{400}
	metrics := observability.NewMetrics()
	actuator := &recordingActuator{}
	c := newTestControllerWith(t, f, actuator, withMetrics(metrics))

	scaledUp := reconcileOnce(t, c)
	if scaledUp.Reason != scaling.ReasonScaleUp || scaledUp.TargetReplicas != 6 {
		t.Fatalf("setup: got %s to %d, want ScaleUp to 6", scaledUp.Reason, scaledUp.TargetReplicas)
	}

	// The write lands, and four of the new pods find nowhere to run.
	f.setReplicas(6)
	markPending(f.pods[2:], f.clock.now_())

	held := reconcileOnce(t, c)
	if held.Reason != scaling.ReasonHoldPendingPods {
		t.Fatalf("before the timeout: Reason = %s, want %s", held.Reason, scaling.ReasonHoldPendingPods)
	}

	f.advance(pastPendingTimeout)
	revert := reconcileOnce(t, c)

	if revert.Reason != scaling.ReasonRevertPendingPods {
		t.Fatalf("Reason = %s, want %s", revert.Reason, scaling.ReasonRevertPendingPods)
	}
	if revert.TargetReplicas != 2 {
		t.Errorf("reverted to %d, want 2 — the count before our scale-up", revert.TargetReplicas)
	}
	if last := actuator.last(); last.Reason != scaling.ReasonRevertPendingPods {
		t.Errorf("the actuator last received %s; the revert must go through it like any other write", last.Reason)
	}
	if got := remediations(t, metrics, config.OnPendingRevert); got != 1 {
		t.Errorf("revert remediations = %v, want 1", got)
	}
}

// One intervention per incident. The informer cache can show the stuck pods
// for another reconcile after the revert lands, and an operator counting
// remediations wants incidents, not ticks.
func TestWatchdog_CountsOncePerEpisode(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{400}
	metrics := observability.NewMetrics()
	c := newTestControllerWith(t, f, &recordingActuator{}, withMetrics(metrics))

	reconcileOnce(t, c)
	f.setReplicas(6)
	markPending(f.pods[2:], f.clock.now_())

	for range 3 {
		f.advance(pastPendingTimeout)
		reconcileOnce(t, c)
	}
	if got := remediations(t, metrics, config.OnPendingRevert); got != 1 {
		t.Fatalf("revert remediations = %v after one episode, want 1", got)
	}

	// The episode ends when nothing is stuck — here the pods scheduled after
	// all — and the next one counts afresh. The replica count is left alone so
	// nothing reads as an external change.
	f.setReplicas(6)
	if d := reconcileOnce(t, c); d.PendingTimedOut {
		t.Fatal("setup: nothing is stuck, yet the timeout is still marked")
	}
	markPending(f.pods[4:], f.clock.now_())
	f.advance(pastPendingTimeout)

	second := reconcileOnce(t, c)
	if second.Reason != scaling.ReasonRevertPendingPods || second.TargetReplicas != 4 {
		t.Fatalf("second episode: got %s to %d, want %s to 4", second.Reason, second.TargetReplicas,
			scaling.ReasonRevertPendingPods)
	}
	if got := remediations(t, metrics, config.OnPendingRevert); got != 2 {
		t.Errorf("revert remediations = %v after a second episode, want 2", got)
	}
}

// IT-07, freeze: nothing is written, and the intervention is still counted.
func TestWatchdog_FreezeHoldsAndCounts(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Pending.OnPendingTimeout = config.OnPendingFreeze

	f := scalableFixture()
	f.pressure = []int64{400}
	metrics := observability.NewMetrics()
	actuator := &recordingActuator{}
	c := newTestControllerWith(t, f, actuator, withConfig(cfg), withMetrics(metrics))

	reconcileOnce(t, c)
	f.setReplicas(6)
	markPending(f.pods[2:], f.clock.now_())
	f.advance(pastPendingTimeout)

	decision := reconcileOnce(t, c)

	if decision.Action == scaling.ActionWrite {
		t.Fatalf("freeze wrote %d -> %d", decision.CurrentReplicas, decision.TargetReplicas)
	}
	if got := len(actuator.writes()); got != 1 {
		t.Errorf("%d writes reached the actuator, want only the original scale-up", got)
	}
	if got := remediations(t, metrics, config.OnPendingFreeze); got != 1 {
		t.Errorf("freeze remediations = %v, want 1", got)
	}
}

// ADR-17: an external scale-up is adopted as the baseline, so its stuck pods
// are not ours to revert. Reverting them would undo another actor's decision —
// fighting it by another route.
func TestWatchdog_DoesNotRevertAnExternalScaleUp(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{50}
	metrics := observability.NewMetrics()
	actuator := &recordingActuator{}
	c := newTestControllerWith(t, f, actuator, withMetrics(metrics))

	reconcileOnce(t, c)

	// Someone else scales to 5; three of their pods stick.
	f.setReplicas(5)
	markPending(f.pods[2:], f.clock.now_())
	reconcileOnce(t, c)

	f.advance(pastPendingTimeout)
	decision := reconcileOnce(t, c)

	if decision.Reason == scaling.ReasonRevertPendingPods {
		t.Fatalf("reverted %d -> %d after an external scale-up", decision.CurrentReplicas, decision.TargetReplicas)
	}
	if !decision.PendingTimedOut {
		t.Error("the timeout passed but was not marked")
	}
	if got := remediations(t, metrics, config.OnPendingFreeze); got != 1 {
		t.Errorf("recorded as freeze %v times, want 1: with nothing to revert, holding is what actually happened", got)
	}
	if got := remediations(t, metrics, config.OnPendingRevert); got != 0 {
		t.Errorf("recorded %v reverts that never happened", got)
	}
}

// IT-08: a quota refuses to create the pods. spec.replicas rises, no pod
// appears — not even a Pending one — and the watchdog must still act.
func TestWatchdog_RevertsReplicasThatNeverMaterialised(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{400}
	actuator := &recordingActuator{}
	c := newTestControllerWith(t, f, actuator)

	reconcileOnce(t, c)

	// The write lands; the ReplicaSet's pod creations are all rejected.
	replicas := int32(6)
	f.deployment.Spec.Replicas = &replicas

	held := reconcileOnce(t, c)
	if held.Reason != scaling.ReasonHoldPendingPods {
		t.Fatalf("unmaterialised replicas must block scale-ups: Reason = %s", held.Reason)
	}

	f.advance(pastPendingTimeout)
	revert := reconcileOnce(t, c)

	if revert.Reason != scaling.ReasonRevertPendingPods || revert.TargetReplicas != 2 {
		t.Errorf("got %s to %d, want %s to 2", revert.Reason, revert.TargetReplicas, scaling.ReasonRevertPendingPods)
	}
}

// The clock restarts when the gap closes, so a gap that opens again later is
// timed from its own start rather than from the first one.
func TestTrackUnmaterialised_ResetsWhenTheGapCloses(t *testing.T) {
	t.Parallel()

	c := newTestController(t, *scalableFixture())
	t0 := start

	if got := c.trackUnmaterialised(2, t0); got != 0 {
		t.Errorf("first sighting aged %v, want 0", got)
	}
	if got := c.trackUnmaterialised(2, t0.Add(time.Minute)); got != time.Minute {
		t.Errorf("aged %v, want 1m", got)
	}
	if got := c.trackUnmaterialised(0, t0.Add(2*time.Minute)); got != 0 {
		t.Errorf("a closed gap aged %v, want 0", got)
	}
	if got := c.trackUnmaterialised(1, t0.Add(3*time.Minute)); got != 0 {
		t.Errorf("a reopened gap aged %v, want 0", got)
	}
}

// --- helpers --------------------------------------------------------------

func withMetrics(m *observability.Metrics) controllerOpt {
	return func(o *Options) { o.Metrics = m }
}

func reconcileOnce(t *testing.T, c *Controller) scaling.Decision {
	t.Helper()
	decision, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return decision
}

// markPending turns pods into unschedulable ones, Pending since the given time.
func markPending(pods []*corev1.Pod, since time.Time) {
	for _, pod := range pods {
		pod.Spec.NodeName = ""
		pod.Status.Phase = corev1.PodPending
		pod.Status.Conditions = []corev1.PodCondition{{
			Type:               corev1.PodScheduled,
			Status:             corev1.ConditionFalse,
			Reason:             corev1.PodReasonUnschedulable,
			LastTransitionTime: metav1.NewTime(since),
		}}
	}
}

func remediations(t *testing.T, m *observability.Metrics, action string) float64 {
	t.Helper()

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "kss_pending_pod_remediations_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "action" && label.GetValue() == action {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("kss_pending_pod_remediations_total{action=%q} is not exported", action)
	return 0
}

func (a *recordingActuator) writes() []scaling.Decision {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]scaling.Decision(nil), a.applied...)
}

func (a *recordingActuator) last() scaling.Decision {
	writes := a.writes()
	if len(writes) == 0 {
		return scaling.Decision{}
	}
	return writes[len(writes)-1]
}
