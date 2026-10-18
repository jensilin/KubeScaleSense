package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	kubernetesaccess "github.com/jensilin/KubeScaleSense/internal/kubernetes"
	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// FR-21 through the reconcile loop: pod-deletion-cost is refreshed from each
// pod's own work immediately before a scale-down, and never stands in the way
// of one.

func TestDeletionCost_RefreshedBeforeTheScaleDownIsWritten(t *testing.T) {
	t.Parallel()

	f := idleEightReplicas()
	f.workByPodIP = workForPods(8, func(i int) int64 { return int64(10 * (8 - i)) })
	actuator := &recordingActuator{}
	costs := &recordingCosts{actuator: actuator}
	c := newTestControllerWith(t, f, actuator, withDeletionCost(costs))

	driveToScaleDown(t, f, c)

	calls := costs.all()
	if len(calls) != 8 {
		t.Fatalf("%d pods had their cost set, want all 8 Ready pods", len(calls))
	}
	for i, call := range calls {
		if want := int32(10 * (8 - i)); call.cost != want {
			t.Errorf("%s cost = %d, want %d — its own outstanding work", call.pod, call.cost, want)
		}
		if call.writesBefore != 0 {
			t.Errorf("%s was annotated after the scale-down was written; the ReplicaSet controller had "+
				"already chosen its victims", call.pod)
		}
	}
	if len(actuator.writes()) != 1 {
		t.Errorf("%d writes, want the one scale-down", len(actuator.writes()))
	}
}

// A pod already carrying the right cost is not patched again: a scale-down
// should cost the API server as few writes as it can.
func TestDeletionCost_UnchangedCostsAreNotRewritten(t *testing.T) {
	t.Parallel()

	f := idleEightReplicas()
	f.workByPodIP = workForPods(8, func(int) int64 { return 5 })
	for _, pod := range f.pods[:6] {
		pod.Annotations = map[string]string{kubernetesaccess.DeletionCostAnnotation: "5"}
	}
	costs := &recordingCosts{}
	c := newTestControllerWith(t, f, &recordingActuator{}, withDeletionCost(costs))

	driveToScaleDown(t, f, c)

	if got := len(costs.all()); got != 2 {
		t.Errorf("%d pods patched, want only the 2 whose cost changed", got)
	}
}

// All or nothing: one Ready pod the source did not scrape means no cost is
// refreshed, rather than a mix of fresh values and ones from an earlier
// scale-down. The scale-down itself is unaffected.
func TestDeletionCost_IncompleteCoverageRefreshesNothing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		work map[string]int64
	}{
		{"one pod was not scraped", workForPods(7, func(int) int64 { return 1 })},
		{"the source has no per-pod view", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := idleEightReplicas()
			f.workByPodIP = tc.work
			actuator := &recordingActuator{}
			costs := &recordingCosts{}
			c := newTestControllerWith(t, f, actuator, withDeletionCost(costs))

			driveToScaleDown(t, f, c)

			if got := len(costs.all()); got != 0 {
				t.Errorf("%d pods patched from an incomplete view", got)
			}
			if len(actuator.writes()) != 1 {
				t.Error("the scale-down did not go ahead")
			}
		})
	}
}

func TestDeletionCost_AWriteFailureDoesNotHoldTheScaleDown(t *testing.T) {
	t.Parallel()

	f := idleEightReplicas()
	f.workByPodIP = workForPods(8, func(int) int64 { return 1 })
	actuator := &recordingActuator{}
	c := newTestControllerWith(t, f, actuator, withDeletionCost(&recordingCosts{err: errors.New("forbidden")}))

	driveToScaleDown(t, f, c)

	if len(actuator.writes()) != 1 {
		t.Error("a failed annotation held the scale-down")
	}
}

// Only demand-driven scale-downs pick victims by cost. A scale-up removes
// nothing, and a revert removes replicas that never started.
func TestDeletionCost_NotRefreshedForOtherWrites(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{400}
	f.workByPodIP = workForPods(2, func(int) int64 { return 1 })
	costs := &recordingCosts{}
	c := newTestControllerWith(t, f, &recordingActuator{}, withDeletionCost(costs))

	reconcileOnce(t, c)
	f.setReplicas(6)
	markPending(f.pods[2:], f.clock.now_())
	f.advance(pastPendingTimeout)
	if d := reconcileOnce(t, c); d.Reason != scaling.ReasonRevertPendingPods {
		t.Fatalf("setup: Reason = %s, want a revert", d.Reason)
	}

	if got := len(costs.all()); got != 0 {
		t.Errorf("%d pods patched for a scale-up and a revert", got)
	}
}

// --- helpers --------------------------------------------------------------

func idleEightReplicas() *fixture {
	f := scalableFixture()
	f.deployment = deployment(8, withRequests(500, 512), withStatus(8, 8))
	f.pods = replicaPods(8)
	f.pressure = []int64{0}
	return f
}

func driveToScaleDown(t *testing.T, f *fixture, c *Controller) {
	t.Helper()
	for range 40 {
		if reconcileOnce(t, c).Reason == scaling.ReasonScaleDown {
			return
		}
		f.advance(15 * time.Second)
	}
	t.Fatal("setup: no scale-down within 10 minutes of zero pressure")
}

func workForPods(n int, work func(i int) int64) map[string]int64 {
	m := make(map[string]int64, n)
	for i := range n {
		m[podIP(i)] = work(i)
	}
	return m
}

func withDeletionCost(w DeletionCostWriter) controllerOpt {
	return func(o *Options) { o.DeletionCost = w }
}

type costCall struct {
	pod          string
	cost         int32
	writesBefore int
}

type recordingCosts struct {
	actuator *recordingActuator
	err      error

	mu    sync.Mutex
	calls []costCall
}

func (r *recordingCosts) SetDeletionCost(_ context.Context, pod string, cost int32) error {
	writes := 0
	if r.actuator != nil {
		writes = len(r.actuator.writes())
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.calls = append(r.calls, costCall{pod: pod, cost: cost, writesBefore: writes})
	return nil
}

func (r *recordingCosts) all() []costCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]costCall(nil), r.calls...)
}
