package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	kubernetesaccess "github.com/jensilin/KubeScaleSense/internal/kubernetes"
	"github.com/jensilin/KubeScaleSense/internal/observability"
	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// The live actuator's tests. Twelve behaviours matter here, and they divide
// into two groups: what it does when everything works, and what it refuses to
// do when something does not.
//
// The second group is the larger one, which is the correct shape for a
// component whose failure mode is "changed the wrong cluster in the wrong
// direction at the wrong moment".

// --- the happy paths ------------------------------------------------------

func TestScaleActuator_AppliesAScaleUp(t *testing.T) {
	t.Parallel()

	target := &stubTarget{replicas: 2, resourceVersion: "1701"}
	actuator, _ := newTestActuator(t, target)

	if err := actuator.Apply(context.Background(), writeDecision(scaling.DirectionUp, 2, 6)); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(target.writes) != 1 {
		t.Fatalf("performed %d writes, want 1: %+v", len(target.writes), target.writes)
	}
	if got := target.writes[0].replicas; got != 6 {
		t.Errorf("wrote %d replicas, want 6", got)
	}
	// The precondition is the point of FR-05: the version written back must be
	// the one just read, not an empty string and not a remembered one.
	if got := target.writes[0].resourceVersion; got != "1701" {
		t.Errorf("wrote with resourceVersion %q, want the one just read (%q)", got, "1701")
	}
	if target.currentCalls != 1 {
		t.Errorf("read the live scale %d times, want exactly 1 before the write", target.currentCalls)
	}
}

func TestScaleActuator_AppliesAScaleDown(t *testing.T) {
	t.Parallel()

	target := &stubTarget{replicas: 6, resourceVersion: "42"}
	actuator, _ := newTestActuator(t, target)

	if err := actuator.Apply(context.Background(), writeDecision(scaling.DirectionDown, 6, 5)); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(target.writes) != 1 || target.writes[0].replicas != 5 {
		t.Fatalf("writes = %+v, want a single write of 5 replicas", target.writes)
	}
}

// A hold must not talk to the API server at all. Most reconciles are holds, and
// an actuator that read the scale subresource on every one of them would put a
// request per interval per controller onto the API server for no purpose — and
// would give a hold a way to fail (NFR-04).
func TestScaleActuator_HoldsPerformNoAPICallWhatsoever(t *testing.T) {
	t.Parallel()

	for _, reason := range []scaling.Reason{
		scaling.ReasonNoChangeWithinTolerance,
		scaling.ReasonHoldInsufficientResources,
		scaling.ReasonHoldStaleMetrics,
		scaling.ReasonHoldCooldown,
		scaling.ReasonErrorAPIFailure,
	} {
		target := &stubTarget{replicas: 3, resourceVersion: "1"}
		actuator, _ := newTestActuator(t, target)

		decision := scaling.Decision{
			Reason:          reason,
			Action:          reason.Action(),
			CurrentReplicas: 3,
			TargetReplicas:  3,
		}
		if err := actuator.Apply(context.Background(), decision); err != nil {
			t.Errorf("%s: Apply returned %v, want nil", reason, err)
		}
		if target.currentCalls != 0 || len(target.writes) != 0 {
			t.Errorf("%s: made %d reads and %d writes, want none for a decision that is not a write",
				reason, target.currentCalls, len(target.writes))
		}
	}
}

// --- the configured envelope ----------------------------------------------

// The engine clamps to [minReplicas, maxReplicas] already. These two tests are
// about what happens when it does not, because the actuator is the last thing
// between a defect and the cluster.
func TestScaleActuator_RefusesBelowMinReplicas(t *testing.T) {
	t.Parallel()

	target := &stubTarget{replicas: 3, resourceVersion: "1"}
	actuator, _ := newTestActuator(t, target)

	err := actuator.Apply(context.Background(), writeDecision(scaling.DirectionDown, 3, 1))
	if err != nil {
		t.Fatalf("a target equal to minReplicas must be allowed, got %v", err)
	}

	target = &stubTarget{replicas: 3, resourceVersion: "1"}
	actuator, _ = newTestActuator(t, target)

	err = actuator.Apply(context.Background(), writeDecision(scaling.DirectionDown, 3, 0))
	if !errors.Is(err, ErrScaleRejected) {
		t.Errorf("scaling to 0 with minReplicas 1 returned %v, want ErrScaleRejected", err)
	}
	if len(target.writes) != 0 {
		t.Errorf("a rejected decision performed %d writes, want 0", len(target.writes))
	}
}

func TestScaleActuator_RefusesAboveMaxReplicas(t *testing.T) {
	t.Parallel()

	target := &stubTarget{replicas: 3, resourceVersion: "1"}
	actuator, _ := newTestActuator(t, target)

	err := actuator.Apply(context.Background(), writeDecision(scaling.DirectionUp, 3, 13))
	if !errors.Is(err, ErrScaleRejected) {
		t.Errorf("scaling to 13 with maxReplicas 12 returned %v, want ErrScaleRejected", err)
	}
	if len(target.writes) != 0 {
		t.Errorf("a rejected decision performed %d writes, want 0", len(target.writes))
	}
	if target.currentCalls != 0 {
		t.Errorf("a rejected decision read the cluster %d times, want 0: the bound is checked before any "+
			"API call, so a bad decision costs nothing", target.currentCalls)
	}
}

// An arbitrary caller-supplied value must not reach the cluster just because it
// arrived through the Actuator interface. This is the "never scale to an
// arbitrary value" property stated as a table.
func TestScaleActuator_RejectsInvalidReplicaCounts(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		current int32
		target  int32
	}{
		{"negative", 3, -1},
		{"zero", 3, 0},
		{"far above the maximum", 3, 10_000},
		{"no change at all", 3, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target := &stubTarget{replicas: tc.current, resourceVersion: "1"}
			actuator, _ := newTestActuator(t, target)

			err := actuator.Apply(context.Background(), writeDecision(scaling.DirectionUp, tc.current, tc.target))
			if !errors.Is(err, ErrScaleRejected) {
				t.Errorf("Apply(%d -> %d) = %v, want ErrScaleRejected", tc.current, tc.target, err)
			}
			if len(target.writes) != 0 {
				t.Errorf("performed %d writes, want 0", len(target.writes))
			}
		})
	}
}

// --- concurrency and stale state ------------------------------------------

// A conflict means someone else wrote first. The decision is discarded, not
// retried: retrying would apply a replica count computed from a cluster state
// that has already been superseded (FR-05, IT-02).
func TestScaleActuator_ConflictDiscardsTheDecisionWithoutRetrying(t *testing.T) {
	t.Parallel()

	target := &stubTarget{
		replicas:        2,
		resourceVersion: "1",
		writeErrs: []error{apierrors.NewConflict(
			schema.GroupResource{Group: "apps", Resource: "deployments"}, "normalizer",
			errors.New("the object has been modified")),
		},
	}
	actuator, probe := newTestActuator(t, target)

	err := actuator.Apply(context.Background(), writeDecision(scaling.DirectionUp, 2, 6))
	if !errors.Is(err, ErrScaleConflict) {
		t.Fatalf("Apply = %v, want ErrScaleConflict", err)
	}
	if len(target.writes) != 1 {
		t.Errorf("attempted %d writes, want exactly 1: a conflict must not be retried inside the reconcile",
			len(target.writes))
	}
	if len(probe.sleeps) != 0 {
		t.Errorf("backed off %v before giving up; a conflict is not a transient failure and should not wait",
			probe.sleeps)
	}
}

// The replica count moving between the snapshot and the write is different from
// the resourceVersion moving: the decision's own arithmetic no longer applies,
// so it is abandoned rather than written.
func TestScaleActuator_AbandonsADecisionComputedFromAStaleReplicaCount(t *testing.T) {
	t.Parallel()

	// Decided from 2 replicas; the cluster now has 4.
	target := &stubTarget{replicas: 4, resourceVersion: "9"}
	actuator, _ := newTestActuator(t, target)

	err := actuator.Apply(context.Background(), writeDecision(scaling.DirectionUp, 2, 6))
	if !errors.Is(err, ErrScaleStale) {
		t.Fatalf("Apply = %v, want ErrScaleStale", err)
	}
	if len(target.writes) != 0 {
		t.Errorf("performed %d writes, want 0: the decision was computed for a cluster that no longer exists",
			len(target.writes))
	}
}

// The same guard, in the form it will actually occur: our own write has landed
// in the cluster but has not yet reached the informer cache, so the next
// reconcile decides the same scale-up again. Without the live read this would
// double-apply — 2 -> 6 followed by 6 -> 10.
func TestScaleActuator_DoesNotReapplyADecisionTheClusterAlreadyReflects(t *testing.T) {
	t.Parallel()

	target := &stubTarget{replicas: 6, resourceVersion: "2"}
	actuator, _ := newTestActuator(t, target)

	err := actuator.Apply(context.Background(), writeDecision(scaling.DirectionUp, 2, 6))
	if !errors.Is(err, ErrScaleStale) {
		t.Fatalf("Apply = %v, want ErrScaleStale", err)
	}
	if len(target.writes) != 0 {
		t.Fatalf("re-applied a decision the cluster already reflects; writes = %+v", target.writes)
	}
}

// --- transient failures ---------------------------------------------------

func TestScaleActuator_RetriesTransientFailuresWithinTheBudget(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"internal error", apierrors.NewInternalError(errors.New("etcd leader election"))},
		{"service unavailable", apierrors.NewServiceUnavailable("apiserver is starting")},
		{"throttled", apierrors.NewTooManyRequests("slow down", 1)},
		{"server timeout", apierrors.NewServerTimeout(
			schema.GroupResource{Group: "apps", Resource: "deployments"}, "update", 1)},
		{"request timeout", apierrors.NewTimeoutError("request timed out", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Fails once, then succeeds.
			target := &stubTarget{replicas: 2, resourceVersion: "1", writeErrs: []error{tc.err}}
			actuator, probe := newTestActuator(t, target)

			if err := actuator.Apply(context.Background(), writeDecision(scaling.DirectionUp, 2, 6)); err != nil {
				t.Fatalf("Apply = %v, want success on the retry", err)
			}
			if len(target.writes) != 2 {
				t.Errorf("attempted %d writes, want 2 (one failure, one success)", len(target.writes))
			}
			if len(probe.sleeps) != 1 {
				t.Errorf("backed off %v, want exactly one pause between the two attempts", probe.sleeps)
			}
		})
	}
}

// The retry is bounded. An API server that is down stays down for longer than a
// reconcile, and the right response is to stop, hold the replica count, and
// decide again from a fresher snapshot — not to keep trying inside a reconcile
// that is already overdue (ADR-14).
func TestScaleActuator_AbandonsTransientFailuresWhenTheBudgetRunsOut(t *testing.T) {
	t.Parallel()

	target := &stubTarget{
		replicas:        2,
		resourceVersion: "1",
		writeErr:        apierrors.NewServiceUnavailable("apiserver is unreachable"),
	}
	actuator, probe := newTestActuator(t, target)

	err := actuator.Apply(context.Background(), writeDecision(scaling.DirectionUp, 2, 6))
	if err == nil {
		t.Fatal("Apply succeeded against a permanently unavailable API server")
	}
	// Not fatal: the cluster may well be fine on the next tick, and failing
	// readiness for a 503 would turn an API blip into a restarted controller.
	if errors.Is(err, ErrScaleFatal) {
		t.Errorf("a 503 was classified fatal (%v); it is transient", err)
	}
	if len(target.writes) < 2 {
		t.Errorf("attempted %d writes, want more than one before giving up", len(target.writes))
	}

	var total time.Duration
	for _, slept := range probe.sleeps {
		total += slept
	}
	if total > testRetryBudget {
		t.Errorf("slept %s in total, which exceeds the %s budget; retries must not outlive the reconcile",
			total, testRetryBudget)
	}
}

// A cancelled reconcile stops immediately rather than finishing its retries.
func TestScaleActuator_StopsWhenTheReconcileIsCancelled(t *testing.T) {
	t.Parallel()

	target := &stubTarget{
		replicas:        2,
		resourceVersion: "1",
		writeErr:        apierrors.NewServiceUnavailable("apiserver is unreachable"),
	}
	actuator, _ := newTestActuator(t, target)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := actuator.Apply(ctx, writeDecision(scaling.DirectionUp, 2, 6)); err == nil {
		t.Fatal("Apply succeeded on a cancelled context")
	}
	if len(target.writes) > 1 {
		t.Errorf("attempted %d writes on a cancelled context, want at most 1", len(target.writes))
	}
}

// --- fatal failures -------------------------------------------------------

// 403 and 404 are configuration errors. Retrying a missing permission produces
// a retry storm against the API server and no progress, so both stop the
// actuation and fail readiness instead (IT-03).
func TestScaleActuator_TreatsPermissionAndMissingTargetAsFatal(t *testing.T) {
	t.Parallel()

	deployments := schema.GroupResource{Group: "apps", Resource: "deployments"}

	for _, tc := range []struct {
		name   string
		err    error
		onRead bool
	}{
		{"forbidden on write", apierrors.NewForbidden(deployments, "normalizer",
			errors.New("cannot update deployments/scale")), false},
		{"unauthorized on write", apierrors.NewUnauthorized("token expired"), false},
		{"target missing on read", apierrors.NewNotFound(deployments, "normalizer"), true},
		{"target missing on write", apierrors.NewNotFound(deployments, "normalizer"), false},
		{"rejected object", apierrors.NewBadRequest("negative replica count"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target := &stubTarget{replicas: 2, resourceVersion: "1"}
			if tc.onRead {
				target.currentErr = tc.err
			} else {
				target.writeErr = tc.err
			}

			actuator, probe := newTestActuator(t, target)

			err := actuator.Apply(context.Background(), writeDecision(scaling.DirectionUp, 2, 6))
			if !errors.Is(err, ErrScaleFatal) {
				t.Fatalf("Apply = %v, want ErrScaleFatal", err)
			}
			if len(probe.sleeps) != 0 {
				t.Errorf("retried a fatal error %d times; a missing permission does not improve with waiting",
					len(probe.sleeps))
			}
			if tc.onRead && len(target.writes) != 0 {
				t.Errorf("wrote despite failing to read the target: %+v", target.writes)
			}
		})
	}
}

// A 403 must reach readiness, because an operator watching pod health should
// see that this controller cannot do its job.
func TestReconcile_FatalActuationFailureFailsReadiness(t *testing.T) {
	t.Parallel()

	f := scaleUpFixture()
	target := &stubTarget{
		replicas:        2,
		resourceVersion: "1",
		writeErr: apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"},
			"normalizer", errors.New("cannot update deployments/scale")),
	}
	actuator, _ := newTestActuator(t, target)
	c := newTestControllerWith(t, f, actuator)

	if _, err := c.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile succeeded despite a forbidden scale write")
	}

	err := c.Ready()
	if err == nil {
		t.Fatal("Ready() reports healthy after a fatal actuation failure")
	}
	if !strings.Contains(err.Error(), "actuation is not possible") {
		t.Errorf("Ready() = %q, want it to name the actuation failure", err)
	}
}

// --- integration with the reconcile loop ----------------------------------

// The gates are not bypassed by the arrival of a real actuator. A decision that
// holds must produce no write however the actuator is wired, which is the
// property that keeps every safety rule in the engine meaningful.
func TestReconcile_SafetyGatesStillBlockWritesWithALiveActuator(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		arrange func(*fixture)
	}{
		{"an HPA competes for the replica count", func(f *fixture) {
			f.hpas = []*autoscalingv2.HorizontalPodAutoscaler{hpa("normalizer", "normalizer")}
		}},
		{"a rollout is in progress", func(f *fixture) {
			f.deployment = deployment(2, withRequests(500, 512), withStatus(1, 1))
		}},
		{"the pressure signal is stale", func(f *fixture) {
			f.signalUnavailable = true
		}},
		{"the cluster has no room", func(f *fixture) {
			f.nodes = []*corev1.Node{node("worker-1", 600, 1024, 10)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := scaleUpFixture()
			tc.arrange(f)

			target := &stubTarget{replicas: 2, resourceVersion: "1"}
			actuator, _ := newTestActuator(t, target)
			c := newTestControllerWith(t, f, actuator)

			decision, _ := c.Reconcile(context.Background())

			if decision.Action == scaling.ActionWrite {
				t.Fatalf("decided to write (%s) despite %s", decision.Reason, tc.name)
			}
			if len(target.writes) != 0 {
				t.Errorf("performed %d writes despite %s: %+v", len(target.writes), tc.name, target.writes)
			}
		})
	}
}

// A failed write must not leave the controller believing it scaled. If the
// cooldown advanced on a failure, the retry on the next tick would be refused
// with HoldCooldown — so one dropped request would cost a full cooldown of
// inaction.
func TestReconcile_AFailedWriteDoesNotStartTheCooldown(t *testing.T) {
	t.Parallel()

	f := scaleUpFixture()
	target := &stubTarget{
		replicas:        2,
		resourceVersion: "1",
		writeErrs:       []error{apierrors.NewServiceUnavailable("apiserver is unreachable")},
	}
	actuator, _ := newTestActuator(t, target)
	// One attempt only, so the reconcile abandons rather than succeeding on a
	// retry: the point of the test is the state after a failure.
	actuator.retryBudget = time.Nanosecond
	c := newTestControllerWith(t, f, actuator)

	if _, err := c.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile succeeded despite the write failing")
	}

	c.mu.Lock()
	lastScaleUp := c.state.lastScaleUp
	hasWritten := c.state.hasWritten
	c.mu.Unlock()

	if !lastScaleUp.IsZero() {
		t.Errorf("lastScaleUp was set to %s after a failed write; the cooldown must start when a write "+
			"succeeds, not when one is attempted", lastScaleUp)
	}
	if hasWritten {
		t.Error("the controller recorded a write that did not happen, which would make the next observed " +
			"replica count look like its own")
	}
}

// The controller must not mistake its own scaling for somebody else's.
//
// This is the failure that would make P3 unusable rather than unsafe: three
// successful scale-ups exceed externalChangeTolerance, and the controller would
// freeze with HoldExternalChange, blaming an external actor that was itself.
func TestReconcile_OwnWritesAreNotCountedAsExternalDrift(t *testing.T) {
	t.Parallel()

	f := scaleUpFixture()
	target := &stubTarget{replicas: 2, resourceVersion: "1"}
	actuator, _ := newTestActuator(t, target)
	c := newTestControllerWith(t, f, actuator)

	decision, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if decision.Action != scaling.ActionWrite {
		t.Fatalf("first reconcile decided %s, want a write to set the scenario up", decision.Reason)
	}
	written := decision.TargetReplicas

	// The write lands, and the informer cache catches up.
	f.setReplicas(written)
	f.advance(time.Minute)

	next, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	if next.Reason == scaling.ReasonHoldExternalChange {
		t.Fatal("the controller treated its own write as an external change")
	}

	c.mu.Lock()
	drifts := len(c.state.externalDriftAt)
	c.mu.Unlock()

	if drifts != 0 {
		t.Errorf("recorded %d external drifts after observing its own write, want 0", drifts)
	}
}

// An actual external change must still be detected once the controller can
// write, otherwise adding the write path would silently remove FR-31.
func TestReconcile_ExternalChangesAreStillDetectedInLiveMode(t *testing.T) {
	t.Parallel()

	f := scaleUpFixture()
	target := &stubTarget{replicas: 2, resourceVersion: "1"}
	actuator, _ := newTestActuator(t, target)
	c := newTestControllerWith(t, f, actuator)

	if _, err := c.Reconcile(context.Background()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	// Somebody runs kubectl scale --replicas=9, which is not what we wrote.
	f.setReplicas(9)
	f.advance(time.Minute)

	if _, err := c.Reconcile(context.Background()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	c.mu.Lock()
	drifts := len(c.state.externalDriftAt)
	c.mu.Unlock()

	if drifts != 1 {
		t.Errorf("recorded %d external drifts after a third-party write, want 1", drifts)
	}
}

// --- construction ---------------------------------------------------------

func TestNewScaleActuator_RequiresASafeEnvelope(t *testing.T) {
	t.Parallel()

	valid := ScaleActuatorOptions{
		Target:      &stubTarget{},
		MinReplicas: 1,
		MaxReplicas: 12,
		RetryBudget: time.Second,
	}

	for _, tc := range []struct {
		name   string
		mutate func(*ScaleActuatorOptions)
	}{
		{"no target", func(o *ScaleActuatorOptions) { o.Target = nil }},
		{"zero minReplicas", func(o *ScaleActuatorOptions) { o.MinReplicas = 0 }},
		{"maximum below minimum", func(o *ScaleActuatorOptions) { o.MaxReplicas = 0 }},
		{"no retry budget", func(o *ScaleActuatorOptions) { o.RetryBudget = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := valid
			tc.mutate(&opts)

			if _, err := NewScaleActuator(opts); err == nil {
				t.Errorf("NewScaleActuator accepted %s", tc.name)
			}
		})
	}

	actuator, err := NewScaleActuator(valid)
	if err != nil {
		t.Fatalf("NewScaleActuator rejected a valid configuration: %v", err)
	}
	if actuator.Mode() != "live" {
		t.Errorf("Mode() = %q, want %q so that logs name the mode unambiguously", actuator.Mode(), "live")
	}
}

// --- helpers --------------------------------------------------------------

// testRetryBudget is short enough to keep the tests fast and long enough to
// allow several attempts at the 100 ms initial delay.
const testRetryBudget = 5 * time.Second

// scaleUpFixture is scalableFixture with enough pressure to decide a scale-up.
//
// The shared fixture's 50 items against the default itemsPerReplica of 50 asks
// for one replica, which is a scale-*down* from two — fine for the tests that
// use it, wrong for every test here that needs a write to happen. 300 items
// asks for six, and the cluster has room for more than the four that would
// take.
func scaleUpFixture() *fixture {
	f := scalableFixture()
	f.pressure = []int64{300}
	return f
}

// stubTarget is a ScaleTarget that records what was asked of it.
//
// Hand-written rather than a generated fake so that the failure injection is
// explicit: writeErrs fails the first N attempts and then succeeds, which is
// the shape of a transient outage, while writeErr fails every attempt, which is
// the shape of an outage that outlives the reconcile.
type stubTarget struct {
	replicas        int32
	resourceVersion string

	currentErr error
	writeErr   error
	writeErrs  []error

	currentCalls int
	writes       []stubWrite
}

type stubWrite struct {
	replicas        int32
	resourceVersion string
}

func (s *stubTarget) Current(context.Context) (kubernetesaccess.ScaleState, error) {
	s.currentCalls++
	if s.currentErr != nil {
		return kubernetesaccess.ScaleState{}, s.currentErr
	}
	return kubernetesaccess.ScaleState{Replicas: s.replicas, ResourceVersion: s.resourceVersion}, nil
}

func (s *stubTarget) Write(_ context.Context, replicas int32, resourceVersion string) error {
	s.writes = append(s.writes, stubWrite{replicas: replicas, resourceVersion: resourceVersion})

	if len(s.writeErrs) > 0 {
		err := s.writeErrs[0]
		s.writeErrs = s.writeErrs[1:]
		return err
	}
	if s.writeErr != nil {
		return s.writeErr
	}

	s.replicas = replicas
	return nil
}

// actuatorProbe records the delays the actuator asked for.
type actuatorProbe struct {
	sleeps []time.Duration
}

// newTestActuator builds a live actuator whose clock and sleep are driven by
// the test, so the retry path is asserted on the delays it requests rather than
// by waiting for them. The injected sleep advances the same clock the deadline
// is measured against, which is what makes the budget terminate.
func newTestActuator(t *testing.T, target ScaleTarget) (*ScaleActuator, *actuatorProbe) {
	t.Helper()

	actuator, err := NewScaleActuator(ScaleActuatorOptions{
		Target:      target,
		MinReplicas: 1,
		MaxReplicas: 12,
		RetryBudget: testRetryBudget,
		Metrics:     observability.NewMetrics(),
		Log:         discardLogger(),
	})
	if err != nil {
		t.Fatalf("NewScaleActuator: %v", err)
	}

	clock := &fakeClock{now: start}
	probe := &actuatorProbe{}

	actuator.now = clock.now_
	actuator.sleep = func(ctx context.Context, d time.Duration) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		probe.sleeps = append(probe.sleeps, d)
		clock.advance(d)
		return nil
	}
	// Deterministic: the jitter's distribution is not what these tests are
	// about, and a random delay would make the budget assertions flaky.
	actuator.jitter = func(d time.Duration) time.Duration { return d }

	return actuator, probe
}

// writeDecision builds the minimum decision the actuator acts on.
func writeDecision(direction scaling.Direction, current, target int32) scaling.Decision {
	reason := scaling.ReasonScaleUp
	if direction == scaling.DirectionDown {
		reason = scaling.ReasonScaleDown
	}
	return scaling.Decision{
		Reason:          reason,
		Direction:       direction,
		Action:          scaling.ActionWrite,
		CurrentReplicas: current,
		TargetReplicas:  target,
	}
}
