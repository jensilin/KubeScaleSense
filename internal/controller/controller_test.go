package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"

	"github.com/jensilin/KubeScaleSense/internal/config"
	kubernetesaccess "github.com/jensilin/KubeScaleSense/internal/kubernetes"
	kssmetrics "github.com/jensilin/KubeScaleSense/internal/metrics"
	"github.com/jensilin/KubeScaleSense/internal/observability"
	"github.com/jensilin/KubeScaleSense/internal/resources"
	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

const miB int64 = 1024 * 1024

var start = time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)

// --- construction ---------------------------------------------------------

func TestNew_RequiresItsCollaborators(t *testing.T) {
	t.Parallel()

	valid := func() Options {
		return Options{
			Config:   testConfig(),
			Reader:   &kubernetesaccess.StaticReader{Synced: true},
			Signal:   &scriptedSignal{},
			Actuator: NewDryRunActuator(discardLogger()),
		}
	}

	if _, err := New(valid()); err != nil {
		t.Fatalf("a complete Options must be accepted: %v", err)
	}

	for name, breakIt := range map[string]func(*Options){
		"config":   func(o *Options) { o.Config = nil },
		"reader":   func(o *Options) { o.Reader = nil },
		"signal":   func(o *Options) { o.Signal = nil },
		"actuator": func(o *Options) { o.Actuator = nil },
	} {
		opts := valid()
		breakIt(&opts)
		if _, err := New(opts); err == nil {
			t.Errorf("a missing %s must be rejected at construction, not discovered on the first reconcile", name)
		}
	}
}

// --- VerifyTarget: the checks config validation could not perform ---------

func TestVerifyTarget(t *testing.T) {
	t.Parallel()

	t.Run("returns the effective pod request", func(t *testing.T) {
		t.Parallel()

		c := newTestController(t, fixture{deployment: deployment(2, withRequests(500, 512))})

		got, err := c.VerifyTarget(context.Background())
		if err != nil {
			t.Fatalf("VerifyTarget: %v", err)
		}
		if got.CPUMilli != 500 || got.MemoryBytes != 512*miB {
			t.Errorf("request = %v, want 500m/512Mi", got)
		}
	})

	// A-03: a template with no requests makes every fit calculation
	// meaningless, so the controller refuses to start rather than reporting
	// confident nonsense.
	t.Run("a template without requests is fatal", func(t *testing.T) {
		t.Parallel()

		c := newTestController(t, fixture{deployment: deployment(2)})

		_, err := c.VerifyTarget(context.Background())
		if err == nil {
			t.Fatal("a template without resource requests must fail startup")
		}
		if !containsAll(err.Error(), "cpu", "A-03") {
			t.Errorf("error should explain what to add and why, got: %v", err)
		}
	})

	// FR-19: two controllers writing one replica count cannot be reconciled.
	t.Run("a competing HPA is fatal", func(t *testing.T) {
		t.Parallel()

		c := newTestController(t, fixture{
			deployment: deployment(2, withRequests(500, 512)),
			hpas:       []*autoscalingv2.HorizontalPodAutoscaler{hpa("competitor", "normalizer")},
		})

		_, err := c.VerifyTarget(context.Background())
		if err == nil {
			t.Fatal("a competing HPA must fail startup")
		}
		if !containsAll(err.Error(), "competitor", "FR-19") {
			t.Errorf("error should name the HPA and the requirement, got: %v", err)
		}
	})

	t.Run("an HPA on another workload is fine", func(t *testing.T) {
		t.Parallel()

		c := newTestController(t, fixture{
			deployment: deployment(2, withRequests(500, 512)),
			hpas:       []*autoscalingv2.HorizontalPodAutoscaler{hpa("unrelated", "something-else")},
		})

		if _, err := c.VerifyTarget(context.Background()); err != nil {
			t.Errorf("VerifyTarget: %v", err)
		}
	})

	t.Run("an unreadable target is fatal", func(t *testing.T) {
		t.Parallel()

		c := newTestController(t, fixture{})

		if _, err := c.VerifyTarget(context.Background()); err == nil {
			t.Error("a missing target Deployment must fail startup")
		}
	})
}

// --- the reconcile loop ---------------------------------------------------

// The headline behaviour of the phase: a real scale-up decision, reported in
// full, with the cluster left exactly as it was.
func TestReconcile_DecidesWithoutChangingAnything(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{400} // wants 8 replicas

	actuator := &recordingActuator{}
	c := newTestControllerWith(t, f, actuator)

	before := f.deployment.DeepCopy()

	decision, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if decision.Reason != scaling.ReasonScaleUp {
		t.Errorf("Reason = %q, want %q", decision.Reason, scaling.ReasonScaleUp)
	}
	if decision.TargetReplicas != 6 {
		t.Errorf("TargetReplicas = %d, want 6 (2 + maxScaleUpStep)", decision.TargetReplicas)
	}
	if decision.FitCapacity <= 0 {
		t.Errorf("FitCapacity = %d, want the fixture's spare room to be counted", decision.FitCapacity)
	}

	// The decision reached the actuator, which is the seam a write would
	// eventually pass through.
	if len(actuator.applied) != 1 || actuator.applied[0].Reason != scaling.ReasonScaleUp {
		t.Errorf("actuator saw %v, want one ScaleUp", actuator.applied)
	}

	// And the cluster object is untouched, field for field.
	if got := *f.deployment.Spec.Replicas; got != *before.Spec.Replicas {
		t.Errorf("spec.replicas = %d, want it unchanged at %d", got, *before.Spec.Replicas)
	}
	if f.deployment.Generation != before.Generation {
		t.Error("the Deployment's generation changed; something wrote to it")
	}
}

// The default actuator is the dry-run one, and it changes nothing even when the
// decision says to scale.
func TestDryRunActuator_WritesNothing(t *testing.T) {
	t.Parallel()

	logger, logs := captureLogger()
	actuator := NewDryRunActuator(logger)

	if actuator.Mode() != "dry-run" {
		t.Errorf("Mode() = %q, want dry-run", actuator.Mode())
	}

	err := actuator.Apply(context.Background(), scaling.Decision{
		Reason:          scaling.ReasonScaleUp,
		Action:          scaling.ActionWrite,
		Direction:       scaling.DirectionUp,
		CurrentReplicas: 2,
		TargetReplicas:  6,
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	out := logs.String()
	// "would" rather than "scaled": a log line reading "scaled 2 -> 6" from a
	// controller that scaled nothing is how a dry-run trace gets mistaken for
	// evidence that autoscaling works.
	if !containsAll(out, "dry run", "would_action", "never written") {
		t.Errorf("the dry-run log must be unmistakable, got: %s", out)
	}
	if containsAll(out, "scaled ") {
		t.Errorf("the log must not claim a scale happened, got: %s", out)
	}

	// A hold is a no-op, not a log line.
	logs.reset()
	if err := actuator.Apply(context.Background(), scaling.Decision{Reason: scaling.ReasonHoldCooldown, Action: scaling.ActionNone}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if logs.String() != "" {
		t.Errorf("a hold must produce no actuator output, got: %s", logs.String())
	}
}

// Counterfactual bookkeeping: in dry-run the cooldown timers advance as though
// the write had happened. Without it a dry run would re-report ScaleUp every
// interval forever and the cooldown machinery would go unexercised in exactly
// the phase built to validate it.
func TestReconcile_CooldownAdvancesInDryRun(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{400, 400, 400}

	c := newTestControllerWith(t, f, &recordingActuator{})

	first, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if first.Reason != scaling.ReasonScaleUp {
		t.Fatalf("first Reason = %q, want %q", first.Reason, scaling.ReasonScaleUp)
	}

	f.advance(15 * time.Second)
	second, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if second.Reason != scaling.ReasonHoldCooldown {
		t.Errorf("second Reason = %q, want %q: the timer must advance even though nothing was written", second.Reason, scaling.ReasonHoldCooldown)
	}

	// Past the cooldown it acts again.
	f.advance(2 * time.Minute)
	third, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("third Reconcile: %v", err)
	}
	if third.Reason != scaling.ReasonScaleUp {
		t.Errorf("third Reason = %q, want %q once the cooldown has passed", third.Reason, scaling.ReasonScaleUp)
	}
}

// Only reconciles that computed demand may append to the stabilization window.
// Recording a zero for a metrics outage would let the outage empty the window
// and permit a scale-down justified by missing data (DR-04).
func TestReconcile_StaleSignalDoesNotPopulateTheWindow(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.signalUnavailable = true

	c := newTestControllerWith(t, f, &recordingActuator{})

	for range 30 {
		decision, err := c.Reconcile(context.Background())
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if decision.Reason != scaling.ReasonHoldStaleMetrics {
			t.Fatalf("Reason = %q, want %q", decision.Reason, scaling.ReasonHoldStaleMetrics)
		}
		if decision.DemandComputed {
			t.Fatal("demand cannot have been computed from an unavailable signal")
		}
		f.advance(15 * time.Second)
	}

	c.mu.Lock()
	samples := len(c.state.desiredHistory)
	c.mu.Unlock()

	if samples != 0 {
		t.Errorf("desiredHistory has %d samples after 30 blind reconciles, want 0", samples)
	}
}

// The window fills from real observations and then permits a scale-down: the
// full DR-04 / FS-27 path, driven through the controller rather than asserted
// on a hand-built snapshot.
func TestReconcile_ScalesDownOnlyAfterTheWindowIsCovered(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.deployment = deployment(8, withRequests(500, 512), withStatus(8, 8))
	f.pods = replicaPods(8)
	f.pressure = []int64{0}

	c := newTestControllerWith(t, f, &recordingActuator{})

	// The first reconciles hold: the window is not yet covered.
	for range 5 {
		decision, err := c.Reconcile(context.Background())
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if decision.Reason != scaling.ReasonHoldStabilizationWindow {
			t.Fatalf("early Reason = %q, want %q", decision.Reason, scaling.ReasonHoldStabilizationWindow)
		}
		f.advance(15 * time.Second)
	}

	// Keep going until the 300 s window is both populated and long enough.
	var last scaling.Decision
	for range 25 {
		var err error
		last, err = c.Reconcile(context.Background())
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if last.Reason == scaling.ReasonScaleDown {
			break
		}
		f.advance(15 * time.Second)
	}

	if last.Reason != scaling.ReasonScaleDown {
		t.Fatalf("Reason = %q, want %q once the window is covered", last.Reason, scaling.ReasonScaleDown)
	}
	if last.TargetReplicas != 7 {
		t.Errorf("TargetReplicas = %d, want 7 (one step down from 8)", last.TargetReplicas)
	}
	if got := *f.deployment.Spec.Replicas; got != 8 {
		t.Errorf("spec.replicas = %d, want it still 8: a ScaleDown decision must not write either", got)
	}
}

// History is retained for longer than one window, because the coverage check
// requires the oldest sample to be at least a window old. Retaining exactly one
// window would make the gate permanently unsatisfiable.
func TestHistoryRetention_ExceedsTheWindow(t *testing.T) {
	t.Parallel()

	if historyRetention < 2 {
		t.Fatalf("historyRetention = %d; it must exceed one window or a scale-down can never become eligible", historyRetention)
	}
}

func TestPruneHistory(t *testing.T) {
	t.Parallel()

	history := []scaling.DesiredSample{
		{At: start.Add(-20 * time.Minute), Desired: 9},
		{At: start.Add(-2 * time.Minute), Desired: 3},
		{At: start, Desired: 1},
	}

	got := pruneHistory(history, 10*time.Minute, start)
	if len(got) != 2 {
		t.Errorf("pruneHistory kept %d samples, want 2", len(got))
	}
	for _, sample := range got {
		if start.Sub(sample.At) > 10*time.Minute {
			t.Errorf("sample from %v survived a 10m retention", sample.At)
		}
	}
}

// --- external drift -------------------------------------------------------

// Phase 1 issues no writes, so every change to spec.replicas is by definition
// someone else's — which makes this the ideal phase to validate the detection,
// before there is a write of our own to confuse it with.
func TestReconcile_DetectsExternalReplicaChanges(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{50} // quiet: the decision stays out of the way

	c := newTestControllerWith(t, f, &recordingActuator{})

	// The first reconcile establishes the baseline and reports no drift.
	if _, err := c.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// A GitOps loop rewrites the replica count three times, which is the
	// configured tolerance.
	for i, replicas := range []int32{4, 5, 6} {
		f.advance(15 * time.Second)
		f.setReplicas(replicas)

		decision, err := c.Reconcile(context.Background())
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if decision.Reason == scaling.ReasonHoldExternalChange {
			t.Fatalf("drift %d: held at the tolerance boundary, which should still be adopted silently", i+1)
		}
	}

	// The fourth exceeds the tolerance, and the controller stops acting rather
	// than fighting an actor it cannot outrun.
	f.advance(15 * time.Second)
	f.setReplicas(7)

	decision, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if decision.Reason != scaling.ReasonHoldExternalChange {
		t.Errorf("Reason = %q, want %q after exceeding externalChangeTolerance", decision.Reason, scaling.ReasonHoldExternalChange)
	}
}

// A drift resets the history, because the history described a replica count
// that no longer exists.
func TestReconcile_ExternalDriftResetsTheHistory(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{50}

	c := newTestControllerWith(t, f, &recordingActuator{})

	for range 5 {
		if _, err := c.Reconcile(context.Background()); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		f.advance(15 * time.Second)
	}

	c.mu.Lock()
	before := len(c.state.desiredHistory)
	c.mu.Unlock()
	if before == 0 {
		t.Fatal("the fixture should have accumulated history")
	}

	f.setReplicas(9)
	if _, err := c.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	c.mu.Lock()
	after := len(c.state.desiredHistory)
	c.mu.Unlock()

	// One sample: the reconcile that observed the drift.
	if after != 1 {
		t.Errorf("desiredHistory has %d samples after a drift, want 1", after)
	}
}

// An actor that changed the count once an hour ago must not disable scaling
// permanently, so drifts are counted only within the stabilization window.
func TestReconcile_DriftCountDecaysOutOfTheWindow(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{50}

	c := newTestControllerWith(t, f, &recordingActuator{})

	if _, err := c.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	for _, replicas := range []int32{3, 4, 5, 6} {
		f.advance(15 * time.Second)
		f.setReplicas(replicas)
		if _, err := c.Reconcile(context.Background()); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}

	// An hour later the drifts have aged out of the window.
	f.advance(time.Hour)
	decision, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if decision.Reason == scaling.ReasonHoldExternalChange {
		t.Error("old drifts must age out of the window rather than disabling scaling forever")
	}
}

// --- failure handling -----------------------------------------------------

// An unreadable cluster produces ErrorAPIFailure and no action in either
// direction (I-7).
func TestReconcile_UnreadableClusterHoldsPosition(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	c := newTestControllerWith(t, f, &recordingActuator{})

	f.reader.Err = errors.New("apiserver unreachable")

	decision, err := c.Reconcile(context.Background())
	if err == nil {
		t.Fatal("an observation failure must be returned so the caller can log it")
	}
	if decision.Reason != scaling.ReasonErrorAPIFailure {
		t.Errorf("Reason = %q, want %q", decision.Reason, scaling.ReasonErrorAPIFailure)
	}
	if decision.Action != scaling.ActionNone {
		t.Errorf("Action = %q, want no action on an unreadable cluster", decision.Action)
	}
	if decision.TargetReplicas != decision.CurrentReplicas {
		t.Error("an invalid snapshot must not move the replica count")
	}
}

// Deciding from a half-populated cache would under-count requested resources
// and over-state capacity, so the controller declines to decide until the
// caches are synced.
func TestReconcile_UnsyncedCachesAreAnObservationFailure(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	c := newTestControllerWith(t, f, &recordingActuator{})
	f.reader.Synced = false

	decision, err := c.Reconcile(context.Background())
	if err == nil {
		t.Fatal("unsynced caches must be reported")
	}
	if decision.Reason != scaling.ReasonErrorAPIFailure {
		t.Errorf("Reason = %q, want %q", decision.Reason, scaling.ReasonErrorAPIFailure)
	}
}

// The template lost its requests after startup validation passed.
func TestReconcile_TemplateWithoutRequestsIsAnObservationFailure(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.deployment.Spec.Template.Spec.Containers[0].Resources.Requests = nil
	c := newTestControllerWith(t, f, &recordingActuator{})

	decision, err := c.Reconcile(context.Background())
	if err == nil {
		t.Fatal("a template that lost its requests must be reported")
	}
	if decision.Reason != scaling.ReasonErrorAPIFailure {
		t.Errorf("Reason = %q, want %q", decision.Reason, scaling.ReasonErrorAPIFailure)
	}
}

func TestReconcile_ActuatorFailureIsReported(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{400}

	wantErr := errors.New("actuator broke")
	c := newTestControllerWith(t, f, &recordingActuator{err: wantErr})

	if _, err := c.Reconcile(context.Background()); !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want it to wrap the actuator failure", err)
	}
}

func TestReady(t *testing.T) {
	t.Parallel()

	t.Run("unsynced caches", func(t *testing.T) {
		t.Parallel()

		f := scalableFixture()
		c := newTestControllerWith(t, f, &recordingActuator{})
		f.reader.Synced = false

		if err := c.Ready(); err == nil {
			t.Error("readiness must fail while the caches are unsynced")
		}
	})

	t.Run("before the first reconcile", func(t *testing.T) {
		t.Parallel()

		c := newTestControllerWith(t, scalableFixture(), &recordingActuator{})

		if err := c.Ready(); err == nil {
			t.Error("readiness must fail before a reconcile has completed")
		}
	})

	t.Run("after a successful reconcile", func(t *testing.T) {
		t.Parallel()

		c := newTestControllerWith(t, scalableFixture(), &recordingActuator{})
		if _, err := c.Reconcile(context.Background()); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if err := c.Ready(); err != nil {
			t.Errorf("Ready = %v, want nil", err)
		}
	})

	// `signal.source: none` never produces a usable sample and is a legitimate
	// configuration — the controller is then working exactly as designed,
	// freezing rather than guessing — so it must not fail readiness.
	t.Run("an unavailable signal does not fail readiness", func(t *testing.T) {
		t.Parallel()

		f := scalableFixture()
		f.signalUnavailable = true
		c := newTestControllerWith(t, f, &recordingActuator{})

		if _, err := c.Reconcile(context.Background()); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if err := c.Ready(); err != nil {
			t.Errorf("Ready = %v, want nil: a frozen controller with no signal is behaving correctly", err)
		}
	})

	t.Run("after a failed reconcile", func(t *testing.T) {
		t.Parallel()

		f := scalableFixture()
		c := newTestControllerWith(t, f, &recordingActuator{})

		f.reader.Err = errors.New("apiserver unreachable")
		if _, err := c.Reconcile(context.Background()); err == nil {
			t.Fatal("expected the reconcile to fail")
		}
		if err := c.Ready(); err == nil {
			t.Error("readiness must report the last failure")
		}
	})
}

// A failed reconcile is logged and the loop continues: exiting would turn a
// transient API blip into a restart loop that observes nothing at all.
func TestRun_ContinuesAfterAFailedReconcileAndStopsOnCancel(t *testing.T) {
	t.Parallel()

	f := scalableFixture()

	cfg := testConfig()
	cfg.Controller.Interval = config.Duration(10 * time.Millisecond)
	c := newTestControllerWith(t, f, &recordingActuator{}, withConfig(cfg))
	f.reader.Err = errors.New("apiserver unreachable")

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the context expired")
	}

	if c.reconcileCount() == 0 {
		t.Error("the loop should have attempted reconciles despite the failures")
	}
}

func TestRun_StopsPromptlyOnAnAlreadyCancelledContext(t *testing.T) {
	t.Parallel()

	c := newTestControllerWith(t, scalableFixture(), &recordingActuator{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return on a cancelled context")
	}
}

// --- fixtures -------------------------------------------------------------

type fixture struct {
	deployment *appsv1.Deployment
	replicaSet *appsv1.ReplicaSet
	nodes      []*corev1.Node
	pods       []*corev1.Pod
	hpas       []*autoscalingv2.HorizontalPodAutoscaler

	pressure          []int64
	signalUnavailable bool

	reader *kubernetesaccess.StaticReader
	clock  *fakeClock
}

// advance moves the fake clock and refreshes the nodes' Ready heartbeats.
//
// Both halves are needed: a real kubelet keeps reporting, and without the
// refresh every node would age past the heartbeat grace period and be excluded
// as stale — which is correct behaviour for a silent kubelet, but not what
// these tests are about. internal/resources covers the stale-heartbeat case
// directly.
func (f *fixture) advance(d time.Duration) {
	f.clock.advance(d)

	now := metav1.NewTime(f.clock.now_())
	for _, node := range f.nodes {
		for i := range node.Status.Conditions {
			if node.Status.Conditions[i].Type == corev1.NodeReady {
				node.Status.Conditions[i].LastHeartbeatTime = now
			}
		}
	}
}

// scalableFixture is a two-replica target on a cluster with room to grow.
func scalableFixture() *fixture {
	return &fixture{
		deployment: deployment(2, withRequests(500, 512), withStatus(2, 2)),
		nodes: []*corev1.Node{
			node("w-1", 4000, 8192, 110),
			node("w-2", 4000, 8192, 110),
		},
		pods:     replicaPods(2),
		pressure: []int64{50},
	}
}

func (f *fixture) setReplicas(n int32) {
	f.deployment.Spec.Replicas = &n
	f.deployment.Status.Replicas = n
	f.deployment.Status.ReadyReplicas = n
	f.deployment.Status.UpdatedReplicas = n
	f.pods = replicaPods(int(n))
	if f.reader != nil {
		f.reader.PodList = f.pods
	}
}

type controllerOpt func(*Options)

func withConfig(cfg *config.Config) controllerOpt {
	return func(o *Options) { o.Config = cfg }
}

func newTestController(t *testing.T, f fixture) *Controller {
	t.Helper()
	return newTestControllerWith(t, &f, &recordingActuator{})
}

func newTestControllerWith(t *testing.T, f *fixture, actuator Actuator, opts ...controllerOpt) *Controller {
	t.Helper()

	if f.replicaSet == nil && f.deployment != nil {
		f.replicaSet = replicaSet(f.deployment)
	}
	if f.clock == nil {
		f.clock = &fakeClock{now: start}
	}
	clock := f.clock.now_

	reader := &kubernetesaccess.StaticReader{
		Synced:   true,
		NodeList: f.nodes,
		PodList:  f.pods,
		HPAList:  f.hpas,
	}
	if f.deployment != nil {
		reader.DeploymentList = []*appsv1.Deployment{f.deployment}
		reader.ReplicaSetList = []*appsv1.ReplicaSet{f.replicaSet}
	}
	f.reader = reader

	resourceOpts, err := resources.OptionsFrom(testConfig().Resources)
	if err != nil {
		t.Fatalf("OptionsFrom: %v", err)
	}

	options := Options{
		Config:      testConfig(),
		Reader:      reader,
		Signal:      &scriptedSignal{values: f.pressure, unavailable: f.signalUnavailable, clock: clock},
		Utilization: kssmetrics.NewUtilizationCollector(&emptyMetricsLister{}),
		Resources:   resourceOpts,
		Metrics:     observability.NewMetrics(),
		Actuator:    actuator,
		Log:         discardLogger(),
		Clock:       clock,
	}
	for _, opt := range opts {
		opt(&options)
	}

	c, err := New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func testConfig() *config.Config {
	cfg := config.Default()
	cfg.Target.Namespace = "data-pipeline"
	cfg.Target.Deployment = "normalizer"
	cfg.Workload.Signal.Source = config.SignalSourceNone
	// Smoothing off so expectations stay exact; UT-13 covers the smoother.
	cfg.Workload.BacklogSmoothing.Mode = config.SmoothingModeNone
	return cfg
}

type deploymentOpt func(*appsv1.Deployment)

func deployment(replicas int32, opts ...deploymentOpt) *appsv1.Deployment {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "normalizer",
			Namespace:  "data-pipeline",
			UID:        "deployment-uid",
			Generation: 1,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "normalizer"}}},
			},
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Replicas:           replicas,
			UpdatedReplicas:    replicas,
		},
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

func withRequests(cpuMilli, memoryMiB int64) deploymentOpt {
	return func(d *appsv1.Deployment) {
		d.Spec.Template.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI),
			corev1.ResourceMemory: *resource.NewQuantity(memoryMiB*miB, resource.BinarySI),
		}
	}
}

func withStatus(ready, updated int32) deploymentOpt {
	return func(d *appsv1.Deployment) {
		d.Status.ReadyReplicas = ready
		d.Status.UpdatedReplicas = updated
	}
}

func replicaSet(owner *appsv1.Deployment) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "normalizer-1",
			Namespace:       owner.Namespace,
			UID:             "replicaset-uid",
			Annotations:     map[string]string{"deployment.kubernetes.io/revision": "1"},
			OwnerReferences: []metav1.OwnerReference{{UID: owner.UID, Kind: "Deployment", Name: owner.Name}},
		},
	}
}

func replicaPods(count int) []*corev1.Pod {
	pods := make([]*corev1.Pod, 0, count)
	for i := range count {
		pods = append(pods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "normalizer-1-" + string(rune('a'+i)),
				Namespace:       "data-pipeline",
				OwnerReferences: []metav1.OwnerReference{{UID: types.UID("replicaset-uid"), Kind: "ReplicaSet"}},
			},
			Spec: corev1.PodSpec{
				NodeName: "w-1",
				Containers: []corev1.Container{{
					Name: "normalizer",
					Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
						corev1.ResourceMemory: *resource.NewQuantity(512*miB, resource.BinarySI),
					}},
				}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(start.Add(-time.Hour))},
					{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(start.Add(-time.Hour))},
				},
			},
		})
	}
	return pods
}

func node(name string, cpuMilli, memoryMiB, pods int64) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(memoryMiB*miB, resource.BinarySI),
				corev1.ResourcePods:   *resource.NewQuantity(pods, resource.DecimalSI),
			},
			Conditions: []corev1.NodeCondition{{
				Type:              corev1.NodeReady,
				Status:            corev1.ConditionTrue,
				LastHeartbeatTime: metav1.NewTime(start),
			}},
		},
	}
}

func hpa(name, target string) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "data-pipeline"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Deployment", Name: target},
		},
	}
}

// --- doubles --------------------------------------------------------------

// recordingActuator captures what it was asked to do. It writes nothing, for
// the same reason the real one does not.
type recordingActuator struct {
	mu      sync.Mutex
	applied []scaling.Decision
	err     error
}

func (a *recordingActuator) Apply(_ context.Context, decision scaling.Decision) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if decision.Action == scaling.ActionWrite {
		a.applied = append(a.applied, decision)
	}
	return a.err
}

func (a *recordingActuator) Mode() string { return "recording" }

// scriptedSignal replays a fixed pressure series, holding the final value.
type scriptedSignal struct {
	values      []int64
	unavailable bool
	clock       func() time.Time

	mu     sync.Mutex
	cursor int
}

func (s *scriptedSignal) Source() string { return "test" }

func (s *scriptedSignal) Collect(ctx context.Context) (kssmetrics.Sample, error) {
	if err := ctx.Err(); err != nil {
		return kssmetrics.Unavailable(), err
	}
	if s.unavailable || len(s.values) == 0 {
		return kssmetrics.Unavailable(), nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	value := s.values[s.cursor]
	if s.cursor+1 < len(s.values) {
		s.cursor++
	}

	at := start
	if s.clock != nil {
		at = s.clock()
	}
	return kssmetrics.Sample{PressureItems: value, SampledAt: at, Available: true}, nil
}

func (s *scriptedSignal) Close() error { return nil }

// emptyMetricsLister stands in for a cluster with no metrics-server, which is
// the state the utilization signal must degrade gracefully in.
type emptyMetricsLister struct{}

func (emptyMetricsLister) ListPodMetrics(context.Context, string) ([]metricsv1beta1.PodMetrics, error) {
	return nil, nil
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) now_() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

type syncBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

func (b *syncBuffer) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = nil
}

func captureLogger() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	return slog.New(slog.NewJSONHandler(buf, nil)), buf
}

func containsAll(haystack string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(haystack, needle) {
			return false
		}
	}
	return true
}
