package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"

	"github.com/jensilin/KubeScaleSense/internal/config"
	kubernetesaccess "github.com/jensilin/KubeScaleSense/internal/kubernetes"
	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// FR-24 and IT-04: an event for every state change, the first occurrence of
// each hold announced at once, and repeats rate-limited to the backoff cadence.

func TestEvents_AnAppliedScaleUpIsAnnounced(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{400}
	events := &recordingEvents{}
	c := newTestControllerWith(t, f, &recordingActuator{}, withEvents(events))

	decision := reconcileOnce(t, c)
	if decision.Reason != scaling.ReasonScaleUp {
		t.Fatalf("setup: Reason = %s, want ScaleUp", decision.Reason)
	}

	got := events.only(t)
	if got.Reason != eventScaledUp || got.Type != kubernetesaccess.EventNormal || got.Action != string(scaling.ReasonScaleUp) {
		t.Errorf("event = %s/%s/%s, want %s/Normal/ScaleUp", got.Reason, got.Type, got.Action, eventScaledUp)
	}
	if !strings.Contains(got.Note, "2 -> 6") {
		t.Errorf("note %q does not say what changed", got.Note)
	}
}

// A write that did not happen is not a state change. The failure is in the log
// and the actuation metrics; an event saying "ScaledUp" would be a lie.
func TestEvents_AFailedWriteIsNotAnnounced(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{400}
	events := &recordingEvents{}
	c := newTestControllerWith(t, f, &recordingActuator{err: ErrScaleConflict}, withEvents(events))

	if _, err := c.Reconcile(context.Background()); !errors.Is(err, ErrScaleConflict) {
		t.Fatalf("setup: Reconcile returned %v, want the conflict", err)
	}
	if got := events.all(); len(got) != 0 {
		t.Errorf("a failed write produced events: %+v", got)
	}
}

func TestEvents_TheSteadyStateIsNotNews(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{100} // exactly two replicas' worth
	events := &recordingEvents{}
	c := newTestControllerWith(t, f, &recordingActuator{}, withEvents(events))

	for range 3 {
		if d := reconcileOnce(t, c); d.Reason != scaling.ReasonNoChangeWithinTolerance {
			t.Fatalf("setup: Reason = %s, want NoChangeWithinTolerance", d.Reason)
		}
		f.advance(15 * time.Second)
	}
	if got := events.all(); len(got) != 0 {
		t.Errorf("the steady state produced events: %+v", got)
	}
}

// A repeating hold is announced once, then once per backoff cycle: 30s, then
// 60s, on the default schedule.
func TestEvents_ARepeatingHoldFollowsTheBackoffCadence(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.hpas = []*autoscalingv2.HorizontalPodAutoscaler{hpa("normalizer", "normalizer")}
	events := &recordingEvents{}
	c := newTestControllerWith(t, f, &recordingActuator{}, withEvents(events))

	// Offsets from the first reconcile, and how many events should exist after
	// each one.
	for _, step := range []struct {
		at   time.Duration
		want int
	}{
		{0, 1},                 // first occurrence: at once
		{10 * time.Second, 1},  // within the first 30s cycle
		{31 * time.Second, 2},  // the cycle elapsed
		{61 * time.Second, 2},  // the next cycle is 60s, from 31s
		{91 * time.Second, 3},  // and has now elapsed
		{120 * time.Second, 3}, // the one after is 120s
	} {
		f.advance(step.at - (f.clock.now_().Sub(start)))
		if d := reconcileOnce(t, c); d.Reason != scaling.ReasonHoldScalingConflict {
			t.Fatalf("setup: Reason = %s, want HoldScalingConflict", d.Reason)
		}
		if got := len(events.all()); got != step.want {
			t.Fatalf("after %s: %d events, want %d", step.at, got, step.want)
		}
	}

	for _, event := range events.all() {
		if event.Reason != eventScalingConflict || event.Type != kubernetesaccess.EventWarning {
			t.Errorf("event = %s/%s, want %s/Warning", event.Reason, event.Type, eventScalingConflict)
		}
	}
}

func TestEvents_AnExternalChangeIsAnnouncedWhenAdopted(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	events := &recordingEvents{}
	c := newTestControllerWith(t, f, &recordingActuator{}, withEvents(events))

	reconcileOnce(t, c)
	before := len(events.all())

	f.setReplicas(5)
	reconcileOnce(t, c)

	var adopted []kubernetesaccess.Event
	for _, event := range events.all()[before:] {
		if event.Action == actionAdoptBaseline {
			adopted = append(adopted, event)
		}
	}
	if len(adopted) != 1 {
		t.Fatalf("%d adoption events, want 1", len(adopted))
	}
	if adopted[0].Reason != eventExternalScaleDetected || !strings.Contains(adopted[0].Note, "2 -> 5") {
		t.Errorf("event = %s %q, want %s naming 2 -> 5", adopted[0].Reason, adopted[0].Note, eventExternalScaleDetected)
	}

	// Announced once, not on every reconcile that follows.
	reconcileOnce(t, c)
	for _, event := range events.all()[before+len(adopted):] {
		if event.Action == actionAdoptBaseline {
			t.Error("the same adoption was announced twice")
		}
	}
}

// Under freeze the reason is HoldPendingPods, which on its own does not say a
// timeout passed. The event has to.
func TestEvents_APendingTimeoutIsAnnouncedUnderFreeze(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Pending.OnPendingTimeout = config.OnPendingFreeze

	f := scalableFixture()
	f.pressure = []int64{400}
	events := &recordingEvents{}
	c := newTestControllerWith(t, f, &recordingActuator{}, withConfig(cfg), withEvents(events))

	reconcileOnce(t, c)
	f.setReplicas(6)
	markPending(f.pods[2:], f.clock.now_())
	f.advance(pastPendingTimeout)
	reconcileOnce(t, c)

	last := events.all()[len(events.all())-1]
	if last.Reason != eventPodPendingTimeout || last.Type != kubernetesaccess.EventWarning {
		t.Fatalf("last event = %s/%s, want %s/Warning", last.Reason, last.Type, eventPodPendingTimeout)
	}
	if !strings.Contains(last.Note, "4 pods unschedulable") || !strings.Contains(last.Note, "freeze") {
		t.Errorf("note %q should name the stuck pods and the mode", last.Note)
	}
}

func TestEvents_ARevertIsAnnouncedWithItsTarget(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{400}
	events := &recordingEvents{}
	c := newTestControllerWith(t, f, &recordingActuator{}, withEvents(events))

	reconcileOnce(t, c)
	f.setReplicas(6)
	markPending(f.pods[2:], f.clock.now_())
	f.advance(pastPendingTimeout)
	reconcileOnce(t, c)

	last := events.all()[len(events.all())-1]
	if last.Reason != eventPodPendingTimeout || last.Action != string(scaling.ReasonRevertPendingPods) {
		t.Fatalf("last event = %s/%s, want %s/%s", last.Reason, last.Action, eventPodPendingTimeout,
			scaling.ReasonRevertPendingPods)
	}
	if !strings.Contains(last.Note, "reverting 6 -> 2") {
		t.Errorf("note %q should say what the revert did", last.Note)
	}
}

// An event is a note for a human. Failing to write one must not fail, delay,
// or change the reconcile it describes.
func TestEvents_ARecorderFailureDoesNotAffectTheReconcile(t *testing.T) {
	t.Parallel()

	f := scalableFixture()
	f.pressure = []int64{400}
	actuator := &recordingActuator{}
	c := newTestControllerWith(t, f, actuator, withEvents(&recordingEvents{err: errors.New("forbidden")}))

	decision, err := c.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile returned %v; an event failure leaked into the reconcile", err)
	}
	if decision.Reason != scaling.ReasonScaleUp || len(actuator.writes()) != 1 {
		t.Errorf("the scale-up did not go ahead: %s, %d writes", decision.Reason, len(actuator.writes()))
	}
	if err := c.Ready(); err != nil {
		t.Errorf("readiness failed over an event: %v", err)
	}
}

// Every reason the engine can return is either announced with a type, a
// reason, and a message, or deliberately silent.
func TestDescribeDecision_CoversEveryReason(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	silent := map[scaling.Reason]bool{
		scaling.ReasonNoChangeWithinTolerance: true,
		scaling.ReasonErrorAPIFailure:         true,
	}

	for _, reason := range scaling.Reasons {
		event, ok := describeDecision(scaling.Decision{Reason: reason, Action: reason.Action()}, scaling.Snapshot{}, cfg, start)
		if silent[reason] {
			if ok {
				t.Errorf("%s is announced; it should be silent", reason)
			}
			continue
		}
		if !ok {
			t.Errorf("%s produces no event (FR-24)", reason)
			continue
		}
		if event.Type != kubernetesaccess.EventNormal && event.Type != kubernetesaccess.EventWarning {
			t.Errorf("%s: type %q", reason, event.Type)
		}
		if event.Reason == "" || event.Note == "" {
			t.Errorf("%s: empty reason or note: %+v", reason, event)
		}
		if event.Action != string(reason) {
			t.Errorf("%s: action %q, want the decision reason so the event matches the log line", reason, event.Action)
		}
	}
}

func TestHoldEventLimiter(t *testing.T) {
	t.Parallel()

	backoff := config.HoldBackoffConfig{
		Initial: config.Duration(30 * time.Second),
		Max:     config.Duration(5 * time.Minute),
		Factor:  2,
	}

	t.Run("the cadence grows to max and stays there", func(t *testing.T) {
		t.Parallel()

		want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 5 * time.Minute, 5 * time.Minute}
		for n, w := range want {
			if got := cycle(n+1, backoff); got != w {
				t.Errorf("cycle(%d) = %s, want %s", n+1, got, w)
			}
		}
	})

	t.Run("a reason quiet for longer than max is a first occurrence again", func(t *testing.T) {
		t.Parallel()

		l := holdEventLimiter{}
		if !l.due("HoldCooldown", start, backoff) {
			t.Fatal("first occurrence not due")
		}
		if l.due("HoldCooldown", start.Add(time.Second), backoff) {
			t.Fatal("repeat due inside the first cycle")
		}
		if !l.due("HoldCooldown", start.Add(time.Second+5*time.Minute+time.Second), backoff) {
			t.Error("a recurrence after a quiet period was suppressed")
		}
	})

	t.Run("alternating reasons each keep their own cadence", func(t *testing.T) {
		t.Parallel()

		l := holdEventLimiter{}
		emitted := 0
		for i := range 20 {
			key := "HoldCooldown"
			if i%2 == 1 {
				key = "HoldBackoff"
			}
			if l.due(key, start.Add(time.Duration(i)*5*time.Second), backoff) {
				emitted++
			}
		}
		// 100s of flapping: each reason at 0, 30s, and 90s.
		if emitted != 6 {
			t.Errorf("%d events over 100s of flapping between two reasons, want 6", emitted)
		}
	})
}

// --- helpers --------------------------------------------------------------

func withEvents(e EventRecorder) controllerOpt {
	return func(o *Options) { o.Events = e }
}

type recordingEvents struct {
	mu       sync.Mutex
	recorded []kubernetesaccess.Event
	err      error
}

func (r *recordingEvents) Record(_ context.Context, event kubernetesaccess.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.recorded = append(r.recorded, event)
	return nil
}

func (r *recordingEvents) all() []kubernetesaccess.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]kubernetesaccess.Event(nil), r.recorded...)
}

func (r *recordingEvents) only(t *testing.T) kubernetesaccess.Event {
	t.Helper()
	all := r.all()
	if len(all) != 1 {
		t.Fatalf("%d events recorded, want exactly 1: %+v", len(all), all)
	}
	return all[0]
}
