package scaling

import (
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/config"
	"github.com/jensilin/KubeScaleSense/internal/resources"
)

// now is fixed. Decide takes the time as a parameter precisely so that no test
// in this package ever sleeps or reads the clock.
var now = time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)

// --- UT-01: backlog-derived target ---------------------------------------

func TestDecide_BacklogDerivedTarget(t *testing.T) {
	t.Parallel()

	// itemsPerReplica = 50, so the target is ceil(backlog / 50).
	tests := []struct {
		backlog int64
		want    int32
	}{
		{backlog: 0, want: 0},
		{backlog: 1, want: 1},
		{backlog: 49, want: 1},
		{backlog: 50, want: 1},
		{backlog: 51, want: 2},
		{backlog: 400, want: 8},
	}

	for _, tt := range tests {
		s := baseSnapshot()
		s.Backlog = fresh(tt.backlog)

		got := Decide(s, baseConfig(), now)
		if got.DesiredBacklog != tt.want {
			t.Errorf("backlog %d: DesiredBacklog = %d, want %d", tt.backlog, got.DesiredBacklog, tt.want)
		}
	}
}

// ceilDiv guards the division itself. itemsPerReplica <= 0 is rejected at
// config time, but a division-by-zero panic in the decision engine would take
// the controller down rather than degrade it, so the floor is belt and braces.
func TestCeilDiv_NonPositiveInputs(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ a, b, want int64 }{
		{a: 0, b: 50, want: 0},
		{a: -5, b: 50, want: 0},
		{a: 100, b: 0, want: 0},
		{a: 100, b: -50, want: 0},
		{a: 1, b: 1, want: 1},
	} {
		if got := ceilDiv(tc.a, tc.b); got != tc.want {
			t.Errorf("ceilDiv(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

// --- UT-02: utilization-derived target -----------------------------------

func TestDecide_UtilizationDerivedTarget(t *testing.T) {
	t.Parallel()

	// ceil(ReadyReplicas x U / U_target), with U in milli-percent and a 70 %
	// target: 4 replicas at 90 % want ceil(4 x 90 / 70) = 6.
	tests := []struct {
		name       string
		ready      int32
		milliPct   int64
		available  bool
		sampledAgo time.Duration
		want       int32
	}{
		{name: "at target", ready: 4, milliPct: 70_000, available: true, want: 4},
		{name: "above target", ready: 4, milliPct: 90_000, available: true, want: 6},
		{name: "well below target", ready: 4, milliPct: 10_000, available: true, want: 1},
		{name: "idle", ready: 4, milliPct: 0, available: true, want: 0},
		{name: "fractional rounds up", ready: 3, milliPct: 75_000, available: true, want: 4},
		{
			// No Ready pods means no denominator. The signal is omitted rather
			// than divided by zero.
			name: "no ready replicas omits the signal", ready: 0, milliPct: 90_000, available: true, want: 0,
		},
		{
			// Utilization is optional: missing it degrades the decision to
			// backlog-only rather than freezing it.
			name: "unavailable omits the signal", ready: 4, milliPct: 90_000, available: false, want: 0,
		},
		{
			name: "stale omits the signal", ready: 4, milliPct: 90_000, available: true, sampledAgo: 5 * time.Minute, want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := baseSnapshot()
			s.ReadyReplicas = tt.ready
			s.CurrentReplicas = maxInt32(tt.ready, 1)
			s.AvgCPUMilliPercent = Signal[int64]{
				Value:     tt.milliPct,
				SampledAt: now.Add(-tt.sampledAgo),
				Available: tt.available,
			}

			got := Decide(s, baseConfig(), now)
			if got.DesiredUtilization != tt.want {
				t.Errorf("DesiredUtilization = %d, want %d", got.DesiredUtilization, tt.want)
			}
		})
	}
}

// --- UT-03: signal combination -------------------------------------------

func TestDecide_CombinesSignalsWithMax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		backlog  int64
		ready    int32
		milliPct int64
		wantRaw  int32
	}{
		{name: "backlog alone raises", backlog: 400, ready: 2, milliPct: 10_000, wantRaw: 8},
		{name: "utilization alone raises", backlog: 50, ready: 6, milliPct: 95_000, wantRaw: 9},
		{name: "the higher of the two wins", backlog: 400, ready: 6, milliPct: 95_000, wantRaw: 9},
		{name: "both low", backlog: 50, ready: 2, milliPct: 10_000, wantRaw: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := baseSnapshot()
			s.Backlog = fresh(tt.backlog)
			s.ReadyReplicas = tt.ready
			s.CurrentReplicas = tt.ready
			s.AvgCPUMilliPercent = freshMilliPercent(tt.milliPct)

			if got := Decide(s, baseConfig(), now).DesiredRaw; got != tt.wantRaw {
				t.Errorf("DesiredRaw = %d, want %d", got, tt.wantRaw)
			}
		})
	}
}

// A backlog keeps the floor up on its own, which is why "never scale down while
// work is outstanding" needs no separate guard (FR-22).
func TestDecide_BacklogBlocksScaleDownAtLowCPU(t *testing.T) {
	t.Parallel()

	s := readyToScaleDown()
	s.CurrentReplicas, s.ReadyReplicas = 8, 8
	s.Backlog = fresh(400)                          // wants 8
	s.AvgCPUMilliPercent = freshMilliPercent(5_000) // 5 %: wants 1
	s.DesiredHistory = quietHistory(now, 1)

	got := Decide(s, baseConfig(), now)
	if got.Reason != ReasonNoChangeWithinTolerance {
		t.Errorf("Reason = %q, want %q: the backlog target matches the current count", got.Reason, ReasonNoChangeWithinTolerance)
	}
	if got.TargetReplicas != 8 {
		t.Errorf("TargetReplicas = %d, want 8: idle CPU must not remove replicas that have work queued", got.TargetReplicas)
	}
}

// --- UT-04: clamping ------------------------------------------------------

func TestDecide_Clamping(t *testing.T) {
	t.Parallel()

	cfg := baseConfig() // min 1, max 12

	t.Run("at max with demand above it", func(t *testing.T) {
		t.Parallel()

		s := baseSnapshot()
		s.CurrentReplicas, s.ReadyReplicas = 12, 12
		s.Backlog = fresh(2000) // wants 40

		got := Decide(s, cfg, now)
		if got.Reason != ReasonHoldAtMaxReplicas {
			t.Errorf("Reason = %q, want %q", got.Reason, ReasonHoldAtMaxReplicas)
		}
		// The unclamped value is reported so the shortfall is visible: this is
		// what turns "we are at max" into "raise the limit to 40".
		if got.DesiredRaw != 40 {
			t.Errorf("DesiredRaw = %d, want 40 — the unclamped demand must be reported", got.DesiredRaw)
		}
		if got.DesiredClamped != 12 {
			t.Errorf("DesiredClamped = %d, want 12", got.DesiredClamped)
		}
		if got.TargetReplicas != 12 {
			t.Errorf("TargetReplicas = %d, want 12: a hold changes nothing", got.TargetReplicas)
		}
	})

	t.Run("below max the clamp does not bind", func(t *testing.T) {
		t.Parallel()

		s := baseSnapshot()
		s.CurrentReplicas, s.ReadyReplicas = 8, 8
		s.Backlog = fresh(2000)
		s.FitCapacity = 10

		got := Decide(s, cfg, now)
		if got.Reason != ReasonScaleUp {
			t.Errorf("Reason = %q, want %q: demand exceeds max but we are not at max yet", got.Reason, ReasonScaleUp)
		}
		if got.TargetReplicas != 12 {
			t.Errorf("TargetReplicas = %d, want 12 (clamped, and within the 4-replica step)", got.TargetReplicas)
		}
	})

	t.Run("at min with demand below it", func(t *testing.T) {
		t.Parallel()

		s := readyToScaleDown()
		s.CurrentReplicas, s.ReadyReplicas = 1, 1
		s.Backlog = fresh(0)

		got := Decide(s, cfg, now)
		if got.Reason != ReasonHoldAtMinReplicas {
			t.Errorf("Reason = %q, want %q", got.Reason, ReasonHoldAtMinReplicas)
		}
		if got.TargetReplicas != 1 {
			t.Errorf("TargetReplicas = %d, want 1", got.TargetReplicas)
		}
	})

	t.Run("above min the clamp does not bind", func(t *testing.T) {
		t.Parallel()

		s := readyToScaleDown()
		s.CurrentReplicas, s.ReadyReplicas = 4, 4
		s.Backlog = fresh(0)

		got := Decide(s, cfg, now)
		if got.Reason != ReasonScaleDown {
			t.Errorf("Reason = %q, want %q", got.Reason, ReasonScaleDown)
		}
	})
}

// Someone else set the replica count above maxReplicas. The clamp is a hard
// bound rather than a target to converge on gradually, so the decision returns
// into range at once instead of spending a step limit per interval outside the
// workload's own policy.
func TestDecide_ScaleDownFromAboveMaxReturnsIntoRangeImmediately(t *testing.T) {
	t.Parallel()

	s := readyToScaleDown()
	s.CurrentReplicas, s.ReadyReplicas = 40, 40
	s.Backlog = fresh(0)
	s.DesiredHistory = quietHistory(now, 1)

	got := Decide(s, baseConfig(), now)
	if got.Reason != ReasonScaleDown {
		t.Fatalf("Reason = %q, want %q", got.Reason, ReasonScaleDown)
	}
	if got.TargetReplicas != 12 {
		t.Errorf("TargetReplicas = %d, want 12 (maxReplicas): the clamp is not subject to the step limit", got.TargetReplicas)
	}
}

// --- UT-05: deadband ------------------------------------------------------

func TestDecide_Deadband(t *testing.T) {
	t.Parallel()

	cfg := baseConfig() // tolerancePercent 10

	tests := []struct {
		name    string
		current int32
		desired int32
		want    bool // within tolerance
	}{
		{name: "identical", current: 20, desired: 20, want: true},
		{name: "one below the band", current: 20, desired: 21, want: true},
		{name: "exactly at the band acts", current: 20, desired: 22, want: false},
		{name: "beyond the band acts", current: 20, desired: 25, want: false},
		{name: "down, inside the band", current: 20, desired: 19, want: true},
		{name: "down, exactly at the band acts", current: 20, desired: 18, want: false},
		// Relative tolerance is inert at small counts, by design: at two
		// replicas every change is significant and hysteresis comes from the
		// cooldowns instead.
		{name: "2 to 3 always acts", current: 2, desired: 3, want: false},
		{name: "1 to 2 always acts", current: 1, desired: 2, want: false},
		{name: "zero current never holds", current: 0, desired: 0, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := withinTolerance(tt.desired, tt.current, cfg.Scaling.TolerancePercent); got != tt.want {
				t.Errorf("withinTolerance(%d, %d, 10) = %v, want %v", tt.desired, tt.current, got, tt.want)
			}
		})
	}
}

func TestDecide_DeadbandProducesTheSteadyState(t *testing.T) {
	t.Parallel()

	s := baseSnapshot()
	s.CurrentReplicas, s.ReadyReplicas = 11, 11
	s.Backlog = fresh(600) // wants 12, inside the 10 % band around 11

	got := Decide(s, baseConfig(), now)
	if got.Reason != ReasonNoChangeWithinTolerance {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonNoChangeWithinTolerance)
	}
	// The steady state is not a hold: conflating the two makes "how often are
	// we blocked?" read as permanently saturated.
	if got.Blocked {
		t.Error("NoChangeWithinTolerance must not be reported as blocked")
	}
	if got.Direction != DirectionNone {
		t.Errorf("Direction = %q, want %q", got.Direction, DirectionNone)
	}
}

// --- UT-06: step limits ---------------------------------------------------

func TestDecide_StepLimits(t *testing.T) {
	t.Parallel()

	t.Run("scale-up step is applied after clamping", func(t *testing.T) {
		t.Parallel()

		s := baseSnapshot()
		s.CurrentReplicas, s.ReadyReplicas = 2, 2
		s.Backlog = fresh(400) // wants 8
		s.FitCapacity = 10

		got := Decide(s, baseConfig(), now)
		if got.TargetReplicas != 6 {
			t.Errorf("TargetReplicas = %d, want 6 (2 + maxScaleUpStep 4)", got.TargetReplicas)
		}
		if got.DesiredClamped != 8 {
			t.Errorf("DesiredClamped = %d, want 8 — the pre-step value must stay visible", got.DesiredClamped)
		}
		if got.RequestedDelta != 4 {
			t.Errorf("RequestedDelta = %d, want 4", got.RequestedDelta)
		}
	})

	t.Run("a corrupt backlog cannot exceed one step", func(t *testing.T) {
		t.Parallel()

		s := baseSnapshot()
		s.CurrentReplicas, s.ReadyReplicas = 2, 2
		s.Backlog = fresh(1_000_000) // wants 20000
		s.FitCapacity = 1000

		got := Decide(s, baseConfig(), now)
		if got.TargetReplicas != 6 {
			t.Errorf("TargetReplicas = %d, want 6: a corrupt reading must not jump to maxReplicas in one write", got.TargetReplicas)
		}
	})

	t.Run("scale-down step is one replica", func(t *testing.T) {
		t.Parallel()

		s := readyToScaleDown()
		s.CurrentReplicas, s.ReadyReplicas = 12, 12
		s.Backlog = fresh(0) // wants 1
		s.DesiredHistory = quietHistory(now, 1)

		got := Decide(s, baseConfig(), now)
		if got.Reason != ReasonScaleDown {
			t.Fatalf("Reason = %q, want %q", got.Reason, ReasonScaleDown)
		}
		if got.TargetReplicas != 11 {
			t.Errorf("TargetReplicas = %d, want 11 (12 − maxScaleDownStep 1)", got.TargetReplicas)
		}
		if got.RequestedDelta != -1 {
			t.Errorf("RequestedDelta = %d, want -1", got.RequestedDelta)
		}
	})

	t.Run("scale-down never crosses minReplicas", func(t *testing.T) {
		t.Parallel()

		cfg := baseConfig()
		cfg.Scaling.MaxScaleDownStep = 10

		s := readyToScaleDown()
		s.CurrentReplicas, s.ReadyReplicas = 3, 3
		s.Backlog = fresh(0)
		s.DesiredHistory = quietHistory(now, 1)

		got := Decide(s, cfg, now)
		if got.TargetReplicas != cfg.Target.MinReplicas {
			t.Errorf("TargetReplicas = %d, want %d", got.TargetReplicas, cfg.Target.MinReplicas)
		}
	})
}

// --- UT-07 / UT-26: guard precedence -------------------------------------

// The precedence is asserted by building a snapshot in which *every* guard
// fires at once and then peeling them off one at a time. Testing each guard in
// isolation would pass even if the order were wrong, and the order is the part
// that decides which of several true statements an operator is told.
func TestDecide_GuardPrecedence(t *testing.T) {
	t.Parallel()

	cfg := baseConfig()

	// Everything is wrong simultaneously.
	worst := func() Snapshot {
		s := Snapshot{
			Valid:             false,
			CurrentReplicas:   2,
			ReadyReplicas:     1,
			HPAPresent:        true,
			ExternalDrifts:    int32(cfg.Scaling.ExternalChangeTolerance) + 1,
			RolloutInProgress: true,
			Backlog:           Signal[int64]{Value: 400, SampledAt: now.Add(-10 * time.Minute), Available: true},
			FitCapacity:       0,
			PendingOurPods:    3,
			UnhealthyOurPods:  2,
			UnhealthyPodAge:   10 * time.Minute,
			LastScaleUp:       now.Add(-time.Second),
			LastScaleDown:     now.Add(-time.Second),
			HoldBackoff:       BackoffState{Attempts: 3, NextEligible: now.Add(5 * time.Minute), FitCapacityAtArm: 0},
		}
		return s
	}

	// Each step removes exactly the condition the previous reason keyed on.
	steps := []struct {
		reason Reason
		fix    func(*Snapshot)
	}{
		{ReasonErrorAPIFailure, func(s *Snapshot) { s.Valid = true }},
		{ReasonHoldScalingConflict, func(s *Snapshot) { s.HPAPresent = false }},
		{ReasonHoldExternalChange, func(s *Snapshot) { s.ExternalDrifts = 0 }},
		{ReasonHoldRolloutInProgress, func(s *Snapshot) { s.RolloutInProgress = false }},
		{ReasonHoldStaleMetrics, func(s *Snapshot) { s.Backlog = fresh(400) }},
		{ReasonHoldPendingPods, func(s *Snapshot) { s.PendingOurPods = 0 }},
		{ReasonHoldUnhealthyPods, func(s *Snapshot) { s.UnhealthyPodAge = 0; s.UnhealthyOurPods = 0 }},
		{ReasonHoldBackoff, func(s *Snapshot) { s.HoldBackoff = ClearBackoff() }},
		{ReasonHoldCooldown, func(s *Snapshot) { s.LastScaleUp = time.Time{} }},
		{ReasonHoldInsufficientResources, func(s *Snapshot) { s.FitCapacity = 10 }},
		{ReasonScaleUp, nil},
	}

	s := worst()
	for i, step := range steps {
		got := Decide(s, cfg, now)
		if got.Reason != step.reason {
			t.Fatalf("step %d: Reason = %q, want %q\nsnapshot: %+v", i, got.Reason, step.reason, s)
		}
		if step.fix != nil {
			step.fix(&s)
		}
	}
}

// The same assertion for the scale-down path, whose guards are only reachable
// once demand points down.
func TestDecide_ScaleDownGuardPrecedence(t *testing.T) {
	t.Parallel()

	cfg := baseConfig()

	s := Snapshot{
		Valid:           true,
		CurrentReplicas: 8,
		ReadyReplicas:   6, // not settled
		Backlog:         fresh(0),
		LastScaleDown:   now.Add(-time.Second), // inside the cooldown
		DesiredHistory:  nil,                   // uncovered window
		HoldBackoff:     ClearBackoff(),
	}

	steps := []struct {
		reason Reason
		fix    func(*Snapshot)
	}{
		{ReasonHoldReplicasSettling, func(s *Snapshot) { s.ReadyReplicas = 8 }},
		{ReasonHoldCooldown, func(s *Snapshot) { s.LastScaleDown = time.Time{} }},
		{ReasonHoldStabilizationWindow, func(s *Snapshot) { s.DesiredHistory = quietHistory(now, 1) }},
		{ReasonScaleDown, nil},
	}

	for i, step := range steps {
		got := Decide(s, cfg, now)
		if got.Reason != step.reason {
			t.Fatalf("step %d: Reason = %q, want %q", i, got.Reason, step.reason)
		}
		if step.fix != nil {
			step.fix(&s)
		}
	}
}

// The DR-03 case, stated as a regression test because the original design got
// it wrong: six replicas, two Ready, CPU pinned at 100 %, backlog low. The
// utilization average over Ready pods alone says "scale down", and doing so
// would remove pods added seconds earlier.
func TestDecide_DoesNotScaleDownWhileReplicasAreSettling(t *testing.T) {
	t.Parallel()

	s := readyToScaleDown()
	s.CurrentReplicas, s.ReadyReplicas = 6, 2
	s.Backlog = fresh(50) // wants 1
	s.AvgCPUMilliPercent = freshMilliPercent(100_000)
	s.DesiredHistory = quietHistory(now, 1)

	got := Decide(s, baseConfig(), now)
	if got.Reason != ReasonHoldReplicasSettling {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonHoldReplicasSettling)
	}
	if got.TargetReplicas != 6 {
		t.Errorf("TargetReplicas = %d, want 6", got.TargetReplicas)
	}
}

// --- UT-08: the feasibility gate -----------------------------------------

func TestDecide_FeasibilityGate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fitCapacity  int32
		allowPartial bool
		wantReason   Reason
		wantTarget   int32
		wantDeficit  int32
		wantBackoff  BackoffAction
	}{
		{
			name: "capacity covers the request", fitCapacity: 4, allowPartial: true,
			wantReason: ReasonScaleUp, wantTarget: 6, wantBackoff: BackoffReset,
		},
		{
			name: "more capacity than needed", fitCapacity: 99, allowPartial: true,
			wantReason: ReasonScaleUp, wantTarget: 6, wantBackoff: BackoffReset,
		},
		{
			name: "partial progress beats none", fitCapacity: 2, allowPartial: true,
			wantReason: ReasonScaleUpPartial, wantTarget: 4, wantDeficit: 2, wantBackoff: BackoffArm,
		},
		{
			name: "one pod still counts", fitCapacity: 1, allowPartial: true,
			wantReason: ReasonScaleUpPartial, wantTarget: 3, wantDeficit: 3, wantBackoff: BackoffArm,
		},
		{
			name: "nothing fits", fitCapacity: 0, allowPartial: true,
			wantReason: ReasonHoldInsufficientResources, wantTarget: 2, wantDeficit: 4, wantBackoff: BackoffArm,
		},
		{
			// With partial disabled the same capacity produces a hold: the
			// operator has chosen all-or-nothing.
			name: "partial disabled routes to a hold", fitCapacity: 2, allowPartial: false,
			wantReason: ReasonHoldInsufficientResources, wantTarget: 2, wantDeficit: 4, wantBackoff: BackoffArm,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := baseConfig()
			cfg.Scaling.AllowPartialScaleUp = tt.allowPartial

			s := baseSnapshot()
			s.CurrentReplicas, s.ReadyReplicas = 2, 2
			s.Backlog = fresh(400) // wants 8, step-limited to 6, so delta 4
			s.FitCapacity = tt.fitCapacity
			s.Blocking = resources.DimensionCPU

			got := Decide(s, cfg, now)

			if got.Reason != tt.wantReason {
				t.Errorf("Reason = %q, want %q", got.Reason, tt.wantReason)
			}
			if got.TargetReplicas != tt.wantTarget {
				t.Errorf("TargetReplicas = %d, want %d", got.TargetReplicas, tt.wantTarget)
			}
			if got.Deficit != tt.wantDeficit {
				t.Errorf("Deficit = %d, want %d", got.Deficit, tt.wantDeficit)
			}
			if got.BackoffAction != tt.wantBackoff {
				t.Errorf("BackoffAction = %q, want %q", got.BackoffAction, tt.wantBackoff)
			}
			// The binding dimension travels with the decision: it is what
			// turns "insufficient resources" into "add CPU".
			if got.Blocking != resources.DimensionCPU {
				t.Errorf("Blocking = %q, want %q", got.Blocking, resources.DimensionCPU)
			}
		})
	}
}

// --- UT-09 / UT-25: backoff ----------------------------------------------

func TestArmBackoff_GrowthAndCap(t *testing.T) {
	t.Parallel()

	cfg := config.Default().Scaling.HoldBackoff // 30s, 15m, factor 2

	want := []time.Duration{
		30 * time.Second,
		60 * time.Second,
		2 * time.Minute,
		4 * time.Minute,
		8 * time.Minute,
		15 * time.Minute, // 16m would exceed the cap
		15 * time.Minute,
		15 * time.Minute,
	}

	state := ClearBackoff()
	for i, wantInterval := range want {
		var interval time.Duration
		state, interval = armBackoff(state, 0, cfg, now)

		if interval != wantInterval {
			t.Errorf("attempt %d: interval = %v, want %v", i+1, interval, wantInterval)
		}
		if state.Attempts != i+1 {
			t.Errorf("attempt %d: Attempts = %d, want %d", i+1, state.Attempts, i+1)
		}
		if got, want := state.NextEligible, now.Add(wantInterval); !got.Equal(want) {
			t.Errorf("attempt %d: NextEligible = %v, want %v", i+1, got, want)
		}
	}
}

// A factor of 1 or less is rejected by config validation. The loop guard is the
// belt to that braces: without it the growth loop would spin without growing.
func TestArmBackoff_NonGrowingFactorTerminates(t *testing.T) {
	t.Parallel()

	cfg := config.HoldBackoffConfig{
		Initial: config.Duration(30 * time.Second),
		Max:     config.Duration(15 * time.Minute),
		Factor:  1.0,
	}

	state := BackoffState{Attempts: 50}
	_, interval := armBackoff(state, 0, cfg, now)
	if interval != 30*time.Second {
		t.Errorf("interval = %v, want the initial 30s when the factor cannot grow", interval)
	}

	invalid := config.Default()
	invalid.Target.Namespace, invalid.Target.Deployment = "ns", "d"
	invalid.Workload.Signal.Source = config.SignalSourceNone
	invalid.Scaling.HoldBackoff.Factor = 1.0
	if err := invalid.Validate(); err == nil {
		t.Error("a non-growing holdBackoff.factor must be rejected at startup")
	}
}

// ClearBackoff uses -1 rather than 0 for the capacity at arm, so that "cleared"
// is distinguishable from "armed when the cluster genuinely had no room". With
// 0 the next reconcile would see F=0 <= 0 and believe a backoff was active.
func TestClearBackoff_IsDistinguishableFromArmedAtZeroCapacity(t *testing.T) {
	t.Parallel()

	cleared := ClearBackoff()
	if cleared.Armed() {
		t.Error("a cleared backoff must not report as armed")
	}
	if cleared.FitCapacityAtArm != -1 {
		t.Errorf("FitCapacityAtArm = %d, want -1", cleared.FitCapacityAtArm)
	}

	// Proof that it matters: a cleared backoff must not suppress a scale-up on
	// a full cluster.
	s := baseSnapshot()
	s.CurrentReplicas, s.ReadyReplicas = 2, 2
	s.Backlog = fresh(400)
	s.FitCapacity = 0
	s.HoldBackoff = cleared

	if got := Decide(s, baseConfig(), now); got.Reason != ReasonHoldInsufficientResources {
		t.Errorf("Reason = %q, want %q — a cleared backoff must not look armed", got.Reason, ReasonHoldInsufficientResources)
	}
}

// UT-25: the backoff is level-triggered on capacity, not merely time-based.
// This is the DR-01 correction: gating on time alone made the documented
// "recovers within one interval" claim unreachable.
func TestDecide_BackoffResetsWhenCapacityRecovers(t *testing.T) {
	t.Parallel()

	armed := BackoffState{
		Attempts:         2,
		NextEligible:     now.Add(10 * time.Minute), // still well inside the interval
		FitCapacityAtArm: 1,
	}

	tests := []struct {
		name        string
		fitCapacity int32
		wantReason  Reason
		wantTarget  int32
	}{
		{
			name: "capacity unchanged keeps the hold", fitCapacity: 1,
			wantReason: ReasonHoldBackoff, wantTarget: 2,
		},
		{
			name: "capacity fallen keeps the hold", fitCapacity: 0,
			wantReason: ReasonHoldBackoff, wantTarget: 2,
		},
		{
			// A node came back: the statement the backoff was making is no
			// longer true, so it is abandoned in the same reconcile rather
			// than waiting out an interval that is now meaningless.
			name: "capacity recovered scales up immediately", fitCapacity: 4,
			wantReason: ReasonScaleUp, wantTarget: 6,
		},
		{
			name: "capacity partially recovered makes partial progress", fitCapacity: 2,
			wantReason: ReasonScaleUpPartial, wantTarget: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := baseSnapshot()
			s.CurrentReplicas, s.ReadyReplicas = 2, 2
			s.Backlog = fresh(400)
			s.FitCapacity = tt.fitCapacity
			s.HoldBackoff = armed

			got := Decide(s, baseConfig(), now)
			if got.Reason != tt.wantReason {
				t.Errorf("Reason = %q, want %q", got.Reason, tt.wantReason)
			}
			if got.TargetReplicas != tt.wantTarget {
				t.Errorf("TargetReplicas = %d, want %d", got.TargetReplicas, tt.wantTarget)
			}
		})
	}
}

// DR-02: a partial scale-up is evidence of a shortfall, not of recovery. Having
// it reset the backoff would clear it on the same reconcile that armed it and
// restore the retry storm the backoff exists to prevent.
func TestDecide_PartialScaleUpArmsRatherThanResets(t *testing.T) {
	t.Parallel()

	s := baseSnapshot()
	s.CurrentReplicas, s.ReadyReplicas = 2, 2
	s.Backlog = fresh(400)
	s.FitCapacity = 2
	s.HoldBackoff = BackoffState{Attempts: 1, NextEligible: now.Add(-time.Second), FitCapacityAtArm: 2}

	got := Decide(s, baseConfig(), now)
	if got.Reason != ReasonScaleUpPartial {
		t.Fatalf("Reason = %q, want %q", got.Reason, ReasonScaleUpPartial)
	}
	if got.BackoffAction != BackoffArm {
		t.Errorf("BackoffAction = %q, want %q", got.BackoffAction, BackoffArm)
	}
	if got.Backoff.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2 — a partial scale-up advances the backoff", got.Backoff.Attempts)
	}
	// Re-armed at the capacity actually used, so a later increase above it
	// still counts as recovery.
	if got.Backoff.FitCapacityAtArm != 2 {
		t.Errorf("FitCapacityAtArm = %d, want 2", got.Backoff.FitCapacityAtArm)
	}
}

// A successful scale-up clears the backoff outright.
func TestDecide_SuccessfulScaleUpResetsTheBackoff(t *testing.T) {
	t.Parallel()

	s := baseSnapshot()
	s.CurrentReplicas, s.ReadyReplicas = 2, 2
	s.Backlog = fresh(400)
	s.FitCapacity = 10
	s.HoldBackoff = BackoffState{Attempts: 5, NextEligible: now.Add(-time.Second), FitCapacityAtArm: 1}

	got := Decide(s, baseConfig(), now)
	if got.Reason != ReasonScaleUp {
		t.Fatalf("Reason = %q, want %q", got.Reason, ReasonScaleUp)
	}
	if got.BackoffAction != BackoffReset || got.Backoff.Armed() {
		t.Errorf("BackoffAction = %q, Backoff = %+v; want a reset to the cleared state", got.BackoffAction, got.Backoff)
	}
}

// Demand falling below the current count means the shortfall the backoff was a
// statement about has gone away, so it is cleared even though nothing scaled.
func TestDecide_BackoffResetsWhenDemandFalls(t *testing.T) {
	t.Parallel()

	s := baseSnapshot()
	s.CurrentReplicas, s.ReadyReplicas = 6, 6
	s.Backlog = fresh(100) // wants 2, well below current
	s.FitCapacity = 0
	s.HoldBackoff = BackoffState{Attempts: 4, NextEligible: now.Add(10 * time.Minute), FitCapacityAtArm: 0}
	s.DesiredHistory = nil // window uncovered, so the scale-down itself holds

	got := Decide(s, baseConfig(), now)
	if got.BackoffAction != BackoffReset {
		t.Errorf("BackoffAction = %q, want %q", got.BackoffAction, BackoffReset)
	}
	if got.Backoff.Armed() {
		t.Errorf("Backoff = %+v, want cleared", got.Backoff)
	}
}

// Sitting at maxReplicas under genuinely high demand must not look like demand
// having fallen, which is why the reset compares against the *unclamped*
// desired value.
func TestDecide_BackoffIsNotResetByTheClampAtMaxReplicas(t *testing.T) {
	t.Parallel()

	s := baseSnapshot()
	s.CurrentReplicas, s.ReadyReplicas = 12, 12
	s.Backlog = fresh(2000) // wants 40; clamped to 12, which equals current
	s.FitCapacity = 0
	s.HoldBackoff = BackoffState{Attempts: 3, NextEligible: now.Add(10 * time.Minute), FitCapacityAtArm: 0}

	got := Decide(s, baseConfig(), now)
	if got.BackoffAction != BackoffUnchanged {
		t.Errorf("BackoffAction = %q, want %q: demand is still 40, not 12", got.BackoffAction, BackoffUnchanged)
	}
	if got.Backoff.Attempts != 3 {
		t.Errorf("Attempts = %d, want the armed value 3 to be preserved", got.Backoff.Attempts)
	}
}

// Mere elapsed time is not recovery. Once the interval expires the engine
// retries, and if capacity is still absent the backoff grows rather than
// restarting at the initial interval.
func TestDecide_ElapsedTimeAloneDoesNotResetTheBackoff(t *testing.T) {
	t.Parallel()

	s := baseSnapshot()
	s.CurrentReplicas, s.ReadyReplicas = 2, 2
	s.Backlog = fresh(400)
	s.FitCapacity = 0
	s.HoldBackoff = BackoffState{Attempts: 3, NextEligible: now.Add(-time.Hour), FitCapacityAtArm: 0}

	got := Decide(s, baseConfig(), now)
	if got.Reason != ReasonHoldInsufficientResources {
		t.Fatalf("Reason = %q, want %q: the interval expired, so the engine retries", got.Reason, ReasonHoldInsufficientResources)
	}
	if got.Backoff.Attempts != 4 {
		t.Errorf("Attempts = %d, want 4: the counter continues rather than restarting", got.Backoff.Attempts)
	}
	if got.BackoffInterval != 4*time.Minute {
		t.Errorf("BackoffInterval = %v, want 4m (30s doubled three times)", got.BackoffInterval)
	}
}

// Guards that return before demand is known must leave the backoff alone: a
// reconcile that could not read the cluster says nothing about capacity.
func TestDecide_EarlyGuardsLeaveTheBackoffUntouched(t *testing.T) {
	t.Parallel()

	armed := BackoffState{Attempts: 2, NextEligible: now.Add(time.Minute), FitCapacityAtArm: 1}

	for _, tc := range []struct {
		name   string
		mutate func(*Snapshot)
	}{
		{name: "invalid snapshot", mutate: func(s *Snapshot) { s.Valid = false }},
		{name: "hpa present", mutate: func(s *Snapshot) { s.HPAPresent = true }},
		{name: "rollout", mutate: func(s *Snapshot) { s.RolloutInProgress = true }},
		{name: "stale backlog", mutate: func(s *Snapshot) { s.Backlog = Signal[int64]{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := baseSnapshot()
			s.HoldBackoff = armed
			tc.mutate(&s)

			got := Decide(s, baseConfig(), now)
			if got.BackoffAction != BackoffUnchanged {
				t.Errorf("BackoffAction = %q, want %q", got.BackoffAction, BackoffUnchanged)
			}
			if got.Backoff != armed {
				t.Errorf("Backoff = %+v, want it preserved as %+v", got.Backoff, armed)
			}
			if got.DemandComputed {
				t.Error("DemandComputed must be false when the guard fired before demand was known")
			}
		})
	}
}

// --- UT-10: the stabilization window -------------------------------------

func TestSnapshot_MaxDesiredInWindow(t *testing.T) {
	t.Parallel()

	window := 5 * time.Minute

	s := Snapshot{DesiredHistory: []DesiredSample{
		{At: now.Add(-10 * time.Minute), Desired: 99}, // outside
		{At: now.Add(-4 * time.Minute), Desired: 3},
		{At: now.Add(-2 * time.Minute), Desired: 7},
		{At: now.Add(-time.Minute), Desired: 2},
	}}

	got, found := s.MaxDesiredInWindow(window, now)
	if !found {
		t.Fatal("samples exist inside the window")
	}
	if got != 7 {
		t.Errorf("max = %d, want 7 (the 99 is outside the window)", got)
	}

	if _, found := (Snapshot{}).MaxDesiredInWindow(window, now); found {
		t.Error("an empty history must report no sample found, not a max of zero")
	}
}

// A single busy sample anywhere in the window pins the replica count: the
// workload must be sustainably quiet, not momentarily quiet.
func TestDecide_OneBusySampleBlocksScaleDownUntilItExpires(t *testing.T) {
	t.Parallel()

	cfg := baseConfig() // window 300s

	// A quiet history with one spike four minutes ago.
	history := quietHistory(now, 1)
	history = append(history, DesiredSample{At: now.Add(-4 * time.Minute), Desired: 12})

	s := readyToScaleDown()
	s.CurrentReplicas, s.ReadyReplicas = 8, 8
	s.Backlog = fresh(0)
	s.DesiredHistory = history

	if got := Decide(s, cfg, now); got.Reason != ReasonHoldStabilizationWindow {
		t.Errorf("Reason = %q, want %q while the spike is inside the window", got.Reason, ReasonHoldStabilizationWindow)
	}

	// Two minutes later the spike has aged out, and the same history permits
	// the scale-down. The backlog is re-sampled because a signal from two
	// minutes ago would itself be stale.
	later := now.Add(2 * time.Minute)
	s.Backlog = Signal[int64]{SampledAt: later, Available: true}
	s.DesiredHistory = append(quietHistory(later, 1), DesiredSample{At: now.Add(-4 * time.Minute), Desired: 12})

	if got := Decide(s, cfg, later); got.Reason != ReasonScaleDown {
		t.Errorf("Reason = %q, want %q once the spike has aged out", got.Reason, ReasonScaleDown)
	}
}

// DR-04 / FS-27: without a coverage check, a metrics outage leaves a nearly
// empty window whose max() is trivially low, and the controller scales down on
// the strength of missing data.
func TestSnapshot_WindowCovered(t *testing.T) {
	t.Parallel()

	const (
		window   = 5 * time.Minute
		interval = 15 * time.Second
		coverage = 0.8
	)
	// expected = ceil(300/15) = 20 samples; 80 % of that is 16.

	tests := []struct {
		name    string
		history []DesiredSample
		want    bool
	}{
		{name: "empty history", history: nil, want: false},
		{name: "fully covered", history: samplesEvery(now, interval, 21, 1), want: true},
		{name: "exactly at the coverage threshold", history: samplesEvery(now, interval, 16, 1), want: false},
		{
			// 3 of 20 samples: the case FS-27 describes, where the max() of
			// what little survived the outage is trivially low.
			name: "sparse window after an outage", history: samplesEvery(now, interval, 3, 1), want: false,
		},
		{
			// Enough samples, but they only span a minute: a freshly started
			// controller must wait rather than scale down on its first quiet
			// minute.
			name:    "enough samples spanning too short a period",
			history: samplesEvery(now, time.Second, 20, 1),
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := Snapshot{DesiredHistory: tt.history}
			if got := s.WindowCovered(window, interval, coverage, now); got != tt.want {
				t.Errorf("WindowCovered = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSnapshot_WindowCovered_DegenerateConfiguration(t *testing.T) {
	t.Parallel()

	s := Snapshot{DesiredHistory: samplesEvery(now, 15*time.Second, 40, 1)}

	if s.WindowCovered(0, 15*time.Second, 0.8, now) {
		t.Error("a zero window must not be reported as covered")
	}
	if s.WindowCovered(5*time.Minute, 0, 0.8, now) {
		t.Error("a zero interval must not be reported as covered")
	}
}

// --- UT-11: cooldowns -----------------------------------------------------

func TestDecide_Cooldowns(t *testing.T) {
	t.Parallel()

	cfg := baseConfig() // up 60s, down 300s

	t.Run("scale-up cooldown boundary", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			age  time.Duration
			want Reason
		}{
			{age: 0, want: ReasonHoldCooldown},
			{age: 59 * time.Second, want: ReasonHoldCooldown},
			{age: 60 * time.Second, want: ReasonScaleUp}, // exactly at the cooldown acts
			{age: 90 * time.Second, want: ReasonScaleUp},
		} {
			s := baseSnapshot()
			s.CurrentReplicas, s.ReadyReplicas = 2, 2
			s.Backlog = fresh(400)
			s.FitCapacity = 10
			s.LastScaleUp = now.Add(-tc.age)

			if got := Decide(s, cfg, now); got.Reason != tc.want {
				t.Errorf("age %v: Reason = %q, want %q", tc.age, got.Reason, tc.want)
			}
		}
	})

	t.Run("scale-down cooldown boundary", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			age  time.Duration
			want Reason
		}{
			{age: 0, want: ReasonHoldCooldown},
			{age: 299 * time.Second, want: ReasonHoldCooldown},
			{age: 300 * time.Second, want: ReasonScaleDown},
		} {
			s := readyToScaleDown()
			s.CurrentReplicas, s.ReadyReplicas = 8, 8
			s.Backlog = fresh(0)
			s.DesiredHistory = quietHistory(now, 1)
			s.LastScaleDown = now.Add(-tc.age)

			if got := Decide(s, cfg, now); got.Reason != tc.want {
				t.Errorf("age %v: Reason = %q, want %q", tc.age, got.Reason, tc.want)
			}
		}
	})

	// The asymmetry is the point: reacting quickly to load is cheap and
	// reversible, retreating quickly is expensive and disruptive.
	t.Run("cooldowns are asymmetric", func(t *testing.T) {
		t.Parallel()

		age := 90 * time.Second // past the up cooldown, inside the down one

		up := baseSnapshot()
		up.CurrentReplicas, up.ReadyReplicas = 2, 2
		up.Backlog = fresh(400)
		up.FitCapacity = 10
		up.LastScaleUp = now.Add(-age)
		if got := Decide(up, cfg, now); got.Reason != ReasonScaleUp {
			t.Errorf("up: Reason = %q, want %q after 90s", got.Reason, ReasonScaleUp)
		}

		down := readyToScaleDown()
		down.CurrentReplicas, down.ReadyReplicas = 8, 8
		down.Backlog = fresh(0)
		down.DesiredHistory = quietHistory(now, 1)
		down.LastScaleDown = now.Add(-age)
		if got := Decide(down, cfg, now); got.Reason != ReasonHoldCooldown {
			t.Errorf("down: Reason = %q, want %q after 90s", got.Reason, ReasonHoldCooldown)
		}
	})

	// A scale-down cannot follow hard on a scale-up, but the mechanism is G9
	// and G11 rather than C_down: the new pods are not yet Ready, and once they
	// are, the high desired sample that triggered the scale-up is still inside
	// the stabilization window.
	t.Run("a scale-up is not immediately undone", func(t *testing.T) {
		t.Parallel()

		s := readyToScaleDown()
		s.CurrentReplicas, s.ReadyReplicas = 6, 6
		s.Backlog = fresh(0)
		s.LastScaleUp = now.Add(-30 * time.Second)
		// The spike that caused the scale-up 30 s ago is still in the window.
		s.DesiredHistory = append(quietHistory(now, 1), DesiredSample{At: now.Add(-30 * time.Second), Desired: 6})

		if got := Decide(s, cfg, now); got.Reason != ReasonHoldStabilizationWindow {
			t.Errorf("Reason = %q, want %q", got.Reason, ReasonHoldStabilizationWindow)
		}
	})
}

// --- UT-12: stale and missing signals ------------------------------------

func TestDecide_StaleBacklogFreezesBothDirections(t *testing.T) {
	t.Parallel()

	cfg := baseConfig() // metricsStaleAfter 60s

	backlogs := map[string]Signal[int64]{
		"missing":     {},
		"unavailable": {Value: 400, SampledAt: now, Available: false},
		"no sample time": {
			Value: 400, Available: true,
		},
		"stale": {Value: 400, SampledAt: now.Add(-61 * time.Second), Available: true},
	}

	for name, backlog := range backlogs {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Would otherwise scale up.
			up := baseSnapshot()
			up.CurrentReplicas, up.ReadyReplicas = 2, 2
			up.FitCapacity = 10
			up.Backlog = backlog
			if got := Decide(up, cfg, now); got.Reason != ReasonHoldStaleMetrics {
				t.Errorf("up: Reason = %q, want %q", got.Reason, ReasonHoldStaleMetrics)
			}

			// Would otherwise scale down. This is the dangerous direction:
			// treating a missing backlog as zero would drain the pool during a
			// monitoring outage.
			down := readyToScaleDown()
			down.CurrentReplicas, down.ReadyReplicas = 8, 8
			down.DesiredHistory = quietHistory(now, 1)
			down.Backlog = backlog
			got := Decide(down, cfg, now)
			if got.Reason != ReasonHoldStaleMetrics {
				t.Errorf("down: Reason = %q, want %q", got.Reason, ReasonHoldStaleMetrics)
			}
			if got.TargetReplicas != 8 {
				t.Errorf("down: TargetReplicas = %d, want 8 — unknown demand must never remove replicas", got.TargetReplicas)
			}
		})
	}
}

func TestDecide_StalenessBoundary(t *testing.T) {
	t.Parallel()

	cfg := baseConfig()

	for _, tc := range []struct {
		age  time.Duration
		want Reason
	}{
		{age: 60 * time.Second, want: ReasonScaleUp},          // exactly at the limit is fresh
		{age: 61 * time.Second, want: ReasonHoldStaleMetrics}, // one second past is not
	} {
		s := baseSnapshot()
		s.CurrentReplicas, s.ReadyReplicas = 2, 2
		s.FitCapacity = 10
		s.Backlog = Signal[int64]{Value: 400, SampledAt: now.Add(-tc.age), Available: true}

		if got := Decide(s, cfg, now); got.Reason != tc.want {
			t.Errorf("age %v: Reason = %q, want %q", tc.age, got.Reason, tc.want)
		}
	}
}

// Small clock skew between the controller and a metrics source is normal.
// Flapping into HoldStaleMetrics because of it would be worse than tolerating
// it, so a future timestamp is treated as age zero.
func TestSignal_FutureTimestampIsFresh(t *testing.T) {
	t.Parallel()

	s := Signal[int64]{Value: 1, SampledAt: now.Add(5 * time.Second), Available: true}

	if !s.Fresh(time.Second, now) {
		t.Error("a slightly future sample must be treated as fresh, not stale")
	}
	if got := s.Age(now); got != 0 {
		t.Errorf("Age = %v, want 0", got)
	}
}

func TestSignal_AgeOfAnAbsentSignal(t *testing.T) {
	t.Parallel()

	if got := (Signal[int64]{}).Age(now); got != 0 {
		t.Errorf("Age = %v, want 0 for an absent signal", got)
	}
	if got := (Signal[int64]{Value: 5, Available: true}).Age(now); got != 0 {
		t.Errorf("Age = %v, want 0 when SampledAt was never set", got)
	}
}

// Missing utilization degrades the decision rather than freezing it: the
// backlog alone is enough to scale on.
func TestDecide_MissingUtilizationStillScales(t *testing.T) {
	t.Parallel()

	s := baseSnapshot()
	s.CurrentReplicas, s.ReadyReplicas = 2, 2
	s.Backlog = fresh(400)
	s.AvgCPUMilliPercent = Signal[int64]{} // metrics-server unavailable
	s.FitCapacity = 10

	got := Decide(s, baseConfig(), now)
	if got.Reason != ReasonScaleUp {
		t.Errorf("Reason = %q, want %q: utilization is a safety net, not a prerequisite", got.Reason, ReasonScaleUp)
	}
	if got.DesiredUtilization != 0 {
		t.Errorf("DesiredUtilization = %d, want 0", got.DesiredUtilization)
	}
}

// --- Reason codes ---------------------------------------------------------

// Every reason must be reachable from some snapshot. An unreachable code is a
// permanently empty metric label and a lie in the documentation.
func TestReasons_AreAllReachable(t *testing.T) {
	t.Parallel()

	cfg := baseConfig()
	noPartial := baseConfig()
	noPartial.Scaling.AllowPartialScaleUp = false

	cases := map[Reason]func() Decision{
		ReasonErrorAPIFailure: func() Decision {
			s := baseSnapshot()
			s.Valid = false
			return Decide(s, cfg, now)
		},
		ReasonHoldScalingConflict: func() Decision {
			s := baseSnapshot()
			s.HPAPresent = true
			return Decide(s, cfg, now)
		},
		ReasonHoldExternalChange: func() Decision {
			s := baseSnapshot()
			s.ExternalDrifts = 4
			return Decide(s, cfg, now)
		},
		ReasonHoldRolloutInProgress: func() Decision {
			s := baseSnapshot()
			s.RolloutInProgress = true
			return Decide(s, cfg, now)
		},
		ReasonHoldStaleMetrics: func() Decision {
			s := baseSnapshot()
			s.Backlog = Signal[int64]{}
			return Decide(s, cfg, now)
		},
		ReasonHoldAtMaxReplicas: func() Decision {
			s := baseSnapshot()
			s.CurrentReplicas, s.ReadyReplicas = 12, 12
			s.Backlog = fresh(2000)
			return Decide(s, cfg, now)
		},
		ReasonHoldAtMinReplicas: func() Decision {
			s := baseSnapshot()
			s.CurrentReplicas, s.ReadyReplicas = 1, 1
			s.Backlog = fresh(0)
			return Decide(s, cfg, now)
		},
		ReasonNoChangeWithinTolerance: func() Decision {
			s := baseSnapshot()
			s.CurrentReplicas, s.ReadyReplicas = 11, 11
			s.Backlog = fresh(600)
			return Decide(s, cfg, now)
		},
		ReasonHoldPendingPods: func() Decision {
			s := scaleUpWanted()
			s.PendingOurPods = 1
			return Decide(s, cfg, now)
		},
		ReasonHoldUnhealthyPods: func() Decision {
			s := scaleUpWanted()
			s.UnhealthyOurPods = 1
			s.UnhealthyPodAge = 10 * time.Minute
			return Decide(s, cfg, now)
		},
		ReasonHoldBackoff: func() Decision {
			s := scaleUpWanted()
			s.FitCapacity = 0
			s.HoldBackoff = BackoffState{Attempts: 1, NextEligible: now.Add(time.Minute), FitCapacityAtArm: 0}
			return Decide(s, cfg, now)
		},
		ReasonHoldCooldown: func() Decision {
			s := scaleUpWanted()
			s.LastScaleUp = now
			return Decide(s, cfg, now)
		},
		ReasonScaleUp: func() Decision {
			return Decide(scaleUpWanted(), cfg, now)
		},
		ReasonScaleUpPartial: func() Decision {
			s := scaleUpWanted()
			s.FitCapacity = 1
			return Decide(s, cfg, now)
		},
		ReasonHoldInsufficientResources: func() Decision {
			s := scaleUpWanted()
			s.FitCapacity = 0
			return Decide(s, cfg, now)
		},
		ReasonHoldReplicasSettling: func() Decision {
			s := readyToScaleDown()
			s.CurrentReplicas, s.ReadyReplicas = 8, 6
			s.Backlog = fresh(0)
			return Decide(s, cfg, now)
		},
		ReasonHoldStabilizationWindow: func() Decision {
			s := readyToScaleDown()
			s.CurrentReplicas, s.ReadyReplicas = 8, 8
			s.Backlog = fresh(0)
			s.DesiredHistory = nil
			return Decide(s, cfg, now)
		},
		ReasonScaleDown: func() Decision {
			s := readyToScaleDown()
			s.CurrentReplicas, s.ReadyReplicas = 8, 8
			s.Backlog = fresh(0)
			s.DesiredHistory = quietHistory(now, 1)
			return Decide(s, cfg, now)
		},
	}

	for _, reason := range Reasons {
		build, ok := cases[reason]
		if !ok {
			t.Errorf("no snapshot produces %q; an unreachable reason code is a permanently empty metric label", reason)
			continue
		}
		if got := build(); got.Reason != reason {
			t.Errorf("case for %q produced %q instead", reason, got.Reason)
		}
	}

	if len(cases) != len(Reasons) {
		t.Errorf("%d cases for %d reason codes", len(cases), len(Reasons))
	}
}

func TestReason_ActionAndIsHold(t *testing.T) {
	t.Parallel()

	writes := map[Reason]bool{ReasonScaleUp: true, ReasonScaleUpPartial: true, ReasonScaleDown: true}

	for _, r := range Reasons {
		wantAction := ActionNone
		if writes[r] {
			wantAction = ActionWrite
		}
		if got := r.Action(); got != wantAction {
			t.Errorf("%s.Action() = %q, want %q", r, got, wantAction)
		}

		// NoChangeWithinTolerance is not a hold: nothing was blocked because
		// nothing was wanted.
		wantHold := !writes[r] && r != ReasonNoChangeWithinTolerance
		if got := r.IsHold(); got != wantHold {
			t.Errorf("%s.IsHold() = %v, want %v", r, got, wantHold)
		}
	}
}

// --- UT-22: properties ----------------------------------------------------

// The highest-value test in the suite. "When nothing fits, replicas never
// increase" is the project's central safety claim, and a property test asserts
// it across thousands of generated states rather than the handful an author
// thought to enumerate.
func TestDecide_Properties(t *testing.T) {
	t.Parallel()

	cfg := baseConfig()

	properties := map[string]func(fuzzSnapshot) bool{
		"a hold never changes the replica count": func(f fuzzSnapshot) bool {
			d := Decide(f.Snapshot, cfg, now)
			if d.Action == ActionWrite {
				return true
			}
			return d.TargetReplicas == f.CurrentReplicas
		},

		"a write is always inside the configured clamp": func(f fuzzSnapshot) bool {
			d := Decide(f.Snapshot, cfg, now)
			if d.Action != ActionWrite {
				return true
			}
			return d.TargetReplicas >= cfg.Target.MinReplicas && d.TargetReplicas <= cfg.Target.MaxReplicas
		},

		"zero fit capacity never increases replicas": func(f fuzzSnapshot) bool {
			f.FitCapacity = 0
			d := Decide(f.Snapshot, cfg, now)
			return d.TargetReplicas <= f.CurrentReplicas
		},

		"a scale-up never exceeds the fit capacity": func(f fuzzSnapshot) bool {
			d := Decide(f.Snapshot, cfg, now)
			if d.TargetReplicas <= f.CurrentReplicas {
				return true
			}
			return d.TargetReplicas-f.CurrentReplicas <= f.FitCapacity
		},

		"a scale-up never exceeds maxScaleUpStep": func(f fuzzSnapshot) bool {
			d := Decide(f.Snapshot, cfg, now)
			return d.TargetReplicas-f.CurrentReplicas <= cfg.Scaling.MaxScaleUpStep
		},

		"a scale-down never exceeds maxScaleDownStep": func(f fuzzSnapshot) bool {
			d := Decide(f.Snapshot, cfg, now)
			return f.CurrentReplicas-d.TargetReplicas <= cfg.Scaling.MaxScaleDownStep
		},

		"an unavailable backlog freezes the replica count": func(f fuzzSnapshot) bool {
			// The guards above G2 are cleared, because otherwise one of them
			// legitimately claims the reason and the property would be
			// asserting guard precedence instead of staleness.
			f.Valid, f.HPAPresent, f.RolloutInProgress, f.ExternalDrifts = true, false, false, 0
			f.Backlog.Available = false

			d := Decide(f.Snapshot, cfg, now)
			return d.TargetReplicas == f.CurrentReplicas && d.Reason == ReasonHoldStaleMetrics
		},

		"an invalid snapshot freezes the replica count": func(f fuzzSnapshot) bool {
			f.Valid = false
			d := Decide(f.Snapshot, cfg, now)
			return d.TargetReplicas == f.CurrentReplicas && d.Reason == ReasonErrorAPIFailure
		},

		"exactly one reason code, drawn from the canonical list": func(f fuzzSnapshot) bool {
			d := Decide(f.Snapshot, cfg, now)
			for _, r := range Reasons {
				if d.Reason == r {
					return true
				}
			}
			return false
		},

		"the same snapshot always yields the same decision": func(f fuzzSnapshot) bool {
			first := Decide(f.Snapshot, cfg, now)
			second := Decide(f.Snapshot, cfg, now)
			return first == second
		},

		"the reported direction agrees with the movement": func(f fuzzSnapshot) bool {
			d := Decide(f.Snapshot, cfg, now)
			switch {
			case d.TargetReplicas > f.CurrentReplicas:
				return d.Direction == DirectionUp
			case d.TargetReplicas < f.CurrentReplicas:
				return d.Direction == DirectionDown
			default:
				return true // a hold keeps the direction demand pointed in
			}
		},
	}

	for name, property := range properties {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Seeded, so a failure is reproducible.
			if err := quick.Check(property, &quick.Config{
				MaxCount: 2000,
				Rand:     rand.New(rand.NewSource(0x5ca1ab1e)),
			}); err != nil {
				t.Errorf("property does not hold: %v", err)
			}
		})
	}
}

// fuzzSnapshot generates snapshots across the whole interesting state space.
//
// CurrentReplicas is generated inside [minReplicas, maxReplicas]: the clamp is
// a hard bound that the controller's own writes always respect, and the one
// out-of-range case — an external actor setting the count above the maximum —
// is covered explicitly by
// TestDecide_ScaleDownFromAboveMaxReturnsIntoRangeImmediately, where the step
// limit deliberately does not apply.
type fuzzSnapshot struct {
	Snapshot
}

func (fuzzSnapshot) Generate(rnd *rand.Rand, _ int) reflect.Value {
	cfg := baseConfig()

	current := cfg.Target.MinReplicas + int32(rnd.Intn(int(cfg.Target.MaxReplicas-cfg.Target.MinReplicas+1)))

	s := Snapshot{
		Valid:           rnd.Intn(10) > 0, // usually valid, occasionally not
		CurrentReplicas: current,
		ReadyReplicas:   int32(rnd.Intn(int(current) + 1)),

		Backlog: Signal[int64]{
			Value:     int64(rnd.Intn(2000)),
			SampledAt: now.Add(-time.Duration(rnd.Intn(120)) * time.Second),
			Available: rnd.Intn(8) > 0,
		},
		AvgCPUMilliPercent: Signal[int64]{
			Value:     int64(rnd.Intn(150_000)),
			SampledAt: now.Add(-time.Duration(rnd.Intn(120)) * time.Second),
			Available: rnd.Intn(4) > 0,
		},

		FitCapacity:    int32(rnd.Intn(12)),
		CandidateNodes: int32(rnd.Intn(5)),

		RolloutInProgress: rnd.Intn(10) == 0,
		HPAPresent:        rnd.Intn(20) == 0,
		ExternalDrifts:    int32(rnd.Intn(6)),

		LastScaleUp:   now.Add(-time.Duration(rnd.Intn(600)) * time.Second),
		LastScaleDown: now.Add(-time.Duration(rnd.Intn(600)) * time.Second),

		PendingOurPods:   int32(rnd.Intn(3)),
		UnhealthyOurPods: int32(rnd.Intn(3)),
		UnhealthyPodAge:  time.Duration(rnd.Intn(600)) * time.Second,

		HoldBackoff: ClearBackoff(),
	}

	if rnd.Intn(3) == 0 {
		s.HoldBackoff = BackoffState{
			Attempts:         1 + rnd.Intn(6),
			NextEligible:     now.Add(time.Duration(rnd.Intn(600)-300) * time.Second),
			FitCapacityAtArm: int32(rnd.Intn(6)),
		}
	}

	// A history of random length, sometimes covering the window and sometimes
	// not, so the coverage gate is exercised in both states.
	count := rnd.Intn(25)
	for i := range count {
		s.DesiredHistory = append(s.DesiredHistory, DesiredSample{
			At:      now.Add(-time.Duration(i*15) * time.Second),
			Desired: int32(rnd.Intn(14)),
		})
	}

	return reflect.ValueOf(fuzzSnapshot{s})
}

// --- helpers ---------------------------------------------------------------

// baseConfig is the documented default configuration with a target set, which
// is the fixture every expectation in this file is stated against.
func baseConfig() *config.Config {
	cfg := config.Default()
	cfg.Target.Namespace = "data-pipeline"
	cfg.Target.Deployment = "normalizer"
	cfg.Workload.Signal.Source = config.SignalSourceNone
	return cfg
}

// baseSnapshot is a valid, quiet, unblocked snapshot: two replicas, both Ready,
// a fresh backlog of 50 items, and room to grow. Tests change only the field
// they are about.
func baseSnapshot() Snapshot {
	return Snapshot{
		Valid:           true,
		CurrentReplicas: 2,
		ReadyReplicas:   2,
		PodRequest:      resources.Request{CPUMilli: 500, MemoryBytes: 512 * 1024 * 1024},
		Backlog:         fresh(50),
		FitCapacity:     10,
		CandidateNodes:  3,
		Blocking:        resources.DimensionCPU,
		HoldBackoff:     ClearBackoff(),
	}
}

// scaleUpWanted is a snapshot whose demand asks for four more replicas and
// whose guards are all clear, so it reaches the feasibility gate.
func scaleUpWanted() Snapshot {
	s := baseSnapshot()
	s.CurrentReplicas, s.ReadyReplicas = 2, 2
	s.Backlog = fresh(400)
	s.FitCapacity = 10
	return s
}

// readyToScaleDown clears every scale-down guard except the one under test.
func readyToScaleDown() Snapshot {
	s := baseSnapshot()
	s.CurrentReplicas, s.ReadyReplicas = 8, 8
	s.Backlog = fresh(0)
	s.DesiredHistory = quietHistory(now, 1)
	return s
}

func fresh(backlog int64) Signal[int64] {
	return Signal[int64]{Value: backlog, SampledAt: now, Available: true}
}

func freshMilliPercent(v int64) Signal[int64] {
	return Signal[int64]{Value: v, SampledAt: now, Available: true}
}

// quietHistory returns a fully covered 300 s window in which the desired count
// never rose above desired: 21 samples at the default 15 s interval, the oldest
// exactly one window old.
func quietHistory(at time.Time, desired int32) []DesiredSample {
	return samplesEvery(at, 15*time.Second, 21, desired)
}

func samplesEvery(at time.Time, interval time.Duration, count int, desired int32) []DesiredSample {
	samples := make([]DesiredSample, 0, count)
	for i := range count {
		samples = append(samples, DesiredSample{
			At:      at.Add(-time.Duration(i) * interval),
			Desired: desired,
		})
	}
	return samples
}
