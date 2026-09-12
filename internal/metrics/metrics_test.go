package metrics

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"

	"github.com/jensilin/KubeScaleSense/internal/config"
	"github.com/jensilin/KubeScaleSense/internal/resources"
)

var now = time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)

const miB int64 = 1024 * 1024

// --- UT-13: EWMA smoothing ------------------------------------------------

// With alpha 0.4 a step change reaches about 87 % of its true value within four
// samples. That is the trade smoothing makes — a real spike is delayed by
// roughly a minute at the default interval, a single bad scrape is absorbed —
// so the step response is asserted rather than assumed.
func TestSmoother_EWMAStepResponse(t *testing.T) {
	t.Parallel()

	s := NewSmoother(config.SmoothingConfig{Mode: config.SmoothingModeEWMA, Alpha: 0.4})

	// The first sample primes the average instead of being blended with zero,
	// which would otherwise halve the first reading and make a controller that
	// just started look idle.
	if got := s.Apply(0); got != 0 {
		t.Fatalf("first sample: got %d, want 0", got)
	}

	// Step from 0 to 1000.
	want := []int64{400, 640, 784, 870}
	for i, expected := range want {
		if got := s.Apply(1000); got != expected {
			t.Errorf("sample %d after the step: got %d, want %d", i+1, got, expected)
		}
	}

	// ~87 % of the step within four samples.
	if got := s.Apply(1000); got < 900 {
		t.Errorf("fifth sample = %d, want the average to have converged above 900", got)
	}
}

func TestSmoother_PrimesOnTheFirstSample(t *testing.T) {
	t.Parallel()

	s := NewSmoother(config.SmoothingConfig{Mode: config.SmoothingModeEWMA, Alpha: 0.4})

	if got := s.Apply(500); got != 500 {
		t.Errorf("first sample = %d, want the raw 500: a fresh controller must not look idle", got)
	}
}

// `none` is exactly the identity function, not an EWMA with alpha 1. They are
// indistinguishable in most cases but not all, and the unit tests rely on the
// distinction to keep their expectations exact.
func TestSmoother_NoneModeIsPassThrough(t *testing.T) {
	t.Parallel()

	s := NewSmoother(config.SmoothingConfig{Mode: config.SmoothingModeNone, Alpha: 0.4})

	for _, v := range []int64{0, 1, 1000, 7, 999_999, 0} {
		if got := s.Apply(v); got != v {
			t.Errorf("Apply(%d) = %d, want %d", v, got, v)
		}
	}
	if s.Mode() != config.SmoothingModeNone {
		t.Errorf("Mode() = %q, want %q", s.Mode(), config.SmoothingModeNone)
	}
}

// A single bad scrape is the thing smoothing is for: one spurious reading must
// not move the average far enough to trigger an action.
func TestSmoother_AbsorbsASingleOutlier(t *testing.T) {
	t.Parallel()

	s := NewSmoother(config.SmoothingConfig{Mode: config.SmoothingModeEWMA, Alpha: 0.4})

	for range 10 {
		s.Apply(100)
	}
	spiked := s.Apply(10_000)
	if spiked > 4_100 {
		t.Errorf("a single 10000 outlier moved the average to %d; smoothing should damp it", spiked)
	}

	recovered := s.Apply(100)
	if recovered > 2_500 {
		t.Errorf("average = %d after the outlier passed, want it falling back toward 100", recovered)
	}
}

func TestSmoother_Reset(t *testing.T) {
	t.Parallel()

	s := NewSmoother(config.SmoothingConfig{Mode: config.SmoothingModeEWMA, Alpha: 0.4})

	for range 5 {
		s.Apply(1000)
	}
	s.Reset()

	// After a reset the next sample primes again, so stale smoothing does not
	// outlive the state it described.
	if got := s.Apply(10); got != 10 {
		t.Errorf("first sample after Reset = %d, want the raw 10", got)
	}
}

// Integer state, not float64: reproducibility at boundary values must not
// depend on evaluation order or platform rounding (DR-15, NFR-07).
func TestSmoother_IsReproducible(t *testing.T) {
	t.Parallel()

	series := []int64{0, 137, 4211, 4211, 88, 0, 999, 1}

	run := func() []int64 {
		s := NewSmoother(config.SmoothingConfig{Mode: config.SmoothingModeEWMA, Alpha: 0.4})
		out := make([]int64, 0, len(series))
		for _, v := range series {
			out = append(out, s.Apply(v))
		}
		return out
	}

	first, second := run(), run()
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("sample %d differs between runs: %d vs %d", i, first[i], second[i])
		}
	}
}

// --- UT-23: warmup exclusion ---------------------------------------------

func TestIsReadyAndWarm(t *testing.T) {
	t.Parallel()

	const warmup = time.Minute

	tests := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{
			name: "ready and warm",
			pod:  pod("a", readySince(now.Add(-2*time.Minute))),
			want: true,
		},
		{
			name: "ready for exactly the warmup period",
			pod:  pod("a", readySince(now.Add(-warmup))),
			want: true,
		},
		{
			// Ready but not yet sent a request: it reports near-zero CPU and
			// would drag the average down exactly when a scale-up is in
			// progress, suppressing the next one.
			name: "ready but still warming",
			pod:  pod("a", readySince(now.Add(-30*time.Second))),
			want: false,
		},
		{
			name: "not ready",
			pod:  pod("a", condition(corev1.PodReady, corev1.ConditionFalse, now.Add(-time.Hour))),
			want: false,
		},
		{
			name: "no ready condition",
			pod:  pod("a"),
			want: false,
		},
		{
			// Nothing to judge warmth by, so it waits for the next sample
			// rather than risking the dilution warmup exists to prevent.
			name: "ready with no transition time",
			pod:  pod("a", condition(corev1.PodReady, corev1.ConditionTrue, time.Time{})),
			want: false,
		},
		{
			name: "pending",
			pod:  pod("a", readySince(now.Add(-time.Hour)), podPhase(corev1.PodPending)),
			want: false,
		},
		{
			name: "terminating",
			pod:  pod("a", readySince(now.Add(-time.Hour)), podTerminating()),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isReadyAndWarm(tt.pod, warmup, now); got != tt.want {
				t.Errorf("isReadyAndWarm = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUtilizationCollector_Collect(t *testing.T) {
	t.Parallel()

	request := resources.Request{CPUMilli: 500, MemoryBytes: 512 * miB}
	const warmup = time.Minute

	warm := []*corev1.Pod{
		pod("p1", readySince(now.Add(-5*time.Minute))),
		pod("p2", readySince(now.Add(-5*time.Minute))),
	}

	t.Run("averages over eligible pods", func(t *testing.T) {
		t.Parallel()

		lister := &fakeLister{metrics: []metricsv1beta1.PodMetrics{
			podMetrics("p1", now, 250, 256), // 50 % CPU
			podMetrics("p2", now, 450, 384), // 90 % CPU
		}}

		got, err := NewUtilizationCollector(lister).Collect(context.Background(), "ns", warm, request, warmup, now)
		if err != nil {
			t.Fatalf("Collect: %v", err)
		}
		if !got.Available {
			t.Fatal("signal should be available")
		}
		// (250 + 450) / (2 x 500) = 70 %
		if got.AvgCPUMilliPercent != 70_000 {
			t.Errorf("AvgCPUMilliPercent = %d, want 70000", got.AvgCPUMilliPercent)
		}
		// (256 + 384) / (2 x 512) = 62.5 %
		if got.AvgMemMilliPercent != 62_500 {
			t.Errorf("AvgMemMilliPercent = %d, want 62500", got.AvgMemMilliPercent)
		}
		if got.EligiblePods != 2 {
			t.Errorf("EligiblePods = %d, want 2", got.EligiblePods)
		}
	})

	t.Run("utilization is relative to the request, not the node", func(t *testing.T) {
		t.Parallel()

		lister := &fakeLister{metrics: []metricsv1beta1.PodMetrics{podMetrics("p1", now, 1000, 512)}}

		// 1000m used against a 500m request is 200 %: a pod may legitimately
		// burst above its request, and the signal must say so rather than
		// saturating at 100 %.
		got, err := NewUtilizationCollector(lister).Collect(context.Background(), "ns", warm[:1], request, warmup, now)
		if err != nil {
			t.Fatalf("Collect: %v", err)
		}
		if got.AvgCPUMilliPercent != 200_000 {
			t.Errorf("AvgCPUMilliPercent = %d, want 200000", got.AvgCPUMilliPercent)
		}
	})

	t.Run("warming pods are excluded from the average", func(t *testing.T) {
		t.Parallel()

		pods := []*corev1.Pod{
			pod("p1", readySince(now.Add(-5*time.Minute))),
			pod("p2", readySince(now.Add(-5*time.Second))), // just Ready
		}
		lister := &fakeLister{metrics: []metricsv1beta1.PodMetrics{
			podMetrics("p1", now, 450, 256),
			podMetrics("p2", now, 0, 8), // idle because it is new
		}}

		got, err := NewUtilizationCollector(lister).Collect(context.Background(), "ns", pods, request, warmup, now)
		if err != nil {
			t.Fatalf("Collect: %v", err)
		}
		if got.EligiblePods != 1 {
			t.Errorf("EligiblePods = %d, want 1", got.EligiblePods)
		}
		// 450/500 = 90 %, not the 45 % the idle newcomer would have produced.
		if got.AvgCPUMilliPercent != 90_000 {
			t.Errorf("AvgCPUMilliPercent = %d, want 90000: a warming pod must not dilute the average", got.AvgCPUMilliPercent)
		}
	})

	t.Run("no warm pod reports unavailable rather than zero", func(t *testing.T) {
		t.Parallel()

		pods := []*corev1.Pod{pod("p1", readySince(now.Add(-time.Second)))}
		lister := &fakeLister{metrics: []metricsv1beta1.PodMetrics{podMetrics("p1", now, 0, 8)}}

		got, err := NewUtilizationCollector(lister).Collect(context.Background(), "ns", pods, request, warmup, now)
		if err != nil {
			t.Fatalf("Collect: %v", err)
		}
		if got.Available {
			t.Error("with no eligible pod the signal must be unavailable, not zero: zero would read as an idle workload")
		}
		if got.AvgCPUMilliPercent != 0 || got.EligiblePods != 0 {
			t.Errorf("got %+v, want a zero-valued unavailable signal", got)
		}
	})

	t.Run("eligible pods with no samples yet report unavailable", func(t *testing.T) {
		t.Parallel()

		lister := &fakeLister{metrics: nil} // metrics-server has not sampled them

		got, err := NewUtilizationCollector(lister).Collect(context.Background(), "ns", warm, request, warmup, now)
		if err != nil {
			t.Fatalf("Collect: %v", err)
		}
		if got.Available {
			t.Error("no samples must report unavailable")
		}
	})

	t.Run("metrics of other pods are ignored", func(t *testing.T) {
		t.Parallel()

		lister := &fakeLister{metrics: []metricsv1beta1.PodMetrics{
			podMetrics("p1", now, 500, 512),
			podMetrics("someone-elses-pod", now, 4000, 4096),
		}}

		got, err := NewUtilizationCollector(lister).Collect(context.Background(), "ns", warm[:1], request, warmup, now)
		if err != nil {
			t.Fatalf("Collect: %v", err)
		}
		if got.EligiblePods != 1 || got.AvgCPUMilliPercent != 100_000 {
			t.Errorf("got %+v, want one pod at 100000 milli-percent", got)
		}
	})

	t.Run("the aggregate is only as fresh as its stalest contributor", func(t *testing.T) {
		t.Parallel()

		lister := &fakeLister{metrics: []metricsv1beta1.PodMetrics{
			podMetrics("p1", now, 250, 256),
			podMetrics("p2", now.Add(-90*time.Second), 250, 256),
		}}

		got, err := NewUtilizationCollector(lister).Collect(context.Background(), "ns", warm, request, warmup, now)
		if err != nil {
			t.Fatalf("Collect: %v", err)
		}
		if want := now.Add(-90 * time.Second); !got.SampledAt.Equal(want) {
			t.Errorf("SampledAt = %v, want the oldest contributing sample %v", got.SampledAt, want)
		}
	})

	t.Run("metrics-server absent degrades rather than fails", func(t *testing.T) {
		t.Parallel()

		wantErr := errors.New("the server could not find the requested resource")
		lister := &fakeLister{err: wantErr}

		got, err := NewUtilizationCollector(lister).Collect(context.Background(), "ns", warm, request, warmup, now)
		if !errors.Is(err, wantErr) {
			t.Errorf("error = %v, want it wrapped for diagnostics", err)
		}
		// The error is diagnostic detail about unavailability, so a caller that
		// ignores it still behaves safely.
		if got.Available {
			t.Error("the sample must be unavailable when the lister failed")
		}
	})

	t.Run("a template without requests has no denominator", func(t *testing.T) {
		t.Parallel()

		lister := &fakeLister{metrics: []metricsv1beta1.PodMetrics{podMetrics("p1", now, 250, 256)}}

		got, err := NewUtilizationCollector(lister).Collect(context.Background(), "ns", warm, resources.Request{}, warmup, now)
		if err != nil {
			t.Fatalf("Collect: %v", err)
		}
		if got.Available {
			t.Error("without a request there is no percentage to report")
		}
	})
}

// --- workload signal sources ---------------------------------------------

func TestNewWorkloadSignal(t *testing.T) {
	t.Parallel()

	t.Run("none", func(t *testing.T) {
		t.Parallel()

		signal, err := NewWorkloadSignal(config.SignalConfig{Source: config.SignalSourceNone}, func() time.Time { return now })
		if err != nil {
			t.Fatalf("NewWorkloadSignal: %v", err)
		}
		if signal.Source() != config.SignalSourceNone {
			t.Errorf("Source() = %q, want %q", signal.Source(), config.SignalSourceNone)
		}
		if err := signal.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	t.Run("synthetic", func(t *testing.T) {
		t.Parallel()

		path := writeScript(t, "loop: true\nsamples:\n  - pressureItems: 10\n")

		signal, err := NewWorkloadSignal(config.SignalConfig{
			Source:        config.SignalSourceSynthetic,
			SyntheticPath: path,
		}, func() time.Time { return now })
		if err != nil {
			t.Fatalf("NewWorkloadSignal: %v", err)
		}
		if signal.Source() != config.SignalSourceSynthetic {
			t.Errorf("Source() = %q, want %q", signal.Source(), config.SignalSourceSynthetic)
		}
	})

	// A misconfigured source must fail at startup rather than silently falling
	// back to something that happens to work: substituting `none` for a broken
	// `http` would freeze the controller forever and report it as a metrics
	// problem.
	t.Run("http rejects an unusable endpoint at startup", func(t *testing.T) {
		t.Parallel()

		_, err := NewWorkloadSignal(config.SignalConfig{
			Source:   config.SignalSourceHTTP,
			Endpoint: "normalizer-metrics:8081/metrics", // no scheme
		}, func() time.Time { return now })

		if err == nil {
			t.Fatal("an endpoint with no scheme must be rejected at startup")
		}
	})

	t.Run("an unknown source has no implementation", func(t *testing.T) {
		t.Parallel()

		if _, err := NewWorkloadSignal(config.SignalConfig{Source: "telepathy"}, nil); err == nil {
			t.Error("an unknown source must not return a nil interface")
		}
	})
}

// The `none` source is not a stub: HoldStaleMetrics forever, no action in
// either direction, is exactly what "uncertainty means freeze" requires.
func TestNoneSignal_IsPermanentlyUnavailableWithoutError(t *testing.T) {
	t.Parallel()

	got, err := NoneSignal{}.Collect(context.Background())
	if err != nil {
		t.Errorf("Collect returned %v; being unavailable is this source's specified behaviour, not a fault", err)
	}
	if got.Available {
		t.Error("the none source must never report an available sample")
	}
}

func TestSyntheticSignal_ReplaysTheScript(t *testing.T) {
	t.Parallel()

	path := writeScript(t, `
loop: true
samples:
  - pressureItems: 100
    inFlightRequests: 4
    requestRateMilliPerSec: 5000
    processingRateMilliPerSec: 4000
    processingLatencyMillis: 250
  - pressureItems: 200
  - available: false
  - pressureItems: 50
    ageSeconds: 600
`)

	signal, err := NewSyntheticSignal(path, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewSyntheticSignal: %v", err)
	}

	first := collect(t, signal)
	if first.PressureItems != 100 || first.InFlightRequests != 4 {
		t.Errorf("first sample = %+v, want 100 items and 4 in flight", first)
	}
	if first.RequestRateMilliPerSec != 5000 || first.ProcessingLatencyMillis != 250 {
		t.Errorf("first sample rates = %+v, want the scripted values", first)
	}
	// Available defaults to true when the key is absent, which is why it is a
	// pointer in the script: a series of samples silently defaulting to
	// unavailable would be a confusing failure.
	if !first.Available {
		t.Error("a sample with no `available` key must default to available")
	}

	if second := collect(t, signal); second.PressureItems != 200 {
		t.Errorf("second sample = %d items, want 200", second.PressureItems)
	}

	if third := collect(t, signal); third.Available {
		t.Error("the third sample scripts unavailability")
	}

	// ageSeconds backdates the sample, which is how a frozen source is
	// scripted without breaking a real dependency.
	fourth := collect(t, signal)
	if want := now.Add(-10 * time.Minute); !fourth.SampledAt.Equal(want) {
		t.Errorf("SampledAt = %v, want %v (backdated by ageSeconds)", fourth.SampledAt, want)
	}

	// loop: true wraps back to the start.
	if wrapped := collect(t, signal); wrapped.PressureItems != 100 {
		t.Errorf("after the last sample = %d items, want the series to loop back to 100", wrapped.PressureItems)
	}
}

// Without loop, the final sample repeats: that models a workload which reached
// a steady state, rather than one that vanished.
func TestSyntheticSignal_WithoutLoopHoldsTheFinalSample(t *testing.T) {
	t.Parallel()

	path := writeScript(t, "samples:\n  - pressureItems: 10\n  - pressureItems: 20\n")

	signal, err := NewSyntheticSignal(path, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewSyntheticSignal: %v", err)
	}

	collect(t, signal)
	for range 5 {
		if got := collect(t, signal); got.PressureItems != 20 {
			t.Fatalf("got %d items, want the final sample 20 to repeat", got.PressureItems)
		}
	}
}

func TestSyntheticSignal_RejectsBadScripts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "no samples", body: "loop: true\nsamples: []\n", wantErr: "no samples defined"},
		{name: "empty file", body: "", wantErr: "no samples defined"},
		{
			// A mistyped key must be an error rather than a silently ignored
			// line that makes the replay differ from what the author wrote.
			name:    "unknown key",
			body:    "samples:\n  - pressureItem: 10\n",
			wantErr: "field pressureItem not found",
		},
		{name: "malformed yaml", body: "samples: [{", wantErr: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewSyntheticSignal(writeScript(t, tt.body), func() time.Time { return now })
			if err == nil {
				t.Fatal("expected an error")
			}
			if tt.wantErr != "" && !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestSyntheticSignal_RequiresAPath(t *testing.T) {
	t.Parallel()

	if _, err := NewSyntheticSignal("", func() time.Time { return now }); err == nil {
		t.Error("an empty syntheticPath must be an error")
	}
	if _, err := NewSyntheticSignal(filepath.Join(t.TempDir(), "absent.yaml"), func() time.Time { return now }); err == nil {
		t.Error("an unreadable script must be an error")
	}
}

// The script is read once, at construction, so that editing it mid-run cannot
// make a replay non-reproducible.
func TestSyntheticSignal_IgnoresLaterEditsToTheScript(t *testing.T) {
	t.Parallel()

	path := writeScript(t, "samples:\n  - pressureItems: 10\n")

	signal, err := NewSyntheticSignal(path, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewSyntheticSignal: %v", err)
	}

	if err := os.WriteFile(path, []byte("samples:\n  - pressureItems: 9999\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if got := collect(t, signal); got.PressureItems != 10 {
		t.Errorf("got %d items, want the 10 loaded at construction", got.PressureItems)
	}
}

// A source that blocks past the timeout is unavailable, not slow.
func TestSyntheticSignal_HonoursTheContext(t *testing.T) {
	t.Parallel()

	path := writeScript(t, "samples:\n  - pressureItems: 10\n")
	signal, err := NewSyntheticSignal(path, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewSyntheticSignal: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := signal.Collect(ctx)
	if err == nil {
		t.Error("a cancelled context must be reported")
	}
	if got.Available {
		t.Error("the sample must be unavailable when the context is done")
	}
}

func TestUnavailable_IsTheCanonicalNoAnswer(t *testing.T) {
	t.Parallel()

	got := Unavailable()
	if got.Available || got.PressureItems != 0 || !got.SampledAt.IsZero() {
		t.Errorf("Unavailable() = %+v, want a zero sample with Available false", got)
	}
}

// --- helpers ---------------------------------------------------------------

type fakeLister struct {
	metrics []metricsv1beta1.PodMetrics
	err     error
}

func (f *fakeLister) ListPodMetrics(context.Context, string) ([]metricsv1beta1.PodMetrics, error) {
	return f.metrics, f.err
}

func collect(t *testing.T, signal WorkloadSignal) Sample {
	t.Helper()

	got, err := signal.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return got
}

func writeScript(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "pressure.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type podOpt func(*corev1.Pod)

func pod(name string, opts ...podOpt) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func readySince(at time.Time) podOpt {
	return condition(corev1.PodReady, corev1.ConditionTrue, at)
}

func condition(kind corev1.PodConditionType, status corev1.ConditionStatus, at time.Time) podOpt {
	return func(p *corev1.Pod) {
		cond := corev1.PodCondition{Type: kind, Status: status}
		if !at.IsZero() {
			cond.LastTransitionTime = metav1.NewTime(at)
		}
		p.Status.Conditions = append(p.Status.Conditions, cond)
	}
}

func podPhase(phase corev1.PodPhase) podOpt {
	return func(p *corev1.Pod) { p.Status.Phase = phase }
}

func podTerminating() podOpt {
	return func(p *corev1.Pod) {
		ts := metav1.NewTime(now.Add(-time.Second))
		p.DeletionTimestamp = &ts
	}
}

func podMetrics(name string, at time.Time, cpuMilli, memoryMiB int64) metricsv1beta1.PodMetrics {
	return metricsv1beta1.PodMetrics{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Timestamp:  metav1.NewTime(at),
		Containers: []metricsv1beta1.ContainerMetrics{{
			Name: "app",
			Usage: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(memoryMiB*miB, resource.BinarySI),
			},
		}},
	}
}
