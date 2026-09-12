package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/config"
)

// Sample is one observation of workload pressure.
//
// Only PressureItems drives the decision in v0.2. The rate and latency fields
// are observability-only and are recorded from Phase 1 onward so that a
// latency- or arrival-rate-driven demand model can later be derived from
// measured data rather than guessed (FR-39).
//
// Rates and latencies are held in milli-units for the same reason the rest of
// the pipeline is integer: exact reproducibility at boundary values (DR-15).
type Sample struct {
	// PressureItems is outstanding work: queued plus in flight.
	PressureItems int64

	// InFlightRequests is what is being served right now. Reported, and the
	// natural input for pod-deletion-cost in P3. It makes "the pool is busy but
	// nothing is queued" visible, which is otherwise indistinguishable from
	// idle.
	InFlightRequests int64

	RequestRateMilliPerSec    int64
	ProcessingRateMilliPerSec int64
	ProcessingLatencyMillis   int64

	// SampledAt is when the *source* produced this value, which is not
	// necessarily when we received it. Freshness is judged on the older of the
	// two, so that a source which is reachable but frozen is detected (DR-14).
	SampledAt time.Time

	// Available is false whenever the value is not trustworthy. A zero
	// PressureItems with Available true means "no work"; Available false means
	// "no answer". Collapsing the two is the failure this type exists to
	// prevent.
	Available bool
}

// Unavailable is the canonical "no answer" sample. Every failure path returns
// this rather than a zero-valued Sample with Available left true, so that a
// forgotten field cannot turn an outage into an apparent idle pipeline.
func Unavailable() Sample {
	return Sample{Available: false}
}

// WorkloadSignal is the replaceable source of workload pressure.
//
// Implementations must obey two rules:
//
// Collect reports unavailability in the returned Sample, and any error it
// returns is diagnostic detail *about* that unavailability rather than a
// separate outcome. A caller that ignores the error still behaves safely.
//
// Collect must respect the context deadline. A source that blocks past
// signal.timeout is unavailable, not slow.
type WorkloadSignal interface {
	// Source is the configured source name, used as the `source` metric label.
	Source() string

	// Collect returns the newest observation.
	Collect(ctx context.Context) (Sample, error)

	// Close releases any resources held by the source.
	Close() error
}

// NewWorkloadSignal builds the configured source.
//
// An unimplemented source is a startup error rather than a silent fallback to
// something that happens to work: quietly substituting `none` for a
// misconfigured `http` would make the controller freeze forever and report it as
// a metrics problem.
func NewWorkloadSignal(cfg config.SignalConfig, clock func() time.Time) (WorkloadSignal, error) {
	switch cfg.Source {
	case config.SignalSourceSynthetic:
		return NewSyntheticSignal(cfg.SyntheticPath, clock)

	case config.SignalSourceNone:
		return NoneSignal{}, nil

	case config.SignalSourceHTTP:
		return nil, fmt.Errorf(
			"workload.signal.source %q is not implemented until P2 — Demonstration workload, because it "+
				"scrapes the Normalizer's metrics endpoint and the Normalizer does not exist yet; "+
				"use %q or %q in this phase",
			config.SignalSourceHTTP, config.SignalSourceSynthetic, config.SignalSourceNone)

	default:
		// Unreachable: configuration validation rejects unknown sources. Kept
		// so that adding a source to the config enum without adding it here
		// fails loudly instead of returning a nil interface.
		return nil, fmt.Errorf("workload.signal.source %q has no implementation", cfg.Source)
	}
}

// NoneSignal is permanently unavailable.
//
// It is not a stub: it is the honest way to run the controller with no demand
// source at all, and the resulting behaviour — HoldStaleMetrics forever, no
// action in either direction — is exactly what "uncertainty means freeze"
// requires. P0's deployment manifest sets it for that reason.
type NoneSignal struct{}

// Source implements WorkloadSignal.
func (NoneSignal) Source() string { return config.SignalSourceNone }

// Collect implements WorkloadSignal. It always reports unavailable, and does so
// without an error, because being unavailable is this source's specified
// behaviour rather than a fault.
func (NoneSignal) Collect(context.Context) (Sample, error) {
	return Unavailable(), nil
}

// Close implements WorkloadSignal.
func (NoneSignal) Close() error { return nil }
