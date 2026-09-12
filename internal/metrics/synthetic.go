package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/jensilin/KubeScaleSense/internal/config"
)

// SyntheticSignal replays a scripted pressure series from a file, advancing one
// step per Collect.
//
// It exists so that P1 can be built, demonstrated, and tested before the
// demonstration pipeline exists — but its real value is reproducibility. A
// scripted series makes the decision engine's behaviour repeatable in a way a
// live pipeline never is, so an E2E assertion on a reason-code sequence can be
// exact rather than approximate.
//
// The script is read once, at construction, so that a mid-run edit cannot make
// a replay non-reproducible. An unreadable or empty script is a startup error.
type SyntheticSignal struct {
	path   string
	clock  func() time.Time
	script syntheticScript

	mu     sync.Mutex
	cursor int
}

// syntheticScript is the on-disk format.
type syntheticScript struct {
	// Loop restarts the series after the last sample. With Loop false the final
	// sample repeats indefinitely, which models a workload that reached a
	// steady state rather than one that vanished.
	Loop    bool              `yaml:"loop"`
	Samples []syntheticSample `yaml:"samples"`
}

// syntheticSample is one scripted step.
//
// Available and Age exist so that the script can exercise the failure half of
// the demand path — HoldStaleMetrics from an unavailable source, and from a
// source that answers with an old sample — without needing to break a real
// dependency.
type syntheticSample struct {
	PressureItems             int64 `yaml:"pressureItems"`
	InFlightRequests          int64 `yaml:"inFlightRequests"`
	RequestRateMilliPerSec    int64 `yaml:"requestRateMilliPerSec"`
	ProcessingRateMilliPerSec int64 `yaml:"processingRateMilliPerSec"`
	ProcessingLatencyMillis   int64 `yaml:"processingLatencyMillis"`

	// Available defaults to true when the key is absent, which is why it is a
	// pointer: a script full of samples that silently defaulted to unavailable
	// would be a confusing failure.
	Available *bool `yaml:"available"`

	// AgeSeconds backdates SampledAt, simulating a frozen source.
	AgeSeconds int64 `yaml:"ageSeconds"`
}

// NewSyntheticSignal loads and validates the script.
func NewSyntheticSignal(path string, clock func() time.Time) (*SyntheticSignal, error) {
	if path == "" {
		return nil, fmt.Errorf("workload.signal.syntheticPath is empty; the synthetic source needs a scripted series to replay")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading the synthetic pressure series %q: %w", path, err)
	}

	script, err := parseSyntheticScript(raw)
	if err != nil {
		return nil, fmt.Errorf("parsing the synthetic pressure series %q: %w", path, err)
	}

	return &SyntheticSignal{path: path, clock: clock, script: script}, nil
}

// parseSyntheticScript decodes the script strictly, so that a mistyped key is an
// error rather than a silently ignored line that makes a replay behave
// differently from what the author wrote.
func parseSyntheticScript(raw []byte) (syntheticScript, error) {
	var script syntheticScript

	dec := yaml.NewDecoder(bytesReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&script); err != nil && !errors.Is(err, io.EOF) {
		return syntheticScript{}, err
	}
	// An empty document decodes to io.EOF, which as an operator-facing message
	// says nothing at all. An empty file is an easy mistake to make in a
	// ConfigMap, so it falls through to the specific error below.
	if len(script.Samples) == 0 {
		return syntheticScript{}, fmt.Errorf("no samples defined; add at least one entry under `samples:`")
	}
	return script, nil
}

// Source implements WorkloadSignal.
func (s *SyntheticSignal) Source() string { return config.SignalSourceSynthetic }

// Collect returns the next scripted sample.
//
// The context is honoured but never blocks: reading from memory cannot time out,
// and pretending otherwise by sleeping would make the unit tests slow for no
// gain in fidelity.
func (s *SyntheticSignal) Collect(ctx context.Context) (Sample, error) {
	if err := ctx.Err(); err != nil {
		return Unavailable(), err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.script.Samples[s.cursor]
	s.advance()

	available := true
	if entry.Available != nil {
		available = *entry.Available
	}
	if !available {
		return Unavailable(), nil
	}

	now := s.clock()
	return Sample{
		PressureItems:             entry.PressureItems,
		InFlightRequests:          entry.InFlightRequests,
		RequestRateMilliPerSec:    entry.RequestRateMilliPerSec,
		ProcessingRateMilliPerSec: entry.ProcessingRateMilliPerSec,
		ProcessingLatencyMillis:   entry.ProcessingLatencyMillis,
		SampledAt:                 now.Add(-time.Duration(entry.AgeSeconds) * time.Second),
		Available:                 true,
	}, nil
}

// advance moves the cursor, wrapping when the script loops and holding on the
// final sample when it does not.
func (s *SyntheticSignal) advance() {
	if s.cursor+1 < len(s.script.Samples) {
		s.cursor++
		return
	}
	if s.script.Loop {
		s.cursor = 0
	}
}

// Close implements WorkloadSignal.
func (s *SyntheticSignal) Close() error { return nil }
