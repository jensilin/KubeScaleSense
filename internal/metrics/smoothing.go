package metrics

import (
	"bytes"
	"io"

	"github.com/jensilin/KubeScaleSense/internal/config"
)

// milliScale is the fixed-point scale used by the smoother's internal state.
const milliScale int64 = 1000

// Smoother damps single-sample artefacts in the pressure signal with an
// exponentially weighted moving average.
//
// The state is carried in integer milli-items rather than float64. A float EWMA
// accumulated across reconciles makes exact reproducibility depend on evaluation
// order and platform rounding, which defeats the determinism requirement at
// precisely the boundary values a table-driven test would choose (DR-15,
// NFR-07).
//
// Smoothing is a genuine trade and both values are exported as metrics so it
// stays auditable: with alpha 0.4 a step change reaches about 87 % of its true
// value within four samples, so a real spike is delayed by roughly one minute at
// the default interval while a single bad scrape is absorbed.
type Smoother struct {
	mode       string
	alphaMilli int64

	primed     bool
	stateMilli int64
}

// NewSmoother builds a smoother from the validated configuration.
func NewSmoother(cfg config.SmoothingConfig) *Smoother {
	return &Smoother{
		mode:       cfg.Mode,
		alphaMilli: int64(cfg.Alpha * float64(milliScale)),
	}
}

// Mode reports the configured mode, for the startup log line.
func (s *Smoother) Mode() string { return s.mode }

// Apply returns the smoothed value for a raw observation.
//
// In `none` mode this is exactly the identity function — not an EWMA with
// alpha 1, which would be indistinguishable in most cases but not all. The unit
// tests run in `none` mode so their expectations stay exact.
//
// The first observation primes the average rather than being blended with a
// zero, which would otherwise halve the very first reading and make a
// controller that just started look idle.
func (s *Smoother) Apply(raw int64) int64 {
	if s.mode != config.SmoothingModeEWMA {
		return raw
	}

	rawMilli := raw * milliScale
	if !s.primed {
		s.primed = true
		s.stateMilli = rawMilli
		return raw
	}

	s.stateMilli = (s.alphaMilli*rawMilli + (milliScale-s.alphaMilli)*s.stateMilli) / milliScale

	// Round half up on the way out, so the reported value is the nearest item
	// count rather than always the floor.
	return (s.stateMilli + milliScale/2) / milliScale
}

// Reset discards the accumulated average. Used when the controller adopts an
// externally changed replica count and resets its history, so that stale
// smoothing does not outlive the state it described.
func (s *Smoother) Reset() {
	s.primed = false
	s.stateMilli = 0
}

// bytesReader is a tiny helper so that the YAML decoders in this package can be
// constructed from a byte slice without each file importing bytes.
func bytesReader(raw []byte) io.Reader {
	return bytes.NewReader(raw)
}
