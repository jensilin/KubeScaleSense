package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Load profiles.
//
// The demonstration needs a spike whose shape is known in advance, because the
// interesting behaviour is what the controller does on the *edges*: the
// scale-up when pressure rises, the stabilization window when it falls. A
// profile makes those edges happen at a stated time rather than whenever the
// operator gets round to it.
const (
	// ProfileConstant sends at one rate until the record budget runs out.
	ProfileConstant = "constant"

	// ProfileLowHighLow is the canonical demo shape: a quiet baseline, a spike
	// that demands more replicas than are running, then a return to quiet so
	// the scale-down path is exercised too.
	ProfileLowHighLow = "low-high-low"
)

// Plan is a fully-specified run. Every field is explicit: a load generator with
// hidden defaults produces measurements nobody can reproduce.
type Plan struct {
	Profile string

	// LowRate and HighRate are records per second. Fractional rates are
	// allowed, because a baseline of one record every few seconds is a
	// perfectly reasonable quiet period.
	LowRate  float64
	HighRate float64

	// Phase is how long each of the three phases lasts under low-high-low.
	Phase time.Duration

	// Records caps the run. Zero means "until the profile finishes", which for
	// a constant profile means forever.
	Records int

	// Burst is how many records are sent back to back at each tick. Bursts are
	// how a queue is made to form: ten records arriving together at a pod with
	// four slots queues six of them, where ten records spread over a second
	// might queue none.
	Burst int

	// Pad inflates each record, for payload-size sensitivity.
	Pad int

	// RecordsPerFile applies to file mode only.
	RecordsPerFile int
}

// DefaultPlan is the shape used by the documented demo narrative.
func DefaultPlan() Plan {
	return Plan{
		Profile:        ProfileConstant,
		LowRate:        5,
		HighRate:       80,
		Phase:          60 * time.Second,
		Records:        0,
		Burst:          1,
		Pad:            0,
		RecordsPerFile: 50,
	}
}

// Validate rejects a plan that cannot produce the load it describes.
func (p Plan) Validate() error {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	switch p.Profile {
	case ProfileConstant, ProfileLowHighLow:
	default:
		add("profile %q must be %q or %q", p.Profile, ProfileConstant, ProfileLowHighLow)
	}

	if p.LowRate <= 0 {
		add("low-rate must be greater than 0, got %g", p.LowRate)
	}
	if p.Profile == ProfileLowHighLow {
		if p.HighRate <= 0 {
			add("high-rate must be greater than 0, got %g", p.HighRate)
		}
		if p.HighRate <= p.LowRate {
			add("high-rate (%g) must exceed low-rate (%g), or the spike is not a spike", p.HighRate, p.LowRate)
		}
		if p.Phase <= 0 {
			add("phase must be greater than 0, got %s", p.Phase)
		}
	}
	if p.Records < 0 {
		add("records must not be negative, got %d", p.Records)
	}
	if p.Burst < 1 {
		add("burst must be at least 1, got %d", p.Burst)
	}
	if p.Pad < 0 {
		add("pad must not be negative, got %d", p.Pad)
	}
	if p.RecordsPerFile < 1 {
		add("records-per-file must be at least 1, got %d", p.RecordsPerFile)
	}

	if len(problems) > 0 {
		return errors.New("invalid plan: " + strings.Join(problems, "; "))
	}
	return nil
}

// phase is one segment of a run at a fixed rate.
type phase struct {
	name     string
	rate     float64
	duration time.Duration
}

// phases expands the plan into the segments to be played in order.
//
// A constant profile is one open-ended phase; low-high-low is three bounded
// ones. Expanding the profile here, rather than branching inside the send loop,
// keeps the loop indifferent to which profile it is playing — and makes the
// shape of a run assertable in a unit test without sending anything.
func (p Plan) phases() []phase {
	if p.Profile == ProfileLowHighLow {
		return []phase{
			{name: "low", rate: p.LowRate, duration: p.Phase},
			{name: "high", rate: p.HighRate, duration: p.Phase},
			{name: "low", rate: p.LowRate, duration: p.Phase},
		}
	}
	return []phase{{name: "constant", rate: p.LowRate, duration: 0}}
}

// interval is the gap between bursts needed to sustain a rate.
//
// Computed from the burst size, so that "80 records per second in bursts of 10"
// means eight bursts a second rather than eighty.
func (p Plan) interval(rate float64) time.Duration {
	perBurst := rate / float64(p.Burst)
	if perBurst <= 0 {
		return time.Second
	}
	return time.Duration(float64(time.Second) / perBurst)
}
