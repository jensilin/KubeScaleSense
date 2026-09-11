package config

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that round-trips through YAML as a Go duration
// string ("15s", "5m"), which is the form the documented configuration schema
// uses. The standard library's time.Duration marshals as an integer count of
// nanoseconds, so a plain field would silently read "15s" as an error and
// "15" as 15ns.
type Duration time.Duration

// Duration returns the value as a time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalYAML accepts only a quoted or bare duration string. Bare numbers are
// rejected rather than guessed at: "interval: 15" is far more likely to mean
// fifteen seconds than fifteen nanoseconds, and an autoscaler that reconciles
// every 15ns is a denial of service against its own API server.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("must be a duration string such as %q or %q", "30s", "5m")
	}

	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("%q is not a valid duration: expected a form such as %q, %q or %q", s, "500ms", "30s", "15m")
	}

	*d = Duration(parsed)
	return nil
}

// MarshalYAML keeps round-tripping symmetric, so a config dumped back out is
// still valid input.
func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }
