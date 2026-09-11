package config

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Source records where the effective configuration came from, so that startup
// logging can state it rather than leaving an operator to guess which of a
// file, an override, or a default is in force.
type Source struct {
	// Path is the config file that was read, or "" if only defaults and
	// environment overrides were used.
	Path string
	// EnvOverrides lists the KSS_* variables that were applied, sorted. Names
	// only: values are never recorded here, so a Source is always safe to log.
	EnvOverrides []string
}

func (s Source) String() string {
	base := "defaults"
	if s.Path != "" {
		base = "file:" + s.Path
	}
	if len(s.EnvOverrides) == 0 {
		return base
	}
	return fmt.Sprintf("%s + %d env override(s)", base, len(s.EnvOverrides))
}

// Loader resolves configuration from a file, environment overrides, and
// defaults. Every external dependency is a field so that the whole loader is
// testable without touching a real filesystem or process environment.
type Loader struct {
	// ReadFile reads the YAML config file. Defaults to os.ReadFile.
	ReadFile func(string) ([]byte, error)
	// Getenv reads a single environment variable. Defaults to os.Getenv.
	Getenv func(string) string
	// Environ lists the full environment, used to detect misspelled KSS_*
	// overrides. Defaults to os.Environ.
	Environ func() []string
	// OpenFile probes a path for readability. Defaults to opening the file.
	OpenFile func(string) (io.ReadCloser, error)
}

// Load reads, overrides, and validates configuration using the real
// filesystem and process environment.
func Load(path string) (*Config, Source, error) {
	return (&Loader{}).Load(path)
}

// Load resolves the effective configuration.
//
// Order is defaults, then the YAML file, then KSS_* environment overrides, then
// validation. Validation runs last and exactly once, so a value supplied by any
// layer is held to the same standard.
func (l *Loader) Load(path string) (*Config, Source, error) {
	readFile := l.ReadFile
	if readFile == nil {
		readFile = os.ReadFile
	}
	getenv := l.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	environ := l.Environ
	if environ == nil {
		environ = os.Environ
	}

	cfg := Default()
	src := Source{Path: path}

	if path != "" {
		data, err := readFile(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil, src, fmt.Errorf("config file %s does not exist", path)
			}
			return nil, src, fmt.Errorf("reading config file %s: %w", path, err)
		}
		if err := decodeYAML(data, cfg); err != nil {
			return nil, src, fmt.Errorf("parsing config file %s: %w", path, err)
		}
	}

	if err := applyEnvOverrides(cfg, getenv, environ); err != nil {
		return nil, src, err
	}
	src.EnvOverrides = appliedOverrides(getenv)

	if err := cfg.Validate(); err != nil {
		return nil, src, err
	}
	if err := l.validateSignalAccess(cfg); err != nil {
		return nil, src, err
	}

	return cfg, src, nil
}

// decodeYAML parses strictly: an unknown key is an error, not a warning.
//
// Strictness does real work here beyond catching typos. CR-5 requires that no
// secrets live in this file, and rejecting unknown keys means an attempt to add
// a `password:` or `token:` field fails at startup instead of sitting in a
// ConfigMap being quietly ignored.
func decodeYAML(data []byte, cfg *Config) error {
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)

	if err := dec.Decode(cfg); err != nil {
		if errors.Is(err, io.EOF) {
			// An empty file is a valid "use all defaults" statement; Validate
			// still rejects it, because target has no default.
			return nil
		}
		return fmt.Errorf("%w\n  recognised keys: %s", err, strings.Join(Keys(), ", "))
	}

	// A second document would be silently dropped by a single Decode call.
	var extra yaml.Node
	if err := dec.Decode(&extra); err == nil {
		return errors.New("contains more than one YAML document; the config file must hold exactly one")
	}
	return nil
}

// validateSignalAccess is the one check that needs the filesystem, which is why
// it is separate from the pure Validate: a synthetic signal source whose
// scripted series cannot be read would start successfully and then report the
// pressure signal as permanently unavailable, freezing the controller for a
// reason that looks like a metrics outage rather than a typo.
func (l *Loader) validateSignalAccess(cfg *Config) error {
	if cfg.Workload.Signal.Source != SignalSourceSynthetic {
		return nil
	}
	path := cfg.Workload.Signal.SyntheticPath

	open := l.OpenFile
	if open == nil {
		open = func(name string) (io.ReadCloser, error) { return os.Open(name) }
	}

	f, err := open(path)
	if err == nil {
		_ = f.Close()
		return nil
	}

	problem := fmt.Sprintf("cannot be read (%v); workload.signal.source is %q, so this file must exist and be readable at startup",
		err, SignalSourceSynthetic)
	if errors.Is(err, fs.ErrNotExist) {
		problem = fmt.Sprintf("does not exist; workload.signal.source is %q, so mount the scripted pressure series here or set the source to %q",
			SignalSourceSynthetic, SignalSourceNone)
	}

	return &ValidationError{Errors: []FieldError{{
		Key:     "workload.signal.syntheticPath",
		Value:   path,
		Problem: problem,
		Ref:     "FR-36",
	}}}
}

func appliedOverrides(getenv func(string) string) []string {
	var applied []string
	for _, name := range EnvVars() {
		if getenv(name) != "" {
			applied = append(applied, name)
		}
	}
	sort.Strings(applied)
	return applied
}
