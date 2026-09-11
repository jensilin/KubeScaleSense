package config

import (
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
	"time"
)

// testLoader returns a Loader wired to in-memory fakes, so no test touches the
// real filesystem or process environment.
func testLoader(files map[string]string, env map[string]string) *Loader {
	return &Loader{
		ReadFile: func(name string) ([]byte, error) {
			content, ok := files[name]
			if !ok {
				return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
			}
			return []byte(content), nil
		},
		Getenv: func(key string) string { return env[key] },
		Environ: func() []string {
			out := make([]string, 0, len(env))
			for k, v := range env {
				out = append(out, k+"="+v)
			}
			return out
		},
		OpenFile: func(name string) (io.ReadCloser, error) {
			content, ok := files[name]
			if !ok {
				return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
			}
			return io.NopCloser(strings.NewReader(content)), nil
		},
	}
}

// minimalConfig exercises CR-1: a config specifying only the target must load.
const minimalConfig = `
target:
  namespace: data-pipeline
  deployment: normalizer
`

func TestLoad_MinimalConfigUsesDocumentedDefaults(t *testing.T) {
	t.Parallel()

	files := map[string]string{
		"kss.yaml":                          minimalConfig,
		"/etc/kubescalesense/pressure.yaml": "series: []",
	}

	cfg, src, err := testLoader(files, nil).Load("kss.yaml")
	if err != nil {
		t.Fatalf("minimal config must load (CR-1), got: %v", err)
	}

	if cfg.Controller.Interval.Duration() != DefaultInterval {
		t.Errorf("interval = %v, want default %v", cfg.Controller.Interval, DefaultInterval)
	}
	if cfg.Workload.ItemsPerReplica != DefaultItemsPerReplica {
		t.Errorf("itemsPerReplica = %d, want default %d", cfg.Workload.ItemsPerReplica, DefaultItemsPerReplica)
	}
	if cfg.Target.Namespace != "data-pipeline" || cfg.Target.Deployment != "normalizer" {
		t.Errorf("target = %s/%s, want data-pipeline/normalizer", cfg.Target.Namespace, cfg.Target.Deployment)
	}
	if src.Path != "kss.yaml" {
		t.Errorf("source path = %q, want kss.yaml", src.Path)
	}
	if got := src.String(); got != "file:kss.yaml" {
		t.Errorf("source = %q, want file:kss.yaml", got)
	}
}

func TestLoad_FileOverridesDefaults(t *testing.T) {
	t.Parallel()

	files := map[string]string{
		"kss.yaml": `
controller:
  interval: 45s
  dryRun: true
  logFormat: text
target:
  namespace: prod
  deployment: normalizer
  maxReplicas: 30
workload:
  itemsPerReplica: 120
  signal:
    source: none
`,
	}

	cfg, _, err := testLoader(files, nil).Load("kss.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := cfg.Controller.Interval.Duration(); got != 45*time.Second {
		t.Errorf("interval = %v, want 45s", got)
	}
	if !cfg.Controller.DryRun {
		t.Error("dryRun should be true")
	}
	if cfg.Controller.LogFormat != LogFormatText {
		t.Errorf("logFormat = %q, want text", cfg.Controller.LogFormat)
	}
	if cfg.Target.MaxReplicas != 30 {
		t.Errorf("maxReplicas = %d, want 30", cfg.Target.MaxReplicas)
	}
	// Untouched keys must still hold their defaults.
	if cfg.Scaling.ScaleUpCooldown.Duration() != DefaultScaleUpCooldown {
		t.Errorf("scaleUpCooldown = %v, want default", cfg.Scaling.ScaleUpCooldown)
	}
}

func TestLoad_EnvOverridesFile(t *testing.T) {
	t.Parallel()

	files := map[string]string{"kss.yaml": minimalConfig}
	env := map[string]string{
		"KSS_CONTROLLER_INTERVAL":        "30s",
		"KSS_CONTROLLER_DRYRUN":          "true",
		"KSS_TARGET_MAXREPLICAS":         "25",
		"KSS_WORKLOAD_ITEMSPERREPLICA":   "300",
		"KSS_WORKLOAD_SIGNAL_SOURCE":     "none",
		"KSS_SCALING_HOLDBACKOFF_FACTOR": "3.5",
		"KSS_RESOURCES_RESPECTTAINTS":    "false",
	}

	cfg, src, err := testLoader(files, env).Load("kss.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := cfg.Controller.Interval.Duration(); got != 30*time.Second {
		t.Errorf("interval = %v, want 30s from env", got)
	}
	if !cfg.Controller.DryRun {
		t.Error("dryRun should be overridden to true")
	}
	if cfg.Target.MaxReplicas != 25 {
		t.Errorf("maxReplicas = %d, want 25", cfg.Target.MaxReplicas)
	}
	if cfg.Workload.ItemsPerReplica != 300 {
		t.Errorf("itemsPerReplica = %d, want 300", cfg.Workload.ItemsPerReplica)
	}
	if cfg.Workload.Signal.Source != SignalSourceNone {
		t.Errorf("signal.source = %q, want none", cfg.Workload.Signal.Source)
	}
	if cfg.Scaling.HoldBackoff.Factor != 3.5 {
		t.Errorf("holdBackoff.factor = %v, want 3.5", cfg.Scaling.HoldBackoff.Factor)
	}
	if cfg.Resources.RespectTaints {
		t.Error("respectTaints should be overridden to false")
	}

	if len(src.EnvOverrides) != len(env) {
		t.Errorf("source should record %d overrides, got %v", len(env), src.EnvOverrides)
	}
	if !strings.Contains(src.String(), "env override") {
		t.Errorf("source string should mention overrides, got %q", src.String())
	}
}

// Overrides must be held to the same validation standard as file values,
// otherwise an env var becomes a way to smuggle in an unsafe configuration.
func TestLoad_EnvOverrideIsValidated(t *testing.T) {
	t.Parallel()

	files := map[string]string{"kss.yaml": minimalConfig}
	env := map[string]string{"KSS_CONTROLLER_INTERVAL": "1s"}

	_, _, err := testLoader(files, env).Load("kss.yaml")
	if err == nil {
		t.Fatal("an override below the interval floor must be rejected")
	}
	if !strings.Contains(err.Error(), "controller.interval") {
		t.Errorf("error should name the key, got: %v", err)
	}
}

func TestLoad_EnvOverrideParseErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		wantMsg string
	}{
		{
			name:    "unparseable duration",
			env:     map[string]string{"KSS_CONTROLLER_INTERVAL": "soon"},
			wantMsg: "not a valid duration",
		},
		{
			name:    "unparseable bool",
			env:     map[string]string{"KSS_CONTROLLER_DRYRUN": "yes-please"},
			wantMsg: "not a valid boolean",
		},
		{
			name:    "unparseable integer",
			env:     map[string]string{"KSS_TARGET_MAXREPLICAS": "lots"},
			wantMsg: "not a valid integer",
		},
		{
			name:    "unparseable float",
			env:     map[string]string{"KSS_SCALING_SCALEDOWNWINDOWCOVERAGE": "most"},
			wantMsg: "not a valid number",
		},
		{
			// A misspelled override would otherwise leave the controller on a
			// default while the operator believes it took effect.
			name:    "misspelled variable is rejected, not ignored",
			env:     map[string]string{"KSS_CONTROLER_INTERVAL": "30s"},
			wantMsg: "not a recognised configuration override",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			files := map[string]string{"kss.yaml": minimalConfig}
			_, _, err := testLoader(files, tt.env).Load("kss.yaml")
			if err == nil {
				t.Fatal("expected rejection")
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("expected message to contain %q, got: %v", tt.wantMsg, err)
			}
		})
	}
}

// An unknown key is usually a typo, and CR-5 makes strictness load-bearing: an
// attempt to add a credential field must fail rather than be ignored.
func TestLoad_RejectsUnknownKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "misspelled key",
			yaml: minimalConfig + "\ncontroller:\n  intervel: 30s\n",
		},
		{
			name: "smuggled credential",
			yaml: minimalConfig + "\nworkload:\n  signal:\n    password: hunter2\n",
		},
		{
			name: "reintroduced work store",
			yaml: minimalConfig + "\nworkload:\n  workStore:\n    dsn: postgres://localhost/kss\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			files := map[string]string{"kss.yaml": tt.yaml}
			_, _, err := testLoader(files, nil).Load("kss.yaml")
			if err == nil {
				t.Fatal("expected an unknown key to be rejected")
			}
			if !strings.Contains(err.Error(), "recognised keys") {
				t.Errorf("error should list the recognised keys, got: %v", err)
			}
		})
	}
}

func TestLoad_MissingFile(t *testing.T) {
	t.Parallel()

	_, _, err := testLoader(nil, nil).Load("absent.yaml")
	if err == nil {
		t.Fatal("expected an error for a missing config file")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("error should say the file does not exist, got: %v", err)
	}
}

func TestLoad_MalformedYAML(t *testing.T) {
	t.Parallel()

	files := map[string]string{"kss.yaml": "target:\n  namespace: [unclosed\n"}
	_, _, err := testLoader(files, nil).Load("kss.yaml")
	if err == nil {
		t.Fatal("expected a parse error")
	}
	if !strings.Contains(err.Error(), "parsing config file") {
		t.Errorf("error should name the parsing stage, got: %v", err)
	}
}

func TestLoad_BareDurationIsRejected(t *testing.T) {
	t.Parallel()

	// "interval: 15" almost certainly means seconds, but time.Duration would
	// read it as 15ns. Guessing either way is worse than refusing.
	files := map[string]string{"kss.yaml": minimalConfig + "\ncontroller:\n  interval: 15\n"}

	_, _, err := testLoader(files, nil).Load("kss.yaml")
	if err == nil {
		t.Fatal("a bare number must not be accepted as a duration")
	}
	if !strings.Contains(err.Error(), "duration") {
		t.Errorf("error should mention the expected duration form, got: %v", err)
	}
}

func TestLoad_NoFileUsesDefaultsAndEnv(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"KSS_TARGET_NAMESPACE":       "data-pipeline",
		"KSS_TARGET_DEPLOYMENT":      "normalizer",
		"KSS_WORKLOAD_SIGNAL_SOURCE": "none",
	}

	cfg, src, err := testLoader(nil, env).Load("")
	if err != nil {
		t.Fatalf("defaults plus env must be a valid source of config, got: %v", err)
	}
	if cfg.Target.Namespace != "data-pipeline" {
		t.Errorf("namespace = %q, want data-pipeline", cfg.Target.Namespace)
	}
	if !strings.HasPrefix(src.String(), "defaults") {
		t.Errorf("source should report defaults, got %q", src.String())
	}
}

// The synthetic source reads a scripted series from disk. If that file is
// absent the controller would start and then report pressure as permanently
// unavailable, which looks like a metrics outage rather than a typo.
func TestLoad_SyntheticSourceRequiresReadableFile(t *testing.T) {
	t.Parallel()

	files := map[string]string{"kss.yaml": minimalConfig} // pressure file absent

	_, _, err := testLoader(files, nil).Load("kss.yaml")
	if err == nil {
		t.Fatal("synthetic source with an unreadable path must be rejected")
	}

	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if !hasKey(verr, "workload.signal.syntheticPath") {
		t.Errorf("expected a problem on the synthetic path, got %v", verr.Keys())
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("error should say the file is missing, got: %v", err)
	}
}

func TestLoad_NonSyntheticSourceSkipsFileCheck(t *testing.T) {
	t.Parallel()

	files := map[string]string{
		"kss.yaml": minimalConfig + "\nworkload:\n  signal:\n    source: none\n",
	}

	if _, _, err := testLoader(files, nil).Load("kss.yaml"); err != nil {
		t.Fatalf("a non-synthetic source must not require the pressure file, got: %v", err)
	}
}

func TestLoad_RejectsMultipleDocuments(t *testing.T) {
	t.Parallel()

	files := map[string]string{
		"kss.yaml":                          minimalConfig + "\n---\ntarget:\n  namespace: other\n  deployment: other\n",
		"/etc/kubescalesense/pressure.yaml": "series: []",
	}

	_, _, err := testLoader(files, nil).Load("kss.yaml")
	if err == nil {
		t.Fatal("a second YAML document would be silently dropped; it must be rejected")
	}
	if !strings.Contains(err.Error(), "more than one YAML document") {
		t.Errorf("unexpected error: %v", err)
	}
}

// The committed config/kubescalesense.yaml is the documented schema in
// executable form, so it must parse strictly and validate.
func TestLoad_ShippedConfigFileIsValid(t *testing.T) {
	t.Parallel()

	loader := &Loader{
		// Only the pressure-file probe is faked: in-cluster it is mounted from
		// a ConfigMap, and it is deliberately absent on a developer machine.
		OpenFile: func(string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("series: []")), nil
		},
		Getenv:  func(string) string { return "" },
		Environ: func() []string { return nil },
	}

	cfg, _, err := loader.Load("../../config/kubescalesense.yaml")
	if err != nil {
		t.Fatalf("the shipped config file must load and validate, got: %v", err)
	}

	// Spot-check that it really is the documented default set rather than a
	// file that merely happens to parse.
	want := Default()
	want.Target.Namespace = "data-pipeline"
	want.Target.Deployment = "normalizer"

	if *cfg != *want {
		t.Errorf("config/kubescalesense.yaml has drifted from the documented defaults\n got: %+v\nwant: %+v", *cfg, *want)
	}
}
