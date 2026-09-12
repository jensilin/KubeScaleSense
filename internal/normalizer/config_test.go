package normalizer

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultConfig_IsValid(t *testing.T) {
	t.Parallel()

	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("the shipped defaults must validate, got: %v", err)
	}
}

func TestLoadConfig_AppliesEveryOverride(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"NORMALIZER_ADDR":                ":9000",
		"NORMALIZER_ADMIN_ADDR":          ":9001",
		"NORMALIZER_COST_ROUNDS":         "12345",
		"NORMALIZER_PROCESSING_DELAY":    "250ms",
		"NORMALIZER_MAX_CONCURRENT":      "8",
		"NORMALIZER_QUEUE_LIMIT":         "128",
		"NORMALIZER_QUEUE_TIMEOUT":       "7s",
		"NORMALIZER_MAX_BODY_BYTES":      "4096",
		"NORMALIZER_READ_HEADER_TIMEOUT": "2s",
		"NORMALIZER_DRAIN_DELAY":         "4s",
		"NORMALIZER_SHUTDOWN_TIMEOUT":    "40s",
	}

	cfg, err := LoadConfig(fakeEnv(env))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	want := Config{
		Addr:              ":9000",
		AdminAddr:         ":9001",
		CostRounds:        12345,
		ProcessingDelay:   250 * time.Millisecond,
		MaxConcurrent:     8,
		QueueLimit:        128,
		QueueTimeout:      7 * time.Second,
		MaxBodyBytes:      4096,
		ReadHeaderTimeout: 2 * time.Second,
		DrainDelay:        4 * time.Second,
		ShutdownTimeout:   40 * time.Second,
	}
	if cfg != want {
		t.Errorf("config =\n %+v\nwant\n %+v", cfg, want)
	}

	// Every field must be reachable from the environment, or a ConfigMap change
	// silently does nothing and the demo is retuned by rebuilding an image.
	if cfg == DefaultConfig() {
		t.Error("no override was applied")
	}
}

func TestLoadConfig_UnsetVariablesKeepTheDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := LoadConfig(fakeEnv(map[string]string{"NORMALIZER_MAX_CONCURRENT": "2"}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.MaxConcurrent != 2 {
		t.Errorf("MaxConcurrent = %d, want 2", cfg.MaxConcurrent)
	}
	if cfg.Addr != DefaultAddr {
		t.Errorf("Addr = %q, want the default %q", cfg.Addr, DefaultAddr)
	}
}

// An empty variable is treated as unset. A ConfigMap key with no value is a
// half-finished edit, and inheriting the default is safer than parsing "" as
// zero — which for MAX_CONCURRENT would be a pod that accepts records it can
// never process.
func TestLoadConfig_TreatsAnEmptyVariableAsUnset(t *testing.T) {
	t.Parallel()

	cfg, err := LoadConfig(fakeEnv(map[string]string{"NORMALIZER_MAX_CONCURRENT": "  "}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.MaxConcurrent != DefaultMaxConcurrent {
		t.Errorf("MaxConcurrent = %d, want the default %d", cfg.MaxConcurrent, DefaultMaxConcurrent)
	}
}

func TestLoadConfig_RejectsUnparsableValues(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		env     map[string]string
		wantMsg string
	}{
		{
			name:    "cost rounds is not a number",
			env:     map[string]string{"NORMALIZER_COST_ROUNDS": "lots"},
			wantMsg: `NORMALIZER_COST_ROUNDS="lots" is not an integer`,
		},
		{
			name:    "body limit is not a number",
			env:     map[string]string{"NORMALIZER_MAX_BODY_BYTES": "64Ki"},
			wantMsg: "is not an integer",
		},
		{
			name:    "a duration without a unit",
			env:     map[string]string{"NORMALIZER_QUEUE_TIMEOUT": "10"},
			wantMsg: "is not a duration",
		},
		{
			name:    "a nonsense duration",
			env:     map[string]string{"NORMALIZER_PROCESSING_DELAY": "soon"},
			wantMsg: "is not a duration",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := LoadConfig(fakeEnv(tc.env))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("error = %q, want it to contain %q", err, tc.wantMsg)
			}
		})
	}
}

// Every one of these mistakes produces a service that looks healthy and
// behaves wrongly, so each must be a refusal to start rather than a warning.
func TestValidate_RejectsAConfigurationThatCannotBeHonoured(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		mutate  func(*Config)
		wantMsg string
	}{
		{
			name:    "no data address",
			mutate:  func(c *Config) { c.Addr = "" },
			wantMsg: "NORMALIZER_ADDR must not be empty",
		},
		{
			name:    "no admin address",
			mutate:  func(c *Config) { c.AdminAddr = "" },
			wantMsg: "NORMALIZER_ADMIN_ADDR must not be empty",
		},
		{
			name:    "both planes on one port",
			mutate:  func(c *Config) { c.AdminAddr = c.Addr },
			wantMsg: "separate ports",
		},
		{
			name:    "negative cost",
			mutate:  func(c *Config) { c.CostRounds = -1 },
			wantMsg: "COST_ROUNDS must not be negative",
		},
		{
			name:    "negative delay",
			mutate:  func(c *Config) { c.ProcessingDelay = -time.Second },
			wantMsg: "PROCESSING_DELAY must not be negative",
		},
		{
			name:    "no processing slots",
			mutate:  func(c *Config) { c.MaxConcurrent = 0 },
			wantMsg: "records it can never process",
		},
		{
			name:    "negative queue limit",
			mutate:  func(c *Config) { c.QueueLimit = -1 },
			wantMsg: "QUEUE_LIMIT must not be negative",
		},
		{
			name:    "no queue timeout",
			mutate:  func(c *Config) { c.QueueTimeout = 0 },
			wantMsg: "retried by NiFi while still queued",
		},
		{
			name:    "no body limit",
			mutate:  func(c *Config) { c.MaxBodyBytes = 0 },
			wantMsg: "MAX_BODY_BYTES must be at least 1",
		},
		{
			name:    "no header timeout",
			mutate:  func(c *Config) { c.ReadHeaderTimeout = 0 },
			wantMsg: "READ_HEADER_TIMEOUT must be greater than 0",
		},
		{
			name:    "negative drain delay",
			mutate:  func(c *Config) { c.DrainDelay = -time.Second },
			wantMsg: "DRAIN_DELAY must not be negative",
		},
		{
			name:    "no shutdown budget",
			mutate:  func(c *Config) { c.ShutdownTimeout = 0 },
			wantMsg: "SHUTDOWN_TIMEOUT must be greater than 0",
		},
		{
			// The drain would consume the whole budget and leave nothing for the
			// in-flight records it exists to protect.
			name: "the drain delay swallows the shutdown budget",
			mutate: func(c *Config) {
				c.DrainDelay = 30 * time.Second
				c.ShutdownTimeout = 10 * time.Second
			},
			wantMsg: "must be shorter than",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := DefaultConfig()
			tc.mutate(&cfg)

			err := cfg.Validate()
			if err == nil {
				t.Fatalf("%+v was accepted", cfg)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("error = %q, want it to contain %q", err, tc.wantMsg)
			}
		})
	}
}

// Every problem is reported at once: fixing a ConfigMap one restart at a time
// is a slow way to learn about three mistakes.
func TestValidate_ReportsEveryProblemTogether(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.MaxConcurrent = 0
	cfg.QueueTimeout = 0
	cfg.MaxBodyBytes = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"MAX_CONCURRENT", "QUEUE_TIMEOUT", "MAX_BODY_BYTES"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func fakeEnv(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}
