package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/config"
)

func TestRun_Version(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if code := run([]string{"-version"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("exit code = %d, want %d; stderr: %s", code, exitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), AppName) {
		t.Errorf("version output should name the app, got %q", stdout.String())
	}
}

func TestRun_PrintEnv(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if code := run([]string{"-print-env"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("exit code = %d, want %d; stderr: %s", code, exitOK, stderr.String())
	}

	out := stdout.String()
	// The validation error for an unrecognised override points operators at
	// this flag, so it must actually list the variables.
	for _, want := range []string{"KSS_CONTROLLER_INTERVAL", "KSS_TARGET_MAXREPLICAS", "KSS_SCALING_SCALEUPCOOLDOWN"} {
		if !strings.Contains(out, want) {
			t.Errorf("-print-env output missing %s", want)
		}
	}
}

func TestRun_InvalidFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if code := run([]string{"-nonsense"}, &stdout, &stderr); code != exitConfigInvalid {
		t.Fatalf("exit code = %d, want %d", code, exitConfigInvalid)
	}
}

func TestRun_UnexpectedPositionalArgument(t *testing.T) {
	var stdout, stderr bytes.Buffer

	if code := run([]string{"serve"}, &stdout, &stderr); code != exitConfigInvalid {
		t.Fatalf("exit code = %d, want %d", code, exitConfigInvalid)
	}
	if !strings.Contains(stderr.String(), "flags only") {
		t.Errorf("stderr should explain the usage, got %q", stderr.String())
	}
}

// A missing or invalid config must fail fast with a non-zero exit (CR-2), and
// the message must reach stderr in plain text: the structured logger is
// configured by the very config that failed, and a human is reading this from
// a CrashLoopBackOff.
func TestRun_InvalidConfigFailsFast(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("target:\n  namespace: ns\n  deployment: d\n  minReplicas: 9\n  maxReplicas: 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{"-config", path}, &stdout, &stderr)

	if code != exitConfigInvalid {
		t.Fatalf("exit code = %d, want %d", code, exitConfigInvalid)
	}
	msg := stderr.String()
	if !strings.Contains(msg, "target.minReplicas") {
		t.Errorf("stderr should name the offending key, got %q", msg)
	}
	if stdout.Len() != 0 {
		t.Errorf("nothing should be written to stdout on a config failure, got %q", stdout.String())
	}
}

func TestRun_MissingConfigFile(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := run([]string{"-config", filepath.Join(t.TempDir(), "absent.yaml")}, &stdout, &stderr)
	if code != exitConfigInvalid {
		t.Fatalf("exit code = %d, want %d", code, exitConfigInvalid)
	}
	if !strings.Contains(stderr.String(), "does not exist") {
		t.Errorf("stderr should say the file is missing, got %q", stderr.String())
	}
}

// -validate exits zero without starting, which is what makes it usable as a
// pre-deploy check and in CI.
func TestRun_ValidateOnly(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _ := writeValidConfig(t, dir)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"-config", cfgPath, "-validate"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("exit code = %d, want %d; stderr: %s", code, exitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), "configuration is valid") {
		t.Errorf("expected a confirmation line, got %q", stdout.String())
	}
}

// The startup log must carry the facts needed to reconstruct what the process
// is doing: name, version, config source, and dry-run state.
//
// Asserted against logStartup directly rather than by running the binary,
// because a Phase 1 process needs a reachable API server to get past startup and
// this contract is about the log line rather than about the cluster.
func TestLogStartup_EmitsRequiredFields(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _ := writeValidConfig(t, dir)

	cfg, src, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("loading the test config: %v", err)
	}

	var stdout bytes.Buffer
	logStartup(cfg.NewLogger(&stdout).With(slog.String("app", AppName)), cfg, src)

	records := parseJSONLogs(t, stdout.String())
	startup := findRecord(records, "KubeScaleSense observer started")
	if startup == nil {
		t.Fatalf("no startup record found in logs:\n%s", stdout.String())
	}

	if startup["app"] != AppName {
		t.Errorf("app = %v, want %s", startup["app"], AppName)
	}
	if startup["version"] != version {
		t.Errorf("version = %v, want %s", startup["version"], version)
	}
	if got, want := startup["config_source"], "file:"+cfgPath; got != want {
		t.Errorf("config_source = %v, want %v", got, want)
	}
	if startup["phase"] != Phase {
		t.Errorf("phase = %v, want %s", startup["phase"], Phase)
	}

	// Phase 1 is dry-run by construction, and the startup line is where an
	// operator confirms it before trusting the process near a live cluster.
	if startup["dry_run"] != true {
		t.Errorf("dry_run = %v, want true", startup["dry_run"])
	}

	// Nobody reading these logs should be able to conclude that the controller
	// is changing the replica count.
	if findRecord(records, "no scaling is performed in this phase") == nil {
		t.Error("startup should state plainly that no scaling happens in P1")
	}
}

func TestAwaitShutdown_ReturnsOnContextCancellation(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- awaitShutdown(ctx, log) }()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("awaitShutdown returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("awaitShutdown did not return within 5s of cancellation")
	}
}

func TestAwaitShutdown_DoesNotLeakGoroutines(t *testing.T) {
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	// Settle first: the test binary and slog may still be starting goroutines.
	before := stableGoroutineCount(t)

	for range 20 {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // already cancelled: awaitShutdown must return immediately
		if err := awaitShutdown(ctx, log); err != nil {
			t.Fatalf("awaitShutdown returned %v", err)
		}
	}

	after := stableGoroutineCount(t)
	if after > before {
		t.Errorf("goroutine count grew from %d to %d across 20 shutdown cycles", before, after)
	}
}

func TestShutdown_ReportsAnAlreadyExpiredBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := shutdown(ctx); err == nil {
		t.Fatal("an expired shutdown budget must be reported, not ignored")
	}
}

func TestShutdown_SucceedsWithBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown returned %v, want nil", err)
	}
}

// --- helpers ---

// writeValidConfig writes a minimal valid config plus the synthetic pressure
// file it references, and returns both paths.
func writeValidConfig(t *testing.T, dir string) (cfgPath, pressurePath string) {
	t.Helper()

	pressurePath = filepath.Join(dir, "pressure.yaml")
	if err := os.WriteFile(pressurePath, []byte("loop: true\nsamples:\n  - pressureItems: 10\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfgPath = filepath.Join(dir, "kubescalesense.yaml")
	body := "target:\n  namespace: data-pipeline\n  deployment: normalizer\n" +
		"workload:\n  signal:\n    syntheticPath: " + pressurePath + "\n"
	if err := os.WriteFile(cfgPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return cfgPath, pressurePath
}

func parseJSONLogs(t *testing.T, out string) []map[string]any {
	t.Helper()

	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not valid JSON (%v): %q", err, line)
		}
		records = append(records, rec)
	}
	return records
}

func findRecord(records []map[string]any, msg string) map[string]any {
	for _, rec := range records {
		if rec["msg"] == msg {
			return rec
		}
	}
	return nil
}

// stableGoroutineCount waits for the goroutine count to stop changing, so the
// leak check does not race against runtime or test-framework startup.
func stableGoroutineCount(t *testing.T) int {
	t.Helper()

	last := runtime.NumGoroutine()
	stable := 0
	for range 100 {
		time.Sleep(10 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == last {
			if stable++; stable >= 3 {
				return n
			}
			continue
		}
		last, stable = n, 0
	}
	return last
}
