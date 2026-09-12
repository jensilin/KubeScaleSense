package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/normalizer"
)

const sampleRecord = "2026-03-14T12:00:00Z,epdg-01,pdp.sessions.active,4821"

func TestRun_Version(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := run([]string{"-version"}, emptyEnv, &stdout, &stderr); code != 0 {
		t.Errorf("exit code = %d, want 0; stderr = %q", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) == "" {
		t.Error("-version printed nothing")
	}
}

func TestRun_Help(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	if code := run([]string{"-h"}, emptyEnv, &stdout, &stderr); code != 0 {
		t.Errorf("exit code = %d, want 0 for -h", code)
	}
}

// -validate is how a ConfigMap change is checked before it is rolled out to a
// pod that would crash-loop on it.
func TestRun_ValidateAcceptsAGoodConfiguration(t *testing.T) {
	t.Parallel()

	env := fakeEnv(map[string]string{
		"NORMALIZER_MAX_CONCURRENT": "6",
		"NORMALIZER_COST_ROUNDS":    "1000",
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"-validate"}, env, &stdout, &stderr); code != 0 {
		t.Errorf("exit code = %d, want 0; stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "configuration is valid") {
		t.Errorf("stdout = %q, want the validation to be reported", stdout.String())
	}
}

func TestRun_RejectsABadConfigurationBeforeListening(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{name: "no processing slots", env: map[string]string{"NORMALIZER_MAX_CONCURRENT": "0"}},
		{name: "unparsable duration", env: map[string]string{"NORMALIZER_QUEUE_TIMEOUT": "soon"}},
		{name: "both planes on one port", env: map[string]string{
			"NORMALIZER_ADDR":       ":8080",
			"NORMALIZER_ADMIN_ADDR": ":8080",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var stdout, stderr bytes.Buffer
			code := run([]string{"-validate"}, fakeEnv(tc.env), &stdout, &stderr)

			if code != 2 {
				t.Errorf("exit code = %d, want 2", code)
			}
			// The message goes to stderr as plain text: it is the first line a
			// human reads from `kubectl logs` on a crash-looping pod.
			if !strings.Contains(stderr.String(), "invalid configuration") {
				t.Errorf("stderr = %q, want it to explain the problem", stderr.String())
			}
		})
	}
}

func TestRun_RejectsUnknownArguments(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{{"-nonsense"}, {"positional"}} {
		var stdout, stderr bytes.Buffer
		if code := run(args, emptyEnv, &stdout, &stderr); code != 2 {
			t.Errorf("run(%v) exit code = %d, want 2", args, code)
		}
	}
}

// The full lifecycle: both planes listen, a record is normalized end to end,
// and a cancelled context drains and returns cleanly.
func TestServe_ListensNormalizesAndDrains(t *testing.T) {
	t.Parallel()

	cfg := normalizer.DefaultConfig()
	cfg.CostRounds = 8
	cfg.Addr = "127.0.0.1:" + strconv.Itoa(freePort(t))
	cfg.AdminAddr = "127.0.0.1:" + strconv.Itoa(freePort(t))
	cfg.DrainDelay = 10 * time.Millisecond
	cfg.ShutdownTimeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() {
		stopped <- serve(ctx, slog.New(slog.NewJSONHandler(io.Discard, nil)), cfg)
	}()

	waitForReady(t, "http://"+cfg.AdminAddr+"/readyz")

	// The data plane does the actual work.
	response, err := http.Post("http://"+cfg.Addr+"/normalize", "text/plain", strings.NewReader(sampleRecord))
	if err != nil {
		t.Fatalf("POST /normalize: %v", err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %q", response.StatusCode, body)
	}
	if !strings.Contains(string(body), `"checksum"`) {
		t.Errorf("body = %q, want a normalized record", body)
	}

	// The pressure signal must be scrapeable from the admin plane, because that
	// is the controller's only view of this workload.
	exposition := getBody(t, "http://"+cfg.AdminAddr+"/metrics")
	for _, want := range []string{
		"normalizer_requests_queued",
		"normalizer_requests_in_flight",
		"normalizer_requests_received_total",
		"normalizer_requests_completed_total",
		"normalizer_request_duration_seconds",
	} {
		if !strings.Contains(exposition, want) {
			t.Errorf("the exposition is missing %s, which the controller's http signal source requires", want)
		}
	}

	cancel()

	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("serve returned %v, want a clean shutdown", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("serve did not return after the context was cancelled")
	}

	// The listener must actually be closed, or a terminating pod would keep
	// accepting work it has promised to stop taking.
	if _, err := http.Get("http://" + cfg.AdminAddr + "/healthz"); err == nil {
		t.Error("the admin plane is still accepting connections after shutdown")
	}
}

// A port clash must be fatal and immediate. A Normalizer that is running but
// not listening would pass its liveness probe while serving nothing.
func TestServe_ABindFailureIsFatal(t *testing.T) {
	t.Parallel()

	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	defer func() { _ = occupied.Close() }()

	cfg := normalizer.DefaultConfig()
	cfg.Addr = occupied.Addr().String()
	cfg.AdminAddr = "127.0.0.1:" + strconv.Itoa(freePort(t))
	cfg.DrainDelay = 0
	cfg.ShutdownTimeout = time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err = serve(ctx, slog.New(slog.NewJSONHandler(io.Discard, nil)), cfg)
	if err == nil {
		t.Fatal("serve returned nil after failing to bind the data plane")
	}
	if !strings.Contains(err.Error(), cfg.Addr) {
		t.Errorf("error = %v, want it to name the address that could not be bound", err)
	}
}

func TestServe_RejectsAnInvalidConfiguration(t *testing.T) {
	t.Parallel()

	err := serve(context.Background(), slog.New(slog.NewJSONHandler(io.Discard, nil)), normalizer.Config{})
	if err == nil {
		t.Fatal("serve accepted an empty configuration")
	}
}

// --- helpers ---------------------------------------------------------------

func emptyEnv(string) string { return "" }

func fakeEnv(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func freePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probing for a free port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("closing the probe listener: %v", err)
	}
	return port
}

func waitForReady(t *testing.T, url string) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(url)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never became ready", url)
}

func getBody(t *testing.T, url string) string {
	t.Helper()

	response, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", url, err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d: %s", url, response.StatusCode, body)
	}
	return string(body)
}
