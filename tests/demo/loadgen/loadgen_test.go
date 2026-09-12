package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/normalizer"
)

// Determinism is the generator's whole value: a scenario that sends different
// records on every run produces measurements that cannot be compared.
func TestRecord_IsDeterministic(t *testing.T) {
	t.Parallel()

	for _, n := range []int{0, 1, 7, 1000, 99999} {
		first := record(n, 0)
		if second := record(n, 0); first != second {
			t.Errorf("record(%d) changed between calls: %q vs %q", n, first, second)
		}
	}
}

// Every generated record must be one the Normalizer accepts. A generator that
// emits records the workload rejects would drive a demo entirely out of 400s,
// and the pressure signal would never rise.
func TestRecord_IsAlwaysAcceptedByTheNormalizer(t *testing.T) {
	t.Parallel()

	for n := range 500 {
		for _, pad := range []int{0, 1, 64} {
			raw := record(n, pad)
			if _, err := normalizer.Normalize([]byte(raw)); err != nil {
				t.Fatalf("record(%d, %d) = %q is not a valid record: %v", n, pad, raw, err)
			}
		}
	}
}

func TestRecord_DistinctIndicesGiveDistinctRecords(t *testing.T) {
	t.Parallel()

	seen := make(map[string]int, 200)
	for n := range 200 {
		raw := record(n, 0)
		if previous, clash := seen[raw]; clash {
			t.Errorf("record(%d) and record(%d) are identical: %q", previous, n, raw)
		}
		seen[raw] = n
	}
}

// Padding must change the record's size and nothing else about its validity.
func TestRecord_PadControlsPayloadSize(t *testing.T) {
	t.Parallel()

	plain := record(3, 0)
	padded := record(3, 100)

	if len(padded) != len(plain)+101 { // 100 filler characters plus the separator
		t.Errorf("padded length = %d, plain = %d; pad should add exactly the requested bytes",
			len(padded), len(plain))
	}
	if _, err := normalizer.Normalize([]byte(padded)); err != nil {
		t.Errorf("a padded record must still be valid: %v", err)
	}
}

func TestFile_HoldsOneRecordPerLine(t *testing.T) {
	t.Parallel()

	contents := file(0, 10, 0)
	lines := strings.Split(strings.TrimRight(contents, "\n"), "\n")

	if len(lines) != 10 {
		t.Fatalf("got %d lines, want 10", len(lines))
	}
	for i, line := range lines {
		if _, err := normalizer.Normalize([]byte(line)); err != nil {
			t.Errorf("line %d (%q) is not a valid record: %v", i, line, err)
		}
	}
	if !strings.HasSuffix(contents, "\n") {
		t.Error("the file must end with a newline so the last record is not truncated")
	}
}

func TestPlan_Validate(t *testing.T) {
	t.Parallel()

	if err := DefaultPlan().Validate(); err != nil {
		t.Fatalf("the default plan must be valid: %v", err)
	}

	for _, tc := range []struct {
		name    string
		mutate  func(*Plan)
		wantMsg string
	}{
		{name: "unknown profile", mutate: func(p *Plan) { p.Profile = "sawtooth" }, wantMsg: "profile"},
		{name: "no rate", mutate: func(p *Plan) { p.LowRate = 0 }, wantMsg: "low-rate"},
		{
			name: "a spike that is not a spike",
			mutate: func(p *Plan) {
				p.Profile = ProfileLowHighLow
				p.HighRate = p.LowRate
			},
			wantMsg: "not a spike",
		},
		{
			name: "no phase duration",
			mutate: func(p *Plan) {
				p.Profile = ProfileLowHighLow
				p.Phase = 0
			},
			wantMsg: "phase",
		},
		{name: "negative records", mutate: func(p *Plan) { p.Records = -1 }, wantMsg: "records"},
		{name: "no burst", mutate: func(p *Plan) { p.Burst = 0 }, wantMsg: "burst"},
		{name: "negative pad", mutate: func(p *Plan) { p.Pad = -1 }, wantMsg: "pad"},
		{name: "no records per file", mutate: func(p *Plan) { p.RecordsPerFile = 0 }, wantMsg: "records-per-file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			plan := DefaultPlan()
			tc.mutate(&plan)

			err := plan.Validate()
			if err == nil {
				t.Fatalf("%+v was accepted", plan)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("error = %q, want it to mention %q", err, tc.wantMsg)
			}
		})
	}
}

// The spike shape is the demo's script, so it is asserted without sending
// anything: three phases, quiet then loud then quiet.
func TestPlan_LowHighLowHasThreePhases(t *testing.T) {
	t.Parallel()

	plan := DefaultPlan()
	plan.Profile = ProfileLowHighLow
	plan.LowRate = 5
	plan.HighRate = 80
	plan.Phase = 30 * time.Second

	phases := plan.phases()
	if len(phases) != 3 {
		t.Fatalf("got %d phases, want 3", len(phases))
	}
	if phases[0].rate != 5 || phases[1].rate != 80 || phases[2].rate != 5 {
		t.Errorf("phase rates = %g, %g, %g; want 5, 80, 5", phases[0].rate, phases[1].rate, phases[2].rate)
	}
	for i, phase := range phases {
		if phase.duration != 30*time.Second {
			t.Errorf("phase %d duration = %s, want 30s", i, phase.duration)
		}
	}
}

func TestPlan_ConstantIsOneOpenEndedPhase(t *testing.T) {
	t.Parallel()

	phases := DefaultPlan().phases()
	if len(phases) != 1 {
		t.Fatalf("got %d phases, want 1", len(phases))
	}
	if phases[0].duration != 0 {
		t.Errorf("duration = %s, want 0 meaning open-ended", phases[0].duration)
	}
}

// The interval must account for the burst size, or "80 records per second in
// bursts of 10" would send 800.
func TestPlan_IntervalAccountsForBurstSize(t *testing.T) {
	t.Parallel()

	plan := DefaultPlan()
	plan.Burst = 10

	if got := plan.interval(80); got != 125*time.Millisecond {
		t.Errorf("interval = %s, want 125ms (eight bursts of ten per second)", got)
	}

	plan.Burst = 1
	if got := plan.interval(4); got != 250*time.Millisecond {
		t.Errorf("interval = %s, want 250ms", got)
	}
}

// -records must be an exact count, because the conservation assertion at the
// end of a data-integrity scenario compares it against the output.
func TestRun_SendsExactlyTheRequestedRecordCount(t *testing.T) {
	t.Parallel()

	var received atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if _, err := normalizer.Normalize(body); err != nil {
			t.Errorf("the generator sent an invalid record %q: %v", body, err)
		}
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	plan := DefaultPlan()
	plan.LowRate = 2000 // fast, so the test is quick
	plan.Records = 57   // deliberately not a multiple of the burst size
	plan.Burst = 10

	stats := &Stats{}
	target := newHTTPSink(server.URL, plan, 8, 5*time.Second, stats)

	if err := Run(context.Background(), discardLogger(), plan, target, stats); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := target.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got := received.Load(); got != 57 {
		t.Errorf("the server received %d records, want exactly 57", got)
	}
	if got := stats.Records.Load(); got != 57 {
		t.Errorf("stats report %d records, want 57", got)
	}
	if got := stats.Failures.Load(); got != 0 {
		t.Errorf("stats report %d failures, want 0", got)
	}
}

// A rejection is counted, not fatal: a 503 from a saturated pool is the
// expected answer under load, and a generator that gave up on the first one
// could never drive an overload scenario.
func TestRun_CountsRejectionsAndKeepsGoing(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "queue is full", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	plan := DefaultPlan()
	plan.LowRate = 2000
	plan.Records = 20
	plan.Burst = 5

	stats := &Stats{}
	target := newHTTPSink(server.URL, plan, 4, 5*time.Second, stats)

	if err := Run(context.Background(), discardLogger(), plan, target, stats); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := target.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got := stats.Failures.Load(); got != 20 {
		t.Errorf("failures = %d, want 20", got)
	}
	if got := stats.Records.Load(); got != 0 {
		t.Errorf("records = %d, want 0 successful", got)
	}
}

// Files must appear atomically. A half-written file picked up by NiFi's
// ListFile would be split into a truncated final record, which looks exactly
// like data corruption and is entirely the writer's fault.
func TestFileSink_PublishesFilesAtomically(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	plan := DefaultPlan()
	plan.LowRate = 1000
	plan.Records = 4
	plan.Burst = 1
	plan.RecordsPerFile = 25

	stats := &Stats{}
	target, err := newFileSink(dir, plan, stats)
	if err != nil {
		t.Fatalf("newFileSink: %v", err)
	}

	if runErr := Run(context.Background(), discardLogger(), plan, target, stats); runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}

	var records int
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".partial") {
			t.Errorf("%s was left behind; a staging file visible to ListFile defeats the atomic rename", entry.Name())
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".csv") {
			t.Errorf("unexpected file %s", entry.Name())
			continue
		}

		contents, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", entry.Name(), err)
		}
		lines := strings.Split(strings.TrimRight(string(contents), "\n"), "\n")
		if len(lines) != 25 {
			t.Errorf("%s has %d records, want 25", entry.Name(), len(lines))
		}
		records += len(lines)
	}

	if records != 100 {
		t.Errorf("wrote %d records across %d files, want 100", records, len(entries))
	}
	if got := stats.Files.Load(); got != 4 {
		t.Errorf("stats report %d files, want 4", got)
	}
}

// The whole run must stop when the context ends, so that `kubectl delete` on a
// generator Job does not leave records arriving after the scenario is over.
func TestRun_StopsWhenTheContextEnds(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	plan := DefaultPlan()
	plan.LowRate = 50
	plan.Records = 0 // open-ended: only the context can stop it

	stats := &Stats{}
	target := newHTTPSink(server.URL, plan, 4, 5*time.Second, stats)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- Run(ctx, discardLogger(), plan, target, stats) }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil on cancellation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run ignored the cancelled context")
	}
	if err := target.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestRun_RejectsAnInvalidPlan(t *testing.T) {
	t.Parallel()

	plan := DefaultPlan()
	plan.Burst = 0

	if err := Run(context.Background(), discardLogger(), plan, nil, &Stats{}); err == nil {
		t.Error("Run accepted an invalid plan")
	}
}

func TestParse_Flags(t *testing.T) {
	t.Parallel()

	cfg, err := parse([]string{
		"-mode=file", "-dir=/tmp/in", "-records=100", "-rate=25",
		"-burst=5", "-pad=32", "-records-per-file=10",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.mode != "file" || cfg.dir != "/tmp/in" {
		t.Errorf("mode/dir = %q/%q", cfg.mode, cfg.dir)
	}
	if cfg.plan.Records != 100 || cfg.plan.LowRate != 25 || cfg.plan.Burst != 5 || cfg.plan.Pad != 32 {
		t.Errorf("plan = %+v", cfg.plan)
	}
}

func TestParse_RejectsBadInput(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"-mode=telepathy"},
		{"-concurrency=0"},
		{"-timeout=0s"},
		{"-burst=0"},
		{"-profile=sawtooth"},
		{"positional"},
		{"-nonexistent"},
	} {
		if _, err := parse(args, io.Discard); err == nil {
			t.Errorf("parse(%v) was accepted", args)
		}
	}
}

func TestPlanFromQuery(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost,
		"/spike?profile=low-high-low&rate=3&high-rate=90&phase=45s&records=500&burst=8&pad=16", nil)

	plan, err := planFromQuery(DefaultPlan(), request)
	if err != nil {
		t.Fatalf("planFromQuery: %v", err)
	}

	if plan.Profile != ProfileLowHighLow || plan.LowRate != 3 || plan.HighRate != 90 {
		t.Errorf("plan = %+v", plan)
	}
	if plan.Phase != 45*time.Second || plan.Records != 500 || plan.Burst != 8 || plan.Pad != 16 {
		t.Errorf("plan = %+v", plan)
	}
}

func TestPlanFromQuery_RejectsUnusableParameters(t *testing.T) {
	t.Parallel()

	for _, query := range []string{
		"?rate=fast",
		"?records=many",
		"?phase=soon",
		"?profile=sawtooth",
		"?burst=0",
	} {
		request := httptest.NewRequest(http.MethodPost, "/spike"+query, nil)
		if _, err := planFromQuery(DefaultPlan(), request); err == nil {
			t.Errorf("planFromQuery(%q) was accepted", query)
		}
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}
