package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/config"
	"github.com/jensilin/KubeScaleSense/internal/observability"
)

// The lease must be given up only by a process that has stopped acting under
// it. client-go releases the moment its context is cancelled, so the loop has
// to have returned before that.
func TestLead_ShutdownStopsTheLoopBeforeReleasingTheLease(t *testing.T) {
	t.Parallel()

	f := newFakeElector(true)
	ctx, shutdown := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- lead(ctx, f.leadership(), func(loopCtx context.Context) error {
			f.record("loop started")
			<-loopCtx.Done()
			// A reconcile finishing its write after the stop was requested.
			time.Sleep(20 * time.Millisecond)
			f.record("loop returned")
			return nil
		})
	}()

	f.waitFor(t, "loop started")
	shutdown()

	if err := wait(t, done); err != nil {
		t.Fatalf("lead on shutdown = %v, want nil", err)
	}
	if want := []string{"loop started", "loop returned", "released"}; !slices.Equal(f.log(), want) {
		t.Errorf("order = %v, want %v", f.log(), want)
	}
}

// Losing the lease mid-loop stops deciding at once and ends the process with
// errLostLeadership, rather than campaigning again with state from the lost
// term.
func TestLead_LosingTheLeaseStopsTheLoopAndReportsIt(t *testing.T) {
	t.Parallel()

	f := newFakeElector(true)
	done := make(chan error, 1)
	go func() {
		done <- lead(context.Background(), f.leadership(), func(loopCtx context.Context) error {
			f.record("loop started")
			<-loopCtx.Done()
			f.record("loop returned")
			return nil
		})
	}()

	f.waitFor(t, "loop started")
	f.loseLease()

	if err := wait(t, done); !errors.Is(err, errLostLeadership) {
		t.Fatalf("lead after losing the lease = %v, want errLostLeadership", err)
	}
	if want := []string{"loop started", "lost", "loop returned"}; !slices.Equal(f.log(), want) {
		t.Errorf("order = %v, want %v", f.log(), want)
	}
}

// A standby that is shut down never decided anything, and leaves cleanly.
func TestLead_StandbyShutdownNeverRunsTheLoop(t *testing.T) {
	t.Parallel()

	f := newFakeElector(false)
	ctx, shutdown := context.WithCancel(context.Background())

	var ran atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- lead(ctx, f.leadership(), func(context.Context) error {
			ran.Store(true)
			return nil
		})
	}()

	f.waitFor(t, "campaigning")
	shutdown()

	if err := wait(t, done); err != nil {
		t.Fatalf("standby lead on shutdown = %v, want nil", err)
	}
	if ran.Load() {
		t.Error("a standby that never acquired the lease ran the reconcile loop")
	}
	if want := []string{"campaigning", "released"}; !slices.Equal(f.log(), want) {
		t.Errorf("order = %v, want %v", f.log(), want)
	}
}

// An elector that stops by itself without granting leadership leaves nothing to
// lead, and must not leave the process standing by forever.
func TestLead_AnElectorThatGivesUpIsAnError(t *testing.T) {
	t.Parallel()

	f := newFakeElector(false)
	f.giveUp = true

	err := lead(context.Background(), f.leadership(), func(context.Context) error {
		t.Error("the loop ran without the lease")
		return nil
	})
	if err == nil || errors.Is(err, errLostLeadership) {
		t.Fatalf("lead with an elector that gave up = %v, want a distinct error", err)
	}
}

// A loop failing on its own still releases the lease, so a standby takes over
// without waiting out the lease duration, and its error is the one reported.
func TestLead_LoopErrorReleasesTheLease(t *testing.T) {
	t.Parallel()

	f := newFakeElector(true)
	boom := errors.New("fatal reconcile error")

	err := lead(context.Background(), f.leadership(), func(context.Context) error {
		f.record("loop failed")
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("lead = %v, want the loop's error", err)
	}
	if want := []string{"loop failed", "released"}; !slices.Equal(f.log(), want) {
		t.Errorf("order = %v, want %v", f.log(), want)
	}
}

// A standby is ready once synced, so a rolling update can proceed; the leader
// is ready on the controller's own terms.
func TestStandbyReady(t *testing.T) {
	t.Parallel()

	leaderErr := errors.New("no reconcile yet")
	leaderReady := func() error { return leaderErr }

	for _, tc := range []struct {
		name             string
		leading, synced  bool
		wantErr, wantLdr bool
	}{
		{name: "standby, synced", synced: true},
		{name: "standby, not synced", wantErr: true},
		{name: "leader defers to the controller", leading: true, synced: true, wantErr: true, wantLdr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := standbyReady(
				func() bool { return tc.leading },
				func() bool { return tc.synced },
				leaderReady,
			)()
			if (err != nil) != tc.wantErr {
				t.Fatalf("ready = %v, want error: %v", err, tc.wantErr)
			}
			if errors.Is(err, leaderErr) != tc.wantLdr {
				t.Errorf("ready = %v, want the controller's own readiness: %v", err, tc.wantLdr)
			}
		})
	}
}

// -once never writes, whatever the configuration says.
func TestForceReadOnlyForOnce(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name               string
		once, dryRun       bool
		wantForced, wantDR bool
	}{
		{name: "-once with a live config", once: true, wantForced: true, wantDR: true},
		{name: "-once already dry-run", once: true, dryRun: true, wantDR: true},
		{name: "a live loop is left alone"},
		{name: "a dry-run loop is left alone", dryRun: true, wantDR: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := config.Default()
			cfg.Controller.DryRun = tc.dryRun
			if got := forceReadOnlyForOnce(cfg, options{once: tc.once}); got != tc.wantForced {
				t.Errorf("forced = %v, want %v", got, tc.wantForced)
			}
			if cfg.Controller.DryRun != tc.wantDR {
				t.Errorf("dryRun = %v, want %v", cfg.Controller.DryRun, tc.wantDR)
			}
		})
	}
}

// The election follows the write clients: dry-run and a disabled election build
// no lease client at all, which a kubeconfig that does not exist proves.
func TestBuildLeadership_OnlyALiveProcessCampaigns(t *testing.T) {
	t.Parallel()

	opts := options{kubeconfigPath: filepath.Join(t.TempDir(), "does-not-exist")}
	newCfg := func(dryRun, leaderElection bool) *config.Config {
		cfg := config.Default()
		cfg.Target.Namespace = "data-pipeline"
		cfg.Target.Deployment = "normalizer"
		cfg.Controller.DryRun = dryRun
		cfg.Controller.LeaderElection = leaderElection
		return cfg
	}

	t.Run("dry-run", func(t *testing.T) {
		t.Parallel()
		l, err := buildLeadership(newCfg(true, true), opts, observability.NewMetrics(), discard())
		if err != nil || l != nil {
			t.Fatalf("buildLeadership in dry-run = %v, %v; want nil, nil", l, err)
		}
	})

	t.Run("disabled, live", func(t *testing.T) {
		t.Parallel()
		var out bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&out, nil))
		l, err := buildLeadership(newCfg(false, false), opts, observability.NewMetrics(), log)
		if err != nil || l != nil {
			t.Fatalf("buildLeadership with leaderElection: false = %v, %v; want nil, nil", l, err)
		}
		warning := findRecord(parseJSONLogs(t, out.String()),
			"leader election is disabled: run exactly one replica of this controller")
		if warning == nil || warning["level"] != "WARN" {
			t.Errorf("no warning that only one replica may run, got %q", out.String())
		}
	})

	t.Run("live", func(t *testing.T) {
		t.Parallel()
		if _, err := buildLeadership(newCfg(false, true), opts, observability.NewMetrics(), discard()); err == nil {
			t.Error("buildLeadership in live mode accepted a kubeconfig that does not exist, so it cannot have " +
				"tried to build a lease client")
		}
	})
}

// fakeElector stands in for client-go's LeaderElector: it grants leadership with
// a context derived from its own, and on cancellation releases.
type fakeElector struct {
	grant, giveUp bool
	acquired      chan context.Context
	lose          chan struct{}

	mu     sync.Mutex
	events []string
}

func newFakeElector(grant bool) *fakeElector {
	return &fakeElector{grant: grant, acquired: make(chan context.Context, 1), lose: make(chan struct{})}
}

func (f *fakeElector) leadership() *leadership {
	return &leadership{election: f, acquired: f.acquired, leading: &atomic.Bool{}}
}

func (f *fakeElector) Run(ctx context.Context) {
	if f.giveUp {
		return
	}
	leaderCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if f.grant {
		f.acquired <- leaderCtx
	} else {
		f.record("campaigning")
	}
	select {
	case <-ctx.Done():
		f.record("released")
	case <-f.lose:
		f.record("lost")
		cancel()
	}
}

func (f *fakeElector) loseLease() { close(f.lose) }

func (f *fakeElector) record(event string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
}

func (f *fakeElector) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events)
}

func (f *fakeElector) waitFor(t *testing.T, event string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !slices.Contains(f.log(), event) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %q; saw %v", event, f.log())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func wait(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("lead did not return")
		return nil
	}
}

func discard() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }
