package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/jensilin/KubeScaleSense/internal/config"
	kubernetesaccess "github.com/jensilin/KubeScaleSense/internal/kubernetes"
	kssmetrics "github.com/jensilin/KubeScaleSense/internal/metrics"
	"github.com/jensilin/KubeScaleSense/internal/observability"
	"github.com/jensilin/KubeScaleSense/internal/resources"
	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// historyRetention is how much longer than the stabilization window the desired
// samples are kept.
//
// It must exceed the window, not merely equal it: the coverage check requires
// the oldest retained sample to be at least a window old, so retaining exactly
// one window would leave the gate permanently unsatisfiable.
const historyRetention = 2

// Options wires the controller's collaborators.
//
// Every field is an interface or a value, and none of them is a Kubernetes
// clientset. Through P2 that was the structural half of a no-write guarantee.
// From P3 the guarantee is narrower but the structure is unchanged: the only
// field that can reach the API server with a mutating request is Actuator, and
// the only mutation any implementation of it can express is a replica count on
// one named Deployment.
type Options struct {
	Config      *config.Config
	Reader      kubernetesaccess.ClusterReader
	Signal      kssmetrics.WorkloadSignal
	Utilization *kssmetrics.UtilizationCollector
	Resources   resources.Options
	Metrics     *observability.Metrics
	Actuator    Actuator
	Log         *slog.Logger

	// Clock is injected so that cooldowns and windows are tested by advancing
	// a fake clock rather than by sleeping. A test suite that sleeps for 300
	// seconds to exercise a 300 second window is a suite nobody runs.
	Clock func() time.Time
}

// Controller runs the reconcile loop.
type Controller struct {
	cfg         *config.Config
	reader      kubernetesaccess.ClusterReader
	signal      kssmetrics.WorkloadSignal
	utilization *kssmetrics.UtilizationCollector
	resources   resources.Options
	metrics     *observability.Metrics
	actuator    Actuator
	log         *slog.Logger
	now         func() time.Time
	smoother    *kssmetrics.Smoother

	mu    sync.Mutex
	state state
}

// state is the controller's memory between reconciles.
type state struct {
	lastScaleUp      time.Time
	lastScaleDown    time.Time
	desiredHistory   []scaling.DesiredSample
	backoff          scaling.BackoffState
	lastGoodReplicas int32

	// observedReplicas is the replica count we last saw.
	observedReplicas int32
	hasBaseline      bool
	externalDriftAt  []time.Time

	// lastWrittenReplicas is the value this controller most recently wrote.
	//
	// P1 and P2 needed no such field: they wrote nothing, so every change to
	// spec.replicas was external by definition. From P3 that is no longer true,
	// and without this the controller's own scale-ups would be counted as
	// external drift and it would freeze itself with HoldExternalChange after
	// three successful scalings. ADR-17 defines the distinction in exactly
	// these terms: a change is external when the observed value differs from
	// what we last wrote *and* we did not write it.
	lastWrittenReplicas int32
	hasWritten          bool

	reconciles    int
	lastReconcile time.Time
	lastError     error

	// lastFatalError holds an actuation failure that will not fix itself: a
	// missing scale permission, or a target that no longer exists. Separate
	// from lastError because readiness must distinguish "this cluster cannot be
	// observed right now" from "this controller is misconfigured and will never
	// work" (ADR-14).
	lastFatalError error
}

// New builds a controller.
func New(opts Options) (*Controller, error) {
	switch {
	case opts.Config == nil:
		return nil, errors.New("controller: configuration is required")
	case opts.Reader == nil:
		return nil, errors.New("controller: a ClusterReader is required")
	case opts.Signal == nil:
		return nil, errors.New("controller: a workload signal is required")
	case opts.Actuator == nil:
		return nil, errors.New("controller: an actuator is required")
	}

	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	return &Controller{
		cfg:         opts.Config,
		reader:      opts.Reader,
		signal:      opts.Signal,
		utilization: opts.Utilization,
		resources:   opts.Resources,
		metrics:     opts.Metrics,
		actuator:    opts.Actuator,
		log:         log,
		now:         clock,
		smoother:    kssmetrics.NewSmoother(opts.Config.Workload.BacklogSmoothing),
		state:       state{backoff: scaling.ClearBackoff()},
	}, nil
}

// VerifyTarget performs the startup checks that config validation could not,
// because they need a cluster (config.ClusterChecks).
//
// Both are fatal rather than degraded. A target whose pod template declares no
// requests makes fit capacity meaningless, and an HPA on the same target means
// two controllers would fight over one field — in each case starting up and
// reporting decisions would produce confident nonsense.
func (c *Controller) VerifyTarget(_ context.Context) (resources.Request, error) {
	namespace, name := c.cfg.Target.Namespace, c.cfg.Target.Deployment

	deployment, err := c.reader.Deployment(namespace, name)
	if err != nil {
		return resources.Request{}, fmt.Errorf("target Deployment %s/%s cannot be read: %w", namespace, name, err)
	}

	request, err := resources.EffectivePodRequest(&deployment.Spec.Template.Spec)
	if err != nil {
		return resources.Request{}, err
	}

	hpas, err := c.reader.HorizontalPodAutoscalers(namespace)
	if err != nil {
		return resources.Request{}, fmt.Errorf("checking for a competing HorizontalPodAutoscaler: %w", err)
	}
	if hpa := kubernetesaccess.ConflictingHPA(hpas, name); hpa != nil {
		return resources.Request{}, fmt.Errorf(
			"HorizontalPodAutoscaler %s already targets Deployment %s/%s; two controllers writing one replica "+
				"count cannot be reconciled, so remove one of them (FR-19)",
			kubernetesaccess.DescribeHPA(hpa), namespace, name)
	}

	return request, nil
}

// Ready reports whether this replica should be deciding, for /readyz.
//
// Readiness keys on synced caches and a completed observation cycle rather than
// on a *usable* demand sample. `signal.source: none` never produces one and is a
// legitimate configuration — the controller is then working exactly as designed,
// freezing rather than guessing — so failing readiness for it would make a
// correct configuration look broken. Signal availability is reported separately
// by kss_workload_signal_source_up.
func (c *Controller) Ready() error {
	if !c.reader.HasSynced() {
		return errors.New("informer caches have not synced")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state.reconciles == 0 {
		return errors.New("no reconcile has completed yet")
	}
	// Reported before lastError because it is the more actionable of the two: a
	// 403 on the scale subresource means an operator has to change the Role,
	// and no amount of waiting will clear it.
	if c.state.lastFatalError != nil {
		return fmt.Errorf("actuation is not possible: %w", c.state.lastFatalError)
	}
	if c.state.lastError != nil {
		return fmt.Errorf("last reconcile failed: %w", c.state.lastError)
	}
	return nil
}

// Run reconciles on the configured interval until the context is cancelled.
//
// A failed reconcile is logged and the loop continues. That is the fail-safe
// stance the design requires: an unreadable cluster produces ErrorAPIFailure and
// no action in either direction, and exiting would turn a transient API blip
// into a restart loop that observes nothing at all (I-7, ADR-14).
func (c *Controller) Run(ctx context.Context) error {
	interval := c.cfg.Controller.Interval.Duration()

	c.log.Info("reconcile loop starting",
		slog.String("interval", interval.String()),
		slog.String("actuator", c.actuator.Mode()),
		slog.String("target", c.cfg.Target.Namespace+"/"+c.cfg.Target.Deployment),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if _, err := c.Reconcile(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.log.Error("reconcile failed; holding position", slog.Any("error", err))
		}

		select {
		case <-ctx.Done():
			c.log.Info("reconcile loop stopping", slog.Int("reconciles", c.reconcileCount()))
			return nil
		case <-ticker.C:
		}
	}
}

// Reconcile performs one observation and decision.
//
// The returned error describes a failure to *observe*. It is not a failure to
// decide: an unobservable cluster still produces a decision, and that decision
// is ErrorAPIFailure with no action.
func (c *Controller) Reconcile(ctx context.Context) (scaling.Decision, error) {
	now := c.now()
	start := now

	snapshot, feasibility, sample, rawBacklog, observeErr := c.observe(ctx, now)
	observeDone := c.now()

	decision := scaling.Decide(snapshot, c.cfg, now)
	decideDone := c.now()

	// Folded before the write because these outputs describe the *decision*,
	// not its effect: the stabilization window must keep filling and the
	// backoff must keep advancing whether or not a write succeeds. A hold that
	// arms the backoff performs no write at all.
	c.applyDecisionState(decision, now)

	obs := observability.Observation{
		Decision:        decision,
		Snapshot:        snapshot,
		Feasibility:     feasibility,
		Sample:          sample,
		SignalSource:    c.signal.Source(),
		RawBacklog:      rawBacklog,
		DryRun:          c.cfg.Controller.DryRun,
		Now:             now,
		ObserveDuration: observeDone.Sub(start),
		DecideDuration:  decideDone.Sub(observeDone),
		TotalDuration:   c.now().Sub(start),
	}

	if c.metrics != nil {
		c.metrics.RecordReconcile(obs)
	}
	observability.LogDecision(c.log, obs, c.cfg.Controller.DryRun)
	observability.LogNodeDetail(c.log, obs)

	actuateErr := c.actuator.Apply(ctx, decision)
	if actuateErr == nil {
		// Only a successful write advances the cooldown timers and the revert
		// target. Advancing them before the write — which is what P1 did, and
		// could afford to, because its actuator could not fail — would make a
		// failed actuation impose a cooldown on the retry that follows it, so
		// the controller would sit out the next interval having changed
		// nothing.
		c.applyWriteState(decision, now)
	}

	c.mu.Lock()
	c.state.reconciles++
	c.state.lastReconcile = now
	c.state.lastError = observeErr
	if errors.Is(actuateErr, ErrScaleFatal) {
		c.state.lastFatalError = actuateErr
	}
	c.mu.Unlock()

	if actuateErr != nil {
		return decision, fmt.Errorf("actuator: %w", actuateErr)
	}
	return decision, observeErr
}

// observe assembles the snapshot.
//
// Every read here comes from an informer cache or from metrics.k8s.io, and none
// of them can mutate anything. A failure at any step yields an invalid snapshot
// rather than a partial one: deciding from half a view of the cluster is worse
// than declining to decide.
func (c *Controller) observe(ctx context.Context, now time.Time) (scaling.Snapshot, resources.Feasibility, kssmetrics.Sample, int64, error) {
	var (
		snapshot    scaling.Snapshot
		feasibility resources.Feasibility
		sample      = kssmetrics.Unavailable()
		rawBacklog  int64
	)

	c.mu.Lock()
	snapshot.LastScaleUp = c.state.lastScaleUp
	snapshot.LastScaleDown = c.state.lastScaleDown
	snapshot.DesiredHistory = append([]scaling.DesiredSample(nil), c.state.desiredHistory...)
	snapshot.HoldBackoff = c.state.backoff
	snapshot.LastGoodReplicas = c.state.lastGoodReplicas
	c.mu.Unlock()

	if !c.reader.HasSynced() {
		return snapshot, feasibility, sample, rawBacklog, errors.New("informer caches have not synced")
	}

	namespace, name := c.cfg.Target.Namespace, c.cfg.Target.Deployment

	target, err := kubernetesaccess.ObserveTarget(c.reader, namespace, name, now)
	if err != nil {
		c.recordAPIError("deployments", "get", err)
		return snapshot, feasibility, sample, rawBacklog, err
	}

	snapshot.CurrentReplicas = target.CurrentReplicas
	snapshot.ReadyReplicas = target.ReadyReplicas
	snapshot.RolloutInProgress = target.RolloutInProgress
	snapshot.PendingOurPods = target.PendingPods
	snapshot.OldestPendingAge = target.OldestPendingAge
	snapshot.UnhealthyOurPods = target.UnhealthyPods
	snapshot.UnhealthyPodAge = target.UnhealthyPodAge

	snapshot.ExternalDrifts = c.trackExternalDrift(target.CurrentReplicas, now)

	podRequest, err := resources.EffectivePodRequest(&target.Deployment.Spec.Template.Spec)
	if err != nil {
		// The template changed underneath us after startup validation passed.
		return snapshot, feasibility, sample, rawBacklog, err
	}
	snapshot.PodRequest = podRequest

	nodes, err := c.reader.Nodes()
	if err != nil {
		c.recordAPIError("nodes", "list", err)
		return snapshot, feasibility, sample, rawBacklog, err
	}
	pods, err := c.reader.Pods()
	if err != nil {
		c.recordAPIError("pods", "list", err)
		return snapshot, feasibility, sample, rawBacklog, err
	}

	// Feasibility is computed on every reconcile, including ones that will
	// return HoldBackoff, because the backoff reset is level-triggered on
	// observed capacity (DR-01).
	//
	// The documented optimization skips the node walk when G0..G4 already settle
	// the decision. Computing it unconditionally is a strict superset of what
	// correctness requires and costs O(N+P) integer arithmetic on cached
	// objects — a few hundred microseconds at the NFR-02 target scale — so the
	// simpler control flow is worth more than the saving.
	feasibility = resources.Calculate(&target.Deployment.Spec.Template.Spec, podRequest, nodes, pods, c.resources, now)
	snapshot.FitCapacity = feasibility.FitCapacity
	snapshot.CandidateNodes = feasibility.CandidateNodes
	snapshot.Blocking = feasibility.Blocking
	snapshot.FreeCPUMilli = feasibility.FreeCPUMilli
	snapshot.FreeMemoryBytes = feasibility.FreeMemoryBytes

	hpas, err := c.reader.HorizontalPodAutoscalers(namespace)
	if err != nil {
		c.recordAPIError("horizontalpodautoscalers", "list", err)
		return snapshot, feasibility, sample, rawBacklog, err
	}
	snapshot.HPAPresent = kubernetesaccess.ConflictingHPA(hpas, name) != nil

	// The demand signal is collected last and its failure is *not* an
	// observation failure: an unavailable signal is a specified state that the
	// engine handles with HoldStaleMetrics, not an error that invalidates the
	// snapshot.
	sample, rawBacklog = c.collectPressure(ctx)
	snapshot.Backlog = scaling.Signal[int64]{
		Value:     sample.PressureItems,
		SampledAt: sample.SampledAt,
		Available: sample.Available,
	}
	snapshot.BacklogRaw = rawBacklog

	c.collectUtilization(ctx, &snapshot, target, podRequest, now)

	snapshot.Valid = true
	return snapshot, feasibility, sample, rawBacklog, nil
}

// collectPressure reads the workload signal and applies smoothing.
//
// Returns the smoothed sample and the raw value, so that both can be exported
// and the effect of smoothing stays auditable rather than invisible.
func (c *Controller) collectPressure(ctx context.Context) (kssmetrics.Sample, int64) {
	timeout := c.cfg.Workload.Signal.Timeout.Duration()
	collectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	sample, err := c.signal.Collect(collectCtx)
	if err != nil {
		c.log.Warn("workload signal unavailable; the engine will hold rather than assume no work",
			slog.String("source", c.signal.Source()),
			slog.String("timeout", timeout.String()),
			slog.Any("error", err),
		)
	}
	if !sample.Available {
		return sample, 0
	}

	raw := sample.PressureItems

	// Freshness is judged on the older of the source-reported sample time and
	// our own receive time, so a source that answers instantly with a frozen
	// value is still detected as stale (DR-14).
	received := c.now()
	if sample.SampledAt.IsZero() || sample.SampledAt.After(received) {
		sample.SampledAt = received
	}

	sample.PressureItems = c.smoother.Apply(raw)
	return sample, raw
}

// collectUtilization fills in the utilization half of the demand signal.
func (c *Controller) collectUtilization(
	ctx context.Context,
	snapshot *scaling.Snapshot,
	target kubernetesaccess.TargetState,
	podRequest resources.Request,
	now time.Time,
) {
	if c.utilization == nil {
		return
	}

	utilization, err := c.utilization.Collect(
		ctx, c.cfg.Target.Namespace, target.Pods, podRequest,
		c.cfg.Workload.PodWarmupPeriod.Duration(), now)
	if err != nil {
		// metrics.k8s.io not being installed is an expected state: pressure-only
		// scaling continues and only the utilization safety net is lost.
		c.log.Warn("pod utilization unavailable; continuing on the pressure signal alone",
			slog.Any("error", err))
		c.recordAPIError("pods.metrics.k8s.io", "list", err)
	}

	snapshot.AvgCPUMilliPercent = scaling.Signal[int64]{
		Value:     utilization.AvgCPUMilliPercent,
		SampledAt: utilization.SampledAt,
		Available: utilization.Available,
	}
	snapshot.AvgMemMilliPercent = scaling.Signal[int64]{
		Value:     utilization.AvgMemMilliPercent,
		SampledAt: utilization.SampledAt,
		Available: utilization.Available,
	}
}

// trackExternalDrift detects and counts replica changes made by other actors.
//
// A drift is adopted as the new baseline rather than fought: the history and the
// smoothed average are reset, because both described a replica count that no
// longer exists. Only if drift *recurs* beyond the tolerance does the controller
// stop acting (DR-07).
//
// From P3 this has to tell two kinds of change apart, because the controller is
// now one of the actors that can cause one. A count that matches our own last
// write is not drift, and treating it as drift would be self-defeating in the
// most literal way: three successful scale-ups would exceed the tolerance and
// the controller would refuse to act on the grounds that someone kept scaling
// the Deployment — that someone being itself (ADR-17).
func (c *Controller) trackExternalDrift(observed int32, now time.Time) int32 {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.state.hasBaseline {
		c.state.hasBaseline = true
		c.state.observedReplicas = observed
		c.state.lastGoodReplicas = observed
		return 0
	}

	switch {
	case observed == c.state.observedReplicas:
		// Nothing moved.

	case c.state.hasWritten && observed == c.state.lastWrittenReplicas:
		// Our own write arriving through the informer cache. Adopted silently:
		// no event, no drift increment, and the history is deliberately kept.
		// The window records how much demand there was, which is still true —
		// unlike an external change, this one is the outcome the history was
		// used to justify.
		c.log.Debug("observed our own replica count change",
			slog.Int("from", int(c.state.observedReplicas)),
			slog.Int("to", int(observed)),
		)
		c.state.observedReplicas = observed

	default:
		c.log.Warn("replica count changed by another actor; adopting it as the new baseline",
			slog.Int("from", int(c.state.observedReplicas)),
			slog.Int("to", int(observed)),
			slog.Int("last_written_by_us", int(c.state.lastWrittenReplicas)),
			slog.Bool("we_have_ever_written", c.state.hasWritten),
			slog.String("detail", "the observed count is neither the previous value nor the one this controller "+
				"last wrote, so another actor set it (FR-31)"),
		)
		c.state.observedReplicas = observed
		c.state.externalDriftAt = append(c.state.externalDriftAt, now)
		c.state.desiredHistory = nil
		c.smoother.Reset()
		if c.metrics != nil {
			c.metrics.RecordExternalScaleChange()
		}
	}

	// Drifts are counted within the stabilization window, so an actor that
	// changed the count once an hour ago does not permanently disable scaling.
	window := c.cfg.Scaling.ScaleDownStabilizationWindow.Duration()
	kept := c.state.externalDriftAt[:0]
	for _, at := range c.state.externalDriftAt {
		if now.Sub(at) <= window {
			kept = append(kept, at)
		}
	}
	c.state.externalDriftAt = kept
	return int32(len(kept))
}

// applyDecisionState folds the outputs that describe the decision itself: the
// stabilization window and the backoff.
//
// Both must advance regardless of what the actuator then does, and for the
// holds there is nothing for the actuator to do at all.
func (c *Controller) applyDecisionState(decision scaling.Decision, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Only reconciles that actually computed demand append a sample. A
	// HoldStaleMetrics or ErrorAPIFailure reconcile knows nothing about demand,
	// and recording a zero for it would let an outage empty the window and
	// permit a scale-down justified by missing data (DR-04).
	if decision.DemandComputed {
		c.state.desiredHistory = append(c.state.desiredHistory, scaling.DesiredSample{
			At:      now,
			Desired: decision.DesiredRaw,
		})
		c.state.desiredHistory = pruneHistory(
			c.state.desiredHistory,
			c.cfg.Scaling.ScaleDownStabilizationWindow.Duration()*historyRetention,
			now)
	}

	switch decision.BackoffAction {
	case scaling.BackoffArm, scaling.BackoffReset:
		c.state.backoff = decision.Backoff
	case scaling.BackoffUnchanged:
	}
}

// applyWriteState folds the consequences of a write that actually happened.
//
// In dry-run this is still reached, because the dry-run actuator reports
// success — which is the counterfactual bookkeeping P1 relied on and P3 keeps:
// the timers advance as though the write had happened, so the cooldowns stay
// exercised and a dry-run trace remains directly comparable with a live one.
//
// In live mode it is reached only after the replica count has been written.
func (c *Controller) applyWriteState(decision scaling.Decision, now time.Time) {
	if decision.Action != scaling.ActionWrite {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Recorded for both directions and before the direction switch, because
	// external-change detection needs to recognise *any* value we wrote. A
	// scale-down we omitted here would come back on the next reconcile as
	// somebody else's change.
	c.state.lastWrittenReplicas = decision.TargetReplicas
	c.state.hasWritten = true

	switch decision.Direction {
	case scaling.DirectionUp:
		c.state.lastScaleUp = now
		// The pre-scale count, which is the revert target if the new pods turn
		// out to be unschedulable (FS-06).
		c.state.lastGoodReplicas = decision.CurrentReplicas
	case scaling.DirectionDown:
		c.state.lastScaleDown = now
	case scaling.DirectionNone:
	}
}

// pruneHistory drops samples older than the retention horizon.
func pruneHistory(history []scaling.DesiredSample, retain time.Duration, now time.Time) []scaling.DesiredSample {
	kept := history[:0]
	for _, sample := range history {
		if now.Sub(sample.At) <= retain {
			kept = append(kept, sample)
		}
	}
	return kept
}

// recordAPIError counts a failed read, extracting the HTTP status where the
// error carries one.
//
// The status code is what distinguishes the cases an operator must treat
// differently: a 403 is a misconfigured Role and will not improve on its own,
// while a 503 is a control plane having a bad minute.
func (c *Controller) recordAPIError(resource, verb string, err error) {
	if c.metrics == nil || err == nil {
		return
	}
	code := "unknown"
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		code = strconv.Itoa(int(status.Status().Code))
	}
	c.metrics.RecordAPIError(resource, verb, code)
}

func (c *Controller) reconcileCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.reconciles
}
