package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	kubernetesaccess "github.com/jensilin/KubeScaleSense/internal/kubernetes"
	"github.com/jensilin/KubeScaleSense/internal/observability"
	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// The four ways an actuation can decline to change the cluster.
//
// They are separate values because the caller must treat them differently, and
// a single "actuation failed" would collapse distinctions that matter: a
// conflict is routine, a rejection is a bug in the engine, and a fatal error
// means the controller should stop claiming to be ready.
var (
	// ErrScaleConflict reports that the target's resourceVersion moved between
	// the read and the write. The decision is discarded rather than retried:
	// retrying would write a replica count computed from a cluster state that
	// no longer exists, which is the lost update the precondition exists to
	// prevent (FR-05, ADR-12).
	ErrScaleConflict = errors.New("the target was modified concurrently; the decision is discarded and recomputed on the next reconcile")

	// ErrScaleStale reports that the replica count itself changed between the
	// snapshot and the write. Distinct from a conflict: the version moving is
	// expected and harmless, while the replica count moving means the decision
	// was computed against a cluster that no longer exists.
	ErrScaleStale = errors.New("the target's replica count changed after the snapshot was taken; the decision is abandoned rather than applied to a state it was not computed for")

	// ErrScaleRejected reports a decision the actuator refuses to carry out.
	// Reaching it means the engine produced something outside its own
	// contract, so the actuator declines and says so loudly rather than
	// quietly clamping and leaving the defect invisible.
	ErrScaleRejected = errors.New("the actuator refused the requested replica count")

	// ErrScaleFatal reports a failure that will not improve by retrying: the
	// Role is missing the scale verb, or the target does not exist. Both are
	// configuration errors, and both fail readiness rather than driving a
	// retry storm (ADR-14).
	ErrScaleFatal = errors.New("scale actuation failed for a reason retrying cannot fix")
)

// retryInitialDelay is the first pause between attempts at a transient failure.
//
// Small relative to the reconcile interval on purpose: the retry exists to ride
// out a single throttled or dropped request, not to wait out an outage. An
// outage is handled by abandoning the write and re-deciding next tick, which is
// the same answer with a fresher snapshot.
const retryInitialDelay = 100 * time.Millisecond

// ScaleTarget is the write capability the actuator needs.
//
// Two methods, one Deployment, one subresource. The controller package is given
// this interface rather than a client for the same reason it is given
// ClusterReader rather than a clientset: what the reconcile loop can do to a
// cluster should be legible from the type it holds.
type ScaleTarget interface {
	// Current reads the live replica count and the version it was read at.
	Current(ctx context.Context) (kubernetesaccess.ScaleState, error)

	// Write sets the replica count, conditional on resourceVersion.
	Write(ctx context.Context, replicas int32, resourceVersion string) error
}

// ScaleActuator changes the target Deployment's replica count.
//
// This is the write path P0 through P2 deliberately did not have. Three things
// are worth knowing about it before reading the code:
//
// It re-reads before it writes. The decision was computed from an informer
// cache that may be seconds behind, and those seconds are enough for someone
// else to have scaled the Deployment. So the live count is read first and the
// decision is abandoned unless the cluster still looks the way the decision
// assumed — which also makes a duplicated write impossible when our own
// previous write has not yet reached the cache.
//
// It re-checks the bounds it was configured with. The engine clamps every
// decision to [minReplicas, maxReplicas] already (FR-02), so a value arriving
// here outside them is a bug; the point of checking twice is that the second
// check is the one standing between the bug and the cluster.
//
// It never escalates. Every failure path ends in "no change and try again with
// a fresher view", never in "try harder", because the null action is the safe
// action for an autoscaler (I-7).
type ScaleActuator struct {
	target      ScaleTarget
	minReplicas int32
	maxReplicas int32
	retryBudget time.Duration

	metrics *observability.Metrics
	log     *slog.Logger
	now     func() time.Time

	// sleep and jitter are injected so the retry path is tested by asserting on
	// the delays it asks for rather than by waiting for them.
	sleep  func(ctx context.Context, d time.Duration) error
	jitter func(d time.Duration) time.Duration
}

// ScaleActuatorOptions wires the actuator.
type ScaleActuatorOptions struct {
	Target      ScaleTarget
	MinReplicas int32
	MaxReplicas int32

	// RetryBudget bounds the total time spent retrying transient failures
	// inside one reconcile. ADR-14 sets it to the reconcile interval: a retry
	// that outlived the interval would overlap the next reconcile and start
	// two writes from two different snapshots.
	RetryBudget time.Duration

	Metrics *observability.Metrics
	Log     *slog.Logger
	Clock   func() time.Time
}

// NewScaleActuator builds the live actuator.
//
// The bounds are required and validated here rather than defaulted. An actuator
// that defaulted its own limits would be one whose safety envelope came from
// nowhere an operator could see.
func NewScaleActuator(opts ScaleActuatorOptions) (*ScaleActuator, error) {
	switch {
	case opts.Target == nil:
		return nil, errors.New("controller: a ScaleTarget is required for live actuation")
	case opts.MinReplicas < 1:
		return nil, fmt.Errorf("controller: minReplicas must be at least 1, got %d", opts.MinReplicas)
	case opts.MaxReplicas < opts.MinReplicas:
		return nil, fmt.Errorf("controller: maxReplicas (%d) must be at least minReplicas (%d)",
			opts.MaxReplicas, opts.MinReplicas)
	case opts.RetryBudget <= 0:
		return nil, fmt.Errorf("controller: the retry budget must be positive, got %s", opts.RetryBudget)
	}

	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	return &ScaleActuator{
		target:      opts.Target,
		minReplicas: opts.MinReplicas,
		maxReplicas: opts.MaxReplicas,
		retryBudget: opts.RetryBudget,
		metrics:     opts.Metrics,
		log:         log,
		now:         clock,
		sleep:       sleepContext,
		jitter:      fullJitter,
	}, nil
}

// Mode implements Actuator.
//
// "live" rather than "real" or "enabled", because this string appears in the
// startup banner and in every reconcile log line, and it is the answer to "is
// this thing going to touch my cluster?".
func (a *ScaleActuator) Mode() string { return "live" }

// Apply implements Actuator by writing the decided replica count.
//
// A decision that is not a write returns immediately, without an API call. That
// matters beyond efficiency: it means the holds — which are most reconciles —
// cannot fail, cannot conflict, and leave no trace in the audit log. The
// controller only talks to the API server when it has decided to change
// something (NFR-04).
func (a *ScaleActuator) Apply(ctx context.Context, decision scaling.Decision) error {
	if decision.Action != scaling.ActionWrite {
		return nil
	}

	direction := string(decision.Direction)

	if err := a.validate(decision); err != nil {
		// Counted and logged at error level: this is the actuator refusing the
		// engine, which is either a defect or a configuration that changed
		// underneath a decision in flight. Either way somebody needs to know.
		a.record(direction, outcomeError)
		a.log.Error("refusing to actuate a decision outside the configured bounds",
			slog.Int("requested_replicas", int(decision.TargetReplicas)),
			slog.Int("min_replicas", int(a.minReplicas)),
			slog.Int("max_replicas", int(a.maxReplicas)),
			slog.String("reason", string(decision.Reason)),
			slog.Any("error", err),
		)
		return err
	}

	deadline := a.now().Add(a.retryBudget)
	delay := retryInitialDelay

	for attempt := 1; ; attempt++ {
		err := a.attempt(ctx, decision)
		if err == nil {
			a.record(direction, outcomeApplied)
			a.log.Info("replica count updated",
				slog.String("direction", direction),
				slog.String("reason", string(decision.Reason)),
				slog.Int("from_replicas", int(decision.CurrentReplicas)),
				slog.Int("to_replicas", int(decision.TargetReplicas)),
				slog.Int("fit_capacity", int(decision.FitCapacity)),
				slog.Int("attempts", attempt),
				slog.String("detail", "written through the scale subresource with a resourceVersion precondition; "+
					"the new capacity is not assumed and is confirmed by subsequent observation"),
			)
			return nil
		}

		switch {
		case errors.Is(err, ErrScaleConflict):
			a.record(direction, outcomeConflict)
			a.log.Info("scale write conflicted; discarding the decision and re-deciding next reconcile",
				slog.String("direction", direction),
				slog.Int("attempts", attempt),
				slog.String("detail", "a conflict means someone else wrote first, so this decision was computed "+
					"against a cluster state that no longer exists (FR-05)"),
			)
			return err

		case errors.Is(err, ErrScaleStale):
			a.record(direction, outcomeError)
			a.log.Warn("abandoning the decision: the target's replica count moved after the snapshot",
				slog.String("direction", direction),
				slog.Int("decided_from_replicas", int(decision.CurrentReplicas)),
				slog.Any("error", err),
			)
			return err

		case errors.Is(err, ErrScaleFatal):
			a.record(direction, outcomeError)
			a.log.Error("scale actuation failed fatally; not retrying",
				slog.String("direction", direction),
				slog.Any("error", err),
				slog.String("detail", "a missing scale permission or a missing target is a configuration error, "+
					"so readiness fails rather than a retry loop forming (ADR-14)"),
			)
			return err
		}

		// Everything remaining is transient. Retry inside the budget, then
		// abandon: the next reconcile will decide again from a fresher
		// snapshot, which is a better input than this one anyway.
		if ctx.Err() != nil {
			a.record(direction, outcomeError)
			return fmt.Errorf("scale actuation abandoned, the reconcile was cancelled: %w", err)
		}

		wait := a.jitter(delay)
		if remaining := deadline.Sub(a.now()); remaining <= wait {
			a.record(direction, outcomeError)
			a.log.Warn("giving up on a transient scale failure for this reconcile",
				slog.String("direction", direction),
				slog.Int("attempts", attempt),
				slog.String("retry_budget", a.retryBudget.String()),
				slog.Any("error", err),
				slog.String("detail", "no replica count was written; the decision is recomputed on the next tick"),
			)
			return err
		}

		a.log.Warn("transient scale failure; retrying within this reconcile",
			slog.String("direction", direction),
			slog.Int("attempt", attempt),
			slog.String("backoff", wait.String()),
			slog.Any("error", err),
		)

		if sleepErr := a.sleep(ctx, wait); sleepErr != nil {
			a.record(direction, outcomeError)
			return fmt.Errorf("scale actuation abandoned during backoff: %w", errors.Join(err, sleepErr))
		}
		delay *= 2
	}
}

// attempt performs one read-check-write cycle and classifies the outcome.
func (a *ScaleActuator) attempt(ctx context.Context, decision scaling.Decision) error {
	live, err := a.target.Current(ctx)
	if err != nil {
		return a.classify(err)
	}

	// The staleness check. The decision knows what the replica count was when
	// the snapshot was taken; if the live count differs, the world moved and
	// every number in the decision — the deficit, the step limit, the fit
	// capacity it was gated on — was computed for a cluster that is not the one
	// in front of us.
	//
	// This is also what makes our own writes idempotent. The informer cache can
	// still be reporting the pre-write count on the next reconcile, which would
	// produce the same decision a second time; the live read catches it and
	// this decision is dropped instead of applied twice.
	if live.Replicas != decision.CurrentReplicas {
		return fmt.Errorf("%w (decided from %d replicas, the cluster now reports %d)",
			ErrScaleStale, decision.CurrentReplicas, live.Replicas)
	}

	if err := a.target.Write(ctx, decision.TargetReplicas, live.ResourceVersion); err != nil {
		return a.classify(err)
	}
	return nil
}

// validate re-checks the decision against the configured envelope.
func (a *ScaleActuator) validate(decision scaling.Decision) error {
	target := decision.TargetReplicas

	switch {
	case target < a.minReplicas:
		return fmt.Errorf("%w: %d is below minReplicas (%d)", ErrScaleRejected, target, a.minReplicas)
	case target > a.maxReplicas:
		return fmt.Errorf("%w: %d is above maxReplicas (%d)", ErrScaleRejected, target, a.maxReplicas)
	case target == decision.CurrentReplicas:
		// A write decision that changes nothing. Harmless to the cluster, but
		// it would put a no-op entry in the audit log and a spurious increment
		// on the applied counter, and it means the engine and the actuator
		// disagree about what a write is.
		return fmt.Errorf("%w: %d replicas is what the target already has, so the decision is not a change",
			ErrScaleRejected, target)
	}
	return nil
}

// classify maps an API error onto the actuator's four outcomes.
//
// The mapping is ADR-14's table, and the defaulting direction is deliberate:
// anything unrecognised is treated as transient, so an unfamiliar error causes
// a retry and then a hold rather than a permanent readiness failure.
func (a *ScaleActuator) classify(err error) error {
	switch {
	case apierrors.IsConflict(err):
		return fmt.Errorf("%w: %w", ErrScaleConflict, err)

	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		return fmt.Errorf("%w: the ServiceAccount cannot write the scale subresource, so the Role is missing "+
			"apps/deployments/scale with update: %w", ErrScaleFatal, err)

	case apierrors.IsNotFound(err):
		return fmt.Errorf("%w: the target Deployment does not exist: %w", ErrScaleFatal, err)

	case apierrors.IsInvalid(err), apierrors.IsBadRequest(err):
		// The API server rejected the object itself. Retrying sends the same
		// rejected object again, so this is fatal in the "will not improve"
		// sense even though it is not a permission problem.
		return fmt.Errorf("%w: the API server rejected the scale object: %w", ErrScaleFatal, err)

	default:
		return err
	}
}

// record increments the scale-action counter when metrics are wired.
func (a *ScaleActuator) record(direction, outcome string) {
	if a.metrics == nil {
		return
	}
	a.metrics.RecordScaleAction(direction, outcome)
}

// Outcome label values for kss_scale_actions_total, kept next to the code that
// emits them so a new outcome cannot be added without seeing the existing set.
const (
	outcomeApplied  = "applied"
	outcomeConflict = "conflict"
	outcomeError    = "error"
)

// sleepContext waits, or returns early if the reconcile is cancelled.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// fullJitter spreads a retry uniformly over (0, d].
//
// Full jitter rather than a fixed delay because the failure this retries is
// often the API server shedding load, and several controllers retrying in
// lockstep is how a throttled API server stays throttled.
func fullJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d))) + 1
}
