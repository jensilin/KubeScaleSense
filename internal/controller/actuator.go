package controller

import (
	"context"
	"log/slog"

	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// Actuator applies a decision.
//
// The interface exists in Phase 1 so that the seam where writes will eventually
// happen is visible, reviewable, and already covered by tests — rather than
// being carved out later inside the reconcile loop, which is how a "dry-run
// only" controller acquires a write path nobody reviewed.
//
// There is exactly one implementation in this repository, and it writes nothing.
// The real one arrives in P3 together with the scale subresource permission,
// the resourceVersion precondition, and the conflict handling that a write
// needs in order to be safe.
type Actuator interface {
	// Apply carries out the decision. Implementations must treat a decision
	// whose Action is ActionNone as a no-op.
	Apply(ctx context.Context, decision scaling.Decision) error

	// Mode names the actuator in logs and in the startup banner, so that "is
	// this thing going to touch my cluster?" is answerable from the first lines
	// of output.
	Mode() string
}

// DryRunActuator reports what would have happened and changes nothing.
//
// It is not a stub or a test double: for the whole of Phase 1 this is the
// intended production behaviour, and the phase exists to validate that the
// decisions it reports are the right ones before anything is allowed to act on
// them.
type DryRunActuator struct {
	log *slog.Logger
}

// NewDryRunActuator builds the no-op actuator.
func NewDryRunActuator(log *slog.Logger) DryRunActuator {
	return DryRunActuator{log: log}
}

// Mode implements Actuator.
func (DryRunActuator) Mode() string { return "dry-run" }

// Apply implements Actuator by logging the counterfactual.
//
// The message is phrased as "would scale" rather than "scaled" deliberately.
// Log lines outlive the context in which they were written, and a line reading
// "scaled 2 -> 6" from a controller that scaled nothing is how a dry-run trace
// gets mistaken for evidence that autoscaling works.
func (a DryRunActuator) Apply(_ context.Context, decision scaling.Decision) error {
	if decision.Action != scaling.ActionWrite {
		return nil
	}

	a.log.Info("dry run: no change made to the cluster",
		slog.String("would_action", string(decision.Reason)),
		slog.Int("from_replicas", int(decision.CurrentReplicas)),
		slog.Int("to_replicas", int(decision.TargetReplicas)),
		slog.Int("fit_capacity", int(decision.FitCapacity)),
		slog.String("blocking_dimension", string(decision.Blocking)),
		slog.String("detail", "Phase 1 observes and decides only; the replica count is never written"),
	)
	return nil
}
