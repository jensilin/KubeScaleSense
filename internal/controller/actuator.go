package controller

import (
	"context"
	"log/slog"

	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// Actuator applies a decision.
//
// The interface existed from Phase 1 so that the seam where writes would
// eventually happen was visible, reviewable, and already covered by tests —
// rather than being carved out later inside the reconcile loop, which is how a
// "dry-run only" controller acquires a write path nobody reviewed.
//
// P3 filled the seam. There are now exactly two implementations: DryRunActuator
// below, which writes nothing and is still the default, and ScaleActuator in
// scale.go, which writes a replica count through the scale subresource. A third
// appearing is a test failure, because the number of things in this repository
// that can change a cluster is a property worth counting.
//
// The contract both share: returning nil means "the decision has been carried
// out". The reconcile loop keys its cooldown timers on that, so an
// implementation must not report success for a write it did not make.
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
// It is not a stub or a test double. For the whole of P1 and P2 it was the
// intended production behaviour, and it remains the default and the documented
// way to validate a configuration against a real cluster before allowing it to
// act. It performs no API call of any kind, which is what makes "dry-run
// mutates nothing" a structural property rather than a claim about a flag.
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
// gets mistaken for evidence that autoscaling works. The live actuator's
// message is deliberately different — "replica count updated" — so the two
// traces cannot be confused with one another.
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
		slog.String("detail", "this process holds no client that can write; the replica count is not changed"),
	)
	return nil
}
