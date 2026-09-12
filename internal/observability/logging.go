package observability

import (
	"context"
	"log/slog"

	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// LogDecision emits the one line per reconcile that makes a run auditable.
//
// The field set is stable and matches architecture § 8.3. Every intermediate
// value is present deliberately: the requirement is that a single line
// reconstructs the whole decision, so that "why 4 and not 8?" is answered by
// reading rather than by re-running the controller with debug logging and
// hoping the state recurs (NFR-10).
//
// Durations are rendered as seconds or as strings rather than with slog.Duration,
// which JSON-encodes a duration as a raw nanosecond count that nobody reads
// correctly at a glance.
func LogDecision(log *slog.Logger, obs Observation, dryRun bool) {
	d := obs.Decision
	s := obs.Snapshot
	f := obs.Feasibility

	attrs := []any{
		slog.String("reason", string(d.Reason)),
		slog.String("direction", string(d.Direction)),
		slog.Bool("dry_run", dryRun),
		slog.Bool("blocked", d.Blocked),

		slog.Int("current", int(s.CurrentReplicas)),
		slog.Int("ready", int(s.ReadyReplicas)),
		slog.Int("would_target", int(d.TargetReplicas)),
		slog.Int("desiredBacklog", int(d.DesiredBacklog)),
		slog.Int("desiredUtilization", int(d.DesiredUtilization)),
		slog.Int("desiredRaw", int(d.DesiredRaw)),
		slog.Int("desiredClamped", int(d.DesiredClamped)),
		slog.Int("stepLimited", int(d.StepLimited)),
		slog.Int("requestedDelta", int(d.RequestedDelta)),
		slog.Int("deficit", int(d.Deficit)),

		slog.Bool("backlogAvailable", s.Backlog.Available),
		slog.Int64("backlog", obs.RawBacklog),
		slog.Int64("backlogSmoothed", s.Backlog.Value),
		slog.Float64("sampleAgeSec", s.Backlog.Age(obs.Now).Seconds()),

		slog.Bool("cpuAvailable", s.AvgCPUMilliPercent.Available),
		slog.Float64("avgCpuPct", milliToUnit(s.AvgCPUMilliPercent.Value)),

		slog.Int64("podRequestCpuMilli", f.PodRequest.CPUMilli),
		slog.Int64("podRequestMemMiB", f.PodRequest.MemoryMiB()),

		slog.Int("candidateNodes", int(f.CandidateNodes)),
		slog.Int("totalNodes", int(f.TotalNodes)),
		slog.Any("excludedNodes", nonZeroExclusions(f.Exclusions)),
		slog.Int("fitCapacity", int(f.FitCapacity)),
		slog.Int("rawFitSum", int(f.RawFitSum)),
		slog.String("blocking", string(f.Blocking)),
		slog.Int64("freeCpuMilli", f.FreeCPUMilli),
		slog.Int64("freeMemMiB", f.FreeMemoryBytes/(1024*1024)),

		slog.Int("pendingPods", int(s.PendingOurPods)),
		slog.Int("unhealthyPods", int(s.UnhealthyOurPods)),
		slog.Float64("unhealthyPodAgeSec", s.UnhealthyPodAge.Seconds()),

		slog.Int("backoffAttempts", d.Backoff.Attempts),
		slog.Float64("backoffSec", d.BackoffInterval.Seconds()),
		slog.String("backoffAction", string(d.BackoffAction)),
		slog.Int("backoffFitCapacityAtArm", int(d.Backoff.FitCapacityAtArm)),

		slog.Bool("rolloutInProgress", s.RolloutInProgress),
		slog.Bool("hpaPresent", s.HPAPresent),
		slog.Int("externalDrifts", int(s.ExternalDrifts)),
		slog.Float64("reconcileSec", obs.TotalDuration.Seconds()),
	}

	if !d.Backoff.NextEligible.IsZero() {
		attrs = append(attrs, slog.String("nextEligible", d.Backoff.NextEligible.UTC().Format("2006-01-02T15:04:05Z")))
	}

	// A hold that blocks demand is a warning, because it is a state an operator
	// may need to act on — add capacity, raise a limit, fix an image. The steady
	// state and the actions are info.
	if d.Blocked && d.Reason != scaling.ReasonNoChangeWithinTolerance {
		log.Warn("decision", attrs...)
		return
	}
	log.Info("decision", attrs...)
}

// nonZeroExclusions drops the reasons that excluded nothing, so the log line
// carries {"taint":1} rather than eight zeroes.
func nonZeroExclusions(exclusions map[string]int32) map[string]int32 {
	if len(exclusions) == 0 {
		return nil
	}
	out := make(map[string]int32, len(exclusions))
	for reason, count := range exclusions {
		if count > 0 {
			out[reason] = count
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// LogNodeDetail emits the per-node fit breakdown at debug level.
//
// Separate from the decision line because it is per-node and would bury the
// decision in a large cluster, but it is the first thing needed when a fit
// capacity looks wrong — the manual validation gate compares exactly these
// numbers against `kubectl describe node`.
func LogNodeDetail(log *slog.Logger, obs Observation) {
	if !log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	for i := range obs.Feasibility.Nodes {
		n := obs.Feasibility.Nodes[i]
		log.Debug("node fit",
			slog.String("node", n.Name),
			slog.Int64("allocatableCpuMilli", n.AllocatableCPU),
			slog.Int64("requestedCpuMilli", n.RequestedCPU),
			slog.Int64("freeCpuMilli", n.FreeCPUMilli),
			slog.Int64("allocatableMemMiB", n.AllocatableMemory/(1024*1024)),
			slog.Int64("requestedMemMiB", n.RequestedMemory/(1024*1024)),
			slog.Int64("freeMemMiB", n.FreeMemoryBytes/(1024*1024)),
			slog.Int64("freeSlots", n.FreeSlots),
			slog.Int("fit", int(n.Fit)),
			slog.String("binding", string(n.Binding)),
		)
	}
}
