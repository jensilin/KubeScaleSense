package controller

import (
	"context"
	"log/slog"
	"time"

	kubernetesaccess "github.com/jensilin/KubeScaleSense/internal/kubernetes"
	kssmetrics "github.com/jensilin/KubeScaleSense/internal/metrics"
	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// DeletionCostWriter sets pod-deletion-cost on pods of the target (FR-21).
//
// One method, one annotation. Nil in dry-run, and when the signal source cannot
// say how busy each pod is.
type DeletionCostWriter interface {
	SetDeletionCost(ctx context.Context, pod string, cost int32) error
}

// refreshDeletionCosts points a scale-down at the least busy pods by setting
// their deletion costs immediately before the replica count is lowered.
//
// Only for ScaleDown. A revert removes replicas that never started, and the
// ReplicaSet controller takes unscheduled pods first whatever their cost. And
// best-effort throughout: the costs are a preference the ReplicaSet controller
// may override anyway (DR-11), so failing to refresh them is a log line, never
// a reason to hold a scale-down the engine has justified.
func (c *Controller) refreshDeletionCosts(ctx context.Context, decision scaling.Decision, sample kssmetrics.Sample,
	now time.Time,
) {
	if c.deletionCost == nil || decision.Reason != scaling.ReasonScaleDown {
		return
	}

	target, err := kubernetesaccess.ObserveTarget(c.reader, c.cfg.Target.Namespace, c.cfg.Target.Deployment, now)
	if err != nil {
		c.log.Warn("pod-deletion-cost not refreshed; the scale-down goes ahead unbiased", slog.Any("error", err))
		return
	}
	costs, err := kubernetesaccess.DeletionCosts(target.Pods, sample.WorkByPodIP)
	if err != nil {
		c.log.Info("pod-deletion-cost not refreshed; the scale-down goes ahead with the costs already set",
			slog.String("reason", err.Error()))
		return
	}

	// Bounded so a slow API server cannot hold the scale-down past the
	// reconcile that decided it.
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Controller.Interval.Duration()/2)
	defer cancel()

	written, failed := 0, 0
	cheapest := costs[0]
	for _, cost := range costs {
		if cost.Cost < cheapest.Cost {
			cheapest = cost
		}
		if !cost.Changed {
			continue
		}
		if err := c.deletionCost.SetDeletionCost(ctx, cost.Pod, cost.Cost); err != nil {
			failed++
			c.log.Warn("could not set pod-deletion-cost", slog.String("pod", cost.Pod), slog.Any("error", err))
			continue
		}
		written++
	}

	c.log.Info("pod-deletion-cost refreshed before scale-down",
		slog.Int("ready_pods", len(costs)),
		slog.Int("written", written),
		slog.Int("failed", failed),
		slog.String("cheapest_pod", cheapest.Pod),
		slog.Int("cheapest_cost", int(cheapest.Cost)),
		slog.String("detail", "cost is each pod's own queued plus in-flight work; the ReplicaSet controller "+
			"prefers the lowest, after any pod that is not Ready"),
	)
}
