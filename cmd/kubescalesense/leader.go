package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/jensilin/KubeScaleSense/internal/config"
	kubernetesaccess "github.com/jensilin/KubeScaleSense/internal/kubernetes"
	"github.com/jensilin/KubeScaleSense/internal/observability"
)

// errLostLeadership ends a process whose lease was taken or could not be
// renewed while it was deciding.
//
// The process exits rather than campaigning again: its cooldowns, history, and
// backoff describe decisions made under a lease it no longer holds, and the
// successor has made decisions since. A fresh process starts from the cluster
// alone, cooled down and with an empty history (FS-17).
var errLostLeadership = errors.New("lost the leader lease; exiting so that the next start begins from a clean, " +
	"cooled-down state (FS-17)")

// elector is the part of kubernetesaccess.Election that lead drives, so the
// ordering lead exists for can be tested without an API server.
type elector interface {
	Run(ctx context.Context)
}

// leadership is a process's election and what it reports about it.
type leadership struct {
	election elector
	// acquired receives the leading context once, from OnStartedLeading.
	acquired <-chan context.Context
	leading  *atomic.Bool
}

// buildLeadership builds the election, on the same rule as the write clients:
// only a process that can write takes part.
//
// A dry-run process builds no lease client. It changes nothing, so there is no
// writer to elect, and a dry-run process holding the lease would keep a live
// one from acting — the observer blocking the thing it observes.
func buildLeadership(
	cfg *config.Config,
	opts options,
	metrics *observability.Metrics,
	log *slog.Logger,
) (*leadership, error) {
	switch {
	case cfg.Controller.DryRun:
		log.Info("leader election not started",
			slog.String("detail", "dry-run writes nothing, so there is no writer to elect; every replica reports "+
				"its own decisions and none holds the lease"),
		)
		return nil, nil
	case !cfg.Controller.LeaderElection:
		log.Warn("leader election is disabled: run exactly one replica of this controller",
			slog.String("detail", "without the lease, every replica decides and writes, and two replicas would "+
				"write the same replica count (FR-26)"),
		)
		return nil, nil
	}

	namespace, name := cfg.Target.Namespace, kubernetesaccess.LeaseName(cfg.Target.Deployment)

	leaseConfig, err := kubernetesaccess.NewLeaseRESTConfig(opts.kubeconfigPath, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("building the lease client configuration: %w", err)
	}
	leaseClients, err := kubernetesaccess.NewClients(leaseConfig)
	if err != nil {
		return nil, fmt.Errorf("building the lease client: %w", err)
	}

	identity := kubernetesaccess.ElectionIdentity()
	acquired := make(chan context.Context, 1)
	leading := &atomic.Bool{}

	election, err := kubernetesaccess.NewElection(kubernetesaccess.ElectionConfig{
		Client:    leaseClients.Core,
		Namespace: namespace,
		Name:      name,
		Identity:  identity,
		OnStartedLeading: func(ctx context.Context) {
			leading.Store(true)
			metrics.SetLeader(true)
			log.Info("acquired the leader lease; this replica now decides and writes",
				slog.String("lease", namespace+"/"+name), slog.String("identity", identity))
			acquired <- ctx
		},
		OnStoppedLeading: func() {
			leading.Store(false)
			metrics.SetLeader(false)
		},
		OnNewLeader: func(holder string) {
			if holder != identity {
				log.Info("standing by: another replica holds the leader lease",
					slog.String("lease", namespace+"/"+name), slog.String("leader", holder))
			}
		},
	})
	if err != nil {
		return nil, err
	}

	log.Info("lease client built",
		slog.String("writable_path", kubernetesaccess.LeasePath(namespace, name)),
		slog.String("identity", identity),
		slog.String("lease_duration", kubernetesaccess.DefaultLeaseDuration.String()),
		slog.String("detail", "the transport permits reading and renewing this one lease and creating it by name, "+
			"and refuses every other request"),
	)

	return &leadership{election: election, acquired: acquired, leading: leading}, nil
}

// lead runs loop only while this process holds the lease.
//
// The order of the two stops is the point. The elector releases the lease the
// moment its context is cancelled, so the loop must have returned first: a
// release while a reconcile is still writing would let a standby take over
// with the old leader mid-write. Cancelling the loop and waiting for it, then
// releasing, means the lease is only ever given up by a process that has
// stopped acting under it.
//
// It returns nil on shutdown, and errLostLeadership when the lease was lost
// while the loop ran.
func lead(ctx context.Context, l *leadership, loop func(context.Context) error) error {
	electionCtx, cancelElection := context.WithCancel(context.Background())
	electionDone := make(chan struct{})
	go func() {
		defer close(electionDone)
		l.election.Run(electionCtx)
	}()
	release := func() {
		cancelElection()
		<-electionDone
	}

	var leaderCtx context.Context
	select {
	case <-ctx.Done():
		release()
		return nil
	case <-electionDone:
		// client-go campaigns until its context is cancelled, so this is an
		// elector that gave up on its own; there is nothing to lead.
		cancelElection()
		return errors.New("leader election stopped before the lease was acquired")
	case leaderCtx = <-l.acquired:
	}

	loopCtx, cancelLoop := context.WithCancel(leaderCtx)
	stopOnShutdown := context.AfterFunc(ctx, cancelLoop)
	loopErr := loop(loopCtx)
	stopOnShutdown()
	cancelLoop()

	lost := ctx.Err() == nil && leaderCtx.Err() != nil
	release()

	if lost {
		return errLostLeadership
	}
	return loopErr
}

// standbyReady is readiness for a replica that may be standing by.
//
// A standby is ready once its caches have synced: it is healthy and prepared
// to take over, which is what "intentionally passive" means in architecture
// § 8.3. Reporting it unready would stall every rolling update, because the new
// replica can only lead after the old one has gone.
func standbyReady(leading func() bool, synced func() bool, leaderReady observability.ReadinessCheck) observability.ReadinessCheck {
	return func() error {
		if leading() {
			return leaderReady()
		}
		if !synced() {
			return errors.New("informer caches have not synced")
		}
		return nil
	}
}

// forceReadOnlyForOnce makes -once read-only whatever the configuration says.
//
// -once is for inspecting a cluster by hand. Left to the configuration, a
// single reconcile against a live config would build the actuator and could
// write — outside the lease, beside a running leader, and from a laptop.
func forceReadOnlyForOnce(cfg *config.Config, opts options) bool {
	if !opts.once || cfg.Controller.DryRun {
		return false
	}
	cfg.Controller.DryRun = true
	return true
}
