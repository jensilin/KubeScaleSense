// Package controller is the only stateful, side-effecting component — and in
// Phase 1 the side effects are limited to logs and metrics.
//
// Each tick it reads the cluster and the workload signal, computes feasibility,
// calls the pure decision engine, and reports the result. It does not write to
// Kubernetes, and it cannot: it holds a read-only ClusterReader and an Actuator
// whose only implementation in this repository logs what it would have done.
//
// The state kept here is the small amount the engine needs but cannot derive
// from the cluster: when we last scaled in each direction, the ring buffer of
// desired-replica samples backing the stabilization window, the backoff state,
// and the last-good replica count. None of it is persisted. After a restart the
// controller re-derives everything from the cluster and behaves as if freshly
// cooled down, which is both simpler and safer than trusting a stale on-disk
// view of a cluster that has moved on (NFR-08).
//
// One bookkeeping decision deserves stating, because it is the only place where
// dry-run behaviour is not simply "the live path minus the write". When a
// decision would have scaled, the cooldown timers advance as though it had.
// Without that, a dry run would re-report ScaleUp every interval forever — the
// replica count never changes, so demand never falls — and the cooldown and
// backoff machinery would go entirely unexercised in the phase built to
// validate it. The replica count still never converges, which is honest: no
// write happened.
package controller
