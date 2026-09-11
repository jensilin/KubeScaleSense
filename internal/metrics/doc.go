// Package metrics will produce the demand half of the decision snapshot: the
// workload-pressure signal behind a replaceable source interface, and pod CPU
// utilization from metrics.k8s.io averaged over Ready and warm pods.
//
// Not implemented: this package is introduced in Phase 1 (observation) with the
// `synthetic` and `none` signal sources, which are enough to exercise the whole
// demand path before any pipeline exists. The `http` source, which scrapes the
// Normalizer's own metrics endpoint, arrives in Phase 2.
//
// The source interface is written before its implementations on purpose; see
// docs/architecture.md ADR-22 and docs/implementation-plan.md § P1.
package metrics
