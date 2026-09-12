// Package metrics produces the demand half of the decision snapshot.
//
// It has two independent halves, and their independence is the point.
//
// The workload-pressure signal sits behind the WorkloadSignal interface and
// knows nothing about Kubernetes. The controller's subject is the scaling
// decision; where the pressure number comes from is the workload's business, so
// swapping a scripted file for an HTTP scrape, a queue depth, or a broker
// backlog changes one implementation of one interface and nothing else (FR-36,
// I-22, ADR-22).
//
// The interface was written before its implementations, deliberately. If
// `synthetic` had been added later as a test double it would have ended up
// shaped like a test double; as one of the first real implementations it keeps
// the interface narrow enough that a future source is a drop-in.
//
// Pod CPU utilization comes from metrics.k8s.io and is inherently
// Kubernetes-shaped, so it lives behind its own small interface over a
// PodMetrics lister rather than pretending to be portable.
//
// The rule both halves obey: an unavailable signal is reported as unavailable,
// never as zero. Reading "no answer" as "no work" is how an autoscaler scales a
// busy system to the floor during a monitoring outage, and it is the single
// most important thing this package does not do (FR-38, I-7).
//
// Phase 1 implements the `synthetic` and `none` sources, which is enough to
// exercise the entire demand path — including staleness and unavailability —
// before the demonstration pipeline exists. The `http` source arrives in P2.
package metrics
