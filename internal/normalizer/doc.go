// Package normalizer is the demonstration workload: a small, stateless HTTP
// service that turns raw PM records into normalized ones.
//
// It is deliberately not part of KubeScaleSense. The controller observes it and
// nothing more; this package imports nothing from internal/ and knows nothing
// about Kubernetes, scaling, or the controller's configuration. That separation
// is what makes the demonstration honest — a workload that cooperated with its
// autoscaler would prove nothing about autoscaling a workload that does not.
//
// Three properties carry the whole demonstration:
//
// Normalization is a pure function of the request body (WR-03). A retried
// request therefore produces a byte-identical result, which is what makes
// NiFi's at-least-once retry safe and is the entire durability argument of the
// v0.2 pipeline (D-03, FS-29). The response body depends on the input and on
// nothing else — not on the pod, not on the time, and not on the configured
// processing cost.
//
// Nothing is shared between requests or between pods (WR-01). There is no
// database, no queue, no cache, and no per-pod identity, so any replica can
// serve any request and pod loss costs a request rather than data (D-04).
//
// The work is real CPU work, and its amount is configurable (P2 § 4). Pressure
// rises because records are genuinely queueing behind a bounded number of
// processing slots, not because a counter was told to go up. An autoscaler
// demonstrated against a faked metric demonstrates nothing.
//
// The pressure signal the controller scrapes is queued + in-flight requests
// (ADR-22, WR-07). Both halves matter: in-flight alone saturates at the
// concurrency limit and stops growing precisely when demand starts to exceed
// capacity, which is the moment the signal most needs to keep rising.
package normalizer
