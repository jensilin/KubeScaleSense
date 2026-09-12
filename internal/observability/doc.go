// Package observability owns the Prometheus registry, the health endpoints, and
// the one-line-per-reconcile decision log.
//
// The contract is architecture § 8, and the metric names there are treated as a
// stable API: they appear in the demo dashboards, in the E2E assertions, and in
// the tuning guidance, so a rename is a breaking change rather than a cleanup.
//
// The design bias is to export the *derivation* of a decision, not just its
// outcome. An operator asking "why is it holding at 4 replicas when the backlog
// is 400?" should be able to answer it from the metrics and one log line:
// desired uncapped, desired clamped, fit capacity, the blocking dimension, the
// candidate node count, and why nodes were excluded are all exported for exactly
// that question.
//
// Three metrics from architecture § 8.1 report zero throughout Phase 1, and that
// is a truthful value rather than a stub: kss_scale_actions_total and
// kss_pending_pod_remediations_total count real mutations, of which a dry-run
// phase performs none. kss_leader is deliberately *not* registered, because
// leader election arrives with P4 and a gauge claiming leadership that no lease
// backs would be a fabrication.
package observability
