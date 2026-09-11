// Package observability will own the Prometheus metric set (the canonical
// kss_* names in docs/architecture.md § 8.1), the /healthz and /readyz
// handlers, and the one-line-per-reconcile decision log.
//
// Not implemented: this package is introduced in Phase 1 (observation).
//
// Phase 0 configures structured logging only, via config.NewLogger, and binds
// no listeners — controller.metricsAddr and controller.healthAddr are validated
// but not yet served.
package observability
