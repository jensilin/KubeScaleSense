// Package scaling will hold the decision engine: the Snapshot and Decision
// types, the reason-code enum, and the pure function
// Decide(Snapshot, Config, now) -> Decision.
//
// Not implemented: this package is introduced in Phase 1 (observation), where
// it runs in permanent dry-run.
//
// Its defining constraint is purity — no I/O, no clock reads, no API access, so
// that the project's central safety claim can be asserted by a property test
// over generated states (docs/architecture.md P-2, docs/test-plan.md UT-22).
package scaling
