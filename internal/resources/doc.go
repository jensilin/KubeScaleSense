// Package resources will own the fit-capacity model: candidate node filtering,
// the effective pod request, per-node free requestable resources, and the
// per-node floored fit count.
//
// Not implemented: this package is introduced in Phase 1 (observation), where
// it is also the subject of the manual validation gate that compares its output
// against `kubectl describe node` before any actuation code exists.
//
// The specification it implements is docs/resource-calculation.md, which is
// storage- and workload-agnostic by design.
package resources
