// Package kubernetes will own the typed client-go clientset, the Node, Pod,
// Deployment and ReplicaSet informers, the scale-subresource write path, and
// the event recorder.
//
// Not implemented: this package is introduced in Phase 1 (observation) and
// gains its write path in Phase 3 (actuation). See
// docs/architecture.md § 4.2 for the contract it must satisfy.
//
// Phase 0 makes no Kubernetes API calls of any kind, so this file exists only
// to fix the package boundary.
package kubernetes
