// Package controller will own the reconcile loop, the actuator that writes the
// scale subresource, the cooldown and stabilization-window state, and the
// Pending and unhealthy-pod watchdogs.
//
// Not implemented: the loop is assembled in Phase 1 in dry-run only, and gains
// the ability to write in Phase 3 (actuation).
//
// Phase 0 has no reconcile loop. cmd/kubescalesense waits for a shutdown
// signal and exits; it does not tick.
package controller
