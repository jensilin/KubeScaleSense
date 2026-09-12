// Package scaling is the pure core of the controller: it decides what the
// replica count should be and never changes it.
//
// Decide is a function of its arguments and nothing else. It performs no I/O,
// reads no clock, touches no package-level state, and sleeps never — every
// input, including the current time and the controller's own history, arrives
// through the Snapshot (I-5). That is what makes the combinatorially
// interesting behaviour — "node drained, metrics stale, backoff armed, 3 of 4
// pods fit" — a five-line fixture instead of a flaky cluster test.
//
// The algorithm is specified in docs/scaling-algorithm.md and implemented here
// in the same order, because the order is load-bearing rather than incidental:
//
//	Step 1  desired replicas   max(backlog target, utilization target), clamped
//	Step 2  direction          deadband, then per-decision step limits
//	Step 3  stability gates    G0..G12, first match wins
//	Step 4  feasibility gate   fit capacity vs. requested delta
//
// Two invariants are worth stating here because a future refactor could quietly
// break either one:
//
// Exactly one reason code is returned per call, and it is the *first* matching
// guard. Guard order is what makes the reported reason the most actionable one
// rather than an arbitrary one — reporting a cooldown when pods are already
// Pending would send an operator to the wrong place (I-6).
//
// Fit capacity is supplied by the caller for every snapshot, including ones that
// will return HoldBackoff. The backoff reset is level-triggered on observed
// capacity, so evaluating feasibility after the backoff gate would make the
// reset condition unobservable and silently degrade the design into a
// timer-based backoff (I-10, DR-01).
package scaling
