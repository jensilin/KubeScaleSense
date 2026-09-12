// Package resources answers one question and deliberately no others: given the
// target's pod template, how many additional replicas could Kubernetes place
// right now?
//
// It is the executable form of docs/resource-calculation.md, and it contains no
// policy. Whether to *use* the capacity it reports belongs to internal/scaling
// (architecture § 4.4). The four steps mirror that document exactly:
//
//	Step 1  candidate node set        — Candidates
//	Step 2  effective pod request     — EffectivePodRequest
//	Step 3  per-node free requestable — nodeFree
//	Step 4  fit capacity              — Calculate
//
// Two properties are load-bearing and are the reason this package holds no I/O
// and no clock of its own:
//
// Flooring happens per node, before summing. That ordering is what encodes
// fragmentation, and reversing it produces the single most common capacity
// miscalculation — ten nodes with 400m free each do not fit a 500m pod
// (resource-calculation § 1).
//
// Feasibility is computed from allocatable minus requests minus reserve, never
// from utilization. Utilization is not the scheduler's currency: a pod
// requesting 2 cores and burning 50m still holds 2 cores of scheduling budget
// (I-1, ADR-05).
package resources
