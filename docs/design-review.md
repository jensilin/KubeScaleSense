# KubeScaleSense — Design Review (v0.1 design set)

> Reviewer: senior Kubernetes engineer · Scope: correctness of the resource-aware autoscaling concept
> Reviewed: `requirements.md`, `architecture.md`, `scaling-algorithm.md`, `resource-calculation.md`,
> `failure-scenarios.md`, `test-plan.md`, `implementation-plan.md` (design set, pre-implementation)

Related documents: [requirements](requirements.md) · [architecture](architecture.md) ·
[scaling-algorithm](scaling-algorithm.md) · [resource-calculation](resource-calculation.md) ·
[failure-scenarios](failure-scenarios.md) · [test-plan](test-plan.md) · [implementation-plan](implementation-plan.md)

**Verdict.** The central thesis is sound and the resource model is the right one: requests against
allocatable, per node, floored per node, filtered by hard predicates. Sixteen findings follow. Two are
critical — one of them makes a headline claim in the current design *unreachable as specified*, and one makes
the demo pipeline's data-safety claim unimplementable on the proposed environment. All fixes have been folded
into the design set; this document records the reasoning and the resulting limits.

> **Status update (v0.1.2).** All sixteen findings are closed. [DR-12](#dr-12-the-poc-work-store-cannot-provide-the-claimed-semantics-on-the-proposed-environment)
> was the last one requiring an architectural decision rather than a specification fix, and it is now resolved
> by replacing the shared-filesystem work store with a PostgreSQL work-item table
> ([ADR-18](architecture.md#adr-18-what-is-the-durable-work-store-for-the-poc-pipeline)). The settled
> architecture is [architecture § 11](architecture.md#11-phase-1-architecture-baseline); **no Phase 1 blockers
> remain**.

The most important conceptual correction is [§2](#2-three-statements-that-are-not-the-same-thing): the design
was, in places, sliding between three different statements about cluster capacity that are not equivalent.

---

## 1. What the review confirms as correct

Stated first, so the findings can be read as refinements rather than a rewrite.

| Area | Assessment |
| --- | --- |
| **Resource basis** | `allocatable − Σ requests − reserve`, per node, is the correct basis and mirrors `NodeResourcesFit`. The explicit rejection of `allocatable − usage` is the single most important decision in the design ([ADR-05](architecture.md#adr-05-capacity-allocatable-or-requested)) |
| **Per-node flooring before summing** | Correct, and the reason fragmentation is handled. Aggregate-then-divide is the classic error and is correctly confined to human-facing metrics |
| **Independent resource dimensions** | `min(cpuFit, memFit, slotFit)` is right; reporting the binding dimension is what makes a HOLD actionable |
| **Greedy counting for identical pods** | Correct, and stronger than the design claimed — see the additivity argument in [§3, DR-16](#dr-16-greedy-fit-counting-was-under-justified) |
| **Terminating pods still counted** | Correct and matches scheduler behaviour: a pod holds its resources until deleted, not until `deletionTimestamp` is set |
| **Taint semantics** | Correctly scoped: `NoSchedule`/`NoExecute` are hard, `PreferNoSchedule` is not, and the kubelet pressure taints are handled for free by the same code path — a genuinely elegant consequence |
| **Fail-safe = freeze** | Right choice, consistently applied. "Never scale down on unknown state" is the correct invariant |
| **Purity of the decision engine** | The `Snapshot → Decision` boundary is what makes this design testable at all, and the property test (UT-22) is the right instrument for the core safety claim |
| **Durability as a workload property** | Correct framing. An autoscaler cannot make a lossy pipeline safe, and the design says so instead of over-claiming |

---

## 2. Three statements that are not the same thing

The design set used "resources are available" in places where it meant three distinct things. Making the
distinction explicit changes what the POC is allowed to claim.

| | L1 — "the cluster has spare resources" | L2 — "the cluster can probably schedule *this* pod" | L3 — "the cluster actually scheduled the pod" |
| --- | --- | --- | --- |
| **Kind of statement** | Aggregate, resource-shaped | Per-pod, per-node, predicate-filtered estimate | Observed fact |
| **Derived from** | Sums over the cluster: capacity, allocatable, or usage | This pod's effective request vs. per-node `allocatable − requests − reserve`, on nodes passing modelled predicates | `PodScheduled=True` on a real pod, assigned by `kube-scheduler` |
| **In KubeScaleSense** | `kss_free_requestable_cpu_millicores` (**reporting only**) | `kss_fit_capacity_pods` — **the only input to the gate** | Pod status observed by the watchdog |
| **Guarantees** | Almost nothing about admission | That no *modelled* predicate rejects N pods, at the time of the read | That it happened |
| **Why it can be wrong** | Fragmentation, taints, selectors, existing commitments, pod-count limits | Unmodelled predicates, quota, races against other schedulers of work | It cannot be wrong; it is the ground truth |
| **Failure if confused with L3** | "60 % idle" clusters that can schedule nothing | Pods Pending despite a passing gate | — |

Three consequences the design now states explicitly:

1. **L1 is never a decision input.** It is exported for humans, and [FS-04](failure-scenarios.md#fs-04-fragmentation-free-resources-exist-but-nothing-fits)
   documents that L1 and L2 can *correctly* disagree (900 m free, zero placements).
2. **L2 is a probability, not a promise.** The word "probably" is load-bearing: the gate reduces the chance of
   a Pending pod, it does not eliminate it. Every claim in the design set is now phrased against L2, and the
   POC's guarantee is "never *knowingly* infeasible", not "never Pending".
3. **L3 must be verified, not assumed.** The gap between L2 and L3 is closed only by observing pod status —
   which is why the Pending watchdog is a correctness requirement rather than an operational nicety, and why
   the new health guard ([DR-06](#dr-06-scheduled-but-unhealthy-pods-cause-unbounded-scale-up)) extends
   verification past scheduling into actually running.

---

## 3. Findings

Severity: **Critical** = a stated guarantee is false or unreachable · **High** = wrong behaviour in a
plausible scenario · **Medium** = correctness gap with bounded impact · **Low** = imprecision.

### DR-01: Backoff gate makes the "recovery in one interval" claim unreachable

**Severity: Critical.** Guard order + reason-code correctness.

The design claims backoff reset is level-triggered on observed capacity increase, so that adding a node
recovers scaling within one interval "not one backoff period"
([ADR-11](architecture.md#adr-11-how-do-we-avoid-repeatedly-attempting-an-impossible-scale),
[scaling-algorithm § 9.2](scaling-algorithm.md#92-reset-is-level-triggered-on-capacity-not-timer-driven), and
asserted by E2E-03). But the specified guard order evaluates backoff (G6) **before** feasibility (G8), and both
[architecture § 5.1](architecture.md#51-decision-pipeline) and
[resource-calculation § 9](resource-calculation.md#9-complexity-and-performance) state that fit capacity is
computed only after the up-gates, as a performance optimization.

So while backoff is armed, `F` is never recomputed, the reset condition can never be observed, and the
controller waits out the full 15-minute backoff even though the informer saw the new node immediately. The
claim is false as specified, and E2E-03 would fail.

The design set was also internally inconsistent here: [architecture § 5.2](architecture.md#52-one-reconcile-end-to-end)
shows `Feasibility()` being called on *every* reconcile, contradicting §5.1.

**Fix (applied).** Feasibility is computed whenever the decision reaches the direction check and the direction
is up — including while backoff is armed. `BackoffState` records `fitCapacityAtArm`, and the reset fires when
`F > fitCapacityAtArm` or `F >= Δ_req`. The optimization is restated correctly: the node walk is skipped only
when G0–G4 already settle the decision (invalid snapshot, conflict, stale signals, clamp, deadband), which is
the true steady-state path. Cost is unchanged in practice: `O(N + P)` arithmetic on cached objects.

### DR-02: Partial scale-up both arms and resets the backoff

**Severity: High.** Specification contradiction.

`ScaleUpPartial` is specified to arm backoff, while the reset conditions include "a scale action succeeded".
A partial scale-up is a successful scale action, so the two rules contradict: an implementer following the
reset list would clear the backoff immediately after arming it, and a cluster stuck at partial capacity would
retry every interval — exactly the retry storm backoff exists to prevent
([FS-07](failure-scenarios.md#fs-07-repeated-impossible-scale-attempts)).

**Fix (applied).** Only a **complete** scale-up (`F >= Δ_req`) resets backoff. `ScaleUpPartial` arms or
advances it, because a partial scale-up is evidence of a shortfall, not of recovery.

### DR-03: Demand-driven scale-down can fire while replicas are still starting

**Severity: High.** Oscillation and premature pod termination.

`desiredUtilization = ceil(R_ready × U_cpu / U_target)` uses **Ready** replicas as the base — deliberately, to
avoid starting pods diluting the average. But that choice has an unexamined consequence in the *other*
direction: while a scale-up is settling, `R_ready < R_cur`, so `desiredUtilization` is computed from a smaller
base and can land below `R_cur` even at 100 % utilization. With a simultaneous backlog lull, `desiredRaw <
R_cur` and the controller can remove pods it added seconds earlier — scale-up oscillation with a plausible
trigger, not just a theoretical one.

Worked case: `R_cur = 6`, `R_ready = 2` (four pods still starting), `U_cpu = 100 %`, `U_target = 70 %` →
`desiredUtilization = ceil(2 × 100/70) = 3`. Backlog has just been drained to 80 → `desiredBacklog = 2`. So
`desiredRaw = 3 < 6`, and only the cooldown stands between this and a scale-down.

**Fix (applied).** New gate: a **demand-driven** scale-down requires `readyReplicas == currentReplicas`
(`HoldReplicasSettling`). Watchdog remediation writes are explicitly exempt, since a revert must be able to
remove a pod that is by definition not Ready ([DR-06](#dr-06-scheduled-but-unhealthy-pods-cause-unbounded-scale-up)
covers the case where pods never become Ready at all).

### DR-04: Stabilization-window gaps permit a "blind" scale-down

**Severity: High.** The scale-down guard can be satisfied by *missing* data.

Scale-down requires `max(desired over W) < R_cur`, with `desiredHistory` appended once per reconcile. But
reconciles that return early — `HoldStaleMetrics`, `ErrorAPIFailure`, `HoldScalingConflict` — never compute
`desiredRaw` and so append nothing. After a five-minute metrics outage the window contains only the handful of
samples recorded since recovery. If demand happened to be low in those samples, `max()` over a nearly-empty
window is low, the guard passes, and the controller scales down on the strength of five minutes of *no
information*.

This is the same class of bug as interpreting "no backlog data" as "no backlog", which the design correctly
rejects elsewhere — it just re-entered through the history buffer.

**Fix (applied).** Scale-down additionally requires the window to be **covered**: at least
`scaleDownWindowCoverage` (default 0.8) of the `ceil(W / interval)` expected samples must be present, and the
oldest must be at least `W` old. Gaps are treated as unknown, which blocks scale-down
(`HoldStabilizationWindow`). This also subsumes the restart case (FS-17) as a special case of an empty window
rather than as separate reasoning.

### DR-05: New-pod warmup dilutes the utilization average

**Severity: Medium.** Signal correctness.

Averaging over Ready pods fixes the *starting*-pod dilution but not the *warm-up* dilution: a pod that has
passed its readiness probe but has not yet claimed work reports near-zero CPU while counting fully in the
average. During a scale-up this depresses `U_cpu`, lowering `desiredUtilization` at the moment it matters.
Upstream HPA has two dedicated knobs for exactly this (`--horizontal-pod-autoscaler-initial-readiness-delay`
and the CPU initialization period), which is good evidence the effect is real rather than theoretical.

**Fix (applied).** Pods Ready for less than `workload.podWarmupPeriod` (default 60 s) are excluded from the
utilization average. If that leaves no eligible pods, the utilization signal is reported unavailable rather
than fabricated — the engine then decides what a missing signal means, consistent with the `Signal[T]` design.

### DR-06: Scheduled-but-unhealthy pods cause unbounded scale-up

**Severity: High.** The watchdog watches the wrong half of the failure space.

The design detects pods that never *schedule*. It has nothing for pods that schedule and then never *become
useful*: `ImagePullBackOff`, `CrashLoopBackOff`, a failing readiness probe, a bad config or missing secret.
Consequences compound in the wrong direction:

- Those pods consume real cluster capacity (they are scheduled, so their requests are committed) while
  processing nothing.
- The backlog therefore keeps growing, so `desiredBacklog` keeps rising.
- The controller keeps scaling up, adding more broken replicas, consuming more capacity — and, because the
  requests are real, potentially starving *other* workloads on the cluster.

A resource-aware autoscaler that reacts to a broken image by consuming the cluster is a worse failure than the
Pending pods the project set out to prevent, and nothing in the v0.1 design stops it.

**Fix (applied).** New guard: if any pod of the target's current ReplicaSet is `PodScheduled=True` but not
Ready for longer than `pending.podStartupTimeout` (default 300 s), scale-ups are blocked
(`HoldUnhealthyPods`) with a `Warning` event. Deliberately **no** auto-revert, unlike the Pending case: these
pods may recover, and removing replicas from a partially-broken pool can remove the working ones. Holding
plus alerting is the honest response — the controller cannot fix a broken image, and it should stop making the
situation more expensive. Recorded as [FS-24](failure-scenarios.md#fs-24-scheduled-but-unhealthy-pods).

### DR-07: External writers of `spec.replicas` are undetected

**Severity: High.** The conflict analysis covers HPAs and misses the more common case.

[FS-16](failure-scenarios.md#fs-16-competing-controller-on-the-same-target) handles a competing HPA, but the
likelier conflict in a real deployment is a **GitOps controller**: Argo CD or Flux reconciling `replicas: 2`
from git will revert every scale-up within seconds, and the design's only response would be to scale up again
next interval. That is an oscillation loop with an external actor, invisible to the HPA check, burning API
budget and terminating pods on every cycle. Manual `kubectl scale` and other operators fall in the same class.

Note that `resourceVersion` preconditions do **not** help here: the external write succeeds, we observe the new
value, and our next write is against a fresh version. Optimistic concurrency prevents lost updates, not
disagreements about intent.

**Fix (applied).** The controller records `lastWrittenReplicas`. If observed `spec.replicas` differs from that
value and we did not write it, an external change is inferred: adopt the observed value as the new baseline
(level-triggered), reset cooldown timers and `desiredHistory`, and emit `ExternalScaleDetected` once. If drift
recurs more than `scaling.externalChangeTolerance` times (default 3) inside the stabilization window, stop
acting entirely (`HoldExternalChange`) — the same "refuse rather than fight" stance as the HPA case, for the
same reason: whichever controller writes last would win, so there is no safe way to compete. Recorded as
[FS-22](failure-scenarios.md#fs-22-external-actor-changes-the-replica-count) and
[ADR-17](architecture.md#adr-17-how-do-we-handle-other-writers-of-the-replica-count).

### DR-08: Rollouts and `maxSurge` are unaccounted for

**Severity: Medium.** Transient over-commitment during a deployment update.

Fit capacity is computed against current pods, which correctly includes surge and terminating pods of an
in-flight rollout. What is *not* modelled is that a scale-up requested **during** a rollout will itself be
surged: with `maxSurge: 25 %`, asking for 4 more replicas transiently requires up to 5 pods' worth of
resources. The gate approves 4, the Deployment controller asks for 5, and the extra one is the pod that goes
Pending.

**Fix (applied).** While a rollout is in progress (`metadata.generation != status.observedGeneration`, or
`updatedReplicas != replicas`), both directions hold with `HoldRolloutInProgress`. Rollouts are short and
operator-initiated, so a brief scaling pause is the cheap correct answer; the alternative — requiring
`F >= Δ_req + surge(Δ_req)` — is more permissive but adds a second, subtler resource calculation for a rare
window, and was rejected as unnecessary complexity for the POC.

### DR-09: RBAC omits ReplicaSet reads that the watchdog requires

**Severity: Medium.** The design cannot do what it says with the permissions it grants.

The Pending watchdog is specified over "pods owned by the target's **current** ReplicaSet"
([architecture § 4.6](architecture.md#46-controller-internalcontroller)), and distinguishing current from old
ReplicaSets is essential during a rollout — otherwise a doomed pod of a superseded ReplicaSet triggers a
revert of a healthy new one. But the RBAC table grants no access to `apps/replicasets`.

**Fix (applied).** Added `apps/replicasets: get, list, watch`. Also corrected a related over-claim: without
`events` read permission (deliberately not granted) the controller cannot quote the `FailedCreate` message
that names a quota, so the [FS-10](failure-scenarios.md#fs-10-namespace-resourcequota-blocks-pod-creation)
event text now says what the controller can actually observe — replicas not materialising — and points the
operator at quota and `LimitRange` as candidates.

### DR-10: `PodDisruptionBudget` does not protect against scale-down

**Severity: High.** A stated data-protection mechanism does not apply to the case it was listed under.

[D-07](requirements.md#7-data-loss-protection-assumptions) presented a PDB as protection for the processing
pool, in a list about scale-down and termination safety. PDBs are enforced **only by the Eviction API** — node
drains, the descheduler, cluster-autoscaler node consolidation. A ReplicaSet scale-down deletes pods directly
and is entirely unaffected by any PDB, no matter how strict. An implementer trusting D-07 would believe
scale-down was bounded by `minAvailable` when it is not.

**Fix (applied).** D-07 is rescoped to what a PDB actually does — protecting the pool during node-level
disruption ([FS-13](failure-scenarios.md#fs-13-node-failure-or-drain-removes-workers)) — and scale-down safety
is attributed solely to its real mechanisms: `maxScaleDownStep`, deletion-cost ordering, graceful drain
(`preStop` + grace period), and lease-based reclaim as backstop.

### DR-11: `pod-deletion-cost` ordering was described too loosely

**Severity: Low.** Imprecision that could mislead.

The design implies deletion cost selects the victim. In reality the ReplicaSet controller sorts candidates by,
in order: unassigned pods, then `Pending` before `Running`, then **not-Ready before Ready**, then lower
deletion cost, then colocation, restart count, and age. So deletion cost only decides *within the Ready
cohort*, and any not-Ready pod is removed first regardless of its cost — including a busy pod experiencing a
brief readiness blip. It also requires the `PodDeletionCost` feature gate (beta, default-on since 1.22).

**Fix (applied).** Documented precisely, and the ordering restated so the real guarantee is clear: deletion
cost is a preference among healthy pods, while the actual safety guarantee for in-flight work is graceful
drain plus lease-based reclaim. This does not change the design, only what it promises.

### DR-12: The POC work store cannot provide the claimed semantics on the proposed environment

**Severity: Critical.** The data-safety argument rests on an unimplementable primitive.

[D-02](requirements.md#7-data-loss-protection-assumptions) and
[D-03](requirements.md#7-data-loss-protection-assumptions) depend on **atomic rename** in a store shared by all
processing pods. Both proposed options break that:

- **RWX PVC on kind.** kind ships `local-path-provisioner`, which provides node-local `ReadWriteOnce`
  hostPath volumes. There is no RWX storage class in a default kind cluster. Worse than an outright failure,
  the manifest can *appear* to work: each pod binds a local directory, so pods on different nodes see
  different contents. Items written by NiFi become invisible to workers on other nodes and sit unprocessed —
  indistinguishable from data loss during a demo, and directly at odds with the multi-node topology the
  fit-capacity demo requires.
- **MinIO / S3.** Object stores have no atomic rename; "rename" is copy-then-delete. Two pods can therefore
  both "claim" the same item, and the claim protocol in D-02 provides no mutual exclusion at all. Correctness
  would rest entirely on D-03 idempotency, which is a much weaker position than the design implies.

**Fix (applied, then superseded).** The review promoted Q-2 from an open question to a blocking prerequisite
for P2, recommending either an in-cluster NFS RWX provisioner or MinIO with a conditional-write claim.

**Closed (v0.1.2) — neither option was taken.** The architecture-closure step rejected both and replaced the
primitive instead of repairing it: the work store is a **PostgreSQL work-item table**, items are claimed with
`FOR UPDATE SKIP LOCKED` under a lease, and output plus acknowledgement commit in **one transaction**
([ADR-18](architecture.md#adr-18-what-is-the-durable-work-store-for-the-poc-pipeline)).

Why that is the stronger answer, and not merely a different one:

- **Exclusion becomes a database guarantee** rather than an application convention. `SKIP LOCKED` is exactly
  this problem's primitive; NFS rename is a primitive whose correctness depends on mount options and
  duplicate-request caching.
- **The dual-write problem disappears.** Because the output insert and the acknowledgement are one commit, a
  partial output and an acknowledged-but-unwritten item become *unrepresentable*. Every other candidate — NFS,
  object storage, and message brokers alike — splits ack from output across two systems.
- **Idempotency is enforced by a `PRIMARY KEY`**, so [D-03](requirements.md#7-data-loss-protection-assumptions)
  is provable instead of aspirational.
- **The reaper disappears entirely**: lease expiry is handled inside the claim query, which also dissolved
  [Q-3](implementation-plan.md#8-open-questions).
- **Less infrastructure, not more**: one container and one `ReadWriteOnce` PVC, with no CSI driver, no RWX
  StorageClass, and no object store — on the same kind cluster the demo already needs.

The change also closed [Q-1](implementation-plan.md#8-open-questions): with work landed in the store, the
backlog signal is the store's claimable count, which deletes the NiFi REST client and its credentials from the
controller ([ADR-19](architecture.md#adr-19-where-does-the-backlog-signal-come-from)).

Two new failure modes were *introduced* and are recorded rather than glossed over: the store is a single point
of failure and can saturate ([FS-28](failure-scenarios.md#fs-28-work-store-unavailable-or-saturated)), and a
lease that expires while its worker is alive causes duplicate processing — wasted CPU, never wrong output
([FS-29](failure-scenarios.md#fs-29-lease-expires-while-the-worker-is-still-alive)). Verified by DI-08
(exclusive claiming), DI-09 (transactional ack under `SIGKILL`), and DI-10 (saturation), all gating P2.

### DR-13: Leader handoff interacts with external-change detection

**Severity: Medium.** Interaction between two otherwise-correct mechanisms.

A new leader has no `lastWrittenReplicas`, so the DR-07 detector would classify the *previous leader's*
legitimate write as an external change on the first reconcile, emitting a spurious event and resetting state
that was already reset. Separately, the lease-loss window means an outgoing leader could theoretically have a
write in flight as the new one starts; `resourceVersion` preconditions make the loser's write fail, which is
correct, but the design should say so rather than leave it implied.

**Fix (applied).** On startup and on acquiring leadership, the controller adopts observed `spec.replicas` as
its baseline **silently** (no external-change event, no drift counter increment). The concurrent-write case is
documented as resolved by the precondition: one write wins, the loser re-reads and re-decides
([FS-26](failure-scenarios.md#fs-26-leadership-handoff-races-with-an-in-flight-write)).

### DR-14: Staleness measured only from local receive time misses a frozen source

**Severity: Medium.** A stuck metrics pipeline reads as fresh.

`sampledAt` was underspecified. If it is the *source's* timestamp, clock skew between NiFi and the controller
distorts staleness in both directions. If it is the controller's *receive* time, a frozen metrics-server that
happily serves a ten-minute-old sample looks perfectly fresh, and the controller acts on stale data believing
it has satisfied FR-09.

**Fix (applied).** Freshness uses **both**: local receive age (authoritative, immune to skew) **and** the
source-reported sample age where the source provides one — `PodMetrics.timestamp`/`window` for metrics-server.
A signal is stale if either exceeds `metricsStaleAfter`, with a small skew tolerance on the source clock. Folded
into [FS-08](failure-scenarios.md#fs-08-stale-resource-or-metric-information).

### DR-15: Floating-point arithmetic weakens the determinism claim

**Severity: Low.** [NFR-07](requirements.md#5-non-functional-requirements) asserts identical snapshots produce
identical decisions.

`ceil(R_ready × U_cpu / U_target)` in float64, plus a float EWMA carried across reconciles, makes exact
reproducibility depend on evaluation order and platform rounding at exact boundaries — which is precisely
where table-driven tests place their cases.

**Fix (applied).** Integer/rational arithmetic in milli-units for the utilization ratio (the approach upstream
HPA takes), with rounding specified once; the EWMA state is kept in integer milli-items and is disabled by
default in unit tests. Cheap, and it makes the determinism claim exact rather than approximate.

### DR-16: Greedy fit counting was under-justified

**Severity: Low** (documentation), but it removes a real doubt about the model.

The design asserts greedy per-node counting is "exact for the modelled predicates" without argument. Since
`kube-scheduler` places pods **online and without backtracking**, a reviewer is entitled to ask whether the
scheduler can strand capacity that the offline count promised.

It cannot, and the reason is worth recording: because all target pods are identical
([A-02](requirements.md#6-workload-and-environment-assumptions)), placing one pod on any node with
`fit(n) >= 1` reduces that node's count by exactly one and the cluster total by exactly one — `fit(n)` is
defined by floor division of free resources by a *fixed* pod size, so the arithmetic is exactly additive.
No placement choice among candidate nodes can strand a unit that the count promised, so any online greedy
order achieves the counted total. This holds only while the pods are identical and nothing else consumes
capacity concurrently — which is exactly what `perNodeReserve*` and `fitCapacityMarginPods` are for.

**Fix (applied).** The argument and its two preconditions are recorded in
[resource-calculation § 5.1](resource-calculation.md#51-why-greedy-counting-is-safe-for-identical-pods).

### Also reviewed, no change required

| Concern | Finding |
| --- | --- |
| Scheduler-level default topology constraints | A cluster admin can configure hard `defaultConstraints` for the `PodTopologySpread` plugin, so "no constraints in the pod spec" ≠ "no topology predicate". Added to the unmodelled-predicate table; the watchdog already covers the consequence |
| `LimitRange` mutating requests at admission | Only matters for templates without explicit requests, which startup validation already rejects. Noted alongside quota |
| Priority/preemption causing under-estimates | Confirmed: the error direction is conservative (a false HOLD), which is acceptable |
| BestEffort neighbours over-estimating free capacity | Already documented as a known over-estimation with the correct mitigation; deferring the usage guardrail remains right for a POC |
| `HoldPendingPods` blocking only scale-ups | Correct as designed, and now consistent with the DR-03 gate exemption for remediation writes |
| Asymmetric cooldowns and the `max()` window | Sound; DR-03 and DR-04 close the two paths that could have bypassed them |

---

## 4. Answers to the review checklist

| # | Question | Assessment after fixes |
| --- | --- | --- |
| 1 | Incorrect assumptions about CPU/memory availability | Basis was already correct. Two assumption errors found and fixed: PDB scope (DR-10) and staleness measurement (DR-14). Known over-estimation from under-requesting neighbours remains documented, not fixed — correct for a POC |
| 2 | Capacity vs. allocatable vs. utilization vs. requested | Correct throughout, and now sharpened by the L1/L2/L3 framing in [§2](#2-three-statements-that-are-not-the-same-thing) |
| 3 | Controller scales but pod stays Pending | Enumerated and bounded: unmodelled predicates, quota, races, and rollout surge (DR-08). The POC guarantee is now stated as "never knowingly infeasible", with detection and remediation as the closing mechanism |
| 4 | Scheduling constraints | Taints, `nodeSelector`, required node affinity modelled; preferred affinity correctly ignored; anti-affinity and topology spread explicitly unmodelled, including the admin-configured-default case |
| 5 | Stale-information races | Layered and adequate; DR-14 fixed the frozen-source hole; the read/decide/write race is bounded by reserves, preconditions, and the watchdog |
| 6 | Multiple concurrent scaling decisions | Leader election plus preconditions cover self-concurrency (DR-13). External writers were the real gap and are now handled (DR-07) |
| 7 | Scale-up oscillation | DR-03 and DR-05 closed the two real mechanisms — the Ready-base scale-down and warmup dilution |
| 8 | Scale-down oscillation | Damping was sound; DR-04 closed the window-gap bypass |
| 9 | Repeated scale attempts with no resources | Backoff design is right, but DR-01 made recovery unreachable and DR-02 contradicted itself. Both fixed |
| 10 | API failures and timeouts | Sound: freeze, bounded retry, `409` → re-decide, fatal on `403`/`404` |
| 11 | Target Deployment already changing replicas | Was the largest single omission: DR-07 (external writers) and DR-08 (rollouts) |
| 12 | Pod startup failures | Was entirely absent and is the worst behavioural bug found: DR-06 |
| 13 | Insufficient-resource scenarios | Well designed; partial scale-up, HOLD, backoff, and deficit alerting are the right set once DR-01/DR-02 are fixed |
| 14 | Data-loss scenarios | Framing correct, but the POC's primitive was unimplementable (DR-12, now closed by a database-enforced claim) and one protection was misattributed (DR-10) |
| 15 | Deterministic and testable | Structurally excellent; DR-15 (float math) and DR-04 (history as engine input) were the only real threats |

---

## 5. Immutable Phase 1 decisions

These are settled. Changing any of them invalidates parts of the design set, so each should change only in
response to a **new requirement**, via an amended ADR and an updated test — not during implementation for
convenience.

| # | Decision | Anchored in |
| --- | --- | --- |
| I-1 | Feasibility is computed from **`allocatable − Σ requests − reserve`, per node**. Utilization is never a feasibility input | [ADR-05](architecture.md#adr-05-capacity-allocatable-or-requested) |
| I-2 | Per-node floor **before** summing; aggregate free resources are reporting-only | [resource-calculation § 5](resource-calculation.md#5-step-4--fit-capacity) |
| I-3 | Resource dimensions are independent; per-node fit is `min(cpu, memory, podSlots)` and the binding dimension is reported | [ADR-04](architecture.md#adr-04-how-should-available-memory-be-calculated) |
| I-4 | A scale-up is issued **only** for pods believed placeable; when short, scale by what fits or hold | [ADR-09](architecture.md#adr-09-what-happens-when-demand-is-high-but-resources-are-insufficient) |
| I-5 | The decision engine is a **pure function** `Decide(Snapshot, Config, now)`; all state and clock readings enter through the snapshot | [P-2](architecture.md#2-architectural-principles) |
| I-6 | Exactly **one reason code** per reconcile, identical across logs, events, and metric labels | [scaling-algorithm § 2](scaling-algorithm.md#2-decision-outcomes-and-reason-codes) |
| I-7 | Uncertainty ⇒ **freeze**. Never scale on stale/missing primary signals; never scale down on unknown state | [ADR-14](architecture.md#adr-14-what-happens-if-kubernetes-api-calls-fail) |
| I-8 | Demand is `max(backlog target, utilization target)`; either may raise, both must be low to lower | [ADR-02](architecture.md#adr-02-how-should-the-desired-replica-count-be-calculated) |
| I-9 | Scale-down is **strictly** more damped than scale-up: longer cooldown, `max()` over a covered window, one replica per step | [scaling-algorithm § 8](scaling-algorithm.md#8-hysteresis-and-oscillation-control) |
| I-10 | Backoff reset is **level-triggered on observed capacity**, never timer-based; only a complete scale-up resets it | [DR-01](#dr-01-backoff-gate-makes-the-recovery-in-one-interval-claim-unreachable), [DR-02](#dr-02-partial-scale-up-both-arms-and-resets-the-backoff) |
| I-11 | The controller **never deletes pods** and holds no pod-delete permission; replica count is the only actuator | [architecture § 7](architecture.md#7-kubernetes-permissions-and-rbac) |
| I-12 | KubeScaleSense must be the **sole writer** of the target's replica count; competing writers cause refusal, not competition | [ADR-17](architecture.md#adr-17-how-do-we-handle-other-writers-of-the-replica-count) |
| I-13 | L2 is verified against L3: **the Pending and unhealthy-pod watchdogs are correctness requirements**, not operational extras | [§2](#2-three-statements-that-are-not-the-same-thing), [ADR-13](architecture.md#adr-13-what-happens-if-a-newly-created-pod-stays-pending) |
| I-14 | Durability is a **workload** property (D-01…D-06); the controller's contribution is avoiding involuntary eviction and never bypassing graceful termination | [ADR-15](architecture.md#adr-15-how-do-we-protect-data-processing-when-a-worker-pod-crashes) |
| I-15 | One target `Deployment`, one controller, config from a ConfigMap. No CRD, no multi-target, no scheduler simulation in Phase 1 | [requirements § 3](requirements.md#3-non-goals) |
| I-16 | The estimator models a **documented subset** of predicates; the subset is a published contract, and additions require a test proving the new predicate's effect | [resource-calculation § 6](resource-calculation.md#6-predicates-modelled-and-predicates-ignored) |
| I-17 | The work store is a **PostgreSQL work-item table**; claiming is `FOR UPDATE SKIP LOCKED` under a lease, and output plus acknowledgement commit in **one transaction**. No shared filesystem, no atomic-rename claim | [ADR-18](architecture.md#adr-18-what-is-the-durable-work-store-for-the-poc-pipeline) |
| I-18 | Workers are **pull-based**, so replica count is the throughput knob | [ADR-18](architecture.md#adr-18-what-is-the-durable-work-store-for-the-poc-pipeline), [A-13](requirements.md#103-assumptions-that-must-hold-for-the-guarantees-to-be-meaningful) |
| I-19 | The backlog signal is the work store's **claimable count**, read over a **read-only** connection; NiFi's queue depth is observability only | [ADR-19](architecture.md#adr-19-where-does-the-backlog-signal-come-from) |

Full statement of what the POC does and does not guarantee:
[requirements § 10](requirements.md#10-design-limitations-and-assumptions). The settled architecture these
decisions describe: [architecture § 11](architecture.md#11-phase-1-architecture-baseline).
