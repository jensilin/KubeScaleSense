# KubeScaleSense — Requirements

> Status: **Design (pre-implementation)** · Version: 0.1 · Owner: Lead Architect
> This document is the canonical source for requirement IDs (`FR-xx`, `NFR-xx`), assumptions (`A-xx`),
> and the configuration reference. Other documents link here instead of restating them.

Related documents: [architecture](architecture.md) · [scaling-algorithm](scaling-algorithm.md) ·
[resource-calculation](resource-calculation.md) · [failure-scenarios](failure-scenarios.md) ·
[test-plan](test-plan.md) · [implementation-plan](implementation-plan.md)

---

## 1. Problem statement

A file-processing pipeline ingests files over SFTP, routes them through Apache NiFi 2.6.7, and normalizes
them in a pool of Kubernetes processing pods. The incoming file rate is bursty: a quiet period of a few
files per minute can be followed by a spike of thousands of files.

With a fixed replica count the pipeline has two failure modes:

- **Under-provisioned.** The backlog grows faster than it drains, latency climbs, and downstream SLAs are missed.
- **Naively over-provisioned.** Scaling up "because CPU is high" ignores whether the cluster can actually
  *schedule* the new pods. In a resource-constrained cluster the result is Pending pods, node resource
  pressure, evictions of already-running workers, interrupted processing, and — if the pipeline is not
  durable — lost files.

The standard Kubernetes Horizontal Pod Autoscaler computes a desired replica count from *observed
utilization* and writes it to the target's `scale` subresource. It has no notion of whether the delta it just
requested is schedulable; it delegates that to the scheduler and, if the scheduler fails, the pods simply sit
in `Pending`. In a cluster that cannot grow on demand (no cluster autoscaler, fixed node pool, or a hard
namespace quota) this converts a *capacity* problem into a *reliability* problem.

**KubeScaleSense is a resource-aware autoscaling controller that decides scaling actions from both
workload demand and Kubernetes resource availability, and refuses to issue a scale-up it believes the
cluster cannot schedule.**

The distinction that drives the whole design:

| Question | Answered by | Sufficient for a safe scale-up decision? |
| --- | --- | --- |
| "Is the workload busy?" | Utilization metrics: CPU, memory, backlog | No — it only establishes *demand* |
| "Can the cluster place N more pods of this shape?" | Node allocatable minus already-requested resources, filtered by schedulability | Yes — this establishes *feasibility* |

Demand without feasibility produces Pending pods. Feasibility without demand produces waste.
KubeScaleSense requires both.

---

## 2. Goals

| ID | Goal |
| --- | --- |
| G-1 | Scale a single target processing workload (a Kubernetes `Deployment`) in response to measured workload demand |
| G-2 | Evaluate real cluster schedulability *before* increasing replicas, using allocatable and requested resources rather than utilization alone |
| G-3 | Never knowingly issue a scale-up that leaves pods Pending; prefer a smaller, schedulable scale-up or an explicit HOLD |
| G-4 | Make every decision explainable: a machine-readable reason code, a human-readable log line, and a Kubernetes Event |
| G-5 | Be stable: no replica oscillation, no retry storms against an impossible scale target |
| G-6 | Protect in-flight work during scale-down and pod termination (documented assumptions, [§7](#7-data-loss-protection-assumptions)) |
| G-7 | Be demonstrable end-to-end on a laptop-scale local Kubernetes cluster (kind), including the insufficient-resource case |
| G-8 | Keep the POC minimal and single-binary, with a documented evolution path to a production controller |

### Scope boundary

```mermaid
flowchart TB
    subgraph IN["In scope for v0.1"]
        direction TB
        DEMAND["Measure workload demand<br/>backlog + utilization<br/>FR-06…FR-09"]
        FEAS["Evaluate schedulability<br/>allocatable − requested, per node<br/>FR-10…FR-15"]
        DEC["Decide and actuate<br/>scale, partial scale, or hold<br/>FR-01…FR-05, FR-16…FR-22"]
        OBS["Explain every decision<br/>reason code, event, metric<br/>FR-23…FR-27"]
        DEMAND --> DEC
        FEAS --> DEC
        DEC --> OBS
    end

    subgraph OUTS["Out of scope for v0.1"]
        direction TB
        O1["Scaling NiFi · NG-1"]
        O2["Adding or removing nodes · NG-2"]
        O3["Right-sizing pod requests · NG-3"]
        O4["Full scheduler simulation · NG-6, NG-7"]
        O5["Quota, priority, extended resources<br/>NG-8, NG-9, NG-10"]
    end

    classDef inscope fill:#238636,color:#fff,stroke:#116329
    classDef outscope fill:#6e7681,color:#fff,stroke:#484f58
    class DEMAND,FEAS,DEC,OBS inscope
    class O1,O2,O3,O4,O5 outscope
```

Demand and schedulability are deliberately separate inputs to one decision: neither alone may authorize a
scale-up.

---

## 3. Non-goals

Explicitly out of scope for v0.1 (the POC). Each is revisited in
[implementation-plan § Deferred scope](implementation-plan.md#7-deferred-scope-and-when-to-pick-it-up).

| ID | Non-goal | Rationale |
| --- | --- | --- |
| NG-1 | Autoscaling NiFi itself | NiFi 2.6.7 is the ingestion/buffer component and is treated as a fixed-size durable buffer. Scaling a NiFi cluster involves flow-state rebalancing and cluster coordinator concerns that are a separate project. See [ADR-01](architecture.md#adr-01-what-exactly-is-being-scaled) |
| NG-2 | Adding or removing **nodes** (cluster autoscaling) | KubeScaleSense scales pods within a fixed node pool. Cooperation with a cluster autoscaler is a Phase 5 topic |
| NG-3 | Vertical scaling / right-sizing pod requests | Requests are an input to the decision, not an output. No VPA interaction |
| NG-4 | Scale-to-zero | The POC keeps `minReplicas >= 1`; a zero-replica state needs a wake-up path that does not exist yet |
| NG-5 | Multiple target workloads per controller instance | One controller, one target. Multi-target is a config/CRD change, not an algorithm change |
| NG-6 | A full scheduler simulation | KubeScaleSense implements a deliberately conservative subset of scheduler predicates ([resource-calculation § 6](resource-calculation.md#6-predicates-modelled-and-predicates-ignored)). It is an *estimator*, not a scheduler |
| NG-7 | Pod topology spread constraints, inter-pod affinity/anti-affinity | Modelling these correctly requires simulating placement, not just counting capacity. Documented as a known source of over-estimation |
| NG-8 | Priority classes and preemption | The POC assumes the target workload does not preempt other pods |
| NG-9 | Extended/custom resources such as GPUs, hugepages | v0.1 models CPU, memory, and pod-count only |
| NG-10 | `ResourceQuota` / `LimitRange` awareness | A namespace quota can block a scale-up that node capacity permits. Detected reactively via the Pending-pod watchdog in v0.1, modelled proactively in Phase 3 |
| NG-11 | Custom Resource Definition (CRD) API | v0.1 is configured by a ConfigMap-mounted YAML file. A `ScalingPolicy` CRD is Phase 4 |
| NG-12 | High-availability active/active controller | Leader election gives active/passive; concurrent decision-making is not needed |
| NG-13 | Replacing or coexisting with an HPA on the same target | Two controllers writing the same `scale` subresource will fight. Enforced by [FR-19](#4-functional-requirements) |

---

## 4. Functional requirements

### Target and scaling actions

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-01 | The controller SHALL scale exactly one target workload, identified by namespace + `Deployment` name, via the `scale` subresource | Must |
| FR-02 | The controller SHALL clamp all decisions to `[minReplicas, maxReplicas]` | Must |
| FR-03 | The controller SHALL limit a single scale-up to `maxScaleUpStep` replicas and a single scale-down to `maxScaleDownStep` replicas | Must |
| FR-04 | The controller SHALL support a `dryRun` mode that computes and reports decisions without mutating cluster state | Must |
| FR-05 | The controller SHALL use optimistic concurrency (`resourceVersion`) when updating the target and SHALL re-read and re-decide on conflict rather than retrying a stale value | Must |

### Workload demand

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-06 | The controller SHALL collect a backlog signal (count of items waiting to be processed) from the configured workload source | Must |
| FR-07 | The controller SHALL collect per-pod CPU and memory usage for the target workload from `metrics.k8s.io` | Must |
| FR-08 | The controller SHALL compute desired replicas as the maximum of a backlog-derived target and a utilization-derived target, per [scaling-algorithm § 3](scaling-algorithm.md#3-step-1--compute-desired-replicas) | Must |
| FR-09 | The controller SHALL treat metrics older than `metricsStaleAfter` as unusable, and SHALL NOT scale in either direction on stale data | Must |

### Resource awareness

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-10 | The controller SHALL derive the target pod's effective resource requests from the live `Deployment` pod template, including init/sidecar containers and pod overhead, per [resource-calculation § 3](resource-calculation.md#3-step-2--effective-pod-request-of-the-target-workload) | Must |
| FR-11 | The controller SHALL compute per-node free requestable CPU and memory as `allocatable − sum(requests of non-terminal pods) − perNodeReserve` | Must |
| FR-12 | The controller SHALL exclude nodes that are not `Ready`, are `unschedulable`/cordoned, carry an untolerated `NoSchedule`/`NoExecute` taint, or fail the target's `nodeSelector` / required node affinity | Must |
| FR-13 | The controller SHALL compute fit capacity as the sum over candidate nodes of how many whole target pods fit on each node, bounded by remaining pod slots, minus `fitCapacityMarginPods` | Must |
| FR-14 | The controller SHALL scale up only if `fitCapacity >= requestedDelta`, or — when `allowPartialScaleUp` is true — by `min(requestedDelta, fitCapacity)` | Must |
| FR-15 | When demand requires more replicas than fit capacity allows, the controller SHALL emit a `HoldInsufficientResources` decision recording desired, feasible, and blocking dimension (CPU / memory / pod slots) | Must |

### Stability

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-16 | The controller SHALL enforce asymmetric cooldowns (`scaleUpCooldown`, `scaleDownCooldown`) and a scale-down stabilization window | Must |
| FR-17 | The controller SHALL apply a `tolerancePercent` deadband so that small demand fluctuations produce no action | Must |
| FR-18 | The controller SHALL apply exponential backoff to repeated infeasible scale-up attempts, resetting when fit capacity increases or a scale action succeeds | Must |
| FR-19 | The controller SHALL detect an HPA targeting the same workload and SHALL refuse to act (fatal config error) while one exists | Must |
| FR-20 | The controller SHALL detect pods of the target that remain unschedulable for longer than `pendingPodTimeout` and apply the configured `onPendingTimeout` remediation | Must |

### Safety of in-flight work

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-21 | The controller SHALL set `controller.kubernetes.io/pod-deletion-cost` so scale-down removes the least-busy pods first | Should |
| FR-22 | The controller SHALL NOT scale down while the backlog implies the remaining replicas would be immediately insufficient (guaranteed by the `max()` in FR-08) | Must |

### Operability

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-23 | The controller SHALL expose Prometheus metrics, `/healthz`, and `/readyz` per [architecture § 8](architecture.md#8-observability-requirements) | Must |
| FR-24 | The controller SHALL emit a Kubernetes Event for every state-changing decision and for the first occurrence of each HOLD reason (rate-limited thereafter) | Must |
| FR-25 | The controller SHALL run with a least-privilege RBAC role per [architecture § 7](architecture.md#7-kubernetes-permissions-and-rbac) | Must |
| FR-26 | The controller SHALL support leader election so that only one replica makes decisions | Should |
| FR-27 | On unrecoverable API failure the controller SHALL take no scaling action and SHALL surface the failure via metrics, logs, and readiness | Must |

---

## 5. Non-functional requirements

| ID | Requirement | Target |
| --- | --- | --- |
| NFR-01 | Decision latency: time from backlog crossing a threshold to a `scale` write | ≤ 2 × `interval` (≤ 30 s at defaults) |
| NFR-02 | Reconcile loop duration (p95) | ≤ 500 ms for a 50-node / 1000-pod cluster |
| NFR-03 | Controller resource footprint | ≤ 100 m CPU, ≤ 128 MiB memory steady state |
| NFR-04 | API server load | Read path served from informer caches; no `LIST` per reconcile; writes only on state change |
| NFR-05 | Correctness bias | Decisions must be conservative: prefer HOLD over an unschedulable scale-up. False HOLDs are acceptable; Pending pods are not |
| NFR-06 | Decision engine testability | Core algorithm is a pure function of a snapshot struct → decision struct, with no I/O, ≥ 90 % unit coverage |
| NFR-07 | Determinism | Identical input snapshot ⇒ identical decision (clock and randomness injected) |
| NFR-08 | Startup safety | No scaling action until all informer caches have synced and one full metrics sample exists |
| NFR-09 | Portability | Runs on kind, minikube, and managed Kubernetes 1.29+ with no code change |
| NFR-10 | Auditability | Every decision reconstructible from logs alone: inputs, intermediate values, reason code |

---

## 6. Workload and environment assumptions

| ID | Assumption | Consequence if violated |
| --- | --- | --- |
| A-01 | The processing workload is **horizontally scalable and stateless per item**: any replica can process any item | Scaling changes throughput non-linearly; the backlog model breaks down |
| A-02 | All replicas of the target have **identical resource requests** (single pod template, no in-place resize) | Fit capacity math must become per-pod-shape |
| A-03 | The target pod template **sets CPU and memory requests explicitly** | Fit capacity is unbounded/undefined for zero-request pods; startup validation rejects this ([FR-10](#4-functional-requirements)) |
| A-04 | `metrics-server` (or another `metrics.k8s.io` provider) is installed | Utilization-derived desired replicas unavailable; controller degrades to backlog-only and reports it |
| A-05 | The cluster has a **fixed node pool** during the POC | With a cluster autoscaler present, a HOLD may be pessimistic; see [NG-2](#3-non-goals) |
| A-06 | KubeScaleSense is the **only** writer of the target's replica count | Fighting controllers, oscillation ([FR-19](#4-functional-requirements)) |
| A-07 | Item processing time is bounded and shorter than `terminationGracePeriodSeconds` | Graceful drain cannot complete; relies on the in-flight reaper ([§7](#7-data-loss-protection-assumptions)) |

---

## 7. Data-loss protection assumptions

KubeScaleSense **cannot make an unsafe pipeline safe**. It can only avoid making a safe pipeline unsafe.
The following properties are required *of the workload* for the "no data loss" claim to hold; they are design
requirements on the POC pipeline, verified by the tests in
[test-plan § 6](test-plan.md#6-data-integrity-scenarios), and analysed in
[failure-scenarios § 4](failure-scenarios.md#4-data-loss-analysis).

| ID | Property | Mechanism in the POC pipeline |
| --- | --- | --- |
| D-01 | **Durable ingestion buffer.** A file accepted from SFTP survives processing-pod loss | NiFi FlowFile/content repositories on a PersistentVolume; NiFi deletes the remote file only after the FlowFile is committed to its repository |
| D-02 | **Work is claimed, not pushed.** Processing pods pull items; NiFi never depends on a specific pod being alive | NiFi writes to a shared work store (`/work/incoming`, RWX PVC or MinIO bucket); pods claim an item by atomic rename into `/work/inflight/<pod>/` |
| D-03 | **At-least-once with idempotent output.** A re-processed item produces the same result | Output written to a temporary name and atomically renamed to its final name; the input is deleted only after that rename succeeds |
| D-04 | **Crash recovery.** An item claimed by a pod that dies is reprocessed | Reaper (a sidecar or CronJob) returns items in `/work/inflight/*` older than `inflightReclaimAfter` to `/work/incoming` |
| D-05 | **Graceful drain on scale-down.** A terminating pod finishes its current item | `preStop` hook stops claiming new items and waits for the current one; `terminationGracePeriodSeconds` > max item processing time ([A-07](#6-workload-and-environment-assumptions)) |
| D-06 | **Least-busy-first eviction.** Scale-down prefers idle pods | Pods publish their in-flight count; controller writes `pod-deletion-cost` ([FR-21](#4-functional-requirements)) |
| D-07 | **Eviction resistance for the pool as a whole** | `PodDisruptionBudget` with `minAvailable: 1`; per-pod resource *limits* set so a busy pod cannot starve a node and trigger node-pressure eviction of its peers |

> **The most important protection is D-01 + D-02**: because the queue is durable and pull-based, an
> over-aggressive or wrong scaling decision degrades *throughput*, never *durability*. Resource-aware
> scaling exists to protect throughput and cluster health, not as the last line of defence against data loss.

Note that the parameters governing these properties — `inflightReclaimAfter` (reaper),
`terminationGracePeriodSeconds`, and the `preStop` drain timeout — belong to the **processing workload's**
manifests, not to the controller configuration in [§8](#8-configuration-requirements). The controller neither
reads nor validates them; it only assumes they are set consistently with
[A-07](#6-workload-and-environment-assumptions).

---

## 8. Configuration requirements

Configuration is a YAML file mounted from a ConfigMap and parsed at startup, with per-key environment
variable overrides (`KSS_` + upper-snake path, e.g. `KSS_SCALING_SCALEUPCOOLDOWN`). This is the canonical
parameter list; other documents reference these names verbatim.

**Requirements on configuration handling:**

- **CR-1** Every parameter has a documented default; a minimal config specifies only `target`.
- **CR-2** Config is validated at startup and the controller **fails fast with a non-zero exit** on invalid
  input (e.g. `minReplicas > maxReplicas`, target pod template without resource requests, `interval` < 5 s).
- **CR-3** Config is **immutable at runtime** in v0.1: a ConfigMap change requires a pod restart. Hot reload is Phase 4.
- **CR-4** Defaults must be safe for a small cluster: conservative headroom, partial scale-up enabled,
  scale-down slower than scale-up.
- **CR-5** No secrets in the config file; NiFi credentials come from a mounted `Secret`.

```yaml
# config/kubescalesense.yaml — all values shown are the defaults
controller:
  interval: 15s                 # reconcile period
  leaderElection: true
  dryRun: false
  metricsAddr: ":8080"
  healthAddr: ":8081"
  logLevel: info                # debug|info|warn|error
  logFormat: json

target:
  namespace: data-pipeline
  deployment: file-processor
  minReplicas: 1
  maxReplicas: 12

workload:
  source: nifi                  # nifi | http | none  (http and none exist for testing and for P1 bootstrap)
  itemsPerReplica: 50           # backlog items one replica is expected to hold
  targetCPUUtilizationPercent: 70   # of the pod's CPU *request*
  metricsStaleAfter: 60s
  backlogSmoothing:
    mode: ewma                  # none | ewma
    alpha: 0.4
  nifi:
    baseURL: https://nifi.data-pipeline.svc.cluster.local:8443/nifi-api
    connectionIDs: []           # queued FlowFile counts of these connections are summed
    credentialsSecret: nifi-api-credentials
    timeout: 5s
    insecureSkipTLSVerify: false

resources:
  perNodeReserveCPUMilli: 200   # safety margin left free on every candidate node
  perNodeReserveMemoryMiB: 256
  fitCapacityMarginPods: 1      # subtracted from total computed fit capacity
  respectNodeSelector: true
  respectNodeAffinity: true     # required-during-scheduling only
  respectTaints: true
  nodeLabelSelector: ""         # optional extra restriction of the candidate node set

scaling:
  scaleUpCooldown: 60s
  scaleDownCooldown: 300s
  scaleDownStabilizationWindow: 300s
  tolerancePercent: 10          # deadband around current replicas
  maxScaleUpStep: 4
  maxScaleDownStep: 1
  allowPartialScaleUp: true
  holdBackoff:
    initial: 30s
    max: 15m
    factor: 2.0

pending:
  pendingPodTimeout: 120s
  onPendingTimeout: revert      # revert | freeze | none
```

### Parameter reference

| Parameter | Default | Meaning | Consumed by |
| --- | --- | --- | --- |
| `controller.interval` | `15s` | Reconcile period. Lower = faster reaction, more API/metrics load | [architecture § 5](architecture.md#5-scaling-decision-flow) |
| `controller.dryRun` | `false` | Decide and report, never mutate | [FR-04](#4-functional-requirements) |
| `controller.leaderElection` | `true` | Active/passive HA via a `Lease` | [FR-26](#4-functional-requirements) |
| `target.namespace` / `.deployment` | — | The scaled workload. **Required** | [ADR-01](architecture.md#adr-01-what-exactly-is-being-scaled) |
| `target.minReplicas` / `.maxReplicas` | `1` / `12` | Hard clamp on every decision | [scaling-algorithm § 3.4](scaling-algorithm.md#34-clamping) |
| `workload.itemsPerReplica` | `50` | Backlog-to-replica conversion factor | [scaling-algorithm § 3.1](scaling-algorithm.md#31-backlog-derived-target) |
| `workload.targetCPUUtilizationPercent` | `70` | Utilization target, relative to CPU **request** | [scaling-algorithm § 3.2](scaling-algorithm.md#32-utilization-derived-target) |
| `workload.metricsStaleAfter` | `60s` | Age at which a metric sample is unusable | [FR-09](#4-functional-requirements), [FS-08](failure-scenarios.md#fs-08-stale-resource-or-metric-information) |
| `workload.backlogSmoothing.*` | `ewma`, `0.4` | Damps single-sample backlog spikes | [scaling-algorithm § 8.4](scaling-algorithm.md#84-signal-smoothing) |
| `workload.nifi.*` | — | NiFi REST endpoint, connection IDs, credentials | [architecture § 4.3](architecture.md#43-workload-metrics-collector-internalmetrics) |
| `resources.perNodeReserveCPUMilli` | `200` | Per-node CPU held back from fit math | [resource-calculation § 4](resource-calculation.md#4-step-3--per-node-free-requestable-resources) |
| `resources.perNodeReserveMemoryMiB` | `256` | Per-node memory held back from fit math | [resource-calculation § 4](resource-calculation.md#4-step-3--per-node-free-requestable-resources) |
| `resources.fitCapacityMarginPods` | `1` | Global pessimism margin on fit capacity | [resource-calculation § 5](resource-calculation.md#5-step-4--fit-capacity) |
| `resources.respectTaints` / `respectNodeSelector` / `respectNodeAffinity` | `true` | Enable scheduling predicates in candidate-node filtering | [resource-calculation § 2](resource-calculation.md#2-step-1--candidate-node-set) |
| `resources.nodeLabelSelector` | `""` | Restrict the candidate node set further (e.g. a dedicated pool) | [resource-calculation § 2](resource-calculation.md#2-step-1--candidate-node-set) |
| `scaling.scaleUpCooldown` | `60s` | Minimum interval between scale-ups | [scaling-algorithm § 5](scaling-algorithm.md#5-step-3--stability-gates) |
| `scaling.scaleDownCooldown` | `300s` | Minimum interval between scale-downs | [scaling-algorithm § 5](scaling-algorithm.md#5-step-3--stability-gates) |
| `scaling.scaleDownStabilizationWindow` | `300s` | Scale down only if desired stayed lower for this whole window | [scaling-algorithm § 7](scaling-algorithm.md#7-step-5--scale-down-logic) |
| `scaling.tolerancePercent` | `10` | Deadband; suppresses churn | [scaling-algorithm § 8.1](scaling-algorithm.md#81-deadband-tolerance) |
| `scaling.maxScaleUpStep` / `maxScaleDownStep` | `4` / `1` | Per-decision rate limit | [FR-03](#4-functional-requirements) |
| `scaling.allowPartialScaleUp` | `true` | Take the schedulable part of an infeasible scale-up | [ADR-09](architecture.md#adr-09-what-happens-when-demand-is-high-but-resources-are-insufficient) |
| `scaling.holdBackoff.*` | `30s` / `15m` / `2.0` | Backoff for repeatedly infeasible scale-ups | [FR-18](#4-functional-requirements), [FS-07](failure-scenarios.md#fs-07-repeated-impossible-scale-attempts) |
| `pending.pendingPodTimeout` | `120s` | How long a pod may stay unschedulable before remediation | [FS-06](failure-scenarios.md#fs-06-newly-created-pod-stays-pending) |
| `pending.onPendingTimeout` | `revert` | `revert` to last-good replicas, `freeze` scale-ups, or `none` | [FS-06](failure-scenarios.md#fs-06-newly-created-pod-stays-pending) |

---

## 9. Glossary

| Term | Meaning |
| --- | --- |
| **Capacity** | `node.status.capacity` — the node's total hardware resources. Not used for fit decisions ([ADR-05](architecture.md#adr-05-capacity-allocatable-or-requested)) |
| **Allocatable** | `node.status.allocatable` — capacity minus kube/system reserved and eviction thresholds. The scheduler's real budget |
| **Requested** | Sum of container `resources.requests` of non-terminal pods on a node. What the scheduler has already committed |
| **Usage / utilization** | Live consumption from `metrics.k8s.io`. Signals *demand*, never *schedulability* |
| **Effective pod request** | The request the scheduler charges for one target pod, including sidecars and pod overhead ([resource-calculation § 3](resource-calculation.md#3-step-2--effective-pod-request-of-the-target-workload)) |
| **Fit capacity** | Estimated number of additional target pods placeable on the candidate node set right now |
| **Requested delta** | `targetReplicas − currentReplicas` for a scale-up |
| **Candidate node** | A node passing all modelled scheduling predicates for the target pod |
| **HOLD** | A decision to leave replicas unchanged despite unmet demand, with a reason code |
| **Backlog** | Count of items waiting to be processed (NiFi queued FlowFiles and/or files in `/work/incoming`) |
| **Reason code** | Canonical enum labelling every decision ([scaling-algorithm § 2](scaling-algorithm.md#2-decision-outcomes-and-reason-codes)) |
