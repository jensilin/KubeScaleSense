# KubeScaleSense — Implementation Plan

> Status: **Design (pre-implementation)** · Version: 0.2
> Canonical source for: phase definitions (`P0`–`P5`), exit criteria, risk register, deferred scope, and the
> definition of done for the POC.

Related documents: [requirements](requirements.md) · [architecture](architecture.md) ·
[scaling-algorithm](scaling-algorithm.md) · [resource-calculation](resource-calculation.md) ·
[failure-scenarios](failure-scenarios.md) · [test-plan](test-plan.md)

**Nothing in this plan has been implemented yet.** The repository currently contains this design set only.

**Post-review status.** The [design review](design-review.md) produced sixteen findings, all folded into the
design. Three change this plan rather than only the specification:

- **P1** gains the corrected feasibility/backoff ordering ([DR-01](design-review.md#dr-01-backoff-gate-makes-the-recovery-in-one-interval-claim-unreachable)) and integer arithmetic in the engine.
- **P2** gained, then lost, a **blocking prerequisite**: the work store had to be proven to support atomic,
  exclusive, cross-node item claiming before the pipeline could be built on it ([DR-12](design-review.md#dr-12-the-poc-work-store-cannot-provide-the-claimed-semantics-on-the-proposed-environment)).
  See the v0.2 note below — the prerequisite is gone because the store is gone.
- **P3** gains three guards that are correctness requirements, not polish: the unhealthy-pod guard, external
  replica-change detection, and the rollout hold.

**Architecture simplification (v0.2).** The v0.1.2 answer to DR-12 was to add a PostgreSQL work-item table
with `FOR UPDATE SKIP LOCKED` claiming and transactional acknowledgement. That answer worked, and it was
**disproportionate**: a database, a schema, two roles, a JDBC driver, a lease protocol, and a pruning CronJob
were all added to a project whose actual subject is *resource-aware scaling decisions*, none of which needed
any of it. v0.2 removes the entire work store and connects NiFi to the Normalizer over **HTTP through a
ClusterIP Service**
([ADR-21](architecture.md#adr-21-how-does-work-reach-the-normalizer-pods),
[ADR-22](architecture.md#adr-22-where-does-the-workload-pressure-signal-come-from),
[architecture § 11](architecture.md#11-phase-1-architecture-baseline)).

The effect on this plan is almost entirely subtractive:

| Change | Effect on the plan |
| --- | --- |
| No work store | **P2 shrinks by roughly a third.** No StatefulSet, schema, roles, PVC, JDBC init container, claim loop, lease protocol, or cleanup CronJob |
| No claim protocol to prove | The DR-12 prerequisite dissolves rather than closing; DI-08 is repurposed to a **throughput** gate instead of a claim-exclusivity gate |
| Pluggable pressure signal | P1 can ship a complete, testable demand path with the `synthetic` source and never block on the pipeline; the `http` source in P2 is a metrics scrape, not a database client |
| Durability moved to the workload | The plan no longer promises data durability as a deliverable; it promises that scaling actions do not *cause* loss, which is a different and much more defensible claim ([requirements § 7](requirements.md#7-durability-boundary-and-workload-responsibilities)) |

Nothing in P1's resource model, P3's hysteresis, or P4's operability changed — the simplification is confined
to how work reaches the pods and where the pressure number comes from. All seven open questions remain
resolved or deferred with a stated default ([§8](#8-open-questions)); **no Phase 1 blockers remain.**

---

## 1. Sequencing principle

The phase order follows one rule: **observe correctly before acting at all.**

The riskiest thing this controller does is write a replica count. The second riskiest is computing fit
capacity, because a wrong estimate is invisible until pods go Pending. So the plan front-loads the estimator
and runs it in permanent dry-run against a real cluster (P1), where its output can be checked by hand against
`kubectl describe node`, before any actuation code exists (P3). By the time the controller can write, its
inputs have already been validated against reality.

The corollary: a phase is only complete when its exit tests pass. "Written but unverified" carries no credit,
because for an autoscaler the interesting failures are precisely the ones that look fine in code review.

```mermaid
flowchart LR
    P0["P0 · Foundation<br/>module, config, CI"] --> P1["P1 · Observation<br/>resources + metrics + Decide<br/><b>dry-run only</b>"]
    P1 --> P2["P2 · Demo pipeline<br/>SFTP + NiFi + Normalizer<br/>+ ballast + generator"]
    P2 --> P3["P3 · Actuation<br/>scale writes, hysteresis,<br/>watchdog, events"]
    P3 --> P4["P4 · Hardening<br/>HA, dashboards, full matrix,<br/>runbook — POC complete"]
    P4 --> P5["P5 · Production evolution<br/>CRD, predicates, quota,<br/>autoscaler cooperation"]

    classDef poc fill:#238636,color:#fff,stroke:#116329
    classDef future fill:#6e7681,color:#fff,stroke:#484f58
    class P0,P1,P2,P3,P4 poc
    class P5 future
```

---

## 2. Phase overview

| Phase | Goal | Effort | Cumulative capability |
| --- | --- | --- | --- |
| **P0** | Foundation: module, layout, config, CI | ~1 week | Builds, validates config, refuses bad input |
| **P1** | Observation: fit capacity + demand + decisions, dry-run | ~2 weeks | Reports what it *would* do; estimator verified by hand |
| **P2** | Demonstration workload | ~1 week | Real pipeline normalizes files over HTTP; pressure signal live; `itemsPerReplica` measured |
| **P3** | Actuation and stability | ~2 weeks | Actually scales, safely; the demo works end to end |
| **P4** | Hardening and operability | ~1.5 weeks | **POC complete**: HA, dashboards, full test matrix, runbook |
| P5 | Production evolution | open-ended | CRD, higher predicate fidelity, quota, multi-target |

Total to a demonstrable, tested POC: **~7.5 weeks** of one engineer's focused effort — half a week less than
v0.1.2, which is the direct schedule dividend of deleting the work store.

```mermaid
gantt
    title KubeScaleSense — indicative schedule (dates illustrative)
    dateFormat YYYY-MM-DD
    axisFormat %b %d

    section P0 Foundation
    Module, layout, CI            :p0a, 2026-09-14, 3d
    Config schema + validation    :p0b, after p0a, 3d

    section P1 Observation
    Kubernetes clients + informers :p1a, after p0b, 3d
    resources: fit capacity        :p1b, after p1a, 5d
    metrics: pressure + utilization :p1c, after p1b, 3d
    scaling: pure Decide           :p1d, after p1c, 4d
    Observability + dry-run run    :p1e, after p1d, 3d

    section P2 Demo pipeline
    Normalizer app + Service       :p2a, after p1e, 3d
    SFTP + NiFi InvokeHTTP flow    :p2b, after p2a, 3d
    kind topology, ballast, generator :p2c, after p2b, 3d

    section P3 Actuation
    Scale writes + events          :p3a, after p2c, 3d
    Cooldowns, window, backoff     :p3b, after p3a, 3d
    Pending watchdog + deletion cost :p3c, after p3b, 4d
    E2E + DI suites                :p3d, after p3c, 4d

    section P4 Hardening
    Leader election, health, HA    :p4a, after p3d, 3d
    Dashboards, alerts, runbook    :p4b, after p4a, 3d
    PF + soak, docs reconciliation :p4c, after p4b, 4d
```

---

## 3. Phase details

### P0 — Foundation

**Goal.** A repository that builds, tests, and refuses to run with an unsafe configuration.

**Deliverables**

- Go module `github.com/jensilin/KubeScaleSense`, Go ≥ 1.24, layout per
  [architecture § 4](architecture.md#4-component-responsibilities).
- `internal/config`: schema, defaults, `KSS_*` env overrides, validation (CR-1…CR-5), plus
  `config/kubescalesense.yaml` matching [requirements § 8](requirements.md#8-configuration-requirements) exactly.
- `cmd/kubescalesense`: flag parsing, structured logger, signal handling, graceful shutdown. No cluster access yet.
- `Makefile` (`build`, `test`, `lint`, `demo-up`, `demo-down`), `Dockerfile` (distroless, non-root),
  `.golangci.yml`.
- CI skeleton per [test-plan § 10](test-plan.md#10-ci-pipeline), including the markdown link check over `docs/`.
- `deploy/kubescalesense/` skeleton: ServiceAccount, ClusterRole, Role, their bindings, ConfigMap, Deployment
  (not yet applied). The two roles ship with **empty rule sets**: P0 makes no API call, so least privilege
  ([FR-25](requirements.md#4-functional-requirements)) grants nothing, and every rule from
  [architecture § 7](architecture.md#7-kubernetes-permissions-and-rbac) is instead listed in the manifest
  against the phase that introduces it. No `Secret` is created in this phase or any later one — the controller
  has no workload credentials to hold ([CR-5](requirements.md#8-configuration-requirements)).

**Exit criteria**

- `make build test lint` green; `UT-21` passes (every invalid-config case yields a specific error).
- A deliberately broken config fails startup with a non-zero exit and an actionable message.
- Docs link check passes.

**Not in this phase.** Any Kubernetes API call.

**Status: complete.** The exit criteria above are met: `go build ./...`, `go test ./...`, `make build`,
`make test`, `make lint`, the docs link check, and the container image build all pass, and a deliberately
broken config is rejected with a non-zero exit and a message naming the offending key, its value, the
constraint, and the override variable.

**Environment.** The toolchain is installed: Go 1.27.1 (the module targets Go ≥ 1.24), Docker, `kubectl`,
`make`, and `golangci-lint`, under WSL 2. `kind` is the one item still outstanding; it is first needed by the
P1 manual validation gate below, not by P0. Either way the list is shorter than it was in v0.1.2: no
PostgreSQL image and no pinned JDBC driver.

---

### P1 — Observation (dry-run only)

**Goal.** Compute fit capacity, collect demand, and produce decisions that are *reported but never acted on*.
This is the phase where the project's central claim is validated.

**Deliverables**

- `internal/kubernetes`: clientset, informers for Node / Pod (cluster-scoped) / Deployment, pods indexed by
  `spec.nodeName`, cache-sync gating, event recorder (unused until P3).
- `internal/resources`: the full pipeline of
  [resource-calculation](resource-calculation.md) — candidate filtering with taints, `nodeSelector`, required
  node affinity; effective pod request with sidecars, init containers, and overhead; per-node free requestable
  resources; fit capacity with margin; blocking dimension.
- `internal/metrics`: `metrics.k8s.io` utilization (Ready **and warm** pods only,
  [FR-28](requirements.md#review-driven-requirements-v011)) and the **workload-pressure signal behind a
  replaceable source interface** ([FR-36](requirements.md#workload-signal-requirements-v02)). P1 implements
  `synthetic` (a scripted series read from a file) and `none`, which is enough to exercise the entire demand
  path — including staleness and unavailability — before the pipeline exists. The `http` source arrives in P2
  and is a plain metrics scrape ([ADR-22](architecture.md#adr-22-where-does-the-workload-pressure-signal-come-from)).
  No NiFi REST client and no database client is built at any point, in any phase.

  **The interface is written first and the sources second, deliberately.** If `synthetic` is added later as a
  test double, it ends up shaped like a test double; if it is one of the first two real implementations, the
  interface stays narrow enough that a future source (a broker's queue depth, a queue-length exporter) is a
  drop-in. IT-16 exists to hold that line.
- `internal/scaling`: `Snapshot`, `Decision`, reason codes, and `Decide` implementing
  [scaling-algorithm §§ 3–6](scaling-algorithm.md#3-step-1--compute-desired-replicas). Cooldown/window/backoff
  fields exist in the snapshot and are honoured, but the controller does not yet maintain history across
  restarts of the loop. Feasibility is computed before the up-gates, and `BackoffState` carries
  `fitCapacityAtArm`, from the outset — retrofitting the ordering later is how
  [DR-01](design-review.md#dr-01-backoff-gate-makes-the-recovery-in-one-interval-claim-unreachable) happens.
  Utilization and smoothing arithmetic is integer from the start
  ([FR-35](requirements.md#review-driven-requirements-v011)).
- `internal/observability`: full metric set, `/healthz`, `/readyz`, the one-line-per-reconcile decision log.
- `controller.dryRun` **hard-defaulted to true** for this phase; the actuator is a no-op logger.

**Exit criteria**

- `UT-01`…`UT-26` pass with ≥ 90 % coverage in `internal/scaling` and `internal/resources`.
- `UT-19` reproduces the reference worked example (`F = 4`) from
  [resource-calculation § 7](resource-calculation.md#7-worked-example) exactly.
- `IT-01`, `IT-05`, `IT-12` pass.
- **Manual validation gate:** on a kind cluster with ballast pods, `kss_fit_capacity_pods` and per-node free
  resources are compared by hand against `kubectl describe node` output across at least five capacity states,
  including one where the control-plane taint must exclude a node and one fragmentation case. Any discrepancy
  blocks the phase.

**Why the manual gate exists.** A unit test proves the arithmetic matches its author's expectations; only a
comparison against a real cluster proves the *model* matches Kubernetes. This is the cheapest possible place to
discover a misunderstanding of `allocatable`, sidecar accounting, or taint semantics — before any code can
scale anything.

**Not in this phase.** Writes of any kind, cooldown state persistence, the watchdog, the demo pipeline.

---

### P2 — Demonstration workload

**Goal.** A real pipeline whose pressure signal is a genuine demand signal, and a cluster topology in which
resource exhaustion is reachable on purpose.

**Architecture is settled before this phase starts.** Work reaches the pods as an HTTP request through
`normalizer-service`; there is no store to build, provision, or prove
([ADR-21](architecture.md#adr-21-how-does-work-reach-the-normalizer-pods)). The v0.1.2 blocking prerequisite
has dissolved along with the store it applied to ([§8.2](#82-formerly-blocking-items-now-dissolved)).

**One test still gates the phase, but a different one.** `DI-08` — *throughput scales with replicas* — must
pass before any other P2 exit criterion is assessed. The reason is exactly parallel to the old gate: a
scaling controller demonstrated against a workload whose throughput does not respond to replica count is a
controller demonstrating nothing, however correct its arithmetic
([FS-28](failure-scenarios.md#fs-28-adding-replicas-does-not-add-throughput)).

**Deliverables**

- `cmd/normalizer` + `internal/normalizer` — the workload, a small Go HTTP server:
  - `POST /normalize` accepting a raw record and returning the normalized result, with **no persistence, no
    shared state, and no dependency on any other pod**
    ([WR-01](requirements.md#workload-signal-requirements-v02)…[WR-03](requirements.md#workload-signal-requirements-v02)).
  - A configurable CPU cost per record, so a spike can be made to demand a known replica count.
  - Normalization implemented as a **pure function of the request body**, so a retried request produces a
    byte-identical result ([D-03](requirements.md#7-durability-boundary-and-workload-responsibilities), DI-04).
  - `/healthz` and `/readyz`, with `/readyz` failing immediately on `SIGTERM` so the Service withdraws the pod
    before the drain begins ([WR-06](requirements.md#workload-signal-requirements-v02),
    [FS-14](failure-scenarios.md#fs-14-scale-down-terminates-a-busy-pod)).
  - `/metrics` exposing queued and in-flight request counts, request rate, processing rate, and latency — the
    source the `http` pressure signal scrapes, and the input to `pod-deletion-cost`
    ([WR-07](requirements.md#workload-signal-requirements-v02)).
- `deploy/normalizer/`: Deployment (`500m` / `512Mi` requests, memory limit,
  `terminationGracePeriodSeconds` > maximum request time per [A-07](requirements.md#6-workload-and-environment-assumptions)),
  and the `normalizer-service` ClusterIP Service that fans requests across replicas.
- `deploy/demo/`: `atmoz/sftp`; NiFi 2.6.7 StatefulSet with its repositories on PVCs
  ([D-01](requirements.md#7-durability-boundary-and-workload-responsibilities)); `file-generator` Job
  producing controlled spikes (rate, count, size distribution); `ballast` Deployment for deterministic
  capacity shaping; and the kind topology from
  [test-plan § 7](test-plan.md#7-local-demonstration-environment).
- NiFi flow `ListSFTP → FetchSFTP → SplitRecord → InvokeHTTP → PutFile`, with:
  - **Retry configured on the failure relationships** with bounded back-off. This single processor
    configuration is the load-bearing dependency of the whole v0.2 durability story
    ([A-12](requirements.md#103-assumptions-that-must-hold-for-the-guarantees-to-be-meaningful),
    [failure-scenarios § 4](failure-scenarios.md#4-data-loss-analysis)) — it is a *deliverable*, not a setting.
  - **Concurrent tasks set above `maxReplicas`**, or the demo shows scaling with no throughput effect
    (FS-28). `make demo-up` asserts this.
  - **A response timeout above the maximum normalization time**, or slow requests are retried while still in
    flight ([FS-29](failure-scenarios.md#fs-29-a-retried-request-is-normalized-twice)).
- `internal/metrics` `http` pressure source: bounded-timeout scrape of the Normalizer's `/metrics`, staleness
  stamping, EWMA smoothing, and *unavailable*-on-error semantics — never zero-on-error
  ([FR-37](requirements.md#workload-signal-requirements-v02)…[FR-38](requirements.md#workload-signal-requirements-v02)).

**Exit criteria**

- `DI-08` passes on the 3-worker kind cluster **before** any other P2 exit criterion is assessed.
- Records flow SFTP → NiFi → `normalizer-service` → Normalizer → output at a fixed replica count, with the
  end-to-end conservation assertion holding (`DI-04`, `DI-05`, `DI-07`).
- The controller's pressure metric tracks the Normalizer's reported outstanding work within one sample
  interval, and `IT-16` confirms that `synthetic`, `http`, and `none` all satisfy the same contract —
  including unavailable-not-zero.
- **`itemsPerReplica` measured, not guessed:** at fixed replica counts, find the pressure level at which
  per-request latency reaches the SLO; record the measurement and its method in `docs/` alongside the
  resulting default.
- Ballast reliably drives `kss_fit_capacity_pods` to a chosen value, verified against the P1 manual gate method.

**Not in this phase.** Any scaling. The replica count is set by hand throughout P2 — which is exactly what
makes the `itemsPerReplica` measurement and the DI-08 throughput curve clean.

---

### P3 — Actuation and stability

**Goal.** The controller scales, safely, and the demo tells the story end to end.

**Deliverables**

- Actuator: `deployments/scale` update with `resourceVersion` precondition, `409` → re-read and re-decide,
  bounded jittered retry for transient errors, fatal handling of `403`/`404`
  ([ADR-14](architecture.md#adr-14-what-happens-if-kubernetes-api-calls-fail)).
- Controller state: `lastScaleUp`/`lastScaleDown`, `desiredHistory` ring buffer, `holdBackoff`,
  `lastGoodReplicas`.
- Hysteresis: deadband, asymmetric cooldowns, scale-down stabilization window, exponential hold backoff with
  level-triggered reset ([scaling-algorithm § 8](scaling-algorithm.md#8-hysteresis-and-oscillation-control),
  [§ 9](scaling-algorithm.md#9-insufficient-resource-behaviour-and-backoff)).
- Partial scale-up.
- Pending-pod watchdog with `revert` / `freeze` / `none`, including the quota signature (replicas not
  materialising into pods) ([FS-06](failure-scenarios.md#fs-06-newly-created-pod-stays-pending),
  [FS-10](failure-scenarios.md#fs-10-namespace-resourcequota-blocks-pod-creation)).
- **Review-driven guards** — correctness requirements, not polish: the unhealthy-pod guard
  ([FR-30](requirements.md#review-driven-requirements-v011)), external replica-change detection and adoption
  ([FR-31](requirements.md#review-driven-requirements-v011)), the rollout hold
  ([FR-32](requirements.md#review-driven-requirements-v011)), and the settled-replicas plus window-coverage
  preconditions on scale-down ([FR-29](requirements.md#review-driven-requirements-v011)).
- `pod-deletion-cost` refresh before scale-down.
- HPA conflict detection ([FS-16](failure-scenarios.md#fs-16-competing-controller-on-the-same-target)).
- Kubernetes events with rate limiting ([architecture § 8.2](architecture.md#82-kubernetes-events)).
- Scenario scripts `make demo-scenario-1..5` per [test-plan § 7.4](test-plan.md#74-demo-narrative).

**Exit criteria**

- `IT-02`, `IT-03`, `IT-04`, `IT-06`, `IT-07`, `IT-08`, `IT-11`, `IT-13`, `IT-14`, `IT-15` pass.
- `E2E-01`…`E2E-06`, `E2E-08`, `E2E-09`, `E2E-10` pass; `DI-01`, `DI-02`, `DI-03`, `DI-07` pass.
- `IT-15` specifically demonstrates that a broken image with a rising backlog produces **no** further
  scale-ups — the DR-06 loop is closed.
- **The headline assertion:** in `E2E-02` (demand for 8 replicas, room for 2) the controller performs a partial
  scale-up and then holds, and **no pod of the target is ever Pending for more than `pendingPodTimeout`**,
  while the replica deficit is visible in metrics and events throughout.
- The HPA side-by-side comparison from [test-plan § 7.4](test-plan.md#74-demo-narrative) is recorded: same
  cluster, same load, Pending pods with the HPA and none with KubeScaleSense.

---

### P4 — Hardening and operability (POC complete)

**Goal.** Something a second engineer can run, observe, and trust.

**Deliverables**

- Leader election via `Lease`; clean stop on lease loss ([FR-26](requirements.md#4-functional-requirements)).
- Metrics-source degradation paths finalized ([FS-15](failure-scenarios.md#fs-15-metrics-source-unavailable)).
- Grafana dashboard: desired vs. current replicas, fit capacity, blocking dimension, decision-reason
  distribution, workload pressure, processing rate, node exclusions — laid out so the resource-awareness story
  is readable at a glance, with pressure and processing rate adjacent so a flat throughput curve under rising
  replicas is impossible to miss (FS-28).
- Prometheus alert rules from
  [failure-scenarios § 6](failure-scenarios.md#6-alerting-recommendations), with the replica-deficit alert as
  the primary one.
- `docs/runbook.md`: what each reason code means and what to do about it.
- `PF-01`…`PF-03`, `SK-01`, `E2E-07`, `E2E-11`, `DI-06`.
- Portability check on minikube ([NFR-09](requirements.md#5-non-functional-requirements)).
- Documentation reconciliation: measured values folded back into the defaults; any design decision that
  changed during implementation updated in these documents (and its ADR amended rather than silently edited).

**Exit criteria**

- Full test matrix green; every reason code observed at least once in `SK-01`.
- `NFR-01`…`NFR-04` measured and met.
- A demo run by someone other than the author, from `make demo-up` to conclusions, with no undocumented steps.

**Definition of done for the POC:** see [§6](#6-definition-of-done-for-the-poc).

---

### P5 — Production evolution (not scheduled)

Direction only, per [architecture § 10](architecture.md#10-evolution-to-a-production-controller). Each item is
additive; none invalidates the P1–P4 algorithm.

| Item | Trigger to start |
| --- | --- |
| `ScalingPolicy` CRD + controller-runtime migration | A second target workload, or a need for status conditions/history |
| Topology spread and hostname anti-affinity modelling | First real Pending-pod remediation traced to [FS-05](failure-scenarios.md#fs-05-unmodelled-scheduling-predicate-causes-a-wrong-fit-estimate) |
| Proactive `ResourceQuota`/`LimitRange` modelling | Deployment into a quota-governed namespace |
| Queueing-theory demand model (arrival rate × service time) | `itemsPerReplica` proves unstable across file mixes |
| Cluster-autoscaler cooperation | Deployment onto an elastic node pool, where a HOLD is pessimistic |
| Scheduler-framework-based feasibility | Predicate fidelity becomes the dominant error source |
| Multi-target and fair-share arbitration | Two pipelines competing for one node pool |
| Node-usage guardrail (blend requests with measured usage) | Under-requesting neighbours cause measurable throughput loss ([resource-calculation § 4.1](resource-calculation.md#41-known-over-estimation-under-requesting-neighbours)) |

---

## 4. What is deliberately *not* built in the POC

Restated here so that the plan is honest about its boundaries — the full rationale for each is in
[requirements § 3](requirements.md#3-non-goals):

NiFi autoscaling · node provisioning · vertical scaling · scale-to-zero · multiple targets · full scheduler
simulation · topology spread and pod anti-affinity · priority/preemption · GPUs and extended resources ·
quota modelling · CRD API · active/active HA · HPA coexistence.

Two of these deserve a note because they are the ones most likely to be requested first:

- **NiFi autoscaling** ([NG-1](requirements.md#3-non-goals)) would change the project's shape, not just its
  scope: NiFi holds flow state, so scaling it means cluster-coordinator participation and flow rebalancing.
  Trigger for reconsideration: measurements showing NiFi ingestion, not processing, as the bottleneck.
- **Cluster autoscaler cooperation** ([NG-2](requirements.md#3-non-goals)) inverts the meaning of a HOLD: on an
  elastic cluster, "insufficient resources" is a temporary condition that the autoscaler will fix, so the right
  behaviour is to express demand and wait (`onPendingTimeout: none`) rather than revert.

---

## 5. Risk register

| # | Risk | Impact | Likelihood | Mitigation | Owning phase |
| --- | --- | --- | --- | --- | --- |
| R-1 | Fit-capacity model diverges from real scheduler behaviour | Pending pods; the core claim fails | Medium | P1 manual validation gate; `fitCapacityMarginPods`; Pending watchdog; predicate table as an explicit contract | P1, P3 |
| R-2 | `itemsPerReplica` unstable across file mixes | Under- or over-provisioning | High | Measure in P2; utilization signal as the safety net ([scaling-algorithm § 3.2](scaling-algorithm.md#32-utilization-derived-target)); queueing model in P5 | P2 |
| R-3 | ~~NiFi REST/auth surprises~~ → ~~NiFi→PostgreSQL integration~~ → **NiFi `InvokeHTTP` configuration** (retry relationships, concurrency, response timeout) | Pipeline silently loses records, or scaling has no throughput effect | Medium | Three explicit P2 deliverables rather than settings; `make demo-up` asserts concurrency > `maxReplicas`; DI-04/DI-08/DI-09 fail loudly if any of the three is wrong. Third revision of this risk, and the smallest: HTTP has no driver to provision and no schema to agree on ([ADR-21](architecture.md#adr-21-how-does-work-reach-the-normalizer-pods)) | P2 |
| R-4 | kind cannot make resource exhaustion reproducible | Demo unconvincing | Low | Ballast pods as the primary mechanism (deterministic, and a better illustration than small nodes); kubelet reserved as a refinement | P2 |
| R-5 | Hysteresis parameters mis-tuned; sluggish or flappy | Poor demo, distrust | Medium | `E2E-06` bounds action counts; `SK-01` soak; tuning table in [scaling-algorithm § 12](scaling-algorithm.md#12-tuning-guidance) | P3, P4 |
| R-6 | Durability assumptions violated by the workload implementation | S1 data loss in the demo | **High in v0.2** (was Medium) | D-01…D-07 are explicit requirements with owning DI tests; [FS-4.3](failure-scenarios.md#43-what-would-invalidate-the-claim) lists invalidating changes as review items. **The likelihood rose deliberately:** v0.1.2 enforced durability in a database, v0.2 depends on a NiFi processor configuration and an idempotent function, both of which a well-meaning change can break silently ([A-12](requirements.md#103-assumptions-that-must-hold-for-the-guarantees-to-be-meaningful)) | P2, P3 |
| R-7 | Scope creep toward a production controller | POC never lands | High | Phase gates with test-based exit criteria; P5 exists to absorb good ideas without absorbing schedule | all |
| R-8 | Metrics cardinality (per-node series) on large clusters | Prometheus load | Low | Per-node metrics toggleable; bounded by cluster size ([resource-calculation § 9](resource-calculation.md#9-complexity-and-performance)) | P4 |
| R-9 | Docs drift from implementation | Design set becomes misleading | Medium | Link check in CI; P4 documentation reconciliation task; ADRs amended, not rewritten | P4 |
| R-10 | ~~Work store cannot provide atomic exclusive claiming~~ **Dissolved in v0.2** | Was S1 in the demo | — | There is no shared store and no claim protocol; each request is handed to exactly one pod by the Service ([ADR-21](architecture.md#adr-21-how-does-work-reach-the-normalizer-pods), [FS-25](failure-scenarios.md#fs-25-withdrawn-shared-work-store-claim-semantics)) | Dissolved |
| R-13 | ~~Work store becomes the bottleneck~~ → **NiFi client concurrency bounds throughput** | Replicas stop converting into throughput; the demo appears to scale while achieving nothing | Medium | `InvokeHTTP` concurrency > `maxReplicas`, asserted by `make demo-up`; processing rate plotted beside replica count; **DI-08 gates P2 and fails the build on a flat curve**; recorded as [A-13](requirements.md#103-assumptions-that-must-hold-for-the-guarantees-to-be-meaningful)/[A-15](requirements.md#103-assumptions-that-must-hold-for-the-guarantees-to-be-meaningful) ([FS-28](failure-scenarios.md#fs-28-adding-replicas-does-not-add-throughput)) | P2, P4 |
| R-14 | ~~`leaseDuration` mis-set below the maximum item time~~ → **NiFi response timeout below the maximum normalization time** | Duplicate normalization under peak load — wasted capacity that looks like insufficient capacity | Medium | Asserted at demo start-up; duplicate-rate metric; DI-04 proves duplicates are byte-identical and therefore harmless, so the failure costs CPU rather than correctness ([FS-29](failure-scenarios.md#fs-29-a-retried-request-is-normalized-twice)) | P2 |
| R-15 | The Normalizer acquires shared state during implementation — a cache, a counter, an output index | Silently reintroduces the coupling v0.2 removed; A-15 breaks and DI-08 goes flat | Medium | WR-01…WR-03 are testable requirements, not guidance; DI-08 is the detector; a stateless-by-review rule on `internal/normalizer` ([FS-4.3](failure-scenarios.md#43-what-would-invalidate-the-claim)) | P2 |
| R-11 | A GitOps controller or operator also manages `replicas` | Unbounded oscillation with an external actor | Medium | Detect, adopt, and abstain ([FR-31](requirements.md#review-driven-requirements-v011)); deployment prerequisite to grant field ownership ([A-10](requirements.md#103-assumptions-that-must-hold-for-the-guarantees-to-be-meaningful)) | P3 |
| R-12 | Performance-motivated refactors reintroduce the DR-01 ordering bug | Backoff silently becomes timer-based; recovery latency grows to 15 min | Medium | UT-25 asserts reset-on-recomputed-capacity; [I-10](design-review.md#5-immutable-phase-1-decisions) marks the ordering immutable | P3, P4 |

R-1, R-6, and R-13 are the three that would invalidate the project's claims rather than merely delay it, which
is why each has a gate rather than only a mitigation: R-1 has the P1 manual validation gate, R-6 has the
conservation assertion in every DI test, and R-13 has DI-08 gating all of P2.

**R-13's promotion is the honest cost of v0.2.** With a database in the middle, "does adding a replica add
throughput?" was answered by the store's concurrency; with an HTTP client in the middle, it is answered by
that client's configuration, which lives outside this repository. The controller can be entirely correct and
still demonstrate nothing, so the risk moved from *the store is a bottleneck* to *the caller is a bottleneck*
— a smaller problem, but one that fails more quietly.

---

## 6. Definition of done for the POC

The POC is complete when all of the following are true:

1. **Resource-aware behaviour demonstrated.** `E2E-02` shows demand for 8 replicas, room for 2, a partial
   scale-up, a sustained HOLD, and **zero Pending pods** — with the deficit visible in metrics and events.
2. **The contrast is recorded.** The same scenario with a stock HPA produces Pending pods; both runs captured.
3. **Recovery is prompt.** `E2E-03` shows a scale-up within one interval of capacity appearing, mid-backoff.
4. **No oscillation.** `E2E-06` and `SK-01` show bounded action counts under sawtooth load.
5. **Scaling does not cause data loss.** Every DI test's end-to-end conservation assertion passes, including
   under crash, eviction, and scale-down. Stated precisely: the claim is that the controller's actions do not
   *destroy* work, not that the POC guarantees durability — that belongs to NiFi's retry and the Normalizer's
   idempotency ([requirements § 7](requirements.md#7-durability-boundary-and-workload-responsibilities)).
6. **Replicas convert into throughput.** `DI-08` shows processing rate rising monotonically and near-linearly
   from 1 to 8 replicas. Without this, every other item above measures a controller scaling a workload that
   does not respond ([FS-28](failure-scenarios.md#fs-28-adding-replicas-does-not-add-throughput)).
7. **Failure behaviour is specified and verified.** Every `FS-xx` has a passing owning test
   ([test-plan § 11](test-plan.md#11-traceability-matrix)).
8. **Explainable.** Every decision is reconstructible from a single log line; every reason code is documented
   in the runbook.
9. **Reproducible by someone else.** `make demo-up` → five scenarios → `make demo-down` on a clean machine.
10. **Docs consistent with code.** Measured defaults folded back in; changed decisions reflected in their ADRs.
11. **Review findings closed.** Every `DR-xx` fix has a passing owning test, and the guarantee/non-guarantee
    statement in [requirements § 10](requirements.md#10-design-limitations-and-assumptions) matches observed
    behaviour — in particular that the POC claims "never *knowingly* infeasible", not "never Pending".

Deliberately **not** part of done: a CRD, multi-target support, production HA guarantees, or scheduler-grade
predicate fidelity.

The [immutable Phase 1 decisions](design-review.md#5-immutable-phase-1-decisions) (`I-1`…`I-16`) are the
constraints all of the above is built on. Changing one during implementation — for convenience, performance, or
expedience — invalidates parts of this plan and its tests, so it requires an amended ADR first.

---

## 7. Deferred scope and when to pick it up

| Deferred item | Non-goal | Deferred because | Pick it up when |
| --- | --- | --- | --- |
| Topology spread / pod anti-affinity modelling | [NG-7](requirements.md#3-non-goals) | Requires simulating placement; a naive approximation is worse than none ([ADR-08](architecture.md#adr-08-how-do-node-affinity-rules-affect-the-calculation)) | A target workload actually uses them, or `kss_pending_pod_remediations_total` climbs |
| `ResourceQuota` / `LimitRange` awareness | [NG-10](requirements.md#3-non-goals) | Reactive detection is cheap and sufficient for the POC | Deploying into a quota-governed namespace |
| Extended resources (GPU, hugepages, ephemeral storage) | [NG-9](requirements.md#3-non-goals) | Not present in the workload; the model generalises without redesign | The Normalizer pods need a device plugin resource |
| Priority classes and preemption | [NG-8](requirements.md#3-non-goals) | Turns an autoscaler into a cluster arbiter | The workload is genuinely higher-priority than its neighbours |
| Cluster autoscaler cooperation | [NG-2](requirements.md#3-non-goals) | Changes the meaning of a HOLD entirely | Running on an elastic node pool |
| Vertical scaling / request right-sizing | [NG-3](requirements.md#3-non-goals) | Interacts with the fit model in ways that need their own design | Requests prove chronically wrong ([resource-calculation § 4.1](resource-calculation.md#41-known-over-estimation-under-requesting-neighbours)) |
| Scale-to-zero | [NG-4](requirements.md#3-non-goals) | Needs a wake-up path that does not exist | Idle periods are long and costly |
| Multi-target / CRD | [NG-5](requirements.md#3-non-goals), [NG-11](requirements.md#3-non-goals) | Config-shaped change, not an algorithm change | A second workload needs scaling |
| Node-usage guardrail | — | Introduces a second, softer notion of "full" needing its own tuning | Measured throughput loss from under-requesting neighbours |
| Predictive / queueing-theory scaling | — | Needs a measured service-time distribution, which P2 only begins to collect | `itemsPerReplica` proves unstable (R-2) |
| Hot config reload | CR-3 | Restart is acceptable for a single-instance POC | Operators tune parameters frequently in production |

---

## 8. Open questions

**All questions are now closed for Phase 1.** Five are resolved by decision, two are deferred with a stated
default that implementation will follow unless data contradicts it. **No Phase 1 blockers remain.**

| # | Question | Status | Resolution and why |
| --- | --- | --- | --- |
| Q-1 | Pressure signal: NiFi queued FlowFiles, work-store count, or both? | **Resolved — reframed in v0.2** | **Neither, and the question was the wrong shape.** v0.1.2 answered "the work store's claimable count"; v0.2 answers "**whatever the configured source reports**", because the controller should not have an opinion about where a number comes from. It consumes *outstanding work* through a replaceable interface, and P1 ships `synthetic` and `none` while P2 adds `http`. No NiFi REST client and no database client is built in any phase ([ADR-22](architecture.md#adr-22-where-does-the-workload-pressure-signal-come-from), [FR-36](requirements.md#workload-signal-requirements-v02)) |
| Q-2 | Work store: RWX PVC or MinIO/S3? | **Dissolved in v0.2** | **There is no work store.** v0.1.2 answered this with a PostgreSQL work-item table, which was correct for the question and wrong for the project: it added a component, a schema, a driver, and a claim protocol to a controller that never reads or writes work. NiFi posts each record over HTTP and the Service picks a pod; the question of *where work rests between producer and consumer* no longer arises, because it does not rest ([ADR-21](architecture.md#adr-21-how-does-work-reach-the-normalizer-pods)) |
| Q-3 | Reaper as a sidecar or a CronJob? | **Dissolved** | **Neither — there is no reaper, and now no cleanup job either.** v0.1.2 dissolved the reaper into the claim query but kept a CronJob to prune `done` rows; v0.2 removes that too, along with the table it pruned. Crash recovery is NiFi re-sending a failed request ([FS-12](failure-scenarios.md#fs-12-normalizer-pod-crashes-mid-request)) |
| Q-4 | Does the utilization signal earn its keep? | **Deferred to P4, default: keep** | Genuinely needs data — the question is empirical, not architectural. P3 records how often utilization was the binding signal; removing it early would forfeit the only safety net against a wrong `itemsPerReplica` ([R-2](#5-risk-register)). Deferring costs nothing because the signal is already designed and tested |
| Q-5 | Should `HoldPendingPods` block revert-driven scale-downs? | **Resolved** | No. Remediation writes bypass the scale-down gates, including the settled-replicas gate — otherwise remediation would be impossible ([DR-03](design-review.md#dr-03-demand-driven-scale-down-can-fire-while-replicas-are-still-starting)) |
| Q-6 | Is `fitCapacityMarginPods: 1` right for all cluster sizes? | **Deferred to P4, default: 1** | Also empirical: the right margin follows from the observed estimate-versus-reality error rate, which only exists after P3. A constant is correct for the 3-node POC cluster; scaling it with cluster size is a Phase 5 concern that no Phase 1 code decision forecloses |
| Q-7 | Which environment runs E2E in CI? | **Resolved** | **kind on GitHub-hosted runners**, with the ballast sized to fit a 7 GB runner, and E2E/DI label-gated so they do not run on every push. Chosen because a self-hosted cluster adds infrastructure ownership to a POC; if the runner proves too small, the fallback is to shrink node count rather than to acquire hardware ([§4](#p4--hardening-and-operability-poc-complete)) |

### 8.1 Prerequisites for starting Phase 1 implementation

Every item is satisfied or explicitly scheduled; none is outstanding.

| # | Prerequisite | Status |
| --- | --- | --- |
| 1 | Architecture baseline agreed and written down | **Done** — [architecture § 11](architecture.md#11-phase-1-architecture-baseline) |
| 2 | ~~Durable work-store mechanism chosen, with claim/ack/recovery semantics specified as executable SQL~~ | **No longer a prerequisite** — the store is gone. Replaced by: *work-delivery mechanism chosen and its failure semantics stated* → **Done**, [ADR-21](architecture.md#adr-21-how-does-work-reach-the-normalizer-pods) |
| 3 | Demand-signal source decided and its dependency surface fixed | **Done** — [ADR-22](architecture.md#adr-22-where-does-the-workload-pressure-signal-come-from); a replaceable interface with three implementations, no credentials, no database |
| 4 | All open questions resolved or deferred with a default | **Done** — [§8](#8-open-questions); Q-4 and Q-6 deferred to P4 with defaults `keep` and `1` |
| 5 | Design-review findings folded in, each with an owning test | **Done** — `DR-01`…`DR-16`, [test-plan § 11](test-plan.md#11-traceability-matrix) |
| 6 | Immutable decisions recorded, so implementation cannot drift silently | **Done** — [`I-1`…`I-16`](design-review.md#5-immutable-phase-1-decisions), plus `I-20`…`I-22` for v0.2; `I-17`…`I-19` are superseded and marked as such |
| 7 | Config schema frozen for P0 (keys, defaults, validation rules) | **Done** — [requirements § 8](requirements.md#8-configuration-requirements), now including `workload.signal.*` in place of `workload.workStore.*` |
| 8 | Toolchain: Go ≥ 1.24, kind ≥ 0.23, kubectl ≥ 1.29, Docker, `make` | **Done except `kind`** — Go 1.27.1, Docker, `kubectl`, `make`, and `golangci-lint` are installed; `kind` is first needed by the P1 validation gate, not by P0 ([test-plan § 7.3](test-plan.md#73-prerequisites)). No database image and no JDBC driver, so the toolchain is the Go/Kubernetes basics and nothing else |
| 9 | RBAC set final, including the `replicasets` read added by review | **Done** — [architecture § 7](architecture.md#7-kubernetes-permissions-and-rbac) |
| 10 | CI shape decided (kind on hosted runners, E2E label-gated) | **Done** — Q-7 |

**Not prerequisites, deliberately.** `itemsPerReplica` (measured in P2, not guessed beforehand), the
utilization signal's long-term value (Q-4), and the fit-capacity margin policy (Q-6). Each is an empirical
question that P0/P1 code does not foreclose, and blocking on data that does not exist yet would be false rigour.

### 8.2 Formerly-blocking items, now dissolved

Each of these was answered twice: closed by adding a component in v0.1.2, then dissolved by removing the
component in v0.2. Recording both steps matters, because the second step is only defensible if the first one's
reasoning is visible — the database was not a mistake given the question, the *question* was the mistake.

| Item | Was (v0.1) | Closed as (v0.1.2) | Now (v0.2) |
| --- | --- | --- | --- |
| [DR-12](design-review.md#dr-12-the-poc-work-store-cannot-provide-the-claimed-semantics-on-the-proposed-environment) / Q-2 | **Blocking prerequisite for P2** — the work store's claim semantics were unimplementable on kind | Database-enforced claiming, gated by DI-08/DI-09 ([ADR-18](architecture.md#adr-18-what-is-the-durable-work-store-for-the-poc-pipeline)) | **Dissolved.** No store, so no claim semantics to implement or verify ([ADR-21](architecture.md#adr-21-how-does-work-reach-the-normalizer-pods)) |
| Q-1 | Open fork, to be measured in P2 | The store's claimable count ([ADR-19](architecture.md#adr-19-where-does-the-backlog-signal-come-from)) | **Reframed.** Any source behind one interface ([ADR-22](architecture.md#adr-22-where-does-the-workload-pressure-signal-come-from)) |
| Q-3 | Open fork, to be decided in P2 | Dissolved into the claim query; a pruning CronJob remained | **Fully dissolved.** No reaper, no CronJob, no table |

**What replaced the gate.** DR-12's gate existed because an unproven claim primitive would have invalidated
every conservation assertion downstream. v0.2 has an equivalent structural risk in a different place — an
unproven *throughput* response would invalidate every scaling demonstration downstream — so DI-08 inherits the
gating role rather than P2 losing its gate altogether
([FS-28](failure-scenarios.md#fs-28-adding-replicas-does-not-add-throughput)).
