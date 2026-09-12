# The demonstration pipeline

```
input files ──▶ NiFi 2.6.7 ──HTTP POST──▶ normalizer-service ──▶ Normalizer pods ──▶ normalized output
                                                     │
                              normalizer-metrics ◀── pressure scrape ──── KubeScaleSense (dry-run)
```

A real workload for KubeScaleSense to measure. NiFi moves records, the
Normalizer does real CPU work on them, and the controller watches — and in
Phase 2 that is **all** it does. Replicas start at 1 and stay at 1.

## Safety

`make demo-up` creates a kind cluster named `kubescalesense-demo` and touches
nothing else. Every `kubectl` call in these scripts is pinned to
`--context kind-kubescalesense-demo` and the scripts abort if that context does
not exist; the ambient current-context is never read. `make demo-down` takes no
arguments and deletes that one cluster, only if `kind` reports owning it.

Any existing cluster in your kubeconfig — k3s, a work cluster, anything — is
unreachable from these scripts by construction, not by care.

## Prerequisites

| Tool | Version | Why |
| --- | --- | --- |
| `docker` | any recent | kind's container runtime |
| `kind` | ≥ 0.23 | the isolated cluster |
| `kubectl` | ≥ 1.29 | everything |
| `jq`, `curl` | any | the NiFi flow assertion reads the REST API |

```bash
go install sigs.k8s.io/kind@v0.23.0
# or: https://kind.sigs.k8s.io/docs/user/quick-start/#installation
```

Expect to need about **6 GiB of memory and 4 CPUs**. The four-node topology plus
NiFi's JVM plus several Normalizer replicas does not fit in much less, and the
failure mode is OOM kills that look like a broken pipeline. `demo-up.sh` checks
and warns rather than refusing, because "it may well work" is true and being
told why it didn't is what matters.

Short of memory? `make demo-spike MODE=http` drives the same workload straight
at `normalizer-service` with NiFi out of the path. That exercises the controller,
the signal, the feasibility calculation, and the dry-run guarantee — everything
except NiFi itself.

## Running it

```bash
make demo-up                   # create the cluster and bring up the pipeline
make demo-observe              # follow the controller's decisions
make demo-spike                # drive LOW -> HIGH -> LOW through NiFi
make demo-down                 # delete the cluster
```

The interesting part is `make demo-observe` while `make demo-spike` runs. During
the HIGH phase the controller logs something like:

```json
{"msg":"decision","currentReplicas":1,"pressure":340,"demandReplicas":8,
 "fitCapacity":3,"targetReplicas":4,"reason":"ScaleUpPartial","dryRun":true}
```

Read that as: demand is 8 replicas, only 3 more will fit on this cluster, so
the controller would go to 4 — and **it does not**, because `dryRun` is true.

```bash
kubectl --context kind-kubescalesense-demo -n data-pipeline get deploy normalizer
# NAME         READY   UP-TO-DATE   AVAILABLE
# normalizer   1/1     1            1
```

Still 1, during and after the spike. That is the Phase 2 acceptance criterion,
and it is also why `demo-up.sh` reads the controller's `dryRun` back out of the
cluster and dies if it is anything but `true`.

To see the pipeline work at other replica counts, set them yourself:

```bash
kubectl --context kind-kubescalesense-demo -n data-pipeline scale deploy/normalizer --replicas=3
```

Nothing in the Normalizer changes; pressure is summed over however many pods
are Ready.

## Workload generation

`tests/demo/loadgen` is a **demonstration tool, not part of KubeScaleSense** —
it lives under `tests/` for exactly that reason. It knows nothing about
scaling, replicas, or Kubernetes; it produces records.

Everything is deterministic: the same flags produce byte-identical records, so
two runs are comparable and a surprising result is reproducible.

```bash
tests/e2e/demo-spike.sh                     # 3-phase spike through NiFi
RECORDS=2000 HIGH_RATE=400 tests/e2e/demo-spike.sh
MODE=http CONCURRENCY=32 tests/e2e/demo-spike.sh
```

| Variable | Default | Controls |
| --- | --- | --- |
| `RECORDS` | 1200 | total records |
| `LOW_RATE` | 20 | records/second in the LOW phases |
| `HIGH_RATE` | 200 | records/second in the HIGH phase |
| `PHASE` | 60s | duration of each phase |
| `BURST` | 10 | records per batch — bursty arrival, not a metronome |
| `PAD` | 0 | extra bytes per record, for payload-size effects |
| `CONCURRENCY` | 16 | in-flight requests (`MODE=http` only) |

The LOW → HIGH → LOW shape is the point. A constant load shows a controller
scaling up once; a spike shows scale-up, the fit ceiling, the cooldown, and the
asymmetric scale-down — which is where the design's actual behaviour lives.

## Input, and the SFTP path it stands in for

The demo generates records into an `emptyDir` shared between the `loadgen`
sidecar and NiFi in the `nifi-0` pod, and NiFi picks them up with `ListFile` /
`FetchFile`.

The realistic deployment replaces two processors and changes nothing else:

| Demo | Realistic |
| --- | --- |
| `ListFile` on `/data/input` | `ListSFTP` on the collector's outbox |
| `FetchFile` | `FetchSFTP` |

Hostname, credentials, and host-key policy are then operator-supplied — which
is exactly why the demo does not use SFTP. Committing an SFTP account to this
repository to make `make demo-up` work would be trading the project's
no-credentials property for a convenience, and the pipeline downstream of
`FetchFile` is byte-for-byte identical either way.

A **sidecar over an `emptyDir`**, rather than a shared volume, because the
architecture forbids a shared RWX filesystem (ADR-13) — and an `emptyDir` shared
between two containers in one pod is the only way to share a directory without
one. The generator can also be driven over HTTP: it serves `POST /spike` on
`:9000`, which is how `demo-spike.sh` triggers a run without restarting NiFi.

## Output

`PutFile` writes normalized records to `/data/output` inside the NiFi pod, and
rejected records to `/data/output/rejected`.

```bash
kubectl --context kind-kubescalesense-demo -n data-pipeline exec nifi-0 -c nifi -- \
  sh -c 'ls /data/output | wc -l; head -1 /data/output/$(ls /data/output | head -1)'
```

**This is not durable storage and the project does not claim it is.** It is an
`emptyDir`: the output is gone when the pod is. Phase 2 demonstrates that a
workload can be measured and that the measurement drives a correct decision. It
does not demonstrate end-to-end durable delivery, and the v0.2 architecture
deliberately contains nothing that would — no database, no broker, no object
store. Durability, where it exists, is NiFi's retry on a failed POST and
nothing more (D-02).

## The NiFi flow

`nifi/flow/normalizer-pipeline.json` is the flow:

```
ListFile ─▶ FetchFile ─▶ SplitText ─▶ InvokeHTTP ─┬─▶ Response  ─▶ PutFile /data/output
                                                  └─▶ No Retry  ─▶ PutFile /data/output/rejected
```

### Three settings that are deliverables, not preferences

Each one fails **silently** — the pipeline keeps running and the numbers stop
meaning what they appear to — which is why `make demo-up` asserts all three
against the running instance and refuses to declare success if any is wrong.

**Concurrent tasks (16) must exceed `maxReplicas` (12).** Work is *pushed*, so
throughput is `min(client concurrency, replica capacity)`. If NiFi dispatches
fewer concurrent requests than there are replicas, the controller scales up
correctly and throughput does not improve — the graph shows replicas rising
while the backlog also rises, which is indistinguishable from a broken
controller (A-13, FS-28).

**`Retry` and `Failure` are retried; `No Retry` is not.** There is no queue, no
store, and no transaction between NiFi and the pods, so retry on a failed POST
is the entire durability guarantee (D-02). But a `400` means the record is
malformed and will fail identically forever — retrying *that* is an infinite
loop that turns one bad line into a pipeline outage. Hence `No Retry` goes to
the rejected sink, unretried, and still visibly accounted for.

**Read timeout (60s) must exceed the Normalizer's worst case (10s queue + 0s
delay).** Otherwise every slow record is retried while the first attempt is
still being served, and the pool does duplicate work exactly under the load
where it can least afford to (FS-29).

`tests/demo/nififlow` checks all three in the shipped file on every commit;
`tests/e2e/assert-nifi-flow.sh` checks them in the running NiFi. Both are
needed: the file can be correct and never imported, and a running flow can be
edited in the UI.

### Importing it

`demo-up.sh` cannot import a flow — NiFi's API requires an authenticated
session and this demo deliberately has no credentials — so this is one manual
step:

```bash
kubectl --context kind-kubescalesense-demo -n data-pipeline port-forward nifi-0 8080:8080
```

Open <http://127.0.0.1:8080/nifi>, drag a **Process Group** onto the canvas,
choose **Browse**, and upload `nifi/flow/normalizer-pipeline.json`. Then enter
the group, select all, and start it. Verify with:

```bash
tests/e2e/assert-nifi-flow.sh 12
```

> **Caveat, stated plainly.** This flow definition has **not** been imported
> into a running NiFi in this repository's development environment, because
> `kind` is not installed there and NiFi was never started. It is written
> against the NiFi 2.6.7 bundle coordinates and its invariants are
> machine-checked as a file, but "NiFi accepts this JSON" is unverified. If the
> import fails, build the flow by hand from the table below — the properties,
> not the file, are the specification.

| Processor | Property | Value |
| --- | --- | --- |
| `ListFile` | Input Directory | `/data/input` |
| | File Filter | `pm-.*\.csv` |
| `FetchFile` | Completion Strategy | `Delete File` |
| `SplitText` | Line Split Count | `1` |
| `InvokeHTTP` | HTTP Method | `POST` |
| | HTTP URL | `http://normalizer-service.data-pipeline.svc/normalize` |
| | Request Content-Type | `text/plain` |
| | Socket Read Timeout | `60 secs` |
| | Concurrent tasks | `16` |
| | Retried relationships | `Retry`, `Failure` |
| | Retry count / backoff | `10` / penalize, max `5 mins` |
| `PutFile` (output) | Directory | `/data/output` |
| `PutFile` (rejected) | Directory | `/data/output/rejected` |

NiFi's role is to move records and retry failures. It does not know the cluster
exists: it holds no autoscaling logic, reads no metrics, and has no path to the
Kubernetes API.

## Ballast

`ballast.yaml` runs `pause` containers with `requests == limits`, pinned to
specific nodes, so the demo cluster's nodes have *unequal* free capacity.

Without it every node looks the same, the per-node bin-packing calculation
reduces to division, and the feasibility logic — the part of KubeScaleSense that
is actually novel — is never exercised. With it, a node that can fit two more
pods and a node that can fit none are both present, and `HoldNoCapacity` and
`ScaleUpPartial` become reachable states rather than untested code paths.

Adjust the replica counts to move the fit ceiling and watch the decision change.

## What is in this directory

| File | |
| --- | --- |
| `namespace.yaml` | the `data-pipeline` namespace |
| `nifi.yaml` | NiFi 2.6.7 StatefulSet, headless Service, `loadgen` sidecar |
| `nifi/flow/normalizer-pipeline.json` | the flow definition |
| `ballast.yaml` | uneven node capacity, so feasibility matters |
| `kubescalesense-configmap.yaml` | the controller's demo config: `http` signal, `dryRun: true` |

The Normalizer's own manifests are in [`../normalizer/`](../normalizer/README.md).

## NiFi has no credentials, on purpose

The StatefulSet sets `NIFI_WEB_HTTP_PORT=8080`, which runs NiFi's HTTP listener
with anonymous access and skips NiFi 2.x's single-user credential generation
entirely. No Secret is created and none is committed.

This is acceptable *only* because the endpoint is not reachable from outside the
cluster: there is no Ingress and no NodePort, so getting to the UI requires
`kubectl port-forward`, which requires cluster credentials already. It would be
wrong in any shared environment. A real deployment uses HTTPS with a real
identity provider, and the flow above is unchanged by that.
