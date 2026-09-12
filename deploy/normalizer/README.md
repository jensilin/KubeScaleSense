# Normalizer

The demonstration workload: a small Go service that turns one raw performance
record into one normalized JSON record, and costs real CPU doing it.

It exists to give KubeScaleSense something true to measure. It is **not** part
of KubeScaleSense, knows nothing about scaling, and has no Kubernetes
permissions of any kind.

## Interface

| Endpoint | Port | Purpose |
| --- | --- | --- |
| `POST /normalize` | 8080 | One record in, one normalized record out |
| `GET /healthz` | 8081 | Liveness — the process is running |
| `GET /readyz` | 8081 | Readiness — accepting work and not draining |
| `GET /metrics` | 8081 | Prometheus exposition |

Input is one CSV line, `timestamp,node,counter,value`:

```
2026-03-14T12:00:00Z,epdg-01,pdp.sessions.active,4821
```

Output is one JSON object with a fixed key order and a checksum over the
canonical fields:

```json
{"observedAt":"2026-03-14T12:00:00Z","node":"epdg-01","counter":"pdp.sessions.active","value":4821,"checksum":"0b511d44…"}
```

### Status codes are a contract, not a detail

NiFi decides whether to retry from the status code alone, so each one means
exactly one thing:

| Code | Meaning | NiFi's obligation |
| --- | --- | --- |
| `200` | Normalized. The body is the record. | Write the output; do not re-send. |
| `400` | The record is malformed and always will be. | **Never retry.** Route to the rejected sink. |
| `503` | Back-pressure: the queue is full or the wait timed out. | Retry with backoff. |
| `500` | Internal failure. | Retry with backoff. |

The distinction between 400 and 503 is the poison-pill guard. Retrying a `400`
is an infinite loop that turns one bad line into a pipeline outage, so
`No Retry` is deliberately *not* in the flow's retried relationships.

A response is never sent before the work is finished, so a `200` means the
record was normalized — not that it was accepted for normalization.

## The two Services, and why there are two

```
normalizer-service    ClusterIP   :80   -> 8080   data plane, load-balanced
normalizer-metrics    headless    :8081          every Ready pod, individually
```

`normalizer-service` is what NiFi posts to. A ClusterIP is exactly right here:
each request should go to whichever pod is free.

`normalizer-metrics` is headless (`clusterIP: None`) and that is load-bearing.
Pressure is *queued + in-flight summed over all pods*. Scraping a ClusterIP
would return one pod's numbers and under-report total pressure by roughly the
replica count — correct-looking at one replica, and wrong at every other count,
in the direction that causes scale-down under load. The headless Service
resolves to one address per Ready pod, and the controller's `http` signal source
scrapes and sums all of them.

The scrape is **all-or-nothing**: if any pod fails to answer, the whole sample
is reported unavailable rather than summed short. A partial sum is
indistinguishable from a genuine drop in load, and the controller would react to
it by scaling down.

`publishNotReadyAddresses` is `false`. A pod that is starting has no meaningful
queue depth, and including it would dilute the average that decides scale-down.

## Configuration

Every setting is a `NORMALIZER_`-prefixed environment variable, supplied by
[`configmap.yaml`](configmap.yaml). `normalizer -validate` checks a
configuration and exits without serving; the container does the same on startup
and exits non-zero rather than serving with a configuration nobody intended.

The settings that change the demonstration's behaviour:

| Variable | Demo value | What it controls |
| --- | --- | --- |
| `NORMALIZER_COST_ROUNDS` | `250000` | CPU cost per record. The knob that makes a pod saturate. |
| `NORMALIZER_MAX_CONCURRENT` | `4` | Records processed at once. Above this, records queue. |
| `NORMALIZER_QUEUE_LIMIT` | `64` | Records allowed to wait. Beyond this, `503`. |
| `NORMALIZER_QUEUE_TIMEOUT` | `10s` | How long a record may wait before being shed. |
| `NORMALIZER_PROCESSING_DELAY` | `0s` | Added latency without CPU cost, for testing stalls. |

`COST_ROUNDS` is iterated SHA-256, not a sleep and not an infinite loop: it is
work the Go compiler cannot elide, it is deterministic, and it shows up in
`container_cpu_usage_seconds_total` the same way real parsing would. Its digest
is returned in the `X-Normalizer-Work-Digest` **header**, never in the body —
so retuning the cost never changes a single output byte, which is what keeps
the byte-identical determinism guarantee (DI-04) true.

### The one number KubeScaleSense depends on

```yaml
resources:
  requests:
    cpu: 500m
    memory: 512Mi
```

Resource feasibility is computed from the **request**, so this value decides how
many replicas fit on the demo cluster. Raising it makes the demonstration
impossible on a small machine; lowering it makes every replica fit and the
feasibility logic never engages, which is the interesting part.

The CPU *limit* is `2000m` — four times the request, because four concurrent
records at full cost genuinely want more than 500m and throttling a workload
while measuring its queue depth would make the measurement meaningless. Memory
request and limit are equal (`512Mi`), so the pod is Guaranteed for memory and
cannot be OOM-killed by its own burst.

KubeScaleSense reads these values from the cluster. It is **not** told them, and
the Normalizer does not compute anything about capacity — the controller owns
that calculation.

## Draining

Termination order matters, because a pod that stops answering its readiness
probe before it stops accepting work will have requests routed to it while it
shuts down:

1. `/readyz` starts failing (`normalizer_ready` goes to 0).
2. The process waits `NORMALIZER_DRAIN_DELAY` (5s) — long enough for kubelet to
   observe the failure and for endpoint removal to propagate.
3. The data plane stops accepting connections and finishes in-flight records.
4. The admin plane closes last, so the final metrics remain scrapeable.

`terminationGracePeriodSeconds: 45` is above `DRAIN_DELAY + SHUTDOWN_TIMEOUT`
(5s + 30s), so the sequence completes before SIGKILL rather than being cut off
mid-record.

`/healthz` keeps returning 200 while draining, deliberately: a draining pod is
not broken, and failing liveness would have kubelet restart a pod that is
shutting down correctly.

## Tests

```bash
go test ./internal/normalizer/... ./cmd/normalizer/...
```

No cluster required. Coverage is 98% of `internal/normalizer`, including the
determinism guarantee (equivalent inputs produce byte-identical output),
concurrent stability, capacity enforcement, queue shedding, and the drain order
above.
