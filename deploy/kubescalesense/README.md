# Controller manifests

The controller's identity, permissions, configuration, and pod. The demo
environment applies them (`make demo-up`, with the demo ConfigMap from
`deploy/demo/`); nothing in CI does.

## Files

| File | Purpose |
| --- | --- |
| `namespace.yaml` | The `kubescalesense` namespace, separate from the workload's |
| `serviceaccount.yaml` | The controller's identity; the only credential it holds |
| `clusterrole.yaml` | Cluster-scoped reads: nodes, pods, and `metrics.k8s.io` |
| `clusterrolebinding.yaml` | Binds the ClusterRole to the ServiceAccount |
| `role.yaml` | Namespaced rules in `data-pipeline`: reads of the target, and the three writes |
| `rolebinding.yaml` | Binds the Role to the ServiceAccount |
| `configmap.yaml` | A copy of `config/kubescalesense.yaml` |
| `deployment.yaml` | The controller pod |

There is no `Secret`, in this phase or any later one. The controller has no
workload credentials to hold: the pressure signal is generated locally or
scraped over in-cluster HTTP ([CR-5](../../docs/requirements.md#8-configuration-requirements)).

## How the roles grew

Least privilege ([FR-25](../../docs/requirements.md#4-functional-requirements))
means each rule is added in the phase that first needs it, alongside the code
that uses it. P0 shipped empty roles; P1 added the reads; P3 added the writes.

The canonical target state is the RBAC table in
[architecture § 7](../../docs/architecture.md#7-kubernetes-permissions-and-rbac).
The one rule still deferred — `leases`, for leader election in P4 — is listed in
`role.yaml` with the phase that introduces it, so the gap between "what the
design specifies" and "what is granted today" is visible in the file.

The write rules are all namespaced and all in `role.yaml`:

| Rule | What it can change |
| --- | --- |
| `apps` `deployments/scale` get, update, patch | `spec.replicas` of the target — the only write that changes how many pods run |
| `events.k8s.io` `events` create | Adds an announcement about the target |
| core `pods` patch | The `pod-deletion-cost` annotation; RBAC alone would allow more, so the controller's transport rejects any other patch body |

`internal/controller/nowrite_test.go` fails the build if a write rule is added
or widened, or a write call site appears, without being enumerated there.

The phase split on `deployments` is worth noticing: the **read** arrived in P1
so the fit estimator could be validated against a live cluster in dry-run,
while `deployments/scale` was withheld until P3. A P1 bug could not change a
replica count even if it tried.

## Applying them by hand

```sh
kubectl apply -f namespace.yaml
kubectl create namespace data-pipeline   # the target namespace, if absent
kubectl apply -f .
```

With the shipped `dryRun: true` the controller observes and reports decisions,
and builds no client that can write. Set `controller.dryRun: false` in the
ConfigMap to let it act.
