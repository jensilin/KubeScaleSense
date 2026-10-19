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
| `role.yaml` | Namespaced rules in `data-pipeline`: reads of the target, and the four writes |
| `rolebinding.yaml` | Binds the Role to the ServiceAccount |
| `configmap.yaml` | A copy of `config/kubescalesense.yaml` |
| `deployment.yaml` | The controller: two replicas, one of which holds the leader lease |

There is no `Secret`, in this phase or any later one. The controller has no
workload credentials to hold: the pressure signal is generated locally or
scraped over in-cluster HTTP ([CR-5](../../docs/requirements.md#8-configuration-requirements)).

## How the roles grew

Least privilege ([FR-25](../../docs/requirements.md#4-functional-requirements))
means each rule is added in the phase that first needs it, alongside the code
that uses it. P0 shipped empty roles; P1 added the reads; P3 added the writes;
P4 added the leader lease. The manifests now grant the whole RBAC table in
[architecture § 7](../../docs/architecture.md#7-kubernetes-permissions-and-rbac).

The write rules are all namespaced and all in `role.yaml`:

| Rule | What it can change |
| --- | --- |
| `apps` `deployments/scale` get, update, patch | `spec.replicas` of the target — the only write that changes how many pods run |
| `events.k8s.io` `events` create | Adds an announcement about the target |
| core `pods` patch | The `pod-deletion-cost` annotation; RBAC alone would allow more, so the controller's transport rejects any other patch body |
| `coordination.k8s.io` `leases` get, create, update | The leader lease `kubescalesense-normalizer`; RBAC cannot confine `create` to a name, so the transport refuses any other lease |

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
and builds no client that can write; both replicas report, and neither takes
the lease. Set `controller.dryRun: false` in the ConfigMap to let it act. The
replicas then elect one writer through the lease, and the other stands by:

```sh
kubectl -n data-pipeline get lease kubescalesense-normalizer \
  -o jsonpath='{.spec.holderIdentity}'      # <pod name>_<uuid> of the leader
```

Setting `leaderElection: false` as well is only safe with `replicas: 1`; the
controller says so in a warning at startup.
