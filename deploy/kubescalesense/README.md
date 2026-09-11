# Controller manifests

Phase 0 skeleton. Nothing here is applied by any `make` target or CI job — the
manifests exist so that the RBAC surface, security context, and config mount can
be reviewed before any code depends on them.

## Files

| File | Purpose |
| --- | --- |
| `namespace.yaml` | The `kubescalesense` namespace, separate from the workload's |
| `serviceaccount.yaml` | The controller's identity; the only credential it holds |
| `clusterrole.yaml` | Cluster-scoped rules — **empty in P0** |
| `clusterrolebinding.yaml` | Binds the ClusterRole to the ServiceAccount |
| `role.yaml` | Namespaced rules in `data-pipeline` — **empty in P0** |
| `rolebinding.yaml` | Binds the Role to the ServiceAccount |
| `configmap.yaml` | A copy of `config/kubescalesense.yaml` |
| `deployment.yaml` | The controller pod |

There is no `Secret`, in this phase or any later one. The controller has no
workload credentials to hold: the pressure signal is generated locally or
scraped over in-cluster HTTP ([CR-5](../../docs/requirements.md#8-configuration-requirements)).

## Why the roles are empty

`cmd/kubescalesense` makes no Kubernetes API calls in Phase 0. Least privilege
([FR-25](../../docs/requirements.md#4-functional-requirements)) therefore means
an empty rule set, so each rule is added in the phase that first needs it,
alongside the code that uses it.

The canonical target state is the RBAC table in
[architecture § 7](../../docs/architecture.md#7-kubernetes-permissions-and-rbac).
Each deferred rule is listed in `clusterrole.yaml` and `role.yaml` with the
phase that introduces it, so the gap between "what the design specifies" and
"what is granted today" is visible in the file rather than tracked elsewhere.

The phase split on `deployments` is worth noticing: the **read** arrives in P1 so
the fit estimator can be validated against a live cluster in dry-run, while
`deployments/scale` is withheld until P3. A P1 bug therefore cannot change a
replica count even if it tries.

## Applying them by hand

Phase 0 is not expected to run in a cluster, but it will:

```sh
kubectl apply -f namespace.yaml
kubectl create namespace data-pipeline   # the target namespace, if absent
kubectl apply -f .
```

The pod will start, log that the foundation is up, and sit idle. It does not
scale anything, and `kubectl logs` says so explicitly on every start.
