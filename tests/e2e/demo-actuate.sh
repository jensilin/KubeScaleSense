#!/usr/bin/env bash
#
# Turn the demo controller's actuation on or off.
#
#   tests/e2e/demo-actuate.sh on    # dryRun: false — the controller writes
#   tests/e2e/demo-actuate.sh off   # dryRun: true  — back to observing
#
# This is deliberately a separate command from demo-up.sh. The documented order
# is to bring the environment up in dry-run, check that the decisions it reports
# are the ones you expected, and only then let it act on them — and an
# environment that could come up live would make that step skippable.
#
# SAFETY. Like every script here, this touches exactly one cluster: the kind
# cluster named kubescalesense-demo. Every kubectl call is pinned to
# --context kind-kubescalesense-demo and the script aborts if that context does
# not exist, so it cannot act on whatever cluster your kubeconfig happens to be
# pointed at.
set -euo pipefail

CLUSTER_NAME="kubescalesense-demo"
CONTEXT="kind-${CLUSTER_NAME}"
CONTROLLER_NAMESPACE="kubescalesense"
NAMESPACE="data-pipeline"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m warning:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m error:\033[0m %s\n' "$*" >&2; exit 1; }

mode="${1:-}"
case "${mode}" in
  on)  dry_run="false" ;;
  off) dry_run="true"  ;;
  *)   die "usage: $0 on|off" ;;
esac

kubectl config get-contexts -o name | grep -qx "${CONTEXT}" \
  || die "context ${CONTEXT} does not exist; run make demo-up first"
kubectl() { command kubectl --context "${CONTEXT}" "$@"; }

# The RBAC check, before the mode change rather than after.
#
# Without deployments/scale the controller would come up live, decide correctly,
# and fail every write with a 403 — which is the intended behaviour on a
# misconfigured cluster, but a confusing way to discover that the Role was not
# applied. Asking the API server directly is better than reading the manifest:
# it answers what the ServiceAccount can actually do.
if [[ "${mode}" == "on" ]]; then
  log "checking the controller's scale permission"
  # --subresource=scale, not the older `deployments/scale` spelling: kubectl
  # stopped resolving the slash form and now answers `no` for a permission that
  # is in fact granted, which would make this gate refuse a correct cluster.
  can_scale="$(kubectl auth can-i update deployments --subresource=scale \
    -n "${NAMESPACE}" \
    --as "system:serviceaccount:${CONTROLLER_NAMESPACE}:kubescalesense" 2>/dev/null || true)"

  if [[ "${can_scale}" != "yes" ]]; then
    die "the controller's ServiceAccount cannot update deployments/scale in ${NAMESPACE}. Apply deploy/kubescalesense/role.yaml and deploy/kubescalesense/rolebinding.yaml, then retry."
  fi

  # And the permission it must NOT have. A Role that granted `deployments`
  # update would let a bug rewrite the pod template, and this is the cheapest
  # possible moment to notice that someone widened it.
  can_update="$(kubectl auth can-i update deployments \
    -n "${NAMESPACE}" \
    --as "system:serviceaccount:${CONTROLLER_NAMESPACE}:kubescalesense" 2>/dev/null || true)"

  if [[ "${can_update}" == "yes" ]]; then
    die "the controller's ServiceAccount can update the Deployment object itself, not just its scale subresource. That is wider than P3 intends and would let a bug rewrite the pod template (FS-21)."
  fi

  can_delete_pods="$(kubectl auth can-i delete pods \
    -n "${NAMESPACE}" \
    --as "system:serviceaccount:${CONTROLLER_NAMESPACE}:kubescalesense" 2>/dev/null || true)"

  if [[ "${can_delete_pods}" == "yes" ]]; then
    die "the controller's ServiceAccount can delete pods. It must never hold that permission (I-11): scale-down goes through the ReplicaSet controller so that PodDisruptionBudgets and graceful termination apply."
  fi
fi

log "setting dryRun: ${dry_run} in the demo ConfigMap"

# Rendered from the shipped file rather than patched in place, so the ConfigMap
# always matches deploy/demo/kubescalesense-configmap.yaml with exactly one
# value changed. A sequence of in-place patches drifts from the file that
# documents it.
rendered="$(sed -E "s/^([[:space:]]*)dryRun:.*/\1dryRun: ${dry_run}/" \
  deploy/demo/kubescalesense-configmap.yaml)"

grep -q "dryRun: ${dry_run}" <<<"${rendered}" \
  || die "failed to rewrite dryRun in deploy/demo/kubescalesense-configmap.yaml"

kubectl apply -f - <<<"${rendered}"

# A restart is required, not a nicety: the configuration is read once at
# startup, deliberately, so that the mode a running process is in cannot change
# underneath it.
log "restarting the controller so it reads the new mode"
kubectl -n "${CONTROLLER_NAMESPACE}" rollout restart deployment/kubescalesense
kubectl -n "${CONTROLLER_NAMESPACE}" rollout status deployment/kubescalesense --timeout=120s

# Confirmed from the controller's own startup log rather than from the
# ConfigMap. The ConfigMap says what we asked for; the log says what the
# process believes, which is the thing that matters.
log "confirming the mode from the controller's startup log"
sleep 2
banner="$(kubectl -n "${CONTROLLER_NAMESPACE}" logs deployment/kubescalesense --tail=50 2>/dev/null || true)"

if [[ "${mode}" == "on" ]]; then
  if grep -q "live actuation is enabled" <<<"${banner}"; then
    log "the controller is LIVE and will change the replica count of ${NAMESPACE}/normalizer"
  else
    warn "the controller did not announce live actuation. Check:"
    warn "  kubectl --context ${CONTEXT} -n ${CONTROLLER_NAMESPACE} logs deployment/kubescalesense"
  fi
else
  if grep -q "no scaling will be performed" <<<"${banner}"; then
    log "the controller is back in dry-run and writes nothing"
  else
    warn "the controller did not announce dry-run. Check:"
    warn "  kubectl --context ${CONTEXT} -n ${CONTROLLER_NAMESPACE} logs deployment/kubescalesense"
  fi
fi
