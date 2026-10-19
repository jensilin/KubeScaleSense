#!/usr/bin/env bash
#
# Print the logs of the controller replica that holds the leader lease.
#
#   tests/e2e/controller-logs.sh [kubectl logs flags]    e.g. -f --tail=20
#
# The controller runs two replicas (FR-26). `kubectl logs
# deployment/kubescalesense` reads whichever one kubectl picks, and in live mode
# that is the standby half the time: a log that says it is standing by and
# holds no decision. The lease holder's identity is <pod name>_<uuid>, so the
# pod to read is what precedes the underscore.
#
# In dry-run no replica campaigns and the lease has no holder. Every replica
# then reports its own decisions, so any one of them is the right log.
#
# SAFETY. Reads only, and only from the kind cluster named kubescalesense-demo.
set -euo pipefail

CONTEXT="kind-kubescalesense-demo"
CONTROLLER_NAMESPACE="kubescalesense"
NAMESPACE="data-pipeline"
LEASE="kubescalesense-normalizer"

die() { printf '\033[1;31m error:\033[0m %s\n' "$*" >&2; exit 1; }

kubectl config get-contexts -o name 2>/dev/null | grep -qx "${CONTEXT}" \
  || die "context ${CONTEXT} does not exist. Run: make demo-up"

kubectl() { command kubectl --context "${CONTEXT}" "$@"; }

holder="$(kubectl -n "${NAMESPACE}" get lease "${LEASE}" \
  -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || true)"
pod="${holder%%_*}"

# A holder whose pod is gone is a leader that died without releasing: the lease
# has not expired yet, and no replica is deciding until it does.
if [[ -n "${pod}" ]] && kubectl -n "${CONTROLLER_NAMESPACE}" get pod "${pod}" >/dev/null 2>&1; then
  printf '# logs of %s, which holds lease %s/%s\n' "${pod}" "${NAMESPACE}" "${LEASE}" >&2
  kubectl -n "${CONTROLLER_NAMESPACE}" logs "pod/${pod}" "$@"
else
  if [[ -n "${pod}" ]]; then
    printf '# lease %s/%s is held by %s, which no longer exists; reading any replica\n' \
      "${NAMESPACE}" "${LEASE}" "${pod}" >&2
  else
    printf '# lease %s/%s has no holder (dry-run, or between leaders); reading any replica\n' \
      "${NAMESPACE}" "${LEASE}" >&2
  fi
  kubectl -n "${CONTROLLER_NAMESPACE}" logs deployment/kubescalesense "$@"
fi
