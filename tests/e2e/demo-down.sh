#!/usr/bin/env bash
#
# Tear down the demonstration environment.
#
# SAFETY. This deletes exactly one cluster, by name, and only if kind reports
# that it owns it. It cannot touch a cluster kind did not create, it never reads
# the ambient current-context, and it takes no arguments — there is no way to
# point it at something else.
set -euo pipefail

CLUSTER_NAME="kubescalesense-demo"

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m error:\033[0m %s\n' "$*" >&2; exit 1; }

command -v kind >/dev/null 2>&1 || die "kind is not installed, so there is no demo cluster to delete."

if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  log "no kind cluster named ${CLUSTER_NAME}; nothing to do"
  exit 0
fi

log "deleting kind cluster ${CLUSTER_NAME}"
kind delete cluster --name "${CLUSTER_NAME}"

log "done. No other cluster was touched."
