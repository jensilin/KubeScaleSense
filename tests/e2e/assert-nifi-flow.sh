#!/usr/bin/env bash
#
# Assert the NiFi flow's load-bearing settings against the running instance.
#
# The same three properties tests/demo/nififlow checks in the shipped JSON, but
# checked here against what NiFi actually loaded. Both are needed: the file can
# be correct and not imported, and the running flow can be edited in the UI.
# All three failures are silent — the pipeline keeps working and the numbers
# stop meaning what they appear to — which is why they are asserted rather than
# documented.
#
#   usage: assert-nifi-flow.sh <maxReplicas>
set -euo pipefail

MAX_REPLICAS="${1:?usage: assert-nifi-flow.sh <maxReplicas>}"

CLUSTER_NAME="kubescalesense-demo"
CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="data-pipeline"
LOCAL_PORT="${LOCAL_PORT:-18080}"

ok()   { printf '  \033[1;32mok\033[0m      %s\n' "$*"; }
bad()  { printf '  \033[1;31mFAILED\033[0m  %s\n' "$*" >&2; }
skip() { printf '  \033[1;33mskip\033[0m    %s\n' "$*"; }
die()  { printf '\033[1;31m error:\033[0m %s\n' "$*" >&2; exit 1; }

command -v jq >/dev/null 2>&1 || die "jq is required to read NiFi's REST responses."
command -v curl >/dev/null 2>&1 || die "curl is required."

kubectl config get-contexts -o name 2>/dev/null | grep -qx "${CONTEXT}" \
  || die "context ${CONTEXT} does not exist. Run: make demo-up"

kubectl() { command kubectl --context "${CONTEXT}" "$@"; }

# A port-forward rather than a Service of type NodePort: the NiFi UI and API are
# deliberately not reachable from outside the cluster, because the demo runs
# NiFi with anonymous access and no credentials.
kubectl -n "${NAMESPACE}" port-forward statefulset/nifi "${LOCAL_PORT}:8080" >/dev/null 2>&1 &
forward_pid=$!
trap 'kill "${forward_pid}" 2>/dev/null || true' EXIT

api="http://127.0.0.1:${LOCAL_PORT}/nifi-api"
for _ in $(seq 1 30); do
  if curl -fsS "${api}/system-diagnostics" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "${api}/system-diagnostics" >/dev/null 2>&1 \
  || die "NiFi's API did not answer on ${api}. Is nifi-0 running?"

# Find the InvokeHTTP processor wherever in the flow it lives.
processors="$(curl -fsS "${api}/flow/search-results?q=InvokeHTTP" | jq -r '.searchResultsDTO.processorResults[]?.id')"
[[ -n "${processors}" ]] || {
  bad "no InvokeHTTP processor exists in the flow."
  echo "        The flow has not been imported. See deploy/demo/README.md." >&2
  exit 1
}

failures=0
echo "checking the NiFi flow against maxReplicas=${MAX_REPLICAS}"

for id in ${processors}; do
  processor="$(curl -fsS "${api}/processors/${id}")"
  name="$(jq -r '.component.name' <<<"${processor}")"

  # A-13 / FS-28: concurrency above maxReplicas, or scaling adds replicas that
  # nothing dispatches to and the demo shows scaling with no effect.
  tasks="$(jq -r '.component.config.concurrentlySchedulableTaskCount' <<<"${processor}")"
  if [[ "${tasks}" -gt "${MAX_REPLICAS}" ]]; then
    ok "${name}: ${tasks} concurrent tasks > maxReplicas ${MAX_REPLICAS}"
  else
    bad "${name}: ${tasks} concurrent tasks is not above maxReplicas ${MAX_REPLICAS} (A-13, FS-28)"
    echo "        At this setting the pool's last replicas idle and scaling changes nothing." >&2
    failures=$((failures + 1))
  fi

  # D-02: retry on the transient-failure relationships is the pipeline's whole
  # durability guarantee.
  retried="$(jq -r '[.component.config.retriedRelationships[]?] | join(",")' <<<"${processor}")"
  for relationship in Retry Failure; do
    if [[ ",${retried}," == *",${relationship},"* ]]; then
      ok "${name}: ${relationship} is retried"
    else
      bad "${name}: ${relationship} is not retried (retried: ${retried:-none}) (D-02)"
      failures=$((failures + 1))
    fi
  done

  # The poison-pill guard: a 400 must not be retried forever.
  if [[ ",${retried}," == *",No Retry,"* ]]; then
    bad "${name}: \"No Retry\" is configured as retried; a malformed record would be retried forever"
    failures=$((failures + 1))
  else
    ok "${name}: malformed records are not retried"
  fi

  # FS-29: the read timeout must exceed the Normalizer's worst case, or slow
  # records are retried while still in flight.
  read_timeout="$(jq -r '.component.config.properties["Socket Read Timeout"] // empty' <<<"${processor}")"
  if [[ -z "${read_timeout}" ]]; then
    skip "${name}: no explicit read timeout found in the processor's properties"
  else
    seconds="$(awk '{ print $1 }' <<<"${read_timeout}")"
    if [[ "${seconds%%.*}" -gt 15 ]]; then
      ok "${name}: read timeout ${read_timeout} exceeds the Normalizer's worst case"
    else
      bad "${name}: read timeout ${read_timeout} is not comfortably above the Normalizer's queue timeout (FS-29)"
      failures=$((failures + 1))
    fi
  fi
done

if [[ "${failures}" -gt 0 ]]; then
  die "${failures} flow assertion(s) failed. The demonstration would be misleading; fix the flow first."
fi

printf '\033[1;32mthe flow is configured correctly\033[0m\n'
