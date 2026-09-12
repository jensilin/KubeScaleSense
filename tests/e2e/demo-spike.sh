#!/usr/bin/env bash
#
# Drive a deterministic LOW -> HIGH -> LOW spike at the pipeline.
#
# Two paths, because they answer different questions:
#
#   MODE=file  files -> NiFi -> HTTP -> Normalizer. The full pipeline, and the
#              one the demo narrative tells.
#   MODE=http  records -> Normalizer. NiFi removed from the path, so a flat
#              throughput curve cannot be blamed on the client's concurrency.
#              This is the mode DI-08 measures in (FS-28).
#
# The generator is not part of KubeScaleSense and the controller knows nothing
# about it. It only produces load.
set -euo pipefail

CLUSTER_NAME="kubescalesense-demo"
CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="data-pipeline"

MODE="${MODE:-file}"
RECORDS="${RECORDS:-2000}"
LOW_RATE="${LOW_RATE:-5}"
HIGH_RATE="${HIGH_RATE:-120}"
PHASE="${PHASE:-90s}"
BURST="${BURST:-10}"
PAD="${PAD:-0}"
CONCURRENCY="${CONCURRENCY:-16}"

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m error:\033[0m %s\n' "$*" >&2; exit 1; }

kubectl config get-contexts -o name 2>/dev/null | grep -qx "${CONTEXT}" \
  || die "context ${CONTEXT} does not exist. Run: make demo-up"

kubectl() { command kubectl --context "${CONTEXT}" "$@"; }

case "${MODE}" in
  file)
    log "spiking through NiFi: ${RECORDS} records, ${LOW_RATE} -> ${HIGH_RATE}/s, bursts of ${BURST}"
    # The producer sidecar already has the input directory mounted, so the
    # records are written where NiFi's ListFile is watching. Executed directly
    # rather than shelled, because the image has no shell.
    kubectl -n "${NAMESPACE}" exec statefulset/nifi -c input-producer -- \
      /usr/local/bin/loadgen \
        -mode=file \
        -dir=/data/input \
        -profile=low-high-low \
        -records="${RECORDS}" \
        -rate="${LOW_RATE}" \
        -high-rate="${HIGH_RATE}" \
        -phase="${PHASE}" \
        -burst="${BURST}" \
        -pad="${PAD}"
    ;;

  http)
    log "spiking the Normalizer directly: ${RECORDS} records, ${LOW_RATE} -> ${HIGH_RATE}/s, ${CONCURRENCY} in flight"
    # A Job rather than an exec: the concurrency needs to come from outside the
    # pipeline, and a Job is easy to run several of if one client cannot offer
    # enough load.
    job="loadgen-$(date +%s)"
    kubectl -n "${NAMESPACE}" run "${job}" \
      --image=loadgen:dev \
      --image-pull-policy=IfNotPresent \
      --restart=Never \
      --attach \
      --rm \
      -- \
      -mode=http \
      -url=http://normalizer-service.data-pipeline.svc/normalize \
      -profile=low-high-low \
      -records="${RECORDS}" \
      -rate="${LOW_RATE}" \
      -high-rate="${HIGH_RATE}" \
      -phase="${PHASE}" \
      -burst="${BURST}" \
      -pad="${PAD}" \
      -concurrency="${CONCURRENCY}"
    ;;

  *)
    die "MODE must be \"file\" or \"http\", got ${MODE}"
    ;;
esac

cat <<EOF

$(log "spike finished")

  What to look at, and what it should say:

    The pressure the controller saw, summed over pods:
      kubectl --context ${CONTEXT} -n kubescalesense logs deployment/kubescalesense | tail -40

    The replica count, which must NOT have changed:
      kubectl --context ${CONTEXT} -n ${NAMESPACE} get deployment normalizer

    The Normalizer's own view (port-forward, because the image is distroless
    and has no shell to curl from):
      kubectl --context ${CONTEXT} -n ${NAMESPACE} port-forward deployment/normalizer 8081:8081 &
      curl -s localhost:8081/metrics | grep normalizer_requests

  A SCALE_UP decision with the replica count still at 1 is the P2 result, not a
  bug: the controller reports what it would do, and P3 is what does it.
EOF
