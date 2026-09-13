#!/usr/bin/env bash
#
# Bring up the demonstration environment.
#
# Creates an isolated kind cluster named kubescalesense-demo, builds and loads
# the three images, installs metrics-server, and applies the pipeline with the
# controller in dry-run.
#
# SAFETY. This script touches exactly one Kubernetes cluster: the kind cluster
# it creates. Every kubectl call is pinned to --context kind-kubescalesense-demo,
# and the script aborts if that context does not exist. It never reads the
# ambient current-context, so it cannot act on a cluster you happen to be
# pointed at — which for anyone with a real cluster in their kubeconfig is the
# difference between a demo and an incident.
set -euo pipefail

CLUSTER_NAME="kubescalesense-demo"
CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="data-pipeline"
CONTROLLER_NAMESPACE="kubescalesense"

# Pinned so the demo is reproducible. Bumping it is a deliberate edit with a
# re-run of DI-08 behind it, not something that happens overnight.
METRICS_SERVER_VERSION="v0.9.0"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

# The value NiFi's concurrency must exceed (A-13, FS-28). Read from the config
# this demo actually applies rather than hardcoded, so the assertion cannot
# drift away from the setting it is protecting. tests/demo/nififlow keeps this
# file and the shipped default in agreement.
MAX_REPLICAS="$(awk '/^[[:space:]]*maxReplicas:/ { print $2; exit }' deploy/demo/kubescalesense-configmap.yaml)"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m warning:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m error:\033[0m %s\n' "$*" >&2; exit 1; }

require_tool() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required but not installed. $2"
}

log "checking prerequisites"
require_tool docker "Install Docker or Podman; kind needs a container runtime."
require_tool kubectl "Install kubectl >= 1.29."
require_tool kind    "Install kind >= 0.32: go install sigs.k8s.io/kind@v0.32.0, or see https://kind.sigs.k8s.io/docs/user/quick-start/#installation"

# kind needs roughly 6 GiB of memory and 4 CPUs to run four nodes alongside
# NiFi's JVM and several Normalizer replicas. Checked and reported rather than
# enforced: the demo may well run in less, but a pipeline that OOM-kills looks
# like a broken pipeline and this is where that confusion is cheapest to
# prevent.
if command -v free >/dev/null 2>&1; then
  total_mib="$(free -m | awk '/^Mem:/ { print $2 }')"
  if [[ "${total_mib}" -lt 6000 ]]; then
    warn "this host reports ${total_mib} MiB of memory; the four-node demo with NiFi expects ~6 GiB."
    warn "expect OOM kills. Reduce NiFi's heap in deploy/demo/nifi.yaml, or run the pipeline"
    warn "without NiFi using: make demo-spike MODE=http"
  fi
fi
if command -v nproc >/dev/null 2>&1; then
  cpus="$(nproc)"
  if [[ "${cpus}" -lt 4 ]]; then
    warn "this host reports ${cpus} CPUs; the demo pods request more than that in total."
    warn "Pods will be Pending for reasons unrelated to the behaviour being demonstrated."
  fi
fi

if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  log "cluster ${CLUSTER_NAME} already exists, reusing it"
else
  log "creating cluster ${CLUSTER_NAME}"
  kind create cluster --config tests/e2e/kind-cluster.yaml --wait 120s
fi

# From here on, every command is pinned to this context.
kubectl config get-contexts -o name | grep -qx "${CONTEXT}" \
  || die "context ${CONTEXT} does not exist after cluster creation; refusing to continue against an unknown cluster"
kubectl() { command kubectl --context "${CONTEXT}" "$@"; }

log "building images"
docker build -t kubescalesense:dev -f Dockerfile .
docker build -t normalizer:dev -f Dockerfile.normalizer .
docker build -t loadgen:dev -f Dockerfile.loadgen .

log "loading images into the cluster"
kind load docker-image --name "${CLUSTER_NAME}" kubescalesense:dev normalizer:dev loadgen:dev

log "installing metrics-server ${METRICS_SERVER_VERSION}"
# --kubelet-insecure-tls is required on kind: the kubelets serve certificates
# that metrics-server cannot verify. This is a demo cluster, it is not reachable
# from outside the host, and utilization-derived demand is one of the two inputs
# the project is about (A-04).
#
# Pinned rather than tracking `releases/latest`. A demo that installs whatever
# was released this morning is a demo whose failures cannot be distinguished
# from its subject's: the utilization signal feeding the decision comes from
# here, so an upstream change to it would look like a KubeScaleSense regression.
kubectl apply -f "https://github.com/kubernetes-sigs/metrics-server/releases/download/${METRICS_SERVER_VERSION}/components.yaml"
kubectl patch deployment metrics-server -n kube-system --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
kubectl rollout status deployment/metrics-server -n kube-system --timeout=180s

log "applying the pipeline"
kubectl apply -f deploy/demo/namespace.yaml
kubectl apply -f deploy/normalizer/configmap.yaml
kubectl apply -f deploy/normalizer/deployment.yaml
kubectl apply -f deploy/normalizer/service.yaml
kubectl apply -f deploy/demo/ballast.yaml
kubectl apply -f deploy/demo/nifi.yaml

log "applying the controller, in dry-run"
kubectl apply -f deploy/kubescalesense/namespace.yaml
kubectl apply -f deploy/kubescalesense/serviceaccount.yaml
kubectl apply -f deploy/kubescalesense/clusterrole.yaml
kubectl apply -f deploy/kubescalesense/clusterrolebinding.yaml
kubectl apply -f deploy/kubescalesense/role.yaml
kubectl apply -f deploy/kubescalesense/rolebinding.yaml
kubectl apply -f deploy/demo/kubescalesense-configmap.yaml
kubectl apply -f deploy/kubescalesense/deployment.yaml

log "waiting for the workloads"
kubectl rollout status deployment/normalizer -n "${NAMESPACE}" --timeout=180s
kubectl rollout status deployment/kubescalesense -n "${CONTROLLER_NAMESPACE}" --timeout=180s
kubectl rollout status statefulset/nifi -n "${NAMESPACE}" --timeout=600s || {
  warn "NiFi did not become ready in time. It is the heaviest component here; check"
  warn "  kubectl --context ${CONTEXT} -n ${NAMESPACE} logs nifi-0 -c nifi"
  warn "The direct-HTTP demonstration does not need NiFi: make demo-spike MODE=http"
}

# The environment always comes up in dry-run, checked rather than asserted in
# prose.
#
# P3 gave the controller a write path, which makes this check more useful than
# it was rather than obsolete: the documented order is to validate the decisions
# against the real cluster first and enable actuation second, so an environment
# that came up live would skip the step that makes the live run meaningful.
# Switching modes is a separate, deliberate command — see demo-actuate.sh.
dry_run="$(kubectl -n "${CONTROLLER_NAMESPACE}" get configmap kubescalesense-config \
  -o jsonpath='{.data.kubescalesense\.yaml}' | awk '/dryRun:/ { print $2; exit }')"
[[ "${dry_run}" == "true" ]] \
  || die "the shipped demo config has dryRun: ${dry_run:-unset}; the environment must come up in dry-run, and actuation is enabled afterwards with tests/e2e/demo-actuate.sh on"

log "verifying the NiFi flow's load-bearing settings"
tests/e2e/assert-nifi-flow.sh "${MAX_REPLICAS}" || {
  warn "the NiFi flow is not configured correctly yet."
  warn "Import deploy/demo/nifi/flow/normalizer-pipeline.json — see deploy/demo/README.md — then re-run:"
  warn "  tests/e2e/assert-nifi-flow.sh ${MAX_REPLICAS}"
}

cat <<EOF

$(log "the demonstration environment is up")

  Replicas start at 1 and KubeScaleSense will NOT change them yet: the
  environment comes up in dry-run, so everything below observes.

  Watch the decision the controller would make:
    kubectl --context ${CONTEXT} -n ${CONTROLLER_NAMESPACE} logs -f deployment/kubescalesense

  Drive a spike through the full pipeline (files -> NiFi -> HTTP -> Normalizer):
    make demo-spike

  Drive one straight at the Normalizer, with NiFi out of the path:
    make demo-spike MODE=http

  Once the dry-run decisions look right, let the controller act on them:
    make demo-actuate            # and: make demo-dry-run, to go back

  Compare the fit calculation against the cluster by hand:
    kubectl --context ${CONTEXT} describe node

  Tear it all down (deletes only the ${CLUSTER_NAME} cluster):
    make demo-down
EOF
