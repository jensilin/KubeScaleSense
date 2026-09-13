#!/usr/bin/env bash
#
# Measure workload.itemsPerReplica against the Normalizer's latency SLO.
#
#   itemsPerReplica = the workload pressure one replica can carry while
#                     request latency still meets p95 <= 500 ms
#
# The method is the one docs/scaling-algorithm.md § 3.1 prescribes: hold the
# replica count fixed, raise the load in steps, find the pressure level at
# which per-request latency reaches the SLO, and divide by the replica count.
#
# Why this is measured rather than reasoned about: itemsPerReplica is the
# divisor converting pressure into replicas, so it sets the gain of the whole
# control loop. Too low and the controller over-provisions on every spike; too
# high and it under-provisions and the backlog grows anyway. A guess here is a
# guess about the number that matters most.
#
# THE STEP VARIABLE IS IN-FLIGHT COUNT, NOT OFFERED RATE, and that is the whole
# design of this script. Stepping the rate does not work, and the first attempt
# at this measurement proved it: below the pod's service rate the queue stays
# empty and pressure reads ~1, and one step above it the queue is unstable and
# runs straight to its limit, so pressure jumps from ~1 to 64 with nothing in
# between. Neither number is the answer to "how much pressure can one replica
# carry", because no offered rate produces a steady intermediate queue depth.
#
# A closed-loop client fixes this. Holding N requests in flight pins pressure
# at N by construction — the client cannot send the N+1th until one comes back
# — so latency can be measured as a function of pressure, which is the function
# the controller actually needs. The crossing point is where p95 reaches the
# SLO.
#
# TWO MEASUREMENT HAZARDS, both handled below rather than hoped away:
#
#   Rejections are fast. A saturated Normalizer answers 503 immediately, so
#   counting every response in the latency figure would make an overloaded pod
#   look faster than a busy one. Latency is therefore measured over the
#   `normalized` outcome only, and the rejection rate is reported alongside it:
#   a step that sheds load has not met the SLO, it has stopped trying.
#
#   Deltas, not totals. The histogram is cumulative over the pod's lifetime, so
#   each step is measured as the difference between a snapshot before and after
#   it. Without that, a fast early step would keep flattering every later one.
#
# SAFETY. Touches exactly one cluster: the kind cluster kubescalesense-demo.
# Every kubectl call is pinned to its context and the script aborts if that
# context does not exist.
set -euo pipefail

CLUSTER_NAME="kubescalesense-demo"
CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="data-pipeline"
CONTROLLER_NAMESPACE="kubescalesense"
DEPLOYMENT="normalizer"

# The agreed SLO. 0.5 is deliberately one of the histogram's bucket boundaries,
# which means the proportion of requests within it is read directly from the
# exposition rather than interpolated out of it — so the SLO is evaluated
# exactly, and the measurement does not depend on a quantile estimator.
SLO_SECONDS="${SLO_SECONDS:-0.5}"
SLO_QUANTILE="${SLO_QUANTILE:-0.95}"

# Load shed above this fraction disqualifies a step: the pod is no longer
# serving the offered load, so its latency says nothing about capacity.
MAX_REJECT_FRACTION="${MAX_REJECT_FRACTION:-0.01}"

REPLICAS="${REPLICAS:-1}"

# The pressure levels to hold, in requests in flight. The Normalizer serves
# NORMALIZER_MAX_CONCURRENT at once and queues up to NORMALIZER_QUEUE_LIMIT, so
# levels above their sum can only shed and are there to show the ceiling.
LEVELS="${LEVELS:-2 4 8 12 16 24 32 48 64}"

# Records per level. The client is limited by LEVELS, not by this rate, so the
# rate only has to be high enough not to be the constraint; the step ends when
# the records run out, which takes longer at low pressure than at high.
OFFER_RATE="${OFFER_RATE:-2000}"
RECORDS_PER_LEVEL="${RECORDS_PER_LEVEL:-2000}"
SETTLE="${SETTLE:-8}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m warning:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m error:\033[0m %s\n' "$*" >&2; exit 1; }

kubectl config get-contexts -o name 2>/dev/null | grep -qx "${CONTEXT}" \
  || die "context ${CONTEXT} does not exist. Run: make demo-up"
kubectl() { command kubectl --context "${CONTEXT}" "$@"; }

# The controller must not move the replica count while we are measuring what
# one replica can do. Checked rather than assumed, because a live controller
# would silently invalidate every row of the table.
dry_run="$(kubectl -n "${CONTROLLER_NAMESPACE}" get configmap kubescalesense-config \
  -o jsonpath='{.data.kubescalesense\.yaml}' 2>/dev/null | awk '/dryRun:/ { print $2; exit }')"
if [[ "${dry_run}" != "true" ]]; then
  die "the controller has dryRun: ${dry_run:-unset}. The replica count must be held fixed for this measurement, so run: make demo-dry-run"
fi

pods() {
  kubectl -n "${NAMESPACE}" get pods -l "app.kubernetes.io/name=${DEPLOYMENT}" \
    --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'
}

# Scraped through the API server's pod proxy rather than by port-forwarding.
# The Normalizer image is distroless and has no shell to curl from, and the
# proxy reaches each pod individually — which port-forwarding to a Deployment
# does not, because it picks one pod and hides the rest.
scrape() {
  kubectl get --raw "/api/v1/namespaces/${NAMESPACE}/pods/${1}:8081/proxy/metrics" 2>/dev/null || true
}

# Sum, across every Running pod, of the requests that completed within the SLO
# and the requests that completed at all. Restricted to the normalized outcome:
# see the hazard note above.
histogram_snapshot() {
  local within=0 total=0 rejected=0 pod raw
  for pod in $(pods); do
    raw="$(scrape "${pod}")"
    [[ -n "${raw}" ]] || continue

    within=$(( within + $(awk -v slo="${SLO_SECONDS}" '
      /^normalizer_request_duration_seconds_bucket\{/ &&
      /outcome="normalized"/ && $0 ~ ("le=\"" slo "\"") { printf "%d", $2; found=1; exit }
      END { if (!found) print 0 }' <<<"${raw}") ))

    total=$(( total + $(awk '
      /^normalizer_request_duration_seconds_count\{/ && /outcome="normalized"/ { printf "%d", $2; found=1; exit }
      END { if (!found) print 0 }' <<<"${raw}") ))

    rejected=$(( rejected + $(awk '
      /^normalizer_requests_completed_total\{/ && /outcome="overloaded"/ { printf "%d", $2; found=1; exit }
      END { if (!found) print 0 }' <<<"${raw}") ))
  done
  printf '%d %d %d\n' "${within}" "${total}" "${rejected}"
}

# The pressure the controller would see: queued + in-flight, summed over pods.
# Sampled from the same numbers the signal source reads, so the measured
# capacity is expressed in the same units the decision engine divides by.
pressure_now() {
  local sum=0 pod raw
  for pod in $(pods); do
    raw="$(scrape "${pod}")"
    [[ -n "${raw}" ]] || continue
    sum=$(( sum + $(awk '
      /^normalizer_requests_queued / { queued = $2 }
      /^normalizer_requests_in_flight / { inflight = $2 }
      END { printf "%d", queued + inflight }' <<<"${raw}") ))
  done
  printf '%d\n' "${sum}"
}

log "pinning ${DEPLOYMENT} at ${REPLICAS} replica(s)"
kubectl -n "${NAMESPACE}" scale "deployment/${DEPLOYMENT}" --replicas="${REPLICAS}"
kubectl -n "${NAMESPACE}" rollout status "deployment/${DEPLOYMENT}" --timeout=180s

requests="$(kubectl -n "${NAMESPACE}" get "deployment/${DEPLOYMENT}" \
  -o jsonpath='{.spec.template.spec.containers[0].resources}')"
cost="$(kubectl -n "${NAMESPACE}" get configmap normalizer-config \
  -o jsonpath='{.data}' 2>/dev/null || true)"

cat <<EOF

Measurement setup
  replicas            ${REPLICAS}
  resources           ${requests}
  normalizer config   ${cost}
  SLO                 p${SLO_QUANTILE} <= ${SLO_SECONDS}s on the normalized outcome
  max shed allowed    ${MAX_REJECT_FRACTION}
  pressure levels     ${LEVELS} requests in flight
  records per level   ${RECORDS_PER_LEVEL}
  settle between      ${SETTLE}s

EOF

printf '%8s %10s %10s %9s %9s %9s %9s %s\n' \
  "inflight" "served" "within" "frac" "shed" "press_avg" "press_max" "SLO"
printf '%s\n' "-------- ---------- ---------- --------- --------- --------- --------- ----"

best_level=0
best_pressure=0

for level in ${LEVELS}; do
  read -r within0 total0 rejected0 <<<"$(histogram_snapshot)"

  # Pressure is sampled while the load runs, in the background, because the
  # quantity of interest is the sustained queue depth during the step rather
  # than whatever it happens to be when the step ends.
  samples="$(mktemp)"
  ( while true; do pressure_now >>"${samples}"; sleep 1; done ) &
  sampler=$!

  job="measure-$(date +%s)"
  kubectl -n "${NAMESPACE}" run "${job}" \
    --image=loadgen:dev \
    --image-pull-policy=IfNotPresent \
    --restart=Never \
    --attach \
    --rm \
    --quiet \
    -- \
    -mode=http \
    -url="http://normalizer-service.${NAMESPACE}.svc/normalize" \
    -profile=constant \
    -records="${RECORDS_PER_LEVEL}" \
    -rate="${OFFER_RATE}" \
    -burst=1 \
    -concurrency="${level}" \
    >/dev/null 2>&1 || warn "the load generator exited non-zero at ${level} in flight"

  kill "${sampler}" 2>/dev/null || true
  wait "${sampler}" 2>/dev/null || true

  read -r within1 total1 rejected1 <<<"$(histogram_snapshot)"

  served=$(( total1 - total0 ))
  within=$(( within1 - within0 ))
  shed=$(( rejected1 - rejected0 ))

  read -r press_avg press_max <<<"$(awk '
    { sum += $1; if ($1 > max) max = $1; n++ }
    END { if (n) printf "%.1f %d", sum / n, max; else print "0.0 0" }' "${samples}")"
  rm -f "${samples}"

  if (( served == 0 )); then
    printf '%8s %10s %10s %9s %9s %9s %9s %s\n' \
      "${level}" "0" "0" "n/a" "${shed}" "${press_avg}" "${press_max}" "NO DATA"
    continue
  fi

  fraction="$(awk -v w="${within}" -v s="${served}" 'BEGIN { printf "%.4f", w / s }')"
  shed_fraction="$(awk -v r="${shed}" -v s="${served}" 'BEGIN { printf "%.4f", r / (s + r) }')"

  verdict="$(awk -v f="${fraction}" -v q="${SLO_QUANTILE}" -v sf="${shed_fraction}" -v ms="${MAX_REJECT_FRACTION}" \
    'BEGIN { if (f >= q && sf <= ms) print "PASS"; else if (sf > ms) print "SHED"; else print "FAIL" }')"

  printf '%8s %10s %10s %9s %9s %9s %9s %s\n' \
    "${level}" "${served}" "${within}" "${fraction}" "${shed_fraction}" \
    "${press_avg}" "${press_max}" "${verdict}"

  if [[ "${verdict}" == "PASS" ]]; then
    best_level="${level}"
    best_pressure="${press_avg}"
  fi

  sleep "${SETTLE}"
done

echo

if [[ "${best_level}" == "0" ]]; then
  warn "no pressure level met the SLO. Either the lowest level already saturates one"
  warn "replica, or something other than the Normalizer is the bottleneck. Do not"
  warn "derive itemsPerReplica from this run; widen LEVELS downwards and repeat."
  exit 1
fi

items_per_replica="$(awk -v p="${best_pressure}" -v r="${REPLICAS}" \
  'BEGIN { printf "%d", (p / r) + 0.5 }')"

cat <<EOF
$(log "result")

  Highest in-flight level meeting p${SLO_QUANTILE} <= ${SLO_SECONDS}s   ${best_level} requests
  Pressure the controller measured there           ${best_pressure} items
  Replicas                                         ${REPLICAS}

  itemsPerReplica = ${best_pressure} / ${REPLICAS} = ${items_per_replica}

  What this number means: at ${items_per_replica} outstanding items per replica the
  Normalizer still answers 95% of requests within ${SLO_SECONDS}s. Above it, latency
  crosses the SLO, which is the point at which another replica is warranted.

  Limitations of this measurement are recorded in docs/scaling-algorithm.md
  section 3.5; read them before treating the figure as a property of the
  workload rather than of this cluster.
EOF
