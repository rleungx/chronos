#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

ETCD_ENDPOINTS="${CHRONOS_CLUSTER_FAULT_ETCD_ENDPOINTS:-127.0.0.1:2379,127.0.0.1:22379,127.0.0.1:32379}"
SERVICE_ENDPOINT="${CHRONOS_CLUSTER_FAULT_SERVICE_ENDPOINT:-127.0.0.1:50051}"
ADVERTISE_ENDPOINT="${CHRONOS_CLUSTER_FAULT_ADVERTISE_ENDPOINT:-}"
METRICS_ENDPOINT="${CHRONOS_CLUSTER_FAULT_METRICS_ENDPOINT:-127.0.0.1:9898}"
BENCH_DURATION_SECS="${CHRONOS_CLUSTER_FAULT_DURATION_SECS:-8}"
UNIQUE_SUFFIX="$(date +%s)-$$"
ETCD_PREFIX="${CHRONOS_CLUSTER_FAULT_ETCD_PREFIX:-/chronos-cluster-fault-${UNIQUE_SUFFIX}}"
TIMELINE_NAMESPACE="${CHRONOS_CLUSTER_FAULT_NAMESPACE:-cluster-fault-${UNIQUE_SUFFIX}}"
WORKER_ID="${CHRONOS_CLUSTER_FAULT_WORKER_ID:-worker-cluster-fault}"
ARTIFACT_ROOT="${CHRONOS_CLUSTER_FAULT_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-}}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RELEASE_BIN_DIR="${CHRONOS_RELEASE_BIN_DIR:-${REPO_ROOT}/target/release}"
ADVERTISE_ENDPOINT="$(derive_local_advertise_endpoint "${SERVICE_ENDPOINT}" "chronos-cluster-fault" "${ADVERTISE_ENDPOINT}")"

if [[ -n "${ARTIFACT_ROOT}" ]]; then
  ARTIFACT_DIR="${ARTIFACT_ROOT%/}/cluster-leader-loss"
  mkdir -p "${ARTIFACT_DIR}"
  CHRONOS_LOG="${ARTIFACT_DIR}/chronos.log"
  FAULT_BENCH_LOG="${ARTIFACT_DIR}/fault-span-bench.log"
  POST_BENCH_LOG="${ARTIFACT_DIR}/post-fault-bench.log"
  ETCD_BEFORE_LOG="${ARTIFACT_DIR}/etcd-before.json"
  ETCD_AFTER_LOG="${ARTIFACT_DIR}/etcd-after.json"
  SUMMARY_LOG="${ARTIFACT_DIR}/summary.txt"
  INDEX_LOG="${ARTIFACT_DIR}/artifact-index.txt"
else
  ARTIFACT_DIR=""
  CHRONOS_LOG="$(mktemp -t chronos-cluster-fault.XXXXXX.log)"
  FAULT_BENCH_LOG="$(mktemp -t chronos-cluster-fault-span.XXXXXX.log)"
  POST_BENCH_LOG="$(mktemp -t chronos-cluster-fault-post.XXXXXX.log)"
  ETCD_BEFORE_LOG="$(mktemp -t chronos-cluster-fault-etcd-before.XXXXXX.json)"
  ETCD_AFTER_LOG="$(mktemp -t chronos-cluster-fault-etcd-after.XXXXXX.json)"
  SUMMARY_LOG="$(mktemp -t chronos-cluster-fault-summary.XXXXXX.txt)"
  INDEX_LOG=""
fi

RESULT="failure"
CHRONOS_PID=""
BENCH_PID=""
FAULTED_CONTAINER=""
OLD_LEADER_ID=""
NEW_LEADER_ID=""
OLD_RAFT_TERM=""
NEW_RAFT_TERM=""
OLD_LEADER_ENDPOINT=""
NEW_LEADER_ENDPOINT=""
BENCH_STARTED_AT_MS=""
FAULT_INJECTED_AT_MS=""
LEADER_CHANGED_AT_MS=""
BENCH_FINISHED_AT_MS=""
ALLOCATIONS_AT_LEADER_CHANGE=""
ALLOCATIONS_AFTER_BENCH=""
BENCH_ACTIVE_BEFORE_FAULT="false"
BENCH_ALIVE_AFTER_LEADER_CHANGE="false"
OLD_LEADER_STOPPED="false"

now_ms() {
  python3 -c 'import time; print(time.time_ns() // 1_000_000)'
}

process_is_running() {
  local state
  state="$(ps -o state= -p "$1" 2>/dev/null | tr -d '[:space:]')" || return 1
  [[ -n "${state}" && "${state}" != "Z" ]]
}

leader_fields() {
  local status_file=$1
  python3 - "${status_file}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    statuses = json.load(handle)
if not statuses:
    raise SystemExit("empty etcd endpoint status")
leader_id = statuses[0]["Status"]["leader"]
for item in statuses:
    status = item["Status"]
    if status["header"]["member_id"] == leader_id:
        print(f"{leader_id}\t{item['Endpoint']}\t{status['raftTerm']}")
        break
else:
    raise SystemExit(f"leader {leader_id} is not present in endpoint status")
PY
}

endpoint_container() {
  local endpoint=$1
  endpoint="${endpoint#http://}"
  printf '%s\n' "${endpoint%%:*}"
}

metric_value() {
  curl --max-time 2 -fsS "http://${METRICS_ENDPOINT}/metrics" |
    awk '$1 == "tso_allocate_total" { value = $2 } END { print value + 0 }'
}

metric_or_zero() {
  local key=$1
  local file=$2
  local value=""
  if [[ -f "${file}" ]]; then
    value="$(extract_metric "${key}" "${file}")"
  fi
  printf '%s\n' "${value:-0}"
}

run_allocation_bench() {
  local duration_secs=$1
  local output_log=$2
  env \
    CHRONOS_CONTROL_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" \
    CHRONOS_CONTROL_BENCH_NAMESPACE="${TIMELINE_NAMESPACE}" \
    CHRONOS_CONTROL_BENCH_SCENARIO=allocate_only \
    CHRONOS_CONTROL_BENCH_CONCURRENCY=1 \
    CHRONOS_CONTROL_BENCH_TIMELINES=1 \
    CHRONOS_CONTROL_BENCH_DURATION_SECS="${duration_secs}" \
    CHRONOS_CONTROL_BENCH_WARMUP_SECS=0 \
    CHRONOS_CONTROL_BENCH_ALLOCATE_BATCH=1 \
    CHRONOS_CONTROL_BENCH_ALLOCATE_REQUEST_TIMEOUT_MS=500 \
    CHRONOS_CONTROL_BENCH_IDEMPOTENCY=false \
    "${RELEASE_BIN_DIR}/chronos-control-bench" >"${output_log}" 2>&1
}

write_summary() {
  cat >"${SUMMARY_LOG}" <<EOF
result=${RESULT}
started_at=${STARTED_AT}
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
etcd_endpoints=${ETCD_ENDPOINTS}
service_endpoint=${SERVICE_ENDPOINT}
metrics_endpoint=${METRICS_ENDPOINT}
etcd_prefix=${ETCD_PREFIX}
timeline_namespace=${TIMELINE_NAMESPACE}
worker_id=${WORKER_ID}
old_leader_id=${OLD_LEADER_ID}
new_leader_id=${NEW_LEADER_ID}
old_leader_endpoint=${OLD_LEADER_ENDPOINT}
new_leader_endpoint=${NEW_LEADER_ENDPOINT}
old_raft_term=${OLD_RAFT_TERM}
new_raft_term=${NEW_RAFT_TERM}
old_leader_stopped=${OLD_LEADER_STOPPED}
bench_active_before_fault=${BENCH_ACTIVE_BEFORE_FAULT}
bench_alive_after_leader_change=${BENCH_ALIVE_AFTER_LEADER_CHANGE}
bench_started_at_ms=${BENCH_STARTED_AT_MS}
fault_injected_at_ms=${FAULT_INJECTED_AT_MS}
leader_changed_at_ms=${LEADER_CHANGED_AT_MS}
bench_finished_at_ms=${BENCH_FINISHED_AT_MS}
allocations_at_leader_change=${ALLOCATIONS_AT_LEADER_CHANGE}
allocations_after_bench=${ALLOCATIONS_AFTER_BENCH}
fault_allocate_requests_total=$(metric_or_zero "allocate_requests_total" "${FAULT_BENCH_LOG}")
fault_allocate_success_total=$(metric_or_zero "allocate_success_total" "${FAULT_BENCH_LOG}")
fault_allocate_failed_total=$(metric_or_zero "allocate_failed_total" "${FAULT_BENCH_LOG}")
fault_monotonicity_violations_total=$(metric_or_zero "monotonicity_violations_total" "${FAULT_BENCH_LOG}")
fault_first_tso=$(metric_or_zero "allocation_first_tso" "${FAULT_BENCH_LOG}")
fault_last_tso=$(metric_or_zero "allocation_last_tso" "${FAULT_BENCH_LOG}")
post_allocate_requests_total=$(metric_or_zero "allocate_requests_total" "${POST_BENCH_LOG}")
post_allocate_success_total=$(metric_or_zero "allocate_success_total" "${POST_BENCH_LOG}")
post_monotonicity_violations_total=$(metric_or_zero "monotonicity_violations_total" "${POST_BENCH_LOG}")
post_first_tso=$(metric_or_zero "allocation_first_tso" "${POST_BENCH_LOG}")
post_last_tso=$(metric_or_zero "allocation_last_tso" "${POST_BENCH_LOG}")
chronos_log=${CHRONOS_LOG}
fault_bench_log=${FAULT_BENCH_LOG}
post_bench_log=${POST_BENCH_LOG}
etcd_before_log=${ETCD_BEFORE_LOG}
etcd_after_log=${ETCD_AFTER_LOG}
EOF
}

cleanup() {
  local exit_code=$?
  if [[ -n "${BENCH_PID}" ]] && kill -0 "${BENCH_PID}" 2>/dev/null; then
    kill "${BENCH_PID}" 2>/dev/null || true
    wait "${BENCH_PID}" 2>/dev/null || true
  fi
  if [[ -n "${CHRONOS_PID}" ]] && kill -0 "${CHRONOS_PID}" 2>/dev/null; then
    kill "${CHRONOS_PID}" 2>/dev/null || true
    wait "${CHRONOS_PID}" 2>/dev/null || true
  fi
  RESULT=$([[ ${exit_code} -eq 0 ]] && echo success || echo failure)
  write_summary
  write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
  make etcd-cluster-reset >/dev/null 2>&1 || true
}
trap cleanup EXIT

make etcd-cluster-reset >/dev/null
make etcd-cluster-up >/dev/null
wait_for_etcd_cluster 60 1 "http://chronos-etcd-1:2379,http://chronos-etcd-2:2379,http://chronos-etcd-3:2379"
ensure_release_binaries "${RELEASE_BIN_DIR}" chronos chronos-control-bench

env \
  CHRONOS_SECURITY_MODE=dev-insecure \
  CHRONOS_METADATA=etcd \
  CHRONOS_BIND_ADDR="${SERVICE_ENDPOINT}" \
  CHRONOS_ADVERTISE_ENDPOINT="${ADVERTISE_ENDPOINT}" \
  CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINT}" \
  CHRONOS_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
  CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" \
  CHRONOS_WORKER_ID="${WORKER_ID}" \
  CHRONOS_LEASE_TTL_MS=1500 \
  CHRONOS_GENERATOR_LEASE_TTL_MS=1500 \
  CHRONOS_GENERATOR_MAINTENANCE_INTERVAL_MS=100 \
  CHRONOS_SAFETY_GAP_MS=500 \
  CHRONOS_MAX_CLOCK_SKEW_MS=500 \
  "${RELEASE_BIN_DIR}/chronos" >"${CHRONOS_LOG}" 2>&1 &
CHRONOS_PID=$!

wait_for_http "http://${METRICS_ENDPOINT}/readyz" "chronos readyz" 60 1

docker exec chronos-etcd-1 /usr/local/bin/etcdctl \
  --endpoints=http://chronos-etcd-1:2379,http://chronos-etcd-2:2379,http://chronos-etcd-3:2379 \
  endpoint status -w json >"${ETCD_BEFORE_LOG}"
IFS=$'\t' read -r OLD_LEADER_ID OLD_LEADER_ENDPOINT OLD_RAFT_TERM < <(
  leader_fields "${ETCD_BEFORE_LOG}"
)
FAULTED_CONTAINER="$(endpoint_container "${OLD_LEADER_ENDPOINT}")"

ALL_CONTAINERS=(chronos-etcd-1 chronos-etcd-2 chronos-etcd-3)
FOLLOWER_CONTAINERS=()
FOLLOWER_ENDPOINTS=()
for container in "${ALL_CONTAINERS[@]}"; do
  if [[ "${container}" != "${FAULTED_CONTAINER}" ]]; then
    FOLLOWER_CONTAINERS+=("${container}")
    FOLLOWER_ENDPOINTS+=("http://${container}:2379")
  fi
done
FOLLOWER_ENDPOINT_CSV="$(join_by_comma "${FOLLOWER_ENDPOINTS[@]}")"

BASELINE_ALLOCATIONS="$(metric_value)"
BENCH_STARTED_AT_MS="$(now_ms)"
run_allocation_bench "${BENCH_DURATION_SECS}" "${FAULT_BENCH_LOG}" &
BENCH_PID=$!

for _attempt in $(seq 1 80); do
  CURRENT_ALLOCATIONS="$(metric_value)"
  if python3 -c 'import sys; raise SystemExit(0 if float(sys.argv[1]) > float(sys.argv[2]) else 1)' \
    "${CURRENT_ALLOCATIONS}" "${BASELINE_ALLOCATIONS}"; then
    BENCH_ACTIVE_BEFORE_FAULT="true"
    break
  fi
  process_is_running "${BENCH_PID}" || {
    echo "allocation bench exited before fault injection" >&2
    exit 1
  }
  sleep 0.1
done
[[ "${BENCH_ACTIVE_BEFORE_FAULT}" == "true" ]] || {
  echo "allocation bench did not become active before fault injection" >&2
  exit 1
}

FAULT_INJECTED_AT_MS="$(now_ms)"
docker stop --time 1 "${FAULTED_CONTAINER}" >/dev/null
if [[ "$(docker inspect -f '{{.State.Running}}' "${FAULTED_CONTAINER}")" == "false" ]]; then
  OLD_LEADER_STOPPED="true"
fi

for _attempt in $(seq 1 80); do
  if docker exec "${FOLLOWER_CONTAINERS[0]}" /usr/local/bin/etcdctl \
    --endpoints="${FOLLOWER_ENDPOINT_CSV}" endpoint status -w json >"${ETCD_AFTER_LOG}.tmp" 2>/dev/null; then
    if IFS=$'\t' read -r CANDIDATE_LEADER_ID CANDIDATE_LEADER_ENDPOINT CANDIDATE_RAFT_TERM < <(
      leader_fields "${ETCD_AFTER_LOG}.tmp" 2>/dev/null
    ); then
      if [[ "${CANDIDATE_LEADER_ID}" != "${OLD_LEADER_ID}" ]]; then
        NEW_LEADER_ID="${CANDIDATE_LEADER_ID}"
        NEW_LEADER_ENDPOINT="${CANDIDATE_LEADER_ENDPOINT}"
        NEW_RAFT_TERM="${CANDIDATE_RAFT_TERM}"
        LEADER_CHANGED_AT_MS="$(now_ms)"
        mv "${ETCD_AFTER_LOG}.tmp" "${ETCD_AFTER_LOG}"
        break
      fi
    fi
  fi
  sleep 0.25
done
[[ -n "${NEW_LEADER_ID}" ]] || {
  echo "cluster did not elect a different etcd leader" >&2
  exit 1
}
if process_is_running "${BENCH_PID}"; then
  BENCH_ALIVE_AFTER_LEADER_CHANGE="true"
  ALLOCATIONS_AT_LEADER_CHANGE="$(metric_value)"
else
  echo "allocation bench did not span the etcd leader election" >&2
  exit 1
fi

wait "${BENCH_PID}"
BENCH_PID=""
BENCH_FINISHED_AT_MS="$(now_ms)"
ALLOCATIONS_AFTER_BENCH="$(metric_value)"
run_allocation_bench 1 "${POST_BENCH_LOG}"

RESULT="success"
write_summary
bash "${REPO_ROOT}/hack/verify-cluster-leader-loss.sh" "${SUMMARY_LOG}"
write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
echo "[cluster-leader-loss] success"
