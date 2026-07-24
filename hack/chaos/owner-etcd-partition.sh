#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

DIRECT_ETCD_ENDPOINTS="${CHRONOS_OWNER_PARTITION_DIRECT_ETCD_ENDPOINTS:-127.0.0.1:2379,127.0.0.1:22379,127.0.0.1:32379}"
PROXY_ETCD_ENDPOINTS="${CHRONOS_OWNER_PARTITION_PROXY_ETCD_ENDPOINTS:-127.0.0.1:12379,127.0.0.1:22380,127.0.0.1:32380}"
INTERNAL_ETCD_ENDPOINTS="http://chronos-etcd-1:2379,http://chronos-etcd-2:2379,http://chronos-etcd-3:2379"
SERVICE_ENDPOINT="${CHRONOS_OWNER_PARTITION_SERVICE_ENDPOINT:-127.0.0.1:50051}"
ADVERTISE_ENDPOINT="${CHRONOS_OWNER_PARTITION_ADVERTISE_ENDPOINT:-}"
METRICS_ENDPOINT="${CHRONOS_OWNER_PARTITION_METRICS_ENDPOINT:-127.0.0.1:9898}"
BENCH_DURATION_SECS="${CHRONOS_OWNER_PARTITION_DURATION_SECS:-12}"
WAIT_ATTEMPTS="${CHRONOS_OWNER_PARTITION_WAIT_ATTEMPTS:-120}"
WAIT_INTERVAL_SECS="${CHRONOS_OWNER_PARTITION_WAIT_INTERVAL_SECS:-0.25}"
UNIQUE_SUFFIX="$(date +%s)-$$"
ETCD_PREFIX="${CHRONOS_OWNER_PARTITION_ETCD_PREFIX:-/chronos-owner-partition-${UNIQUE_SUFFIX}}"
TIMELINE_NAMESPACE="${CHRONOS_OWNER_PARTITION_NAMESPACE:-owner-partition-${UNIQUE_SUFFIX}}"
WORKER_ID="${CHRONOS_OWNER_PARTITION_WORKER_ID:-worker-owner-partition}"
FAULT_INSTANCE_ID="${CHRONOS_OWNER_PARTITION_FAULT_INSTANCE_ID:-owner-partition-${UNIQUE_SUFFIX}-fault}"
RECOVERY_INSTANCE_ID="${CHRONOS_OWNER_PARTITION_RECOVERY_INSTANCE_ID:-owner-partition-${UNIQUE_SUFFIX}-recovery}"
ARTIFACT_ROOT="${CHRONOS_OWNER_PARTITION_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-}}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RELEASE_BIN_DIR="${CHRONOS_RELEASE_BIN_DIR:-${REPO_ROOT}/target/release}"
ADVERTISE_ENDPOINT="$(derive_local_advertise_endpoint "${SERVICE_ENDPOINT}" "chronos-owner-partition" "${ADVERTISE_ENDPOINT}")"

csv_to_array DIRECT_ENDPOINT_ARRAY "${DIRECT_ETCD_ENDPOINTS}"
csv_to_array PROXY_ENDPOINT_ARRAY "${PROXY_ETCD_ENDPOINTS}"
[[ "${#DIRECT_ENDPOINT_ARRAY[@]}" -eq 3 && "${#PROXY_ENDPOINT_ARRAY[@]}" -eq 3 ]] || {
  echo "owner partition requires exactly three direct endpoints and three proxy endpoints" >&2
  exit 1
}

if [[ -n "${ARTIFACT_ROOT}" ]]; then
  ARTIFACT_DIR="${ARTIFACT_ROOT%/}/owner-etcd-partition"
  mkdir -p "${ARTIFACT_DIR}"
  FAULT_CHRONOS_LOG="${ARTIFACT_DIR}/fault-chronos.log"
  RECOVERY_CHRONOS_LOG="${ARTIFACT_DIR}/recovery-chronos.log"
  FAULT_BENCH_LOG="${ARTIFACT_DIR}/fault-span-bench.log"
  POST_BENCH_LOG="${ARTIFACT_DIR}/post-recovery-bench.log"
  ETCD_DURING_LOG="${ARTIFACT_DIR}/etcd-during-partition.txt"
  SUMMARY_LOG="${ARTIFACT_DIR}/summary.txt"
  INDEX_LOG="${ARTIFACT_DIR}/artifact-index.txt"
  PROXY_LOG_PREFIX="${ARTIFACT_DIR}/proxy"
else
  ARTIFACT_DIR=""
  FAULT_CHRONOS_LOG="$(mktemp -t chronos-owner-partition-fault.XXXXXX.log)"
  RECOVERY_CHRONOS_LOG="$(mktemp -t chronos-owner-partition-recovery.XXXXXX.log)"
  FAULT_BENCH_LOG="$(mktemp -t chronos-owner-partition-bench.XXXXXX.log)"
  POST_BENCH_LOG="$(mktemp -t chronos-owner-partition-post.XXXXXX.log)"
  ETCD_DURING_LOG="$(mktemp -t chronos-owner-partition-etcd.XXXXXX.txt)"
  SUMMARY_LOG="$(mktemp -t chronos-owner-partition-summary.XXXXXX.txt)"
  INDEX_LOG=""
  PROXY_LOG_PREFIX="$(mktemp -u -t chronos-owner-partition-proxy.XXXXXX)"
fi

RESULT="failure"
CHRONOS_PID=""
BENCH_PID=""
PROXY_PIDS=()
PROXY_LOGS=()
BENCH_ACTIVE_BEFORE_PARTITION="false"
PROXIES_STOPPED="false"
CHRONOS_EXITED_AFTER_PARTITION="false"
BENCH_ALIVE_AFTER_CHRONOS_EXIT="false"
ETCD_CLUSTER_HEALTHY_DURING_PARTITION="false"
IDENTITY_LEASE_LOST_OBSERVED="false"
IDENTITY_SHUTDOWN_OBSERVED="false"
PROXIES_RESUMED="false"
IDENTITY_RELEASED_BEFORE_RESTART="false"
RECOVERY_CHRONOS_READY="false"
RECOVERY_DATA_PLANE_ATTEMPTS=0
PARTITIONED_PROXY_COUNT=0
HEALTHY_ETCD_MEMBERS_DURING_PARTITION=0
CHRONOS_PARTITION_EXIT_STATUS=""
BENCH_STARTED_AT_MS=""
PARTITION_STARTED_AT_MS=""
CHRONOS_EXITED_AT_MS=""
BENCH_FINISHED_AT_MS=""
PARTITION_ENDED_AT_MS=""
RECOVERY_READY_AT_MS=""
POST_FINISHED_AT_MS=""

process_is_running() {
  local state
  state="$(ps -o state= -p "$1" 2>/dev/null | tr -d '[:space:]')" || return 1
  [[ -n "${state}" && "${state}" != "Z" ]]
}

metric_value() {
  curl --max-time 2 -fsS "http://${METRICS_ENDPOINT}/metrics" |
    awk '$1 == "tso_allocate_total" { value = $2 } END { print value + 0 }'
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

wait_for_recovery_allocation() {
  local attempt
  local attempt_log
  attempt_log="$(mktemp -t chronos-owner-partition-post-attempt.XXXXXX.log)"
  for attempt in $(seq 1 20); do
    RECOVERY_DATA_PLANE_ATTEMPTS="${attempt}"
    if run_allocation_bench 1 "${attempt_log}" &&
      awk -F= '
        $1 == "allocate_success_total" { success = $2 + 0 }
        $1 == "allocate_failed_total" { failed = $2 + 0 }
        END { exit !(success > 0 && failed == 0) }
      ' "${attempt_log}"; then
      mv "${attempt_log}" "${POST_BENCH_LOG}"
      return 0
    fi
    sleep 0.25
  done
  mv "${attempt_log}" "${POST_BENCH_LOG}"
  echo "post-partition allocation did not recover after 20 attempts" >&2
  return 1
}

start_chronos() {
  local output_log=$1
  local instance_id=$2
  env \
    CHRONOS_SECURITY_MODE=dev-insecure \
    CHRONOS_METADATA=etcd \
    CHRONOS_BIND_ADDR="${SERVICE_ENDPOINT}" \
    CHRONOS_ADVERTISE_ENDPOINT="${ADVERTISE_ENDPOINT}" \
    CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINT}" \
    CHRONOS_ETCD_ENDPOINTS="${PROXY_ETCD_ENDPOINTS}" \
    CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" \
    CHRONOS_WORKER_ID="${WORKER_ID}" \
    CHRONOS_INSTANCE_ID="${instance_id}" \
    CHRONOS_ETCD_TIMEOUT_MS=500 \
    CHRONOS_LEASE_TTL_MS=3000 \
    CHRONOS_GENERATOR_LEASE_TTL_MS=3000 \
    CHRONOS_GENERATOR_MAINTENANCE_INTERVAL_MS=100 \
    CHRONOS_SAFETY_GAP_MS=500 \
    CHRONOS_MAX_CLOCK_SKEW_MS=500 \
    "${RELEASE_BIN_DIR}/chronos" >"${output_log}" 2>&1 &
  CHRONOS_PID=$!
  wait_for_http "http://${METRICS_ENDPOINT}/readyz" "chronos readyz" \
    "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"
}

start_proxies() {
  local index
  for index in 0 1 2; do
    local proxy="${PROXY_ENDPOINT_ARRAY[${index}]}"
    local direct="${DIRECT_ENDPOINT_ARRAY[${index}]}"
    local proxy_host="${proxy%:*}"
    local proxy_port="${proxy##*:}"
    local direct_host="${direct%:*}"
    local direct_port="${direct##*:}"
    local proxy_log="${PROXY_LOG_PREFIX}-$((index + 1)).log"
    python3 "${REPO_ROOT}/hack/lib/tcp_proxy.py" \
      --listen-host "${proxy_host}" \
      --listen-port "${proxy_port}" \
      --target-host "${direct_host}" \
      --target-port "${direct_port}" >"${proxy_log}" 2>&1 &
    PROXY_PIDS+=("$!")
    PROXY_LOGS+=("${proxy_log}")
  done

  for index in 0 1 2; do
    local pid="${PROXY_PIDS[${index}]}"
    local ready="false"
    for _attempt in $(seq 1 "${WAIT_ATTEMPTS}"); do
      if grep -q '^ready=' "${PROXY_LOGS[${index}]}" 2>/dev/null; then
        ready="true"
        break
      fi
      process_is_running "${pid}" || {
        echo "TCP proxy ${pid} exited during startup" >&2
        return 1
      }
      sleep "${WAIT_INTERVAL_SECS}"
    done
    [[ "${ready}" == "true" ]] || {
      echo "TCP proxy ${pid} did not become ready" >&2
      return 1
    }
  done
}

stop_proxies() {
  local pid
  PARTITIONED_PROXY_COUNT=0
  for pid in "${PROXY_PIDS[@]}"; do
    kill -STOP "${pid}"
  done
  for pid in "${PROXY_PIDS[@]}"; do
    local state
    state="$(ps -o state= -p "${pid}" 2>/dev/null | tr -d '[:space:]')"
    if [[ "${state}" == T* ]]; then
      PARTITIONED_PROXY_COUNT=$((PARTITIONED_PROXY_COUNT + 1))
    fi
  done
  if [[ "${PARTITIONED_PROXY_COUNT}" -eq "${#PROXY_PIDS[@]}" ]]; then
    PROXIES_STOPPED="true"
  fi
}

resume_proxies() {
  local pid
  for pid in "${PROXY_PIDS[@]}"; do
    kill -CONT "${pid}" 2>/dev/null || true
  done
  PROXIES_RESUMED="true"
}

wait_for_identity_release() {
  local key="${ETCD_PREFIX}/identity/instances/${FAULT_INSTANCE_ID}"
  for _attempt in $(seq 1 "${WAIT_ATTEMPTS}"); do
    local output
    if output="$(docker exec chronos-etcd-1 /usr/local/bin/etcdctl \
      --endpoints="${INTERNAL_ETCD_ENDPOINTS}" get "${key}" --keys-only 2>/dev/null)" &&
      [[ -z "${output}" ]]; then
      IDENTITY_RELEASED_BEFORE_RESTART="true"
      return 0
    fi
    sleep "${WAIT_INTERVAL_SECS}"
  done
  echo "identity lease key did not expire before restart: ${key}" >&2
  return 1
}

write_summary() {
  cat >"${SUMMARY_LOG}" <<EOF
result=${RESULT}
started_at=${STARTED_AT}
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
direct_etcd_endpoints=${DIRECT_ETCD_ENDPOINTS}
proxy_etcd_endpoints=${PROXY_ETCD_ENDPOINTS}
service_endpoint=${SERVICE_ENDPOINT}
metrics_endpoint=${METRICS_ENDPOINT}
etcd_prefix=${ETCD_PREFIX}
timeline_namespace=${TIMELINE_NAMESPACE}
worker_id=${WORKER_ID}
fault_instance_id=${FAULT_INSTANCE_ID}
recovery_instance_id=${RECOVERY_INSTANCE_ID}
bench_active_before_partition=${BENCH_ACTIVE_BEFORE_PARTITION}
proxies_stopped=${PROXIES_STOPPED}
chronos_exited_after_partition=${CHRONOS_EXITED_AFTER_PARTITION}
bench_alive_after_chronos_exit=${BENCH_ALIVE_AFTER_CHRONOS_EXIT}
etcd_cluster_healthy_during_partition=${ETCD_CLUSTER_HEALTHY_DURING_PARTITION}
identity_lease_lost_observed=${IDENTITY_LEASE_LOST_OBSERVED}
identity_shutdown_observed=${IDENTITY_SHUTDOWN_OBSERVED}
proxies_resumed=${PROXIES_RESUMED}
identity_released_before_restart=${IDENTITY_RELEASED_BEFORE_RESTART}
recovery_chronos_ready=${RECOVERY_CHRONOS_READY}
recovery_data_plane_attempts=${RECOVERY_DATA_PLANE_ATTEMPTS}
partitioned_proxy_count=${PARTITIONED_PROXY_COUNT}
healthy_etcd_members_during_partition=${HEALTHY_ETCD_MEMBERS_DURING_PARTITION}
bench_started_at_ms=${BENCH_STARTED_AT_MS}
partition_started_at_ms=${PARTITION_STARTED_AT_MS}
chronos_exited_at_ms=${CHRONOS_EXITED_AT_MS}
bench_finished_at_ms=${BENCH_FINISHED_AT_MS}
partition_ended_at_ms=${PARTITION_ENDED_AT_MS}
recovery_ready_at_ms=${RECOVERY_READY_AT_MS}
post_finished_at_ms=${POST_FINISHED_AT_MS}
chronos_partition_exit_status=${CHRONOS_PARTITION_EXIT_STATUS}
fault_allocate_requests_total=$(metric_or_zero "allocate_requests_total" "${FAULT_BENCH_LOG}")
fault_allocate_success_total=$(metric_or_zero "allocate_success_total" "${FAULT_BENCH_LOG}")
fault_allocate_failed_total=$(metric_or_zero "allocate_failed_total" "${FAULT_BENCH_LOG}")
fault_monotonicity_violations_total=$(metric_or_zero "monotonicity_violations_total" "${FAULT_BENCH_LOG}")
fault_first_tso=$(metric_or_zero "allocation_first_tso" "${FAULT_BENCH_LOG}")
fault_last_tso=$(metric_or_zero "allocation_last_tso" "${FAULT_BENCH_LOG}")
post_allocate_requests_total=$(metric_or_zero "allocate_requests_total" "${POST_BENCH_LOG}")
post_allocate_success_total=$(metric_or_zero "allocate_success_total" "${POST_BENCH_LOG}")
post_allocate_failed_total=$(metric_or_zero "allocate_failed_total" "${POST_BENCH_LOG}")
post_monotonicity_violations_total=$(metric_or_zero "monotonicity_violations_total" "${POST_BENCH_LOG}")
post_first_tso=$(metric_or_zero "allocation_first_tso" "${POST_BENCH_LOG}")
post_last_tso=$(metric_or_zero "allocation_last_tso" "${POST_BENCH_LOG}")
fault_chronos_log=${FAULT_CHRONOS_LOG}
recovery_chronos_log=${RECOVERY_CHRONOS_LOG}
fault_bench_log=${FAULT_BENCH_LOG}
post_bench_log=${POST_BENCH_LOG}
etcd_during_partition_log=${ETCD_DURING_LOG}
EOF
}

cleanup() {
  local exit_code=$?
  if [[ -n "${BENCH_PID}" ]] && process_is_running "${BENCH_PID}"; then
    kill "${BENCH_PID}" 2>/dev/null || true
    wait "${BENCH_PID}" 2>/dev/null || true
  fi
  if [[ -n "${CHRONOS_PID}" ]] && process_is_running "${CHRONOS_PID}"; then
    kill "${CHRONOS_PID}" 2>/dev/null || true
    wait "${CHRONOS_PID}" 2>/dev/null || true
  fi
  resume_proxies
  local pid
  for pid in "${PROXY_PIDS[@]}"; do
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  done
  RESULT=$([[ ${exit_code} -eq 0 ]] && echo success || echo failure)
  write_summary
  write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
  make etcd-cluster-reset >/dev/null 2>&1 || true
}
trap cleanup EXIT

make etcd-cluster-reset >/dev/null
make etcd-cluster-up >/dev/null
wait_for_etcd_cluster 60 1 "${INTERNAL_ETCD_ENDPOINTS}"
ensure_release_binaries "${RELEASE_BIN_DIR}" chronos chronos-control-bench
start_proxies
start_chronos "${FAULT_CHRONOS_LOG}" "${FAULT_INSTANCE_ID}"

BASELINE_ALLOCATIONS="$(metric_value)"
BENCH_STARTED_AT_MS="$(now_ms)"
run_allocation_bench "${BENCH_DURATION_SECS}" "${FAULT_BENCH_LOG}" &
BENCH_PID=$!

for _attempt in $(seq 1 80); do
  CURRENT_ALLOCATIONS="$(metric_value)"
  if python3 -c 'import sys; raise SystemExit(0 if float(sys.argv[1]) > float(sys.argv[2]) else 1)' \
    "${CURRENT_ALLOCATIONS}" "${BASELINE_ALLOCATIONS}"; then
    BENCH_ACTIVE_BEFORE_PARTITION="true"
    break
  fi
  process_is_running "${BENCH_PID}" || {
    echo "allocation bench exited before owner partition" >&2
    exit 1
  }
  sleep 0.1
done
[[ "${BENCH_ACTIVE_BEFORE_PARTITION}" == "true" ]] || {
  echo "allocation bench did not become active before owner partition" >&2
  exit 1
}

PARTITION_STARTED_AT_MS="$(now_ms)"
stop_proxies
[[ "${PROXIES_STOPPED}" == "true" ]] || {
  echo "failed to stop every owner-to-etcd proxy" >&2
  exit 1
}

for _attempt in $(seq 1 "${WAIT_ATTEMPTS}"); do
  if ! process_is_running "${CHRONOS_PID}"; then
    CHRONOS_EXITED_AT_MS="$(now_ms)"
    CHRONOS_EXITED_AFTER_PARTITION="true"
    set +e
    wait "${CHRONOS_PID}"
    CHRONOS_PARTITION_EXIT_STATUS=$?
    set -e
    CHRONOS_PID=""
    break
  fi
  sleep "${WAIT_INTERVAL_SECS}"
done
[[ "${CHRONOS_EXITED_AFTER_PARTITION}" == "true" ]] || {
  echo "chronos did not exit after losing its owner-to-etcd path" >&2
  exit 1
}

if process_is_running "${BENCH_PID}"; then
  BENCH_ALIVE_AFTER_CHRONOS_EXIT="true"
else
  echo "allocation bench did not span the fail-closed exit" >&2
  exit 1
fi

docker exec chronos-etcd-1 /usr/local/bin/etcdctl \
  --endpoints="${INTERNAL_ETCD_ENDPOINTS}" endpoint health >"${ETCD_DURING_LOG}" 2>&1
HEALTHY_ETCD_MEMBERS_DURING_PARTITION="$(grep -c 'is healthy' "${ETCD_DURING_LOG}" || true)"
if [[ "${HEALTHY_ETCD_MEMBERS_DURING_PARTITION}" -eq 3 ]]; then
  ETCD_CLUSTER_HEALTHY_DURING_PARTITION="true"
fi

wait "${BENCH_PID}"
BENCH_PID=""
BENCH_FINISHED_AT_MS="$(now_ms)"

if grep -q '"event":"keepalive_lost"' "${FAULT_CHRONOS_LOG}"; then
  IDENTITY_LEASE_LOST_OBSERVED="true"
fi
if grep -q '"shutdown_trigger":"identity_lease_lost"' "${FAULT_CHRONOS_LOG}"; then
  IDENTITY_SHUTDOWN_OBSERVED="true"
fi

resume_proxies
PARTITION_ENDED_AT_MS="$(now_ms)"
wait_for_identity_release
start_chronos "${RECOVERY_CHRONOS_LOG}" "${RECOVERY_INSTANCE_ID}"
RECOVERY_CHRONOS_READY="true"
RECOVERY_READY_AT_MS="$(now_ms)"
wait_for_recovery_allocation
POST_FINISHED_AT_MS="$(now_ms)"

RESULT="success"
write_summary
bash "${REPO_ROOT}/hack/verify-owner-etcd-partition.sh" "${SUMMARY_LOG}"
write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
echo "[owner-etcd-partition] success"
