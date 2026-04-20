#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

ETCD_ENDPOINTS="${CHRONOS_REBALANCE_ETCD_ENDPOINTS:-127.0.0.1:2379}"
SERVICE_ENDPOINT_A="${CHRONOS_REBALANCE_SERVICE_ENDPOINT_A:-127.0.0.1:50051}"
SERVICE_ENDPOINT_B="${CHRONOS_REBALANCE_SERVICE_ENDPOINT_B:-127.0.0.1:50052}"
ADVERTISE_ENDPOINT_A="${CHRONOS_REBALANCE_ADVERTISE_ENDPOINT_A:-}"
ADVERTISE_ENDPOINT_B="${CHRONOS_REBALANCE_ADVERTISE_ENDPOINT_B:-}"
METRICS_ENDPOINT_A="${CHRONOS_REBALANCE_METRICS_ENDPOINT_A:-127.0.0.1:9898}"
METRICS_ENDPOINT_B="${CHRONOS_REBALANCE_METRICS_ENDPOINT_B:-127.0.0.1:9899}"
BENCH_DURATION_SECS="${CHRONOS_REBALANCE_DURATION_SECS:-10}"
BENCH_WARMUP_SECS="${CHRONOS_REBALANCE_WARMUP_SECS:-2}"
BENCH_CONCURRENCY="${CHRONOS_REBALANCE_CONCURRENCY:-8}"
BENCH_TIMELINES="${CHRONOS_REBALANCE_TIMELINES:-32}"
BENCH_ALLOCATE_BATCH="${CHRONOS_REBALANCE_ALLOCATE_BATCH:-1}"
TRANSFER_INTERVAL_MS="${CHRONOS_REBALANCE_TRANSFER_INTERVAL_MS:-250}"
TRANSFER_TARGET_GENERATORS="${CHRONOS_REBALANCE_TARGET_GENERATORS:-0,1}"
WAIT_ATTEMPTS="${CHRONOS_REBALANCE_WAIT_ATTEMPTS:-60}"
WAIT_INTERVAL_SECS="${CHRONOS_REBALANCE_WAIT_INTERVAL_SECS:-1}"
UNIQUE_SUFFIX="$(date +%s)-$$"
ETCD_PREFIX="${CHRONOS_REBALANCE_ETCD_PREFIX:-/chronos-rebalance-${UNIQUE_SUFFIX}}"
TIMELINE_NAMESPACE="${CHRONOS_REBALANCE_NAMESPACE:-rebalancebench-${UNIQUE_SUFFIX}}"
WORKER_ID_A="${CHRONOS_REBALANCE_WORKER_ID_A:-worker-rebalance-a}"
WORKER_ID_B="${CHRONOS_REBALANCE_WORKER_ID_B:-worker-rebalance-b}"
SAFETY_GAP_MS="${CHRONOS_REBALANCE_SAFETY_GAP_MS:-1}"
ALLOCATE_SUCCESS_PER_SEC_MIN="${CHRONOS_REBALANCE_ALLOCATE_SUCCESS_PER_SEC_MIN:-5}"
ALLOCATE_LATENCY_P95_US_MAX="${CHRONOS_REBALANCE_ALLOCATE_LATENCY_P95_US_MAX:-500000}"
ROUTE_REFRESH_P95_US_MAX="${CHRONOS_REBALANCE_ROUTE_REFRESH_P95_US_MAX:-500000}"
TRANSFER_LATENCY_P95_US_MAX="${CHRONOS_REBALANCE_TRANSFER_LATENCY_P95_US_MAX:-5000000}"
TRANSFER_FAILED_TOTAL_MAX="${CHRONOS_REBALANCE_TRANSFER_FAILED_TOTAL_MAX:-64}"
ARTIFACT_ROOT="${CHRONOS_REBALANCE_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-}}"
KEEP_ARTIFACTS_ON_SUCCESS="${CHRONOS_REBALANCE_KEEP_ARTIFACTS_ON_SUCCESS:-${CHRONOS_KEEP_ARTIFACTS_ON_SUCCESS:-0}}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ -n "${ARTIFACT_ROOT}" ]]; then
  ARTIFACT_DIR="${ARTIFACT_ROOT%/}/rebalance"
  mkdir -p "${ARTIFACT_DIR}"
  KEEP_ARTIFACTS_ON_SUCCESS=1
  CHRONOS_A_LOG="${ARTIFACT_DIR}/chronos-a.log"
  CHRONOS_B_LOG="${ARTIFACT_DIR}/chronos-b.log"
  REBALANCE_LOG="${ARTIFACT_DIR}/rebalance-bench.log"
  ETCD_LOG="${ARTIFACT_DIR}/etcd.log"
  DOCKER_PS_LOG="${ARTIFACT_DIR}/docker-ps.txt"
  READYZ_A_LOG="${ARTIFACT_DIR}/readyz-a.txt"
  READYZ_B_LOG="${ARTIFACT_DIR}/readyz-b.txt"
  METRICS_A_LOG="${ARTIFACT_DIR}/metrics-a.txt"
  METRICS_B_LOG="${ARTIFACT_DIR}/metrics-b.txt"
  SUMMARY_LOG="${ARTIFACT_DIR}/summary.txt"
  INDEX_LOG="${ARTIFACT_DIR}/artifact-index.txt"
else
  ARTIFACT_DIR=""
  CHRONOS_A_LOG="$(mktemp -t chronos-rebalance-a.XXXXXX.log)"
  CHRONOS_B_LOG="$(mktemp -t chronos-rebalance-b.XXXXXX.log)"
  REBALANCE_LOG="$(mktemp -t chronos-rebalance-bench.XXXXXX.log)"
  ETCD_LOG=""
  DOCKER_PS_LOG=""
  READYZ_A_LOG=""
  READYZ_B_LOG=""
  METRICS_A_LOG=""
  METRICS_B_LOG=""
  SUMMARY_LOG=""
  INDEX_LOG=""
fi

CHRONOS_PID_A=""
CHRONOS_PID_B=""
RESULT="failure"
RELEASE_BIN_DIR="${REPO_ROOT}/target/release"
ADVERTISE_ENDPOINT_A="$(derive_local_advertise_endpoint "${SERVICE_ENDPOINT_A}" "chronos-rebalance-a" "${ADVERTISE_ENDPOINT_A}")"
ADVERTISE_ENDPOINT_B="$(derive_local_advertise_endpoint "${SERVICE_ENDPOINT_B}" "chronos-rebalance-b" "${ADVERTISE_ENDPOINT_B}")"

write_summary() {
  [[ -n "${SUMMARY_LOG}" ]] || return 0
  mkdir -p "${ARTIFACT_DIR}"
  cat >"${SUMMARY_LOG}" <<EOF
result=${RESULT}
started_at=${STARTED_AT}
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
host=$(hostname)
etcd_endpoints=${ETCD_ENDPOINTS}
service_endpoint_a=${SERVICE_ENDPOINT_A}
service_endpoint_b=${SERVICE_ENDPOINT_B}
metrics_endpoint_a=${METRICS_ENDPOINT_A}
metrics_endpoint_b=${METRICS_ENDPOINT_B}
bench_duration_secs=${BENCH_DURATION_SECS}
bench_warmup_secs=${BENCH_WARMUP_SECS}
bench_concurrency=${BENCH_CONCURRENCY}
bench_timelines=${BENCH_TIMELINES}
bench_allocate_batch=${BENCH_ALLOCATE_BATCH}
transfer_interval_ms=${TRANSFER_INTERVAL_MS}
transfer_target_generators=${TRANSFER_TARGET_GENERATORS}
etcd_prefix=${ETCD_PREFIX}
timeline_namespace=${TIMELINE_NAMESPACE}
worker_id_a=${WORKER_ID_A}
worker_id_b=${WORKER_ID_B}
safety_gap_ms=${SAFETY_GAP_MS}
allocate_success_per_sec_min=${ALLOCATE_SUCCESS_PER_SEC_MIN}
allocate_latency_p95_us_max=${ALLOCATE_LATENCY_P95_US_MAX}
route_refresh_p95_us_max=${ROUTE_REFRESH_P95_US_MAX}
transfer_latency_p95_us_max=${TRANSFER_LATENCY_P95_US_MAX}
transfer_failed_total_max=${TRANSFER_FAILED_TOTAL_MAX}
artifact_dir=${ARTIFACT_DIR}
artifact_index=${INDEX_LOG}
chronos_a_log=${CHRONOS_A_LOG}
chronos_b_log=${CHRONOS_B_LOG}
rebalance_bench_log=${REBALANCE_LOG}
EOF
}

capture_diagnostics() {
  [[ -n "${ARTIFACT_DIR}" ]] && mkdir -p "${ARTIFACT_DIR}"
  [[ -n "${DOCKER_PS_LOG}" ]] && docker ps -a >"${DOCKER_PS_LOG}" 2>/dev/null || true
  [[ -n "${ETCD_LOG}" ]] && docker logs chronos-etcd >"${ETCD_LOG}" 2>&1 || true
  [[ -n "${READYZ_A_LOG}" ]] && curl --max-time 2 -fsS "http://${METRICS_ENDPOINT_A}/readyz" >"${READYZ_A_LOG}" 2>&1 || true
  [[ -n "${READYZ_B_LOG}" ]] && curl --max-time 2 -fsS "http://${METRICS_ENDPOINT_B}/readyz" >"${READYZ_B_LOG}" 2>&1 || true
  [[ -n "${METRICS_A_LOG}" ]] && curl --max-time 2 -fsS "http://${METRICS_ENDPOINT_A}/metrics" >"${METRICS_A_LOG}" 2>&1 || true
  [[ -n "${METRICS_B_LOG}" ]] && curl --max-time 2 -fsS "http://${METRICS_ENDPOINT_B}/metrics" >"${METRICS_B_LOG}" 2>&1 || true
}

cleanup() {
  local exit_code=$?
  capture_diagnostics
  RESULT=$([[ ${exit_code} -eq 0 ]] && echo success || echo failure)
  write_summary
  write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
  make etcd-reset >/dev/null 2>&1 || true
  if [[ -n "${CHRONOS_PID_A}" ]] && kill -0 "${CHRONOS_PID_A}" 2>/dev/null; then
    kill "${CHRONOS_PID_A}" 2>/dev/null || true
    wait "${CHRONOS_PID_A}" 2>/dev/null || true
  fi
  if [[ -n "${CHRONOS_PID_B}" ]] && kill -0 "${CHRONOS_PID_B}" 2>/dev/null; then
    kill "${CHRONOS_PID_B}" 2>/dev/null || true
    wait "${CHRONOS_PID_B}" 2>/dev/null || true
  fi
  make etcd-reset >/dev/null 2>&1 || true
  if [[ ${exit_code} -ne 0 ]]; then
    echo
    echo "[rebalance] chronos-a log: ${CHRONOS_A_LOG}" >&2
    echo "[rebalance] chronos-b log: ${CHRONOS_B_LOG}" >&2
    echo "[rebalance] bench log: ${REBALANCE_LOG}" >&2
    [[ -n "${ARTIFACT_DIR}" ]] && echo "[rebalance] artifacts: ${ARTIFACT_DIR}" >&2
  elif [[ "${KEEP_ARTIFACTS_ON_SUCCESS}" != "1" ]]; then
    rm -f "${CHRONOS_A_LOG}" "${CHRONOS_B_LOG}" "${REBALANCE_LOG}"
  fi
}
trap cleanup EXIT

wait_for_etcd() {
  for _attempt in $(seq 1 "${WAIT_ATTEMPTS}"); do
    if make etcd-health >/dev/null 2>&1; then
      return 0
    fi
    sleep "${WAIT_INTERVAL_SECS}"
  done
  echo "etcd did not become healthy after ${WAIT_ATTEMPTS} attempts" >&2
  return 1
}

echo "[rebalance] resetting etcd"
make etcd-reset >/dev/null
echo "[rebalance] starting etcd"
make etcd-up >/dev/null
wait_for_etcd

echo "[rebalance] building release binaries"
cargo build --locked --release --bin chronos --bin chronos-control-bench >/dev/null

echo "[rebalance] starting chronos worker A"
env \
  CHRONOS_SECURITY_MODE=dev-insecure \
  CHRONOS_METADATA=etcd \
  CHRONOS_BIND_ADDR="${SERVICE_ENDPOINT_A}" \
  CHRONOS_ADVERTISE_ENDPOINT="${ADVERTISE_ENDPOINT_A}" \
  CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINT_A}" \
  CHRONOS_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
  CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" \
  CHRONOS_WORKER_ID="${WORKER_ID_A}" \
  CHRONOS_SAFETY_GAP_MS="${SAFETY_GAP_MS}" \
  "${RELEASE_BIN_DIR}/chronos" >"${CHRONOS_A_LOG}" 2>&1 &
CHRONOS_PID_A=$!

echo "[rebalance] starting chronos worker B"
env \
  CHRONOS_SECURITY_MODE=dev-insecure \
  CHRONOS_METADATA=etcd \
  CHRONOS_BIND_ADDR="${SERVICE_ENDPOINT_B}" \
  CHRONOS_ADVERTISE_ENDPOINT="${ADVERTISE_ENDPOINT_B}" \
  CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINT_B}" \
  CHRONOS_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
  CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" \
  CHRONOS_WORKER_ID="${WORKER_ID_B}" \
  CHRONOS_SAFETY_GAP_MS="${SAFETY_GAP_MS}" \
  "${RELEASE_BIN_DIR}/chronos" >"${CHRONOS_B_LOG}" 2>&1 &
CHRONOS_PID_B=$!

wait_for_http "http://${METRICS_ENDPOINT_A}/readyz" "chronos worker A readyz" "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"
wait_for_http "http://${METRICS_ENDPOINT_B}/readyz" "chronos worker B readyz" "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"

echo "[rebalance] running control-plane rebalance benchmark"
env \
  CHRONOS_CONTROL_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT_A}" \
  CHRONOS_CONTROL_BENCH_NAMESPACE="${TIMELINE_NAMESPACE}" \
  CHRONOS_CONTROL_BENCH_SCENARIO=allocate_during_rebalance \
  CHRONOS_CONTROL_BENCH_CONCURRENCY="${BENCH_CONCURRENCY}" \
  CHRONOS_CONTROL_BENCH_TIMELINES="${BENCH_TIMELINES}" \
  CHRONOS_CONTROL_BENCH_DURATION_SECS="${BENCH_DURATION_SECS}" \
  CHRONOS_CONTROL_BENCH_WARMUP_SECS="${BENCH_WARMUP_SECS}" \
  CHRONOS_CONTROL_BENCH_ALLOCATE_BATCH="${BENCH_ALLOCATE_BATCH}" \
  CHRONOS_CONTROL_BENCH_TRANSFER_INTERVAL_MS="${TRANSFER_INTERVAL_MS}" \
  CHRONOS_CONTROL_BENCH_TRANSFER_TARGET_GENERATORS="${TRANSFER_TARGET_GENERATORS}" \
  "${RELEASE_BIN_DIR}/chronos-control-bench" | tee "${REBALANCE_LOG}"

curl -fsS "http://${METRICS_ENDPOINT_A}/readyz" | grep -qx 'ready'
curl -fsS "http://${METRICS_ENDPOINT_B}/readyz" | grep -qx 'ready'
curl -fsS "http://${METRICS_ENDPOINT_A}/metrics" | grep -q '^tso_startup_ready'
curl -fsS "http://${METRICS_ENDPOINT_B}/metrics" | grep -q '^tso_startup_ready'

assert_positive_metric "allocate_success_total" "${REBALANCE_LOG}"
assert_positive_metric "transfer_attempts_total" "${REBALANCE_LOG}"
assert_positive_metric "transfer_success_total" "${REBALANCE_LOG}"
assert_positive_metric "route_refresh_total" "${REBALANCE_LOG}"
assert_metric_at_least "allocate_success_per_sec" "${REBALANCE_LOG}" "${ALLOCATE_SUCCESS_PER_SEC_MIN}"
assert_metric_at_most "allocate_latency_p95_us" "${REBALANCE_LOG}" "${ALLOCATE_LATENCY_P95_US_MAX}"
assert_metric_at_most "route_refresh_p95_us" "${REBALANCE_LOG}" "${ROUTE_REFRESH_P95_US_MAX}"
assert_metric_at_most "transfer_latency_p95_us" "${REBALANCE_LOG}" "${TRANSFER_LATENCY_P95_US_MAX}"
assert_metric_at_most "transfer_failed_total" "${REBALANCE_LOG}" "${TRANSFER_FAILED_TOTAL_MAX}"
assert_zero_metric "monotonicity_violations_total" "${REBALANCE_LOG}"

RESULT="success"
write_summary
write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
echo "[rebalance] success"
