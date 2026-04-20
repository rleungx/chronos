#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

ETCD_ENDPOINTS="${CHRONOS_SOAK_ETCD_ENDPOINTS:-127.0.0.1:2379}"
SERVICE_ENDPOINT="${CHRONOS_SOAK_SERVICE_ENDPOINT:-127.0.0.1:50051}"
ADVERTISE_ENDPOINT="${CHRONOS_SOAK_ADVERTISE_ENDPOINT:-}"
METRICS_ENDPOINT="${CHRONOS_SOAK_METRICS_ENDPOINT:-127.0.0.1:9898}"
SOAK_DURATION_SECS="${CHRONOS_SOAK_DURATION_SECS:-15}"
SOAK_WARMUP_SECS="${CHRONOS_SOAK_WARMUP_SECS:-3}"
SOAK_CONCURRENCY="${CHRONOS_SOAK_CONCURRENCY:-32}"
SOAK_TIMELINES="${CHRONOS_SOAK_TIMELINES:-32}"
SOAK_BATCH="${CHRONOS_SOAK_BATCH:-1}"
CONTROL_TIMELINES="${CHRONOS_SOAK_CONTROL_TIMELINES:-2000}"
CONTROL_PAGE_SIZE="${CHRONOS_SOAK_CONTROL_PAGE_SIZE:-200}"
CONTROL_CONCURRENCY="${CHRONOS_SOAK_CONTROL_CONCURRENCY:-8}"
FILTERED_CONCURRENCY="${CHRONOS_SOAK_FILTERED_CONCURRENCY:-4}"
WAIT_ATTEMPTS="${CHRONOS_SOAK_WAIT_ATTEMPTS:-60}"
WAIT_INTERVAL_SECS="${CHRONOS_SOAK_WAIT_INTERVAL_SECS:-1}"
SOAK_REQ_PER_SEC_MIN="${CHRONOS_SOAK_REQ_PER_SEC_MIN:-50}"
SOAK_LATENCY_P95_US_MAX="${CHRONOS_SOAK_LATENCY_P95_US_MAX:-200000}"
SOAK_LATENCY_P99_US_MAX="${CHRONOS_SOAK_LATENCY_P99_US_MAX:-500000}"
CONTROL_REQ_PER_SEC_MIN="${CHRONOS_SOAK_CONTROL_REQ_PER_SEC_MIN:-10}"
CONTROL_RPC_P95_US_MAX="${CHRONOS_SOAK_CONTROL_RPC_P95_US_MAX:-500000}"
CONTROL_SCAN_P95_US_MAX="${CHRONOS_SOAK_CONTROL_SCAN_P95_US_MAX:-1000000}"
FILTERED_REQ_PER_SEC_MIN="${CHRONOS_SOAK_FILTERED_REQ_PER_SEC_MIN:-10}"
FILTERED_RPC_P95_US_MAX="${CHRONOS_SOAK_FILTERED_RPC_P95_US_MAX:-500000}"
FILTERED_SCAN_P95_US_MAX="${CHRONOS_SOAK_FILTERED_SCAN_P95_US_MAX:-1500000}"
UNIQUE_SUFFIX="$(date +%s)-$$"
ETCD_PREFIX="${CHRONOS_SOAK_ETCD_PREFIX:-/chronos-soak-${UNIQUE_SUFFIX}}"
WORKER_ID="${CHRONOS_SOAK_WORKER_ID:-worker-soak}"
SAFETY_GAP_MS="${CHRONOS_SOAK_SAFETY_GAP_MS:-1}"
ARTIFACT_ROOT="${CHRONOS_SOAK_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-}}"
KEEP_ARTIFACTS_ON_SUCCESS="${CHRONOS_SOAK_KEEP_ARTIFACTS_ON_SUCCESS:-${CHRONOS_KEEP_ARTIFACTS_ON_SUCCESS:-0}}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ -n "${ARTIFACT_ROOT}" ]]; then
  ARTIFACT_DIR="${ARTIFACT_ROOT%/}/soak"
  mkdir -p "${ARTIFACT_DIR}"
  KEEP_ARTIFACTS_ON_SUCCESS=1
  CHRONOS_LOG="${ARTIFACT_DIR}/chronos.log"
  BENCH_LOG="${ARTIFACT_DIR}/bench.log"
  CONTROL_STATUS_LOG="${ARTIFACT_DIR}/status.log"
  CONTROL_FILTERED_LOG="${ARTIFACT_DIR}/status-filtered.log"
  ETCD_LOG="${ARTIFACT_DIR}/etcd.log"
  DOCKER_PS_LOG="${ARTIFACT_DIR}/docker-ps.txt"
  READYZ_LOG="${ARTIFACT_DIR}/readyz.txt"
  METRICS_LOG="${ARTIFACT_DIR}/metrics.txt"
  SUMMARY_LOG="${ARTIFACT_DIR}/summary.txt"
  INDEX_LOG="${ARTIFACT_DIR}/artifact-index.txt"
else
  ARTIFACT_DIR=""
  CHRONOS_LOG="$(mktemp -t chronos-soak.XXXXXX.log)"
  BENCH_LOG="$(mktemp -t chronos-soak-bench.XXXXXX.log)"
  CONTROL_STATUS_LOG="$(mktemp -t chronos-soak-status.XXXXXX.log)"
  CONTROL_FILTERED_LOG="$(mktemp -t chronos-soak-status-filtered.XXXXXX.log)"
  ETCD_LOG=""
  DOCKER_PS_LOG=""
  READYZ_LOG=""
  METRICS_LOG=""
  SUMMARY_LOG=""
  INDEX_LOG=""
fi

CHRONOS_PID=""
RESULT="failure"
RELEASE_BIN_DIR="${REPO_ROOT}/target/release"
ADVERTISE_ENDPOINT="$(derive_local_advertise_endpoint "${SERVICE_ENDPOINT}" "chronos-soak" "${ADVERTISE_ENDPOINT}")"

write_summary() {
  [[ -n "${SUMMARY_LOG}" ]] || return 0
  mkdir -p "${ARTIFACT_DIR}"
  cat >"${SUMMARY_LOG}" <<EOF
result=${RESULT}
started_at=${STARTED_AT}
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
host=$(hostname)
etcd_endpoints=${ETCD_ENDPOINTS}
service_endpoint=${SERVICE_ENDPOINT}
metrics_endpoint=${METRICS_ENDPOINT}
soak_duration_secs=${SOAK_DURATION_SECS}
soak_warmup_secs=${SOAK_WARMUP_SECS}
soak_concurrency=${SOAK_CONCURRENCY}
soak_timelines=${SOAK_TIMELINES}
soak_batch=${SOAK_BATCH}
control_timelines=${CONTROL_TIMELINES}
control_page_size=${CONTROL_PAGE_SIZE}
control_concurrency=${CONTROL_CONCURRENCY}
filtered_concurrency=${FILTERED_CONCURRENCY}
soak_req_per_sec_min=${SOAK_REQ_PER_SEC_MIN}
soak_latency_p95_us_max=${SOAK_LATENCY_P95_US_MAX}
soak_latency_p99_us_max=${SOAK_LATENCY_P99_US_MAX}
control_req_per_sec_min=${CONTROL_REQ_PER_SEC_MIN}
control_rpc_p95_us_max=${CONTROL_RPC_P95_US_MAX}
control_scan_p95_us_max=${CONTROL_SCAN_P95_US_MAX}
filtered_req_per_sec_min=${FILTERED_REQ_PER_SEC_MIN}
filtered_rpc_p95_us_max=${FILTERED_RPC_P95_US_MAX}
filtered_scan_p95_us_max=${FILTERED_SCAN_P95_US_MAX}
etcd_prefix=${ETCD_PREFIX}
worker_id=${WORKER_ID}
safety_gap_ms=${SAFETY_GAP_MS}
artifact_dir=${ARTIFACT_DIR}
artifact_index=${INDEX_LOG}
chronos_log=${CHRONOS_LOG}
bench_log=${BENCH_LOG}
status_log=${CONTROL_STATUS_LOG}
filtered_status_log=${CONTROL_FILTERED_LOG}
EOF
}

capture_diagnostics() {
  [[ -n "${ARTIFACT_DIR}" ]] && mkdir -p "${ARTIFACT_DIR}"
  [[ -n "${DOCKER_PS_LOG}" ]] && docker ps -a >"${DOCKER_PS_LOG}" 2>/dev/null || true
  [[ -n "${ETCD_LOG}" ]] && docker logs chronos-etcd >"${ETCD_LOG}" 2>&1 || true
  [[ -n "${READYZ_LOG}" ]] && curl --max-time 2 -fsS "http://${METRICS_ENDPOINT}/readyz" >"${READYZ_LOG}" 2>&1 || true
  [[ -n "${METRICS_LOG}" ]] && curl --max-time 2 -fsS "http://${METRICS_ENDPOINT}/metrics" >"${METRICS_LOG}" 2>&1 || true
}

cleanup() {
  local exit_code=$?
  capture_diagnostics
  RESULT=$([[ ${exit_code} -eq 0 ]] && echo success || echo failure)
  write_summary
  write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
  make etcd-reset >/dev/null 2>&1 || true
  if [[ -n "${CHRONOS_PID}" ]] && kill -0 "${CHRONOS_PID}" 2>/dev/null; then
    kill "${CHRONOS_PID}" 2>/dev/null || true
    wait "${CHRONOS_PID}" 2>/dev/null || true
  fi
  make etcd-reset >/dev/null 2>&1 || true
  if [[ ${exit_code} -ne 0 ]]; then
    echo
    echo "[soak] chronos log: ${CHRONOS_LOG}" >&2
    echo "[soak] bench log: ${BENCH_LOG}" >&2
    echo "[soak] status log: ${CONTROL_STATUS_LOG}" >&2
    echo "[soak] filtered status log: ${CONTROL_FILTERED_LOG}" >&2
    [[ -n "${ARTIFACT_DIR}" ]] && echo "[soak] artifacts: ${ARTIFACT_DIR}" >&2
  elif [[ "${KEEP_ARTIFACTS_ON_SUCCESS}" != "1" ]]; then
    rm -f "${CHRONOS_LOG}" "${BENCH_LOG}" "${CONTROL_STATUS_LOG}" "${CONTROL_FILTERED_LOG}"
  fi
}
trap cleanup EXIT

echo "[soak] resetting etcd"
make etcd-reset >/dev/null
echo "[soak] starting etcd"
make etcd-up >/dev/null
wait_for_etcd "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"

echo "[soak] building release binaries"
cargo build --locked --release --bin chronos --bin chronos-bench --bin chronos-control-bench >/dev/null

echo "[soak] starting chronos (release, etcd-backed)"
env \
  CHRONOS_SECURITY_MODE=dev-insecure \
  CHRONOS_METADATA=etcd \
  CHRONOS_BIND_ADDR="${SERVICE_ENDPOINT}" \
  CHRONOS_ADVERTISE_ENDPOINT="${ADVERTISE_ENDPOINT}" \
  CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINT}" \
  CHRONOS_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
  CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" \
  CHRONOS_WORKER_ID="${WORKER_ID}" \
  CHRONOS_SAFETY_GAP_MS="${SAFETY_GAP_MS}" \
  "${RELEASE_BIN_DIR}/chronos" >"${CHRONOS_LOG}" 2>&1 &
CHRONOS_PID=$!

wait_for_http "http://${METRICS_ENDPOINT}/readyz" "chronos readyz" "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"

echo "[soak] running chronos-bench"
env \
  CHRONOS_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" \
  CHRONOS_BENCH_CONCURRENCY="${SOAK_CONCURRENCY}" \
  CHRONOS_BENCH_TIMELINES="${SOAK_TIMELINES}" \
  CHRONOS_BENCH_BATCH="${SOAK_BATCH}" \
  CHRONOS_BENCH_DURATION_SECS="${SOAK_DURATION_SECS}" \
  CHRONOS_BENCH_WARMUP_SECS="${SOAK_WARMUP_SECS}" \
  "${RELEASE_BIN_DIR}/chronos-bench" | tee "${BENCH_LOG}"

echo "[soak] running control-plane status benchmark"
env \
  CHRONOS_CONTROL_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" \
  CHRONOS_CONTROL_BENCH_SCENARIO=status_scan \
  CHRONOS_CONTROL_BENCH_CONCURRENCY="${CONTROL_CONCURRENCY}" \
  CHRONOS_CONTROL_BENCH_TIMELINES="${CONTROL_TIMELINES}" \
  CHRONOS_CONTROL_BENCH_PAGE_SIZE="${CONTROL_PAGE_SIZE}" \
  CHRONOS_CONTROL_BENCH_DURATION_SECS="${SOAK_DURATION_SECS}" \
  CHRONOS_CONTROL_BENCH_WARMUP_SECS="${SOAK_WARMUP_SECS}" \
  "${RELEASE_BIN_DIR}/chronos-control-bench" | tee "${CONTROL_STATUS_LOG}"

echo "[soak] running filtered control-plane status benchmark"
env \
  CHRONOS_CONTROL_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" \
  CHRONOS_CONTROL_BENCH_SCENARIO=status_scan_filtered \
  CHRONOS_CONTROL_BENCH_CONCURRENCY="${FILTERED_CONCURRENCY}" \
  CHRONOS_CONTROL_BENCH_TIMELINES="${CONTROL_TIMELINES}" \
  CHRONOS_CONTROL_BENCH_PAGE_SIZE="${CONTROL_PAGE_SIZE}" \
  CHRONOS_CONTROL_BENCH_DURATION_SECS="${SOAK_DURATION_SECS}" \
  CHRONOS_CONTROL_BENCH_WARMUP_SECS="${SOAK_WARMUP_SECS}" \
  CHRONOS_CONTROL_BENCH_FILTER_OWNER_ENDPOINT="${ADVERTISE_ENDPOINT}" \
  "${RELEASE_BIN_DIR}/chronos-control-bench" | tee "${CONTROL_FILTERED_LOG}"

echo "[soak] validating readiness and metrics surfaces"
curl -fsS "http://${METRICS_ENDPOINT}/readyz" | grep -qx 'ready'
curl -fsS "http://${METRICS_ENDPOINT}/metrics" | grep -q '^tso_build_info'
curl -fsS "http://${METRICS_ENDPOINT}/metrics" | grep -q '^tso_startup_ready'
curl -fsS "http://${METRICS_ENDPOINT}/metrics" | grep -q '^tso_allocate_total'

assert_metric_at_least "req_per_sec" "${BENCH_LOG}" "${SOAK_REQ_PER_SEC_MIN}"
assert_metric_at_most "latency_p95_us" "${BENCH_LOG}" "${SOAK_LATENCY_P95_US_MAX}"
assert_metric_at_most "latency_p99_us" "${BENCH_LOG}" "${SOAK_LATENCY_P99_US_MAX}"
assert_metric_at_least "req_per_sec" "${CONTROL_STATUS_LOG}" "${CONTROL_REQ_PER_SEC_MIN}"
assert_metric_at_most "rpc_latency_p95_us" "${CONTROL_STATUS_LOG}" "${CONTROL_RPC_P95_US_MAX}"
assert_metric_at_most "scan_latency_p95_us" "${CONTROL_STATUS_LOG}" "${CONTROL_SCAN_P95_US_MAX}"
assert_metric_at_least "req_per_sec" "${CONTROL_FILTERED_LOG}" "${FILTERED_REQ_PER_SEC_MIN}"
assert_metric_at_most "rpc_latency_p95_us" "${CONTROL_FILTERED_LOG}" "${FILTERED_RPC_P95_US_MAX}"
assert_metric_at_most "scan_latency_p95_us" "${CONTROL_FILTERED_LOG}" "${FILTERED_SCAN_P95_US_MAX}"

RESULT="success"
write_summary
write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
echo "[soak] success"
