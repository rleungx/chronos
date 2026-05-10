#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

ARTIFACT_ROOT="${CHRONOS_FAILOVER_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-}}"
ARTIFACT_NAME="${CHRONOS_FAILOVER_ARTIFACT_NAME:-failover}"
KEEP_ARTIFACTS_ON_SUCCESS="${CHRONOS_FAILOVER_KEEP_ARTIFACTS_ON_SUCCESS:-${CHRONOS_KEEP_ARTIFACTS_ON_SUCCESS:-0}}"
BENCH_DURATION_SECS="${CHRONOS_FAILOVER_BENCH_DURATION_SECS:-5}"
BENCH_WARMUP_SECS="${CHRONOS_FAILOVER_BENCH_WARMUP_SECS:-1}"
ALLOCATE_SUCCESS_PER_SEC_MIN="${CHRONOS_FAILOVER_ALLOCATE_SUCCESS_PER_SEC_MIN:-0.1}"
ALLOCATE_LATENCY_P95_US_MAX="${CHRONOS_FAILOVER_ALLOCATE_LATENCY_P95_US_MAX:-500000}"
ALLOCATE_LATENCY_P999_US_MAX="${CHRONOS_FAILOVER_ALLOCATE_LATENCY_P999_US_MAX:-2000000}"
ROUTE_REFRESH_P999_US_MAX="${CHRONOS_FAILOVER_ROUTE_REFRESH_P999_US_MAX:-2000000}"
FAILOVER_LATENCY_P95_US_MAX="${CHRONOS_FAILOVER_FAILOVER_LATENCY_P95_US_MAX:-${CHRONOS_FAILOVER_LATENCY_P95_US_MAX:-10000000}}"
FAILOVER_LATENCY_P999_US_MAX="${CHRONOS_FAILOVER_FAILOVER_LATENCY_P999_US_MAX:-${CHRONOS_FAILOVER_LATENCY_P999_US_MAX:-10000000}}"
FIRST_SUCCESS_AFTER_KILL_MS_MAX="${CHRONOS_FAILOVER_FIRST_SUCCESS_AFTER_KILL_MS_MAX:-15000}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ -n "${ARTIFACT_ROOT}" ]]; then
  ARTIFACT_DIR="${ARTIFACT_ROOT%/}/${ARTIFACT_NAME}"
  mkdir -p "${ARTIFACT_DIR}"
  KEEP_ARTIFACTS_ON_SUCCESS=1
  FAILOVER_LOG="${ARTIFACT_DIR}/failover-bench.log"
  SUMMARY_LOG="${ARTIFACT_DIR}/summary.txt"
  INDEX_LOG="${ARTIFACT_DIR}/artifact-index.txt"
else
  ARTIFACT_DIR=""
  FAILOVER_LOG="$(mktemp -t chronos-failover-bench.XXXXXX.log)"
  SUMMARY_LOG=""
  INDEX_LOG=""
fi

RESULT="failure"
RELEASE_BIN_DIR="${CHRONOS_RELEASE_BIN_DIR:-${REPO_ROOT}/target/release}"

write_summary() {
  [[ -n "${SUMMARY_LOG}" ]] || return 0
  mkdir -p "${ARTIFACT_DIR}"
  cat >"${SUMMARY_LOG}" <<EOF
result=${RESULT}
started_at=${STARTED_AT}
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
host=$(hostname)
artifact_dir=${ARTIFACT_DIR}
artifact_index=${INDEX_LOG}
failover_bench_log=${FAILOVER_LOG}
allocate_success_per_sec_min=${ALLOCATE_SUCCESS_PER_SEC_MIN}
allocate_latency_p95_us_max=${ALLOCATE_LATENCY_P95_US_MAX}
allocate_latency_p999_us_max=${ALLOCATE_LATENCY_P999_US_MAX}
route_refresh_p999_us_max=${ROUTE_REFRESH_P999_US_MAX}
failover_latency_p95_us_max=${FAILOVER_LATENCY_P95_US_MAX}
failover_latency_p999_us_max=${FAILOVER_LATENCY_P999_US_MAX}
first_success_after_kill_ms_max=${FIRST_SUCCESS_AFTER_KILL_MS_MAX}
bench_duration_secs=${BENCH_DURATION_SECS}
bench_warmup_secs=${BENCH_WARMUP_SECS}
EOF
}

cleanup() {
  local exit_code=$?
  RESULT=$([[ ${exit_code} -eq 0 ]] && echo success || echo failure)
  write_summary
  write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
  if [[ ${exit_code} -ne 0 ]]; then
    echo
    echo "[failover] log: ${FAILOVER_LOG}" >&2
    [[ -n "${ARTIFACT_DIR}" ]] && echo "[failover] artifacts: ${ARTIFACT_DIR}" >&2
  elif [[ "${KEEP_ARTIFACTS_ON_SUCCESS}" != "1" ]]; then
    rm -f "${FAILOVER_LOG}"
  fi
}
trap cleanup EXIT

echo "[failover] preparing release binaries"
ensure_release_binaries "${RELEASE_BIN_DIR}" chronos chronos-failover-bench

echo "[failover] running failover benchmark"
CHRONOS_FAILOVER_BENCH_ALLOCATE_REQUEST_TIMEOUT_MS="${CHRONOS_FAILOVER_BENCH_ALLOCATE_REQUEST_TIMEOUT_MS:-2000}" \
CHRONOS_FAILOVER_BENCH_ROUTE_REFRESH_TIMEOUT_MS="${CHRONOS_FAILOVER_BENCH_ROUTE_REFRESH_TIMEOUT_MS:-500}" \
CHRONOS_FAILOVER_BENCH_DURATION_SECS="${BENCH_DURATION_SECS}" \
CHRONOS_FAILOVER_BENCH_WARMUP_SECS="${BENCH_WARMUP_SECS}" \
"${RELEASE_BIN_DIR}/chronos-failover-bench" | tee "${FAILOVER_LOG}"

assert_positive_metric "failover_attempts_total" "${FAILOVER_LOG}"
assert_positive_metric "failover_success_total" "${FAILOVER_LOG}"
assert_positive_metric "first_success_after_kill_ms" "${FAILOVER_LOG}"
assert_metric_at_most "allocate_latency_p95_us" "${FAILOVER_LOG}" "${ALLOCATE_LATENCY_P95_US_MAX}"
assert_metric_at_most "allocate_latency_p999_us" "${FAILOVER_LOG}" "${ALLOCATE_LATENCY_P999_US_MAX}"
assert_metric_at_most "route_refresh_p999_us" "${FAILOVER_LOG}" "${ROUTE_REFRESH_P999_US_MAX}"
assert_metric_at_most "failover_latency_p95_us" "${FAILOVER_LOG}" "${FAILOVER_LATENCY_P95_US_MAX}"
assert_metric_at_most "failover_latency_p999_us" "${FAILOVER_LOG}" "${FAILOVER_LATENCY_P999_US_MAX}"
assert_metric_at_most "first_success_after_kill_ms" "${FAILOVER_LOG}" "${FIRST_SUCCESS_AFTER_KILL_MS_MAX}"
assert_zero_metric "monotonicity_violations_total" "${FAILOVER_LOG}"

RESULT="success"
write_summary
write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
echo "[failover] success"
