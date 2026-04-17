#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"

ARTIFACT_ROOT="${CHRONOS_FAILOVER_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-}}"
KEEP_ARTIFACTS_ON_SUCCESS="${CHRONOS_FAILOVER_KEEP_ARTIFACTS_ON_SUCCESS:-${CHRONOS_KEEP_ARTIFACTS_ON_SUCCESS:-0}}"
ALLOCATE_SUCCESS_PER_SEC_MIN="${CHRONOS_FAILOVER_ALLOCATE_SUCCESS_PER_SEC_MIN:-1}"
ALLOCATE_LATENCY_P95_US_MAX="${CHRONOS_FAILOVER_ALLOCATE_LATENCY_P95_US_MAX:-500000}"
FAILOVER_LATENCY_P95_US_MAX="${CHRONOS_FAILOVER_LATENCY_P95_US_MAX:-10000000}"
FIRST_SUCCESS_AFTER_KILL_MS_MAX="${CHRONOS_FAILOVER_FIRST_SUCCESS_AFTER_KILL_MS_MAX:-15000}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ -n "${ARTIFACT_ROOT}" ]]; then
  ARTIFACT_DIR="${ARTIFACT_ROOT%/}/failover"
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
failover_latency_p95_us_max=${FAILOVER_LATENCY_P95_US_MAX}
first_success_after_kill_ms_max=${FIRST_SUCCESS_AFTER_KILL_MS_MAX}
EOF
}

write_artifact_index() {
  [[ -n "${INDEX_LOG}" ]] || return 0
  mkdir -p "${ARTIFACT_DIR}"
  python3 - <<'PY' "${ARTIFACT_DIR}" "${INDEX_LOG}"
from pathlib import Path
import sys

artifact_dir = Path(sys.argv[1])
index_path = Path(sys.argv[2])
lines = []
for path in sorted(p for p in artifact_dir.iterdir() if p.is_file()):
    lines.append(f"{path.name}\t{path.stat().st_size}")
index_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
PY
}

cleanup() {
  local exit_code=$?
  RESULT=$([[ ${exit_code} -eq 0 ]] && echo success || echo failure)
  write_summary
  write_artifact_index
  if [[ ${exit_code} -ne 0 ]]; then
    echo
    echo "[failover] log: ${FAILOVER_LOG}" >&2
    [[ -n "${ARTIFACT_DIR}" ]] && echo "[failover] artifacts: ${ARTIFACT_DIR}" >&2
  elif [[ "${KEEP_ARTIFACTS_ON_SUCCESS}" != "1" ]]; then
    rm -f "${FAILOVER_LOG}"
  fi
}
trap cleanup EXIT

extract_metric() {
  local key=$1
  local file=$2
  awk -F '=' -v key="${key}" '$1 == key { print $2; exit }' "${file}"
}

assert_metric_present() {
  local key=$1
  local file=$2
  local value
  value="$(extract_metric "${key}" "${file}")"
  if [[ -z "${value}" ]]; then
    echo "missing metric ${key} in ${file}" >&2
    return 1
  fi
}

assert_positive_metric() {
  local key=$1
  local file=$2
  local value
  value="$(extract_metric "${key}" "${file}")"
  if [[ -z "${value}" ]]; then
    echo "missing metric ${key} in ${file}" >&2
    return 1
  fi
  python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) > 0 else 1)' "${value}" || {
    echo "metric ${key} must be > 0, got ${value}" >&2
    return 1
  }
}

assert_zero_metric() {
  local key=$1
  local file=$2
  local value
  value="$(extract_metric "${key}" "${file}")"
  if [[ -z "${value}" ]]; then
    echo "missing metric ${key} in ${file}" >&2
    return 1
  fi
  [[ "${value}" == "0" ]] || {
    echo "metric ${key} must be 0, got ${value}" >&2
    return 1
  }
}

assert_metric_at_least() {
  local key=$1
  local file=$2
  local minimum=$3
  local value
  value="$(extract_metric "${key}" "${file}")"
  if [[ -z "${value}" ]]; then
    echo "missing metric ${key} in ${file}" >&2
    return 1
  fi
  python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) >= float(sys.argv[2]) else 1)' "${value}" "${minimum}" || {
    echo "metric ${key} must be >= ${minimum}, got ${value}" >&2
    return 1
  }
}

assert_metric_at_most() {
  local key=$1
  local file=$2
  local maximum=$3
  local value
  value="$(extract_metric "${key}" "${file}")"
  if [[ -z "${value}" ]]; then
    echo "missing metric ${key} in ${file}" >&2
    return 1
  fi
  python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) <= float(sys.argv[2]) else 1)' "${value}" "${maximum}" || {
    echo "metric ${key} must be <= ${maximum}, got ${value}" >&2
    return 1
  }
}

echo "[failover] building binaries"
cargo build --locked --bin chronos --bin chronos-failover-bench >/dev/null

echo "[failover] running failover benchmark"
target/debug/chronos-failover-bench | tee "${FAILOVER_LOG}"

assert_positive_metric "allocate_success_total" "${FAILOVER_LOG}"
assert_positive_metric "failover_attempts_total" "${FAILOVER_LOG}"
assert_positive_metric "failover_success_total" "${FAILOVER_LOG}"
assert_metric_at_least "allocate_success_per_sec" "${FAILOVER_LOG}" "${ALLOCATE_SUCCESS_PER_SEC_MIN}"
assert_metric_at_most "allocate_latency_p95_us" "${FAILOVER_LOG}" "${ALLOCATE_LATENCY_P95_US_MAX}"
assert_metric_at_most "failover_latency_p95_us" "${FAILOVER_LOG}" "${FAILOVER_LATENCY_P95_US_MAX}"
assert_metric_at_most "first_success_after_kill_ms" "${FAILOVER_LOG}" "${FIRST_SUCCESS_AFTER_KILL_MS_MAX}"
assert_zero_metric "monotonicity_violations_total" "${FAILOVER_LOG}"

RESULT="success"
write_summary
write_artifact_index
echo "[failover] success"
