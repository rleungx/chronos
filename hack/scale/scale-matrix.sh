#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

csv_to_array() {
  local array_name=$1
  local value=$2
  eval "${array_name}=()"
  local -a raw
  IFS=',' read -ra raw <<<"${value}"
  local item
  for item in "${raw[@]}"; do
    item="${item#"${item%%[![:space:]]*}"}"
    item="${item%"${item##*[![:space:]]}"}"
    [[ -n "${item}" ]] && eval "${array_name}+=(\"\${item}\")"
  done
}

require_positive_integer() {
  local name=$1
  local value=$2
  if ! [[ "${value}" =~ ^[0-9]+$ ]] || [[ "${value}" -eq 0 ]]; then
    echo "${name} must be a positive integer, got: ${value}" >&2
    return 1
  fi
}

matrix_workers="${CHRONOS_SCALE_MATRIX_WORKERS:-2,3}"
matrix_efficiency_min="${CHRONOS_SCALE_MATRIX_LINEAR_EFFICIENCY_MIN:-0.55}"
matrix_concurrency_per_worker="${CHRONOS_SCALE_MATRIX_CONCURRENCY_PER_WORKER:-8}"
matrix_timelines_per_worker="${CHRONOS_SCALE_MATRIX_TIMELINES_PER_WORKER:-32}"
artifact_root="${CHRONOS_SCALE_MATRIX_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-artifacts}/scale-matrix}"
release_bin_dir="${CHRONOS_RELEASE_BIN_DIR:-${REPO_ROOT}/target/release}"
summary_log="${artifact_root%/}/summary.txt"
index_log="${artifact_root%/}/artifact-index.txt"

WORKER_COUNTS=()
csv_to_array WORKER_COUNTS "${matrix_workers}"
if [[ "${#WORKER_COUNTS[@]}" -eq 0 ]]; then
  echo "CHRONOS_SCALE_MATRIX_WORKERS must contain at least one worker count" >&2
  exit 1
fi
for worker_count in "${WORKER_COUNTS[@]}"; do
  require_positive_integer "CHRONOS_SCALE_MATRIX_WORKERS" "${worker_count}"
done
require_positive_integer "CHRONOS_SCALE_MATRIX_CONCURRENCY_PER_WORKER" "${matrix_concurrency_per_worker}"
require_positive_integer "CHRONOS_SCALE_MATRIX_TIMELINES_PER_WORKER" "${matrix_timelines_per_worker}"

mkdir -p "${artifact_root}"

echo "[scale-matrix] preparing release binaries"
ensure_release_binaries "${release_bin_dir}" chronos chronos-bench

baseline_workers=""
baseline_req_per_sec=""
RESULT="failure"

write_summary() {
  cat >"${summary_log}" <<EOF
result=${RESULT}
worker_counts=${matrix_workers}
linear_efficiency_min=${matrix_efficiency_min}
concurrency_per_worker=${matrix_concurrency_per_worker}
timelines_per_worker=${matrix_timelines_per_worker}
artifact_dir=${artifact_root}
artifact_index=${index_log}
baseline_workers=${baseline_workers}
baseline_req_per_sec=${baseline_req_per_sec}
EOF
  local worker_count
  for worker_count in "${WORKER_COUNTS[@]}"; do
    local run_summary="${artifact_root%/}/workers-${worker_count}/scale/summary.txt"
    if [[ -f "${run_summary}" ]]; then
      local req_per_sec
      local latency_p95_us
      local latency_p99_us
      local latency_p999_us
      local failures
      local reasons
      local concurrency
      local timelines
      local connect_timeout_ms
      local connect_retry_interval_ms
      local connection_pool_size
      local client_processes
      local owner_affinity
      req_per_sec="$(extract_metric "req_per_sec" "${run_summary}")"
      latency_p95_us="$(extract_metric "latency_p95_us" "${run_summary}")"
      latency_p99_us="$(extract_metric "latency_p99_us" "${run_summary}")"
      latency_p999_us="$(extract_metric "latency_p999_us" "${run_summary}")"
      failures="$(extract_metric "allocation_failed_total" "${run_summary}")"
      reasons="$(extract_metric "allocation_failure_reasons" "${run_summary}")"
      concurrency="$(extract_metric "bench_concurrency" "${run_summary}")"
      timelines="$(extract_metric "bench_timelines" "${run_summary}")"
      connect_timeout_ms="$(extract_metric "bench_connect_timeout_ms" "${run_summary}")"
      connect_retry_interval_ms="$(extract_metric "bench_connect_retry_interval_ms" "${run_summary}")"
      connection_pool_size="$(extract_metric "bench_allocation_connection_pool_size" "${run_summary}")"
      client_processes="$(extract_metric "bench_client_processes" "${run_summary}")"
      owner_affinity="$(extract_metric "owner_affinity" "${run_summary}")"
      {
        printf 'workers_%s_summary=%s\n' "${worker_count}" "${run_summary}"
        printf 'workers_%s_concurrency=%s\n' "${worker_count}" "${concurrency}"
        printf 'workers_%s_timelines=%s\n' "${worker_count}" "${timelines}"
        printf 'workers_%s_connect_timeout_ms=%s\n' "${worker_count}" "${connect_timeout_ms}"
        printf 'workers_%s_connect_retry_interval_ms=%s\n' "${worker_count}" "${connect_retry_interval_ms}"
        printf 'workers_%s_allocation_connection_pool_size=%s\n' "${worker_count}" "${connection_pool_size}"
        printf 'workers_%s_bench_client_processes=%s\n' "${worker_count}" "${client_processes}"
        printf 'workers_%s_owner_affinity=%s\n' "${worker_count}" "${owner_affinity}"
        printf 'workers_%s_req_per_sec=%s\n' "${worker_count}" "${req_per_sec}"
        printf 'workers_%s_latency_p95_us=%s\n' "${worker_count}" "${latency_p95_us}"
        printf 'workers_%s_latency_p99_us=%s\n' "${worker_count}" "${latency_p99_us}"
        printf 'workers_%s_latency_p999_us=%s\n' "${worker_count}" "${latency_p999_us}"
        printf 'workers_%s_allocation_failed_total=%s\n' "${worker_count}" "${failures}"
        printf 'workers_%s_allocation_failure_reasons=%s\n' "${worker_count}" "${reasons}"
      } >>"${summary_log}"
    fi
  done
}

cleanup() {
  local exit_code=$?
  RESULT=$([[ ${exit_code} -eq 0 ]] && echo success || echo failure)
  write_summary
  write_artifact_index "${artifact_root}" "${index_log}"
}
trap cleanup EXIT

for worker_count in "${WORKER_COUNTS[@]}"; do
  run_root="${artifact_root%/}/workers-${worker_count}"
  run_concurrency="${CHRONOS_SCALE_CONCURRENCY:-$((worker_count * matrix_concurrency_per_worker))}"
  run_timelines="${CHRONOS_SCALE_TIMELINES:-$((worker_count * matrix_timelines_per_worker))}"
  mkdir -p "${run_root}"
  echo "[scale-matrix] running ${worker_count}-worker scale bench with concurrency=${run_concurrency} timelines=${run_timelines}"
  env \
    CHRONOS_SKIP_RELEASE_BUILD=1 \
    CHRONOS_RELEASE_BIN_DIR="${release_bin_dir}" \
    CHRONOS_ARTIFACT_DIR="${run_root}" \
    CHRONOS_SCALE_WORKERS="${worker_count}" \
    CHRONOS_SCALE_CONCURRENCY="${run_concurrency}" \
    CHRONOS_SCALE_TIMELINES="${run_timelines}" \
    CHRONOS_SCALE_SCENARIO="scale-matrix-${worker_count}-$(date +%s)-$$" \
    bash hack/scale/scale-etcd.sh

  run_summary="${run_root}/scale/summary.txt"
  req_per_sec="$(extract_metric "req_per_sec" "${run_summary}")"
  if [[ -z "${req_per_sec}" ]]; then
    echo "missing req_per_sec in ${run_summary}" >&2
    exit 1
  fi

  if [[ -z "${baseline_req_per_sec}" ]]; then
    baseline_workers="${worker_count}"
    baseline_req_per_sec="${req_per_sec}"
    continue
  fi

  python3 - <<'PY' "${req_per_sec}" "${worker_count}" "${baseline_req_per_sec}" "${baseline_workers}" "${matrix_efficiency_min}"
import sys

current = float(sys.argv[1])
current_workers = float(sys.argv[2])
baseline = float(sys.argv[3])
baseline_workers = float(sys.argv[4])
minimum_efficiency = float(sys.argv[5])

expected = baseline * (current_workers / baseline_workers) * minimum_efficiency
if current < expected:
    print(
        f"scale matrix linearity check failed: workers={current_workers:g} "
        f"req_per_sec={current:.2f} expected_at_least={expected:.2f} "
        f"baseline_workers={baseline_workers:g} baseline_req_per_sec={baseline:.2f} "
        f"efficiency_min={minimum_efficiency:.2f}",
        file=sys.stderr,
    )
    sys.exit(1)
PY
done

RESULT="success"
write_summary
write_artifact_index "${artifact_root}" "${index_log}"
echo "[scale-matrix] success"
