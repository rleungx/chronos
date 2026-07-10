#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

matrix_workers="${CHRONOS_SCALE_MATRIX_WORKERS:-2,3}"
matrix_profile="${CHRONOS_SCALE_MATRIX_PROFILE:-smoke}"
matrix_efficiency_min="${CHRONOS_SCALE_MATRIX_LINEAR_EFFICIENCY_MIN:-0.55}"
matrix_allow_single_host_plateau="${CHRONOS_SCALE_MATRIX_ALLOW_SINGLE_HOST_PLATEAU:-false}"
matrix_single_host_plateau_min="${CHRONOS_SCALE_MATRIX_SINGLE_HOST_PLATEAU_MIN:-0.95}"
matrix_concurrency_per_worker="${CHRONOS_SCALE_MATRIX_CONCURRENCY_PER_WORKER:-8}"
matrix_timelines_per_worker="${CHRONOS_SCALE_MATRIX_TIMELINES_PER_WORKER:-32}"
matrix_bench_client_processes_per_worker="${CHRONOS_SCALE_MATRIX_BENCH_CLIENT_PROCESSES_PER_WORKER:-1}"
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
require_positive_integer "CHRONOS_SCALE_MATRIX_BENCH_CLIENT_PROCESSES_PER_WORKER" "${matrix_bench_client_processes_per_worker}"

mkdir -p "${artifact_root}"

echo "[scale-matrix] preparing release binaries"
ensure_release_binaries "${release_bin_dir}" chronos chronos-bench

baseline_workers=""
baseline_req_per_sec=""
best_req_per_sec=""
RESULT="failure"

write_summary() {
  cat >"${summary_log}" <<EOF
result=${RESULT}
profile=${matrix_profile}
worker_counts=${matrix_workers}
linear_efficiency_min=${matrix_efficiency_min}
allow_single_host_plateau=${matrix_allow_single_host_plateau}
single_host_plateau_min=${matrix_single_host_plateau_min}
concurrency_per_worker=${matrix_concurrency_per_worker}
timelines_per_worker=${matrix_timelines_per_worker}
bench_client_processes_per_worker=${matrix_bench_client_processes_per_worker}
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
      local route_owner_endpoints
      local route_owner_min_timelines
      local route_owner_max_timelines
      local route_owner_counts
      local owner_endpoint_filter
      local concurrency
      local timelines
      local connect_timeout_ms
      local connect_retry_interval_ms
      local connection_pool_size
      local allocation_client_channels
      local concurrency_per_allocation_channel
      local client_processes
      local worker_index_offsets
      local runtime_worker_threads
      local bench_runtime_worker_threads
      local owner_affinity
      local linear_efficiency
      local linear_expected_req_per_sec
      local linear_plateau_accepted
      local linear_metrics
      req_per_sec="$(extract_metric "req_per_sec" "${run_summary}")"
      latency_p95_us="$(extract_metric "latency_p95_us" "${run_summary}")"
      latency_p99_us="$(extract_metric "latency_p99_us" "${run_summary}")"
      latency_p999_us="$(extract_metric "latency_p999_us" "${run_summary}")"
      failures="$(extract_metric "allocation_failed_total" "${run_summary}")"
      reasons="$(extract_metric "allocation_failure_reasons" "${run_summary}")"
      route_owner_endpoints="$(extract_metric "route_owner_endpoints" "${run_summary}")"
      route_owner_min_timelines="$(extract_metric "route_owner_min_timelines" "${run_summary}")"
      route_owner_max_timelines="$(extract_metric "route_owner_max_timelines" "${run_summary}")"
      route_owner_counts="$(extract_metric "route_owner_counts" "${run_summary}")"
      owner_endpoint_filter="$(extract_metric "owner_endpoint_filter" "${run_summary}")"
      concurrency="$(extract_metric "bench_concurrency" "${run_summary}")"
      timelines="$(extract_metric "bench_timelines" "${run_summary}")"
      connect_timeout_ms="$(extract_metric "bench_connect_timeout_ms" "${run_summary}")"
      connect_retry_interval_ms="$(extract_metric "bench_connect_retry_interval_ms" "${run_summary}")"
      connection_pool_size="$(extract_metric "bench_allocation_connection_pool_size" "${run_summary}")"
      allocation_client_channels="$(extract_metric "allocation_client_channels" "${run_summary}")"
      concurrency_per_allocation_channel="$(extract_metric "concurrency_per_allocation_channel" "${run_summary}")"
      client_processes="$(extract_metric "bench_client_processes" "${run_summary}")"
      worker_index_offsets="$(extract_metric "worker_index_offsets" "${run_summary}")"
      runtime_worker_threads="$(extract_metric "runtime_worker_threads" "${run_summary}")"
      bench_runtime_worker_threads="$(extract_metric "bench_runtime_worker_threads" "${run_summary}")"
      owner_affinity="$(extract_metric "owner_affinity" "${run_summary}")"
      linear_efficiency=""
      linear_expected_req_per_sec=""
      linear_plateau_accepted="false"
      if [[ -n "${baseline_req_per_sec}" && -n "${baseline_workers}" && -n "${req_per_sec}" ]]; then
        linear_metrics="$(
          python3 - "${req_per_sec}" "${worker_count}" "${baseline_req_per_sec}" "${baseline_workers}" "${matrix_efficiency_min}" "${matrix_allow_single_host_plateau}" "${matrix_single_host_plateau_min}" <<'PY'
import sys

current = float(sys.argv[1])
current_workers = float(sys.argv[2])
baseline = float(sys.argv[3])
baseline_workers = float(sys.argv[4])
minimum_efficiency = float(sys.argv[5])
allow_plateau = sys.argv[6].strip().lower() in {"1", "true", "yes", "on"}
plateau_min = float(sys.argv[7])

ideal = baseline * (current_workers / baseline_workers)
efficiency = current / ideal if ideal else 0.0
expected = ideal * minimum_efficiency
plateau_accepted = allow_plateau and current < expected and current >= baseline * plateau_min
print(f"{efficiency:.4f} {expected:.2f} {str(plateau_accepted).lower()}")
PY
        )"
        read -r linear_efficiency linear_expected_req_per_sec linear_plateau_accepted <<<"${linear_metrics}"
      fi
      {
        printf 'workers_%s_summary=%s\n' "${worker_count}" "${run_summary}"
        printf 'workers_%s_route_owner_endpoints=%s\n' "${worker_count}" "${route_owner_endpoints}"
        printf 'workers_%s_route_owner_min_timelines=%s\n' "${worker_count}" "${route_owner_min_timelines}"
        printf 'workers_%s_route_owner_max_timelines=%s\n' "${worker_count}" "${route_owner_max_timelines}"
        printf 'workers_%s_route_owner_counts=%s\n' "${worker_count}" "${route_owner_counts}"
        printf 'workers_%s_owner_endpoint_filter=%s\n' "${worker_count}" "${owner_endpoint_filter}"
        printf 'workers_%s_concurrency=%s\n' "${worker_count}" "${concurrency}"
        printf 'workers_%s_timelines=%s\n' "${worker_count}" "${timelines}"
        printf 'workers_%s_connect_timeout_ms=%s\n' "${worker_count}" "${connect_timeout_ms}"
        printf 'workers_%s_connect_retry_interval_ms=%s\n' "${worker_count}" "${connect_retry_interval_ms}"
        printf 'workers_%s_allocation_connection_pool_size=%s\n' "${worker_count}" "${connection_pool_size}"
        printf 'workers_%s_allocation_client_channels=%s\n' "${worker_count}" "${allocation_client_channels}"
        printf 'workers_%s_concurrency_per_allocation_channel=%s\n' "${worker_count}" "${concurrency_per_allocation_channel}"
        printf 'workers_%s_bench_client_processes=%s\n' "${worker_count}" "${client_processes}"
        printf 'workers_%s_worker_index_offsets=%s\n' "${worker_count}" "${worker_index_offsets}"
        printf 'workers_%s_runtime_worker_threads=%s\n' "${worker_count}" "${runtime_worker_threads}"
        printf 'workers_%s_bench_runtime_worker_threads=%s\n' "${worker_count}" "${bench_runtime_worker_threads}"
        printf 'workers_%s_owner_affinity=%s\n' "${worker_count}" "${owner_affinity}"
        printf 'workers_%s_req_per_sec=%s\n' "${worker_count}" "${req_per_sec}"
        printf 'workers_%s_linear_efficiency=%s\n' "${worker_count}" "${linear_efficiency}"
        printf 'workers_%s_linear_expected_req_per_sec_at_min_efficiency=%s\n' "${worker_count}" "${linear_expected_req_per_sec}"
        printf 'workers_%s_linear_single_host_plateau_accepted=%s\n' "${worker_count}" "${linear_plateau_accepted}"
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
  run_client_processes="${CHRONOS_SCALE_BENCH_CLIENT_PROCESSES:-$((worker_count * matrix_bench_client_processes_per_worker))}"
  if [[ "${run_client_processes}" -gt "${run_concurrency}" ]]; then
    run_client_processes="${run_concurrency}"
  fi
  if [[ "${run_client_processes}" -gt "${run_timelines}" ]]; then
    run_client_processes="${run_timelines}"
  fi
  mkdir -p "${run_root}"
  echo "[scale-matrix] running ${worker_count}-worker scale bench with concurrency=${run_concurrency} timelines=${run_timelines} client_processes=${run_client_processes}"
  env \
    CHRONOS_SKIP_RELEASE_BUILD=1 \
    CHRONOS_RELEASE_BIN_DIR="${release_bin_dir}" \
    CHRONOS_ARTIFACT_DIR="${run_root}" \
    CHRONOS_SCALE_WORKERS="${worker_count}" \
    CHRONOS_SCALE_CONCURRENCY="${run_concurrency}" \
    CHRONOS_SCALE_TIMELINES="${run_timelines}" \
    CHRONOS_SCALE_BENCH_CLIENT_PROCESSES="${run_client_processes}" \
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
    best_req_per_sec="${req_per_sec}"
    continue
  fi

  python3 - "${req_per_sec}" "${worker_count}" "${baseline_req_per_sec}" "${baseline_workers}" "${matrix_efficiency_min}" "${matrix_allow_single_host_plateau}" "${matrix_single_host_plateau_min}" "${best_req_per_sec}" <<'PY'
import sys

current = float(sys.argv[1])
current_workers = float(sys.argv[2])
baseline = float(sys.argv[3])
baseline_workers = float(sys.argv[4])
minimum_efficiency = float(sys.argv[5])
allow_plateau = sys.argv[6].strip().lower() in {"1", "true", "yes", "on"}
plateau_min = float(sys.argv[7])
best = float(sys.argv[8])

expected = baseline * (current_workers / baseline_workers) * minimum_efficiency
if current < expected:
    plateau_floor = best * plateau_min
    if allow_plateau and current >= plateau_floor:
        print(
            f"scale matrix single-host plateau accepted: workers={current_workers:g} "
            f"req_per_sec={current:.2f} linear_expected_at_least={expected:.2f} "
            f"best_req_per_sec={best:.2f} plateau_floor={plateau_floor:.2f}"
        )
        sys.exit(0)
    print(
        f"scale matrix linearity check failed: workers={current_workers:g} "
        f"req_per_sec={current:.2f} expected_at_least={expected:.2f} "
        f"baseline_workers={baseline_workers:g} baseline_req_per_sec={baseline:.2f} "
        f"efficiency_min={minimum_efficiency:.2f}",
        file=sys.stderr,
    )
    sys.exit(1)
PY
  best_req_per_sec="$(python3 - "${best_req_per_sec}" "${req_per_sec}" <<'PY'
import sys

print(f"{max(float(sys.argv[1]), float(sys.argv[2])):.2f}")
PY
  )"
done

RESULT="success"
write_summary
write_artifact_index "${artifact_root}" "${index_log}"
echo "[scale-matrix] success"
