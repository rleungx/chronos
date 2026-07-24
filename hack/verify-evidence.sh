#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

ARTIFACT_ROOT="${1:-${CHRONOS_ARTIFACT_DIR:-artifacts}}"
shift || true

if [[ $# -eq 0 ]]; then
  set -- soak chaos failover auto-failover scale-matrix rebalance restore cluster-leader-loss owner-etcd-partition rolling-upgrade
fi

BUILD_INFO="${ARTIFACT_ROOT%/}/BUILD_INFO"
if [[ ! -f "${BUILD_INFO}" && -f "${ARTIFACT_ROOT%/}/release/BUILD_INFO" ]]; then
  BUILD_INFO="${ARTIFACT_ROOT%/}/release/BUILD_INFO"
fi
[[ -f "${BUILD_INFO}" ]] || {
  echo "missing release BUILD_INFO: ${BUILD_INFO}" >&2
  exit 1
}
expected_commit="$(git rev-parse HEAD)"
evidence_git_commit="$(extract_metric "git_commit" "${BUILD_INFO}")"
evidence_build_commit="$(extract_metric "chronos_build_commit" "${BUILD_INFO}")"
if [[ "${evidence_git_commit}" != "${expected_commit}" || "${evidence_build_commit}" != "${expected_commit}" ]]; then
  echo "release evidence commit mismatch: expected=${expected_commit} git_commit=${evidence_git_commit} chronos_build_commit=${evidence_build_commit}" >&2
  exit 1
fi

verify_dir() {
  local name=$1
  shift
  local dir="${ARTIFACT_ROOT%/}/${name}"
  [[ -d "${dir}" ]] || { echo "missing artifact directory: ${dir}" >&2; return 1; }
  assert_summary_success "${dir}/summary.txt"
  [[ -f "${dir}/artifact-index.txt" ]] || { echo "missing artifact-index.txt in ${dir}" >&2; return 1; }
  for required in "$@"; do
    [[ -f "${dir}/${required}" ]] || { echo "missing ${required} in ${dir}" >&2; return 1; }
  done
}

trim() {
  local value=$1
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

assert_positive_integer_metric() {
  local key=$1
  local file=$2
  local value
  value="$(extract_metric "${key}" "${file}")"
  if [[ -z "${value}" ]]; then
    echo "missing metric ${key} in ${file}" >&2
    return 1
  fi
  if ! [[ "${value}" =~ ^[0-9]+$ ]] || [[ "${value}" -eq 0 ]]; then
    echo "metric ${key} must be a positive integer, got ${value}" >&2
    return 1
  fi
}

assert_metric_file_exists() {
  local key=$1
  local file=$2
  local path
  path="$(extract_metric "${key}" "${file}")"
  if [[ -z "${path}" ]]; then
    echo "missing file metric ${key} in ${file}" >&2
    return 1
  fi
  if [[ ! -s "${path}" ]]; then
    echo "metric ${key} points to missing or empty file ${path} in ${file}" >&2
    return 1
  fi
}

verify_scale_matrix_summary() {
  local summary=$1
  [[ "$(extract_metric "profile" "${summary}")" == "production" ]] || {
    echo "scale matrix evidence must use profile=production: ${summary}" >&2
    return 1
  }
  [[ "$(extract_metric "allow_single_host_plateau" "${summary}")" == "false" ]] || {
    echo "production scale matrix must not allow a single-host plateau: ${summary}" >&2
    return 1
  }
  assert_metric_present "worker_counts" "${summary}"
  assert_metric_present "linear_efficiency_min" "${summary}"
  assert_metric_present "baseline_workers" "${summary}"
  assert_positive_metric "baseline_req_per_sec" "${summary}"

  local worker_counts
  local efficiency_min
  worker_counts="$(extract_metric "worker_counts" "${summary}")"
  efficiency_min="$(extract_metric "linear_efficiency_min" "${summary}")"
  assert_metric_at_least "linear_efficiency_min" "${summary}" "0.80"
  for required_worker in 2 3 5 8; do
    case ",${worker_counts}," in
      *",${required_worker},"*) ;;
      *)
        echo "production scale matrix is missing worker count ${required_worker}: ${summary}" >&2
        return 1
        ;;
    esac
  done

  local -a workers
  IFS=',' read -ra workers <<<"${worker_counts}"
  [[ "${#workers[@]}" -gt 0 ]] || {
    echo "scale matrix summary has no worker counts: ${summary}" >&2
    return 1
  }

  local worker
  for worker in "${workers[@]}"; do
    worker="$(trim "${worker}")"
    [[ -n "${worker}" ]] || continue
    assert_metric_present "workers_${worker}_summary" "${summary}"
    assert_metric_at_least "workers_${worker}_route_owner_endpoints" "${summary}" "${worker}"
    assert_metric_at_least "workers_${worker}_route_owner_min_timelines" "${summary}" "1"
    assert_metric_present "workers_${worker}_route_owner_counts" "${summary}"
    assert_positive_metric "workers_${worker}_concurrency" "${summary}"
    assert_positive_metric "workers_${worker}_timelines" "${summary}"
    assert_positive_metric "workers_${worker}_allocation_client_channels" "${summary}"
    assert_positive_metric "workers_${worker}_concurrency_per_allocation_channel" "${summary}"
    assert_positive_metric "workers_${worker}_bench_client_processes" "${summary}"
    assert_metric_present "workers_${worker}_owner_affinity" "${summary}"
    [[ "$(extract_metric "workers_${worker}_owner_affinity" "${summary}")" == "true" ]] || {
      echo "workers_${worker}_owner_affinity must be true in ${summary}" >&2
      return 1
    }
    assert_positive_metric "workers_${worker}_req_per_sec" "${summary}"
    assert_metric_present "workers_${worker}_linear_expected_req_per_sec_at_min_efficiency" "${summary}"
    assert_metric_at_least "workers_${worker}_linear_efficiency" "${summary}" "${efficiency_min}"
    assert_zero_metric "workers_${worker}_allocation_failed_total" "${summary}"
    assert_metric_present "workers_${worker}_latency_p95_us" "${summary}"
    assert_metric_present "workers_${worker}_latency_p99_us" "${summary}"
    assert_metric_present "workers_${worker}_latency_p999_us" "${summary}"
  done
}

verify_profile_summary() {
  local profile=$1
  [[ -f "${profile}" ]] || {
    echo "missing profile summary: ${profile}" >&2
    return 1
  }
  assert_positive_integer_metric "worker_count" "${profile}"
  assert_positive_metric "bench.req_per_sec" "${profile}"
  assert_zero_metric "bench.allocation_failed_total" "${profile}"
  assert_positive_metric "bench.allocation_client_channels" "${profile}"
  assert_positive_metric "bench.concurrency_per_allocation_channel" "${profile}"
  assert_positive_metric "bench.bench_client_processes" "${profile}"
  assert_metric_file_exists "host_info" "${profile}"
  assert_metric_file_exists "host_load_before" "${profile}"
  assert_metric_file_exists "host_load_after" "${profile}"
  assert_positive_metric "cluster.tso_allocate_total" "${profile}"
  grep -q 'tso_allocate_latency_avg_us=' "${profile}" || {
    echo "profile summary missing derived allocation latency average: ${profile}" >&2
    return 1
  }
  grep -q 'tso_allocate_latency_p95_upper_bound_us=' "${profile}" || {
    echo "profile summary missing allocation p95 upper bound: ${profile}" >&2
    return 1
  }
  grep -q 'tso_allocate_latency_p99_upper_bound_us=' "${profile}" || {
    echo "profile summary missing allocation p99 upper bound: ${profile}" >&2
    return 1
  }
  grep -q 'cached_admission_wait_p99_upper_bound_us=' "${profile}" || {
    echo "profile summary missing cached admission p99 upper bound: ${profile}" >&2
    return 1
  }
  grep -q 'tso_allocate_share=' "${profile}" || {
    echo "profile summary missing per-worker allocation share: ${profile}" >&2
    return 1
  }

  local worker_count
  local idx
  worker_count="$(extract_metric "worker_count" "${profile}")"
  for ((idx = 0; idx < worker_count; idx++)); do
    assert_metric_file_exists "worker.${idx}.metrics" "${profile}"
    assert_metric_present "worker.${idx}.tso_allocate_share" "${profile}"
    assert_metric_present "worker.${idx}.tso_allocate_latency_p95_upper_bound_us" "${profile}"
    assert_metric_present "worker.${idx}.tso_allocate_latency_p99_upper_bound_us" "${profile}"
    assert_metric_present "worker.${idx}.cached_admission_wait_p99_upper_bound_us" "${profile}"
  done
}

verify_scale_run_dir() {
  local run_dir=$1
  local summary="${run_dir}/summary.txt"
  local worker_count
  local idx
  assert_summary_success "${summary}"
  [[ -f "${run_dir}/artifact-index.txt" ]] || { echo "missing artifact-index.txt in ${run_dir}" >&2; return 1; }
  [[ -f "${run_dir}/scale-bench.log" ]] || { echo "missing scale-bench.log in ${run_dir}" >&2; return 1; }
  [[ -f "${run_dir}/profile-summary.txt" ]] || { echo "missing profile-summary.txt in ${run_dir}" >&2; return 1; }
  assert_positive_integer_metric "worker_count" "${summary}"
  assert_zero_metric "allocation_failed_total" "${summary}"
  assert_metric_present "owner_affinity" "${summary}"
  [[ "$(extract_metric "owner_affinity" "${summary}")" == "true" ]] || {
    echo "owner_affinity must be true in ${summary}" >&2
    return 1
  }
  assert_metric_at_least "route_owner_min_timelines" "${summary}" "1"
  worker_count="$(extract_metric "worker_count" "${summary}")"
  assert_metric_at_least "route_owner_endpoints" "${summary}" "${worker_count}"
  for ((idx = 0; idx < worker_count; idx++)); do
    [[ -f "${run_dir}/chronos-${idx}.log" ]] || { echo "missing chronos-${idx}.log in ${run_dir}" >&2; return 1; }
    [[ -f "${run_dir}/metrics-${idx}.txt" ]] || { echo "missing metrics-${idx}.txt in ${run_dir}" >&2; return 1; }
    [[ -f "${run_dir}/readyz-${idx}.txt" ]] || { echo "missing readyz-${idx}.txt in ${run_dir}" >&2; return 1; }
  done
  verify_profile_summary "${run_dir}/profile-summary.txt"
}

for target in "$@"; do
  case "${target}" in
    soak)
      verify_dir soak summary.txt artifact-index.txt chronos.log bench.log status.log status-filtered.log
      assert_metric_at_least "soak_duration_secs" "${ARTIFACT_ROOT%/}/soak/summary.txt" "3600"
      assert_metric_at_least "soak_warmup_secs" "${ARTIFACT_ROOT%/}/soak/summary.txt" "60"
      assert_metric_at_least "control_duration_secs" "${ARTIFACT_ROOT%/}/soak/summary.txt" "300"
      assert_metric_at_least "filtered_duration_secs" "${ARTIFACT_ROOT%/}/soak/summary.txt" "300"
      ;;
    chaos)
      verify_dir chaos summary.txt artifact-index.txt chronos.log recovery-probe.log recovery-bench.log
      assert_metric_at_least "bench_duration_secs" "${ARTIFACT_ROOT%/}/chaos/summary.txt" "30"
      assert_metric_at_least "recovery_probe_completed_attempts" "${ARTIFACT_ROOT%/}/chaos/summary.txt" "1"
      ;;
    failover)
      verify_dir failover summary.txt artifact-index.txt failover-bench.log
      assert_metric_at_least "bench_duration_secs" "${ARTIFACT_ROOT%/}/failover/summary.txt" "60"
      assert_metric_at_least "bench_warmup_secs" "${ARTIFACT_ROOT%/}/failover/summary.txt" "10"
      ;;
    auto-failover)
      verify_dir auto-failover summary.txt artifact-index.txt failover-bench.log
      assert_metric_at_least "bench_duration_secs" "${ARTIFACT_ROOT%/}/auto-failover/summary.txt" "60"
      assert_metric_at_least "bench_warmup_secs" "${ARTIFACT_ROOT%/}/auto-failover/summary.txt" "10"
      ;;
    scale)
      verify_scale_run_dir "${ARTIFACT_ROOT%/}/scale"
      ;;
    scale-matrix)
      matrix_dir="${ARTIFACT_ROOT%/}/scale-matrix"
      [[ -d "${matrix_dir}" ]] || { echo "missing artifact directory: ${matrix_dir}" >&2; exit 1; }
      assert_summary_success "${matrix_dir}/summary.txt"
      verify_scale_matrix_summary "${matrix_dir}/summary.txt"
      [[ -f "${matrix_dir}/artifact-index.txt" ]] || { echo "missing artifact-index.txt in ${matrix_dir}" >&2; exit 1; }
      found_run=0
      for run_dir in "${matrix_dir}"/workers-*/scale; do
        [[ -d "${run_dir}" ]] || continue
        found_run=1
        verify_scale_run_dir "${run_dir}"
      done
      [[ "${found_run}" -eq 1 ]] || { echo "missing scale matrix run artifacts in ${matrix_dir}" >&2; exit 1; }
      ;;
    rebalance)
      verify_dir rebalance summary.txt artifact-index.txt chronos-a.log chronos-b.log rebalance-bench.log
      ;;
    restore)
      verify_dir restore summary.txt artifact-index.txt chronos.log restore-control.log snapshot.db \
        before-probe.log post-snapshot-probe.log restored-replay-probe.log after-restore-probe.log
      bash "${REPO_ROOT}/hack/verify-dr-monotonicity.sh" \
        "${ARTIFACT_ROOT%/}/restore/summary.txt"
      ;;
    cluster-leader-loss)
      verify_dir cluster-leader-loss summary.txt artifact-index.txt chronos.log \
        fault-span-bench.log post-fault-bench.log \
        etcd-before.json etcd-after.json
      bash "${REPO_ROOT}/hack/verify-cluster-leader-loss.sh" \
        "${ARTIFACT_ROOT%/}/cluster-leader-loss/summary.txt"
      ;;
    owner-etcd-partition)
      verify_dir owner-etcd-partition summary.txt artifact-index.txt \
        fault-chronos.log recovery-chronos.log \
        fault-span-bench.log post-recovery-bench.log \
        etcd-during-partition.txt proxy-1.log proxy-2.log proxy-3.log
      bash "${REPO_ROOT}/hack/verify-owner-etcd-partition.sh" \
        "${ARTIFACT_ROOT%/}/owner-etcd-partition/summary.txt"
      ;;
    rolling-upgrade)
      verify_dir rolling-upgrade summary.txt artifact-index.txt \
        continuous-allocation.txt identity-evidence.txt \
        worker-0-old.log worker-1-old.log worker-2-old.log \
        worker-0-new.log worker-1-new.log worker-2-new.log \
        base-build.log current-build.log
      bash "${REPO_ROOT}/hack/verify-rolling-upgrade.sh" \
        "${ARTIFACT_ROOT%/}/rolling-upgrade/summary.txt"
      ;;
    *)
      echo "unsupported evidence target: ${target}" >&2
      exit 1
      ;;
  esac
done

echo "[evidence] verified ${ARTIFACT_ROOT}"
