#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/common.sh"
authority_error() {
  echo "$1" >&2
  return 1
}

verify_scale_evidence_authority() {
  local mode=$1
  local summary=$2
  local count evidence_class
  count="$(awk -F '=' '$1 == "evidence_class" { count++ } END { print count + 0 }' "${summary}")"
  case "${count}:${mode}" in
    0:stress) authority_error "co-located stress evidence is invalid: missing evidence_class"; return ;;
    0:production) authority_error "production scale evidence UNVERIFIED: missing evidence_class; legacy profile=production does not prove topology"; return ;;
    1:stress | 1:production) ;;
    *) authority_error "scale evidence is invalid: evidence_class must appear exactly once, found=${count}"; return ;;
  esac
  evidence_class="$(extract_metric "evidence_class" "${summary}")"
  case "${mode}:${evidence_class}" in
    stress:co_located_stress) return 0 ;;
    stress:*) authority_error "co-located stress evidence is invalid: expected evidence_class=co_located_stress, got=${evidence_class}" ;;
    production:co_located_stress) authority_error "production scale evidence UNVERIFIED: evidence_class=co_located_stress; L2 observed topology evidence is required; stress PASS cannot satisfy release promotion" ;;
    production:*) authority_error "production scale evidence UNVERIFIED: unsupported evidence_class=${evidence_class}; no L2 observed topology verifier is implemented" ;;
  esac
}
verify_scale_workload_profile() {
  local summary=$1
  local profile worker_counts minimum required_worker
  local -a required_workers
  profile="$(extract_metric "profile" "${summary}")"
  worker_counts="$(extract_metric "worker_counts" "${summary}")"
  case "${profile}" in
    smoke) minimum=0.55; required_workers=(2 3) ;;
    production) minimum=0.80; required_workers=(2 3 5 8) ;;
    *) authority_error "scale matrix evidence has unsupported profile=${profile}"; return ;;
  esac
  assert_metric_at_least "linear_efficiency_min" "${summary}" "${minimum}" || return
  [[ "$(extract_metric "allow_single_host_plateau" "${summary}")" == "false" ]] ||
    { authority_error "${profile} scale matrix must not allow a single-host plateau"; return; }
  for required_worker in "${required_workers[@]}"; do
    case ",${worker_counts}," in
      *",${required_worker},"*) ;;
      *) authority_error "${profile} scale matrix is missing worker count ${required_worker}"; return ;;
    esac
  done
}
trim() {
  local value=$1
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}
verify_scale_matrix_summary() {
  local summary=$1
  local authority_mode=$2
  local worker_counts efficiency_min worker key
  local -a workers
  verify_scale_evidence_authority "${authority_mode}" "${summary}" || return
  assert_summary_success "${summary}" || return
  verify_scale_workload_profile "${summary}" || return
  assert_metric_present "worker_counts" "${summary}" || return
  assert_metric_present "linear_efficiency_min" "${summary}" || return
  assert_metric_present "baseline_workers" "${summary}" || return
  assert_positive_metric "baseline_req_per_sec" "${summary}" || return
  worker_counts="$(extract_metric "worker_counts" "${summary}")"
  efficiency_min="$(extract_metric "linear_efficiency_min" "${summary}")"
  IFS=',' read -ra workers <<<"${worker_counts}"
  [[ "${#workers[@]}" -gt 0 ]] || authority_error "scale matrix summary has no worker counts: ${summary}"
  for worker in "${workers[@]}"; do
    worker="$(trim "${worker}")"
    [[ -n "${worker}" ]] || continue
    assert_metric_at_least "workers_${worker}_route_owner_endpoints" "${summary}" "${worker}" || return
    assert_metric_at_least "workers_${worker}_route_owner_min_timelines" "${summary}" "1" || return
    for key in summary route_owner_counts linear_expected_req_per_sec_at_min_efficiency \
      latency_p95_us latency_p99_us latency_p999_us; do
      assert_metric_present "workers_${worker}_${key}" "${summary}" || return
    done
    for key in concurrency timelines allocation_client_channels \
      concurrency_per_allocation_channel bench_client_processes req_per_sec; do
      assert_positive_metric "workers_${worker}_${key}" "${summary}" || return
    done
    [[ "$(extract_metric "workers_${worker}_owner_affinity" "${summary}")" == "true" ]] ||
      { authority_error "workers_${worker}_owner_affinity must be true in ${summary}"; return; }
    assert_metric_at_least "workers_${worker}_linear_efficiency" "${summary}" "${efficiency_min}" || return
    assert_zero_metric "workers_${worker}_allocation_failed_total" "${summary}" || return
  done
}
expect_failure() {
  local expected=$1 output
  shift
  if output="$("$@" 2>&1)"; then authority_error "scale evidence authority unexpectedly passed"; fi
  [[ "${output}" == *"${expected}"* ]] ||
    authority_error "scale evidence authority error mismatch: ${output}"
}
write_profile_fixture() {
  printf 'profile=%s\nworker_counts=%s\nlinear_efficiency_min=%s\nallow_single_host_plateau=%s\n' \
    "$2" "$3" "$4" "$5" >"$1"
}
write_matrix_fixture() {
  local summary=$1 profile=$2 workers=$3 minimum=$4 worker key
  local -a fixture_workers
  write_profile_fixture "${summary}" "${profile}" "${workers}" "${minimum}" false
  printf 'result=success\nevidence_class=co_located_stress\nbaseline_workers=2\nbaseline_req_per_sec=100\n' >>"${summary}"
  IFS=',' read -ra fixture_workers <<<"${workers}"
  for worker in "${fixture_workers[@]}"; do
    printf 'workers_%s_route_owner_endpoints=%s\nworkers_%s_owner_affinity=true\nworkers_%s_allocation_failed_total=0\n' \
      "${worker}" "${worker}" "${worker}" "${worker}" >>"${summary}"
    for key in summary route_owner_min_timelines route_owner_counts concurrency timelines \
      allocation_client_channels concurrency_per_allocation_channel bench_client_processes \
      req_per_sec linear_expected_req_per_sec_at_min_efficiency linear_efficiency \
      latency_p95_us latency_p99_us latency_p999_us; do
      printf 'workers_%s_%s=1\n' "${worker}" "${key}" >>"${summary}"
    done
  done
}
self_test() {
  local summary
  summary="$(mktemp)"
  trap 'rm -f "${summary}"' RETURN
  printf 'evidence_class=co_located_stress\n' >"${summary}"; verify_scale_evidence_authority stress "${summary}"
  : >"${summary}"; expect_failure "missing evidence_class" verify_scale_evidence_authority stress "${summary}"
  printf 'evidence_class=unknown\n' >"${summary}"; expect_failure "got=unknown" verify_scale_evidence_authority stress "${summary}"
  printf 'evidence_class=co_located_stress\nevidence_class=unknown\n' >"${summary}"; expect_failure "found=2" verify_scale_evidence_authority stress "${summary}"
  printf 'evidence_class=co_located_stress\n' >"${summary}"; expect_failure "L2 observed topology evidence is required" verify_scale_evidence_authority production "${summary}"
  : >"${summary}"; expect_failure "legacy profile=production does not prove topology" verify_scale_evidence_authority production "${summary}"
  printf 'evidence_class=production_isolated\n' >"${summary}"; expect_failure "no L2 observed topology verifier is implemented" verify_scale_evidence_authority production "${summary}"
  write_profile_fixture "${summary}" smoke 2,3 0.55 false; verify_scale_workload_profile "${summary}"
  write_profile_fixture "${summary}" smoke 2,3 0.54 false; expect_failure "must be >= 0.55" verify_scale_workload_profile "${summary}"
  write_profile_fixture "${summary}" smoke 2 0.55 false; expect_failure "missing worker count 3" verify_scale_workload_profile "${summary}"
  write_profile_fixture "${summary}" smoke 2,3 0.55 true; expect_failure "must not allow a single-host plateau" verify_scale_workload_profile "${summary}"
  write_profile_fixture "${summary}" unknown 2,3 0.55 false; expect_failure "unsupported profile=unknown" verify_scale_workload_profile "${summary}"
  write_profile_fixture "${summary}" production 2,3,5,8 0.80 false; verify_scale_workload_profile "${summary}"
  write_matrix_fixture "${summary}" smoke 2,3 0.55; verify_scale_matrix_summary "${summary}" stress
  write_matrix_fixture "${summary}" production 2,3,5,8 0.80; verify_scale_matrix_summary "${summary}" stress
  expect_failure "L2 observed topology evidence is required" verify_scale_matrix_summary "${summary}" production
  echo "[scale-evidence-authority] verifier self-test passed"
}
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  [[ "${1:-}" == "--self-test" && $# -eq 1 ]] || { echo "usage: $0 --self-test" >&2; exit 1; }
  self_test
fi
