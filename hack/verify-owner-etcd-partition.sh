#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

verify_owner_etcd_partition() {
  local summary=$1
  [[ -f "${summary}" ]] || {
    echo "missing owner-etcd partition summary: ${summary}" >&2
    return 1
  }
  assert_summary_success "${summary}" || return 1

  local required_true
  for required_true in \
    bench_active_before_partition \
    proxies_stopped \
    chronos_exited_after_partition \
    bench_alive_after_chronos_exit \
    etcd_cluster_healthy_during_partition \
    identity_lease_lost_observed \
    identity_shutdown_observed \
    proxies_resumed \
    identity_released_before_restart \
    recovery_chronos_ready; do
    if [[ "$(extract_metric "${required_true}" "${summary}")" != "true" ]]; then
      echo "${required_true} must be true in ${summary}" >&2
      return 1
    fi
  done

  local required_positive
  for required_positive in \
    bench_started_at_ms partition_started_at_ms chronos_exited_at_ms \
    bench_finished_at_ms partition_ended_at_ms recovery_ready_at_ms post_finished_at_ms \
    recovery_data_plane_attempts \
    fault_allocate_requests_total fault_allocate_success_total fault_allocate_failed_total \
    fault_first_tso fault_last_tso \
    post_allocate_requests_total post_allocate_success_total post_first_tso post_last_tso; do
    require_positive_integer \
      "${required_positive}" \
      "$(extract_metric "${required_positive}" "${summary}")" || return 1
  done

  local required_zero
  for required_zero in \
    chronos_partition_exit_status \
    fault_monotonicity_violations_total \
    post_allocate_failed_total \
    post_monotonicity_violations_total; do
    assert_zero_metric "${required_zero}" "${summary}" || return 1
  done

  python3 - "${summary}" <<'PY'
import sys

summary = {}
with open(sys.argv[1], encoding="utf-8") as handle:
    for line in handle:
        key, separator, value = line.rstrip("\n").partition("=")
        if separator:
            summary[key] = value

def number(key):
    return int(summary[key])

fault_instance = summary.get("fault_instance_id", "")
recovery_instance = summary.get("recovery_instance_id", "")
if not fault_instance or not recovery_instance or fault_instance == recovery_instance:
    raise SystemExit(
        "controlled recovery must use a fresh process identity: "
        f"fault={fault_instance!r} recovery={recovery_instance!r}"
    )

if number("partitioned_proxy_count") != 3:
    raise SystemExit(
        "owner partition must stop all three etcd proxies: "
        f"observed={summary['partitioned_proxy_count']}"
    )
if number("healthy_etcd_members_during_partition") != 3:
    raise SystemExit(
        "the direct etcd cluster must retain all three healthy members during the owner partition: "
        f"observed={summary['healthy_etcd_members_during_partition']}"
    )

started = number("bench_started_at_ms")
partitioned = number("partition_started_at_ms")
exited = number("chronos_exited_at_ms")
bench_finished = number("bench_finished_at_ms")
restored = number("partition_ended_at_ms")
ready = number("recovery_ready_at_ms")
post_finished = number("post_finished_at_ms")
if not started < partitioned < exited < bench_finished <= restored < ready < post_finished:
    raise SystemExit(
        "evidence does not span partition, fail-closed exit, and controlled recovery: "
        f"started={started} partitioned={partitioned} exited={exited} "
        f"bench_finished={bench_finished} restored={restored} ready={ready} "
        f"post_finished={post_finished}"
    )

fault_requests = number("fault_allocate_requests_total")
fault_success = number("fault_allocate_success_total")
fault_failed = number("fault_allocate_failed_total")
if fault_requests != fault_success + fault_failed:
    raise SystemExit(
        "fault-span request accounting is inconsistent: "
        f"requests={fault_requests} success={fault_success} failed={fault_failed}"
    )

post_requests = number("post_allocate_requests_total")
post_success = number("post_allocate_success_total")
if post_requests != post_success:
    raise SystemExit(
        "post-recovery request accounting is inconsistent: "
        f"requests={post_requests} success={post_success}"
    )

fault_first = number("fault_first_tso")
fault_last = number("fault_last_tso")
post_first = number("post_first_tso")
post_last = number("post_last_tso")
if not fault_first <= fault_last < post_first <= post_last:
    raise SystemExit(
        "acknowledged TSO ranges overlap or regress across owner partition recovery: "
        f"fault_span={fault_first}-{fault_last} post={post_first}-{post_last}"
    )
PY
}

self_test() {
  local test_dir
  local summary
  test_dir="$(mktemp -d)"
  summary="${test_dir}/summary.txt"
  trap 'rm -rf "${test_dir}"' RETURN

  cat >"${summary}" <<'EOF'
result=success
bench_active_before_partition=true
proxies_stopped=true
chronos_exited_after_partition=true
bench_alive_after_chronos_exit=true
etcd_cluster_healthy_during_partition=true
identity_lease_lost_observed=true
identity_shutdown_observed=true
proxies_resumed=true
identity_released_before_restart=true
recovery_chronos_ready=true
recovery_data_plane_attempts=2
fault_instance_id=instance-fault
recovery_instance_id=instance-recovery
partitioned_proxy_count=3
healthy_etcd_members_during_partition=3
bench_started_at_ms=1000
partition_started_at_ms=1100
chronos_exited_at_ms=1300
bench_finished_at_ms=1500
partition_ended_at_ms=1500
recovery_ready_at_ms=1700
post_finished_at_ms=1900
chronos_partition_exit_status=0
fault_allocate_requests_total=30
fault_allocate_success_total=20
fault_allocate_failed_total=10
fault_monotonicity_violations_total=0
fault_first_tso=100
fault_last_tso=119
post_allocate_requests_total=10
post_allocate_success_total=10
post_allocate_failed_total=0
post_monotonicity_violations_total=0
post_first_tso=120
post_last_tso=129
EOF
  verify_owner_etcd_partition "${summary}"

  sed 's/healthy_etcd_members_during_partition=3/healthy_etcd_members_during_partition=2/' \
    "${summary}" >"${summary}.invalid"
  if verify_owner_etcd_partition "${summary}.invalid" >/dev/null 2>&1; then
    echo "owner-etcd partition verifier accepted a degraded etcd cluster" >&2
    return 1
  fi

  sed 's/chronos_exited_at_ms=1300/chronos_exited_at_ms=1600/' \
    "${summary}" >"${summary}.invalid"
  if verify_owner_etcd_partition "${summary}.invalid" >/dev/null 2>&1; then
    echo "owner-etcd partition verifier accepted a benchmark that did not span fail-closed exit" >&2
    return 1
  fi

  sed 's/fault_allocate_failed_total=10/fault_allocate_failed_total=0/' \
    "${summary}" >"${summary}.invalid"
  if verify_owner_etcd_partition "${summary}.invalid" >/dev/null 2>&1; then
    echo "owner-etcd partition verifier accepted missing client-visible fail-closed failures" >&2
    return 1
  fi

  sed 's/post_first_tso=120/post_first_tso=119/' "${summary}" >"${summary}.invalid"
  if verify_owner_etcd_partition "${summary}.invalid" >/dev/null 2>&1; then
    echo "owner-etcd partition verifier accepted overlapping TSO ranges" >&2
    return 1
  fi

  sed 's/identity_lease_lost_observed=true/identity_lease_lost_observed=false/' \
    "${summary}" >"${summary}.invalid"
  if verify_owner_etcd_partition "${summary}.invalid" >/dev/null 2>&1; then
    echo "owner-etcd partition verifier accepted missing identity-loss evidence" >&2
    return 1
  fi

  echo "[owner-etcd-partition] verifier self-test passed"
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
  exit 0
fi

[[ $# -eq 1 ]] || {
  echo "usage: $0 <owner-etcd-partition-summary.txt> | --self-test" >&2
  exit 1
}
verify_owner_etcd_partition "$1"
echo "[owner-etcd-partition] verified $1"
