#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

verify_cluster_leader_loss() {
  local summary=$1
  [[ -f "${summary}" ]] || {
    echo "missing clustered leader-loss summary: ${summary}" >&2
    return 1
  }
  assert_summary_success "${summary}" || return 1

  local required_true
  for required_true in old_leader_stopped bench_active_before_fault bench_alive_after_leader_change; do
    if [[ "$(extract_metric "${required_true}" "${summary}")" != "true" ]]; then
      echo "${required_true} must be true in ${summary}" >&2
      return 1
    fi
  done

  local required_positive
  for required_positive in \
    old_leader_id new_leader_id old_raft_term new_raft_term \
    bench_started_at_ms fault_injected_at_ms leader_changed_at_ms bench_finished_at_ms \
    allocations_at_leader_change allocations_after_bench \
    fault_allocate_requests_total fault_allocate_success_total fault_first_tso fault_last_tso \
    post_allocate_requests_total post_allocate_success_total post_first_tso post_last_tso; do
    require_positive_integer \
      "${required_positive}" \
      "$(extract_metric "${required_positive}" "${summary}")" || return 1
  done

  local required_zero
  for required_zero in \
    fault_monotonicity_violations_total \
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

old_leader = number("old_leader_id")
new_leader = number("new_leader_id")
if old_leader == new_leader:
    raise SystemExit(f"etcd leader did not change: {old_leader}")
if number("new_raft_term") <= number("old_raft_term"):
    raise SystemExit(
        "new etcd leader did not advance the raft term: "
        f"old={summary['old_raft_term']} new={summary['new_raft_term']}"
    )

started = number("bench_started_at_ms")
fault = number("fault_injected_at_ms")
changed = number("leader_changed_at_ms")
finished = number("bench_finished_at_ms")
if not started < fault < changed < finished:
    raise SystemExit(
        "allocation bench did not span the complete leader-loss window: "
        f"started={started} fault={fault} changed={changed} finished={finished}"
    )

at_leader_change = number("allocations_at_leader_change")
after_bench = number("allocations_after_bench")
if after_bench <= at_leader_change:
    raise SystemExit(
        "allocation stream had no successful response after the new etcd leader was observed: "
        f"at_leader_change={at_leader_change} after_bench={after_bench}"
    )

fault_first = number("fault_first_tso")
fault_last = number("fault_last_tso")
post_first = number("post_first_tso")
post_last = number("post_last_tso")
if not fault_first <= fault_last < post_first <= post_last:
    raise SystemExit(
        "acknowledged TSO ranges overlap or regress across the etcd leader-loss phases: "
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
old_leader_id=11
new_leader_id=12
old_raft_term=2
new_raft_term=3
old_leader_stopped=true
bench_active_before_fault=true
bench_alive_after_leader_change=true
bench_started_at_ms=1000
fault_injected_at_ms=1100
leader_changed_at_ms=1200
bench_finished_at_ms=1300
allocations_at_leader_change=20
allocations_after_bench=30
fault_allocate_requests_total=20
fault_allocate_success_total=19
fault_monotonicity_violations_total=0
fault_first_tso=100
fault_last_tso=118
post_allocate_requests_total=10
post_allocate_success_total=10
post_monotonicity_violations_total=0
post_first_tso=119
post_last_tso=128
EOF
  verify_cluster_leader_loss "${summary}"

  sed 's/new_leader_id=12/new_leader_id=11/' "${summary}" >"${summary}.invalid"
  if verify_cluster_leader_loss "${summary}.invalid" >/dev/null 2>&1; then
    echo "cluster leader-loss verifier accepted an unchanged leader" >&2
    return 1
  fi

  sed 's/leader_changed_at_ms=1200/leader_changed_at_ms=1400/' "${summary}" >"${summary}.invalid"
  if verify_cluster_leader_loss "${summary}.invalid" >/dev/null 2>&1; then
    echo "cluster leader-loss verifier accepted a benchmark that ended before election" >&2
    return 1
  fi

  sed 's/allocations_after_bench=30/allocations_after_bench=20/' "${summary}" >"${summary}.invalid"
  if verify_cluster_leader_loss "${summary}.invalid" >/dev/null 2>&1; then
    echo "cluster leader-loss verifier accepted no post-election allocation progress" >&2
    return 1
  fi

  sed 's/post_first_tso=119/post_first_tso=118/' "${summary}" >"${summary}.invalid"
  if verify_cluster_leader_loss "${summary}.invalid" >/dev/null 2>&1; then
    echo "cluster leader-loss verifier accepted overlapping TSO ranges" >&2
    return 1
  fi

  sed 's/fault_monotonicity_violations_total=0/fault_monotonicity_violations_total=1/' \
    "${summary}" >"${summary}.invalid"
  if verify_cluster_leader_loss "${summary}.invalid" >/dev/null 2>&1; then
    echo "cluster leader-loss verifier accepted a monotonicity violation" >&2
    return 1
  fi

  echo "[cluster-leader-loss] verifier self-test passed"
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
  exit 0
fi

[[ $# -eq 1 ]] || {
  echo "usage: $0 <cluster-leader-loss-summary.txt> | --self-test" >&2
  exit 1
}
verify_cluster_leader_loss "$1"
echo "[cluster-leader-loss] verified $1"
