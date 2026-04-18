#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

export CHRONOS_TEST_ETCD_ENDPOINTS="${CHRONOS_TEST_ETCD_ENDPOINTS:-127.0.0.1:2379}"

run() {
  echo
  echo "==> $*"
  "$@"
}

require_local_endpoints_or_override() {
  IFS=',' read -ra endpoints <<<"${CHRONOS_TEST_ETCD_ENDPOINTS}"
  for endpoint in "${endpoints[@]}"; do
    endpoint="${endpoint//[[:space:]]/}"
    endpoint="${endpoint#http://}"
    endpoint="${endpoint#https://}"
    host="${endpoint%%:*}"
    host="${host#[}"
    host="${host%]}"
    case "${host}" in
      127.0.0.1|localhost|::1)
        ;;
      *)
        if [[ "${CHRONOS_LAYER4_ALLOW_REMOTE_ETCD:-0}" != "1" ]]; then
          echo "Refusing to run Layer 4 against non-loopback etcd endpoint '${endpoint}'. Set CHRONOS_LAYER4_ALLOW_REMOTE_ETCD=1 to override." >&2
          exit 1
        fi
        ;;
    esac
  done
}

assert_target_contains_tests() {
  local cargo_args=()
  while [[ $# -gt 0 && "$1" != "--" ]]; do
    cargo_args+=("$1")
    shift
  done
  if [[ $# -eq 0 ]]; then
    echo "assert_target_contains_tests requires '--' before test names" >&2
    exit 1
  fi
  shift
  local list_output
  list_output="$(cargo test "${cargo_args[@]}" -- --ignored --list)"
  for test_name in "$@"; do
    if ! grep -Fq "${test_name}: test" <<<"${list_output}"; then
      echo "Layer 4 required ignored test '${test_name}' is missing from 'cargo test ${cargo_args[*]} -- --ignored --list'" >&2
      exit 1
    fi
  done
}

require_local_endpoints_or_override

assert_target_contains_tests --lib -- \
  etcd_route_watch_shutdown_completes \
  etcd_list_timeline_statuses_supports_authoritative_inventory_scan
assert_target_contains_tests --bin chronos -- \
  etcd_identity_lease_loss_flips_readiness_and_triggers_shutdown \
  etcd_startup_rejects_duplicate_instance_identity \
  etcd_startup_failure_after_identity_lease_revokes_lease
assert_target_contains_tests --test metadata_etcd_compat -- \
  etcd_metadata_cas_semantics_match_memory_path \
  etcd_metadata_create_is_atomic_under_contention \
  etcd_metadata_batch_cas_is_atomic
assert_target_contains_tests --test rpc_semantics -- \
  etcd_list_timeline_statuses_rpc_supports_owner_filtered_planned_drain_inventory \
  etcd_list_timeline_statuses_rpc_paginates_across_pages
assert_target_contains_tests --test timeline_rebalance_and_scaling -- \
  etcd_expired_lease_requires_failover_before_issuing_more_tsos \
  etcd_failover_is_blocked_until_expiry_plus_safety_gap_with_skewed_clocks \
  etcd_failover_recovery_floor_survives_remote_restart_before_first_allocation \
  etcd_multihop_failover_at_capacity_horizon_uses_upper_bound_and_fails_closed \
  etcd_remote_transfer_recovery_floor_uses_max_of_graceful_and_generator_floor
assert_target_contains_tests --test multiprocess_etcd_startup -- \
  etcd_spawned_process_rejects_duplicate_instance_identity \
  etcd_spawned_process_failover_preserves_tso_monotonicity

run cargo test \
  --lib \
  --bin chronos \
  --test metadata_etcd_compat \
  --test rpc_semantics \
  --test timeline_rebalance_and_scaling \
  --test multiprocess_etcd_startup \
  etcd_ -- --ignored --test-threads=1
