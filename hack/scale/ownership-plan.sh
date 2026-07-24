#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/ownership.sh"

identity_ttl_plan_values() {
  python3 - "$1" "$2" <<'PY'
import sys

U64_MAX = (1 << 64) - 1

def positive_u64(name: str, raw: str) -> int:
    if not raw.isdecimal():
        raise SystemExit(f"{name} must be a positive decimal u64, got: {raw}")
    value = int(raw)
    if value == 0 or value > U64_MAX:
        raise SystemExit(f"{name} must be in 1..={U64_MAX}, got: {raw}")
    return value

configured_ms = positive_u64("lease ttl ms", sys.argv[1])
safety_gap_ms = positive_u64("safety gap ms", sys.argv[2])
request_seconds = (configured_ms // 1000) + (1 if configured_ms % 1000 else 0)
request_ms = request_seconds * 1000
minimum_wait_ms = request_ms + safety_gap_ms
print(request_seconds, request_ms, minimum_wait_ms)
PY
}

if [[ "${1:-}" == "--self-test" ]]; then
  assert_ttl_plan() {
    local configured_ms=$1
    local safety_gap_ms=$2
    local expected=$3
    local actual
    actual="$(identity_ttl_plan_values "${configured_ms}" "${safety_gap_ms}")"
    [[ "${actual}" == "${expected}" ]] || {
      echo "identity TTL plan mismatch for ${configured_ms}: expected '${expected}', got '${actual}'" >&2
      return 1
    }
  }

  assert_ttl_plan 500 500 "1 1000 1500"
  assert_ttl_plan 1500 500 "2 2000 2500"
  assert_ttl_plan 3000 500 "3 3000 3500"
  assert_ttl_plan 18446744073709551615 500 \
    "18446744073709552 18446744073709552000 18446744073709552500"
  if identity_ttl_plan_values 18446744073709551616 500 >/dev/null 2>&1; then
    echo "identity TTL planner accepted a value above u64" >&2
    exit 1
  fi
  echo "[ownership-plan] self-test passed"
  exit 0
fi

OLD_WORKERS="${1:-${CHRONOS_OWNERSHIP_OLD_WORKERS:-1}}"
NEW_WORKERS="${2:-${CHRONOS_OWNERSHIP_NEW_WORKERS:-${CHRONOS_SCALE_WORKERS:-2}}}"
SHARD_COUNT="${3:-${CHRONOS_OWNERSHIP_SHARDS:-${CHRONOS_OWNERSHIP_GENERATORS:-256}}}"
ASSIGNMENT_SEED="${CHRONOS_OWNERSHIP_ASSIGNMENT_SEED:-20260516}"
PLAN_ID="${CHRONOS_OWNERSHIP_PLAN_ID:-planned-${OLD_WORKERS}-to-${NEW_WORKERS}-$(date -u +%Y%m%dT%H%M%SZ)}"
LEASE_TTL_MS="${CHRONOS_LEASE_TTL_MS:-3000}"
SAFETY_GAP_MS="${CHRONOS_SAFETY_GAP_MS:-500}"

require_positive_integer "old worker count" "${OLD_WORKERS}"
require_positive_integer "new worker count" "${NEW_WORKERS}"
require_positive_integer "ownership shard count" "${SHARD_COUNT}"
require_non_negative_integer "assignment seed" "${ASSIGNMENT_SEED}"
TTL_PLAN_VALUES="$(identity_ttl_plan_values "${LEASE_TTL_MS}" "${SAFETY_GAP_MS}")"
read -r IDENTITY_GRANT_REQUEST_TTL_SECONDS IDENTITY_GRANT_REQUEST_TTL_MS \
  MINIMUM_IDENTITY_LEASE_WAIT_MS <<<"${TTL_PLAN_VALUES}"

moved_total=0
stable_total=0
declare -a incoming_by_new=()
declare -a outgoing_by_old=()

for ((idx = 0; idx < NEW_WORKERS; idx++)); do
  incoming_by_new[idx]=0
done
for ((idx = 0; idx < OLD_WORKERS; idx++)); do
  outgoing_by_old[idx]=0
done

for ((generator_id = 0; generator_id < SHARD_COUNT; generator_id++)); do
  old_owner=$(ownership_owner_for_shard "${OLD_WORKERS}" "${generator_id}" "${ASSIGNMENT_SEED}")
  new_owner=$(ownership_owner_for_shard "${NEW_WORKERS}" "${generator_id}" "${ASSIGNMENT_SEED}")
  if [[ "${old_owner}" -eq "${new_owner}" ]]; then
    stable_total=$((stable_total + 1))
  else
    moved_total=$((moved_total + 1))
    outgoing_by_old[old_owner]=$((outgoing_by_old[old_owner] + 1))
    incoming_by_new[new_owner]=$((incoming_by_new[new_owner] + 1))
  fi
done

join_counts() {
  local array_name=$1
  local count=$2
  local joined=""
  local idx
  local value
  for ((idx = 0; idx < count; idx++)); do
    eval "value=\${${array_name}[idx]}"
    if [[ -n "${joined}" ]]; then
      joined+=","
    fi
    joined+="${idx}:${value}"
  done
  printf '%s' "${joined}"
}

echo "old_workers=${OLD_WORKERS}"
echo "new_workers=${NEW_WORKERS}"
echo "ownership_shard_count=${SHARD_COUNT}"
echo "assignment_seed=${ASSIGNMENT_SEED}"
echo "ownership_plan_id=${PLAN_ID}"
echo "stable_shards_total=${stable_total}"
echo "moved_shards_total=${moved_total}"
echo "moved_ratio_percent=$(awk -v moved="${moved_total}" -v total="${SHARD_COUNT}" 'BEGIN { printf "%.2f", moved * 100 / total }')"
echo "outgoing_shards_by_old_worker=$(join_counts outgoing_by_old "${OLD_WORKERS}")"
echo "incoming_shards_by_new_worker=$(join_counts incoming_by_new "${NEW_WORKERS}")"
echo "kubernetes_statefulset_replicas=${NEW_WORKERS}"
echo "kubernetes_pdb_min_available=$((NEW_WORKERS - 1))"
echo "kubernetes_configmap_env=CHRONOS_OWNERSHIP_PLAN_ID=${PLAN_ID},CHRONOS_GENERATOR_OWNERSHIP_MODULO=${SHARD_COUNT},CHRONOS_OWNERSHIP_WORKER_COUNT=${NEW_WORKERS},CHRONOS_OWNERSHIP_ASSIGNMENT_SEED=${ASSIGNMENT_SEED}"
echo "migration_requires_quiesced_ingress=true"
echo "identity_lease_ttl_configured_ms=${LEASE_TTL_MS}"
echo "identity_lease_grant_request_ttl_seconds=${IDENTITY_GRANT_REQUEST_TTL_SECONDS}"
echo "identity_lease_grant_request_ttl_ms=${IDENTITY_GRANT_REQUEST_TTL_MS}"
echo "minimum_identity_lease_wait_ms=${MINIMUM_IDENTITY_LEASE_WAIT_MS}"
echo "activation_requires_identity_prefix_empty=true"
echo "drain_phase=quiesce_ingress,keep_old_ownership_config,scale_statefulset_to_0,wait_identity_lease_expiry"
echo "activation_phase=apply_new_ownership_plan,scale_statefulset_to_${NEW_WORKERS},wait_ready,restore_ingress"
echo "rollout_order=generate_and_archive_plan,quiesce_ingress,scale_statefulset_to_0,wait_identity_lease_expiry,update_configmap,update_statefulset_replicas,wait_ready,restore_ingress,run_scale_matrix,rebalance_if_needed"
echo "new_worker_env_template_begin"
for ((idx = 0; idx < NEW_WORKERS; idx++)); do
  remainders=$(ownership_remainders_for_worker "${NEW_WORKERS}" "${idx}" "${SHARD_COUNT}" "${ASSIGNMENT_SEED}")
  first_remainder="${remainders%%,*}"
  echo "worker_${idx}=CHRONOS_OWNERSHIP_PLAN_ID=${PLAN_ID} CHRONOS_GENERATOR_OWNERSHIP_MODULO=${SHARD_COUNT} CHRONOS_GENERATOR_OWNERSHIP_REMAINDER=${first_remainder} CHRONOS_GENERATOR_OWNERSHIP_REMAINDERS=${remainders}"
done
echo "new_worker_env_template_end"
