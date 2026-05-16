#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/ownership.sh"

OLD_WORKERS="${1:-${CHRONOS_OWNERSHIP_OLD_WORKERS:-1}}"
NEW_WORKERS="${2:-${CHRONOS_OWNERSHIP_NEW_WORKERS:-${CHRONOS_SCALE_WORKERS:-2}}}"
SHARD_COUNT="${3:-${CHRONOS_OWNERSHIP_SHARDS:-${CHRONOS_OWNERSHIP_GENERATORS:-256}}}"
ASSIGNMENT_SEED="${CHRONOS_OWNERSHIP_ASSIGNMENT_SEED:-20260516}"
PLAN_ID="${CHRONOS_OWNERSHIP_PLAN_ID:-planned-${OLD_WORKERS}-to-${NEW_WORKERS}-$(date -u +%Y%m%dT%H%M%SZ)}"

require_positive_integer "old worker count" "${OLD_WORKERS}"
require_positive_integer "new worker count" "${NEW_WORKERS}"
require_positive_integer "ownership shard count" "${SHARD_COUNT}"
require_non_negative_integer "assignment seed" "${ASSIGNMENT_SEED}"

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
echo "rollout_order=update_configmap,update_statefulset_replicas,wait_ready,run_scale_matrix,rebalance_if_needed"
echo "new_worker_env_template_begin"
for ((idx = 0; idx < NEW_WORKERS; idx++)); do
  remainders=$(ownership_remainders_for_worker "${NEW_WORKERS}" "${idx}" "${SHARD_COUNT}" "${ASSIGNMENT_SEED}")
  first_remainder="${remainders%%,*}"
  echo "worker_${idx}=CHRONOS_OWNERSHIP_PLAN_ID=${PLAN_ID} CHRONOS_GENERATOR_OWNERSHIP_MODULO=${SHARD_COUNT} CHRONOS_GENERATOR_OWNERSHIP_REMAINDER=${first_remainder} CHRONOS_GENERATOR_OWNERSHIP_REMAINDERS=${remainders}"
done
echo "new_worker_env_template_end"
