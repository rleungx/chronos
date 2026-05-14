#!/usr/bin/env bash

set -euo pipefail

require_positive_integer() {
  local name=$1
  local value=$2
  if ! [[ "${value}" =~ ^[0-9]+$ ]] || [[ "${value}" -eq 0 ]]; then
    echo "${name} must be a positive integer, got: ${value}" >&2
    return 1
  fi
}

OLD_WORKERS="${1:-${CHRONOS_OWNERSHIP_OLD_WORKERS:-1}}"
NEW_WORKERS="${2:-${CHRONOS_OWNERSHIP_NEW_WORKERS:-${CHRONOS_SCALE_WORKERS:-2}}}"
GENERATOR_COUNT="${3:-${CHRONOS_OWNERSHIP_GENERATORS:-256}}"
PLAN_ID="${CHRONOS_OWNERSHIP_PLAN_ID:-planned-${OLD_WORKERS}-to-${NEW_WORKERS}-$(date -u +%Y%m%dT%H%M%SZ)}"

require_positive_integer "old worker count" "${OLD_WORKERS}"
require_positive_integer "new worker count" "${NEW_WORKERS}"
require_positive_integer "generator count" "${GENERATOR_COUNT}"

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

for ((generator_id = 0; generator_id < GENERATOR_COUNT; generator_id++)); do
  old_owner=$((generator_id % OLD_WORKERS))
  new_owner=$((generator_id % NEW_WORKERS))
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
echo "generator_count=${GENERATOR_COUNT}"
echo "ownership_plan_id=${PLAN_ID}"
echo "stable_generators_total=${stable_total}"
echo "moved_generators_total=${moved_total}"
echo "moved_ratio_percent=$(awk -v moved="${moved_total}" -v total="${GENERATOR_COUNT}" 'BEGIN { printf "%.2f", moved * 100 / total }')"
echo "outgoing_generators_by_old_remainder=$(join_counts outgoing_by_old "${OLD_WORKERS}")"
echo "incoming_generators_by_new_remainder=$(join_counts incoming_by_new "${NEW_WORKERS}")"
echo "kubernetes_statefulset_replicas=${NEW_WORKERS}"
echo "kubernetes_pdb_min_available=$((NEW_WORKERS - 1))"
echo "kubernetes_configmap_env=CHRONOS_OWNERSHIP_PLAN_ID=${PLAN_ID},CHRONOS_GENERATOR_OWNERSHIP_MODULO=${NEW_WORKERS}"
echo "rollout_order=update_configmap,update_statefulset_replicas,wait_ready,run_scale_matrix,rebalance_if_needed"
echo "new_worker_env_template_begin"
for ((idx = 0; idx < NEW_WORKERS; idx++)); do
  echo "worker_${idx}=CHRONOS_OWNERSHIP_PLAN_ID=${PLAN_ID} CHRONOS_GENERATOR_OWNERSHIP_MODULO=${NEW_WORKERS} CHRONOS_GENERATOR_OWNERSHIP_REMAINDER=${idx}"
done
echo "new_worker_env_template_end"
