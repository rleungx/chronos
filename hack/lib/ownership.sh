#!/usr/bin/env bash

OWNERSHIP_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${OWNERSHIP_LIB_DIR}/common.sh"

ownership_owner_for_shard() {
  local worker_count=$1
  local shard=$2
  local assignment_seed=$3
  local best_worker=0
  local best_score=-1
  local worker
  local mixed
  local score

  for ((worker = 0; worker < worker_count; worker++)); do
    mixed=$((((((shard + 1) * 1103515245) % 2147483647) + (((worker + 1) * 12345) % 2147483647) + (assignment_seed % 2147483647)) % 2147483647))
    mixed=$(((mixed ^ (mixed >> 16)) & 2147483647))
    mixed=$((((mixed * 1103515245) + 12345) % 2147483647))
    mixed=$(((mixed ^ (mixed >> 11)) & 2147483647))
    score=$((((mixed * 1103515245) + 12345 + (worker * 97)) % 2147483647))
    if [[ "${score}" -gt "${best_score}" ]]; then
      best_score="${score}"
      best_worker="${worker}"
    fi
  done

  printf '%s' "${best_worker}"
}

ownership_remainders_for_worker() {
  local worker_count=$1
  local target_worker=$2
  local shard_count=$3
  local assignment_seed=$4
  local joined=""
  local shard
  local owner

  for ((shard = 0; shard < shard_count; shard++)); do
    owner="$(ownership_owner_for_shard "${worker_count}" "${shard}" "${assignment_seed}")"
    if [[ "${owner}" -eq "${target_worker}" ]]; then
      if [[ -n "${joined}" ]]; then
        joined+=","
      fi
      joined+="${shard}"
    fi
  done

  printf '%s' "${joined}"
}
