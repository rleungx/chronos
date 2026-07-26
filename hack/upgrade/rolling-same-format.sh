#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"
source "${SCRIPT_DIR}/baseline.env"

cd "${REPO_ROOT}"

CURRENT_SHA="$(git rev-parse HEAD)"
BASE_SHA="${CHRONOS_UPGRADE_BASE_REF:-${CHRONOS_UPGRADE_BASE_SHA}}"
BASE_SHA="$(git rev-parse "${BASE_SHA}^{commit}")"
BASE_VERSION="${CHRONOS_UPGRADE_BASE_VERSION}"
BASE_CLUSTER_FORMAT="${CHRONOS_UPGRADE_BASE_CLUSTER_FORMAT}"
BASE_METADATA_SCHEMA="${CHRONOS_UPGRADE_BASE_METADATA_SCHEMA}"
CURRENT_VERSION="$(sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -1)"
CURRENT_CLUSTER_FORMAT="$(sed -n 's/^pub const CURRENT_CLUSTER_FORMAT_VERSION: u32 = \([0-9][0-9]*\);/\1/p' src/metadata/types.rs)"
CURRENT_METADATA_SCHEMA="$(sed -n 's/^pub const CURRENT_METADATA_SCHEMA_VERSION: u32 = \([0-9][0-9]*\);/\1/p' src/metadata/types.rs)"
ARTIFACT_ROOT="${CHRONOS_UPGRADE_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-artifacts}}"
ARTIFACT_DIR="${ARTIFACT_ROOT%/}/rolling-upgrade"
ETCD_ENDPOINTS="${CHRONOS_UPGRADE_ETCD_ENDPOINTS:-127.0.0.1:2379,127.0.0.1:22379,127.0.0.1:32379}"
MAX_SUCCESS_GAP_MS="${CHRONOS_UPGRADE_MAX_SUCCESS_GAP_MS:-3000}"
UNIQUE_SUFFIX="$(date +%s)-$$"
ETCD_PREFIX="${CHRONOS_UPGRADE_ETCD_PREFIX:-/chronos-rolling-upgrade-${UNIQUE_SUFFIX}}"
TIMELINE_KEY="${CHRONOS_UPGRADE_TIMELINE_KEY:-bench.rolling-upgrade-${UNIQUE_SUFFIX}.0}"
PROBE_SCENARIO="${TIMELINE_KEY#bench.}"
PROBE_SCENARIO="${PROBE_SCENARIO%.0}"
PLAN_ID="${CHRONOS_UPGRADE_PLAN_ID:-rolling-upgrade-${UNIQUE_SUFFIX}-workers-3}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
BASE_TARGET_DIR="${CHRONOS_UPGRADE_BASE_TARGET_DIR:-${REPO_ROOT}/target/upgrade-baseline-${BASE_SHA:0:12}}"
CURRENT_BIN_DIR="${CHRONOS_RELEASE_BIN_DIR:-${REPO_ROOT}/target/release}"
BASE_BIN="${BASE_TARGET_DIR}/release/chronos"
CURRENT_BIN="${CURRENT_BIN_DIR}/chronos"
BENCH_BIN="${CURRENT_BIN_DIR}/chronos-upgrade-bench"
PROBE_BIN="${CURRENT_BIN_DIR}/chronos-bench"

SERVICE_ENDPOINTS=(127.0.0.1:52051 127.0.0.1:52052 127.0.0.1:52053)
METRICS_ENDPOINTS=(127.0.0.1:9991 127.0.0.1:9992 127.0.0.1:9993)
WORKER_IDS=(upgrade-worker-0 upgrade-worker-1 upgrade-worker-2)
OLD_INSTANCE_IDS=(upgrade-old-0 upgrade-old-1 upgrade-old-2)
NEW_INSTANCE_IDS=(upgrade-new-0 upgrade-new-1 upgrade-new-2)
ROLLBACK_INSTANCE_IDS=(upgrade-rollback-old-0 upgrade-rollback-old-1 upgrade-rollback-old-2)
OLD_PIDS=("" "" "")
NEW_PIDS=("" "" "")
ROLLBACK_PIDS=("" "" "")
ACTIVE_PIDS=("" "" "")
WORKER_LOGS=(
  "${ARTIFACT_DIR}/worker-0-old.log"
  "${ARTIFACT_DIR}/worker-1-old.log"
  "${ARTIFACT_DIR}/worker-2-old.log"
)
COMMAND_FILE="${ARTIFACT_DIR}/command.txt"
ACK_FILE="${ARTIFACT_DIR}/ack.txt"
IDENTITY_LOG="${ARTIFACT_DIR}/identity-evidence.txt"
BENCH_LOG="${ARTIFACT_DIR}/continuous-allocation.txt"
SUMMARY_LOG="${ARTIFACT_DIR}/summary.txt"
INDEX_LOG="${ARTIFACT_DIR}/artifact-index.txt"
BASE_BUILD_LOG="${ARTIFACT_DIR}/base-build.log"
CURRENT_BUILD_LOG="${ARTIFACT_DIR}/current-build.log"
CURRENT_REQUEST_LOG="${ARTIFACT_DIR}/current-only-request.txt"
ROLLBACK_REPLAY_LOG="${ARTIFACT_DIR}/historical-replay-request.txt"
ROLLBACK_FRESH_LOG="${ARTIFACT_DIR}/historical-fresh-request.txt"
CURRENT_REQUEST_ID="rolling-upgrade-cross-binary-${UNIQUE_SUFFIX}"
ROLLBACK_FRESH_REQUEST_ID="rolling-upgrade-historical-fresh-${UNIQUE_SUFFIX}"
RESULT="failure"
BENCH_PID=""
COMMAND_SEQUENCE=0
BASE_SOURCE=""

mkdir -p "${ARTIFACT_DIR}"
: >"${IDENTITY_LOG}"

extract_source_constant() {
  local revision=$1
  local name=$2
  git show "${revision}:src/metadata/types.rs" |
    sed -n "s/^pub const ${name}: u32 = \\([0-9][0-9]*\\);/\\1/p"
}

extract_source_version() {
  local revision=$1
  git show "${revision}:Cargo.toml" |
    sed -n 's/^version = "\([^"]*\)"/\1/p' |
    head -1
}

process_is_running() {
  local pid=$1
  local state
  state="$(ps -o state= -p "${pid}" 2>/dev/null | tr -d '[:space:]')" || return 1
  [[ -n "${state}" && "${state}" != "Z" ]]
}

stop_process() {
  local pid=$1
  [[ -n "${pid}" ]] || return 0
  if process_is_running "${pid}"; then
    kill "${pid}" 2>/dev/null || true
    for _attempt in $(seq 1 100); do
      process_is_running "${pid}" || break
      sleep 0.1
    done
  fi
  if process_is_running "${pid}"; then
    echo "process ${pid} did not stop within 10s" >&2
    return 1
  fi
  wait "${pid}" 2>/dev/null || true
}

write_summary() {
  cat >"${SUMMARY_LOG}" <<EOF
result=${RESULT}
started_at=${STARTED_AT}
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
base_sha=${BASE_SHA}
current_sha=${CURRENT_SHA}
base_build_source=git_archive
current_build_source=exact_worktree
base_version=${BASE_VERSION}
current_version=${CURRENT_VERSION}
base_cluster_format=${BASE_CLUSTER_FORMAT}
current_cluster_format=${CURRENT_CLUSTER_FORMAT}
base_metadata_schema=${BASE_METADATA_SCHEMA}
current_metadata_schema=${CURRENT_METADATA_SCHEMA}
evidence_contract=same_format_forward_and_rollback_v1
forward_upgrade_covered=true
rollback_covered=true
semver_downgrade_covered=false
worker_count=3
replacement_order=0,1,2
rollback_replacement_order=2,1,0
identity_lease_ttl_ms=1500
base_identity_requested_ttl_seconds=1
current_identity_requested_ttl_seconds=2
old_pid_0=${OLD_PIDS[0]}
old_pid_1=${OLD_PIDS[1]}
old_pid_2=${OLD_PIDS[2]}
new_pid_0=${NEW_PIDS[0]}
new_pid_1=${NEW_PIDS[1]}
new_pid_2=${NEW_PIDS[2]}
rollback_pid_0=${ROLLBACK_PIDS[0]}
rollback_pid_1=${ROLLBACK_PIDS[1]}
rollback_pid_2=${ROLLBACK_PIDS[2]}
old_instance_0=${OLD_INSTANCE_IDS[0]}
old_instance_1=${OLD_INSTANCE_IDS[1]}
old_instance_2=${OLD_INSTANCE_IDS[2]}
new_instance_0=${NEW_INSTANCE_IDS[0]}
new_instance_1=${NEW_INSTANCE_IDS[1]}
new_instance_2=${NEW_INSTANCE_IDS[2]}
rollback_instance_0=${ROLLBACK_INSTANCE_IDS[0]}
rollback_instance_1=${ROLLBACK_INSTANCE_IDS[1]}
rollback_instance_2=${ROLLBACK_INSTANCE_IDS[2]}
etcd_endpoints=${ETCD_ENDPOINTS}
etcd_prefix=${ETCD_PREFIX}
service_endpoints=$(join_by_comma "${SERVICE_ENDPOINTS[@]}")
metrics_endpoints=$(join_by_comma "${METRICS_ENDPOINTS[@]}")
timeline_key=${TIMELINE_KEY}
ownership_plan_id=${PLAN_ID}
max_success_gap_ms=${MAX_SUCCESS_GAP_MS}
bench_log=${BENCH_LOG}
identity_log=${IDENTITY_LOG}
base_build_log=${BASE_BUILD_LOG}
current_build_log=${CURRENT_BUILD_LOG}
current_request_log=${CURRENT_REQUEST_LOG}
historical_replay_log=${ROLLBACK_REPLAY_LOG}
historical_fresh_log=${ROLLBACK_FRESH_LOG}
EOF
}

cleanup() {
  local exit_code=$?
  if [[ -n "${BENCH_PID}" ]] && process_is_running "${BENCH_PID}"; then
    kill "${BENCH_PID}" 2>/dev/null || true
    wait "${BENCH_PID}" 2>/dev/null || true
  fi
  for pid in "${ACTIVE_PIDS[@]}"; do
    stop_process "${pid}" || true
  done
  make etcd-cluster-reset >/dev/null 2>&1 || true
  if [[ -n "${BASE_SOURCE}" && -d "${BASE_SOURCE}" ]]; then
    rm -rf "${BASE_SOURCE}"
  fi
  if [[ ${exit_code} -eq 0 ]]; then
    RESULT="success"
  else
    RESULT="failure"
  fi
  write_summary
  write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
}
trap cleanup EXIT

if [[ "${BASE_SHA}" == "${CURRENT_SHA}" ]]; then
  echo "rolling upgrade requires distinct base and current commits: ${BASE_SHA}" >&2
  exit 1
fi
git merge-base --is-ancestor "${BASE_SHA}" "${CURRENT_SHA}" || {
  echo "upgrade base ${BASE_SHA} must be an ancestor of current ${CURRENT_SHA}" >&2
  exit 1
}
if [[ "${CHRONOS_ALLOW_DIRTY_UPGRADE:-0}" != "1" ]] &&
  [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then
  echo "rolling upgrade evidence requires a clean exact worktree" >&2
  git status --short
  exit 1
fi

ACTUAL_BASE_VERSION="$(extract_source_version "${BASE_SHA}")"
ACTUAL_BASE_CLUSTER_FORMAT="$(extract_source_constant "${BASE_SHA}" CURRENT_CLUSTER_FORMAT_VERSION)"
ACTUAL_BASE_METADATA_SCHEMA="$(extract_source_constant "${BASE_SHA}" CURRENT_METADATA_SCHEMA_VERSION)"
if [[ "${ACTUAL_BASE_VERSION}" != "${BASE_VERSION}" ||
  "${ACTUAL_BASE_CLUSTER_FORMAT}" != "${BASE_CLUSTER_FORMAT}" ||
  "${ACTUAL_BASE_METADATA_SCHEMA}" != "${BASE_METADATA_SCHEMA}" ]]; then
  echo "pinned upgrade baseline metadata does not match ${BASE_SHA}" >&2
  exit 1
fi
if [[ "${BASE_VERSION}" != "${CURRENT_VERSION}" ||
  "${BASE_CLUSTER_FORMAT}" != "${CURRENT_CLUSTER_FORMAT}" ||
  "${BASE_METADATA_SCHEMA}" != "${CURRENT_METADATA_SCHEMA}" ]]; then
  echo "rolling gate only supports same-version, same-format history: base=${BASE_VERSION}/format-${BASE_CLUSTER_FORMAT}/schema-${BASE_METADATA_SCHEMA} current=${CURRENT_VERSION}/format-${CURRENT_CLUSTER_FORMAT}/schema-${CURRENT_METADATA_SCHEMA}" >&2
  exit 1
fi
if [[ "bench.${PROBE_SCENARIO}.0" != "${TIMELINE_KEY}" ]]; then
  echo "rolling timeline key must use the probe-compatible bench.<scenario>.0 form: ${TIMELINE_KEY}" >&2
  exit 1
fi

BASE_SOURCE="$(mktemp -d "${TMPDIR:-/tmp}/chronos-upgrade-base.XXXXXX")"
git archive "${BASE_SHA}" | tar -x -C "${BASE_SOURCE}"
echo "[rolling-upgrade] building historical ${BASE_SHA}"
env \
  CHRONOS_BUILD_COMMIT="${BASE_SHA}" \
  CARGO_TARGET_DIR="${BASE_TARGET_DIR}" \
  cargo build --locked --release --manifest-path "${BASE_SOURCE}/Cargo.toml" --bin chronos \
  >"${BASE_BUILD_LOG}" 2>&1
echo "[rolling-upgrade] building current ${CURRENT_SHA}"
if [[ "${CHRONOS_SKIP_RELEASE_BUILD:-0}" == "1" ]]; then
  [[ -x "${CURRENT_BIN}" && -x "${BENCH_BIN}" && -x "${PROBE_BIN}" ]] || {
    echo "missing packaged current chronos/chronos-upgrade-bench/chronos-bench binaries in ${CURRENT_BIN_DIR}" >&2
    exit 1
  }
  printf 'reused_release_binaries=true\ncurrent_sha=%s\n' "${CURRENT_SHA}" >"${CURRENT_BUILD_LOG}"
else
  env CHRONOS_BUILD_COMMIT="${CURRENT_SHA}" \
    cargo build --locked --release --bin chronos --bin chronos-upgrade-bench --bin chronos-bench \
    >"${CURRENT_BUILD_LOG}" 2>&1
fi

start_worker() {
  local binary=$1
  local idx=$2
  local instance_id=$3
  local log=$4
  env \
    CHRONOS_SECURITY_MODE=dev-insecure \
    CHRONOS_METADATA=etcd \
    CHRONOS_BIND_ADDR="${SERVICE_ENDPOINTS[idx]}" \
    CHRONOS_ADVERTISE_ENDPOINT="${SERVICE_ENDPOINTS[idx]}" \
    CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINTS[idx]}" \
    CHRONOS_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
    CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" \
    CHRONOS_WORKER_ID="${WORKER_IDS[idx]}" \
    CHRONOS_INSTANCE_ID="${instance_id}" \
    CHRONOS_OWNERSHIP_PLAN_ID="${PLAN_ID}" \
    CHRONOS_GENERATOR_OWNERSHIP_MODULO=3 \
    CHRONOS_GENERATOR_OWNERSHIP_REMAINDER="${idx}" \
    CHRONOS_GENERATOR_OWNERSHIP_REMAINDERS="${idx}" \
    CHRONOS_LEASE_TTL_MS=1500 \
    CHRONOS_GENERATOR_LEASE_TTL_MS=3000 \
    CHRONOS_GENERATOR_MAINTENANCE_INTERVAL_MS=100 \
    CHRONOS_SAFETY_GAP_MS=500 \
    CHRONOS_MAX_CLOCK_SKEW_MS=500 \
    "${binary}" >"${log}" 2>&1 &
  ACTIVE_PIDS[idx]=$!
}

set_command() {
  local phase=$1
  local target=$2
  local temp="${COMMAND_FILE}.tmp"
  COMMAND_SEQUENCE=$((COMMAND_SEQUENCE + 1))
  printf '%s|%s|%s\n' "${COMMAND_SEQUENCE}" "${phase}" "${target}" >"${temp}"
  mv "${temp}" "${COMMAND_FILE}"
}

wait_for_ack() {
  local sequence=$1
  for _attempt in $(seq 1 400); do
    if [[ -f "${ACK_FILE}" ]] &&
      [[ "$(extract_metric sequence "${ACK_FILE}")" == "${sequence}" ]] &&
      [[ "$(extract_metric status "${ACK_FILE}")" == "serving" ]]; then
      return 0
    fi
    process_is_running "${BENCH_PID}" || {
      echo "continuous upgrade bench exited before command ${sequence} was acknowledged" >&2
      return 1
    }
    sleep 0.05
  done
  echo "upgrade command ${sequence} was not acknowledged within 20s" >&2
  return 1
}

issue_serving_command() {
  local phase=$1
  local target=$2
  local label=$3
  set_command "${phase}" "${target}"
  wait_for_ack "${COMMAND_SEQUENCE}"
  {
    echo "[${label}]"
    cat "${ACK_FILE}"
  } >>"${IDENTITY_LOG}"
  cp "${ACK_FILE}" "${ARTIFACT_DIR}/ack-${COMMAND_SEQUENCE}-${label}.txt"
}

run_request_probe() {
  local request_id=$1
  local output_log=$2
  env \
    CHRONOS_BENCH_ENDPOINT="http://${SERVICE_ENDPOINTS[0]}" \
    CHRONOS_BENCH_CONTROL_ENDPOINTS="http://${SERVICE_ENDPOINTS[0]},http://${SERVICE_ENDPOINTS[1]},http://${SERVICE_ENDPOINTS[2]}" \
    CHRONOS_BENCH_SCENARIO="${PROBE_SCENARIO}" \
    CHRONOS_BENCH_TIMELINES=1 \
    CHRONOS_BENCH_BATCH=1 \
    CHRONOS_BENCH_IDEMPOTENCY=true \
    CHRONOS_BENCH_PROBE_ONLY=true \
    CHRONOS_BENCH_ROUTE_TO_OWNERS=true \
    CHRONOS_BENCH_PROBE_REQUEST_ID="${request_id}" \
    "${PROBE_BIN}" >"${output_log}"
}

echo "[rolling-upgrade] resetting clustered etcd"
make etcd-cluster-reset >/dev/null
make etcd-cluster-up >/dev/null
wait_for_etcd_cluster 60 1 "http://chronos-etcd-1:2379,http://chronos-etcd-2:2379,http://chronos-etcd-3:2379"

for idx in 0 1 2; do
  echo "[rolling-upgrade] starting historical worker ${idx}"
  start_worker "${BASE_BIN}" "${idx}" "${OLD_INSTANCE_IDS[idx]}" "${WORKER_LOGS[idx]}"
  OLD_PIDS[idx]="${ACTIVE_PIDS[idx]}"
done
for idx in 0 1 2; do
  wait_for_http "http://${METRICS_ENDPOINTS[idx]}/readyz" "historical worker ${idx}" 100 0.1
done

printf '1|old_only|%s\n' "${SERVICE_ENDPOINTS[0]}" >"${COMMAND_FILE}"
COMMAND_SEQUENCE=1
env \
  CHRONOS_UPGRADE_BENCH_ENDPOINTS="$(join_by_comma "${SERVICE_ENDPOINTS[@]}")" \
  CHRONOS_UPGRADE_BENCH_COMMAND_FILE="${COMMAND_FILE}" \
  CHRONOS_UPGRADE_BENCH_ACK_FILE="${ACK_FILE}" \
  CHRONOS_UPGRADE_BENCH_TIMELINE_KEY="${TIMELINE_KEY}" \
  CHRONOS_UPGRADE_BENCH_REQUEST_TIMEOUT_MS=500 \
  CHRONOS_UPGRADE_BENCH_MAX_RUNTIME_SECS=180 \
  "${BENCH_BIN}" >"${BENCH_LOG}" 2>&1 &
BENCH_PID=$!

wait_for_ack 1
{
  echo "[old-worker-0]"
  cat "${ACK_FILE}"
} >>"${IDENTITY_LOG}"
cp "${ACK_FILE}" "${ARTIFACT_DIR}/ack-1-old-worker-0.txt"
issue_serving_command old_only "${SERVICE_ENDPOINTS[1]}" old-worker-1
issue_serving_command old_only "${SERVICE_ENDPOINTS[2]}" old-worker-2

for idx in 0 1 2; do
  echo "[rolling-upgrade] replacing worker ${idx}"
  issue_serving_command "replace_${idx}" - "replace-${idx}-started"
  stop_process "${ACTIVE_PIDS[idx]}"
  ACTIVE_PIDS[idx]=""
  new_log="${ARTIFACT_DIR}/worker-${idx}-new.log"
  WORKER_LOGS[idx]="${new_log}"
  start_worker "${CURRENT_BIN}" "${idx}" "${NEW_INSTANCE_IDS[idx]}" "${new_log}"
  NEW_PIDS[idx]="${ACTIVE_PIDS[idx]}"
  wait_for_http "http://${METRICS_ENDPOINTS[idx]}/readyz" "current worker ${idx}" 100 0.1

  if [[ "${idx}" -eq 0 ]]; then
    next_phase=mixed_1
  elif [[ "${idx}" -eq 1 ]]; then
    next_phase=mixed_2
  else
    next_phase=new_only
  fi
  issue_serving_command "${next_phase}" "${SERVICE_ENDPOINTS[idx]}" "new-worker-${idx}"
  sleep 0.5
done

echo "[rolling-upgrade] recording current-only cross-binary request"
run_request_probe "${CURRENT_REQUEST_ID}" "${CURRENT_REQUEST_LOG}"

for idx in 2 1 0; do
  echo "[rolling-upgrade] rolling worker ${idx} back to historical"
  if [[ "${idx}" -eq 2 ]]; then
    safe_endpoint="${SERVICE_ENDPOINTS[1]}"
  elif [[ "${idx}" -eq 1 ]]; then
    safe_endpoint="${SERVICE_ENDPOINTS[2]}"
  else
    safe_endpoint="${SERVICE_ENDPOINTS[1]}"
  fi
  issue_serving_command \
    "rollback_replace_${idx}" "${safe_endpoint}" "rollback-replace-${idx}-started"
  stop_process "${ACTIVE_PIDS[idx]}"
  ACTIVE_PIDS[idx]=""
  rollback_log="${ARTIFACT_DIR}/worker-${idx}-rollback-old.log"
  WORKER_LOGS[idx]="${rollback_log}"
  start_worker "${BASE_BIN}" "${idx}" "${ROLLBACK_INSTANCE_IDS[idx]}" "${rollback_log}"
  ROLLBACK_PIDS[idx]="${ACTIVE_PIDS[idx]}"
  wait_for_http "http://${METRICS_ENDPOINTS[idx]}/readyz" "rollback historical worker ${idx}" 100 0.1

  if [[ "${idx}" -eq 2 ]]; then
    next_phase=rollback_mixed_1
  elif [[ "${idx}" -eq 1 ]]; then
    next_phase=rollback_mixed_2
  else
    next_phase=old_only_after_rollback
  fi
  issue_serving_command "${next_phase}" "${SERVICE_ENDPOINTS[idx]}" "rollback-old-worker-${idx}"
  sleep 0.5
done

echo "[rolling-upgrade] replaying current request under historical workers"
run_request_probe "${CURRENT_REQUEST_ID}" "${ROLLBACK_REPLAY_LOG}"
echo "[rolling-upgrade] recording fresh historical request"
run_request_probe "${ROLLBACK_FRESH_REQUEST_ID}" "${ROLLBACK_FRESH_LOG}"
FRESH_TSO="$(extract_metric probe_last_tso "${ROLLBACK_FRESH_LOG}")"
[[ "${FRESH_TSO}" =~ ^[0-9]+$ ]] || {
  echo "historical fresh probe did not emit a valid final TSO" >&2
  exit 1
}

set_command stop "${FRESH_TSO}"
wait "${BENCH_PID}"
BENCH_PID=""

RESULT="success"
write_summary
bash "${REPO_ROOT}/hack/verify-rolling-upgrade.sh" "${SUMMARY_LOG}"
echo "[rolling-upgrade] passed ${BASE_SHA} -> ${CURRENT_SHA} -> ${BASE_SHA}"
