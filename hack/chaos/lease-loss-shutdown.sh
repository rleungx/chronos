#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"
cd "${REPO_ROOT}"
ETCD_ENDPOINTS="${CHRONOS_CHAOS_ETCD_ENDPOINTS:-127.0.0.1:2379}"
SERVICE_ENDPOINT="${CHRONOS_CHAOS_SERVICE_ENDPOINT:-127.0.0.1:50051}"
METRICS_ENDPOINT="${CHRONOS_CHAOS_METRICS_ENDPOINT:-127.0.0.1:9898}"
WAIT_ATTEMPTS="${CHRONOS_CHAOS_WAIT_ATTEMPTS:-80}"
WAIT_INTERVAL_SECS="${CHRONOS_CHAOS_WAIT_INTERVAL_SECS:-0.25}"
IDENTITY_WAIT_ATTEMPTS="${CHRONOS_CHAOS_IDENTITY_RELEASE_WAIT_ATTEMPTS:-80}"
IDENTITY_WAIT_SECS="${CHRONOS_CHAOS_IDENTITY_RELEASE_POLL_INTERVAL_SECS:-0.25}"
FAULT_DURATION_SECS="${CHRONOS_CHAOS_FAULT_DURATION_SECS:-30}"
SMOKE_DURATION_SECS="${CHRONOS_CHAOS_BENCH_DURATION_SECS:-30}"
RECOVERY_REQ_PER_SEC_MIN="${CHRONOS_CHAOS_RECOVERY_REQ_PER_SEC_MIN:-10}"
RECOVERY_P95_US_MAX="${CHRONOS_CHAOS_RECOVERY_P95_US_MAX:-500000}"
RECOVERY_P999_US_MAX="${CHRONOS_CHAOS_RECOVERY_P999_US_MAX:-1000000}"
UNIQUE_SUFFIX="$(date +%s)-$$"
ETCD_PREFIX="${CHRONOS_CHAOS_ETCD_PREFIX:-/chronos-chaos-${UNIQUE_SUFFIX}}"
WORKER_ID="${CHRONOS_CHAOS_WORKER_ID:-worker-chaos}"
INSTANCE_ID="${CHRONOS_CHAOS_INSTANCE_ID:-${SERVICE_ENDPOINT}}"
SCENARIO="chaos-${UNIQUE_SUFFIX}"
TIMELINE_KEY="bench.${SCENARIO}.allocate_only.0"
ARTIFACT_ROOT="${CHRONOS_CHAOS_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-${REPO_ROOT}/artifacts}}"
RELEASE_BIN_DIR="${CHRONOS_RELEASE_BIN_DIR:-${REPO_ROOT}/target/release}"
ADVERTISE_ENDPOINT="$(derive_local_advertise_endpoint "${SERVICE_ENDPOINT}" "chronos-chaos" "${CHRONOS_CHAOS_ADVERTISE_ENDPOINT:-}")"
ARTIFACT_DIR="${ARTIFACT_ROOT%/}/chaos"
mkdir -p "${ARTIFACT_DIR}"
INITIAL_LOG="${ARTIFACT_DIR}/initial-chronos.log"
RECOVERY_LOG="${ARTIFACT_DIR}/recovery-chronos.log"
OBSERVATION_LOG="${ARTIFACT_DIR}/degrade-observation.txt"
PRE_PROBE_LOG="${ARTIFACT_DIR}/pre-fault-probe.log"
TRACE_LOG="${ARTIFACT_DIR}/fault-span-trace.jsonl"
FAULT_BENCH_LOG="${ARTIFACT_DIR}/fault-span-bench.log"
POST_PROBE_LOG="${ARTIFACT_DIR}/recovery-probe.log"
SMOKE_LOG="${ARTIFACT_DIR}/recovery-bench.log"
SUMMARY_LOG="${ARTIFACT_DIR}/summary.txt"
INDEX_LOG="${ARTIFACT_DIR}/artifact-index.txt"
RESULT=failure
CHRONOS_PID=""; BENCH_PID=""
PROCESS_EXIT_AT_NS=0; PROCESS_EXIT_STATUS=""
IDENTITY_LOST_AT_NS=0; SHUTDOWN_AT_NS=0; AUTHORITY_BARRIER_AT_NS=0
IDENTITY_RELEASED_AT_NS=0
IDENTITY_KEY="${ETCD_PREFIX}/identity/instances/${INSTANCE_ID}"
IDENTITY_RELEASE_ATTEMPT=0; IDENTITY_RELEASE_STATUS=""; IDENTITY_RELEASE_OUTPUT=""
INITIAL_BUILD_COMMIT=""; RECOVERY_BUILD_COMMIT=""
INITIAL_ACQUIRED_AT_NS=0; INITIAL_READY_AT_NS=0
RECOVERY_ACQUIRED_AT_NS=0; RECOVERY_READY_AT_NS=0
FAULT_INJECTED_AT_NS=0; RESTORE_STARTED_AT_NS=0; ETCD_HEALTHY_AT_NS=0; RECOVERY_STARTED_AT_NS=0
BENCH_STARTED_AT_NS=0; BENCH_FINISHED_AT_NS=0; POST_PROBE_FINISHED_AT_NS=0; SMOKE_FINISHED_AT_NS=0
BENCH_ACTIVE_BEFORE_FAULT=false; BENCH_ALIVE_AFTER_RECOVERY_READY=false
now_ns() { python3 -c 'import time; print(time.time_ns())'; }
process_state_running() { [[ -n "$1" && "$1" != Z ]]; }
process_running() {
  local state; [[ -n "${1:-}" ]] || return 1
  state="$(ps -o state= -p "$1" 2>/dev/null | tr -d '[:space:]')" || return 1; process_state_running "${state}"
}
identity_get_released() { [[ "$1" -eq 0 && -z "$2" ]]; }
log_event_ns() {
  python3 - "$1" "$2" "$3" "${4:-}" "${5:-}" "${6:-}" "${7:-}" <<'PY'
import calendar,datetime,json,re,sys; path,key,value,field,required,worker,instance=sys.argv[1:]
for line in open(path, encoding="utf-8"):
    try: row=json.loads(line)
    except json.JSONDecodeError: continue
    if str(row.get(key,"")) == value:
        if required and (not row.get(required) or row.get("result")!="failure" or row.get("worker_id")!=worker or row.get("instance_id")!=instance): continue
        if field: print(row.get(field,"")); break
        match=re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z",row["timestamp"])
        if not match: continue
        seconds=calendar.timegm(datetime.datetime.strptime(match.group(1),"%Y-%m-%dT%H:%M:%S").timetuple())
        print(seconds*1_000_000_000+int((match.group(2) or "").ljust(9,"0")))
        break
PY
}
metric_or_zero() { [[ -f "$2" ]] && extract_metric "$1" "$2" || echo 0; }
write_observation() {
  cat >"${OBSERVATION_LOG}" <<EOF
process_exit_observed_at_unix_ns=${PROCESS_EXIT_AT_NS}
process_exit_status=${PROCESS_EXIT_STATUS}
identity_lease_lost_at_unix_ns=${IDENTITY_LOST_AT_NS}
shutdown_triggered_at_unix_ns=${SHUTDOWN_AT_NS}
identity_key=${IDENTITY_KEY}
identity_release_attempt=${IDENTITY_RELEASE_ATTEMPT}
identity_release_command_status=${IDENTITY_RELEASE_STATUS}
identity_release_output=${IDENTITY_RELEASE_OUTPUT}
identity_released_at_unix_ns=${IDENTITY_RELEASED_AT_NS}
EOF
}
write_summary() {
  cat >"${SUMMARY_LOG}" <<EOF
result=${RESULT}
evidence_contract_version=2
worker_id=${WORKER_ID}
instance_id=${INSTANCE_ID}
timeline_key=${TIMELINE_KEY}
build_commit=${INITIAL_BUILD_COMMIT}
identity_key=${IDENTITY_KEY}
initial_identity_acquired_at_unix_ns=${INITIAL_ACQUIRED_AT_NS}
initial_ready_at_unix_ns=${INITIAL_READY_AT_NS}
pre_probe_finished_at_unix_ns=$(metric_or_zero pre_probe_finished_at_unix_ns "${PRE_PROBE_LOG}")
allocator_started_at_unix_ns=${BENCH_STARTED_AT_NS}
fault_injected_at_unix_ns=${FAULT_INJECTED_AT_NS}
identity_lease_lost_at_unix_ns=${IDENTITY_LOST_AT_NS}
shutdown_triggered_at_unix_ns=${SHUTDOWN_AT_NS}
authority_barrier_completed_at_unix_ns=${AUTHORITY_BARRIER_AT_NS}
restore_started_at_unix_ns=${RESTORE_STARTED_AT_NS}
etcd_healthy_at_unix_ns=${ETCD_HEALTHY_AT_NS}
identity_released_at_unix_ns=${IDENTITY_RELEASED_AT_NS}
recovery_started_at_unix_ns=${RECOVERY_STARTED_AT_NS}
recovery_identity_acquired_at_unix_ns=${RECOVERY_ACQUIRED_AT_NS}
recovery_ready_at_unix_ns=${RECOVERY_READY_AT_NS}
allocator_finished_at_unix_ns=${BENCH_FINISHED_AT_NS}
post_probe_finished_at_unix_ns=${POST_PROBE_FINISHED_AT_NS}
smoke_finished_at_unix_ns=${SMOKE_FINISHED_AT_NS}
bench_active_before_fault=${BENCH_ACTIVE_BEFORE_FAULT}
bench_alive_after_recovery_ready=${BENCH_ALIVE_AFTER_RECOVERY_READY}
EOF
}
cleanup() {
  local exit_code=$?
  process_running "${BENCH_PID}" && kill "${BENCH_PID}" 2>/dev/null || true; [[ -n "${BENCH_PID}" ]] && wait "${BENCH_PID}" 2>/dev/null || true
  process_running "${CHRONOS_PID}" && kill "${CHRONOS_PID}" 2>/dev/null || true; [[ -n "${CHRONOS_PID}" ]] && wait "${CHRONOS_PID}" 2>/dev/null || true
  make etcd-reset >/dev/null 2>&1 || true
  RESULT=$([[ ${exit_code} -eq 0 ]] && echo success || echo failure)
  write_observation; write_summary; write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
}
trap cleanup EXIT
start_chronos() {
  local log=$1
  : >"${log}"
  env CHRONOS_SECURITY_MODE=dev-insecure CHRONOS_METADATA=etcd \
    CHRONOS_BIND_ADDR="${SERVICE_ENDPOINT}" CHRONOS_ADVERTISE_ENDPOINT="${ADVERTISE_ENDPOINT}" \
    CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINT}" CHRONOS_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
    CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" CHRONOS_WORKER_ID="${WORKER_ID}" \
    CHRONOS_INSTANCE_ID="${INSTANCE_ID}" CHRONOS_SAFETY_GAP_MS=500 \
    "${RELEASE_BIN_DIR}/chronos" >"${log}" 2>&1 &
  CHRONOS_PID=$!
  wait_for_http "http://${METRICS_ENDPOINT}/readyz" "chronos readyz" "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"
}
run_probe() {
  local output=$1
  env CHRONOS_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" \
    CHRONOS_BENCH_SCENARIO="${SCENARIO}.allocate_only" CHRONOS_BENCH_TIMELINES=1 \
    CHRONOS_BENCH_BATCH=1 CHRONOS_BENCH_PROBE_ONLY=true \
    "${RELEASE_BIN_DIR}/chronos-bench" >"${output}"
}
wait_for_authority_loss() {
  for attempt in $(seq 1 "${WAIT_ATTEMPTS}"); do
    if [[ "${PROCESS_EXIT_AT_NS}" -eq 0 ]] && ! process_running "${CHRONOS_PID}"; then
      PROCESS_EXIT_AT_NS="$(now_ns)"
      set +e; wait "${CHRONOS_PID}"; PROCESS_EXIT_STATUS=$?; set -e; CHRONOS_PID=""
    fi
    IDENTITY_LOST_AT_NS="$(log_event_ns "${INITIAL_LOG}" event keepalive_lost "" lease_id "${WORKER_ID}" "${INSTANCE_ID}")"
    SHUTDOWN_AT_NS="$(log_event_ns "${INITIAL_LOG}" shutdown_trigger identity_lease_lost)"
    if [[ -n "${IDENTITY_LOST_AT_NS}" && -n "${SHUTDOWN_AT_NS}" ]]; then
      AUTHORITY_BARRIER_AT_NS="$(now_ns)"; return 0
    fi
    sleep "${WAIT_INTERVAL_SECS}"
  done
  echo "initial process never retained identity-loss and shutdown evidence" >&2
  return 1
}
wait_for_identity_release() {
  local output status last_error=""
  for attempt in $(seq 1 "${IDENTITY_WAIT_ATTEMPTS}"); do
    set +e
    output="$(docker exec -e ETCDCTL_API=3 chronos-etcd etcdctl \
      --endpoints="http://${ETCD_ENDPOINTS}" get "${IDENTITY_KEY}" --keys-only 2>&1)"
    status=$?; set -e
    IDENTITY_RELEASE_ATTEMPT=${attempt}; IDENTITY_RELEASE_STATUS=${status}; IDENTITY_RELEASE_OUTPUT=${output}
    if identity_get_released "${status}" "${output}"; then IDENTITY_RELEASED_AT_NS="$(now_ns)"; return 0; fi
    [[ "${status}" -eq 0 ]] || last_error="${output}"
    sleep "${IDENTITY_WAIT_SECS}"
  done
  echo "identity key did not expire: ${IDENTITY_KEY}; last_error=${last_error}" >&2
  return 1
}
if [[ "${CHRONOS_CHAOS_HELPER_SELF_TEST:-0}" == 1 ]]; then
  process_state_running R; ! process_state_running Z
  identity_get_released 0 ""; ! identity_get_released 1 ""; ! identity_get_released 0 key
  echo "lease-loss producer helper self-test PASS"; exit 0
fi
make etcd-reset >/dev/null
make etcd-up >/dev/null
wait_for_etcd "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"
ensure_release_binaries "${RELEASE_BIN_DIR}" chronos chronos-bench chronos-control-bench
start_chronos "${INITIAL_LOG}"
INITIAL_BUILD_COMMIT="$(log_event_ns "${INITIAL_LOG}" event preflight_passed build_commit)"
INITIAL_ACQUIRED_AT_NS="$(log_event_ns "${INITIAL_LOG}" event acquire_succeeded)"
INITIAL_READY_AT_NS="$(log_event_ns "${INITIAL_LOG}" event ready_state_changed)"
run_probe "${PRE_PROBE_LOG}"
echo "pre_probe_finished_at_unix_ns=$(now_ns)" >>"${PRE_PROBE_LOG}"
BENCH_STARTED_AT_NS="$(now_ns)"
env CHRONOS_CONTROL_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" \
  CHRONOS_CONTROL_BENCH_SCENARIO=allocate_only CHRONOS_CONTROL_BENCH_CONCURRENCY=1 \
  CHRONOS_CONTROL_BENCH_TIMELINES=1 CHRONOS_CONTROL_BENCH_TIMELINE_KEY="${TIMELINE_KEY}" \
  CHRONOS_CONTROL_BENCH_DURATION_SECS="${FAULT_DURATION_SECS}" \
  CHRONOS_CONTROL_BENCH_WARMUP_SECS=0 CHRONOS_CONTROL_BENCH_ALLOCATE_BATCH=1 \
  CHRONOS_CONTROL_BENCH_ALLOCATE_REQUEST_TIMEOUT_MS=500 \
  CHRONOS_CONTROL_BENCH_ALLOCATE_RETRY_ATTEMPTS=1 \
  CHRONOS_CONTROL_BENCH_REQUEST_INTERVAL_MS=25 \
  CHRONOS_CONTROL_BENCH_TRACE_FILE="${TRACE_LOG}" \
  CHRONOS_CONTROL_BENCH_TRACE_MAX_RECORDS=2048 \
  "${RELEASE_BIN_DIR}/chronos-control-bench" >"${FAULT_BENCH_LOG}" 2>&1 &
BENCH_PID=$!
for _attempt in $(seq 1 "${WAIT_ATTEMPTS}"); do
  if grep -q '"logical_outcome":"success"' "${TRACE_LOG}" 2>/dev/null; then
    BENCH_ACTIVE_BEFORE_FAULT=true; break
  fi
  process_running "${BENCH_PID}" || { echo "allocator exited before fault" >&2; exit 1; }
  sleep "${WAIT_INTERVAL_SECS}"
done
[[ "${BENCH_ACTIVE_BEFORE_FAULT}" == true ]] || { echo "allocator inactive before fault" >&2; exit 1; }
FAULT_INJECTED_AT_NS="$(now_ns)"
docker stop chronos-etcd >/dev/null
wait_for_authority_loss
for _attempt in $(seq 1 "${WAIT_ATTEMPTS}"); do
  [[ -z "${CHRONOS_PID}" ]] && break
  if ! process_running "${CHRONOS_PID}"; then
    PROCESS_EXIT_AT_NS="$(now_ns)"
    set +e; wait "${CHRONOS_PID}"; PROCESS_EXIT_STATUS=$?; set -e; CHRONOS_PID=""; break
  fi
  sleep "${WAIT_INTERVAL_SECS}"
done
[[ -z "${CHRONOS_PID}" && "${PROCESS_EXIT_STATUS}" == 0 ]] || {
  echo "initial process did not complete identity-loss shutdown cleanly" >&2; exit 1;
}
RESTORE_STARTED_AT_NS="$(now_ns)"
docker start chronos-etcd >/dev/null
wait_for_etcd "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"
ETCD_HEALTHY_AT_NS="$(now_ns)"
wait_for_identity_release
RECOVERY_STARTED_AT_NS="$(now_ns)"
start_chronos "${RECOVERY_LOG}"
RECOVERY_BUILD_COMMIT="$(log_event_ns "${RECOVERY_LOG}" event preflight_passed build_commit)"
RECOVERY_ACQUIRED_AT_NS="$(log_event_ns "${RECOVERY_LOG}" event acquire_succeeded)"
RECOVERY_READY_AT_NS="$(log_event_ns "${RECOVERY_LOG}" event ready_state_changed)"
if process_running "${BENCH_PID}"; then BENCH_ALIVE_AFTER_RECOVERY_READY=true; else
  echo "allocator did not span recovery ready" >&2; exit 1
fi
wait "${BENCH_PID}"; BENCH_PID=""; BENCH_FINISHED_AT_NS="$(now_ns)"
run_probe "${POST_PROBE_LOG}"; POST_PROBE_FINISHED_AT_NS="$(now_ns)"
echo "post_probe_finished_at_unix_ns=${POST_PROBE_FINISHED_AT_NS}" >>"${POST_PROBE_LOG}"
env CHRONOS_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" CHRONOS_BENCH_CONCURRENCY=8 \
  CHRONOS_BENCH_TIMELINES=8 CHRONOS_BENCH_BATCH=1 \
  CHRONOS_BENCH_DURATION_SECS="${SMOKE_DURATION_SECS}" CHRONOS_BENCH_WARMUP_SECS=1 \
  "${RELEASE_BIN_DIR}/chronos-bench" >"${SMOKE_LOG}"
SMOKE_FINISHED_AT_NS="$(now_ns)"
assert_zero_metric allocation_failed_total "${SMOKE_LOG}"
assert_zero_metric allocation_measured_failed_total "${SMOKE_LOG}"
assert_metric_at_least req_per_sec "${SMOKE_LOG}" "${RECOVERY_REQ_PER_SEC_MIN}"
assert_metric_at_most latency_p95_us "${SMOKE_LOG}" "${RECOVERY_P95_US_MAX}"
assert_metric_at_most latency_p999_us "${SMOKE_LOG}" "${RECOVERY_P999_US_MAX}"
RESULT=success
write_observation
write_summary
bash "${REPO_ROOT}/hack/verify-chaos-lease-loss.sh" "${ARTIFACT_DIR}"
echo "[chaos] success"
