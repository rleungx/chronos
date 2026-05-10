#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

ETCD_ENDPOINTS="${CHRONOS_CHAOS_ETCD_ENDPOINTS:-127.0.0.1:2379}"
SERVICE_ENDPOINT="${CHRONOS_CHAOS_SERVICE_ENDPOINT:-127.0.0.1:50051}"
ADVERTISE_ENDPOINT="${CHRONOS_CHAOS_ADVERTISE_ENDPOINT:-}"
METRICS_ENDPOINT="${CHRONOS_CHAOS_METRICS_ENDPOINT:-127.0.0.1:9898}"
WAIT_ATTEMPTS="${CHRONOS_CHAOS_WAIT_ATTEMPTS:-60}"
WAIT_INTERVAL_SECS="${CHRONOS_CHAOS_WAIT_INTERVAL_SECS:-1}"
IDENTITY_RELEASE_WAIT_ATTEMPTS="${CHRONOS_CHAOS_IDENTITY_RELEASE_WAIT_ATTEMPTS:-80}"
IDENTITY_RELEASE_POLL_INTERVAL_SECS="${CHRONOS_CHAOS_IDENTITY_RELEASE_POLL_INTERVAL_SECS:-0.25}"
UNIQUE_SUFFIX="$(date +%s)-$$"
ETCD_PREFIX="${CHRONOS_CHAOS_ETCD_PREFIX:-/chronos-chaos-${UNIQUE_SUFFIX}}"
WORKER_ID="${CHRONOS_CHAOS_WORKER_ID:-worker-chaos}"
INSTANCE_ID="${CHRONOS_CHAOS_INSTANCE_ID:-${SERVICE_ENDPOINT}}"
BENCH_DURATION_SECS="${CHRONOS_CHAOS_BENCH_DURATION_SECS:-2}"
SAFETY_GAP_MS="${CHRONOS_CHAOS_SAFETY_GAP_MS:-1}"
RECOVERY_REQ_PER_SEC_MIN="${CHRONOS_CHAOS_RECOVERY_REQ_PER_SEC_MIN:-10}"
RECOVERY_LATENCY_P95_US_MAX="${CHRONOS_CHAOS_RECOVERY_LATENCY_P95_US_MAX:-500000}"
RECOVERY_LATENCY_P999_US_MAX="${CHRONOS_CHAOS_RECOVERY_LATENCY_P999_US_MAX:-1000000}"
ARTIFACT_ROOT="${CHRONOS_CHAOS_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-}}"
KEEP_ARTIFACTS_ON_SUCCESS="${CHRONOS_CHAOS_KEEP_ARTIFACTS_ON_SUCCESS:-${CHRONOS_KEEP_ARTIFACTS_ON_SUCCESS:-0}}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ -n "${ARTIFACT_ROOT}" ]]; then
  ARTIFACT_DIR="${ARTIFACT_ROOT%/}/chaos"
  mkdir -p "${ARTIFACT_DIR}"
  KEEP_ARTIFACTS_ON_SUCCESS=1
  CHRONOS_LOG="${ARTIFACT_DIR}/chronos.log"
  RECOVERY_BENCH_LOG="${ARTIFACT_DIR}/recovery-bench.log"
  ETCD_LOG="${ARTIFACT_DIR}/etcd.log"
  DOCKER_PS_LOG="${ARTIFACT_DIR}/docker-ps.txt"
  READYZ_LOG="${ARTIFACT_DIR}/readyz.txt"
  METRICS_LOG="${ARTIFACT_DIR}/metrics.txt"
  SUMMARY_LOG="${ARTIFACT_DIR}/summary.txt"
  INDEX_LOG="${ARTIFACT_DIR}/artifact-index.txt"
else
  ARTIFACT_DIR=""
  CHRONOS_LOG="$(mktemp -t chronos-chaos.XXXXXX.log)"
  RECOVERY_BENCH_LOG="$(mktemp -t chronos-chaos-bench.XXXXXX.log)"
  ETCD_LOG=""
  DOCKER_PS_LOG=""
  READYZ_LOG=""
  METRICS_LOG=""
  SUMMARY_LOG=""
  INDEX_LOG=""
fi

CHRONOS_PID=""
RESULT="failure"
RELEASE_BIN_DIR="${CHRONOS_RELEASE_BIN_DIR:-${REPO_ROOT}/target/release}"
ADVERTISE_ENDPOINT="$(derive_local_advertise_endpoint "${SERVICE_ENDPOINT}" "chronos-chaos" "${ADVERTISE_ENDPOINT}")"

write_summary() {
  [[ -n "${SUMMARY_LOG}" ]] || return 0
  mkdir -p "${ARTIFACT_DIR}"
  cat >"${SUMMARY_LOG}" <<EOF
result=${RESULT}
started_at=${STARTED_AT}
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
host=$(hostname)
etcd_endpoints=${ETCD_ENDPOINTS}
service_endpoint=${SERVICE_ENDPOINT}
metrics_endpoint=${METRICS_ENDPOINT}
bench_duration_secs=${BENCH_DURATION_SECS}
identity_release_wait_attempts=${IDENTITY_RELEASE_WAIT_ATTEMPTS}
identity_release_poll_interval_secs=${IDENTITY_RELEASE_POLL_INTERVAL_SECS}
etcd_prefix=${ETCD_PREFIX}
worker_id=${WORKER_ID}
instance_id=${INSTANCE_ID}
safety_gap_ms=${SAFETY_GAP_MS}
recovery_req_per_sec_min=${RECOVERY_REQ_PER_SEC_MIN}
recovery_latency_p95_us_max=${RECOVERY_LATENCY_P95_US_MAX}
recovery_latency_p999_us_max=${RECOVERY_LATENCY_P999_US_MAX}
artifact_dir=${ARTIFACT_DIR}
artifact_index=${INDEX_LOG}
chronos_log=${CHRONOS_LOG}
recovery_bench_log=${RECOVERY_BENCH_LOG}
EOF
}

capture_diagnostics() {
  [[ -n "${ARTIFACT_DIR}" ]] && mkdir -p "${ARTIFACT_DIR}"
  [[ -n "${DOCKER_PS_LOG}" ]] && docker ps -a >"${DOCKER_PS_LOG}" 2>/dev/null || true
  [[ -n "${ETCD_LOG}" ]] && docker logs chronos-etcd >"${ETCD_LOG}" 2>&1 || true
  [[ -n "${READYZ_LOG}" ]] && curl --max-time 2 -fsS "http://${METRICS_ENDPOINT}/readyz" >"${READYZ_LOG}" 2>&1 || true
  [[ -n "${METRICS_LOG}" ]] && curl --max-time 2 -fsS "http://${METRICS_ENDPOINT}/metrics" >"${METRICS_LOG}" 2>&1 || true
}

cleanup() {
  local exit_code=$?
  capture_diagnostics
  RESULT=$([[ ${exit_code} -eq 0 ]] && echo success || echo failure)
  write_summary
  write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
  if [[ -n "${CHRONOS_PID}" ]] && kill -0 "${CHRONOS_PID}" 2>/dev/null; then
    kill "${CHRONOS_PID}" 2>/dev/null || true
    wait "${CHRONOS_PID}" 2>/dev/null || true
  fi
  make etcd-reset >/dev/null 2>&1 || true
  if [[ ${exit_code} -ne 0 ]]; then
    echo
    echo "[chaos] chronos log: ${CHRONOS_LOG}" >&2
    echo "[chaos] recovery bench log: ${RECOVERY_BENCH_LOG}" >&2
    [[ -n "${ARTIFACT_DIR}" ]] && echo "[chaos] artifacts: ${ARTIFACT_DIR}" >&2
  elif [[ "${KEEP_ARTIFACTS_ON_SUCCESS}" != "1" ]]; then
    rm -f "${CHRONOS_LOG}" "${RECOVERY_BENCH_LOG}"
  fi
}
trap cleanup EXIT

wait_for_degrade_or_exit() {
  for _attempt in $(seq 1 "${WAIT_ATTEMPTS}"); do
    if [[ -n "${CHRONOS_PID}" ]] && ! kill -0 "${CHRONOS_PID}" 2>/dev/null; then
      return 0
    fi
    if ! curl --max-time 2 -fsS "http://${METRICS_ENDPOINT}/readyz" >/dev/null 2>&1; then
      return 0
    fi
    sleep "${WAIT_INTERVAL_SECS}"
  done
  echo "chronos neither degraded nor exited after etcd failure injection" >&2
  return 1
}

wait_for_identity_release() {
  local key="${ETCD_PREFIX}/identity/instances/${INSTANCE_ID}"
  local last_error=""
  local attempt
  for attempt in $(seq 1 "${IDENTITY_RELEASE_WAIT_ATTEMPTS}"); do
    local output
    if output="$(docker exec -e ETCDCTL_API=3 chronos-etcd etcdctl --endpoints="http://${ETCD_ENDPOINTS}" get "${key}" --keys-only 2>&1)"; then
      if [[ -z "${output}" ]]; then
        return 0
      fi
      last_error="identity key still present: ${key}"
    else
      last_error="${output}"
    fi

    if [[ -n "${last_error}" && $((attempt % 4)) -eq 0 ]]; then
      echo "[chaos] waiting for identity release: ${last_error}" >&2
    fi

    sleep "${IDENTITY_RELEASE_POLL_INTERVAL_SECS}"
  done
  if [[ -n "${last_error}" ]]; then
    echo "identity lease key did not expire in time: ${key}; last observation: ${last_error}" >&2
  else
    echo "identity lease key did not expire in time: ${key}" >&2
  fi
  return 1
}

start_chronos() {
  : >"${CHRONOS_LOG}"
  env \
    CHRONOS_SECURITY_MODE=dev-insecure \
    CHRONOS_METADATA=etcd \
    CHRONOS_BIND_ADDR="${SERVICE_ENDPOINT}" \
    CHRONOS_ADVERTISE_ENDPOINT="${ADVERTISE_ENDPOINT}" \
    CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINT}" \
    CHRONOS_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
    CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" \
    CHRONOS_WORKER_ID="${WORKER_ID}" \
    CHRONOS_INSTANCE_ID="${INSTANCE_ID}" \
    CHRONOS_SAFETY_GAP_MS="${SAFETY_GAP_MS}" \
    "${RELEASE_BIN_DIR}/chronos" >"${CHRONOS_LOG}" 2>&1 &
  CHRONOS_PID=$!
  wait_for_http "http://${METRICS_ENDPOINT}/readyz" "chronos readyz" "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"
}

echo "[chaos] resetting etcd"
make etcd-reset >/dev/null
echo "[chaos] starting etcd"
make etcd-up >/dev/null
wait_for_etcd "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"

echo "[chaos] preparing release binaries"
ensure_release_binaries "${RELEASE_BIN_DIR}" chronos chronos-bench

echo "[chaos] starting chronos"
start_chronos

echo "[chaos] injecting etcd failure"
docker stop chronos-etcd >/dev/null
wait_for_degrade_or_exit

echo "[chaos] restoring etcd"
docker start chronos-etcd >/dev/null
wait_for_etcd "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"

if [[ -n "${CHRONOS_PID}" ]] && kill -0 "${CHRONOS_PID}" 2>/dev/null; then
  kill "${CHRONOS_PID}" 2>/dev/null || true
  wait "${CHRONOS_PID}" 2>/dev/null || true
fi

echo "[chaos] waiting for identity lease release (${INSTANCE_ID})"
wait_for_identity_release

echo "[chaos] restarting chronos"
start_chronos

echo "[chaos] running recovery smoke benchmark"
env \
  CHRONOS_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" \
  CHRONOS_BENCH_CONCURRENCY=8 \
  CHRONOS_BENCH_TIMELINES=8 \
  CHRONOS_BENCH_BATCH=1 \
  CHRONOS_BENCH_DURATION_SECS="${BENCH_DURATION_SECS}" \
  CHRONOS_BENCH_WARMUP_SECS=1 \
  "${RELEASE_BIN_DIR}/chronos-bench" | tee "${RECOVERY_BENCH_LOG}"

assert_http_body_equals "http://${METRICS_ENDPOINT}/readyz" "ready"
assert_http_metric_present "http://${METRICS_ENDPOINT}/metrics" '^tso_startup_ready'
assert_metric_at_least "req_per_sec" "${RECOVERY_BENCH_LOG}" "${RECOVERY_REQ_PER_SEC_MIN}"
assert_metric_at_most "latency_p95_us" "${RECOVERY_BENCH_LOG}" "${RECOVERY_LATENCY_P95_US_MAX}"
assert_metric_at_most "latency_p999_us" "${RECOVERY_BENCH_LOG}" "${RECOVERY_LATENCY_P999_US_MAX}"
assert_zero_metric "allocation_failed_total" "${RECOVERY_BENCH_LOG}"
assert_zero_metric "allocation_measured_failed_total" "${RECOVERY_BENCH_LOG}"

RESULT="success"
write_summary
write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
echo "[chaos] success"
