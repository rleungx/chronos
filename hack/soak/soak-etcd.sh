#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"

ETCD_ENDPOINTS="${CHRONOS_SOAK_ETCD_ENDPOINTS:-127.0.0.1:2379}"
SERVICE_ENDPOINT="${CHRONOS_SOAK_SERVICE_ENDPOINT:-127.0.0.1:50051}"
METRICS_ENDPOINT="${CHRONOS_SOAK_METRICS_ENDPOINT:-127.0.0.1:9898}"
SOAK_DURATION_SECS="${CHRONOS_SOAK_DURATION_SECS:-15}"
SOAK_WARMUP_SECS="${CHRONOS_SOAK_WARMUP_SECS:-3}"
SOAK_CONCURRENCY="${CHRONOS_SOAK_CONCURRENCY:-32}"
SOAK_TIMELINES="${CHRONOS_SOAK_TIMELINES:-32}"
SOAK_BATCH="${CHRONOS_SOAK_BATCH:-1}"
CONTROL_TIMELINES="${CHRONOS_SOAK_CONTROL_TIMELINES:-2000}"
CONTROL_PAGE_SIZE="${CHRONOS_SOAK_CONTROL_PAGE_SIZE:-200}"
CONTROL_CONCURRENCY="${CHRONOS_SOAK_CONTROL_CONCURRENCY:-8}"
WATCH_CONCURRENCY="${CHRONOS_SOAK_WATCH_CONCURRENCY:-4}"
WATCH_TIMEOUT_MS="${CHRONOS_SOAK_WATCH_TIMEOUT_MS:-5000}"
WAIT_ATTEMPTS="${CHRONOS_SOAK_WAIT_ATTEMPTS:-60}"
WAIT_INTERVAL_SECS="${CHRONOS_SOAK_WAIT_INTERVAL_SECS:-1}"
UNIQUE_SUFFIX="$(date +%s)-$$"
ETCD_PREFIX="${CHRONOS_SOAK_ETCD_PREFIX:-/chronos-soak-${UNIQUE_SUFFIX}}"
WORKER_ID="${CHRONOS_SOAK_WORKER_ID:-worker-soak}"
SAFETY_GAP_MS="${CHRONOS_SOAK_SAFETY_GAP_MS:-1}"
ARTIFACT_ROOT="${CHRONOS_SOAK_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-}}"
KEEP_ARTIFACTS_ON_SUCCESS="${CHRONOS_SOAK_KEEP_ARTIFACTS_ON_SUCCESS:-${CHRONOS_KEEP_ARTIFACTS_ON_SUCCESS:-0}}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ -n "${ARTIFACT_ROOT}" ]]; then
  ARTIFACT_DIR="${ARTIFACT_ROOT%/}/soak"
  mkdir -p "${ARTIFACT_DIR}"
  KEEP_ARTIFACTS_ON_SUCCESS=1
  CHRONOS_LOG="${ARTIFACT_DIR}/chronos.log"
  BENCH_LOG="${ARTIFACT_DIR}/bench.log"
  CONTROL_STATUS_LOG="${ARTIFACT_DIR}/status.log"
  CONTROL_WATCH_LOG="${ARTIFACT_DIR}/watch.log"
  ETCD_LOG="${ARTIFACT_DIR}/etcd.log"
  DOCKER_PS_LOG="${ARTIFACT_DIR}/docker-ps.txt"
  READYZ_LOG="${ARTIFACT_DIR}/readyz.txt"
  METRICS_LOG="${ARTIFACT_DIR}/metrics.txt"
  SUMMARY_LOG="${ARTIFACT_DIR}/summary.txt"
  INDEX_LOG="${ARTIFACT_DIR}/artifact-index.txt"
else
  ARTIFACT_DIR=""
  CHRONOS_LOG="$(mktemp -t chronos-soak.XXXXXX.log)"
  BENCH_LOG="$(mktemp -t chronos-soak-bench.XXXXXX.log)"
  CONTROL_STATUS_LOG="$(mktemp -t chronos-soak-status.XXXXXX.log)"
  CONTROL_WATCH_LOG="$(mktemp -t chronos-soak-watch.XXXXXX.log)"
  ETCD_LOG=""
  DOCKER_PS_LOG=""
  READYZ_LOG=""
  METRICS_LOG=""
  SUMMARY_LOG=""
  INDEX_LOG=""
fi

CHRONOS_PID=""
RESULT="failure"
RELEASE_BIN_DIR="${REPO_ROOT}/target/release"

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
soak_duration_secs=${SOAK_DURATION_SECS}
soak_warmup_secs=${SOAK_WARMUP_SECS}
soak_concurrency=${SOAK_CONCURRENCY}
soak_timelines=${SOAK_TIMELINES}
soak_batch=${SOAK_BATCH}
control_timelines=${CONTROL_TIMELINES}
control_page_size=${CONTROL_PAGE_SIZE}
control_concurrency=${CONTROL_CONCURRENCY}
watch_concurrency=${WATCH_CONCURRENCY}
watch_timeout_ms=${WATCH_TIMEOUT_MS}
etcd_prefix=${ETCD_PREFIX}
worker_id=${WORKER_ID}
safety_gap_ms=${SAFETY_GAP_MS}
artifact_dir=${ARTIFACT_DIR}
artifact_index=${INDEX_LOG}
chronos_log=${CHRONOS_LOG}
bench_log=${BENCH_LOG}
status_log=${CONTROL_STATUS_LOG}
watch_log=${CONTROL_WATCH_LOG}
EOF
}

write_artifact_index() {
  [[ -n "${INDEX_LOG}" ]] || return 0
  mkdir -p "${ARTIFACT_DIR}"
  python3 - <<'PY' "${ARTIFACT_DIR}" "${INDEX_LOG}"
from pathlib import Path
import sys

artifact_dir = Path(sys.argv[1])
index_path = Path(sys.argv[2])
lines = []
for path in sorted(p for p in artifact_dir.iterdir() if p.is_file()):
    lines.append(f"{path.name}\t{path.stat().st_size}")
index_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
PY
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
  write_artifact_index
  if [[ -n "${CHRONOS_PID}" ]] && kill -0 "${CHRONOS_PID}" 2>/dev/null; then
    kill "${CHRONOS_PID}" 2>/dev/null || true
    wait "${CHRONOS_PID}" 2>/dev/null || true
  fi
  make etcd-reset >/dev/null 2>&1 || true
  if [[ ${exit_code} -ne 0 ]]; then
    echo
    echo "[soak] chronos log: ${CHRONOS_LOG}" >&2
    echo "[soak] bench log: ${BENCH_LOG}" >&2
    echo "[soak] status log: ${CONTROL_STATUS_LOG}" >&2
    echo "[soak] watch log: ${CONTROL_WATCH_LOG}" >&2
    [[ -n "${ARTIFACT_DIR}" ]] && echo "[soak] artifacts: ${ARTIFACT_DIR}" >&2
  elif [[ "${KEEP_ARTIFACTS_ON_SUCCESS}" != "1" ]]; then
    rm -f "${CHRONOS_LOG}" "${BENCH_LOG}" "${CONTROL_STATUS_LOG}" "${CONTROL_WATCH_LOG}"
  fi
}
trap cleanup EXIT

wait_for_http() {
  local url=$1
  local name=$2
  for _attempt in $(seq 1 "${WAIT_ATTEMPTS}"); do
    if curl --max-time 2 -fsS "${url}" >/dev/null; then
      return 0
    fi
    sleep "${WAIT_INTERVAL_SECS}"
  done
  echo "${name} did not become healthy: ${url}" >&2
  return 1
}

extract_metric() {
  local key=$1
  local file=$2
  awk -F '=' -v key="${key}" '$1 == key { print $2; exit }' "${file}"
}

assert_positive_metric() {
  local key=$1
  local file=$2
  local value
  value="$(extract_metric "${key}" "${file}")"
  if [[ -z "${value}" ]]; then
    echo "missing metric ${key} in ${file}" >&2
    return 1
  fi
  python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) > 0 else 1)' "${value}" || {
    echo "metric ${key} must be > 0, got ${value}" >&2
    return 1
  }
}

echo "[soak] resetting etcd"
make etcd-reset >/dev/null
echo "[soak] starting etcd"
make etcd-up >/dev/null
make etcd-health >/dev/null

echo "[soak] building release binaries"
cargo build --locked --release --bin chronos --bin chronos-bench --bin chronos-control-bench >/dev/null

echo "[soak] starting chronos (release, etcd-backed)"
env \
  CHRONOS_SECURITY_MODE=dev-insecure \
  CHRONOS_METADATA=etcd \
  CHRONOS_BIND_ADDR="${SERVICE_ENDPOINT}" \
  CHRONOS_ADVERTISE_ENDPOINT="${SERVICE_ENDPOINT}" \
  CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINT}" \
  CHRONOS_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
  CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" \
  CHRONOS_WORKER_ID="${WORKER_ID}" \
  CHRONOS_SAFETY_GAP_MS="${SAFETY_GAP_MS}" \
  "${RELEASE_BIN_DIR}/chronos" >"${CHRONOS_LOG}" 2>&1 &
CHRONOS_PID=$!

wait_for_http "http://${METRICS_ENDPOINT}/readyz" "chronos readyz"

echo "[soak] running chronos-bench"
env \
  CHRONOS_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" \
  CHRONOS_BENCH_CONCURRENCY="${SOAK_CONCURRENCY}" \
  CHRONOS_BENCH_TIMELINES="${SOAK_TIMELINES}" \
  CHRONOS_BENCH_BATCH="${SOAK_BATCH}" \
  CHRONOS_BENCH_DURATION_SECS="${SOAK_DURATION_SECS}" \
  CHRONOS_BENCH_WARMUP_SECS="${SOAK_WARMUP_SECS}" \
  "${RELEASE_BIN_DIR}/chronos-bench" | tee "${BENCH_LOG}"

echo "[soak] running control-plane status benchmark"
env \
  CHRONOS_CONTROL_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" \
  CHRONOS_CONTROL_BENCH_SCENARIO=status_scan \
  CHRONOS_CONTROL_BENCH_CONCURRENCY="${CONTROL_CONCURRENCY}" \
  CHRONOS_CONTROL_BENCH_TIMELINES="${CONTROL_TIMELINES}" \
  CHRONOS_CONTROL_BENCH_PAGE_SIZE="${CONTROL_PAGE_SIZE}" \
  CHRONOS_CONTROL_BENCH_DURATION_SECS="${SOAK_DURATION_SECS}" \
  CHRONOS_CONTROL_BENCH_WARMUP_SECS="${SOAK_WARMUP_SECS}" \
  "${RELEASE_BIN_DIR}/chronos-control-bench" | tee "${CONTROL_STATUS_LOG}"

echo "[soak] running control-plane watch benchmark"
env \
  CHRONOS_CONTROL_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" \
  CHRONOS_CONTROL_BENCH_SCENARIO=watch_all_snapshot \
  CHRONOS_CONTROL_BENCH_CONCURRENCY="${WATCH_CONCURRENCY}" \
  CHRONOS_CONTROL_BENCH_TIMELINES="${CONTROL_TIMELINES}" \
  CHRONOS_CONTROL_BENCH_DURATION_SECS="${SOAK_DURATION_SECS}" \
  CHRONOS_CONTROL_BENCH_WARMUP_SECS="${SOAK_WARMUP_SECS}" \
  CHRONOS_CONTROL_BENCH_WATCH_SNAPSHOT_TIMEOUT_MS="${WATCH_TIMEOUT_MS}" \
  "${RELEASE_BIN_DIR}/chronos-control-bench" | tee "${CONTROL_WATCH_LOG}"

echo "[soak] validating readiness and metrics surfaces"
curl -fsS "http://${METRICS_ENDPOINT}/readyz" | grep -qx 'ready'
curl -fsS "http://${METRICS_ENDPOINT}/metrics" | grep -q '^tso_build_info'
curl -fsS "http://${METRICS_ENDPOINT}/metrics" | grep -q '^tso_startup_ready'
curl -fsS "http://${METRICS_ENDPOINT}/metrics" | grep -q '^tso_allocate_total'

assert_positive_metric "req_per_sec" "${BENCH_LOG}"
assert_positive_metric "req_per_sec" "${CONTROL_STATUS_LOG}"
grep -qx 'timeouts=0' "${CONTROL_WATCH_LOG}"
grep -qx 'stream_errors=0' "${CONTROL_WATCH_LOG}"

RESULT="success"
write_summary
write_artifact_index
echo "[soak] success"
