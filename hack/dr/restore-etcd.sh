#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

ETCD_ENDPOINTS="${CHRONOS_RESTORE_ETCD_ENDPOINTS:-127.0.0.1:2379}"
SERVICE_ENDPOINT="${CHRONOS_RESTORE_SERVICE_ENDPOINT:-127.0.0.1:50051}"
ADVERTISE_ENDPOINT="${CHRONOS_RESTORE_ADVERTISE_ENDPOINT:-}"
METRICS_ENDPOINT="${CHRONOS_RESTORE_METRICS_ENDPOINT:-127.0.0.1:9898}"
UNIQUE_SUFFIX="$(date +%s)-$$"
INSTANCE_ID_BEFORE="${CHRONOS_RESTORE_INSTANCE_ID_BEFORE:-chronos-restore-before-${UNIQUE_SUFFIX}}"
INSTANCE_ID_AFTER="${CHRONOS_RESTORE_INSTANCE_ID_AFTER:-chronos-restore-after-${UNIQUE_SUFFIX}}"
ETCD_PREFIX="${CHRONOS_RESTORE_ETCD_PREFIX:-/chronos-restore-${UNIQUE_SUFFIX}}"
WORKER_ID="${CHRONOS_RESTORE_WORKER_ID:-worker-restore}"
ARTIFACT_ROOT="${CHRONOS_RESTORE_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-}}"
KEEP_ARTIFACTS_ON_SUCCESS="${CHRONOS_RESTORE_KEEP_ARTIFACTS_ON_SUCCESS:-${CHRONOS_KEEP_ARTIFACTS_ON_SUCCESS:-0}}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ -n "${ARTIFACT_ROOT}" ]]; then
  ARTIFACT_DIR="${ARTIFACT_ROOT%/}/restore"
  mkdir -p "${ARTIFACT_DIR}"
  KEEP_ARTIFACTS_ON_SUCCESS=1
  CHRONOS_LOG="${ARTIFACT_DIR}/chronos.log"
  CONTROL_LOG="${ARTIFACT_DIR}/restore-control.log"
  SNAPSHOT_FILE="${ARTIFACT_DIR}/snapshot.db"
  SUMMARY_LOG="${ARTIFACT_DIR}/summary.txt"
  INDEX_LOG="${ARTIFACT_DIR}/artifact-index.txt"
else
  ARTIFACT_DIR=""
  CHRONOS_LOG="$(mktemp -t chronos-restore.XXXXXX.log)"
  CONTROL_LOG="$(mktemp -t chronos-restore-control.XXXXXX.log)"
  SNAPSHOT_FILE="$(mktemp -t chronos-restore-snapshot.XXXXXX.db)"
  SUMMARY_LOG=""
  INDEX_LOG=""
fi

RESULT="failure"
CHRONOS_PID=""
RELEASE_BIN_DIR="${CHRONOS_RELEASE_BIN_DIR:-${REPO_ROOT}/target/release}"
ADVERTISE_ENDPOINT="$(derive_local_advertise_endpoint "${SERVICE_ENDPOINT}" "chronos-restore" "${ADVERTISE_ENDPOINT}")"

write_summary() {
  [[ -n "${SUMMARY_LOG}" ]] || return 0
  cat >"${SUMMARY_LOG}" <<EOF
result=${RESULT}
started_at=${STARTED_AT}
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
etcd_endpoints=${ETCD_ENDPOINTS}
service_endpoint=${SERVICE_ENDPOINT}
advertise_endpoint=${ADVERTISE_ENDPOINT}
metrics_endpoint=${METRICS_ENDPOINT}
instance_id_before=${INSTANCE_ID_BEFORE}
instance_id_after=${INSTANCE_ID_AFTER}
etcd_prefix=${ETCD_PREFIX}
worker_id=${WORKER_ID}
snapshot_file=${SNAPSHOT_FILE}
chronos_log=${CHRONOS_LOG}
control_log=${CONTROL_LOG}
EOF
}

cleanup() {
  local exit_code=$?
  RESULT=$([[ ${exit_code} -eq 0 ]] && echo success || echo failure)
  write_summary
  write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
  if [[ -n "${CHRONOS_PID}" ]] && kill -0 "${CHRONOS_PID}" 2>/dev/null; then
    kill "${CHRONOS_PID}" 2>/dev/null || true
    wait "${CHRONOS_PID}" 2>/dev/null || true
  fi
  make etcd-reset >/dev/null 2>&1 || true
  docker rm -f chronos-etcd-restore >/dev/null 2>&1 || true
}
trap cleanup EXIT

make etcd-reset >/dev/null
make etcd-up >/dev/null
wait_for_etcd 60 1

ensure_release_binaries "${RELEASE_BIN_DIR}" chronos chronos-control-bench

env \
  CHRONOS_SECURITY_MODE=dev-insecure \
  CHRONOS_METADATA=etcd \
  CHRONOS_BIND_ADDR="${SERVICE_ENDPOINT}" \
  CHRONOS_ADVERTISE_ENDPOINT="${ADVERTISE_ENDPOINT}" \
  CHRONOS_INSTANCE_ID="${INSTANCE_ID_BEFORE}" \
  CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINT}" \
  CHRONOS_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
  CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" \
  CHRONOS_WORKER_ID="${WORKER_ID}" \
  CHRONOS_SAFETY_GAP_MS=1 \
  "${RELEASE_BIN_DIR}/chronos" >"${CHRONOS_LOG}" 2>&1 &
CHRONOS_PID=$!

wait_for_http "http://${METRICS_ENDPOINT}/readyz" "chronos readyz" 60 1

env \
  CHRONOS_CONTROL_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" \
  CHRONOS_CONTROL_BENCH_NAMESPACE="restore-${UNIQUE_SUFFIX}" \
  CHRONOS_CONTROL_BENCH_SCENARIO=status_scan \
  CHRONOS_CONTROL_BENCH_SEED_TIMELINES=1 \
  CHRONOS_CONTROL_BENCH_TIMELINES=16 \
  CHRONOS_CONTROL_BENCH_CONCURRENCY=4 \
  CHRONOS_CONTROL_BENCH_DURATION_SECS=2 \
  CHRONOS_CONTROL_BENCH_WARMUP_SECS=1 \
  "${RELEASE_BIN_DIR}/chronos-control-bench" | tee "${CONTROL_LOG}"

docker exec -e ETCDCTL_API=3 chronos-etcd etcdctl --endpoints="http://${ETCD_ENDPOINTS}" snapshot save /tmp/restore.db >/dev/null
docker cp chronos-etcd:/tmp/restore.db "${SNAPSHOT_FILE}" >/dev/null
docker exec chronos-etcd etcdutl snapshot status /tmp/restore.db -w table >/dev/null

kill "${CHRONOS_PID}" >/dev/null 2>&1 || true
wait "${CHRONOS_PID}" 2>/dev/null || true
CHRONOS_PID=""
make etcd-reset >/dev/null

RESTORE_DIR="$(mktemp -d)"
docker run --rm -v "${SNAPSHOT_FILE}:/snapshot.db" -v "${RESTORE_DIR}:/restore" quay.io/coreos/etcd:v3.5.15 \
  etcdutl snapshot restore /snapshot.db --data-dir /restore/data >/dev/null

docker run -d --name chronos-etcd-restore \
  -p 3379:2379 -p 3380:2380 \
  -v "${RESTORE_DIR}/data:/etcd-data" \
  quay.io/coreos/etcd:v3.5.15 \
  /usr/local/bin/etcd \
  --name=chronos-etcd-restore \
  --data-dir=/etcd-data \
  --listen-client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://127.0.0.1:3379 \
  --listen-peer-urls=http://0.0.0.0:2380 \
  --initial-advertise-peer-urls=http://127.0.0.1:3380 \
  --initial-cluster=chronos-etcd-restore=http://127.0.0.1:3380 \
  --initial-cluster-state=new >/dev/null

for _ in $(seq 1 60); do
  if docker exec chronos-etcd-restore /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 endpoint health >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

env \
  CHRONOS_SECURITY_MODE=dev-insecure \
  CHRONOS_METADATA=etcd \
  CHRONOS_BIND_ADDR="${SERVICE_ENDPOINT}" \
  CHRONOS_ADVERTISE_ENDPOINT="${ADVERTISE_ENDPOINT}" \
  CHRONOS_INSTANCE_ID="${INSTANCE_ID_AFTER}" \
  CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINT}" \
  CHRONOS_ETCD_ENDPOINTS="127.0.0.1:3379" \
  CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" \
  CHRONOS_WORKER_ID="${WORKER_ID}" \
  CHRONOS_SAFETY_GAP_MS=1 \
  "${RELEASE_BIN_DIR}/chronos" >"${CHRONOS_LOG}" 2>&1 &
CHRONOS_PID=$!

wait_for_http "http://${METRICS_ENDPOINT}/readyz" "chronos readyz after restore" 60 1

env \
  CHRONOS_CONTROL_BENCH_ENDPOINT="http://${SERVICE_ENDPOINT}" \
  CHRONOS_CONTROL_BENCH_NAMESPACE="restore-${UNIQUE_SUFFIX}" \
  CHRONOS_CONTROL_BENCH_SCENARIO=status_scan \
  CHRONOS_CONTROL_BENCH_SEED_TIMELINES=0 \
  CHRONOS_CONTROL_BENCH_TIMELINES=16 \
  CHRONOS_CONTROL_BENCH_CONCURRENCY=4 \
  CHRONOS_CONTROL_BENCH_DURATION_SECS=2 \
  CHRONOS_CONTROL_BENCH_WARMUP_SECS=1 \
  "${RELEASE_BIN_DIR}/chronos-control-bench" | tee -a "${CONTROL_LOG}"

RESULT="success"
write_summary
write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
echo "[restore] success"
