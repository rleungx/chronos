#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
IMAGE="${CHRONOS_CONTAINER_SMOKE_IMAGE:-chronos:container-check}"
GRPCURL_IMAGE="${CHRONOS_CONTAINER_SMOKE_GRPCURL_IMAGE:-fullstorydev/grpcurl:v1.9.3}"
READY_TIMEOUT_SECS="${CHRONOS_CONTAINER_SMOKE_READY_TIMEOUT_SECS:-15}"
POLL_INTERVAL_SECS="${CHRONOS_CONTAINER_SMOKE_POLL_INTERVAL_SECS:-1}"
CONTAINER_NAME="chronos-container-smoke-$$"
TMPDIR="$(mktemp -d)"
TLS_VOLUME="chronos-container-smoke-tls-$$"
MODE="${CHRONOS_CONTAINER_SMOKE_MODE:-memory}"
PREFIX="/chronos-container-smoke-$$"
STARTED_ETCD=0

cleanup() {
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  docker volume rm -f "${TLS_VOLUME}" >/dev/null 2>&1 || true
  if [[ "${MODE}" == "etcd" ]]; then
    docker exec chronos-etcd /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 del --prefix "${PREFIX}" >/dev/null 2>&1 || true
    if [[ "${STARTED_ETCD}" == "1" ]]; then
      make etcd-reset >/dev/null 2>&1 || true
    fi
  fi
  rm -rf "${TMPDIR}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cat >"${TMPDIR}/openssl.cnf" <<'EOF'
[ req ]
distinguished_name = req_distinguished_name
prompt = no

[ req_distinguished_name ]
CN = localhost

[ server_ext ]
subjectAltName = @alt_names
extendedKeyUsage = serverAuth

[ client_ext ]
extendedKeyUsage = clientAuth

[ alt_names ]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "${TMPDIR}/ca.key" \
  -out "${TMPDIR}/ca.pem" \
  -subj "/CN=chronos-smoke-ca" >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes \
  -keyout "${TMPDIR}/server.key" \
  -out "${TMPDIR}/server.csr" \
  -config "${TMPDIR}/openssl.cnf" >/dev/null 2>&1
openssl x509 -req -days 1 \
  -in "${TMPDIR}/server.csr" \
  -CA "${TMPDIR}/ca.pem" \
  -CAkey "${TMPDIR}/ca.key" \
  -CAcreateserial \
  -out "${TMPDIR}/server.pem" \
  -extfile "${TMPDIR}/openssl.cnf" \
  -extensions server_ext >/dev/null 2>&1

openssl req -newkey rsa:2048 -nodes \
  -keyout "${TMPDIR}/client.key" \
  -out "${TMPDIR}/client.csr" \
  -subj "/CN=chronos-smoke-client" >/dev/null 2>&1
openssl x509 -req -days 1 \
  -in "${TMPDIR}/client.csr" \
  -CA "${TMPDIR}/ca.pem" \
  -CAkey "${TMPDIR}/ca.key" \
  -CAcreateserial \
  -out "${TMPDIR}/client.pem" \
  -extfile "${TMPDIR}/openssl.cnf" \
  -extensions client_ext >/dev/null 2>&1

docker volume create "${TLS_VOLUME}" >/dev/null
docker run --rm \
  --user 0 \
  --entrypoint /bin/sh \
  -v "${TMPDIR}:/input:ro" \
  -v "${TLS_VOLUME}:/tls" \
  "${IMAGE}" \
  -ec 'cp -R /input/. /tls/ && chown -R 10001:10001 /tls && chmod 0644 /tls/* && chmod 0600 /tls/server.key /tls/ca.key && chmod 0644 /tls/client.key'

CLIENT_CERT_FINGERPRINT="$(openssl x509 -in "${TMPDIR}/client.pem" -noout -fingerprint -sha256 | cut -d= -f2 | tr -d ':')"

docker_run_args=(
  -d
  --name "${CONTAINER_NAME}"
  -v "${TLS_VOLUME}:/tls:ro"
  -e CHRONOS_SECURITY_MODE=required
  -e CHRONOS_BIND_ADDR=0.0.0.0:50051
  -e CHRONOS_ADVERTISE_ENDPOINT=10.0.0.10:50051
  -e CHRONOS_METRICS_BIND_ADDR=0.0.0.0:9898
  -e CHRONOS_GRPC_TLS_CERT_FILE=/tls/server.pem
  -e CHRONOS_GRPC_TLS_KEY_FILE=/tls/server.key
  -e CHRONOS_GRPC_CLIENT_CA_FILE=/tls/ca.pem
  -e CHRONOS_GRPC_CONTROL_CERT_ALLOWLIST="${CLIENT_CERT_FINGERPRINT}"
  -e CHRONOS_GRPC_ROUTE_CERT_ALLOWLIST="${CLIENT_CERT_FINGERPRINT}"
  -e CHRONOS_GRPC_TIMESTAMP_CERT_ALLOWLIST="${CLIENT_CERT_FINGERPRINT}"
  -e CHRONOS_GRPC_STATUS_CERT_ALLOWLIST="${CLIENT_CERT_FINGERPRINT}"
  -e CHRONOS_METRICS_TLS_CERT_FILE=/tls/server.pem
  -e CHRONOS_METRICS_TLS_KEY_FILE=/tls/server.key
  -e CHRONOS_METRICS_CLIENT_CA_FILE=/tls/ca.pem
  -e CHRONOS_GRPC_REQUEST_TIMEOUT_MS=100
  -e CHRONOS_GRPC_MAX_REQUEST_BYTES=1024
  -e CHRONOS_GRPC_MAX_CONCURRENT_REQUESTS=16
)

if [[ "${MODE}" == "memory" ]]; then
  docker_run_args+=(
    -p 127.0.0.1::50051
    -p 127.0.0.1::9898
    -e CHRONOS_METADATA=memory
  )
elif [[ "${MODE}" == "etcd" ]]; then
  if ! make etcd-health >/dev/null 2>&1; then
    make etcd-up >/dev/null
    STARTED_ETCD=1
    source "${SCRIPT_DIR}/lib/common.sh"
    wait_for_etcd 20 1
  fi
  docker_run_args+=(
    --network container:chronos-etcd
    -e CHRONOS_METADATA=etcd
    -e CHRONOS_ETCD_ENDPOINTS=127.0.0.1:2379
    -e CHRONOS_ETCD_PREFIX=${PREFIX}
    -e CHRONOS_WORKER_ID=container-smoke-worker
    -e CHRONOS_SAFETY_GAP_MS=1
  )
else
  echo "unsupported CHRONOS_CONTAINER_SMOKE_MODE: ${MODE}" >&2
  exit 1
fi

docker run "${docker_run_args[@]}" "${IMAGE}" >/dev/null

sleep 1
if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  echo "container exited before readiness probe" >&2
  docker inspect \
    --format 'exit_code={{.State.ExitCode}} oom_killed={{.State.OOMKilled}} error={{.State.Error}}' \
    "${CONTAINER_NAME}" >&2 || true
  docker logs "${CONTAINER_NAME}" >&2 || true
  exit 1
fi

if [[ "${MODE}" == "memory" ]]; then
  GRPC_PORT_LINE="$(docker port "${CONTAINER_NAME}" 50051/tcp)"
  GRPC_HOST_PORT="${GRPC_PORT_LINE##*:}"
  PORT_LINE="$(docker port "${CONTAINER_NAME}" 9898/tcp)"
  HOST_PORT="${PORT_LINE##*:}"
  GRPC_TARGET="127.0.0.1:${GRPC_HOST_PORT}"
  READY_URL="https://127.0.0.1:${HOST_PORT}/readyz"
  METRICS_URL="https://127.0.0.1:${HOST_PORT}/metrics"
  CURL_BASE=(curl --silent --show-error --max-time 2 --cacert "${TMPDIR}/ca.pem" --cert "${TMPDIR}/client.pem" --key "${TMPDIR}/client.key")
else
  GRPC_TARGET="127.0.0.1:50051"
  READY_URL="https://127.0.0.1:9898/readyz"
  METRICS_URL="https://127.0.0.1:9898/metrics"
  CURL_BASE=(docker run --rm --network "container:${CONTAINER_NAME}" -v "${TLS_VOLUME}:/tls:ro" curlimages/curl:8.14.1 --silent --show-error --max-time 2 --cacert /tls/ca.pem --cert /tls/client.pem --key /tls/client.key)
fi

grpc_reachable() {
  if [[ "${MODE}" == "memory" ]]; then
    python3 - <<'PY' "${GRPC_HOST_PORT}"
import socket
import sys

sock = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=2)
sock.close()
PY
  else
    docker run --rm \
      --network "container:${CONTAINER_NAME}" \
      python:3.12-alpine \
      python - <<'PY' "${GRPC_TARGET}"
import socket
import sys

host, port = sys.argv[1].rsplit(":", 1)
sock = socket.create_connection((host, int(port)), timeout=2)
sock.close()
PY
  fi
}

grpcurl_call() {
  local data=$1
  local method=$2
  docker run --rm \
    --network "container:${CONTAINER_NAME}" \
    -v "${TLS_VOLUME}:/tls:ro" \
    -v "${REPO_ROOT}:/proto:ro" \
    "${GRPCURL_IMAGE}" \
    -cacert /tls/ca.pem \
    -cert /tls/client.pem \
    -key /tls/client.key \
    -import-path /proto \
    -proto tso.proto \
    -d "${data}" \
    127.0.0.1:50051 \
    "${method}"
}

assert_grpc_mtls_services() {
  local ensure_response
  local route_fields
  local epoch
  local route_version

  grpcurl_call '{}' chronos.tso.v1.TimelineControlService/Health >/dev/null
  ensure_response="$(
    grpcurl_call \
      '{"timelineKey":"container.smoke","desiredResourceTier":"RESOURCE_TIER_SHARED"}' \
      chronos.tso.v1.TimelineRouteService/EnsureTimeline
  )"
  route_fields="$(
    python3 -c 'import json, sys; data=json.load(sys.stdin); route=data["route"]; print(route["epoch"], route["routeVersion"])' \
      <<<"${ensure_response}"
  )"
  read -r epoch route_version <<<"${route_fields}"
  grpcurl_call \
    "{\"timelineKey\":\"container.smoke\",\"count\":1,\"expectedEpoch\":\"${epoch}\",\"expectedRouteVersion\":\"${route_version}\",\"requestTimeoutMs\":100}" \
    chronos.tso.v1.TimestampService/AllocateTimestamps >/dev/null
  grpcurl_call \
    '{"timelineKey":"container.smoke"}' \
    chronos.tso.v1.TimelineStatusService/GetTimelineStatus >/dev/null
}

deadline=$((SECONDS + READY_TIMEOUT_SECS))
while (( SECONDS < deadline )); do
  if READY_BODY="$(${CURL_BASE[@]} "${READY_URL}" 2>/dev/null)"; then
    if [[ "${READY_BODY}" == "ready" ]]; then
      METRICS_BODY="$(${CURL_BASE[@]} "${METRICS_URL}")"
      if grep -q '^tso_startup_ready 1$' <<<"${METRICS_BODY}"; then
        if ! grpc_reachable; then
          echo "gRPC listener was not reachable at ${GRPC_TARGET}" >&2
          exit 1
        fi
        assert_grpc_mtls_services
        if [[ "${MODE}" == "etcd" ]]; then
          lease_deadline=$((SECONDS + 5))
          while (( SECONDS < lease_deadline )); do
            if docker exec chronos-etcd /usr/local/bin/etcdctl --endpoints=http://127.0.0.1:2379 get --prefix "${PREFIX}" | grep -q "${PREFIX}/identity/instances/"; then
              exit 0
            fi
            sleep 1
          done
          echo "etcd-backed smoke did not write expected identity lease keys under ${PREFIX}" >&2
          exit 1
        fi
        exit 0
      fi
    fi
  fi
  sleep "${POLL_INTERVAL_SECS}"
done

echo "container startup smoke failed for image ${IMAGE}" >&2
echo "readyz url: ${READY_URL}" >&2
echo "metrics url: ${METRICS_URL}" >&2
docker logs "${CONTAINER_NAME}" >&2 || true
exit 1
