#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

require_file() {
  local file=$1
  [[ -f "${file}" ]] || {
    echo "missing required client file: ${file}" >&2
    return 1
  }
}

require_contains() {
  local file=$1
  local pattern=$2
  if ! grep -Fq "${pattern}" "${file}"; then
    echo "missing client contract pattern in ${file}: ${pattern}" >&2
    return 1
  fi
}

for file in \
  clients/client-contract.md \
  clients/README.md \
  clients/rust/README.md \
  clients/go/README.md \
  clients/java/README.md \
  clients/cpp/README.md \
  src/client.rs \
  clients/go/client.go \
  clients/java/src/main/java/chronos/client/Client.java \
  clients/cpp/client.h \
  clients/cpp/client.cc \
  examples/rust/client_example.rs \
  examples/go/main.go \
  examples/java/ClientExample.java \
  examples/cpp/client_example.cc; do
  require_file "${file}"
done

for readme in clients/README.md clients/rust/README.md clients/go/README.md clients/java/README.md clients/cpp/README.md; do
  require_contains "${readme}" "ensures the bound timeline"
  require_contains "${readme}" "Stale-route errors"
  require_contains "${readme}" "idempotency"
done

require_contains src/client.rs "ClientConfig::new"
require_contains src/client.rs "with_desired_resource_tier"
require_contains src/client.rs "with_request_timeout_ms"
require_contains src/client.rs "with_stale_route_retry_attempts"
require_contains src/client.rs "with_stale_route_retry_backoff_ms"
require_contains src/client.rs "with_idempotency_enabled"
require_contains src/client.rs "with_client_identity_pem"
require_contains src/client.rs "with_domain_name"
require_contains src/client.rs "require_route"
require_contains src/client.rs "is_stale_route_error"
require_contains src/client.rs "ErrorCode::RouteVersionMismatch"

require_contains clients/go/client.go "func NewWithOptions"
require_contains clients/go/client.go "WithDesiredResourceTier"
require_contains clients/go/client.go "WithRequestTimeoutMs"
require_contains clients/go/client.go "WithStaleRouteRetryAttempts"
require_contains clients/go/client.go "WithStaleRouteRetryBackoffMs"
require_contains clients/go/client.go "WithIdempotency"
require_contains clients/go/client.go "WithTLSClientCertificate"
require_contains clients/go/client.go "WithTLSServerName"
require_contains clients/go/client.go "validateRoute"
require_contains clients/go/client.go "isStaleRouteError"
require_contains clients/go/client.go "ERROR_CODE_ROUTE_VERSION_MISMATCH"

require_contains clients/java/src/main/java/chronos/client/Client.java "Config defaults()"
require_contains clients/java/src/main/java/chronos/client/Client.java "withDesiredResourceTier"
require_contains clients/java/src/main/java/chronos/client/Client.java "withRequestTimeoutMs"
require_contains clients/java/src/main/java/chronos/client/Client.java "withStaleRouteRetryAttempts"
require_contains clients/java/src/main/java/chronos/client/Client.java "withStaleRouteRetryBackoffMs"
require_contains clients/java/src/main/java/chronos/client/Client.java "withIdempotency"
require_contains clients/java/src/main/java/chronos/client/Client.java "withClientIdentityPem"
require_contains clients/java/src/main/java/chronos/client/Client.java "withAuthorityOverride"
require_contains clients/java/src/main/java/chronos/client/Client.java "requireRoute"
require_contains clients/java/src/main/java/chronos/client/Client.java "isStaleRouteError"
require_contains clients/java/src/main/java/chronos/client/Client.java "ERROR_CODE_ROUTE_VERSION_MISMATCH"

if grep -Fq "Status.fromThrowable(err).getCode() == Status.Code.FAILED_PRECONDITION" \
  clients/java/src/main/java/chronos/client/Client.java; then
  echo "Java client must not treat every FAILED_PRECONDITION as stale route" >&2
  exit 1
fi

require_contains clients/cpp/client.h "struct Config"
require_contains clients/cpp/client.h "desired_resource_tier"
require_contains clients/cpp/client.h "request_timeout_ms"
require_contains clients/cpp/client.h "stale_route_retry_attempts"
require_contains clients/cpp/client.h "stale_route_retry_backoff_ms"
require_contains clients/cpp/client.h "idempotency_enabled"
require_contains clients/cpp/client.h "TransportConfig"
require_contains clients/cpp/client.h "ssl_target_name_override"
require_contains clients/cpp/client.cc "InstallRouteLocked"
require_contains clients/cpp/client.cc "IsStaleRouteError"
require_contains clients/cpp/client.cc "ERROR_CODE_ROUTE_VERSION_MISMATCH"

for example in examples/rust/client_example.rs examples/go/main.go examples/java/ClientExample.java examples/cpp/client_example.cc; do
  require_contains "${example}" "127.0.0.1:50051"
  require_contains "${example}" "orders.primary"
done

require_contains examples/rust/client_example.rs "allocate_timestamps(1)"
require_contains examples/go/main.go "AllocateTimestamps(ctx, 1)"
require_contains examples/java/ClientExample.java "allocateTimestamps(1)"
require_contains examples/cpp/client_example.cc "AllocateTimestamps(1)"

echo "[client-conformance] ok"
