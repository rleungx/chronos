#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

for tool in protoc protoc-gen-go protoc-gen-go-grpc; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "${tool} is required to verify generated Go protobuf sources" >&2
    exit 1
  fi
done

require_version() {
  local tool=$1
  local expected=$2
  local actual
  actual="$(${tool} --version)"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "${tool} version mismatch: expected '${expected}', got '${actual}'" >&2
    exit 1
  fi
}

require_version protoc "libprotoc 34.1"
require_version protoc-gen-go "protoc-gen-go v1.35.2"
require_version protoc-gen-go-grpc "protoc-gen-go-grpc 1.5.1"

generated_dir="$(mktemp -d -t chronos-proto-generated.XXXXXX)"
cleanup() {
  rm -rf "${generated_dir}"
}
trap cleanup EXIT

protoc \
  --proto_path="${REPO_ROOT}" \
  --go_out="${generated_dir}" \
  --go_opt=module=github.com/rleungx/chronos \
  --go-grpc_out="${generated_dir}" \
  --go-grpc_opt=module=github.com/rleungx/chronos \
  "${REPO_ROOT}/tso.proto"

for generated_file in tso.pb.go tso_grpc.pb.go; do
  checked_in="clients/go/gen/proto/tso/v1/${generated_file}"
  regenerated="${generated_dir}/gen/proto/tso/v1/${generated_file}"
  if ! diff -u "${checked_in}" "${regenerated}"; then
    echo "checked-in protobuf source is stale: ${checked_in}" >&2
    echo "regenerate it with the pinned protoc-gen-go and protoc-gen-go-grpc versions" >&2
    exit 1
  fi
done

echo "[proto-generated] checked-in Go protobuf sources match tso.proto"
