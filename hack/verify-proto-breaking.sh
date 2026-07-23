#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
if [[ -n "${CHRONOS_PROTO_BASE_REF:-}" ]]; then
  BASE_REF="${CHRONOS_PROTO_BASE_REF}"
elif [[ "${GITHUB_EVENT_NAME:-}" == "push" ]] && git cat-file -e HEAD^:tso.proto 2>/dev/null; then
  BASE_REF="HEAD^"
elif [[ -n "${GITHUB_BASE_REF:-}" ]]; then
  BASE_REF="origin/${GITHUB_BASE_REF}"
else
  BASE_REF="origin/main"
fi

cd "${REPO_ROOT}"
if ! git cat-file -e "${BASE_REF}:tso.proto" 2>/dev/null; then
  echo "protobuf compatibility baseline is unavailable: ${BASE_REF}:tso.proto" >&2
  exit 1
fi
if ! command -v protoc >/dev/null 2>&1; then
  echo "protoc is required for protobuf compatibility verification" >&2
  exit 1
fi

work_dir="$(mktemp -d -t chronos-proto-compat.XXXXXX)"
cleanup() {
  rm -rf "${work_dir}"
}
trap cleanup EXIT

mkdir -p "${work_dir}/baseline"
git show "${BASE_REF}:tso.proto" >"${work_dir}/baseline/tso.proto"
protoc \
  --proto_path="${work_dir}/baseline" \
  --descriptor_set_out="${work_dir}/baseline.pb" \
  "${work_dir}/baseline/tso.proto"
protoc \
  --proto_path="${REPO_ROOT}" \
  --descriptor_set_out="${work_dir}/current.pb" \
  "${REPO_ROOT}/tso.proto"

(
  cd clients/go
  go run ../../hack/proto-compat/main.go "${work_dir}/baseline.pb" "${work_dir}/current.pb"
)

echo "[proto-breaking] tso.proto is backward compatible with ${BASE_REF}"
