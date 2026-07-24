#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

verify_dr_monotonicity() {
  local summary=$1
  local before_high_water
  local post_snapshot_high_water
  local persisted_recovery_floor
  local restored_request_replay_tso
  local after_first_tso
  local replay_verified

  [[ -f "${summary}" ]] || {
    echo "missing DR summary: ${summary}" >&2
    return 1
  }

  before_high_water="$(extract_metric "before_high_water" "${summary}")"
  post_snapshot_high_water="$(extract_metric "post_snapshot_high_water" "${summary}")"
  persisted_recovery_floor="$(extract_metric "persisted_recovery_floor" "${summary}")"
  restored_request_replay_tso="$(extract_metric "restored_request_replay_tso" "${summary}")"
  after_first_tso="$(extract_metric "after_first_tso" "${summary}")"
  replay_verified="$(extract_metric "idempotency_replay_verified" "${summary}")"

  local key
  local value
  for key in before_high_water post_snapshot_high_water persisted_recovery_floor \
    restored_request_replay_tso after_first_tso; do
    value="${!key}"
    if ! [[ "${value}" =~ ^[1-9][0-9]*$ ]]; then
      echo "DR metric ${key} must be a positive integer, got '${value}' in ${summary}" >&2
      return 1
    fi
  done
  if [[ "${replay_verified}" != "true" ]]; then
    echo "DR idempotency replay was not verified in ${summary}" >&2
    return 1
  fi

  python3 - "${before_high_water}" "${post_snapshot_high_water}" \
    "${persisted_recovery_floor}" "${restored_request_replay_tso}" \
    "${after_first_tso}" <<'PY'
import sys

before, post_snapshot, recovery_floor, restored_replay, after = map(int, sys.argv[1:])
if not before < post_snapshot:
    raise SystemExit(
        f"post-snapshot allocation did not advance: before={before} post_snapshot={post_snapshot}"
    )
if before > recovery_floor:
    raise SystemExit(
        f"snapshotted recovery floor did not cover the pre-snapshot allocation: "
        f"before={before} recovery_floor={recovery_floor}"
    )
if restored_replay != before:
    raise SystemExit(
        f"restored request-id replay changed allocation: "
        f"before={before} restored_replay={restored_replay}"
    )
required_floor = max(recovery_floor, post_snapshot)
if not required_floor < after:
    raise SystemExit(
        f"post-restore allocation did not clear all acknowledged high-water marks: "
        f"required_floor={required_floor} after={after}"
    )
PY
}

self_test() {
  local test_dir
  local summary
  test_dir="$(mktemp -d)"
  summary="${test_dir}/summary.txt"
  trap 'rm -rf "${test_dir}"' RETURN

  cat >"${summary}" <<'EOF'
before_high_water=100
post_snapshot_high_water=201
persisted_recovery_floor=200
restored_request_replay_tso=100
after_first_tso=202
idempotency_replay_verified=true
EOF
  verify_dr_monotonicity "${summary}"

  sed 's/after_first_tso=202/after_first_tso=201/' "${summary}" >"${summary}.invalid"
  if verify_dr_monotonicity "${summary}.invalid" >/dev/null 2>&1; then
    echo "DR verifier accepted a post-restore allocation at an acknowledged high-water mark" >&2
    return 1
  fi

  sed 's/persisted_recovery_floor=200/persisted_recovery_floor=99/' "${summary}" >"${summary}.invalid"
  if verify_dr_monotonicity "${summary}.invalid" >/dev/null 2>&1; then
    echo "DR verifier accepted a snapshot floor below the pre-snapshot allocation" >&2
    return 1
  fi

  sed 's/idempotency_replay_verified=true/idempotency_replay_verified=false/' "${summary}" >"${summary}.invalid"
  if verify_dr_monotonicity "${summary}.invalid" >/dev/null 2>&1; then
    echo "DR verifier accepted missing idempotency evidence" >&2
    return 1
  fi

  sed 's/restored_request_replay_tso=100/restored_request_replay_tso=101/' "${summary}" >"${summary}.invalid"
  if verify_dr_monotonicity "${summary}.invalid" >/dev/null 2>&1; then
    echo "DR verifier accepted a changed restored request-id replay" >&2
    return 1
  fi

  echo "[dr-monotonicity] verifier self-test passed"
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
  exit 0
fi

[[ $# -eq 1 ]] || {
  echo "usage: $0 <restore-summary.txt> | --self-test" >&2
  exit 1
}
verify_dr_monotonicity "$1"
echo "[dr-monotonicity] verified $1"
