#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

ARTIFACT_ROOT="${1:-${CHRONOS_ARTIFACT_DIR:-artifacts}}"
shift || true

if [[ $# -eq 0 ]]; then
  set -- soak chaos failover rebalance
fi

verify_dir() {
  local name=$1
  shift
  local dir="${ARTIFACT_ROOT%/}/${name}"
  [[ -d "${dir}" ]] || { echo "missing artifact directory: ${dir}" >&2; return 1; }
  assert_summary_success "${dir}/summary.txt"
  [[ -f "${dir}/artifact-index.txt" ]] || { echo "missing artifact-index.txt in ${dir}" >&2; return 1; }
  for required in "$@"; do
    [[ -f "${dir}/${required}" ]] || { echo "missing ${required} in ${dir}" >&2; return 1; }
  done
}

for target in "$@"; do
  case "${target}" in
    soak)
      verify_dir soak summary.txt artifact-index.txt chronos.log bench.log status.log status-filtered.log
      ;;
    chaos)
      verify_dir chaos summary.txt artifact-index.txt chronos.log recovery-bench.log
      ;;
    failover)
      verify_dir failover summary.txt artifact-index.txt failover-bench.log
      ;;
    rebalance)
      verify_dir rebalance summary.txt artifact-index.txt chronos-a.log chronos-b.log rebalance-bench.log
      ;;
    restore)
      verify_dir restore summary.txt artifact-index.txt chronos.log restore-control.log snapshot.db
      ;;
    *)
      echo "unsupported evidence target: ${target}" >&2
      exit 1
      ;;
  esac
done

echo "[evidence] verified ${ARTIFACT_ROOT}"
