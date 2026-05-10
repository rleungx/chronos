#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

ARTIFACT_ROOT="${1:-${CHRONOS_ARTIFACT_DIR:-artifacts}}"
shift || true

if [[ $# -eq 0 ]]; then
  set -- soak chaos failover auto-failover scale-matrix rebalance restore
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
    auto-failover)
      verify_dir auto-failover summary.txt artifact-index.txt failover-bench.log
      ;;
    scale)
      verify_dir scale summary.txt artifact-index.txt chronos-0.log chronos-1.log scale-bench.log profile-summary.txt
      ;;
    scale-matrix)
      matrix_dir="${ARTIFACT_ROOT%/}/scale-matrix"
      [[ -d "${matrix_dir}" ]] || { echo "missing artifact directory: ${matrix_dir}" >&2; exit 1; }
      assert_summary_success "${matrix_dir}/summary.txt"
      [[ -f "${matrix_dir}/artifact-index.txt" ]] || { echo "missing artifact-index.txt in ${matrix_dir}" >&2; exit 1; }
      found_run=0
      for run_dir in "${matrix_dir}"/workers-*/scale; do
        [[ -d "${run_dir}" ]] || continue
        found_run=1
        assert_summary_success "${run_dir}/summary.txt"
        [[ -f "${run_dir}/artifact-index.txt" ]] || { echo "missing artifact-index.txt in ${run_dir}" >&2; exit 1; }
        [[ -f "${run_dir}/profile-summary.txt" ]] || { echo "missing profile-summary.txt in ${run_dir}" >&2; exit 1; }
        [[ -f "${run_dir}/scale-bench.log" ]] || { echo "missing scale-bench.log in ${run_dir}" >&2; exit 1; }
      done
      [[ "${found_run}" -eq 1 ]] || { echo "missing scale matrix run artifacts in ${matrix_dir}" >&2; exit 1; }
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
