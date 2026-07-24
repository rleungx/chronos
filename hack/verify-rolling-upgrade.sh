#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

verify_rolling_upgrade() {
  local summary=$1
  [[ -f "${summary}" ]] || {
    echo "missing rolling-upgrade summary: ${summary}" >&2
    return 1
  }
  assert_summary_success "${summary}" || return 1

  local bench_log
  local identity_log
  bench_log="$(extract_metric bench_log "${summary}")"
  identity_log="$(extract_metric identity_log "${summary}")"
  [[ -f "${bench_log}" ]] || {
    echo "missing rolling-upgrade bench log: ${bench_log}" >&2
    return 1
  }
  [[ -f "${identity_log}" ]] || {
    echo "missing rolling-upgrade identity log: ${identity_log}" >&2
    return 1
  }
  assert_summary_success "${bench_log}" || return 1

  python3 - "${summary}" "${bench_log}" "${identity_log}" <<'PY'
import re
import sys

PHASES = [
    "old_only",
    "replace_0",
    "mixed_1",
    "replace_1",
    "mixed_2",
    "replace_2",
    "new_only",
]

def metrics(path):
    result = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            key, separator, value = line.rstrip("\n").partition("=")
            if separator:
                result[key] = value
    return result

def number(values, key):
    try:
        return int(values[key])
    except (KeyError, ValueError) as error:
        raise SystemExit(f"{key} must be an integer") from error

summary = metrics(sys.argv[1])
bench = metrics(sys.argv[2])

base_sha = summary.get("base_sha", "")
current_sha = summary.get("current_sha", "")
if not re.fullmatch(r"[0-9a-f]{40}", base_sha) or not re.fullmatch(r"[0-9a-f]{40}", current_sha):
    raise SystemExit("base_sha and current_sha must be exact 40-character git SHAs")
if base_sha == current_sha:
    raise SystemExit("rolling upgrade must use distinct historical and current SHAs")
if summary.get("base_build_source") != "git_archive":
    raise SystemExit("historical binary must be built from the recorded git archive")
if summary.get("current_build_source") != "exact_worktree":
    raise SystemExit("current binary must be built from the exact worktree")
for key in ("version", "cluster_format", "metadata_schema"):
    if summary.get(f"base_{key}") != summary.get(f"current_{key}"):
        raise SystemExit(
            f"rolling gate requires equal {key}: "
            f"base={summary.get(f'base_{key}')} current={summary.get(f'current_{key}')}"
        )
if summary.get("forward_upgrade_only") != "true" or summary.get("rollback_covered") != "false":
    raise SystemExit("evidence must explicitly limit the contract to forward upgrade without rollback")
if number(summary, "worker_count") != 3 or summary.get("replacement_order") != "0,1,2":
    raise SystemExit("rolling gate must replace exactly three workers in order 0,1,2")

for index in range(3):
    old_pid = number(summary, f"old_pid_{index}")
    new_pid = number(summary, f"new_pid_{index}")
    if old_pid <= 0 or new_pid <= 0 or old_pid == new_pid:
        raise SystemExit(
            f"worker {index} must record distinct positive old/new PIDs: "
            f"old={old_pid} new={new_pid}"
        )
    if summary.get(f"old_instance_{index}") == summary.get(f"new_instance_{index}"):
        raise SystemExit(f"worker {index} must use a fresh replacement instance identity")

if number(bench, "command_count") != 9:
    raise SystemExit(f"continuous client must acknowledge 9 serving commands, got {bench.get('command_count')}")
if number(bench, "monotonicity_violations_total") != 0:
    raise SystemExit("continuous client observed duplicate or regressing TSO ranges")
max_gap = number(bench, "global_max_success_gap_ms")
allowed_gap = number(summary, "max_success_gap_ms")
if max_gap > allowed_gap:
    raise SystemExit(
        f"continuous allocation gap exceeded budget: observed={max_gap}ms allowed={allowed_gap}ms"
    )

previous_last = None
for phase in PHASES:
    requests = number(bench, f"{phase}_requests_total")
    success = number(bench, f"{phase}_success_total")
    failed = number(bench, f"{phase}_failed_total")
    first = number(bench, f"{phase}_first_tso")
    last = number(bench, f"{phase}_last_tso")
    if requests != success + failed:
        raise SystemExit(
            f"{phase} request accounting mismatch: requests={requests} success={success} failed={failed}"
        )
    if success <= 0 or first <= 0 or first > last:
        raise SystemExit(
            f"{phase} must contain a non-empty acknowledged range: success={success} range={first}-{last}"
        )
    if previous_last is not None and first <= previous_last:
        raise SystemExit(
            f"phase ranges overlap or regress at {phase}: previous_last={previous_last} first={first}"
        )
    previous_last = last

blocks = {}
current = None
with open(sys.argv[3], encoding="utf-8") as handle:
    for raw in handle:
        line = raw.rstrip("\n")
        if line.startswith("[") and line.endswith("]"):
            current = line[1:-1]
            blocks[current] = {}
        elif current and "=" in line:
            key, value = line.split("=", 1)
            blocks[current][key] = value

expected_order = [
    "old-worker-0",
    "old-worker-1",
    "old-worker-2",
    "replace-0-started",
    "new-worker-0",
    "replace-1-started",
    "new-worker-1",
    "replace-2-started",
    "new-worker-2",
]
if list(blocks) != expected_order:
    raise SystemExit(f"identity evidence order mismatch: observed={list(blocks)}")

last_serving_tso = None
for label in expected_order:
    block = blocks[label]
    serving_tso = number(block, "serving_tso")
    if last_serving_tso is not None and serving_tso <= last_serving_tso:
        raise SystemExit(
            f"serving acknowledgements are not strictly increasing at {label}: "
            f"previous={last_serving_tso} current={serving_tso}"
        )
    last_serving_tso = serving_tso

for generation, expected_commit in (("old", base_sha), ("new", current_sha)):
    for index in range(3):
        block = blocks[f"{generation}-worker-{index}"]
        expected_instance = summary[f"{generation}_instance_{index}"]
        expected_worker = f"upgrade-worker-{index}"
        if block.get("instance_id") != expected_instance:
            raise SystemExit(
                f"{generation} worker {index} identity mismatch: "
                f"expected={expected_instance} observed={block.get('instance_id')}"
            )
        if block.get("worker_id") != expected_worker:
            raise SystemExit(
                f"{generation} worker {index} worker ID mismatch: "
                f"expected={expected_worker} observed={block.get('worker_id')}"
            )
        if block.get("build_commit") != expected_commit:
            raise SystemExit(
                f"{generation} worker {index} build mismatch: "
                f"expected={expected_commit} observed={block.get('build_commit')}"
            )
        if block.get("target_endpoint") != block.get("observed_owner_endpoint"):
            raise SystemExit(
                f"{generation} worker {index} was ready but did not serve the continuous timeline"
            )
PY
}

self_test() {
  local test_dir
  local summary
  local bench
  local identity
  test_dir="$(mktemp -d)"
  summary="${test_dir}/summary.txt"
  bench="${test_dir}/bench.txt"
  identity="${test_dir}/identity.txt"
  trap 'rm -rf "${test_dir}"' RETURN

  cat >"${summary}" <<EOF
result=success
base_sha=1111111111111111111111111111111111111111
current_sha=2222222222222222222222222222222222222222
base_build_source=git_archive
current_build_source=exact_worktree
base_version=0.1.0
current_version=0.1.0
base_cluster_format=2
current_cluster_format=2
base_metadata_schema=1
current_metadata_schema=1
forward_upgrade_only=true
rollback_covered=false
worker_count=3
replacement_order=0,1,2
old_pid_0=101
old_pid_1=102
old_pid_2=103
new_pid_0=201
new_pid_1=202
new_pid_2=203
old_instance_0=upgrade-old-0
old_instance_1=upgrade-old-1
old_instance_2=upgrade-old-2
new_instance_0=upgrade-new-0
new_instance_1=upgrade-new-1
new_instance_2=upgrade-new-2
max_success_gap_ms=3000
bench_log=${bench}
identity_log=${identity}
EOF

  {
    echo "result=success"
    echo "command_count=9"
    echo "monotonicity_violations_total=0"
    echo "global_max_success_gap_ms=120"
    first=100
    for phase in old_only replace_0 mixed_1 replace_1 mixed_2 replace_2 new_only; do
      echo "${phase}_requests_total=10"
      echo "${phase}_success_total=9"
      echo "${phase}_failed_total=1"
      echo "${phase}_first_tso=${first}"
      echo "${phase}_last_tso=$((first + 8))"
      echo "${phase}_max_success_gap_ms=100"
      first=$((first + 9))
    done
  } >"${bench}"

  sequence=0
  serving_tso=90
  for label in \
    old-worker-0 old-worker-1 old-worker-2 replace-0-started new-worker-0 \
    replace-1-started new-worker-1 replace-2-started new-worker-2; do
    sequence=$((sequence + 1))
    serving_tso=$((serving_tso + 1))
    {
      echo "[${label}]"
      echo "sequence=${sequence}"
      echo "status=serving"
      echo "phase=old_only"
      if [[ "${label}" =~ ^(old|new)-worker-([0-2])$ ]]; then
        generation="${BASH_REMATCH[1]}"
        index="${BASH_REMATCH[2]}"
        echo "target_endpoint=127.0.0.1:$((52051 + index))"
        echo "observed_owner_endpoint=127.0.0.1:$((52051 + index))"
        echo "instance_id=upgrade-${generation}-${index}"
        echo "worker_id=upgrade-worker-${index}"
        if [[ "${generation}" == "old" ]]; then
          echo "build_commit=1111111111111111111111111111111111111111"
        else
          echo "build_commit=2222222222222222222222222222222222222222"
        fi
      else
        echo "target_endpoint="
        echo "observed_owner_endpoint=127.0.0.1:52051"
        echo "instance_id="
        echo "worker_id="
        echo "build_commit="
      fi
      echo "serving_tso=${serving_tso}"
    } >>"${identity}"
  done

  verify_rolling_upgrade "${summary}"

  sed 's/current_cluster_format=2/current_cluster_format=3/' "${summary}" >"${summary}.invalid"
  if verify_rolling_upgrade "${summary}.invalid" >/dev/null 2>&1; then
    echo "rolling-upgrade verifier accepted a mixed-format rollout" >&2
    return 1
  fi

  sed 's/global_max_success_gap_ms=120/global_max_success_gap_ms=3001/' \
    "${bench}" >"${bench}.invalid"
  sed "s#bench_log=${bench}#bench_log=${bench}.invalid#" "${summary}" >"${summary}.invalid"
  if verify_rolling_upgrade "${summary}.invalid" >/dev/null 2>&1; then
    echo "rolling-upgrade verifier accepted an excessive allocation outage" >&2
    return 1
  fi

  sed 's/mixed_2_first_tso=136/mixed_2_first_tso=135/' "${bench}" >"${bench}.invalid"
  sed "s#bench_log=${bench}#bench_log=${bench}.invalid#" "${summary}" >"${summary}.invalid"
  if verify_rolling_upgrade "${summary}.invalid" >/dev/null 2>&1; then
    echo "rolling-upgrade verifier accepted overlapping phase ranges" >&2
    return 1
  fi

  sed 's/build_commit=2222222222222222222222222222222222222222/build_commit=1111111111111111111111111111111111111111/' \
    "${identity}" >"${identity}.invalid"
  sed "s#identity_log=${identity}#identity_log=${identity}.invalid#" \
    "${summary}" >"${summary}.invalid"
  if verify_rolling_upgrade "${summary}.invalid" >/dev/null 2>&1; then
    echo "rolling-upgrade verifier accepted a replacement that still ran the old build" >&2
    return 1
  fi

  echo "[rolling-upgrade] verifier self-test passed"
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
  exit 0
fi

[[ $# -eq 1 ]] || {
  echo "usage: $0 <rolling-upgrade-summary.txt> | --self-test" >&2
  exit 1
}
verify_rolling_upgrade "$1"
echo "[rolling-upgrade] verified $1"
