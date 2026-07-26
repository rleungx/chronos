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
  local current_request_log
  local historical_replay_log
  local historical_fresh_log
  bench_log="$(extract_metric bench_log "${summary}")"
  identity_log="$(extract_metric identity_log "${summary}")"
  current_request_log="$(extract_metric current_request_log "${summary}")"
  historical_replay_log="$(extract_metric historical_replay_log "${summary}")"
  historical_fresh_log="$(extract_metric historical_fresh_log "${summary}")"
  [[ -f "${bench_log}" ]] || {
    echo "missing rolling-upgrade bench log: ${bench_log}" >&2
    return 1
  }
  [[ -f "${identity_log}" ]] || {
    echo "missing rolling-upgrade identity log: ${identity_log}" >&2
    return 1
  }
  for probe_log in "${current_request_log}" "${historical_replay_log}" "${historical_fresh_log}"; do
    [[ -f "${probe_log}" ]] || {
      echo "missing rolling-upgrade request probe log: ${probe_log}" >&2
      return 1
    }
  done
  assert_summary_success "${bench_log}" || return 1

  python3 - "${summary}" "${bench_log}" "${identity_log}" \
    "${current_request_log}" "${historical_replay_log}" "${historical_fresh_log}" <<'PY'
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
    "rollback_replace_2",
    "rollback_mixed_1",
    "rollback_replace_1",
    "rollback_mixed_2",
    "rollback_replace_0",
    "old_only_after_rollback",
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
if summary.get("evidence_contract") != "same_format_forward_and_rollback_v1":
    raise SystemExit("evidence must use the same-format forward-and-rollback v1 contract")
if (
    summary.get("forward_upgrade_covered") != "true"
    or summary.get("rollback_covered") != "true"
    or summary.get("semver_downgrade_covered") != "false"
):
    raise SystemExit("evidence must cover same-format forward and rollback without claiming SemVer downgrade")
if (
    number(summary, "worker_count") != 3
    or summary.get("replacement_order") != "0,1,2"
    or summary.get("rollback_replacement_order") != "2,1,0"
):
    raise SystemExit("rolling gate must replace three workers forward 0,1,2 and rollback 2,1,0")
if (
    number(summary, "identity_lease_ttl_ms") != 1500
    or number(summary, "base_identity_requested_ttl_seconds") != 1
    or number(summary, "current_identity_requested_ttl_seconds") != 2
):
    raise SystemExit(
        "1500ms identity semantics must record historical floor=1s and current ceil=2s requests"
    )

for index in range(3):
    old_pid = number(summary, f"old_pid_{index}")
    new_pid = number(summary, f"new_pid_{index}")
    rollback_pid = number(summary, f"rollback_pid_{index}")
    if (
        old_pid <= 0
        or new_pid <= 0
        or rollback_pid <= 0
        or len({old_pid, new_pid, rollback_pid}) != 3
    ):
        raise SystemExit(
            f"worker {index} must record distinct positive old/new/rollback PIDs: "
            f"old={old_pid} new={new_pid} rollback={rollback_pid}"
        )
    instances = {
        summary.get(f"old_instance_{index}"),
        summary.get(f"new_instance_{index}"),
        summary.get(f"rollback_instance_{index}"),
    }
    if None in instances or "" in instances or len(instances) != 3:
        raise SystemExit(f"worker {index} must use fresh forward and rollback instance identities")

if number(bench, "command_count") != 15:
    raise SystemExit(f"continuous client must acknowledge 15 serving commands, got {bench.get('command_count')}")
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
    attempt_failed = number(bench, f"{phase}_attempt_failed_total")
    first = number(bench, f"{phase}_first_tso")
    last = number(bench, f"{phase}_last_tso")
    if requests != success + failed:
        raise SystemExit(
            f"{phase} request accounting mismatch: requests={requests} success={success} failed={failed}"
        )
    if failed != 0:
        raise SystemExit(f"{phase} must have zero logical failures, got {failed}")
    if attempt_failed < 0:
        raise SystemExit(f"{phase} attempt_failed_total must be non-negative")
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
    "rollback-replace-2-started",
    "rollback-old-worker-2",
    "rollback-replace-1-started",
    "rollback-old-worker-1",
    "rollback-replace-0-started",
    "rollback-old-worker-0",
]
if list(blocks) != expected_order:
    raise SystemExit(f"identity evidence order mismatch: observed={list(blocks)}")

service_endpoints = summary.get("service_endpoints", "").split(",")
if len(service_endpoints) != 3 or any(not endpoint for endpoint in service_endpoints):
    raise SystemExit(f"expected three service endpoints, got {service_endpoints}")
expected_ack_contract = {
    "old-worker-0": ("1", "old_only", service_endpoints[0]),
    "old-worker-1": ("2", "old_only", service_endpoints[1]),
    "old-worker-2": ("3", "old_only", service_endpoints[2]),
    "replace-0-started": ("4", "replace_0", ""),
    "new-worker-0": ("5", "mixed_1", service_endpoints[0]),
    "replace-1-started": ("6", "replace_1", ""),
    "new-worker-1": ("7", "mixed_2", service_endpoints[1]),
    "replace-2-started": ("8", "replace_2", ""),
    "new-worker-2": ("9", "new_only", service_endpoints[2]),
    "rollback-replace-2-started": ("10", "rollback_replace_2", service_endpoints[1]),
    "rollback-old-worker-2": ("11", "rollback_mixed_1", service_endpoints[2]),
    "rollback-replace-1-started": ("12", "rollback_replace_1", service_endpoints[2]),
    "rollback-old-worker-1": ("13", "rollback_mixed_2", service_endpoints[1]),
    "rollback-replace-0-started": ("14", "rollback_replace_0", service_endpoints[1]),
    "rollback-old-worker-0": ("15", "old_only_after_rollback", service_endpoints[0]),
}
last_serving_tso = None
for label in expected_order:
    block = blocks[label]
    expected_sequence, expected_phase, expected_target = expected_ack_contract[label]
    observed_contract = (
        block.get("sequence"),
        block.get("phase"),
        block.get("target_endpoint"),
    )
    if block.get("status") != "serving" or observed_contract != (
        expected_sequence,
        expected_phase,
        expected_target,
    ):
        raise SystemExit(
            f"{label} acknowledgement contract mismatch: "
            f"expected=serving/{expected_sequence}/{expected_phase}/{expected_target} "
            f"observed={block.get('status')}/{observed_contract}"
        )
    serving_tso = number(block, "serving_tso")
    if number(block, "observed_epoch") <= 0 or number(block, "observed_route_version") <= 0:
        raise SystemExit(f"{label} must record the actual positive serving epoch and route version")
    if last_serving_tso is not None and serving_tso <= last_serving_tso:
        raise SystemExit(
            f"serving acknowledgements are not strictly increasing at {label}: "
            f"previous={last_serving_tso} current={serving_tso}"
        )
    last_serving_tso = serving_tso

for generation, label_prefix, expected_commit in (
    ("old", "old-worker", base_sha),
    ("new", "new-worker", current_sha),
    ("rollback", "rollback-old-worker", base_sha),
):
    for index in range(3):
        block = blocks[f"{label_prefix}-{index}"]
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

current_request = metrics(sys.argv[4])
historical_replay = metrics(sys.argv[5])
historical_fresh = metrics(sys.argv[6])
for name, probe in (
    ("current-only", current_request),
    ("historical replay", historical_replay),
    ("historical fresh", historical_fresh),
):
    if probe.get("probe_timeline_key") != summary.get("timeline_key"):
        raise SystemExit(
            f"{name} probe targeted {probe.get('probe_timeline_key')}, "
            f"expected {summary.get('timeline_key')}"
        )
    if probe.get("probe_idempotency_replay_verified") != "true":
        raise SystemExit(f"{name} probe did not verify its in-process idempotent replay")
    response_hex = probe.get("probe_response_protobuf_hex", "")
    if not response_hex or len(response_hex) % 2 or not re.fullmatch(r"[0-9a-f]+", response_hex):
        raise SystemExit(f"{name} probe did not emit a canonical full protobuf response")

if (
    current_request["probe_response_protobuf_hex"]
    != historical_replay["probe_response_protobuf_hex"]
):
    raise SystemExit("historical binary replay changed the complete current-created response")
for key in ("probe_first_tso", "probe_last_tso"):
    if number(current_request, key) != number(historical_replay, key):
        raise SystemExit(f"historical binary replay changed {key}")

replay_last = number(historical_replay, "probe_last_tso")
fresh_first = number(historical_fresh, "probe_first_tso")
fresh_last = number(historical_fresh, "probe_last_tso")
if fresh_first <= replay_last:
    raise SystemExit(
        f"fresh historical request did not advance beyond replay: replay={replay_last} fresh={fresh_first}"
    )
if fresh_first <= last_serving_tso:
    raise SystemExit(
        f"fresh historical request did not advance beyond final rollback acknowledgement: "
        f"ack={last_serving_tso} fresh={fresh_first}"
    )
if number(bench, "global_last_tso") <= fresh_last:
    raise SystemExit(
        f"continuous allocator did not advance beyond fresh historical request: "
        f"fresh={fresh_last} allocator={bench.get('global_last_tso')}"
    )
PY
}

verify_rollback_choreography_source() {
  python3 - "${REPO_ROOT}/hack/upgrade/rolling-same-format.sh" <<'PY'
import re
import sys

source = open(sys.argv[1], encoding="utf-8").read()
match = re.search(r"for idx in 2 1 0; do\n(?P<body>.*?)\ndone", source, re.DOTALL)
if not match:
    raise SystemExit("missing rollback 2,1,0 choreography")
body = match.group("body")
mapping = re.search(
    r'if \[\[ "\$\{idx\}" -eq 2 \]\]; then\s+'
    r'safe_endpoint="\$\{SERVICE_ENDPOINTS\[1\]\}".*?'
    r'elif \[\[ "\$\{idx\}" -eq 1 \]\]; then\s+'
    r'safe_endpoint="\$\{SERVICE_ENDPOINTS\[2\]\}".*?'
    r'else\s+safe_endpoint="\$\{SERVICE_ENDPOINTS\[1\]\}"',
    body,
    re.DOTALL,
)
if not mapping:
    raise SystemExit("rollback safe-owner mapping must remain 2->1, 1->2, 0->1")
ack = body.find("issue_serving_command")
stop = body.find('stop_process "${ACTIVE_PIDS[idx]}"')
if ack < 0 or stop < 0 or ack >= stop:
    raise SystemExit("rollback safe-owner serving acknowledgement must happen before target stop")
PY
}

self_test() {
  local test_dir summary bench identity current_request historical_replay historical_fresh
  test_dir="$(mktemp -d)"
  summary="${test_dir}/summary.txt"
  bench="${test_dir}/bench.txt"
  identity="${test_dir}/identity.txt"
  current_request="${test_dir}/current-request.txt"
  historical_replay="${test_dir}/historical-replay.txt"
  historical_fresh="${test_dir}/historical-fresh.txt"
  trap 'rm -rf "${test_dir}"' RETURN
  verify_rollback_choreography_source

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
evidence_contract=same_format_forward_and_rollback_v1
forward_upgrade_covered=true
rollback_covered=true
semver_downgrade_covered=false
worker_count=3
replacement_order=0,1,2
rollback_replacement_order=2,1,0
identity_lease_ttl_ms=1500
base_identity_requested_ttl_seconds=1
current_identity_requested_ttl_seconds=2
service_endpoints=127.0.0.1:52051,127.0.0.1:52052,127.0.0.1:52053
timeline_key=bench.rolling-upgrade.0
old_pid_0=101
old_pid_1=102
old_pid_2=103
new_pid_0=201
new_pid_1=202
new_pid_2=203
rollback_pid_0=301
rollback_pid_1=302
rollback_pid_2=303
old_instance_0=upgrade-old-0
old_instance_1=upgrade-old-1
old_instance_2=upgrade-old-2
new_instance_0=upgrade-new-0
new_instance_1=upgrade-new-1
new_instance_2=upgrade-new-2
rollback_instance_0=upgrade-rollback-old-0
rollback_instance_1=upgrade-rollback-old-1
rollback_instance_2=upgrade-rollback-old-2
max_success_gap_ms=3000
bench_log=${bench}
identity_log=${identity}
current_request_log=${current_request}
historical_replay_log=${historical_replay}
historical_fresh_log=${historical_fresh}
EOF

  {
    echo "result=success"
    echo "command_count=15"
    echo "monotonicity_violations_total=0"
    echo "global_max_success_gap_ms=120"
    echo "global_first_tso=100"
    echo "global_last_tso=260"
    first=100
    for phase in \
      old_only replace_0 mixed_1 replace_1 mixed_2 replace_2 new_only \
      rollback_replace_2 rollback_mixed_1 rollback_replace_1 rollback_mixed_2 \
      rollback_replace_0 old_only_after_rollback; do
      echo "${phase}_requests_total=10"
      echo "${phase}_success_total=10"
      echo "${phase}_failed_total=0"
      echo "${phase}_attempt_failed_total=1"
      echo "${phase}_first_tso=${first}"
      echo "${phase}_last_tso=$((first + 9))"
      echo "${phase}_max_success_gap_ms=100"
      first=$((first + 10))
    done
  } >"${bench}"

  sequence=0
  serving_tso=90
  for label in \
    old-worker-0 old-worker-1 old-worker-2 replace-0-started new-worker-0 \
    replace-1-started new-worker-1 replace-2-started new-worker-2 \
    rollback-replace-2-started rollback-old-worker-2 \
    rollback-replace-1-started rollback-old-worker-1 \
    rollback-replace-0-started rollback-old-worker-0; do
    sequence=$((sequence + 1))
    serving_tso=$((serving_tso + 1))
    {
      echo "[${label}]"
      echo "sequence=${sequence}"
      echo "status=serving"
      case "${label}" in
        old-worker-*) phase=old_only ;;
        replace-0-started) phase=replace_0 ;;
        new-worker-0) phase=mixed_1 ;;
        replace-1-started) phase=replace_1 ;;
        new-worker-1) phase=mixed_2 ;;
        replace-2-started) phase=replace_2 ;;
        new-worker-2) phase=new_only ;;
        rollback-replace-2-started) phase=rollback_replace_2 ;;
        rollback-old-worker-2) phase=rollback_mixed_1 ;;
        rollback-replace-1-started) phase=rollback_replace_1 ;;
        rollback-old-worker-1) phase=rollback_mixed_2 ;;
        rollback-replace-0-started) phase=rollback_replace_0 ;;
        rollback-old-worker-0) phase=old_only_after_rollback ;;
      esac
      echo "phase=${phase}"
      if [[ "${label}" =~ ^(old|new)-worker-([0-2])$ ]]; then
        generation="${BASH_REMATCH[1]}"
        index="${BASH_REMATCH[2]}"
      elif [[ "${label}" =~ ^rollback-old-worker-([0-2])$ ]]; then
        generation=rollback
        index="${BASH_REMATCH[1]}"
      else
        generation=
        index=
      fi
      if [[ -n "${generation}" ]]; then
        echo "target_endpoint=127.0.0.1:$((52051 + index))"
        echo "observed_owner_endpoint=127.0.0.1:$((52051 + index))"
        if [[ "${generation}" == "rollback" ]]; then
          echo "instance_id=upgrade-rollback-old-${index}"
        else
          echo "instance_id=upgrade-${generation}-${index}"
        fi
        echo "worker_id=upgrade-worker-${index}"
        if [[ "${generation}" == "new" ]]; then
          echo "build_commit=2222222222222222222222222222222222222222"
        else
          echo "build_commit=1111111111111111111111111111111111111111"
        fi
      else
        case "${label}" in
          rollback-replace-2-started | rollback-replace-0-started)
            safe_endpoint=127.0.0.1:52052
            ;;
          rollback-replace-1-started)
            safe_endpoint=127.0.0.1:52053
            ;;
          *)
            safe_endpoint=
            ;;
        esac
        echo "target_endpoint=${safe_endpoint}"
        echo "observed_owner_endpoint=${safe_endpoint:-127.0.0.1:52051}"
        echo "instance_id="
        echo "worker_id="
        echo "build_commit="
      fi
      echo "observed_epoch=1"
      echo "observed_route_version=${sequence}"
      echo "serving_tso=${serving_tso}"
    } >>"${identity}"
  done

  cat >"${current_request}" <<EOF
probe_timeline_key=bench.rolling-upgrade.0
probe_first_tso=150
probe_last_tso=150
probe_response_protobuf_hex=0a01
probe_idempotency_replay_verified=true
EOF
  cp "${current_request}" "${historical_replay}"
  cat >"${historical_fresh}" <<EOF
probe_timeline_key=bench.rolling-upgrade.0
probe_first_tso=250
probe_last_tso=250
probe_response_protobuf_hex=0b01
probe_idempotency_replay_verified=true
EOF

  verify_rolling_upgrade "${summary}"

  sed 's/evidence_contract=same_format_forward_and_rollback_v1/evidence_contract=forward_only/' \
    "${summary}" >"${summary}.invalid"
  if verify_rolling_upgrade "${summary}.invalid" >/dev/null 2>&1; then
    echo "rolling-upgrade verifier accepted forward-only evidence as rollback" >&2
    return 1
  fi

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

  sed 's/mixed_2_first_tso=140/mixed_2_first_tso=139/' "${bench}" >"${bench}.invalid"
  sed "s#bench_log=${bench}#bench_log=${bench}.invalid#" "${summary}" >"${summary}.invalid"
  if verify_rolling_upgrade "${summary}.invalid" >/dev/null 2>&1; then
    echo "rolling-upgrade verifier accepted overlapping phase ranges" >&2
    return 1
  fi

  sed 's/rollback_mixed_1_failed_total=0/rollback_mixed_1_failed_total=1/' \
    "${bench}" >"${bench}.invalid"
  sed "s#bench_log=${bench}#bench_log=${bench}.invalid#" "${summary}" >"${summary}.invalid"
  if verify_rolling_upgrade "${summary}.invalid" >/dev/null 2>&1; then
    echo "rolling-upgrade verifier accepted a rollback logical failure" >&2
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

  sed 's/sequence=5/sequence=4/' "${identity}" >"${identity}.invalid"
  sed "s#identity_log=${identity}#identity_log=${identity}.invalid#" \
    "${summary}" >"${summary}.invalid"
  if verify_rolling_upgrade "${summary}.invalid" >/dev/null 2>&1; then
    echo "rolling-upgrade verifier accepted a duplicate acknowledgement sequence" >&2
    return 1
  fi

  sed 's/phase=rollback_mixed_2/phase=old_only/' "${identity}" >"${identity}.invalid"
  sed "s#identity_log=${identity}#identity_log=${identity}.invalid#" \
    "${summary}" >"${summary}.invalid"
  if verify_rolling_upgrade "${summary}.invalid" >/dev/null 2>&1; then
    echo "rolling-upgrade verifier accepted an acknowledgement phase mismatch" >&2
    return 1
  fi

  sed 's/probe_response_protobuf_hex=0a01/probe_response_protobuf_hex=0c01/' \
    "${historical_replay}" >"${historical_replay}.invalid"
  sed "s#historical_replay_log=${historical_replay}#historical_replay_log=${historical_replay}.invalid#" \
    "${summary}" >"${summary}.invalid"
  if verify_rolling_upgrade "${summary}.invalid" >/dev/null 2>&1; then
    echo "rolling-upgrade verifier accepted a changed cross-binary replay response" >&2
    return 1
  fi

  sed 's/global_last_tso=260/global_last_tso=250/' "${bench}" >"${bench}.invalid"
  sed "s#bench_log=${bench}#bench_log=${bench}.invalid#" "${summary}" >"${summary}.invalid"
  if verify_rolling_upgrade "${summary}.invalid" >/dev/null 2>&1; then
    echo "rolling-upgrade verifier accepted an allocator that did not pass the fresh request" >&2
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
