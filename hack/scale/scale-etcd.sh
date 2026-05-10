#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${REPO_ROOT}/hack/lib/common.sh"

cd "${REPO_ROOT}"

require_positive_integer() {
  local name=$1
  local value=$2
  if ! [[ "${value}" =~ ^[0-9]+$ ]] || [[ "${value}" -eq 0 ]]; then
    echo "${name} must be a positive integer, got: ${value}" >&2
    return 1
  fi
}

require_nonnegative_integer() {
  local name=$1
  local value=$2
  if ! [[ "${value}" =~ ^[0-9]+$ ]]; then
    echo "${name} must be a non-negative integer, got: ${value}" >&2
    return 1
  fi
}

csv_to_array() {
  local array_name=$1
  local value=$2
  eval "${array_name}=()"
  local -a raw
  IFS=',' read -ra raw <<<"${value}"
  local item
  for item in "${raw[@]}"; do
    item="${item#"${item%%[![:space:]]*}"}"
    item="${item%"${item##*[![:space:]]}"}"
    [[ -n "${item}" ]] && eval "${array_name}+=(\"\${item}\")"
  done
}

join_by_comma() {
  local joined=""
  local item
  for item in "$@"; do
    if [[ -n "${joined}" ]]; then
      joined+=","
    fi
    joined+="${item}"
  done
  printf '%s' "${joined}"
}

generated_endpoint() {
  local base_port=$1
  local index=$2
  printf '127.0.0.1:%s' "$((base_port + index))"
}

metric_log_name() {
  local index=$1
  printf '%s/metrics-%s.txt' "${ARTIFACT_DIR}" "${index}"
}

readyz_log_name() {
  local index=$1
  printf '%s/readyz-%s.txt' "${ARTIFACT_DIR}" "${index}"
}

WORKER_COUNT="${CHRONOS_SCALE_WORKERS:-2}"
require_positive_integer "CHRONOS_SCALE_WORKERS" "${WORKER_COUNT}"

DEFAULT_BENCH_REQUEST_TIMEOUT_MS=10000
DEFAULT_BENCH_CLIENT_TIMEOUT_MS=15000
DEFAULT_BENCH_CONNECT_TIMEOUT_MS=15000
DEFAULT_BENCH_CONNECT_RETRY_INTERVAL_MS=100
DEFAULT_COLD_REQUEST_TIMEOUT_MS=10000
DEFAULT_COLD_CLIENT_TIMEOUT_MS=15000
DEFAULT_COLD_PROBE_CONCURRENCY=$((WORKER_COUNT * 4))
if [[ "${DEFAULT_COLD_PROBE_CONCURRENCY}" -gt 16 ]]; then
  DEFAULT_COLD_PROBE_CONCURRENCY=16
fi

ETCD_ENDPOINTS="${CHRONOS_SCALE_ETCD_ENDPOINTS:-127.0.0.1:2379}"
SERVICE_BASE_PORT="${CHRONOS_SCALE_SERVICE_BASE_PORT:-50051}"
METRICS_BASE_PORT="${CHRONOS_SCALE_METRICS_BASE_PORT:-9898}"
BENCH_DURATION_SECS="${CHRONOS_SCALE_DURATION_SECS:-5}"
BENCH_WARMUP_SECS="${CHRONOS_SCALE_WARMUP_SECS:-1}"
BENCH_CONCURRENCY="${CHRONOS_SCALE_CONCURRENCY:-32}"
BENCH_TIMELINES="${CHRONOS_SCALE_TIMELINES:-$((WORKER_COUNT * 32))}"
BENCH_BATCH="${CHRONOS_SCALE_BATCH:-1}"
BENCH_REQUEST_TIMEOUT_MS="${CHRONOS_SCALE_REQUEST_TIMEOUT_MS:-${DEFAULT_BENCH_REQUEST_TIMEOUT_MS}}"
BENCH_CLIENT_TIMEOUT_MS="${CHRONOS_SCALE_CLIENT_TIMEOUT_MS:-${DEFAULT_BENCH_CLIENT_TIMEOUT_MS}}"
BENCH_CONNECT_TIMEOUT_MS="${CHRONOS_SCALE_CONNECT_TIMEOUT_MS:-${DEFAULT_BENCH_CONNECT_TIMEOUT_MS}}"
BENCH_CONNECT_RETRY_INTERVAL_MS="${CHRONOS_SCALE_CONNECT_RETRY_INTERVAL_MS:-${DEFAULT_BENCH_CONNECT_RETRY_INTERVAL_MS}}"
BENCH_ALLOCATION_CONNECTION_POOL_SIZE="${CHRONOS_SCALE_ALLOCATION_CONNECTION_POOL_SIZE:-4}"
BENCH_CLIENT_PROCESSES="${CHRONOS_SCALE_BENCH_CLIENT_PROCESSES:-1}"
COLD_PROBE_ENABLED="${CHRONOS_SCALE_COLD_PROBE:-true}"
COLD_PROBE_CONCURRENCY="${CHRONOS_SCALE_COLD_PROBE_CONCURRENCY:-${DEFAULT_COLD_PROBE_CONCURRENCY}}"
COLD_REQUEST_TIMEOUT_MS="${CHRONOS_SCALE_COLD_REQUEST_TIMEOUT_MS:-${DEFAULT_COLD_REQUEST_TIMEOUT_MS}}"
COLD_CLIENT_TIMEOUT_MS="${CHRONOS_SCALE_COLD_CLIENT_TIMEOUT_MS:-${DEFAULT_COLD_CLIENT_TIMEOUT_MS}}"
WAIT_ATTEMPTS="${CHRONOS_SCALE_WAIT_ATTEMPTS:-60}"
WAIT_INTERVAL_SECS="${CHRONOS_SCALE_WAIT_INTERVAL_SECS:-1}"
POST_READY_SLEEP_SECS="${CHRONOS_SCALE_POST_READY_SLEEP_SECS:-1}"
REQ_PER_SEC_MIN="${CHRONOS_SCALE_REQ_PER_SEC_MIN:-50}"
LATENCY_P95_US_MAX="${CHRONOS_SCALE_LATENCY_P95_US_MAX:-250000}"
LATENCY_P99_US_MAX="${CHRONOS_SCALE_LATENCY_P99_US_MAX:-750000}"
LATENCY_P999_US_MAX="${CHRONOS_SCALE_LATENCY_P999_US_MAX:-5000000}"
ROUTE_OWNER_ENDPOINTS_MIN="${CHRONOS_SCALE_ROUTE_OWNER_ENDPOINTS_MIN:-${WORKER_COUNT}}"
COLD_PROBE_LATENCY_P95_US_MAX="${CHRONOS_SCALE_COLD_PROBE_LATENCY_P95_US_MAX:-5000000}"
COLD_PROBE_LATENCY_P999_US_MAX="${CHRONOS_SCALE_COLD_PROBE_LATENCY_P999_US_MAX:-5000000}"
UNIQUE_SUFFIX="$(date +%s)-$$"
ETCD_PREFIX="${CHRONOS_SCALE_ETCD_PREFIX:-/chronos-scale-${UNIQUE_SUFFIX}}"
TIMELINE_SCENARIO="${CHRONOS_SCALE_SCENARIO:-scale-${UNIQUE_SUFFIX}}"
SAFETY_GAP_MS="${CHRONOS_SCALE_SAFETY_GAP_MS:-1}"
ARTIFACT_ROOT="${CHRONOS_SCALE_ARTIFACT_DIR:-${CHRONOS_ARTIFACT_DIR:-}}"
KEEP_ARTIFACTS_ON_SUCCESS="${CHRONOS_SCALE_KEEP_ARTIFACTS_ON_SUCCESS:-${CHRONOS_KEEP_ARTIFACTS_ON_SUCCESS:-0}}"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

require_positive_integer "CHRONOS_SCALE_CONCURRENCY" "${BENCH_CONCURRENCY}"
require_positive_integer "CHRONOS_SCALE_TIMELINES" "${BENCH_TIMELINES}"
require_positive_integer "CHRONOS_SCALE_CONNECT_TIMEOUT_MS" "${BENCH_CONNECT_TIMEOUT_MS}"
require_positive_integer "CHRONOS_SCALE_CONNECT_RETRY_INTERVAL_MS" "${BENCH_CONNECT_RETRY_INTERVAL_MS}"
require_positive_integer "CHRONOS_SCALE_ALLOCATION_CONNECTION_POOL_SIZE" "${BENCH_ALLOCATION_CONNECTION_POOL_SIZE}"
require_positive_integer "CHRONOS_SCALE_BENCH_CLIENT_PROCESSES" "${BENCH_CLIENT_PROCESSES}"
require_positive_integer "CHRONOS_SCALE_COLD_PROBE_CONCURRENCY" "${COLD_PROBE_CONCURRENCY}"
require_nonnegative_integer "CHRONOS_SCALE_POST_READY_SLEEP_SECS" "${POST_READY_SLEEP_SECS}"
if [[ "${BENCH_CLIENT_PROCESSES}" -gt "${BENCH_CONCURRENCY}" ]]; then
  echo "CHRONOS_SCALE_BENCH_CLIENT_PROCESSES must be <= CHRONOS_SCALE_CONCURRENCY" >&2
  exit 1
fi
if [[ "${BENCH_CLIENT_PROCESSES}" -gt "${BENCH_TIMELINES}" ]]; then
  echo "CHRONOS_SCALE_BENCH_CLIENT_PROCESSES must be <= CHRONOS_SCALE_TIMELINES" >&2
  exit 1
fi

SERVICE_ENDPOINTS=()
METRICS_ENDPOINTS=()
ADVERTISE_ENDPOINTS=()
WORKER_IDS=()

if [[ -n "${CHRONOS_SCALE_SERVICE_ENDPOINTS:-}" ]]; then
  csv_to_array SERVICE_ENDPOINTS "${CHRONOS_SCALE_SERVICE_ENDPOINTS}"
else
  for ((idx = 0; idx < WORKER_COUNT; idx++)); do
    SERVICE_ENDPOINTS+=("$(generated_endpoint "${SERVICE_BASE_PORT}" "${idx}")")
  done
  if [[ "${WORKER_COUNT}" -eq 2 ]]; then
    SERVICE_ENDPOINTS[0]="${CHRONOS_SCALE_SERVICE_ENDPOINT_A:-${SERVICE_ENDPOINTS[0]}}"
    SERVICE_ENDPOINTS[1]="${CHRONOS_SCALE_SERVICE_ENDPOINT_B:-${SERVICE_ENDPOINTS[1]}}"
  fi
fi

if [[ -n "${CHRONOS_SCALE_METRICS_ENDPOINTS:-}" ]]; then
  csv_to_array METRICS_ENDPOINTS "${CHRONOS_SCALE_METRICS_ENDPOINTS}"
else
  for ((idx = 0; idx < WORKER_COUNT; idx++)); do
    METRICS_ENDPOINTS+=("$(generated_endpoint "${METRICS_BASE_PORT}" "${idx}")")
  done
  if [[ "${WORKER_COUNT}" -eq 2 ]]; then
    METRICS_ENDPOINTS[0]="${CHRONOS_SCALE_METRICS_ENDPOINT_A:-${METRICS_ENDPOINTS[0]}}"
    METRICS_ENDPOINTS[1]="${CHRONOS_SCALE_METRICS_ENDPOINT_B:-${METRICS_ENDPOINTS[1]}}"
  fi
fi

if [[ -n "${CHRONOS_SCALE_WORKER_IDS:-}" ]]; then
  csv_to_array WORKER_IDS "${CHRONOS_SCALE_WORKER_IDS}"
else
  for ((idx = 0; idx < WORKER_COUNT; idx++)); do
    WORKER_IDS+=("worker-scale-${idx}")
  done
  if [[ "${WORKER_COUNT}" -eq 2 ]]; then
    WORKER_IDS[0]="${CHRONOS_SCALE_WORKER_ID_A:-${WORKER_IDS[0]}}"
    WORKER_IDS[1]="${CHRONOS_SCALE_WORKER_ID_B:-${WORKER_IDS[1]}}"
  fi
fi

if [[ "${#SERVICE_ENDPOINTS[@]}" -ne "${WORKER_COUNT}" ]]; then
  echo "CHRONOS_SCALE_SERVICE_ENDPOINTS must contain ${WORKER_COUNT} entries" >&2
  exit 1
fi
if [[ "${#METRICS_ENDPOINTS[@]}" -ne "${WORKER_COUNT}" ]]; then
  echo "CHRONOS_SCALE_METRICS_ENDPOINTS must contain ${WORKER_COUNT} entries" >&2
  exit 1
fi
if [[ "${#WORKER_IDS[@]}" -ne "${WORKER_COUNT}" ]]; then
  echo "CHRONOS_SCALE_WORKER_IDS must contain ${WORKER_COUNT} entries" >&2
  exit 1
fi

if [[ -n "${CHRONOS_SCALE_ADVERTISE_ENDPOINTS:-}" ]]; then
  csv_to_array ADVERTISE_ENDPOINTS "${CHRONOS_SCALE_ADVERTISE_ENDPOINTS}"
  if [[ "${#ADVERTISE_ENDPOINTS[@]}" -ne "${WORKER_COUNT}" ]]; then
    echo "CHRONOS_SCALE_ADVERTISE_ENDPOINTS must contain ${WORKER_COUNT} entries" >&2
    exit 1
  fi
else
  for ((idx = 0; idx < WORKER_COUNT; idx++)); do
    local_override=""
    if [[ "${WORKER_COUNT}" -eq 2 && "${idx}" -eq 0 ]]; then
      local_override="${CHRONOS_SCALE_ADVERTISE_ENDPOINT_A:-}"
    elif [[ "${WORKER_COUNT}" -eq 2 && "${idx}" -eq 1 ]]; then
      local_override="${CHRONOS_SCALE_ADVERTISE_ENDPOINT_B:-}"
    fi
    ADVERTISE_ENDPOINTS+=("$(derive_local_advertise_endpoint "${SERVICE_ENDPOINTS[idx]}" "chronos-scale-${idx}" "${local_override}")")
  done
fi

HTTP_SERVICE_ENDPOINTS=()
for endpoint in "${SERVICE_ENDPOINTS[@]}"; do
  HTTP_SERVICE_ENDPOINTS+=("http://${endpoint}")
done
CONTROL_ENDPOINTS="$(join_by_comma "${HTTP_SERVICE_ENDPOINTS[@]}")"
SERVICE_ENDPOINTS_CSV="$(join_by_comma "${SERVICE_ENDPOINTS[@]}")"
METRICS_ENDPOINTS_CSV="$(join_by_comma "${METRICS_ENDPOINTS[@]}")"
ADVERTISE_ENDPOINTS_CSV="$(join_by_comma "${ADVERTISE_ENDPOINTS[@]}")"
WORKER_IDS_CSV="$(join_by_comma "${WORKER_IDS[@]}")"

if [[ -n "${ARTIFACT_ROOT}" ]]; then
  ARTIFACT_DIR="${ARTIFACT_ROOT%/}/scale"
  mkdir -p "${ARTIFACT_DIR}"
  KEEP_ARTIFACTS_ON_SUCCESS=1
  BENCH_LOG="${ARTIFACT_DIR}/scale-bench.log"
  PROFILE_LOG="${ARTIFACT_DIR}/profile-summary.txt"
  ETCD_LOG="${ARTIFACT_DIR}/etcd.log"
  DOCKER_PS_LOG="${ARTIFACT_DIR}/docker-ps.txt"
  SUMMARY_LOG="${ARTIFACT_DIR}/summary.txt"
  INDEX_LOG="${ARTIFACT_DIR}/artifact-index.txt"
else
  ARTIFACT_DIR=""
  BENCH_LOG="$(mktemp -t chronos-scale-bench.XXXXXX.log)"
  PROFILE_LOG=""
  ETCD_LOG=""
  DOCKER_PS_LOG=""
  SUMMARY_LOG=""
  INDEX_LOG=""
fi

CHRONOS_PIDS=()
CHRONOS_LOGS=()
READYZ_LOGS=()
METRICS_LOGS=()
BENCH_CLIENT_LOGS=()
for ((idx = 0; idx < WORKER_COUNT; idx++)); do
  if [[ -n "${ARTIFACT_DIR}" ]]; then
    CHRONOS_LOGS+=("${ARTIFACT_DIR}/chronos-${idx}.log")
    READYZ_LOGS+=("$(readyz_log_name "${idx}")")
    METRICS_LOGS+=("$(metric_log_name "${idx}")")
  else
    CHRONOS_LOGS+=("$(mktemp -t chronos-scale-${idx}.XXXXXX.log)")
    READYZ_LOGS+=("")
    METRICS_LOGS+=("")
  fi
done
for ((idx = 0; idx < BENCH_CLIENT_PROCESSES; idx++)); do
  if [[ -n "${ARTIFACT_DIR}" ]]; then
    BENCH_CLIENT_LOGS+=("${ARTIFACT_DIR}/scale-bench-client-${idx}.log")
  else
    BENCH_CLIENT_LOGS+=("$(mktemp -t chronos-scale-bench-client-${idx}.XXXXXX.log)")
  fi
done

RESULT="failure"
RELEASE_BIN_DIR="${CHRONOS_RELEASE_BIN_DIR:-${REPO_ROOT}/target/release}"

write_summary() {
  [[ -n "${SUMMARY_LOG}" ]] || return 0
  mkdir -p "${ARTIFACT_DIR}"
  cat >"${SUMMARY_LOG}" <<EOF
result=${RESULT}
started_at=${STARTED_AT}
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
host=$(hostname)
worker_count=${WORKER_COUNT}
etcd_endpoints=${ETCD_ENDPOINTS}
service_endpoints=${SERVICE_ENDPOINTS_CSV}
advertise_endpoints=${ADVERTISE_ENDPOINTS_CSV}
metrics_endpoints=${METRICS_ENDPOINTS_CSV}
bench_duration_secs=${BENCH_DURATION_SECS}
bench_warmup_secs=${BENCH_WARMUP_SECS}
bench_concurrency=${BENCH_CONCURRENCY}
bench_timelines=${BENCH_TIMELINES}
bench_batch=${BENCH_BATCH}
bench_request_timeout_ms=${BENCH_REQUEST_TIMEOUT_MS}
bench_client_timeout_ms=${BENCH_CLIENT_TIMEOUT_MS}
bench_connect_timeout_ms=${BENCH_CONNECT_TIMEOUT_MS}
bench_connect_retry_interval_ms=${BENCH_CONNECT_RETRY_INTERVAL_MS}
bench_allocation_connection_pool_size=${BENCH_ALLOCATION_CONNECTION_POOL_SIZE}
bench_client_processes=${BENCH_CLIENT_PROCESSES}
cold_probe_enabled=${COLD_PROBE_ENABLED}
cold_probe_concurrency=${COLD_PROBE_CONCURRENCY}
cold_request_timeout_ms=${COLD_REQUEST_TIMEOUT_MS}
cold_client_timeout_ms=${COLD_CLIENT_TIMEOUT_MS}
post_ready_sleep_secs=${POST_READY_SLEEP_SECS}
route_owner_endpoints_min=${ROUTE_OWNER_ENDPOINTS_MIN}
req_per_sec_min=${REQ_PER_SEC_MIN}
latency_p95_us_max=${LATENCY_P95_US_MAX}
latency_p99_us_max=${LATENCY_P99_US_MAX}
latency_p999_us_max=${LATENCY_P999_US_MAX}
cold_probe_latency_p95_us_max=${COLD_PROBE_LATENCY_P95_US_MAX}
cold_probe_latency_p999_us_max=${COLD_PROBE_LATENCY_P999_US_MAX}
etcd_prefix=${ETCD_PREFIX}
timeline_scenario=${TIMELINE_SCENARIO}
worker_ids=${WORKER_IDS_CSV}
ownership_plan_id=${TIMELINE_SCENARIO}
generator_ownership_modulo=${WORKER_COUNT}
generator_ownership_remainders=0..$((WORKER_COUNT - 1))
safety_gap_ms=${SAFETY_GAP_MS}
artifact_dir=${ARTIFACT_DIR}
artifact_index=${INDEX_LOG}
chronos_logs=$(join_by_comma "${CHRONOS_LOGS[@]}")
scale_bench_log=${BENCH_LOG}
scale_bench_client_logs=$(join_by_comma "${BENCH_CLIENT_LOGS[@]}")
profile_summary=${PROFILE_LOG}
EOF

  if [[ -f "${BENCH_LOG}" ]]; then
    local key
    local value
    for key in \
      route_owner_endpoints \
      route_owner_min_timelines \
      route_owner_max_timelines \
      allocation_failed_total \
      allocation_measured_failed_total \
      allocation_failure_reasons \
      allocation_measured_failure_reasons \
      connect_timeout_ms \
      connect_retry_interval_ms \
      bench_client_processes \
      owner_affinity \
      req_per_sec \
      latency_p95_us \
      latency_p99_us \
      latency_p999_us \
      cold_probe_failed_total \
      cold_probe_failure_reasons \
      cold_probe_latency_p95_us \
      cold_probe_latency_p999_us; do
      value="$(extract_metric "${key}" "${BENCH_LOG}")"
      [[ -n "${value}" ]] && printf '%s=%s\n' "${key}" "${value}" >>"${SUMMARY_LOG}"
    done
  fi
}

prom_metric_value() {
  local file=$1
  local metric=$2
  [[ -f "${file}" ]] || return 0
  awk -v metric="${metric}" '$1 == metric { print $2; exit }' "${file}"
}

prom_metric_lines() {
  local file=$1
  local metric=$2
  [[ -f "${file}" ]] || return 0
  awk -v metric="${metric}" 'index($1, metric) == 1 { print }' "${file}"
}

prom_histogram_quantile_upper_bound() {
  local file=$1
  local prefix=$2
  local quantile=$3
  [[ -f "${file}" ]] || return 0
  awk -v prefix="${prefix}" -v quantile="${quantile}" '
    index($1, prefix) == 1 {
      label = $1
      value = $2 + 0
      le = label
      sub(/^.*le="/, "", le)
      sub(/".*$/, "", le)
      if (le == "+Inf") {
        total = value
      } else {
        count += 1
        buckets[count] = le
        values[count] = value
      }
    }
    END {
      if (total <= 0) {
        exit 0
      }
      target = total * quantile
      for (idx = 1; idx <= count; idx += 1) {
        if (values[idx] >= target) {
          print buckets[idx]
          exit 0
        }
      }
      print "+Inf"
    }
  ' "${file}"
}

write_profile_summary() {
  [[ -n "${PROFILE_LOG}" ]] || return 0
  mkdir -p "${ARTIFACT_DIR}"
  {
    echo "result=${RESULT}"
    echo "worker_count=${WORKER_COUNT}"
    echo "bench_log=${BENCH_LOG}"
    if [[ -f "${BENCH_LOG}" ]]; then
      local key
      local value
      for key in \
        allocation_failed_total \
        allocation_failure_reasons \
        allocation_measured_failed_total \
        allocation_measured_failure_reasons \
        connect_timeout_ms \
        connect_retry_interval_ms \
        allocation_connection_pool_size \
        bench_client_processes \
        owner_affinity \
        req_per_sec \
        latency_p50_us \
        latency_p95_us \
        latency_p99_us \
        latency_p999_us \
        latency_max_us \
        cold_probe_failed_total \
        cold_probe_failure_reasons \
        cold_probe_latency_p95_us \
        cold_probe_latency_p999_us \
        cold_probe_latency_max_us; do
        value="$(extract_metric "${key}" "${BENCH_LOG}")"
        [[ -n "${value}" ]] && printf 'bench.%s=%s\n' "${key}" "${value}"
      done
    fi

    local idx
    local metrics_file
    for ((idx = 0; idx < WORKER_COUNT; idx++)); do
      metrics_file="${METRICS_LOGS[idx]}"
      echo "worker.${idx}.metrics=${metrics_file}"
      [[ -f "${metrics_file}" ]] || continue
      local metric
      local value
      for metric in \
        tso_allocate_total \
        tso_allocate_latency_seconds_count \
        tso_allocate_latency_seconds_sum \
        'tso_allocate_stage_latency_seconds_count{path="cached",stage="admission_wait"}' \
        'tso_allocate_stage_latency_seconds_sum{path="cached",stage="admission_wait"}' \
        'tso_allocate_stage_latency_seconds_count{path="cached",stage="serve"}' \
        'tso_allocate_stage_latency_seconds_sum{path="cached",stage="serve"}' \
        tso_timeline_proxy_wait_seconds_count \
        tso_timeline_proxy_wait_seconds_sum \
        tso_timeline_proxy_timeout_total \
        tso_timeline_runtime_cache_entries \
        tso_generator_active_leases \
        tso_generator_min_lease_headroom_ms; do
        value="$(prom_metric_value "${metrics_file}" "${metric}")"
        [[ -n "${value}" ]] && printf 'worker.%s.%s=%s\n' "${idx}" "${metric}" "${value}"
      done
      for metric in \
        'tso_allocate_latency_seconds_bucket' \
        'tso_allocate_stage_latency_seconds_bucket{path="cached",stage="admission_wait",' \
        'tso_allocate_stage_latency_seconds_bucket{path="cached",stage="serve",' \
        'tso_timeline_proxy_wait_seconds_bucket'; do
        value="$(prom_histogram_quantile_upper_bound "${metrics_file}" "${metric}" "0.999")"
        [[ -n "${value}" ]] && printf 'worker.%s.%s_p999_upper_bound_seconds=%s\n' "${idx}" "${metric}" "${value}"
      done
      while IFS= read -r line; do
        [[ -n "${line}" ]] && printf 'worker.%s.%s\n' "${idx}" "${line}"
      done < <(prom_metric_lines "${metrics_file}" "tso_metadata_conflicts_total")
    done
  } >"${PROFILE_LOG}"
}

capture_diagnostics() {
  [[ -n "${ARTIFACT_DIR}" ]] && mkdir -p "${ARTIFACT_DIR}"
  [[ -n "${DOCKER_PS_LOG}" ]] && docker ps -a >"${DOCKER_PS_LOG}" 2>/dev/null || true
  [[ -n "${ETCD_LOG}" ]] && docker logs chronos-etcd >"${ETCD_LOG}" 2>&1 || true
  local idx
  for ((idx = 0; idx < WORKER_COUNT; idx++)); do
    [[ -n "${READYZ_LOGS[idx]}" ]] && curl --max-time 2 -fsS "http://${METRICS_ENDPOINTS[idx]}/readyz" >"${READYZ_LOGS[idx]}" 2>&1 || true
    [[ -n "${METRICS_LOGS[idx]}" ]] && curl --max-time 2 -fsS "http://${METRICS_ENDPOINTS[idx]}/metrics" >"${METRICS_LOGS[idx]}" 2>&1 || true
  done
}

cleanup() {
  local exit_code=$?
  capture_diagnostics
  RESULT=$([[ ${exit_code} -eq 0 ]] && echo success || echo failure)
  write_profile_summary
  write_summary
  write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
  make etcd-reset >/dev/null 2>&1 || true
  local pid
  if [[ "${#CHRONOS_PIDS[@]}" -gt 0 ]]; then
    for pid in "${CHRONOS_PIDS[@]}"; do
      if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
        kill "${pid}" 2>/dev/null || true
        wait "${pid}" 2>/dev/null || true
      fi
    done
  fi
  make etcd-reset >/dev/null 2>&1 || true
  if [[ ${exit_code} -ne 0 ]]; then
    echo
    local idx
    for ((idx = 0; idx < WORKER_COUNT; idx++)); do
      echo "[scale] chronos-${idx} log: ${CHRONOS_LOGS[idx]}" >&2
    done
    echo "[scale] bench log: ${BENCH_LOG}" >&2
    echo "[scale] bench client logs: $(join_by_comma "${BENCH_CLIENT_LOGS[@]}")" >&2
    [[ -n "${ARTIFACT_DIR}" ]] && echo "[scale] artifacts: ${ARTIFACT_DIR}" >&2
  elif [[ "${KEEP_ARTIFACTS_ON_SUCCESS}" != "1" ]]; then
    rm -f "${BENCH_LOG}" "${BENCH_CLIENT_LOGS[@]}" "${CHRONOS_LOGS[@]}"
  fi
}
trap cleanup EXIT

assert_zero_metric_with_reasons() {
  local key=$1
  local file=$2
  local reason_key=$3
  if assert_zero_metric "${key}" "${file}"; then
    return 0
  fi

  local reasons
  reasons="$(extract_metric "${reason_key}" "${file}")"
  [[ -n "${reasons}" ]] && echo "metric ${reason_key}=${reasons}" >&2
  return 1
}

split_count_for_client() {
  local total=$1
  local client_idx=$2
  local base=$((total / BENCH_CLIENT_PROCESSES))
  local remainder=$((total % BENCH_CLIENT_PROCESSES))
  if [[ "${client_idx}" -lt "${remainder}" ]]; then
    printf '%s\n' "$((base + 1))"
  else
    printf '%s\n' "${base}"
  fi
}

aggregate_bench_logs() {
  local output=$1
  shift
  python3 - <<'PY' "${output}" "$@"
from collections import defaultdict
from pathlib import Path
import sys

output = Path(sys.argv[1])
logs = [Path(path) for path in sys.argv[2:]]
rows = []
for path in logs:
    metrics = {}
    with path.open(encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=", 1)
            metrics[key] = value
    rows.append(metrics)

if not rows:
    raise SystemExit("no benchmark logs to aggregate")

def first(key, default=""):
    for row in rows:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return default

def sum_float(key):
    return sum(float(row.get(key, "0") or 0) for row in rows)

def sum_int(key):
    return sum(int(float(row.get(key, "0") or 0)) for row in rows)

def max_int(key):
    return max((int(float(row.get(key, "0") or 0)) for row in rows), default=0)

def parse_counts(value):
    counts = defaultdict(int)
    if not value or value == "none":
        return counts
    for item in value.split(","):
        if not item:
            continue
        label, _, count = item.rpartition(":")
        if not label or not count:
            continue
        counts[label] += int(float(count))
    return counts

def format_counts(counts):
    if not counts:
        return "none"
    return ",".join(f"{label}:{counts[label]}" for label in sorted(counts))

route_counts = defaultdict(int)
failure_counts = defaultdict(int)
measured_failure_counts = defaultdict(int)
cold_failure_counts = defaultdict(int)
for row in rows:
    for owner, count in parse_counts(row.get("route_owner_counts", "")).items():
        route_counts[owner] += count
    for reason, count in parse_counts(row.get("allocation_failure_reasons", "")).items():
        failure_counts[reason] += count
    for reason, count in parse_counts(row.get("allocation_measured_failure_reasons", "")).items():
        measured_failure_counts[reason] += count
    for reason, count in parse_counts(row.get("cold_probe_failure_reasons", "")).items():
        cold_failure_counts[reason] += count

route_values = list(route_counts.values())
lines = [
    ("scenario", first("scenario")),
    ("endpoint", first("endpoint")),
    ("control_endpoints", first("control_endpoints")),
    ("route_to_owners", first("route_to_owners")),
    ("route_owner_endpoints", str(len(route_counts))),
    ("route_owner_min_timelines", str(min(route_values) if route_values else 0)),
    ("route_owner_max_timelines", str(max(route_values) if route_values else 0)),
    ("route_owner_counts", format_counts(route_counts)),
    ("concurrency", str(sum_int("concurrency"))),
    ("timelines", str(sum_int("timelines"))),
    ("batch", first("batch")),
    ("idempotency_enabled", first("idempotency_enabled")),
    ("owner_affinity", first("owner_affinity")),
    ("request_timeout_ms", first("request_timeout_ms")),
    ("client_timeout_ms", first("client_timeout_ms")),
    ("connect_timeout_ms", first("connect_timeout_ms")),
    ("connect_retry_interval_ms", first("connect_retry_interval_ms")),
    ("allocation_connection_pool_size", first("allocation_connection_pool_size")),
    ("bench_client_processes", str(len(rows))),
    ("cold_probe_enabled", first("cold_probe_enabled")),
    ("cold_probe_concurrency", str(sum_int("cold_probe_concurrency"))),
    ("cold_request_timeout_ms", first("cold_request_timeout_ms")),
    ("cold_client_timeout_ms", first("cold_client_timeout_ms")),
    ("cold_probe_requests", str(sum_int("cold_probe_requests"))),
    ("cold_probe_success_total", str(sum_int("cold_probe_success_total"))),
    ("cold_probe_failed_total", str(sum_int("cold_probe_failed_total"))),
    ("cold_probe_failure_reasons", format_counts(cold_failure_counts)),
    ("cold_probe_tsos", str(sum_int("cold_probe_tsos"))),
    ("cold_probe_latency_p50_us", str(max_int("cold_probe_latency_p50_us"))),
    ("cold_probe_latency_p95_us", str(max_int("cold_probe_latency_p95_us"))),
    ("cold_probe_latency_p99_us", str(max_int("cold_probe_latency_p99_us"))),
    ("cold_probe_latency_p999_us", str(max_int("cold_probe_latency_p999_us"))),
    ("cold_probe_latency_max_us", str(max_int("cold_probe_latency_max_us"))),
    ("duration_secs", first("duration_secs")),
    ("requests", str(sum_int("requests"))),
    ("tsos", str(sum_int("tsos"))),
    ("allocation_failed_total", str(sum_int("allocation_failed_total"))),
    ("allocation_measured_failed_total", str(sum_int("allocation_measured_failed_total"))),
    ("allocation_failure_reasons", format_counts(failure_counts)),
    ("allocation_measured_failure_reasons", format_counts(measured_failure_counts)),
    ("req_per_sec", f"{sum_float('req_per_sec'):.2f}"),
    ("tso_per_sec", f"{sum_float('tso_per_sec'):.2f}"),
    ("latency_p50_us", str(max_int("latency_p50_us"))),
    ("latency_p95_us", str(max_int("latency_p95_us"))),
    ("latency_p99_us", str(max_int("latency_p99_us"))),
    ("latency_p999_us", str(max_int("latency_p999_us"))),
    ("latency_max_us", str(max_int("latency_max_us"))),
]
output.write_text("".join(f"{key}={value}\n" for key, value in lines), encoding="utf-8")
PY
}

echo "[scale] resetting etcd"
make etcd-reset >/dev/null
echo "[scale] starting etcd"
make etcd-up >/dev/null
wait_for_etcd "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"

echo "[scale] preparing release binaries"
ensure_release_binaries "${RELEASE_BIN_DIR}" chronos chronos-bench

echo "[scale] validating ${WORKER_COUNT}-worker ownership plan"
for ((idx = 0; idx < WORKER_COUNT; idx++)); do
  env \
    CHRONOS_SECURITY_MODE=dev-insecure \
    CHRONOS_METADATA=etcd \
    CHRONOS_BIND_ADDR="${SERVICE_ENDPOINTS[idx]}" \
    CHRONOS_ADVERTISE_ENDPOINT="${ADVERTISE_ENDPOINTS[idx]}" \
    CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINTS[idx]}" \
    CHRONOS_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
    CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" \
    CHRONOS_WORKER_ID="${WORKER_IDS[idx]}" \
    CHRONOS_OWNERSHIP_PLAN_ID="${TIMELINE_SCENARIO}" \
    CHRONOS_GENERATOR_OWNERSHIP_MODULO="${WORKER_COUNT}" \
    CHRONOS_GENERATOR_OWNERSHIP_REMAINDER="${idx}" \
    CHRONOS_SAFETY_GAP_MS="${SAFETY_GAP_MS}" \
    "${RELEASE_BIN_DIR}/chronos" --check-config >/dev/null
done

for ((idx = 0; idx < WORKER_COUNT; idx++)); do
  echo "[scale] starting chronos worker ${idx}"
  env \
    CHRONOS_SECURITY_MODE=dev-insecure \
    CHRONOS_METADATA=etcd \
    CHRONOS_BIND_ADDR="${SERVICE_ENDPOINTS[idx]}" \
    CHRONOS_ADVERTISE_ENDPOINT="${ADVERTISE_ENDPOINTS[idx]}" \
    CHRONOS_METRICS_BIND_ADDR="${METRICS_ENDPOINTS[idx]}" \
    CHRONOS_ETCD_ENDPOINTS="${ETCD_ENDPOINTS}" \
    CHRONOS_ETCD_PREFIX="${ETCD_PREFIX}" \
    CHRONOS_WORKER_ID="${WORKER_IDS[idx]}" \
    CHRONOS_OWNERSHIP_PLAN_ID="${TIMELINE_SCENARIO}" \
    CHRONOS_GENERATOR_OWNERSHIP_MODULO="${WORKER_COUNT}" \
    CHRONOS_GENERATOR_OWNERSHIP_REMAINDER="${idx}" \
    CHRONOS_SAFETY_GAP_MS="${SAFETY_GAP_MS}" \
    "${RELEASE_BIN_DIR}/chronos" >"${CHRONOS_LOGS[idx]}" 2>&1 &
  CHRONOS_PIDS+=("$!")
done

for ((idx = 0; idx < WORKER_COUNT; idx++)); do
  wait_for_http "http://${METRICS_ENDPOINTS[idx]}/readyz" "chronos worker ${idx} readyz" "${WAIT_ATTEMPTS}" "${WAIT_INTERVAL_SECS}"
done
if [[ "${POST_READY_SLEEP_SECS}" -gt 0 ]]; then
  echo "[scale] waiting ${POST_READY_SLEEP_SECS}s for route watchers and leases to settle"
  sleep "${POST_READY_SLEEP_SECS}"
fi

echo "[scale] running ${WORKER_COUNT}-worker route-aware allocation benchmark with ${BENCH_CLIENT_PROCESSES} client process(es)"
: >"${BENCH_LOG}"
BENCH_CLIENT_PIDS=()
for ((idx = 0; idx < BENCH_CLIENT_PROCESSES; idx++)); do
  client_concurrency="$(split_count_for_client "${BENCH_CONCURRENCY}" "${idx}")"
  client_timelines="$(split_count_for_client "${BENCH_TIMELINES}" "${idx}")"
  client_cold_probe_concurrency="$(split_count_for_client "${COLD_PROBE_CONCURRENCY}" "${idx}")"
  if [[ "${client_cold_probe_concurrency}" -eq 0 ]]; then
    client_cold_probe_concurrency=1
  fi
  env \
    CHRONOS_BENCH_ENDPOINT="${HTTP_SERVICE_ENDPOINTS[0]}" \
    CHRONOS_BENCH_CONTROL_ENDPOINTS="${CONTROL_ENDPOINTS}" \
    CHRONOS_BENCH_ROUTE_TO_OWNERS=true \
    CHRONOS_BENCH_SCENARIO="${TIMELINE_SCENARIO}-client-${idx}" \
    CHRONOS_BENCH_CONCURRENCY="${client_concurrency}" \
    CHRONOS_BENCH_TIMELINES="${client_timelines}" \
    CHRONOS_BENCH_BATCH="${BENCH_BATCH}" \
    CHRONOS_BENCH_DURATION_SECS="${BENCH_DURATION_SECS}" \
    CHRONOS_BENCH_WARMUP_SECS="${BENCH_WARMUP_SECS}" \
    CHRONOS_BENCH_REQUEST_TIMEOUT_MS="${BENCH_REQUEST_TIMEOUT_MS}" \
    CHRONOS_BENCH_CLIENT_TIMEOUT_MS="${BENCH_CLIENT_TIMEOUT_MS}" \
    CHRONOS_BENCH_CONNECT_TIMEOUT_MS="${BENCH_CONNECT_TIMEOUT_MS}" \
    CHRONOS_BENCH_CONNECT_RETRY_INTERVAL_MS="${BENCH_CONNECT_RETRY_INTERVAL_MS}" \
    CHRONOS_BENCH_ALLOCATION_CONNECTION_POOL_SIZE="${BENCH_ALLOCATION_CONNECTION_POOL_SIZE}" \
    CHRONOS_BENCH_COLD_PROBE="${COLD_PROBE_ENABLED}" \
    CHRONOS_BENCH_COLD_PROBE_CONCURRENCY="${client_cold_probe_concurrency}" \
    CHRONOS_BENCH_COLD_REQUEST_TIMEOUT_MS="${COLD_REQUEST_TIMEOUT_MS}" \
    CHRONOS_BENCH_COLD_CLIENT_TIMEOUT_MS="${COLD_CLIENT_TIMEOUT_MS}" \
    "${RELEASE_BIN_DIR}/chronos-bench" >"${BENCH_CLIENT_LOGS[idx]}" 2>&1 &
  BENCH_CLIENT_PIDS+=("$!")
done

BENCH_CLIENT_FAILED=0
for pid in "${BENCH_CLIENT_PIDS[@]}"; do
  if ! wait "${pid}"; then
    BENCH_CLIENT_FAILED=1
  fi
done
if [[ "${BENCH_CLIENT_FAILED}" -ne 0 ]]; then
  echo "[scale] one or more benchmark client processes failed" >&2
  exit 1
fi
aggregate_bench_logs "${BENCH_LOG}" "${BENCH_CLIENT_LOGS[@]}"
cat "${BENCH_LOG}"

for ((idx = 0; idx < WORKER_COUNT; idx++)); do
  assert_http_body_equals "http://${METRICS_ENDPOINTS[idx]}/readyz" "ready"
  assert_http_metric_present "http://${METRICS_ENDPOINTS[idx]}/metrics" '^tso_allocate_total'
  assert_http_metric_absent_or_zero "http://${METRICS_ENDPOINTS[idx]}/metrics" "tso_metadata_errors_total"
  assert_http_metric_absent_or_zero "http://${METRICS_ENDPOINTS[idx]}/metrics" "tso_clock_backwards_total"
  assert_http_metric_absent_or_zero "http://${METRICS_ENDPOINTS[idx]}/metrics" "tso_lease_expired_total"
  assert_http_metric_absent_or_zero "http://${METRICS_ENDPOINTS[idx]}/metrics" "tso_timeline_proxy_timeout_total"
done

assert_metric_at_least "route_owner_endpoints" "${BENCH_LOG}" "${ROUTE_OWNER_ENDPOINTS_MIN}"
assert_metric_at_least "route_owner_min_timelines" "${BENCH_LOG}" "1"
assert_zero_metric_with_reasons "allocation_failed_total" "${BENCH_LOG}" "allocation_failure_reasons"
assert_metric_at_least "req_per_sec" "${BENCH_LOG}" "${REQ_PER_SEC_MIN}"
assert_metric_at_most "latency_p95_us" "${BENCH_LOG}" "${LATENCY_P95_US_MAX}"
assert_metric_at_most "latency_p99_us" "${BENCH_LOG}" "${LATENCY_P99_US_MAX}"
assert_metric_at_most "latency_p999_us" "${BENCH_LOG}" "${LATENCY_P999_US_MAX}"
if [[ "${COLD_PROBE_ENABLED}" == "true" || "${COLD_PROBE_ENABLED}" == "1" ]]; then
  assert_zero_metric_with_reasons "cold_probe_failed_total" "${BENCH_LOG}" "cold_probe_failure_reasons"
  assert_metric_at_most "cold_probe_latency_p95_us" "${BENCH_LOG}" "${COLD_PROBE_LATENCY_P95_US_MAX}"
  assert_metric_at_most "cold_probe_latency_p999_us" "${BENCH_LOG}" "${COLD_PROBE_LATENCY_P999_US_MAX}"
fi

RESULT="success"
write_summary
write_artifact_index "${ARTIFACT_DIR}" "${INDEX_LOG}"
echo "[scale] success"
