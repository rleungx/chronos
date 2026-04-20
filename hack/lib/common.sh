#!/usr/bin/env bash

write_artifact_index() {
  local artifact_dir=$1
  local index_log=$2
  [[ -n "${index_log}" ]] || return 0
  mkdir -p "${artifact_dir}"
  python3 - <<'PY' "${artifact_dir}" "${index_log}"
from pathlib import Path
import sys

artifact_dir = Path(sys.argv[1])
index_path = Path(sys.argv[2])
lines = []
for path in sorted(p for p in artifact_dir.iterdir() if p.is_file()):
    lines.append(f"{path.name}\t{path.stat().st_size}")
index_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
PY
}

wait_for_http() {
  local url=$1
  local name=$2
  local wait_attempts=$3
  local wait_interval_secs=$4
  for _attempt in $(seq 1 "${wait_attempts}"); do
    if curl --max-time 2 -fsS "${url}" >/dev/null; then
      return 0
    fi
    sleep "${wait_interval_secs}"
  done
  echo "${name} did not become healthy: ${url}" >&2
  return 1
}

wait_for_etcd() {
  local wait_attempts=$1
  local wait_interval_secs=$2
  for _attempt in $(seq 1 "${wait_attempts}"); do
    if make etcd-health >/dev/null 2>&1; then
      return 0
    fi
    sleep "${wait_interval_secs}"
  done
  echo "etcd did not become healthy after ${wait_attempts} attempts" >&2
  return 1
}

derive_local_advertise_endpoint() {
  local service_endpoint=$1
  local alias=$2
  local override=${3:-}

  if [[ -n "${override}" ]]; then
    printf '%s\n' "${override}"
    return 0
  fi

  if [[ "${service_endpoint}" != *:* ]]; then
    echo "service endpoint must be host:port, got: ${service_endpoint}" >&2
    return 1
  fi

  local port=${service_endpoint##*:}
  if [[ -z "${port}" ]]; then
    echo "service endpoint must include a port, got: ${service_endpoint}" >&2
    return 1
  fi

  printf '%s.localhost:%s\n' "${alias}" "${port}"
}

extract_metric() {
  local key=$1
  local file=$2
  awk -F '=' -v key="${key}" '$1 == key { print $2; exit }' "${file}"
}

assert_metric_present() {
  local key=$1
  local file=$2
  local value
  value="$(extract_metric "${key}" "${file}")"
  if [[ -z "${value}" ]]; then
    echo "missing metric ${key} in ${file}" >&2
    return 1
  fi
}

assert_positive_metric() {
  local key=$1
  local file=$2
  local value
  value="$(extract_metric "${key}" "${file}")"
  if [[ -z "${value}" ]]; then
    echo "missing metric ${key} in ${file}" >&2
    return 1
  fi
  python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) > 0 else 1)' "${value}" || {
    echo "metric ${key} must be > 0, got ${value}" >&2
    return 1
  }
}

assert_zero_metric() {
  local key=$1
  local file=$2
  local value
  value="$(extract_metric "${key}" "${file}")"
  if [[ -z "${value}" ]]; then
    echo "missing metric ${key} in ${file}" >&2
    return 1
  fi
  [[ "${value}" == "0" ]] || {
    echo "metric ${key} must be 0, got ${value}" >&2
    return 1
  }
}

assert_metric_at_least() {
  local key=$1
  local file=$2
  local minimum=$3
  local value
  value="$(extract_metric "${key}" "${file}")"
  if [[ -z "${value}" ]]; then
    echo "missing metric ${key} in ${file}" >&2
    return 1
  fi
  python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) >= float(sys.argv[2]) else 1)' "${value}" "${minimum}" || {
    echo "metric ${key} must be >= ${minimum}, got ${value}" >&2
    return 1
  }
}

assert_metric_at_most() {
  local key=$1
  local file=$2
  local maximum=$3
  local value
  value="$(extract_metric "${key}" "${file}")"
  if [[ -z "${value}" ]]; then
    echo "missing metric ${key} in ${file}" >&2
    return 1
  fi
  python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) <= float(sys.argv[2]) else 1)' "${value}" "${maximum}" || {
    echo "metric ${key} must be <= ${maximum}, got ${value}" >&2
    return 1
  }
}
