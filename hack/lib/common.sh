#!/usr/bin/env bash

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

require_non_negative_integer() {
  require_nonnegative_integer "$@"
}

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
    if curl --max-time 2 -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep "${wait_interval_secs}"
  done
  echo "${name} did not become healthy: ${url}" >&2
  return 1
}

ensure_release_binaries() {
  local bin_dir=$1
  shift
  if [[ "${CHRONOS_SKIP_RELEASE_BUILD:-0}" == "1" ]]; then
    local bin
    for bin in "$@"; do
      if [[ ! -x "${bin_dir}/${bin}" ]]; then
        echo "missing release binary ${bin_dir}/${bin}; unset CHRONOS_SKIP_RELEASE_BUILD or run make release-package first" >&2
        return 1
      fi
    done
    return 0
  fi

  local cargo_args=(cargo build --locked --release)
  local bin
  for bin in "$@"; do
    cargo_args+=(--bin "${bin}")
  done
  "${cargo_args[@]}" >/dev/null
}

assert_http_body_equals() {
  local url=$1
  local expected=$2
  local body
  body="$(curl -fsS "${url}")" || {
    echo "failed to fetch ${url}" >&2
    return 1
  }
  if [[ "${body}" != "${expected}" ]]; then
    echo "unexpected response from ${url}: expected '${expected}', got '${body}'" >&2
    return 1
  fi
}

assert_http_metric_present() {
  local url=$1
  local pattern=$2
  local snapshot
  snapshot="$(mktemp "${TMPDIR:-/tmp}/chronos-metrics.XXXXXX")" || {
    echo "failed to create temporary metrics snapshot" >&2
    return 1
  }
  if ! curl -fsS "${url}" >"${snapshot}"; then
    rm -f "${snapshot}"
    echo "failed to fetch metrics from ${url}" >&2
    return 1
  fi
  if ! grep -q "${pattern}" "${snapshot}"; then
    rm -f "${snapshot}"
    echo "missing metric pattern ${pattern} from ${url}" >&2
    return 1
  fi
  rm -f "${snapshot}"
}

assert_http_metric_absent_or_zero() {
  local url=$1
  local metric=$2
  local snapshot
  snapshot="$(mktemp "${TMPDIR:-/tmp}/chronos-metrics.XXXXXX")" || {
    echo "failed to create temporary metrics snapshot" >&2
    return 1
  }
  if ! curl -fsS "${url}" >"${snapshot}"; then
    rm -f "${snapshot}"
    echo "failed to fetch metrics from ${url}" >&2
    return 1
  fi
  if ! awk -v metric="${metric}" '
    $0 !~ /^#/ {
      name = $1
      if ((name == metric || index(name, metric "{") == 1) && ($2 + 0) != 0) {
        print
        bad = 1
      }
    }
    END { exit bad ? 1 : 0 }
  ' "${snapshot}"; then
    rm -f "${snapshot}"
    echo "metric ${metric} must be absent or zero in ${url}" >&2
    return 1
  fi
  rm -f "${snapshot}"
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

wait_for_etcd_cluster() {
  local wait_attempts=$1
  local wait_interval_secs=$2
  local endpoints=$3
  for _attempt in $(seq 1 "${wait_attempts}"); do
    if docker exec chronos-etcd-1 /usr/local/bin/etcdctl --endpoints="${endpoints}" endpoint health >/dev/null 2>&1; then
      return 0
    fi
    sleep "${wait_interval_secs}"
  done
  echo "clustered etcd did not become healthy after ${wait_attempts} attempts" >&2
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

  local host
  if [[ "${service_endpoint}" == \[*\]:* ]]; then
    host="${service_endpoint%%]:*}]"
  else
    host="${service_endpoint%:*}"
  fi

  case "${host}" in
    0.0.0.0 | "[::]" | "::")
      printf '127.0.0.1:%s\n' "${port}"
      ;;
    *)
      printf '%s\n' "${service_endpoint}"
      ;;
  esac
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

assert_summary_success() {
  local file=$1
  [[ -f "${file}" ]] || {
    echo "missing summary file ${file}" >&2
    return 1
  }
  grep -q '^result=success$' "${file}" || {
    echo "summary does not report success: ${file}" >&2
    return 1
  }
}
