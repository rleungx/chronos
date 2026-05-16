#!/usr/bin/env bash

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

prom_metric_average_us() {
  local file=$1
  local count_metric=$2
  local sum_metric=$3
  local count
  local sum
  count="$(prom_metric_value "${file}" "${count_metric}")"
  sum="$(prom_metric_value "${file}" "${sum_metric}")"
  [[ -n "${count}" && -n "${sum}" ]] || return 0
  python3 - "${count}" "${sum}" <<'PY'
import sys

count = float(sys.argv[1])
total_seconds = float(sys.argv[2])
if count <= 0:
    raise SystemExit(0)
print(f"{(total_seconds / count) * 1_000_000:.2f}")
PY
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

prom_histogram_quantile_upper_bound_us() {
  local file=$1
  local prefix=$2
  local quantile=$3
  local upper_bound
  upper_bound="$(prom_histogram_quantile_upper_bound "${file}" "${prefix}" "${quantile}")"
  [[ -n "${upper_bound}" && "${upper_bound}" != "+Inf" ]] || return 0
  python3 - "${upper_bound}" <<'PY'
import sys

print(f"{float(sys.argv[1]) * 1_000_000:.0f}")
PY
}
