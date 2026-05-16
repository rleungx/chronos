#!/usr/bin/env bash
set -euo pipefail

chart="${1:-deploy/helm/chronos}"

require_file() {
  local file=$1
  [[ -f "${file}" ]] || {
    echo "missing Helm chart file: ${file}" >&2
    exit 1
  }
}

require_contains() {
  local file=$1
  local pattern=$2
  if ! grep -Fq -- "${pattern}" "${file}"; then
    echo "missing Helm chart pattern in ${file}: ${pattern}" >&2
    exit 1
  fi
}

require_file "${chart}/Chart.yaml"
require_file "${chart}/values.yaml"
require_file "${chart}/values.schema.json"
require_file "${chart}/templates/_helpers.tpl"
require_file "${chart}/templates/configmap.yaml"
require_file "${chart}/templates/statefulset.yaml"
require_file "${chart}/templates/pdb.yaml"
require_file "${chart}/templates/networkpolicy.yaml"
require_file "${chart}/templates/servicemonitor.yaml"

if ! command -v ruby >/dev/null 2>&1; then
  echo "ruby is required for Helm chart static validation" >&2
  exit 1
fi

ruby --disable=gems -rjson -ryaml -e '
chart = ARGV.fetch(0)
values = YAML.load_file(File.join(chart, "values.yaml"))
schema = JSON.parse(File.read(File.join(chart, "values.schema.json")))

def fail!(message)
  warn(message)
  exit(1)
end

replicas = values.fetch("replicaCount").to_i
shards = values.dig("ownership", "shardCount").to_i
fail!("replicaCount must be at least 3") if replicas < 3
fail!("ownership.shardCount must be at least replicaCount") if shards < replicas
fail!("image.tag must not be latest") if values.dig("image", "tag") == "latest"
fail!("security.mode must be required") unless values.dig("security", "mode") == "required"
fail!("etcd.endpoints must use https") unless values.dig("etcd", "endpoints").to_s.start_with?("https://")
fail!("etcd.prefix must be absolute") unless values.dig("etcd", "prefix").to_s.start_with?("/")
fail!("serviceMonitor must be enabled by default") unless values.dig("serviceMonitor", "enabled") == true
fail!("networkPolicy must be enabled by default") unless values.dig("networkPolicy", "enabled") == true
fail!("values.schema.json must constrain replicaCount") unless schema.dig("properties", "replicaCount", "minimum") == 3
fail!("values.schema.json must reject latest image tag") unless schema.dig("properties", "image", "properties", "tag", "not", "const") == "latest"
fail!("values.schema.json must require mTLS security mode") unless schema.dig("properties", "security", "properties", "mode", "const") == "required"
fail!("values.schema.json must constrain ownership shard count") unless schema.dig("properties", "ownership", "properties", "shardCount", "minimum") == 3
' "${chart}"

require_contains "${chart}/templates/configmap.yaml" "CHRONOS_GENERATOR_OWNERSHIP_MODULO"
require_contains "${chart}/templates/configmap.yaml" "CHRONOS_OWNERSHIP_WORKER_COUNT"
require_contains "${chart}/templates/configmap.yaml" "{{ .Values.replicaCount | quote }}"
require_contains "${chart}/templates/statefulset.yaml" "--print-ownership-env"
require_contains "${chart}/templates/statefulset.yaml" 'exec /usr/local/bin/chronos'
require_contains "${chart}/templates/statefulset.yaml" 'image: {{ include "chronos.image" . | quote }}'
require_contains "${chart}/templates/statefulset.yaml" '{{- $affinity := default dict .Values.affinity }}'
require_contains "${chart}/templates/statefulset.yaml" '{{- $nodeAffinity := get $affinity "nodeAffinity" }}'
require_contains "${chart}/templates/statefulset.yaml" '{{- $podAffinity := get $affinity "podAffinity" }}'
require_contains "${chart}/templates/statefulset.yaml" '{{- $customPodAntiAffinity := get $affinity "podAntiAffinity" }}'
require_contains "${chart}/templates/pdb.yaml" "minAvailable: {{ sub (int .Values.replicaCount) 1 }}"

if grep -R "debian:bookworm-slim" "${chart}/templates" >/dev/null; then
  echo "Helm chart must not introduce a separate Debian runtime image; init containers should reuse the Chronos image" >&2
  exit 1
fi
if grep -R "kind: HorizontalPodAutoscaler" "${chart}/templates" >/dev/null; then
  echo "Helm chart must not define HPA for static partitioned ownership" >&2
  exit 1
fi
if grep -R "1103515245\|2147483647\|worker \* 97" "${chart}/templates" >/dev/null; then
  echo "Helm chart must not inline ownership hash constants" >&2
  exit 1
fi

if command -v helm >/dev/null 2>&1; then
  helm lint "${chart}"
  rendered="$(mktemp)"
  trap 'rm -f "${rendered}"' EXIT
  helm template chronos "${chart}" --namespace chronos >"${rendered}"
  grep -Fq "CHRONOS_GENERATOR_OWNERSHIP_MODULO: \"256\"" "${rendered}"
  grep -Fq "CHRONOS_OWNERSHIP_WORKER_COUNT: \"3\"" "${rendered}"
  grep -Fq "minAvailable: 2" "${rendered}"
elif [[ "${CI:-false}" == "true" || "${CHRONOS_REQUIRE_HELM_RENDER:-0}" == "1" ]]; then
  echo "helm is required for Helm render validation in CI" >&2
  exit 1
else
  echo "helm not found; static Helm chart validation completed"
fi

echo "${chart}: ok"
