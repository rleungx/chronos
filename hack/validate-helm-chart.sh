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
fail!("runtime.safetyGapMs must cover runtime.maxClockSkewMs") unless values.dig("runtime", "safetyGapMs").to_i >= values.dig("runtime", "maxClockSkewMs").to_i
fail!("runtime.maxTimelineRecords must be positive") unless values.dig("runtime", "maxTimelineRecords").to_i > 0
fail!("runtime.grpcMaxConnections must be positive") unless values.dig("runtime", "grpcMaxConnections").to_i > 0
fail!("runtime.terminationGracePeriodSeconds must leave time for bounded shutdown") unless values.dig("runtime", "terminationGracePeriodSeconds").to_i >= 120
replica_schema = schema.dig("properties", "replicaCount", "anyOf")
fail!("values.schema.json must allow only drain-zero or at least three replicas") unless replica_schema == [{"const" => 0}, {"minimum" => 3}]
fail!("values.schema.json must reject latest image tag") unless schema.dig("properties", "image", "properties", "tag", "not", "const") == "latest"
fail!("values.schema.json must reject unknown image properties") unless schema.dig("properties", "image", "additionalProperties") == false
fail!("values.schema.json must reject unknown etcd properties") unless schema.dig("properties", "etcd", "additionalProperties") == false
fail!("values.schema.json must require mTLS security mode") unless schema.dig("properties", "security", "properties", "mode", "const") == "required"
fail!("values.schema.json must reject unknown security properties") unless schema.dig("properties", "security", "additionalProperties") == false
fail!("values.schema.json must reject unknown topology spread properties") unless schema.dig("properties", "topologySpreadConstraints", "additionalProperties") == false
fail!("values.schema.json must constrain ownership shard count") unless schema.dig("properties", "ownership", "properties", "shardCount", "minimum") == 3
fail!("values.schema.json must reject removed ownership escape hatches") unless schema.dig("properties", "ownership", "additionalProperties") == false
fail!("values.schema.json must preserve the bounded shutdown window") unless schema.dig("properties", "runtime", "properties", "terminationGracePeriodSeconds", "minimum") == 120
' "${chart}"

require_contains "${chart}/templates/configmap.yaml" "CHRONOS_GENERATOR_OWNERSHIP_MODULO"
require_contains "${chart}/templates/configmap.yaml" "CHRONOS_OWNERSHIP_WORKER_COUNT"
require_contains "${chart}/templates/configmap.yaml" 'refusing ownership topology change'
require_contains "${chart}/templates/configmap.yaml" 'refusing cluster-format change'
require_contains "${chart}/templates/configmap.yaml" '$existingStatefulSet.status.replicas'
require_contains "${chart}/templates/configmap.yaml" 'CHRONOS_CLUSTER_FORMAT_VERSION'
require_contains "${chart}/templates/configmap.yaml" 'CHRONOS_GRPC_MAX_CONNECTIONS'
require_contains "${chart}/templates/_helpers.tpl" 'chronos.ownershipWorkerCount'
require_contains "${chart}/templates/_helpers.tpl" 'chronos.clusterFormatVersion'
require_contains "${chart}/templates/statefulset.yaml" "--print-ownership-env"
require_contains "${chart}/templates/statefulset.yaml" "checksum/config"
require_contains "${chart}/templates/statefulset.yaml" "chronos.io/tls-revision"
require_contains "${chart}/templates/statefulset.yaml" "chronos.io/allowlist-revision"
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
if grep -R "allowUnsafeInPlaceMigration" "${chart}" >/dev/null; then
  echo "Helm chart must not expose an unsafe in-place ownership migration bypass" >&2
  exit 1
fi
if grep -R "1103515245\|2147483647\|worker \* 97" "${chart}/templates" >/dev/null; then
  echo "Helm chart must not inline ownership hash constants" >&2
  exit 1
fi

if command -v helm >/dev/null 2>&1; then
  helm lint "${chart}"
  custom_image_rendered="$(helm template chronos "${chart}" --set-string image.tag=0.1.1)"
  if [[ "$(grep -Fc 'image: "ghcr.io/rleungx/chronos:0.1.1"' <<<"${custom_image_rendered}")" -ne 2 ]]; then
    echo "Helm chart must render the custom image tag for both Chronos containers" >&2
    exit 1
  fi
  custom_digest="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  custom_digest_rendered="$(
    helm template chronos "${chart}" --set-string image.digest="${custom_digest}"
  )"
  if [[ "$(grep -Fc "image: \"ghcr.io/rleungx/chronos@${custom_digest}\"" <<<"${custom_digest_rendered}")" -ne 2 ]]; then
    echo "Helm chart must render the custom image digest for both Chronos containers" >&2
    exit 1
  fi
  if typo_error="$(helm template chronos "${chart}" \
    --set image.digset=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
    2>&1 >/dev/null)"; then
    echo "Helm chart accepted unknown image.digset typo" >&2
    exit 1
  fi
  if ! grep -Fq 'Additional property digset is not allowed' <<<"${typo_error}"; then
    echo "Helm chart rejected image.digset for an unexpected reason: ${typo_error}" >&2
    exit 1
  fi
  custom_etcd_rendered="$(
    helm template chronos "${chart}" \
      --set-string etcd.endpoints=https://custom-etcd.example:2379
  )"
  if ! grep -Fq 'CHRONOS_ETCD_ENDPOINTS: "https://custom-etcd.example:2379"' \
    <<<"${custom_etcd_rendered}"; then
    echo "Helm chart did not render the custom etcd endpoint" >&2
    exit 1
  fi
  if typo_error="$(helm template chronos "${chart}" \
    --set-string etcd.endponts=https://wrong.example:2379 \
    2>&1 >/dev/null)"; then
    echo "Helm chart accepted unknown etcd.endponts typo" >&2
    exit 1
  fi
  if ! grep -Fq 'Additional property endponts is not allowed' <<<"${typo_error}"; then
    echo "Helm chart rejected etcd.endponts for an unexpected reason: ${typo_error}" >&2
    exit 1
  fi
  custom_tls_revision_rendered="$(
    helm template chronos "${chart}" --set-string security.tlsRevision=2
  )"
  ruby --disable=gems -ryaml -e '
    documents = YAML.load_stream(STDIN.read)
    stateful_set = documents.find { |document| document.is_a?(Hash) && document["kind"] == "StatefulSet" }
    annotation = stateful_set&.dig("spec", "template", "metadata", "annotations", "chronos.io/tls-revision")
    abort("Helm chart did not render the custom TLS revision rollout annotation") unless annotation == "2"
  ' <<<"${custom_tls_revision_rendered}"
  if typo_error="$(helm template chronos "${chart}" \
    --set-string security.tlsRevison=2 \
    2>&1 >/dev/null)"; then
    echo "Helm chart accepted unknown security.tlsRevison typo" >&2
    exit 1
  fi
  if ! grep -Fq 'Additional property tlsRevison is not allowed' <<<"${typo_error}"; then
    echo "Helm chart rejected security.tlsRevison for an unexpected reason: ${typo_error}" >&2
    exit 1
  fi
  custom_topology_rendered="$(
    helm template chronos "${chart}" \
      --set-string topologySpreadConstraints.topologyKey=kubernetes.io/hostname
  )"
  ruby --disable=gems -ryaml -e '
    documents = YAML.load_stream(STDIN.read)
    stateful_set = documents.find { |document| document.is_a?(Hash) && document["kind"] == "StatefulSet" }
    constraints = stateful_set&.dig("spec", "template", "spec", "topologySpreadConstraints") || []
    abort("Helm chart did not render exactly one custom topology spread constraint") unless constraints.length == 1
    abort("Helm chart did not render the custom topology key") unless constraints[0]["topologyKey"] == "kubernetes.io/hostname"
  ' <<<"${custom_topology_rendered}"
  disabled_topology_rendered="$(
    helm template chronos "${chart}" --set topologySpreadConstraints.enabled=false
  )"
  ruby --disable=gems -ryaml -e '
    documents = YAML.load_stream(STDIN.read)
    stateful_set = documents.find { |document| document.is_a?(Hash) && document["kind"] == "StatefulSet" }
    constraints = stateful_set&.dig("spec", "template", "spec", "topologySpreadConstraints")
    abort("Helm chart rendered disabled topology spread constraints") unless constraints.nil?
  ' <<<"${disabled_topology_rendered}"
  if typo_error="$(helm template chronos "${chart}" \
    --set-string topologySpreadConstraints.toplogyKey=failure-domain.example/zone \
    2>&1 >/dev/null)"; then
    echo "Helm chart accepted unknown topologySpreadConstraints.toplogyKey typo" >&2
    exit 1
  fi
  if ! grep -Fq 'Additional property toplogyKey is not allowed' <<<"${typo_error}"; then
    echo "Helm chart rejected topologySpreadConstraints.toplogyKey for an unexpected reason: ${typo_error}" >&2
    exit 1
  fi
  if helm template chronos "${chart}" --set ownership.allowUnsafeInPlaceMigration=true >/dev/null 2>&1; then
    echo "Helm chart accepted the removed unsafe in-place migration option" >&2
    exit 1
  fi
  rendered="$(mktemp)"
  trap 'rm -f "${rendered}"' EXIT
  helm template chronos "${chart}" --namespace chronos >"${rendered}"
  if [[ "$(grep -Fc 'image: "ghcr.io/rleungx/chronos:0.1.0"' "${rendered}")" -ne 2 ]]; then
    echo "Helm chart must render the default image for both Chronos containers" >&2
    exit 1
  fi
  grep -Fq 'CHRONOS_ETCD_ENDPOINTS: "https://etcd-client.etcd.svc.cluster.local:2379"' \
    "${rendered}"
  ruby --disable=gems -ryaml -e '
    documents = YAML.load_stream(File.read(ARGV.fetch(0)))
    config_map = documents.find { |document| document.is_a?(Hash) && document["kind"] == "ConfigMap" }
    stateful_set = documents.find { |document| document.is_a?(Hash) && document["kind"] == "StatefulSet" }
    abort("missing rendered ConfigMap or StatefulSet") unless config_map && stateful_set
    abort("default security mode was not rendered") unless config_map.dig("data", "CHRONOS_SECURITY_MODE") == "required"
    annotations = stateful_set.dig("spec", "template", "metadata", "annotations") || {}
    abort("default TLS revision was not rendered") unless annotations["chronos.io/tls-revision"] == "1"
    abort("default allowlist revision was not rendered") unless annotations["chronos.io/allowlist-revision"] == "1"
    pod_spec = stateful_set.dig("spec", "template", "spec") || {}
    topology_constraints = pod_spec["topologySpreadConstraints"] || []
    abort("default topology spread constraint was not rendered") unless topology_constraints.length == 1
    abort("default topology key was not rendered") unless topology_constraints[0]["topologyKey"] == "topology.kubernetes.io/zone"
    abort("default topology scheduling policy was not rendered") unless topology_constraints[0]["whenUnsatisfiable"] == "DoNotSchedule"
    chronos_container = (pod_spec["containers"] || []).find { |container| container["name"] == "chronos" }
    allowlist_refs = (chronos_container&.fetch("env", []) || []).map do |entry|
      entry.dig("valueFrom", "secretKeyRef", "name")
    end.compact
    unless allowlist_refs.count("chronos-client-cert-allowlist") == 4
      abort("default client certificate allowlist Secret refs were not rendered")
    end
    secret_volumes = (pod_spec["volumes"] || []).each_with_object({}) do |volume, result|
      result[volume["name"]] = volume.dig("secret", "secretName")
    end
    abort("default gRPC TLS Secret ref was not rendered") unless secret_volumes["grpc-tls-input"] == "chronos-grpc-tls"
    abort("default metrics TLS Secret ref was not rendered") unless secret_volumes["metrics-tls-input"] == "chronos-metrics-tls"
  ' "${rendered}"
  grep -Fq "CHRONOS_GENERATOR_OWNERSHIP_MODULO: \"256\"" "${rendered}"
  grep -Fq "CHRONOS_OWNERSHIP_WORKER_COUNT: \"3\"" "${rendered}"
  grep -Fq "CHRONOS_OWNERSHIP_ASSIGNMENT_SEED: \"20260516\"" "${rendered}"
  grep -Fq "CHRONOS_OWNERSHIP_PLAN_ID: \"chronos-rendezvous-shards-256-workers-3-seed-20260516\"" "${rendered}"
  grep -Fq "CHRONOS_GRPC_MAX_REQUEST_BYTES: \"1048576\"" "${rendered}"
  if grep -Eq 'CHRONOS_[A-Z0-9_]+: \"[0-9]+(\\.[0-9]+)?e[+-][0-9]+\"' "${rendered}"; then
    echo "Helm chart must render numeric Chronos environment values as decimal integers" >&2
    exit 1
  fi
  grep -Fq "minAvailable: 2" "${rendered}"
elif [[ "${CI:-false}" == "true" || "${CHRONOS_REQUIRE_HELM_RENDER:-0}" == "1" ]]; then
  echo "helm is required for Helm render validation in CI" >&2
  exit 1
else
  echo "helm not found; static Helm chart validation completed"
fi

echo "${chart}: ok"
