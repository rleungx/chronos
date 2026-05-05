#!/usr/bin/env bash
set -euo pipefail

manifest="${1:-deploy/kubernetes/chronos.yaml}"

ruby -ryaml -e '
path = ARGV.fetch(0)
docs = YAML.load_stream(File.read(path)).compact

def fail!(message)
  warn(message)
  exit(1)
end

def dig(document, *path)
  path.reduce(document) do |value, key|
    return nil unless value.is_a?(Hash)
    value[key]
  end
end

def named(docs, kind, name)
  docs.find { |doc| doc["kind"] == kind && dig(doc, "metadata", "name") == name }
end

def env_value(container, name)
  Array(container["env"]).find { |entry| entry["name"] == name }
end

required = {
  "Namespace" => "chronos",
  "ServiceAccount" => "chronos",
  "ConfigMap" => "chronos-config",
  "PodDisruptionBudget" => "chronos",
  "StatefulSet" => "chronos",
  "HorizontalPodAutoscaler" => "chronos",
  "NetworkPolicy" => "chronos-ingress",
  "ServiceMonitor" => "chronos"
}
required.each do |kind, name|
  fail!("missing #{kind}/#{name}") unless named(docs, kind, name)
end

%w[chronos chronos-headless].each do |service_name|
  fail!("missing Service/#{service_name}") unless named(docs, "Service", service_name)
end

config = named(docs, "ConfigMap", "chronos-config")
data = config.fetch("data", {})
{
  "CHRONOS_PROFILE" => "production",
  "CHRONOS_METADATA" => "etcd",
  "CHRONOS_SECURITY_MODE" => "required",
  "CHRONOS_BIND_ADDR" => "0.0.0.0:50051",
  "CHRONOS_HEALTH_BIND_ADDR" => "0.0.0.0:9897",
  "CHRONOS_METRICS_BIND_ADDR" => "0.0.0.0:9898",
  "CHRONOS_AUTO_FAILOVER_ENABLED" => "true"
}.each do |key, expected|
  fail!("ConfigMap chronos-config #{key} must be #{expected}") unless data[key] == expected
end
fail!("CHRONOS_ETCD_ENDPOINTS must use https") unless data.fetch("CHRONOS_ETCD_ENDPOINTS", "").start_with?("https://")

statefulset = named(docs, "StatefulSet", "chronos")
spec = statefulset.fetch("spec")
fail!("StatefulSet must use chronos-headless serviceName") unless spec["serviceName"] == "chronos-headless"
fail!("StatefulSet replicas must be at least 3") unless spec.fetch("replicas", 0).to_i >= 3
fail!("StatefulSet podManagementPolicy must be Parallel") unless spec["podManagementPolicy"] == "Parallel"

pod_spec = dig(statefulset, "spec", "template", "spec") || {}
pod_security = pod_spec.fetch("securityContext", {})
fail!("pod must run as non-root") unless pod_security["runAsNonRoot"] == true
fail!("pod seccompProfile must be RuntimeDefault") unless dig(pod_security, "seccompProfile", "type") == "RuntimeDefault"
fail!("pod must define pod anti-affinity") unless dig(pod_spec, "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution").is_a?(Array)
fail!("pod must define topology spread constraints") unless pod_spec["topologySpreadConstraints"].is_a?(Array)

init = Array(pod_spec["initContainers"]).find { |container| container["name"] == "install-tls" }
fail!("missing install-tls initContainer") unless init
fail!("install-tls image must be pinned by digest") unless init.fetch("image", "").include?("@sha256:")

container = Array(pod_spec["containers"]).find { |entry| entry["name"] == "chronos" }
fail!("missing chronos container") unless container
image = container.fetch("image", "")
fail!("chronos image must not use latest") if image.end_with?(":latest") || image == "latest"

ports = Array(container["ports"]).map { |port| [port["name"], port["containerPort"]] }.to_h
{"grpc" => 50051, "health" => 9897, "metrics" => 9898}.each do |name, port|
  fail!("container must expose #{name}:#{port}") unless ports[name] == port
end

%w[startupProbe readinessProbe livenessProbe].each do |probe_name|
  probe = container[probe_name]
  fail!("container must define #{probe_name}") unless probe
  fail!("#{probe_name} must target health port") unless dig(probe, "httpGet", "port") == "health"
end
fail!("readinessProbe must use /readyz") unless dig(container, "readinessProbe", "httpGet", "path") == "/readyz"
fail!("startupProbe must use /healthz") unless dig(container, "startupProbe", "httpGet", "path") == "/healthz"
fail!("livenessProbe must use /healthz") unless dig(container, "livenessProbe", "httpGet", "path") == "/healthz"

resources = container.fetch("resources", {})
fail!("container must define resource requests") unless resources["requests"].is_a?(Hash)
fail!("container must define resource limits") unless resources["limits"].is_a?(Hash)

security = container.fetch("securityContext", {})
fail!("container must disable privilege escalation") unless security["allowPrivilegeEscalation"] == false
fail!("container must use readOnlyRootFilesystem") unless security["readOnlyRootFilesystem"] == true
fail!("container must drop all capabilities") unless dig(security, "capabilities", "drop")&.include?("ALL")

%w[
  CHRONOS_WORKER_ID
  CHRONOS_INSTANCE_ID
  CHRONOS_ADVERTISE_ENDPOINT
  CHRONOS_GRPC_TLS_CERT_FILE
  CHRONOS_GRPC_TLS_KEY_FILE
  CHRONOS_GRPC_CLIENT_CA_FILE
  CHRONOS_METRICS_TLS_CERT_FILE
  CHRONOS_METRICS_TLS_KEY_FILE
  CHRONOS_METRICS_CLIENT_CA_FILE
  CHRONOS_ETCD_CA_FILE
  CHRONOS_ETCD_CERT_FILE
  CHRONOS_ETCD_KEY_FILE
  CHRONOS_GRPC_CONTROL_CERT_ALLOWLIST
  CHRONOS_GRPC_ROUTE_CERT_ALLOWLIST
  CHRONOS_GRPC_TIMESTAMP_CERT_ALLOWLIST
  CHRONOS_GRPC_STATUS_CERT_ALLOWLIST
].each do |name|
  fail!("container env missing #{name}") unless env_value(container, name)
end

mount_names = Array(container["volumeMounts"]).map { |mount| mount["name"] }
%w[tls-work tmp].each do |name|
  fail!("container must mount #{name}") unless mount_names.include?(name)
end

volume_names = Array(pod_spec["volumes"]).map { |volume| volume["name"] }
%w[tls-work grpc-tls-input metrics-tls-input etcd-tls-input tmp].each do |name|
  fail!("pod volumes missing #{name}") unless volume_names.include?(name)
end

pdb = named(docs, "PodDisruptionBudget", "chronos")
fail!("PDB minAvailable must be at least 2") unless dig(pdb, "spec", "minAvailable").to_i >= 2

hpa = named(docs, "HorizontalPodAutoscaler", "chronos")
fail!("HPA minReplicas must be at least 3") unless dig(hpa, "spec", "minReplicas").to_i >= 3
fail!("HPA maxReplicas must be above minReplicas") unless dig(hpa, "spec", "maxReplicas").to_i > dig(hpa, "spec", "minReplicas").to_i

monitor = named(docs, "ServiceMonitor", "chronos")
endpoint = Array(dig(monitor, "spec", "endpoints")).first || {}
fail!("ServiceMonitor must scrape metrics port") unless endpoint["port"] == "metrics"
fail!("ServiceMonitor must use https") unless endpoint["scheme"] == "https"
fail!("ServiceMonitor must define tlsConfig") unless endpoint["tlsConfig"].is_a?(Hash)

puts "#{path}: ok"
' "$manifest"
