use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use thiserror::Error;

use crate::{ResourceTier, MAX_GENERATORS};

pub const DEFAULT_WORKER_ID: &str = "default-worker";
pub const DEFAULT_ADVERTISE_ENDPOINT: &str = "default-endpoint:50051";
pub const DEFAULT_BIND_ADDR: &str = "[::1]:50051";
pub const DEFAULT_METRICS_BIND_ADDR: &str = "127.0.0.1:9898";
pub const DEFAULT_METADATA_KIND: &str = "memory";
pub const DEFAULT_MAX_TIMELINE_PROXY_LANES: usize = 4_096;
pub const DEFAULT_MAX_TIMELINE_RUNTIME_ENTRIES: usize = 4_096;
pub const DEFAULT_MAX_CONCURRENT_TIMELINE_LOADS: usize = 64;
pub const DEFAULT_MAX_BATCH_PER_REQUEST: u32 = 4_096;
pub const PRODUCTION_MAX_BATCH_PER_REQUEST: u32 = 4_096;
pub const PRODUCTION_MAX_TIMELINE_PROXY_LANES: usize = 4_096;
pub const PRODUCTION_MAX_TIMELINE_RUNTIME_ENTRIES: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsoSecurityMode {
    Required,
    DevInsecure,
}

impl fmt::Display for TsoSecurityMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Required => f.write_str("required"),
            Self::DevInsecure => f.write_str("dev-insecure"),
        }
    }
}

impl FromStr for TsoSecurityMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "required" => Ok(Self::Required),
            "dev-insecure" => Ok(Self::DevInsecure),
            other => Err(format!(
                "CHRONOS_SECURITY_MODE must be one of: required, dev-insecure (got {})",
                other
            )),
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TsoConfigValidationError {
    #[error("worker_id must not be empty")]
    EmptyWorkerId,
    #[error("advertise_endpoint must not be empty")]
    EmptyAdvertiseEndpoint,
    #[error("advertise_endpoint must use host:port format")]
    InvalidAdvertiseEndpointFormat,
    #[error("shared_generators + warm_generators exceeds max generator count {max_generators}")]
    TooManyTierGenerators { max_generators: u32 },
    #[error("generator ownership is misconfigured: modulo {modulo} remainder {remainder}")]
    GeneratorOwnershipMisconfigured { modulo: u32, remainder: u32 },
    #[error("max_batch_per_request must be greater than 0")]
    ZeroMaxBatchPerRequest,
    #[error("lease_ttl_ms must be greater than 0")]
    ZeroLeaseTtl,
    #[error("generator_maintenance_interval_ms must be greater than 0")]
    ZeroGeneratorMaintenanceInterval,
    #[error(
        "generator_maintenance_interval_ms {interval_ms} must be less than lease_ttl_ms {lease_ttl_ms}"
    )]
    GeneratorMaintenanceIntervalTooLarge { interval_ms: u64, lease_ttl_ms: u64 },
    #[error(
        "generator_lease_ttl_ms {lease_ttl_ms} must be greater than generator_maintenance_interval_ms {interval_ms}"
    )]
    GeneratorLeaseTtlTooSmall { interval_ms: u64, lease_ttl_ms: u64 },
    #[error("max_timeline_proxy_lanes must be greater than 0")]
    ZeroMaxTimelineProxyLanes,
    #[error("max_timeline_runtime_entries must be greater than 0")]
    ZeroMaxTimelineRuntimeEntries,
    #[error("max_concurrent_timeline_loads must be greater than 0")]
    ZeroMaxConcurrentTimelineLoads,
    #[error("default resource tier {resource_tier} has no configured generators")]
    MissingDefaultTierCapacity { resource_tier: ResourceTier },
    #[error("{0}")]
    Security(String),
}

pub(crate) fn advertise_endpoint_rejected_for_authoritative_metadata(endpoint: &str) -> bool {
    let Ok(host) = parse_advertise_endpoint_host(endpoint) else {
        return true;
    };
    let host = host.trim();
    if host.is_empty() {
        return true;
    }

    let normalized = host
        .trim_matches(|ch| ch == '[' || ch == ']')
        .to_ascii_lowercase();
    if normalized == "localhost" {
        return true;
    }

    match normalized.parse::<IpAddr>() {
        Ok(ip) => ip.is_unspecified(),
        Err(_) => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerTlsPaths<'a> {
    pub cert_file: &'a str,
    pub key_file: &'a str,
    pub client_ca_file: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientTlsPaths<'a> {
    pub ca_file: &'a str,
    pub cert_file: &'a str,
    pub key_file: &'a str,
}

#[derive(Debug, Clone)]
pub struct TsoConfig {
    pub shared_generators: u32,
    pub warm_generators: u32,
    pub max_batch_per_request: u32,
    pub max_future_borrow_ms: u64,
    pub default_resource_tier: ResourceTier,
    pub lease_ttl_ms: u64,
    pub generator_lease_ttl_ms: u64,
    pub generator_maintenance_interval_ms: u64,
    pub generator_ownership_modulo: u32,
    pub generator_ownership_remainder: u32,
    pub worker_id: String,
    pub instance_id: String,
    pub advertise_endpoint: String,
    pub bind_addr: String,
    pub metrics_bind_addr: String,
    pub metadata_kind: String,
    pub etcd_endpoints: Vec<String>,
    pub production_profile: bool,
    pub security_mode: Option<TsoSecurityMode>,
    pub grpc_tls_cert_file: Option<String>,
    pub grpc_tls_key_file: Option<String>,
    pub grpc_client_ca_file: Option<String>,
    pub grpc_request_timeout_ms: Option<u64>,
    pub grpc_max_request_bytes: Option<usize>,
    pub grpc_max_concurrent_requests: Option<usize>,
    pub metrics_tls_cert_file: Option<String>,
    pub metrics_tls_key_file: Option<String>,
    pub metrics_client_ca_file: Option<String>,
    pub etcd_ca_file: Option<String>,
    pub etcd_cert_file: Option<String>,
    pub etcd_key_file: Option<String>,
    pub etcd_timeout_ms: Option<u64>,
    pub pre_borrow_ms: u64,
    pub max_clock_rewind_ms: u64,
    pub shared_jump_ahead_threshold_ms: u64,
    pub safety_gap_ms: u64,
    pub max_timeline_proxy_lanes: usize,
    pub max_timeline_runtime_entries: usize,
    pub max_concurrent_timeline_loads: usize,
}

impl Default for TsoConfig {
    fn default() -> Self {
        Self {
            shared_generators: 128,
            warm_generators: 128,
            max_batch_per_request: DEFAULT_MAX_BATCH_PER_REQUEST,
            max_future_borrow_ms: 10,
            default_resource_tier: ResourceTier::Shared,
            lease_ttl_ms: 3000,
            generator_lease_ttl_ms: 0,
            generator_maintenance_interval_ms: 200,
            generator_ownership_modulo: 1,
            generator_ownership_remainder: 0,
            worker_id: DEFAULT_WORKER_ID.to_owned(),
            instance_id: String::new(),
            advertise_endpoint: DEFAULT_ADVERTISE_ENDPOINT.to_owned(),
            bind_addr: DEFAULT_BIND_ADDR.to_owned(),
            metrics_bind_addr: DEFAULT_METRICS_BIND_ADDR.to_owned(),
            metadata_kind: DEFAULT_METADATA_KIND.to_owned(),
            etcd_endpoints: Vec::new(),
            production_profile: false,
            security_mode: None,
            grpc_tls_cert_file: None,
            grpc_tls_key_file: None,
            grpc_client_ca_file: None,
            grpc_request_timeout_ms: None,
            grpc_max_request_bytes: None,
            grpc_max_concurrent_requests: None,
            metrics_tls_cert_file: None,
            metrics_tls_key_file: None,
            metrics_client_ca_file: None,
            etcd_ca_file: None,
            etcd_cert_file: None,
            etcd_key_file: None,
            etcd_timeout_ms: None,
            pre_borrow_ms: 1000,
            max_clock_rewind_ms: 30_000,
            shared_jump_ahead_threshold_ms: 5_000,
            safety_gap_ms: 0,
            max_timeline_proxy_lanes: DEFAULT_MAX_TIMELINE_PROXY_LANES,
            max_timeline_runtime_entries: DEFAULT_MAX_TIMELINE_RUNTIME_ENTRIES,
            max_concurrent_timeline_loads: DEFAULT_MAX_CONCURRENT_TIMELINE_LOADS,
        }
    }
}

impl TsoConfig {
    pub fn with_security_mode(mut self, mode: TsoSecurityMode) -> Self {
        self.security_mode = Some(mode);
        self
    }

    pub fn advertise_endpoint_host(&self) -> Result<&str, TsoConfigValidationError> {
        parse_advertise_endpoint_host(&self.advertise_endpoint)
    }

    pub fn apply_profile(&mut self, profile: &str) -> Result<(), String> {
        match profile.to_ascii_lowercase().as_str() {
            "prod" | "production" => {
                self.production_profile = true;
                self.max_batch_per_request = PRODUCTION_MAX_BATCH_PER_REQUEST;
                self.max_timeline_proxy_lanes = PRODUCTION_MAX_TIMELINE_PROXY_LANES;
                self.max_timeline_runtime_entries = PRODUCTION_MAX_TIMELINE_RUNTIME_ENTRIES;
                Ok(())
            }
            other => Err(format!("unknown CHRONOS_PROFILE: {}", other)),
        }
    }

    pub fn effective_instance_id(&self) -> &str {
        if self.instance_id.trim().is_empty() {
            self.advertise_endpoint.as_str()
        } else {
            self.instance_id.as_str()
        }
    }

    pub fn grpc_tls_paths(&self) -> Result<Option<ServerTlsPaths<'_>>, TsoConfigValidationError> {
        server_tls_paths_from_options(
            "gRPC TLS",
            [
                "CHRONOS_GRPC_TLS_CERT_FILE",
                "CHRONOS_GRPC_TLS_KEY_FILE",
                "CHRONOS_GRPC_CLIENT_CA_FILE",
            ],
            (
                self.grpc_tls_cert_file.as_deref(),
                self.grpc_tls_key_file.as_deref(),
                self.grpc_client_ca_file.as_deref(),
            ),
        )
    }

    pub fn metrics_tls_paths(
        &self,
    ) -> Result<Option<ServerTlsPaths<'_>>, TsoConfigValidationError> {
        server_tls_paths_from_options(
            "metrics TLS",
            [
                "CHRONOS_METRICS_TLS_CERT_FILE",
                "CHRONOS_METRICS_TLS_KEY_FILE",
                "CHRONOS_METRICS_CLIENT_CA_FILE",
            ],
            (
                self.metrics_tls_cert_file.as_deref(),
                self.metrics_tls_key_file.as_deref(),
                self.metrics_client_ca_file.as_deref(),
            ),
        )
    }

    pub fn etcd_tls_paths(&self) -> Result<Option<ClientTlsPaths<'_>>, TsoConfigValidationError> {
        client_tls_paths_from_options(
            "etcd TLS",
            [
                "CHRONOS_ETCD_CA_FILE",
                "CHRONOS_ETCD_CERT_FILE",
                "CHRONOS_ETCD_KEY_FILE",
            ],
            (
                self.etcd_ca_file.as_deref(),
                self.etcd_cert_file.as_deref(),
                self.etcd_key_file.as_deref(),
            ),
        )
    }

    pub fn etcd_tls_configured(&self) -> bool {
        self.etcd_ca_file.is_some() || self.etcd_cert_file.is_some() || self.etcd_key_file.is_some()
    }

    pub fn validate_etcd_endpoint_contract(&self) -> Result<(), TsoConfigValidationError> {
        let tls_enabled = self.etcd_tls_configured();
        for endpoint in &self.etcd_endpoints {
            validate_single_etcd_endpoint_contract(endpoint, tls_enabled)?;
        }
        Ok(())
    }

    pub fn validate_authoritative_metadata_runtime_contract(
        &self,
    ) -> Result<(), TsoConfigValidationError> {
        if self.metadata_kind != "etcd" {
            return Ok(());
        }
        if self.worker_id == DEFAULT_WORKER_ID {
            return Err(TsoConfigValidationError::Security(
                "CHRONOS_WORKER_ID must be explicitly set when metadata=etcd".into(),
            ));
        }
        if self.advertise_endpoint == DEFAULT_ADVERTISE_ENDPOINT {
            return Err(TsoConfigValidationError::Security(
                "CHRONOS_ADVERTISE_ENDPOINT must be explicitly set when metadata=etcd".into(),
            ));
        }
        if advertise_endpoint_rejected_for_authoritative_metadata(&self.advertise_endpoint) {
            return Err(TsoConfigValidationError::Security(
                "CHRONOS_ADVERTISE_ENDPOINT must not use localhost or a wildcard address when metadata=etcd"
                    .into(),
            ));
        }
        if self.safety_gap_ms == 0 {
            return Err(TsoConfigValidationError::Security(
                "CHRONOS_SAFETY_GAP_MS must be greater than 0 when metadata=etcd".into(),
            ));
        }
        Ok(())
    }

    pub fn validate_authoritative_metadata_store_contract(
        &self,
        endpoints: &[String],
        prefix: &str,
    ) -> Result<(), TsoConfigValidationError> {
        if endpoints.is_empty() {
            return Err(TsoConfigValidationError::Security(
                "CHRONOS_ETCD_ENDPOINTS must not be empty when metadata=etcd".into(),
            ));
        }
        let mut endpoint_contract = self.clone();
        endpoint_contract.etcd_endpoints = endpoints.to_vec();
        endpoint_contract.validate_etcd_endpoint_contract()?;

        let prefix = prefix.trim();
        if prefix.is_empty() || !prefix.starts_with('/') {
            return Err(TsoConfigValidationError::Security(
                "CHRONOS_ETCD_PREFIX must be a non-empty absolute key prefix when metadata=etcd"
                    .into(),
            ));
        }

        Ok(())
    }

    pub fn resolve_effective_security_mode(
        &self,
    ) -> Result<TsoSecurityMode, TsoConfigValidationError> {
        let has_nonlocal_topology = self.has_nonlocal_topology_signal()?;
        match self.security_mode {
            Some(TsoSecurityMode::DevInsecure) => {
                if self.production_profile {
                    return Err(TsoConfigValidationError::Security(
                        "CHRONOS_SECURITY_MODE=dev-insecure is not allowed with CHRONOS_PROFILE=production"
                            .into(),
                    ));
                }
                if has_nonlocal_topology {
                    return Err(TsoConfigValidationError::Security(
                        "CHRONOS_SECURITY_MODE=dev-insecure is only allowed for loopback/local-only topology"
                            .into(),
                    ));
                }
                Ok(TsoSecurityMode::DevInsecure)
            }
            Some(TsoSecurityMode::Required) => Ok(TsoSecurityMode::Required),
            None if self.production_profile || has_nonlocal_topology => Ok(TsoSecurityMode::Required),
            None => Err(TsoConfigValidationError::Security(
                "CHRONOS_SECURITY_MODE must be explicitly set to dev-insecure for local-only startup"
                    .into(),
            )),
        }
    }

    pub fn validate_for_startup(&self) -> Result<(), TsoConfigValidationError> {
        if self.worker_id.trim().is_empty() {
            return Err(TsoConfigValidationError::EmptyWorkerId);
        }
        if self.advertise_endpoint.trim().is_empty() {
            return Err(TsoConfigValidationError::EmptyAdvertiseEndpoint);
        }
        self.advertise_endpoint_host()?;

        let total_tier_generators = self
            .shared_generators
            .checked_add(self.warm_generators)
            .ok_or(TsoConfigValidationError::TooManyTierGenerators {
                max_generators: MAX_GENERATORS,
            })?;
        if total_tier_generators > MAX_GENERATORS {
            return Err(TsoConfigValidationError::TooManyTierGenerators {
                max_generators: MAX_GENERATORS,
            });
        }
        if self.generator_ownership_modulo == 0
            || self.generator_ownership_remainder >= self.generator_ownership_modulo
        {
            return Err(TsoConfigValidationError::GeneratorOwnershipMisconfigured {
                modulo: self.generator_ownership_modulo,
                remainder: self.generator_ownership_remainder,
            });
        }
        if self.max_batch_per_request == 0 {
            return Err(TsoConfigValidationError::ZeroMaxBatchPerRequest);
        }
        if self.lease_ttl_ms == 0 {
            return Err(TsoConfigValidationError::ZeroLeaseTtl);
        }
        if self.generator_maintenance_interval_ms == 0 {
            return Err(TsoConfigValidationError::ZeroGeneratorMaintenanceInterval);
        }
        if self.generator_maintenance_interval_ms >= self.lease_ttl_ms {
            return Err(
                TsoConfigValidationError::GeneratorMaintenanceIntervalTooLarge {
                    interval_ms: self.generator_maintenance_interval_ms,
                    lease_ttl_ms: self.lease_ttl_ms,
                },
            );
        }
        if self.generator_lease_ttl_ms != 0
            && self.generator_lease_ttl_ms <= self.generator_maintenance_interval_ms
        {
            return Err(TsoConfigValidationError::GeneratorLeaseTtlTooSmall {
                interval_ms: self.generator_maintenance_interval_ms,
                lease_ttl_ms: self.generator_lease_ttl_ms,
            });
        }
        if self.max_timeline_proxy_lanes == 0 {
            return Err(TsoConfigValidationError::ZeroMaxTimelineProxyLanes);
        }
        if self.max_timeline_runtime_entries == 0 {
            return Err(TsoConfigValidationError::ZeroMaxTimelineRuntimeEntries);
        }
        if self.max_concurrent_timeline_loads == 0 {
            return Err(TsoConfigValidationError::ZeroMaxConcurrentTimelineLoads);
        }

        match self.default_resource_tier {
            ResourceTier::Shared if self.shared_generators == 0 => {
                return Err(TsoConfigValidationError::MissingDefaultTierCapacity {
                    resource_tier: ResourceTier::Shared,
                });
            }
            ResourceTier::Warm if self.warm_generators == 0 => {
                return Err(TsoConfigValidationError::MissingDefaultTierCapacity {
                    resource_tier: ResourceTier::Warm,
                });
            }
            _ => {}
        }

        let effective_security_mode = self.resolve_effective_security_mode()?;
        self.validate_security_requirements(effective_security_mode)?;

        Ok(())
    }

    fn validate_security_requirements(
        &self,
        effective_security_mode: TsoSecurityMode,
    ) -> Result<(), TsoConfigValidationError> {
        validate_tls_bundle(
            "gRPC TLS",
            &[
                ("CHRONOS_GRPC_TLS_CERT_FILE", &self.grpc_tls_cert_file),
                ("CHRONOS_GRPC_TLS_KEY_FILE", &self.grpc_tls_key_file),
                ("CHRONOS_GRPC_CLIENT_CA_FILE", &self.grpc_client_ca_file),
            ],
            false,
        )?;
        validate_tls_bundle(
            "metrics TLS",
            &[
                ("CHRONOS_METRICS_TLS_CERT_FILE", &self.metrics_tls_cert_file),
                ("CHRONOS_METRICS_TLS_KEY_FILE", &self.metrics_tls_key_file),
                (
                    "CHRONOS_METRICS_CLIENT_CA_FILE",
                    &self.metrics_client_ca_file,
                ),
            ],
            false,
        )?;
        validate_tls_bundle(
            "etcd TLS",
            &[
                ("CHRONOS_ETCD_CA_FILE", &self.etcd_ca_file),
                ("CHRONOS_ETCD_CERT_FILE", &self.etcd_cert_file),
                ("CHRONOS_ETCD_KEY_FILE", &self.etcd_key_file),
            ],
            false,
        )?;
        validate_positive_optional_u64(
            "CHRONOS_GRPC_REQUEST_TIMEOUT_MS",
            self.grpc_request_timeout_ms,
        )?;
        validate_positive_optional_usize(
            "CHRONOS_GRPC_MAX_REQUEST_BYTES",
            self.grpc_max_request_bytes,
        )?;
        validate_positive_optional_usize(
            "CHRONOS_GRPC_MAX_CONCURRENT_REQUESTS",
            self.grpc_max_concurrent_requests,
        )?;
        validate_positive_optional_u64("CHRONOS_ETCD_TIMEOUT_MS", self.etcd_timeout_ms)?;

        if effective_security_mode != TsoSecurityMode::Required {
            return Ok(());
        }

        if self.grpc_remote_exposed()? {
            validate_tls_bundle(
                "gRPC TLS",
                &[
                    ("CHRONOS_GRPC_TLS_CERT_FILE", &self.grpc_tls_cert_file),
                    ("CHRONOS_GRPC_TLS_KEY_FILE", &self.grpc_tls_key_file),
                    ("CHRONOS_GRPC_CLIENT_CA_FILE", &self.grpc_client_ca_file),
                ],
                true,
            )?;
            require_positive_u64(
                "CHRONOS_GRPC_REQUEST_TIMEOUT_MS",
                self.grpc_request_timeout_ms,
            )?;
            require_positive_usize(
                "CHRONOS_GRPC_MAX_REQUEST_BYTES",
                self.grpc_max_request_bytes,
            )?;
            require_positive_usize(
                "CHRONOS_GRPC_MAX_CONCURRENT_REQUESTS",
                self.grpc_max_concurrent_requests,
            )?;
        }

        if self.metrics_remote_exposed()? {
            validate_tls_bundle(
                "metrics TLS",
                &[
                    ("CHRONOS_METRICS_TLS_CERT_FILE", &self.metrics_tls_cert_file),
                    ("CHRONOS_METRICS_TLS_KEY_FILE", &self.metrics_tls_key_file),
                    (
                        "CHRONOS_METRICS_CLIENT_CA_FILE",
                        &self.metrics_client_ca_file,
                    ),
                ],
                true,
            )?;
        }

        if self.etcd_remote_exposed() {
            validate_tls_bundle(
                "etcd TLS",
                &[
                    ("CHRONOS_ETCD_CA_FILE", &self.etcd_ca_file),
                    ("CHRONOS_ETCD_CERT_FILE", &self.etcd_cert_file),
                    ("CHRONOS_ETCD_KEY_FILE", &self.etcd_key_file),
                ],
                true,
            )?;
            require_positive_u64("CHRONOS_ETCD_TIMEOUT_MS", self.etcd_timeout_ms)?;
        }

        Ok(())
    }

    fn has_nonlocal_topology_signal(&self) -> Result<bool, TsoConfigValidationError> {
        if self.grpc_remote_exposed()? || self.metrics_remote_exposed()? {
            return Ok(true);
        }
        if self.etcd_remote_exposed() {
            return Ok(true);
        }
        Ok(false)
    }

    fn grpc_remote_exposed(&self) -> Result<bool, TsoConfigValidationError> {
        Ok(
            !socket_addr_is_loopback(&self.bind_addr, "CHRONOS_BIND_ADDR")?
                || !endpoint_host_is_local(&self.advertise_endpoint)?,
        )
    }

    fn metrics_remote_exposed(&self) -> Result<bool, TsoConfigValidationError> {
        Ok(!socket_addr_is_loopback(
            &self.metrics_bind_addr,
            "CHRONOS_METRICS_BIND_ADDR",
        )?)
    }

    fn etcd_remote_exposed(&self) -> bool {
        let metadata_kind = self.metadata_kind.trim().to_ascii_lowercase();
        metadata_kind == "etcd"
            && self
                .etcd_endpoints
                .iter()
                .map(|endpoint| endpoint.trim())
                .any(|endpoint| !endpoint.is_empty() && !endpoint_is_local(endpoint))
    }
}

pub fn parse_advertise_endpoint_host(endpoint: &str) -> Result<&str, TsoConfigValidationError> {
    let trimmed = endpoint.trim();
    if let Some(stripped) = trimmed.strip_prefix('[') {
        let Some((host, remainder)) = stripped.split_once(']') else {
            return Err(TsoConfigValidationError::InvalidAdvertiseEndpointFormat);
        };
        if host.is_empty() {
            return Err(TsoConfigValidationError::InvalidAdvertiseEndpointFormat);
        }
        let Some(port) = remainder.strip_prefix(':') else {
            return Err(TsoConfigValidationError::InvalidAdvertiseEndpointFormat);
        };
        if port.parse::<u16>().is_err() {
            return Err(TsoConfigValidationError::InvalidAdvertiseEndpointFormat);
        }
        return Ok(host);
    }

    let Some((host, port)) = trimmed.rsplit_once(':') else {
        return Err(TsoConfigValidationError::InvalidAdvertiseEndpointFormat);
    };
    if host.is_empty() || port.parse::<u16>().is_err() {
        return Err(TsoConfigValidationError::InvalidAdvertiseEndpointFormat);
    }

    Ok(host)
}

fn socket_addr_is_loopback(addr: &str, field_name: &str) -> Result<bool, TsoConfigValidationError> {
    let addr = addr.trim().parse::<SocketAddr>().map_err(|error| {
        TsoConfigValidationError::Security(format!(
            "{field_name} must be a valid socket address: {error}"
        ))
    })?;
    Ok(addr.ip().is_loopback())
}

fn endpoint_host_is_local(endpoint: &str) -> Result<bool, TsoConfigValidationError> {
    let host = parse_advertise_endpoint_host(endpoint)?;
    let normalized = host
        .trim()
        .trim_matches(|ch| ch == '[' || ch == ']')
        .to_ascii_lowercase();
    if normalized == "localhost" {
        return Ok(true);
    }
    match normalized.parse::<IpAddr>() {
        Ok(ip) => Ok(ip.is_loopback()),
        Err(_) => Ok(false),
    }
}

fn endpoint_is_local(endpoint: &str) -> bool {
    endpoint_host_is_local(endpoint).unwrap_or(false)
}

fn validate_single_etcd_endpoint_contract(
    endpoint: &str,
    tls_enabled: bool,
) -> Result<(), TsoConfigValidationError> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return Err(TsoConfigValidationError::Security(
            "CHRONOS_ETCD_ENDPOINTS must not contain empty endpoints".into(),
        ));
    }

    let (scheme, authority) = if let Some(rest) = trimmed.strip_prefix("http://") {
        ("http", rest)
    } else if let Some(rest) = trimmed.strip_prefix("https://") {
        ("https", rest)
    } else {
        ("", trimmed)
    };

    if authority.contains('/') || authority.contains('?') || authority.contains('#') {
        return Err(TsoConfigValidationError::Security(format!(
            "CHRONOS_ETCD_ENDPOINTS entries must not contain a path, query, or fragment: {}",
            trimmed
        )));
    }

    if parse_advertise_endpoint_host(authority).is_err() {
        return Err(TsoConfigValidationError::Security(format!(
            "CHRONOS_ETCD_ENDPOINTS entries must use host:port, http://host:port, or https://host:port: {}",
            trimmed
        )));
    }

    if tls_enabled && scheme == "http" {
        return Err(TsoConfigValidationError::Security(format!(
            "CHRONOS_ETCD_ENDPOINTS entries must use https:// or bare host:port when etcd TLS is configured: {}",
            trimmed
        )));
    }

    Ok(())
}

fn validate_tls_bundle(
    bundle_name: &str,
    fields: &[(&str, &Option<String>)],
    required: bool,
) -> Result<(), TsoConfigValidationError> {
    let present = fields.iter().filter(|(_, value)| value.is_some()).count();
    if present == 0 && !required {
        return Ok(());
    }
    if present != fields.len() {
        let field_list = fields
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(TsoConfigValidationError::Security(format!(
            "{bundle_name} requires a complete bundle: {field_list}"
        )));
    }
    Ok(())
}

fn incomplete_tls_bundle_error(bundle_name: &str, fields: [&str; 3]) -> TsoConfigValidationError {
    TsoConfigValidationError::Security(format!(
        "{bundle_name} requires a complete bundle: {}",
        fields.join(", ")
    ))
}

fn server_tls_paths_from_options<'a>(
    bundle_name: &str,
    fields: [&str; 3],
    values: (Option<&'a str>, Option<&'a str>, Option<&'a str>),
) -> Result<Option<ServerTlsPaths<'a>>, TsoConfigValidationError> {
    match values {
        (None, None, None) => Ok(None),
        (Some(cert_file), Some(key_file), Some(client_ca_file)) => Ok(Some(ServerTlsPaths {
            cert_file,
            key_file,
            client_ca_file,
        })),
        _ => Err(incomplete_tls_bundle_error(bundle_name, fields)),
    }
}

fn client_tls_paths_from_options<'a>(
    bundle_name: &str,
    fields: [&str; 3],
    values: (Option<&'a str>, Option<&'a str>, Option<&'a str>),
) -> Result<Option<ClientTlsPaths<'a>>, TsoConfigValidationError> {
    match values {
        (None, None, None) => Ok(None),
        (Some(ca_file), Some(cert_file), Some(key_file)) => Ok(Some(ClientTlsPaths {
            ca_file,
            cert_file,
            key_file,
        })),
        _ => Err(incomplete_tls_bundle_error(bundle_name, fields)),
    }
}

fn validate_positive_optional_u64(
    field_name: &str,
    value: Option<u64>,
) -> Result<(), TsoConfigValidationError> {
    if value == Some(0) {
        return Err(TsoConfigValidationError::Security(format!(
            "{field_name} must be greater than 0"
        )));
    }
    Ok(())
}

fn validate_positive_optional_usize(
    field_name: &str,
    value: Option<usize>,
) -> Result<(), TsoConfigValidationError> {
    if value == Some(0) {
        return Err(TsoConfigValidationError::Security(format!(
            "{field_name} must be greater than 0"
        )));
    }
    Ok(())
}

fn require_positive_u64(
    field_name: &str,
    value: Option<u64>,
) -> Result<(), TsoConfigValidationError> {
    match value {
        Some(value) if value > 0 => Ok(()),
        _ => Err(TsoConfigValidationError::Security(format!(
            "{field_name} must be explicitly set to a value greater than 0"
        ))),
    }
}

fn require_positive_usize(
    field_name: &str,
    value: Option<usize>,
) -> Result<(), TsoConfigValidationError> {
    match value {
        Some(value) if value > 0 => Ok(()),
        _ => Err(TsoConfigValidationError::Security(format!(
            "{field_name} must be explicitly set to a value greater than 0"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TsoConfig, TsoConfigValidationError, TsoSecurityMode, PRODUCTION_MAX_BATCH_PER_REQUEST,
        PRODUCTION_MAX_TIMELINE_PROXY_LANES, PRODUCTION_MAX_TIMELINE_RUNTIME_ENTRIES,
    };
    use crate::ResourceTier;

    fn valid_config() -> TsoConfig {
        TsoConfig {
            instance_id: "instance-a".into(),
            advertise_endpoint: "127.0.0.1:50051".into(),
            security_mode: Some(TsoSecurityMode::DevInsecure),
            ..TsoConfig::default()
        }
    }

    #[test]
    fn grpc_tls_paths_require_complete_bundle() {
        let config = TsoConfig {
            grpc_tls_cert_file: Some("server.crt".into()),
            ..valid_config()
        };

        assert!(matches!(
            config.grpc_tls_paths(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("gRPC TLS requires a complete bundle")
        ));
    }

    #[test]
    fn metrics_tls_paths_return_complete_bundle() {
        let config = TsoConfig {
            metrics_tls_cert_file: Some("metrics.crt".into()),
            metrics_tls_key_file: Some("metrics.key".into()),
            metrics_client_ca_file: Some("metrics-ca.pem".into()),
            ..valid_config()
        };

        let paths = config
            .metrics_tls_paths()
            .expect("metrics bundle should parse");
        assert_eq!(
            paths,
            Some(super::ServerTlsPaths {
                cert_file: "metrics.crt",
                key_file: "metrics.key",
                client_ca_file: "metrics-ca.pem",
            })
        );
    }

    #[test]
    fn etcd_tls_paths_require_complete_bundle() {
        let config = TsoConfig {
            etcd_ca_file: Some("ca.pem".into()),
            etcd_cert_file: Some("client.pem".into()),
            ..valid_config()
        };

        assert!(matches!(
            config.etcd_tls_paths(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("etcd TLS requires a complete bundle")
        ));
    }

    #[test]
    fn validate_for_startup_accepts_valid_config() {
        assert!(valid_config().validate_for_startup().is_ok());
    }

    #[test]
    fn validate_for_startup_rejects_blank_instance_id() {
        let mut config = valid_config();
        config.instance_id = "  ".into();
        assert!(config.validate_for_startup().is_ok());
        assert_eq!(config.effective_instance_id(), "127.0.0.1:50051");
    }

    #[test]
    fn validate_for_startup_rejects_invalid_advertise_endpoint_format() {
        let mut config = valid_config();
        config.advertise_endpoint = "endpoint-a".into();
        assert_eq!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::InvalidAdvertiseEndpointFormat)
        );
    }

    #[test]
    fn validate_for_startup_rejects_invalid_ownership_partition() {
        let mut config = valid_config();
        config.generator_ownership_modulo = 2;
        config.generator_ownership_remainder = 2;
        assert_eq!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::GeneratorOwnershipMisconfigured {
                modulo: 2,
                remainder: 2,
            })
        );
    }

    #[test]
    fn validate_for_startup_rejects_zero_runtime_capacity() {
        let mut config = valid_config();
        config.max_timeline_runtime_entries = 0;
        assert_eq!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::ZeroMaxTimelineRuntimeEntries)
        );
    }

    #[test]
    fn validate_for_startup_rejects_zero_concurrent_timeline_load_limit() {
        let mut config = valid_config();
        config.max_concurrent_timeline_loads = 0;
        assert_eq!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::ZeroMaxConcurrentTimelineLoads)
        );
    }

    #[test]
    fn validate_for_startup_rejects_maintenance_interval_not_below_lease_ttl() {
        let mut config = valid_config();
        config.lease_ttl_ms = 200;
        config.generator_maintenance_interval_ms = 200;
        assert_eq!(
            config.validate_for_startup(),
            Err(
                TsoConfigValidationError::GeneratorMaintenanceIntervalTooLarge {
                    interval_ms: 200,
                    lease_ttl_ms: 200,
                }
            )
        );
    }

    #[test]
    fn validate_for_startup_rejects_generator_lease_ttl_not_above_maintenance_interval() {
        let mut config = valid_config();
        config.generator_lease_ttl_ms = 200;
        config.generator_maintenance_interval_ms = 200;
        assert_eq!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::GeneratorLeaseTtlTooSmall {
                interval_ms: 200,
                lease_ttl_ms: 200,
            })
        );
    }

    #[test]
    fn validate_for_startup_rejects_missing_default_tier_capacity() {
        let mut config = valid_config();
        config.default_resource_tier = ResourceTier::Warm;
        config.warm_generators = 0;
        assert_eq!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::MissingDefaultTierCapacity {
                resource_tier: ResourceTier::Warm,
            })
        );
    }

    #[test]
    fn effective_instance_id_prefers_explicit_value() {
        let config = valid_config();
        assert_eq!(config.effective_instance_id(), "instance-a");
    }

    #[test]
    fn resolve_effective_security_mode_requires_explicit_dev_insecure_for_local_only_topology() {
        let config = TsoConfig {
            bind_addr: "127.0.0.1:50051".into(),
            metrics_bind_addr: "127.0.0.1:9898".into(),
            advertise_endpoint: "127.0.0.1:50051".into(),
            ..TsoConfig::default()
        };
        assert!(matches!(
            config.resolve_effective_security_mode(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_SECURITY_MODE")
        ));
    }

    #[test]
    fn resolve_effective_security_mode_accepts_explicit_dev_insecure_for_local_only_topology() {
        let config = TsoConfig {
            bind_addr: "127.0.0.1:50051".into(),
            metrics_bind_addr: "127.0.0.1:9898".into(),
            advertise_endpoint: "127.0.0.1:50051".into(),
            ..TsoConfig::default().with_security_mode(TsoSecurityMode::DevInsecure)
        };
        assert_eq!(
            config.resolve_effective_security_mode(),
            Ok(TsoSecurityMode::DevInsecure)
        );
    }

    #[test]
    fn resolve_effective_security_mode_infers_required_for_nonlocal_topology() {
        let config = TsoConfig {
            advertise_endpoint: "10.0.0.10:50051".into(),
            ..TsoConfig::default()
        };
        assert_eq!(
            config.resolve_effective_security_mode(),
            Ok(TsoSecurityMode::Required)
        );
    }

    #[test]
    fn resolve_effective_security_mode_rejects_dev_insecure_with_production_profile() {
        let config = TsoConfig {
            bind_addr: "127.0.0.1:50051".into(),
            metrics_bind_addr: "127.0.0.1:9898".into(),
            advertise_endpoint: "127.0.0.1:50051".into(),
            production_profile: true,
            ..TsoConfig::default().with_security_mode(TsoSecurityMode::DevInsecure)
        };
        assert!(matches!(
            config.resolve_effective_security_mode(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_PROFILE=production")
        ));
    }

    #[test]
    fn validate_for_startup_rejects_required_remote_exposed_grpc_without_tls_bundle() {
        let config = TsoConfig {
            bind_addr: "0.0.0.0:50052".into(),
            advertise_endpoint: "10.0.0.10:50052".into(),
            security_mode: Some(TsoSecurityMode::Required),
            grpc_request_timeout_ms: Some(100),
            grpc_max_request_bytes: Some(1024),
            grpc_max_concurrent_requests: Some(16),
            ..TsoConfig::default()
        };
        assert!(matches!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("gRPC TLS requires a complete bundle")
        ));
    }

    #[test]
    fn validate_for_startup_rejects_required_remote_exposed_grpc_with_zero_timeout() {
        let config = TsoConfig {
            bind_addr: "0.0.0.0:50052".into(),
            advertise_endpoint: "10.0.0.10:50052".into(),
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some("server.crt".into()),
            grpc_tls_key_file: Some("server.key".into()),
            grpc_client_ca_file: Some("ca.pem".into()),
            grpc_request_timeout_ms: Some(0),
            grpc_max_request_bytes: Some(1024),
            grpc_max_concurrent_requests: Some(16),
            ..TsoConfig::default()
        };
        assert!(matches!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_GRPC_REQUEST_TIMEOUT_MS")
        ));
    }

    #[test]
    fn authoritative_metadata_runtime_contract_rejects_default_worker_id() {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            etcd_endpoints: vec!["127.0.0.1:2379".into()],
            advertise_endpoint: "10.0.0.10:50051".into(),
            safety_gap_ms: 1,
            ..valid_config()
        };
        assert!(matches!(
            config.validate_authoritative_metadata_runtime_contract(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_WORKER_ID")
        ));
    }

    #[test]
    fn authoritative_metadata_runtime_contract_rejects_unroutable_advertise_endpoint() {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            advertise_endpoint: "localhost:50051".into(),
            safety_gap_ms: 1,
            ..valid_config()
        };
        assert!(matches!(
            config.validate_authoritative_metadata_runtime_contract(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("localhost")
        ));
    }

    #[test]
    fn authoritative_metadata_runtime_contract_rejects_zero_safety_gap() {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            advertise_endpoint: "10.0.0.10:50051".into(),
            safety_gap_ms: 0,
            ..valid_config()
        };
        assert!(matches!(
            config.validate_authoritative_metadata_runtime_contract(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_SAFETY_GAP_MS")
        ));
    }

    #[test]
    fn authoritative_metadata_store_contract_rejects_invalid_prefix() {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            ..valid_config()
        };
        assert!(matches!(
            config.validate_authoritative_metadata_store_contract(
                &["127.0.0.1:2379".into()],
                "relative-prefix"
            ),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_ETCD_PREFIX")
        ));
    }

    #[test]
    fn default_config_uses_production_safe_capacity_defaults() {
        let config = TsoConfig::default();
        assert_eq!(
            config.max_batch_per_request,
            PRODUCTION_MAX_BATCH_PER_REQUEST
        );
        assert_eq!(
            config.max_timeline_proxy_lanes,
            PRODUCTION_MAX_TIMELINE_PROXY_LANES
        );
        assert_eq!(
            config.max_timeline_runtime_entries,
            PRODUCTION_MAX_TIMELINE_RUNTIME_ENTRIES
        );
    }

    #[test]
    fn production_profile_applies_safer_capacity_defaults() {
        let mut config = TsoConfig::default();
        config.apply_profile("production").unwrap();
        assert_eq!(
            config.max_batch_per_request,
            PRODUCTION_MAX_BATCH_PER_REQUEST
        );
        assert_eq!(
            config.max_timeline_proxy_lanes,
            PRODUCTION_MAX_TIMELINE_PROXY_LANES
        );
        assert_eq!(
            config.max_timeline_runtime_entries,
            PRODUCTION_MAX_TIMELINE_RUNTIME_ENTRIES
        );
    }
}
