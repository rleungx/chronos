use std::fmt;
use std::fs::File;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use thiserror::Error;

use crate::authz::validate_peer_cert_allowlist_entries;
use crate::{ResourceTier, MAX_GENERATORS};

pub const DEFAULT_WORKER_ID: &str = "default-worker";
pub const DEFAULT_ADVERTISE_ENDPOINT: &str = "default-endpoint:50051";
pub const DEFAULT_OWNERSHIP_PLAN_ID: &str = "default";
pub const DEFAULT_BIND_ADDR: &str = "[::1]:50051";
pub const DEFAULT_METRICS_BIND_ADDR: &str = "127.0.0.1:9898";
pub const DEFAULT_METADATA_KIND: &str = "memory";
pub const DEFAULT_MAX_TIMELINE_PROXY_LANES: usize = 4_096;
pub const DEFAULT_MAX_TIMELINE_RUNTIME_ENTRIES: usize = 4_096;
pub const DEFAULT_MAX_CONCURRENT_TIMELINE_LOADS: usize = 64;
pub const DEFAULT_MAX_BATCH_PER_REQUEST: u32 = 4_096;
// Production profile currently keeps the same capacity ceilings as the baseline defaults.
// Separate constants keep that policy explicit and allow tightening later without reshaping
// the profile application logic.
pub const PRODUCTION_MAX_BATCH_PER_REQUEST: u32 = 4_096;
pub const PRODUCTION_MAX_TIMELINE_PROXY_LANES: usize = 4_096;
pub const PRODUCTION_MAX_TIMELINE_RUNTIME_ENTRIES: usize = 4_096;
pub const DEFAULT_REQUEST_RECORD_PENDING_TIMEOUT_MS: u64 = 300_000;
pub const DEFAULT_REQUEST_RECORD_RETENTION_MS: u64 = 3_600_000;
pub const DEFAULT_REQUEST_RECORD_CLEANUP_INTERVAL_MS: u64 = 60_000;
pub const DEFAULT_REQUEST_RECORD_CLEANUP_BATCH_SIZE: usize = 512;
pub const DEFAULT_AUTO_FAILOVER_INTERVAL_MS: u64 = 1_000;
pub const DEFAULT_AUTO_FAILOVER_BATCH_SIZE: usize = 16;

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
    if normalized == "localhost" || normalized.ends_with(".localhost") {
        return true;
    }

    match normalized.parse::<IpAddr>() {
        Ok(ip) => ip.is_unspecified() || ip.is_loopback(),
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
    // A value of 0 means "inherit lease_ttl_ms" during service construction.
    pub generator_lease_ttl_ms: u64,
    pub generator_maintenance_interval_ms: u64,
    pub generator_ownership_modulo: u32,
    pub generator_ownership_remainder: u32,
    pub ownership_plan_id: String,
    pub worker_id: String,
    pub instance_id: String,
    pub advertise_endpoint: String,
    pub bind_addr: String,
    pub health_bind_addr: Option<String>,
    pub metrics_bind_addr: String,
    pub metadata_kind: String,
    pub etcd_endpoints: Vec<String>,
    pub production_profile: bool,
    pub security_mode: Option<TsoSecurityMode>,
    pub grpc_tls_cert_file: Option<String>,
    pub grpc_tls_key_file: Option<String>,
    pub grpc_client_ca_file: Option<String>,
    pub grpc_control_cert_allowlist: Vec<String>,
    pub grpc_route_cert_allowlist: Vec<String>,
    pub grpc_timestamp_cert_allowlist: Vec<String>,
    pub grpc_status_cert_allowlist: Vec<String>,
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
    pub recovery_catchup_budget_ms: u64,
    pub shared_jump_ahead_threshold_ms: u64,
    pub safety_gap_ms: u64,
    pub max_timeline_proxy_lanes: usize,
    pub max_timeline_runtime_entries: usize,
    pub max_concurrent_timeline_loads: usize,
    pub request_record_pending_timeout_ms: u64,
    pub request_record_retention_ms: u64,
    pub request_record_cleanup_interval_ms: u64,
    pub request_record_cleanup_batch_size: usize,
    pub auto_failover_enabled: bool,
    pub auto_failover_interval_ms: u64,
    pub auto_failover_batch_size: usize,
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
            ownership_plan_id: DEFAULT_OWNERSHIP_PLAN_ID.to_owned(),
            worker_id: DEFAULT_WORKER_ID.to_owned(),
            instance_id: String::new(),
            advertise_endpoint: DEFAULT_ADVERTISE_ENDPOINT.to_owned(),
            bind_addr: DEFAULT_BIND_ADDR.to_owned(),
            health_bind_addr: None,
            metrics_bind_addr: DEFAULT_METRICS_BIND_ADDR.to_owned(),
            metadata_kind: DEFAULT_METADATA_KIND.to_owned(),
            etcd_endpoints: Vec::new(),
            production_profile: false,
            security_mode: None,
            grpc_tls_cert_file: None,
            grpc_tls_key_file: None,
            grpc_client_ca_file: None,
            grpc_control_cert_allowlist: Vec::new(),
            grpc_route_cert_allowlist: Vec::new(),
            grpc_timestamp_cert_allowlist: Vec::new(),
            grpc_status_cert_allowlist: Vec::new(),
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
            recovery_catchup_budget_ms: 5_000,
            shared_jump_ahead_threshold_ms: 5_000,
            safety_gap_ms: 0,
            max_timeline_proxy_lanes: DEFAULT_MAX_TIMELINE_PROXY_LANES,
            max_timeline_runtime_entries: DEFAULT_MAX_TIMELINE_RUNTIME_ENTRIES,
            max_concurrent_timeline_loads: DEFAULT_MAX_CONCURRENT_TIMELINE_LOADS,
            request_record_pending_timeout_ms: DEFAULT_REQUEST_RECORD_PENDING_TIMEOUT_MS,
            request_record_retention_ms: DEFAULT_REQUEST_RECORD_RETENTION_MS,
            request_record_cleanup_interval_ms: DEFAULT_REQUEST_RECORD_CLEANUP_INTERVAL_MS,
            request_record_cleanup_batch_size: DEFAULT_REQUEST_RECORD_CLEANUP_BATCH_SIZE,
            auto_failover_enabled: false,
            auto_failover_interval_ms: DEFAULT_AUTO_FAILOVER_INTERVAL_MS,
            auto_failover_batch_size: DEFAULT_AUTO_FAILOVER_BATCH_SIZE,
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
        if !self.metadata_kind.trim().eq_ignore_ascii_case("etcd") {
            return Ok(());
        }
        self.validate_authoritative_metadata_runtime_contract_for_etcd()
    }

    pub fn validate_authoritative_metadata_runtime_contract_for_etcd(
        &self,
    ) -> Result<(), TsoConfigValidationError> {
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
        let local_authoritative_metadata_allowed =
            self.security_mode == Some(TsoSecurityMode::DevInsecure) && !self.production_profile;
        if !local_authoritative_metadata_allowed
            && advertise_endpoint_rejected_for_authoritative_metadata(&self.advertise_endpoint)
        {
            return Err(TsoConfigValidationError::Security(
                "CHRONOS_ADVERTISE_ENDPOINT must not use localhost, .localhost, a loopback IP, or a wildcard address when metadata=etcd"
                    .into(),
            ));
        }
        if self.safety_gap_ms == 0 {
            return Err(TsoConfigValidationError::Security(
                "CHRONOS_SAFETY_GAP_MS must be greater than 0 when metadata=etcd".into(),
            ));
        }
        if self.generator_ownership_modulo > 1
            && self
                .ownership_plan_id
                .trim()
                .eq_ignore_ascii_case(DEFAULT_OWNERSHIP_PLAN_ID)
        {
            return Err(TsoConfigValidationError::Security(
                "CHRONOS_OWNERSHIP_PLAN_ID must be explicitly set when metadata=etcd and generator ownership is partitioned"
                    .into(),
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
        self.validate_health_bind_addr()?;

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
        if self.ownership_plan_id.trim().is_empty() {
            return Err(TsoConfigValidationError::Security(
                "ownership_plan_id must not be blank".into(),
            ));
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
        validate_positive_u64_value(
            "CHRONOS_REQUEST_RECORD_PENDING_TIMEOUT_MS",
            self.request_record_pending_timeout_ms,
        )?;
        validate_positive_u64_value(
            "CHRONOS_REQUEST_RECORD_RETENTION_MS",
            self.request_record_retention_ms,
        )?;
        validate_positive_u64_value(
            "CHRONOS_REQUEST_RECORD_CLEANUP_INTERVAL_MS",
            self.request_record_cleanup_interval_ms,
        )?;
        validate_positive_usize_value(
            "CHRONOS_REQUEST_RECORD_CLEANUP_BATCH_SIZE",
            self.request_record_cleanup_batch_size,
        )?;
        validate_positive_u64_value(
            "CHRONOS_AUTO_FAILOVER_INTERVAL_MS",
            self.auto_failover_interval_ms,
        )?;
        validate_positive_usize_value(
            "CHRONOS_AUTO_FAILOVER_BATCH_SIZE",
            self.auto_failover_batch_size,
        )?;
        if self.request_record_retention_ms < self.request_record_pending_timeout_ms {
            return Err(TsoConfigValidationError::Security(
                "CHRONOS_REQUEST_RECORD_RETENTION_MS must be greater than or equal to CHRONOS_REQUEST_RECORD_PENDING_TIMEOUT_MS"
                    .into(),
            ));
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
        validate_readable_files(&[
            ("CHRONOS_GRPC_TLS_CERT_FILE", &self.grpc_tls_cert_file),
            ("CHRONOS_GRPC_TLS_KEY_FILE", &self.grpc_tls_key_file),
            ("CHRONOS_GRPC_CLIENT_CA_FILE", &self.grpc_client_ca_file),
            ("CHRONOS_METRICS_TLS_CERT_FILE", &self.metrics_tls_cert_file),
            ("CHRONOS_METRICS_TLS_KEY_FILE", &self.metrics_tls_key_file),
            (
                "CHRONOS_METRICS_CLIENT_CA_FILE",
                &self.metrics_client_ca_file,
            ),
            ("CHRONOS_ETCD_CA_FILE", &self.etcd_ca_file),
            ("CHRONOS_ETCD_CERT_FILE", &self.etcd_cert_file),
            ("CHRONOS_ETCD_KEY_FILE", &self.etcd_key_file),
        ])?;
        validate_private_key_files(&[
            ("CHRONOS_GRPC_TLS_KEY_FILE", &self.grpc_tls_key_file),
            ("CHRONOS_METRICS_TLS_KEY_FILE", &self.metrics_tls_key_file),
            ("CHRONOS_ETCD_KEY_FILE", &self.etcd_key_file),
        ])?;
        validate_positive_optional_u64(
            "CHRONOS_GRPC_REQUEST_TIMEOUT_MS",
            self.grpc_request_timeout_ms,
        )?;
        validate_sha256_fingerprint_allowlist(
            "CHRONOS_GRPC_CONTROL_CERT_ALLOWLIST",
            &self.grpc_control_cert_allowlist,
        )?;
        validate_sha256_fingerprint_allowlist(
            "CHRONOS_GRPC_ROUTE_CERT_ALLOWLIST",
            &self.grpc_route_cert_allowlist,
        )?;
        validate_sha256_fingerprint_allowlist(
            "CHRONOS_GRPC_TIMESTAMP_CERT_ALLOWLIST",
            &self.grpc_timestamp_cert_allowlist,
        )?;
        validate_sha256_fingerprint_allowlist(
            "CHRONOS_GRPC_STATUS_CERT_ALLOWLIST",
            &self.grpc_status_cert_allowlist,
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

        if self.grpc_tls_paths()?.is_some() {
            require_non_empty_allowlist(
                "CHRONOS_GRPC_CONTROL_CERT_ALLOWLIST",
                &self.grpc_control_cert_allowlist,
            )?;
            require_non_empty_allowlist(
                "CHRONOS_GRPC_ROUTE_CERT_ALLOWLIST",
                &self.grpc_route_cert_allowlist,
            )?;
            require_non_empty_allowlist(
                "CHRONOS_GRPC_TIMESTAMP_CERT_ALLOWLIST",
                &self.grpc_timestamp_cert_allowlist,
            )?;
            require_non_empty_allowlist(
                "CHRONOS_GRPC_STATUS_CERT_ALLOWLIST",
                &self.grpc_status_cert_allowlist,
            )?;
        }

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

    fn validate_health_bind_addr(&self) -> Result<(), TsoConfigValidationError> {
        let Some(health_bind_addr) = self.health_bind_addr.as_deref() else {
            return Ok(());
        };
        let health_addr = parse_socket_addr(health_bind_addr, "CHRONOS_HEALTH_BIND_ADDR")?;
        let grpc_addr = parse_socket_addr(&self.bind_addr, "CHRONOS_BIND_ADDR")?;
        let metrics_addr = parse_socket_addr(&self.metrics_bind_addr, "CHRONOS_METRICS_BIND_ADDR")?;
        if health_addr == grpc_addr {
            return Err(TsoConfigValidationError::Security(
                "CHRONOS_HEALTH_BIND_ADDR must not reuse CHRONOS_BIND_ADDR".into(),
            ));
        }
        if health_addr == metrics_addr {
            return Err(TsoConfigValidationError::Security(
                "CHRONOS_HEALTH_BIND_ADDR must not reuse CHRONOS_METRICS_BIND_ADDR".into(),
            ));
        }
        Ok(())
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
    let addr = parse_socket_addr(addr, field_name)?;
    Ok(addr.ip().is_loopback())
}

fn parse_socket_addr(addr: &str, field_name: &str) -> Result<SocketAddr, TsoConfigValidationError> {
    addr.trim().parse::<SocketAddr>().map_err(|error| {
        TsoConfigValidationError::Security(format!(
            "{field_name} must be a valid socket address: {error}"
        ))
    })
}

fn validate_sha256_fingerprint_allowlist(
    env_key: &str,
    allowlist: &[String],
) -> Result<(), TsoConfigValidationError> {
    validate_peer_cert_allowlist_entries(allowlist).map_err(|error| {
        TsoConfigValidationError::Security(format!(
            "{env_key} contains an invalid fingerprint: {error}"
        ))
    })
}

fn require_non_empty_allowlist(
    env_key: &str,
    allowlist: &[String],
) -> Result<(), TsoConfigValidationError> {
    if allowlist.is_empty() {
        return Err(TsoConfigValidationError::Security(format!(
            "{env_key} must contain at least one SHA-256 client certificate fingerprint when gRPC mTLS is enabled"
        )));
    }
    Ok(())
}

fn endpoint_host_is_local(endpoint: &str) -> Result<bool, TsoConfigValidationError> {
    let host = parse_advertise_endpoint_host(endpoint)?;
    let normalized = host
        .trim()
        .trim_matches(|ch| ch == '[' || ch == ']')
        .to_ascii_lowercase();
    if normalized == "localhost" || normalized.ends_with(".localhost") {
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

fn validate_readable_files(
    fields: &[(&str, &Option<String>)],
) -> Result<(), TsoConfigValidationError> {
    for (field_name, value) in fields {
        let Some(path) = value.as_deref() else {
            continue;
        };
        File::open(path).map_err(|error| {
            TsoConfigValidationError::Security(format!(
                "{field_name} could not read {path}: {error}"
            ))
        })?;
    }
    Ok(())
}

fn validate_private_key_files(
    fields: &[(&str, &Option<String>)],
) -> Result<(), TsoConfigValidationError> {
    for (field_name, value) in fields {
        let Some(path) = value.as_deref() else {
            continue;
        };
        let metadata = std::fs::symlink_metadata(path).map_err(|error| {
            TsoConfigValidationError::Security(format!(
                "{field_name} could not stat {path}: {error}"
            ))
        })?;
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            return Err(TsoConfigValidationError::Security(format!(
                "{field_name} must not be a symlink: {path}"
            )));
        }
        if !file_type.is_file() {
            return Err(TsoConfigValidationError::Security(format!(
                "{field_name} must reference a regular file: {path}"
            )));
        }
        #[cfg(unix)]
        {
            let mode = metadata.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                return Err(TsoConfigValidationError::Security(format!(
                    "{field_name} must not be group/other accessible: {path} has mode {mode:o}"
                )));
            }
        }
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

fn validate_positive_u64_value(
    field_name: &str,
    value: u64,
) -> Result<(), TsoConfigValidationError> {
    if value == 0 {
        return Err(TsoConfigValidationError::Security(format!(
            "{field_name} must be greater than 0"
        )));
    }
    Ok(())
}

fn validate_positive_usize_value(
    field_name: &str,
    value: usize,
) -> Result<(), TsoConfigValidationError> {
    if value == 0 {
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
    use std::path::PathBuf;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    fn unique_temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "chronos-{}-{}-{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

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
    fn validate_for_startup_rejects_invalid_health_bind_addr() {
        let mut config = valid_config();
        config.health_bind_addr = Some("not-a-socket".into());
        assert!(matches!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_HEALTH_BIND_ADDR")
        ));
    }

    #[test]
    fn validate_for_startup_rejects_health_bind_addr_port_conflict() {
        let mut config = valid_config();
        config.health_bind_addr = Some(config.metrics_bind_addr.clone());
        assert!(matches!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_HEALTH_BIND_ADDR")
        ));
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
    fn validate_for_startup_rejects_blank_ownership_plan_id() {
        let mut config = valid_config();
        config.ownership_plan_id = "  ".into();
        assert!(matches!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("ownership_plan_id")
        ));
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
    fn resolve_effective_security_mode_accepts_localhost_subdomain_for_local_only_topology() {
        let config = TsoConfig {
            bind_addr: "127.0.0.1:50051".into(),
            metrics_bind_addr: "127.0.0.1:9898".into(),
            advertise_endpoint: "chronos-soak.localhost:50051".into(),
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
    fn validate_for_startup_rejects_unreadable_grpc_tls_files() {
        let config = TsoConfig {
            bind_addr: "0.0.0.0:50052".into(),
            advertise_endpoint: "10.0.0.10:50052".into(),
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some("/definitely/missing/server.crt".into()),
            grpc_tls_key_file: Some("/definitely/missing/server.key".into()),
            grpc_client_ca_file: Some("/definitely/missing/ca.pem".into()),
            grpc_control_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into(),
            ],
            grpc_status_cert_allowlist: vec![crate::test_tls::placeholder_client_cert_fingerprint(
            )
            .into()],
            grpc_request_timeout_ms: Some(100),
            grpc_max_request_bytes: Some(1024),
            grpc_max_concurrent_requests: Some(16),
            ..TsoConfig::default()
        };
        assert!(matches!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_GRPC_TLS_CERT_FILE could not read")
        ));
    }

    #[test]
    fn validate_for_startup_rejects_unreadable_metrics_tls_files() {
        let config = TsoConfig {
            metrics_tls_cert_file: Some("/definitely/missing/metrics.crt".into()),
            metrics_tls_key_file: Some("/definitely/missing/metrics.key".into()),
            metrics_client_ca_file: Some("/definitely/missing/metrics-ca.pem".into()),
            ..valid_config()
        };
        assert!(matches!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_METRICS_TLS_CERT_FILE could not read")
        ));
    }

    #[test]
    fn validate_for_startup_rejects_unreadable_etcd_tls_files() {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            etcd_endpoints: vec!["https://10.0.0.20:2379".into()],
            advertise_endpoint: "10.0.0.10:50051".into(),
            security_mode: Some(TsoSecurityMode::Required),
            etcd_ca_file: Some("/definitely/missing/ca.pem".into()),
            etcd_cert_file: Some("/definitely/missing/client.pem".into()),
            etcd_key_file: Some("/definitely/missing/client-key.pem".into()),
            etcd_timeout_ms: Some(100),
            worker_id: "worker-a".into(),
            safety_gap_ms: 1,
            ..valid_config()
        };
        assert!(matches!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_ETCD_CA_FILE could not read")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn validate_for_startup_rejects_group_readable_private_key_files() {
        let dir = unique_temp_dir("config-key-perms-reject");
        let cert_path = dir.join("server.crt");
        let key_path = dir.join("server.key");
        let ca_path = dir.join("ca.pem");
        std::fs::write(&cert_path, b"cert").unwrap();
        std::fs::write(&key_path, b"key").unwrap();
        std::fs::write(&ca_path, b"ca").unwrap();
        std::fs::set_permissions(&cert_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o640)).unwrap();
        std::fs::set_permissions(&ca_path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let config = TsoConfig {
            bind_addr: "0.0.0.0:50052".into(),
            advertise_endpoint: "10.0.0.10:50052".into(),
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some(cert_path.to_string_lossy().into_owned()),
            grpc_tls_key_file: Some(key_path.to_string_lossy().into_owned()),
            grpc_client_ca_file: Some(ca_path.to_string_lossy().into_owned()),
            grpc_control_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into(),
            ],
            grpc_route_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into()
            ],
            grpc_timestamp_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into(),
            ],
            grpc_status_cert_allowlist: vec![crate::test_tls::placeholder_client_cert_fingerprint(
            )
            .into()],
            grpc_request_timeout_ms: Some(100),
            grpc_max_request_bytes: Some(1024),
            grpc_max_concurrent_requests: Some(16),
            ..TsoConfig::default()
        };
        assert!(matches!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_GRPC_TLS_KEY_FILE must not be group/other accessible")
        ));
    }

    #[test]
    fn validate_for_startup_rejects_symlink_private_key_files() {
        let dir = unique_temp_dir("config-key-symlink-reject");
        let cert_path = dir.join("server.crt");
        let key_target_path = dir.join("server.key.real");
        let key_link_path = dir.join("server.key");
        let ca_path = dir.join("ca.pem");
        std::fs::write(&cert_path, b"cert").unwrap();
        std::fs::write(&key_target_path, b"key").unwrap();
        std::fs::write(&ca_path, b"ca").unwrap();

        #[cfg(unix)]
        std::os::unix::fs::symlink(&key_target_path, &key_link_path).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_file(&key_target_path, &key_link_path).unwrap();

        let config = TsoConfig {
            bind_addr: "0.0.0.0:50052".into(),
            advertise_endpoint: "10.0.0.10:50052".into(),
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some(cert_path.to_string_lossy().into_owned()),
            grpc_tls_key_file: Some(key_link_path.to_string_lossy().into_owned()),
            grpc_client_ca_file: Some(ca_path.to_string_lossy().into_owned()),
            grpc_control_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into(),
            ],
            grpc_route_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into()
            ],
            grpc_timestamp_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into(),
            ],
            grpc_status_cert_allowlist: vec![crate::test_tls::placeholder_client_cert_fingerprint(
            )
            .into()],
            grpc_request_timeout_ms: Some(100),
            grpc_max_request_bytes: Some(1024),
            grpc_max_concurrent_requests: Some(16),
            ..TsoConfig::default()
        };
        assert!(matches!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_GRPC_TLS_KEY_FILE must not be a symlink")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn validate_for_startup_accepts_owner_only_private_key_files() {
        let dir = unique_temp_dir("config-key-owner-only-accept");
        let cert_path = dir.join("server.crt");
        let key_path = dir.join("server.key");
        let ca_path = dir.join("ca.pem");
        std::fs::write(&cert_path, b"cert").unwrap();
        std::fs::write(&key_path, b"key").unwrap();
        std::fs::write(&ca_path, b"ca").unwrap();
        std::fs::set_permissions(&cert_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(&ca_path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let config = TsoConfig {
            bind_addr: "0.0.0.0:50052".into(),
            advertise_endpoint: "10.0.0.10:50052".into(),
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some(cert_path.to_string_lossy().into_owned()),
            grpc_tls_key_file: Some(key_path.to_string_lossy().into_owned()),
            grpc_client_ca_file: Some(ca_path.to_string_lossy().into_owned()),
            grpc_control_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into(),
            ],
            grpc_route_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into()
            ],
            grpc_timestamp_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into(),
            ],
            grpc_status_cert_allowlist: vec![crate::test_tls::placeholder_client_cert_fingerprint(
            )
            .into()],
            grpc_request_timeout_ms: Some(100),
            grpc_max_request_bytes: Some(1024),
            grpc_max_concurrent_requests: Some(16),
            ..TsoConfig::default()
        };
        assert!(config.validate_for_startup().is_ok());
    }

    #[test]
    fn validate_for_startup_rejects_required_remote_exposed_grpc_with_zero_timeout() {
        let (cert_path, key_path, ca_path) = crate::test_tls::readable_test_tls_paths();
        let config = TsoConfig {
            bind_addr: "0.0.0.0:50052".into(),
            advertise_endpoint: "10.0.0.10:50052".into(),
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some(cert_path.into()),
            grpc_tls_key_file: Some(key_path.into()),
            grpc_client_ca_file: Some(ca_path.into()),
            grpc_control_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into(),
            ],
            grpc_status_cert_allowlist: vec![crate::test_tls::placeholder_client_cert_fingerprint(
            )
            .into()],
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
            security_mode: Some(TsoSecurityMode::Required),
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
    fn authoritative_metadata_runtime_contract_rejects_loopback_advertise_endpoint() {
        for advertise_endpoint in ["127.0.0.1:50051", "[::1]:50051"] {
            let config = TsoConfig {
                metadata_kind: "etcd".into(),
                worker_id: "worker-a".into(),
                advertise_endpoint: advertise_endpoint.into(),
                security_mode: Some(TsoSecurityMode::Required),
                safety_gap_ms: 1,
                ..valid_config()
            };
            assert!(matches!(
                config.validate_authoritative_metadata_runtime_contract(),
                Err(TsoConfigValidationError::Security(message))
                    if message.contains("loopback IP")
            ));
        }
    }

    #[test]
    fn authoritative_metadata_runtime_contract_rejects_localhost_subdomain() {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            advertise_endpoint: "chronos-a.localhost:50051".into(),
            security_mode: Some(TsoSecurityMode::Required),
            safety_gap_ms: 1,
            ..valid_config()
        };
        assert!(matches!(
            config.validate_authoritative_metadata_runtime_contract(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains(".localhost")
        ));
    }

    #[test]
    fn authoritative_metadata_runtime_contract_accepts_dev_insecure_loopback_for_local_tests() {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            advertise_endpoint: "127.0.0.1:50051".into(),
            safety_gap_ms: 1,
            ..valid_config()
        };
        assert!(config
            .validate_authoritative_metadata_runtime_contract()
            .is_ok());
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
    fn authoritative_metadata_runtime_contract_rejects_default_partitioned_ownership_plan() {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            advertise_endpoint: "10.0.0.10:50051".into(),
            safety_gap_ms: 1,
            generator_ownership_modulo: 2,
            generator_ownership_remainder: 0,
            ownership_plan_id: " Default ".into(),
            ..valid_config()
        };
        assert!(matches!(
            config.validate_authoritative_metadata_runtime_contract(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_OWNERSHIP_PLAN_ID")
        ));
    }

    #[test]
    fn authoritative_metadata_runtime_contract_accepts_explicit_partitioned_ownership_plan() {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            advertise_endpoint: "10.0.0.10:50051".into(),
            safety_gap_ms: 1,
            generator_ownership_modulo: 2,
            generator_ownership_remainder: 0,
            ownership_plan_id: "prod-2026-05-10".into(),
            ..valid_config()
        };
        assert!(config
            .validate_authoritative_metadata_runtime_contract()
            .is_ok());
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
    fn validate_for_startup_rejects_invalid_control_cert_allowlist_entry() {
        let (cert_path, key_path, ca_path) = crate::test_tls::readable_test_tls_paths();
        let config = TsoConfig {
            bind_addr: "0.0.0.0:50052".into(),
            advertise_endpoint: "10.0.0.10:50052".into(),
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some(cert_path.into()),
            grpc_tls_key_file: Some(key_path.into()),
            grpc_client_ca_file: Some(ca_path.into()),
            grpc_control_cert_allowlist: vec!["not-a-sha256".into()],
            grpc_status_cert_allowlist: vec![crate::test_tls::placeholder_client_cert_fingerprint(
            )
            .into()],
            grpc_request_timeout_ms: Some(100),
            grpc_max_request_bytes: Some(1024),
            grpc_max_concurrent_requests: Some(16),
            ..TsoConfig::default()
        };

        assert!(matches!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_GRPC_CONTROL_CERT_ALLOWLIST")
        ));
    }

    #[test]
    fn validate_for_startup_requires_status_cert_allowlist_when_grpc_mtls_is_enabled() {
        let (cert_path, key_path, ca_path) = crate::test_tls::readable_test_tls_paths();
        let config = TsoConfig {
            bind_addr: "0.0.0.0:50052".into(),
            advertise_endpoint: "10.0.0.10:50052".into(),
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some(cert_path.into()),
            grpc_tls_key_file: Some(key_path.into()),
            grpc_client_ca_file: Some(ca_path.into()),
            grpc_control_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into(),
            ],
            grpc_route_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into()
            ],
            grpc_timestamp_cert_allowlist: vec![
                crate::test_tls::placeholder_client_cert_fingerprint().into(),
            ],
            grpc_request_timeout_ms: Some(100),
            grpc_max_request_bytes: Some(1024),
            grpc_max_concurrent_requests: Some(16),
            ..TsoConfig::default()
        };

        assert!(matches!(
            config.validate_for_startup(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("CHRONOS_GRPC_STATUS_CERT_ALLOWLIST")
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
