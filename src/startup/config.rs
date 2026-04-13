use chronos::{ResourceTier, TsoConfig, TsoSecurityMode, DEFAULT_METADATA_KIND};
use std::env;
use std::error::Error;

use crate::AppResult;

pub(crate) const DEFAULT_ETCD_PREFIX: &str = "/chronos";

#[derive(Debug, Clone)]
pub(crate) struct LoadedStartupConfig {
    pub(crate) config: TsoConfig,
    pub(crate) metadata: StartupMetadata,
}

#[derive(Debug, Clone)]
pub(crate) enum StartupMetadata {
    Memory,
    Etcd(EtcdStartupConfig),
}

#[derive(Debug, Clone)]
pub(crate) struct EtcdStartupConfig {
    pub(crate) endpoints: Vec<String>,
    pub(crate) prefix: String,
}

#[derive(Debug, Clone)]
struct MetadataEnvConfig {
    kind: String,
    etcd_endpoints: Vec<String>,
    etcd_prefix: String,
}

impl EtcdStartupConfig {
    pub(crate) fn new(endpoints: Vec<String>, prefix: impl Into<String>) -> Self {
        Self {
            endpoints,
            prefix: prefix.into(),
        }
    }
}

impl StartupMetadata {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Etcd(_) => "etcd",
        }
    }
}

impl LoadedStartupConfig {
    pub(crate) fn new(config: TsoConfig, metadata: StartupMetadata) -> Self {
        Self { config, metadata }
    }

    #[cfg(test)]
    pub(crate) fn memory(config: TsoConfig) -> Self {
        Self::new(config, StartupMetadata::Memory)
    }

    #[cfg(test)]
    pub(crate) fn etcd(config: TsoConfig, prefix: impl Into<String>) -> Self {
        let endpoints = config.etcd_endpoints.clone();
        Self::new(
            config,
            StartupMetadata::Etcd(EtcdStartupConfig::new(endpoints, prefix)),
        )
    }

    pub(crate) fn metadata_kind(&self) -> &'static str {
        self.metadata.kind()
    }
}

fn read_env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn read_csv_env(key: &str) -> Vec<String> {
    env::var(key)
        .unwrap_or_default()
        .split(',')
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .collect()
}

fn parse_generator_ownership(value: &str) -> AppResult<(u32, u32)> {
    let (remainder, modulo) = value.split_once('/').ok_or_else(|| {
        "CHRONOS_GENERATOR_OWNERSHIP must use remainder/modulo format".to_string()
    })?;
    Ok((remainder.trim().parse()?, modulo.trim().parse()?))
}

fn apply_generator_ownership_env(config: &mut TsoConfig) -> AppResult<()> {
    if let Ok(combined_value) = env::var("CHRONOS_GENERATOR_OWNERSHIP") {
        let (combined_remainder, combined_modulo) = parse_generator_ownership(&combined_value)?;
        config.generator_ownership_modulo = combined_modulo;
        config.generator_ownership_remainder = combined_remainder;
    }

    Ok(())
}

fn apply_string_env(key: &str, target: &mut String) {
    if let Ok(value) = env::var(key) {
        *target = value;
    }
}

fn apply_optional_string_env(key: &str, target: &mut Option<String>) {
    if let Ok(value) = env::var(key) {
        *target = Some(value);
    }
}

fn apply_parsed_env<T>(key: &str, target: &mut T) -> AppResult<()>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    if let Ok(value) = env::var(key) {
        *target = value.parse()?;
    }
    Ok(())
}

fn apply_optional_parsed_env<T>(key: &str, target: &mut Option<T>) -> AppResult<()>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    if let Ok(value) = env::var(key) {
        *target = Some(value.parse()?);
    }
    Ok(())
}

fn parse_resource_tier(value: &str) -> Result<ResourceTier, Box<dyn Error>> {
    match value.to_ascii_lowercase().as_str() {
        "shared" => Ok(ResourceTier::Shared),
        "warm" => Ok(ResourceTier::Warm),
        "dedicated" => Ok(ResourceTier::Dedicated),
        _ => Err(format!("invalid resource tier: {}", value).into()),
    }
}

fn apply_profile_env(config: &mut TsoConfig) -> AppResult<()> {
    if let Ok(profile) = env::var("CHRONOS_PROFILE") {
        config
            .apply_profile(&profile)
            .map_err(|error| -> Box<dyn Error> { error.into() })?;
    }
    Ok(())
}

fn apply_identity_env(config: &mut TsoConfig) {
    apply_string_env("CHRONOS_WORKER_ID", &mut config.worker_id);
    apply_string_env("CHRONOS_INSTANCE_ID", &mut config.instance_id);
    apply_string_env("CHRONOS_ADVERTISE_ENDPOINT", &mut config.advertise_endpoint);
    apply_string_env("CHRONOS_BIND_ADDR", &mut config.bind_addr);
    apply_string_env("CHRONOS_METRICS_BIND_ADDR", &mut config.metrics_bind_addr);
}

fn load_metadata_env() -> MetadataEnvConfig {
    MetadataEnvConfig {
        kind: effective_metadata_kind(),
        etcd_endpoints: read_csv_env("CHRONOS_ETCD_ENDPOINTS"),
        etcd_prefix: read_env_or_default("CHRONOS_ETCD_PREFIX", DEFAULT_ETCD_PREFIX),
    }
}

fn apply_metadata_env(config: &mut TsoConfig, metadata: &MetadataEnvConfig) {
    config.metadata_kind = metadata.kind.clone();
    config.etcd_endpoints = metadata.etcd_endpoints.clone();
}

fn apply_security_surface_env(config: &mut TsoConfig) -> AppResult<()> {
    if let Ok(value) = env::var("CHRONOS_SECURITY_MODE") {
        config.security_mode = Some(value.parse::<TsoSecurityMode>()?);
    }
    apply_optional_string_env("CHRONOS_GRPC_TLS_CERT_FILE", &mut config.grpc_tls_cert_file);
    apply_optional_string_env("CHRONOS_GRPC_TLS_KEY_FILE", &mut config.grpc_tls_key_file);
    apply_optional_string_env(
        "CHRONOS_GRPC_CLIENT_CA_FILE",
        &mut config.grpc_client_ca_file,
    );
    apply_optional_string_env(
        "CHRONOS_METRICS_TLS_CERT_FILE",
        &mut config.metrics_tls_cert_file,
    );
    apply_optional_string_env(
        "CHRONOS_METRICS_TLS_KEY_FILE",
        &mut config.metrics_tls_key_file,
    );
    apply_optional_string_env(
        "CHRONOS_METRICS_CLIENT_CA_FILE",
        &mut config.metrics_client_ca_file,
    );
    apply_optional_string_env("CHRONOS_ETCD_CA_FILE", &mut config.etcd_ca_file);
    apply_optional_string_env("CHRONOS_ETCD_CERT_FILE", &mut config.etcd_cert_file);
    apply_optional_string_env("CHRONOS_ETCD_KEY_FILE", &mut config.etcd_key_file);
    Ok(())
}

fn apply_transport_limit_env(config: &mut TsoConfig) -> AppResult<()> {
    apply_optional_parsed_env(
        "CHRONOS_GRPC_REQUEST_TIMEOUT_MS",
        &mut config.grpc_request_timeout_ms,
    )?;
    apply_optional_parsed_env(
        "CHRONOS_GRPC_MAX_REQUEST_BYTES",
        &mut config.grpc_max_request_bytes,
    )?;
    apply_optional_parsed_env(
        "CHRONOS_GRPC_MAX_CONCURRENT_REQUESTS",
        &mut config.grpc_max_concurrent_requests,
    )?;
    apply_optional_parsed_env("CHRONOS_ETCD_TIMEOUT_MS", &mut config.etcd_timeout_ms)?;
    Ok(())
}

fn apply_capacity_env(config: &mut TsoConfig) -> AppResult<()> {
    apply_parsed_env("CHRONOS_SHARED_GENERATORS", &mut config.shared_generators)?;
    apply_parsed_env("CHRONOS_WARM_GENERATORS", &mut config.warm_generators)?;
    apply_parsed_env(
        "CHRONOS_MAX_BATCH_PER_REQUEST",
        &mut config.max_batch_per_request,
    )?;
    apply_parsed_env(
        "CHRONOS_MAX_TIMELINE_PROXY_LANES",
        &mut config.max_timeline_proxy_lanes,
    )?;
    apply_parsed_env(
        "CHRONOS_MAX_TIMELINE_RUNTIME_ENTRIES",
        &mut config.max_timeline_runtime_entries,
    )?;
    if let Ok(value) = env::var("CHRONOS_DEFAULT_RESOURCE_TIER") {
        config.default_resource_tier = parse_resource_tier(&value)?;
    }
    Ok(())
}

fn apply_timing_env(config: &mut TsoConfig) -> AppResult<()> {
    apply_parsed_env(
        "CHRONOS_MAX_FUTURE_BORROW_MS",
        &mut config.max_future_borrow_ms,
    )?;
    apply_parsed_env(
        "CHRONOS_MAX_CLOCK_REWIND_MS",
        &mut config.max_clock_rewind_ms,
    )?;
    apply_parsed_env("CHRONOS_LEASE_TTL_MS", &mut config.lease_ttl_ms)?;
    apply_parsed_env(
        "CHRONOS_GENERATOR_LEASE_TTL_MS",
        &mut config.generator_lease_ttl_ms,
    )?;
    apply_parsed_env(
        "CHRONOS_GENERATOR_MAINTENANCE_INTERVAL_MS",
        &mut config.generator_maintenance_interval_ms,
    )?;
    apply_generator_ownership_env(config)?;
    apply_parsed_env("CHRONOS_PRE_BORROW_MS", &mut config.pre_borrow_ms)?;
    apply_parsed_env(
        "CHRONOS_SHARED_JUMP_AHEAD_THRESHOLD_MS",
        &mut config.shared_jump_ahead_threshold_ms,
    )?;
    apply_parsed_env("CHRONOS_SAFETY_GAP_MS", &mut config.safety_gap_ms)?;
    Ok(())
}

fn build_config_and_metadata_env() -> AppResult<(TsoConfig, MetadataEnvConfig)> {
    let mut config = TsoConfig::default();
    let metadata = load_metadata_env();

    apply_profile_env(&mut config)?;
    apply_identity_env(&mut config);
    apply_metadata_env(&mut config, &metadata);
    apply_security_surface_env(&mut config)?;
    apply_transport_limit_env(&mut config)?;
    apply_capacity_env(&mut config)?;
    apply_timing_env(&mut config)?;

    Ok((config, metadata))
}

fn startup_metadata_from_env(
    metadata_kind: &str,
    metadata_env: MetadataEnvConfig,
) -> AppResult<StartupMetadata> {
    match metadata_kind {
        "memory" => Ok(StartupMetadata::Memory),
        "etcd" => Ok(StartupMetadata::Etcd(EtcdStartupConfig::new(
            metadata_env.etcd_endpoints,
            metadata_env.etcd_prefix,
        ))),
        other => Err(format!("unknown metadata store: {}", other).into()),
    }
}

#[cfg(test)]
pub(crate) fn load_tso_config() -> AppResult<TsoConfig> {
    let (config, _) = build_config_and_metadata_env()?;
    Ok(config)
}

pub(crate) fn load_startup_config() -> AppResult<LoadedStartupConfig> {
    let (config, metadata_env) = build_config_and_metadata_env()?;
    let metadata = startup_metadata_from_env(config.metadata_kind.as_str(), metadata_env)?;
    Ok(LoadedStartupConfig::new(config, metadata))
}

fn effective_metadata_kind() -> String {
    read_env_or_default("CHRONOS_METADATA", DEFAULT_METADATA_KIND).to_ascii_lowercase()
}
