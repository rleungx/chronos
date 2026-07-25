use chronos::{TsoConfig, TsoSecurityMode, DEFAULT_METADATA_KIND};
use std::env;
use std::error::Error;

use crate::AppResult;

pub(crate) const DEFAULT_ETCD_PREFIX: &str = "/chronos";
const DEFAULT_LOG_FILTER: &str = "info";

const REMOVED_STARTUP_TUNING_ENV_VARS: &[&str] = &[
    "CHRONOS_GENERATOR_OWNERSHIP",
    "CHRONOS_REQUEST_RECORD_CLEANUP_BATCH_SIZE",
    "CHRONOS_REQUEST_RECORD_CLEANUP_INTERVAL_MS",
    "CHRONOS_ROUTE_CACHE_TTL_MS",
];

#[derive(Debug, Clone)]
pub(crate) struct LoadedStartupConfig {
    pub(crate) config: TsoConfig,
    pub(crate) metadata: StartupMetadata,
    pub(crate) logging: StartupLoggingConfig,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StartupLogFormat {
    Json,
    Text,
}

impl std::fmt::Display for StartupLogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json => write!(f, "json"),
            Self::Text => write!(f, "text"),
        }
    }
}

impl std::str::FromStr for StartupLogFormat {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "text" => Ok(Self::Text),
            other => Err(format!(
                "unsupported CHRONOS_LOG_FORMAT value: {} (expected json or text)",
                other
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StartupLoggingConfig {
    pub(crate) format: StartupLogFormat,
    pub(crate) filter: String,
}

impl Default for StartupLoggingConfig {
    fn default() -> Self {
        Self {
            format: StartupLogFormat::Json,
            filter: DEFAULT_LOG_FILTER.into(),
        }
    }
}

#[derive(Debug, Clone)]
struct MetadataEnvConfig {
    metadata_kind: String,
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
    pub(crate) fn new(
        config: TsoConfig,
        metadata: StartupMetadata,
        logging: StartupLoggingConfig,
    ) -> Self {
        Self {
            config,
            metadata,
            logging,
        }
    }

    #[cfg(test)]
    pub(crate) fn memory(config: TsoConfig) -> Self {
        Self::new(
            config,
            StartupMetadata::Memory,
            StartupLoggingConfig::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn etcd(config: TsoConfig, prefix: impl Into<String>) -> Self {
        let endpoints = config.etcd_endpoints.clone();
        Self::new(
            config,
            StartupMetadata::Etcd(EtcdStartupConfig::new(endpoints, prefix)),
            StartupLoggingConfig::default(),
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

fn parse_resource_tier_env(value: &str) -> AppResult<chronos::ResourceTier> {
    match value.trim().to_ascii_lowercase().as_str() {
        "shared" => Ok(chronos::ResourceTier::Shared),
        "warm" => Ok(chronos::ResourceTier::Warm),
        "dedicated" => Ok(chronos::ResourceTier::Dedicated),
        other => Err(format!(
            "CHRONOS_DEFAULT_RESOURCE_TIER must be one of shared,warm,dedicated (got {other})"
        )
        .into()),
    }
}

fn read_u32_csv_env(key: &str) -> AppResult<Vec<u32>> {
    let mut values = Vec::new();
    if env::var_os(key).is_none() {
        return Ok(values);
    }
    for value in read_csv_env(key) {
        values.push(value.parse()?);
    }
    Ok(values)
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

fn apply_csv_env(key: &str, target: &mut Vec<String>) {
    if env::var_os(key).is_some() {
        *target = read_csv_env(key);
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
    apply_optional_string_env("CHRONOS_HEALTH_BIND_ADDR", &mut config.health_bind_addr);
    apply_string_env("CHRONOS_METRICS_BIND_ADDR", &mut config.metrics_bind_addr);
}

fn load_logging_env() -> AppResult<StartupLoggingConfig> {
    let mut logging = StartupLoggingConfig::default();
    if let Ok(value) = env::var("CHRONOS_LOG_FORMAT") {
        logging.format = value
            .parse()
            .map_err(|error: String| -> Box<dyn Error> { error.into() })?;
    }
    if let Ok(value) = env::var("CHRONOS_LOG_FILTER") {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err("CHRONOS_LOG_FILTER must not be blank".into());
        }
        tracing_subscriber::EnvFilter::try_new(trimmed)
            .map_err(|error| format!("CHRONOS_LOG_FILTER is invalid: {error}"))?;
        logging.filter = trimmed.to_owned();
    }
    Ok(logging)
}

fn load_metadata_env() -> MetadataEnvConfig {
    MetadataEnvConfig {
        metadata_kind: effective_metadata_kind(),
        etcd_endpoints: read_csv_env("CHRONOS_ETCD_ENDPOINTS"),
        etcd_prefix: read_env_or_default("CHRONOS_ETCD_PREFIX", DEFAULT_ETCD_PREFIX),
    }
}

fn apply_metadata_env(config: &mut TsoConfig, metadata: &MetadataEnvConfig) {
    config.metadata_kind = metadata.metadata_kind.clone();
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
    apply_csv_env(
        "CHRONOS_GRPC_CONTROL_CERT_ALLOWLIST",
        &mut config.grpc_control_cert_allowlist,
    );
    apply_csv_env(
        "CHRONOS_GRPC_ROUTE_CERT_ALLOWLIST",
        &mut config.grpc_route_cert_allowlist,
    );
    apply_csv_env(
        "CHRONOS_GRPC_TIMESTAMP_CERT_ALLOWLIST",
        &mut config.grpc_timestamp_cert_allowlist,
    );
    apply_csv_env(
        "CHRONOS_GRPC_STATUS_CERT_ALLOWLIST",
        &mut config.grpc_status_cert_allowlist,
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
    apply_parsed_env(
        "CHRONOS_GRPC_MAX_CONNECTIONS",
        &mut config.grpc_max_connections,
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
    if let Ok(value) = env::var("CHRONOS_DEFAULT_RESOURCE_TIER") {
        config.default_resource_tier = parse_resource_tier_env(&value)?;
    }
    apply_parsed_env(
        "CHRONOS_MAX_TIMELINE_PROXY_LANES",
        &mut config.max_timeline_proxy_lanes,
    )?;
    apply_parsed_env(
        "CHRONOS_MAX_TIMELINE_RUNTIME_ENTRIES",
        &mut config.max_timeline_runtime_entries,
    )?;
    apply_parsed_env(
        "CHRONOS_MAX_CONCURRENT_TIMELINE_LOADS",
        &mut config.max_concurrent_timeline_loads,
    )?;
    apply_parsed_env(
        "CHRONOS_MAX_TIMELINE_RECORDS",
        &mut config.max_timeline_records,
    )?;
    apply_parsed_env(
        "CHRONOS_AUTO_FAILOVER_BATCH_SIZE",
        &mut config.auto_failover_batch_size,
    )?;
    Ok(())
}

fn apply_generator_ownership_env(config: &mut TsoConfig) -> AppResult<()> {
    apply_string_env("CHRONOS_OWNERSHIP_PLAN_ID", &mut config.ownership_plan_id);
    apply_parsed_env(
        "CHRONOS_GENERATOR_OWNERSHIP_MODULO",
        &mut config.generator_ownership_modulo,
    )?;
    apply_parsed_env(
        "CHRONOS_GENERATOR_OWNERSHIP_REMAINDER",
        &mut config.generator_ownership_remainder,
    )?;
    let remainders = read_u32_csv_env("CHRONOS_GENERATOR_OWNERSHIP_REMAINDERS")?;
    if !remainders.is_empty() {
        config.generator_ownership_remainder = remainders[0];
        config.generator_ownership_remainders = remainders;
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
    apply_parsed_env("CHRONOS_MAX_CLOCK_SKEW_MS", &mut config.max_clock_skew_ms)?;
    apply_parsed_env(
        "CHRONOS_RECOVERY_CATCHUP_BUDGET_MS",
        &mut config.recovery_catchup_budget_ms,
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
    apply_parsed_env("CHRONOS_PRE_BORROW_MS", &mut config.pre_borrow_ms)?;
    apply_parsed_env(
        "CHRONOS_SHARED_JUMP_AHEAD_THRESHOLD_MS",
        &mut config.shared_jump_ahead_threshold_ms,
    )?;
    apply_parsed_env(
        "CHRONOS_AUTO_FAILOVER_ENABLED",
        &mut config.auto_failover_enabled,
    )?;
    apply_parsed_env(
        "CHRONOS_AUTO_FAILOVER_INTERVAL_MS",
        &mut config.auto_failover_interval_ms,
    )?;
    apply_parsed_env("CHRONOS_SAFETY_GAP_MS", &mut config.safety_gap_ms)?;
    apply_parsed_env(
        "CHRONOS_REQUEST_RECORD_PENDING_TIMEOUT_MS",
        &mut config.request_record_pending_timeout_ms,
    )?;
    apply_parsed_env(
        "CHRONOS_REQUEST_RECORD_RETENTION_MS",
        &mut config.request_record_retention_ms,
    )?;
    Ok(())
}

fn reject_removed_startup_tuning_env_vars() -> AppResult<()> {
    let configured_removed_vars = REMOVED_STARTUP_TUNING_ENV_VARS
        .iter()
        .copied()
        .filter(|key| env::var_os(key).is_some())
        .collect::<Vec<_>>();

    if configured_removed_vars.is_empty() {
        return Ok(());
    }

    Err(format!(
        "unsupported startup tuning env var(s): {}. These internal tuning knobs are no longer configurable via environment.",
        configured_removed_vars.join(", ")
    )
    .into())
}

fn validate_cluster_format_env() -> AppResult<()> {
    let Ok(value) = env::var("CHRONOS_CLUSTER_FORMAT_VERSION") else {
        return Ok(());
    };
    let configured: u32 = value.parse()?;
    let expected = chronos::metadata::CURRENT_CLUSTER_FORMAT_VERSION;
    if configured != expected {
        return Err(format!(
            "CHRONOS_CLUSTER_FORMAT_VERSION must match this binary: expected {expected}, got {configured}"
        )
        .into());
    }
    Ok(())
}

fn build_config_and_metadata_env() -> AppResult<(TsoConfig, MetadataEnvConfig, StartupLoggingConfig)>
{
    reject_removed_startup_tuning_env_vars()?;
    validate_cluster_format_env()?;

    let mut config = TsoConfig::default();
    let metadata = load_metadata_env();
    let logging = load_logging_env()?;

    apply_profile_env(&mut config)?;
    apply_identity_env(&mut config);
    apply_metadata_env(&mut config, &metadata);
    apply_security_surface_env(&mut config)?;
    apply_transport_limit_env(&mut config)?;
    apply_capacity_env(&mut config)?;
    apply_generator_ownership_env(&mut config)?;
    apply_timing_env(&mut config)?;

    Ok((config, metadata, logging))
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
    let (config, _, _) = build_config_and_metadata_env()?;
    Ok(config)
}

pub(crate) fn load_startup_config() -> AppResult<LoadedStartupConfig> {
    let (config, metadata_env, logging) = build_config_and_metadata_env()?;
    let metadata = startup_metadata_from_env(config.metadata_kind.as_str(), metadata_env)?;
    Ok(LoadedStartupConfig::new(config, metadata, logging))
}

fn effective_metadata_kind() -> String {
    read_env_or_default("CHRONOS_METADATA", DEFAULT_METADATA_KIND).to_ascii_lowercase()
}
