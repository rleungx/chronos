use std::error::Error;

use chronos::{
    TsoConfig, TsoError, TsoSecurityMode, DEFAULT_MAX_BATCH_PER_REQUEST,
    DEFAULT_MAX_TIMELINE_PROXY_LANES, DEFAULT_MAX_TIMELINE_RUNTIME_ENTRIES,
};
use tracing::{info, warn};

use crate::AppResult;

use super::config::{LoadedStartupConfig, StartupMetadata};

pub(crate) fn validate_build_identity_for_profile(
    config: &TsoConfig,
    identity: chronos::BuildIdentity,
) -> AppResult<()> {
    if config.production_profile && identity.commit == "unknown" {
        return Err(
            "CHRONOS_PROFILE=production requires an auditable build commit; ensure CHRONOS_BUILD_COMMIT or git metadata is available at build time"
                .into(),
        );
    }
    Ok(())
}

pub(crate) fn startup_failure_stage(error: &(dyn Error + 'static)) -> &'static str {
    match error.downcast_ref::<TsoError>() {
        Some(TsoError::InstanceIdentityInUse { .. }) => "identity",
        Some(TsoError::ClusterContractMismatch { .. }) => "contract",
        _ => "metadata",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetricsTransport {
    Plain,
    Mtls,
}

impl MetricsTransport {
    fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Mtls => "mtls",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StartupAdvisory {
    field: &'static str,
    value: u64,
}

impl StartupAdvisory {
    fn new(field: &'static str, value: u64) -> Self {
        Self { field, value }
    }

    pub(crate) fn field(self) -> &'static str {
        self.field
    }

    pub(crate) fn value(self) -> u64 {
        self.value
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ValidatedStartupPlan<'a> {
    startup: &'a LoadedStartupConfig,
    effective_security_mode: TsoSecurityMode,
    metrics_transport: MetricsTransport,
    advisories: Vec<StartupAdvisory>,
}

impl<'a> ValidatedStartupPlan<'a> {
    pub(crate) fn startup(&self) -> &'a LoadedStartupConfig {
        self.startup
    }

    pub(crate) fn effective_security_mode(&self) -> TsoSecurityMode {
        self.effective_security_mode
    }

    pub(crate) fn metrics_transport(&self) -> MetricsTransport {
        self.metrics_transport
    }

    pub(crate) fn advisories(&self) -> &[StartupAdvisory] {
        &self.advisories
    }
}

fn build_startup_advisories(config: &TsoConfig) -> Vec<StartupAdvisory> {
    let mut advisories = Vec::new();
    if config.max_timeline_proxy_lanes > DEFAULT_MAX_TIMELINE_PROXY_LANES {
        advisories.push(StartupAdvisory::new(
            "max_timeline_proxy_lanes",
            config.max_timeline_proxy_lanes as u64,
        ));
    }
    if config.max_timeline_runtime_entries > DEFAULT_MAX_TIMELINE_RUNTIME_ENTRIES {
        advisories.push(StartupAdvisory::new(
            "max_timeline_runtime_entries",
            config.max_timeline_runtime_entries as u64,
        ));
    }
    if config.max_batch_per_request > DEFAULT_MAX_BATCH_PER_REQUEST {
        advisories.push(StartupAdvisory::new(
            "max_batch_per_request",
            config.max_batch_per_request as u64,
        ));
    }
    advisories
}

fn build_validated_startup_plan(
    startup: &LoadedStartupConfig,
) -> AppResult<ValidatedStartupPlan<'_>> {
    let config: &TsoConfig = &startup.config;
    Ok(ValidatedStartupPlan {
        startup,
        effective_security_mode: config.resolve_effective_security_mode()?,
        metrics_transport: if config.metrics_tls_paths()?.is_some() {
            MetricsTransport::Mtls
        } else {
            MetricsTransport::Plain
        },
        advisories: build_startup_advisories(config),
    })
}

pub(crate) fn validate_startup_preflight(
    startup: &LoadedStartupConfig,
) -> AppResult<ValidatedStartupPlan<'_>> {
    let config: &TsoConfig = &startup.config;
    config.validate_for_startup()?;
    validate_build_identity_for_profile(config, chronos::build_identity())?;

    match &startup.metadata {
        StartupMetadata::Memory => build_validated_startup_plan(startup),
        StartupMetadata::Etcd(etcd) => {
            config.validate_authoritative_metadata_store_contract(&etcd.endpoints, &etcd.prefix)?;
            config.validate_authoritative_metadata_runtime_contract()?;
            build_validated_startup_plan(startup)
        }
    }
}

pub(crate) fn log_startup_preflight(plan: &ValidatedStartupPlan<'_>) {
    let startup = plan.startup();
    let config: &TsoConfig = &startup.config;
    info!(
        component = "startup",
        event = "preflight_passed",
        result = "success",
        reason = "validated",
        startup_stage = "config",
        metadata_kind = startup.metadata_kind(),
        security_mode = %plan.effective_security_mode(),
        bind_addr = %config.bind_addr,
        metrics_bind_addr = %config.metrics_bind_addr,
        metrics_transport = plan.metrics_transport().as_str(),
        worker_id = %config.worker_id,
        instance_id = %config.effective_instance_id(),
        advertise_endpoint = %config.advertise_endpoint,
        build_version = chronos::build_version(),
        build_commit = chronos::build_commit(),
        mixed_version_contract_id = chronos::mixed_version_contract_id(),
        default_resource_tier = %config.default_resource_tier,
        shared_generators = config.shared_generators,
        warm_generators = config.warm_generators,
        route_cache_ttl_ms = config.route_cache_ttl_ms,
        max_timeline_proxy_lanes = config.max_timeline_proxy_lanes,
        max_timeline_runtime_entries = config.max_timeline_runtime_entries,
        generator_ownership_modulo = config.generator_ownership_modulo,
        generator_ownership_remainder = config.generator_ownership_remainder
    );

    for advisory in plan.advisories() {
        warn!(
            component = "startup",
            event = "preflight_advisory",
            result = "degraded",
            reason = "override_above_production_default",
            field = advisory.field(),
            value = advisory.value()
        );
    }
}
