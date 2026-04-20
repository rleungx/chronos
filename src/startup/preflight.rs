use std::error::Error;

use chronos::{TsoConfig, TsoError, TsoSecurityMode};
use tracing::info;

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

#[derive(Debug, Clone)]
pub(crate) struct ValidatedStartupPlan<'a> {
    startup: &'a LoadedStartupConfig,
    effective_security_mode: TsoSecurityMode,
    metrics_transport: MetricsTransport,
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
}

fn validate_production_profile_contract(startup: &LoadedStartupConfig) -> AppResult<()> {
    if !startup.config.production_profile {
        return Ok(());
    }

    match &startup.metadata {
        StartupMetadata::Etcd(_) => Ok(()),
        StartupMetadata::Memory => {
            Err("CHRONOS_PROFILE=production requires CHRONOS_METADATA=etcd".into())
        }
    }
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
    })
}

pub(crate) fn validate_startup_preflight(
    startup: &LoadedStartupConfig,
) -> AppResult<ValidatedStartupPlan<'_>> {
    let config: &TsoConfig = &startup.config;
    config.validate_for_startup()?;
    validate_build_identity_for_profile(config, chronos::build_identity())?;
    validate_production_profile_contract(startup)?;

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
        safety_gap_ms = config.safety_gap_ms
    );
}
