use std::sync::Arc;
use std::time::Duration;

use chronos::metadata::{
    ControlPlaneStore, EtcdMetadataStore, IdentityLeaseAuthority, InstanceIdentityLease,
    MemoryMetadataStore,
};
use chronos::{SystemClock, TsoConfig, TsoService};
use tracing::info;

use crate::AppResult;

use super::config::{LoadedStartupConfig, StartupMetadata};

pub(crate) async fn build_tso_service(
    startup: &LoadedStartupConfig,
    clock: Arc<SystemClock>,
) -> AppResult<(Arc<TsoService>, Option<InstanceIdentityLease>)> {
    let mut service_config = startup.config.clone();
    if service_config.instance_id.trim().is_empty() {
        service_config.instance_id = startup.config.effective_instance_id().to_owned();
    }
    let config: &TsoConfig = &service_config;
    match &startup.metadata {
        StartupMetadata::Memory => {
            let metadata = Arc::new(MemoryMetadataStore::new());
            run_metadata_startup_probe(metadata.as_ref()).await?;
            Ok((TsoService::new(service_config, clock, metadata)?, None))
        }
        StartupMetadata::Etcd(etcd) => {
            let metadata = Arc::new(
                EtcdMetadataStore::from_config_with_endpoints(
                    config,
                    etcd.endpoints.clone(),
                    etcd.prefix.clone(),
                )
                .await?,
            );
            let identity_lease = metadata
                .acquire_instance_identity_lease(
                    config.effective_instance_id(),
                    &config.worker_id,
                    &config.advertise_endpoint,
                    Duration::from_millis(config.lease_ttl_ms),
                )
                .await?;
            finalize_etcd_startup(service_config, clock, metadata, identity_lease).await
        }
    }
}

async fn finalize_etcd_startup(
    config: TsoConfig,
    clock: Arc<SystemClock>,
    metadata: Arc<EtcdMetadataStore>,
    mut identity_lease: InstanceIdentityLease,
) -> AppResult<(Arc<TsoService>, Option<InstanceIdentityLease>)> {
    if let Err(error) = metadata
        .verify_instance_identity_write_path(
            identity_lease.lease_id(),
            config.effective_instance_id(),
            &config.worker_id,
            &config.advertise_endpoint,
        )
        .await
    {
        identity_lease.shutdown().await;
        return Err(Box::new(error));
    }

    if let Err(error) = run_metadata_startup_probe(metadata.as_ref()).await {
        identity_lease.shutdown().await;
        return Err(error);
    }

    if config.generator_ownership_modulo > 1 {
        if let Err(error) = metadata.admit_ownership_plan_member(&config).await {
            identity_lease.shutdown().await;
            return Err(Box::new(error));
        }
        info!(
            component = "startup",
            event = "ownership_plan_admitted",
            result = "success",
            reason = "member_accepted",
            ownership_plan_id = %config.ownership_plan_id,
            generator_ownership_modulo = config.generator_ownership_modulo,
            generator_ownership_remainder = config.generator_ownership_remainder,
            worker_id = %config.worker_id,
            advertise_endpoint = %config.advertise_endpoint
        );
    } else {
        info!(
            component = "startup",
            event = "ownership_plan_skipped",
            result = "success",
            reason = "unpartitioned_generator_ownership",
            generator_ownership_modulo = config.generator_ownership_modulo,
            worker_id = %config.worker_id,
            advertise_endpoint = %config.advertise_endpoint
        );
    }

    match TsoService::new(config, clock, metadata) {
        Ok(service) => Ok((service, Some(identity_lease))),
        Err(error) => {
            identity_lease.shutdown().await;
            Err(error.into())
        }
    }
}

pub(crate) async fn run_metadata_startup_probe<M>(metadata: &M) -> AppResult<()>
where
    M: ControlPlaneStore + ?Sized,
{
    info!(
        component = "startup",
        event = "metadata_probe_started",
        result = "success",
        reason = "probe_begin"
    );
    metadata
        .load_timeline_route("__chronos_startup_probe__")
        .await?;
    metadata.load_generator(0).await?;
    let _ = metadata.subscribe_route_updates();
    info!(
        component = "startup",
        event = "metadata_probe_result",
        result = "success",
        reason = "probe_completed"
    );
    Ok(())
}
