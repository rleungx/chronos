use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use tokio::time::Duration;
use tracing::info;

use crate::metadata::ControlPlaneStore;
use crate::plane::{TsoControlPlane, TsoDataPlane};
use crate::runtime::{GeneratorRuntimeState, TimelineRuntimeState};
use crate::{Clock, HealthInfo, TsoConfig, TsoError, MAX_GENERATORS};

use super::background::BackgroundCoordinator;
use super::contention::MetadataContentionCoordinator;
use super::lease::GeneratorLeaseCoordinator;
use super::runtime_coordination::TimelineLoadCoordinator;
use super::TsoService;

static INSTANCE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
static CONTENTION_SEED_COUNTER: AtomicU64 = AtomicU64::new(1);

impl TsoService {
    pub fn new<C, M>(
        mut config: TsoConfig,
        clock: Arc<C>,
        metadata: Arc<M>,
    ) -> Result<Arc<Self>, TsoError>
    where
        C: Clock + 'static,
        M: ControlPlaneStore + 'static,
    {
        crate::metrics::init_build_info_metric();

        if config.generator_lease_ttl_ms == 0 {
            config.generator_lease_ttl_ms = config.lease_ttl_ms;
        }

        config.validate_for_startup().map_err(|error| match error {
            crate::config::TsoConfigValidationError::TooManyTierGenerators { .. } => {
                TsoError::GeneratorIdOutOfRange {
                    generator_id: config.shared_generators + config.warm_generators,
                }
            }
            crate::config::TsoConfigValidationError::GeneratorOwnershipMisconfigured {
                modulo,
                remainder,
            } => TsoError::GeneratorOwnershipMisconfigured { modulo, remainder },
            other => TsoError::Internal(other.to_string()),
        })?;
        config
            .validate_authoritative_metadata_runtime_contract()
            .map_err(|error| TsoError::Internal(error.to_string()))?;
        let instance_id = if config.instance_id.trim().is_empty() {
            format!(
                "{}#{}",
                config.advertise_endpoint,
                INSTANCE_ID_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
            )
        } else {
            config.instance_id.clone()
        };
        let max_timeline_runtime_entries = config.max_timeline_runtime_entries;
        let contention_jitter_seed = Self::contention_jitter_seed(&instance_id);
        let background = BackgroundCoordinator::new();
        let service = Arc::new(Self {
            config,
            instance_id,
            clock,
            metadata: metadata as Arc<dyn ControlPlaneStore>,
            generator_runtime: GeneratorRuntimeState::new(MAX_GENERATORS),
            timeline_runtime: TimelineRuntimeState::new(max_timeline_runtime_entries),
            timeline_load_coordinator: TimelineLoadCoordinator::default(),
            generator_lease_coordinator: GeneratorLeaseCoordinator::default(),
            metadata_contention: MetadataContentionCoordinator::new(contention_jitter_seed),
            background,
            ownership_drift: super::worker_readiness::OwnershipDriftTracker::default(),
        });

        {
            let weak_service = Arc::downgrade(&service);
            let maintenance_shutdown_rx = service.background.shutdown_listener();
            service.background.spawn_tracked(async move {
                Self::background_generator_maintenance_loop(weak_service, maintenance_shutdown_rx)
                    .await;
            });
        }

        let mut metadata_route_updates = service.metadata.subscribe_route_updates();
        let route_notifier = service.timeline_runtime.notifier();
        let route_reset_notifier = service.timeline_runtime.reset_notifier();
        {
            let weak_service = Arc::downgrade(&service);
            let route_update_shutdown_rx = service.background.shutdown_listener();
            service.background.spawn_tracked(async move {
                Self::metadata_route_update_loop(
                    weak_service,
                    &mut metadata_route_updates,
                    route_notifier,
                    route_reset_notifier,
                    route_update_shutdown_rx,
                )
                .await;
            });
        }

        Ok(service)
    }

    fn contention_jitter_seed(instance_id: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        std::process::id().hash(&mut hasher);
        instance_id.hash(&mut hasher);
        CONTENTION_SEED_COUNTER
            .fetch_add(1, AtomicOrdering::Relaxed)
            .hash(&mut hasher);
        hasher.finish().max(1)
    }

    pub async fn shutdown(&self) {
        if !self.background.begin_shutdown() {
            return;
        }

        info!(
            component = "shutdown",
            event = "background_stop_started",
            result = "success",
            reason = "service_shutdown_begin",
            worker_id = %self.config.worker_id,
            instance_id = %self.instance_id,
            advertise_endpoint = %self.config.advertise_endpoint,
            metadata_kind = %self.config.metadata_kind
        );
        self.background.drain_tasks().await;
        self.metadata.shutdown().await;
        info!(
            component = "shutdown",
            event = "background_stop_completed",
            result = "success",
            reason = "service_shutdown_complete",
            worker_id = %self.config.worker_id,
            instance_id = %self.instance_id,
            advertise_endpoint = %self.config.advertise_endpoint,
            metadata_kind = %self.config.metadata_kind
        );
    }

    pub fn health(&self) -> HealthInfo {
        HealthInfo {
            generator_count: self.generator_runtime.generator_count(),
            timeline_count: self.timeline_runtime.timeline_count(),
            worker_id: self.config.worker_id.clone(),
            instance_id: self.instance_id.clone(),
            advertise_endpoint: self.config.advertise_endpoint.clone(),
        }
    }

    pub fn control_plane(self: &Arc<Self>) -> TsoControlPlane {
        TsoControlPlane::new(self.clone())
    }

    pub fn set_worker_readiness_sink(&self, sink: Arc<dyn crate::service::WorkerReadinessSink>) {
        self.ownership_drift.set_sink(sink);
    }

    pub fn data_plane(self: &Arc<Self>) -> TsoDataPlane {
        TsoDataPlane::new(self.clone())
    }

    pub fn advertise_endpoint(&self) -> &str {
        &self.config.advertise_endpoint
    }

    pub(crate) fn max_timeline_proxy_lanes(&self) -> usize {
        self.config.max_timeline_proxy_lanes
    }

    pub fn subscribe_route_changes(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::TimelineRoute> {
        self.timeline_runtime.subscribe_route_changes()
    }

    pub(super) fn metadata_contention_retry_budget(&self) -> Duration {
        self.metadata_contention.retry_budget(&self.config)
    }
}

impl Drop for TsoService {
    fn drop(&mut self) {
        self.background.abort_all();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::TsoService;
    use crate::{metadata::MemoryMetadataStore, ManualClock, TsoConfig, TsoError, TsoSecurityMode};

    #[test]
    fn contention_jitter_seed_is_non_zero() {
        assert_ne!(TsoService::contention_jitter_seed("instance-a"), 0);
    }

    #[test]
    fn contention_jitter_seed_changes_across_calls() {
        let first = TsoService::contention_jitter_seed("instance-a");
        let second = TsoService::contention_jitter_seed("instance-a");
        assert_ne!(first, second);
    }

    #[test]
    fn service_new_rejects_default_worker_id_for_etcd_metadata() {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            etcd_endpoints: vec!["127.0.0.1:2379".into()],
            advertise_endpoint: "10.0.0.10:50051".into(),
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some("server.crt".into()),
            grpc_tls_key_file: Some("server.key".into()),
            grpc_client_ca_file: Some("ca.pem".into()),
            safety_gap_ms: 1,
            ..TsoConfig::default()
        };

        let error = match TsoService::new(
            config,
            Arc::new(ManualClock::new(1_000)),
            Arc::new(MemoryMetadataStore::new()),
        ) {
            Ok(_) => panic!("service construction should reject default worker id for etcd"),
            Err(error) => error,
        };

        assert!(matches!(error, TsoError::Internal(_)));
    }
}
