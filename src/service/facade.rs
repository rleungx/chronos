use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::time::Duration;
use tracing::{info, warn};

use crate::metadata::ControlPlaneStore;
use crate::plane::{TsoControlPlane, TsoDataPlane};
use crate::runtime::{GeneratorRuntimeState, TimelineRuntimeState};
use crate::{
    Clock, HealthInfo, ResourceTier, TimelineRoute, TransferReason, TsoConfig, TsoError,
    MAX_GENERATORS,
};

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
        let max_concurrent_timeline_loads = config.max_concurrent_timeline_loads;
        let contention_jitter_seed = Self::contention_jitter_seed(&instance_id);
        let background = BackgroundCoordinator::new();
        let service = Arc::new(Self {
            config,
            instance_id,
            clock,
            metadata: metadata as Arc<dyn ControlPlaneStore>,
            generator_runtime: GeneratorRuntimeState::new(MAX_GENERATORS),
            timeline_runtime: TimelineRuntimeState::new(max_timeline_runtime_entries),
            generator_admission_gates: (0..MAX_GENERATORS)
                .map(|_| Arc::new(Semaphore::new(1)))
                .collect(),
            generator_fairness_trackers: (0..MAX_GENERATORS)
                .map(|_| {
                    Arc::new(tokio::sync::Mutex::new(
                        super::GeneratorFairnessState::default(),
                    ))
                })
                .collect(),
            generator_fairness_notifiers: (0..MAX_GENERATORS)
                .map(|_| Arc::new(tokio::sync::Notify::new()))
                .collect(),
            timeline_load_coordinator: TimelineLoadCoordinator::default(),
            timeline_load_limiter: Arc::new(Semaphore::new(max_concurrent_timeline_loads)),
            generator_lease_coordinator: GeneratorLeaseCoordinator::default(),
            metadata_contention: MetadataContentionCoordinator::new(contention_jitter_seed),
            background,
            ownership_drift: super::worker_readiness::OwnershipDriftTracker::default(),
            shutdown_gate: AtomicBool::new(false),
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
        self.request_local_shutdown_gate();

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
        self.best_effort_persist_runtime_before_shutdown().await;
        self.background.drain_tasks().await;
        self.invalidate_local_runtime_after_shutdown();
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

    async fn best_effort_persist_runtime_before_shutdown(&self) {
        let (local_timelines, local_generator_ids, candidates) =
            match self.collect_shutdown_inventory().await {
                Ok(inventory) => inventory,
                Err(error) => {
                    warn!(
                        component = "shutdown",
                        event = "runtime_flush_skipped",
                        result = "degraded",
                        reason = %error,
                        advertise_endpoint = %self.config.advertise_endpoint,
                        metadata_kind = %self.config.metadata_kind
                    );
                    return;
                }
            };

        for timeline in &local_timelines {
            if let Err(error) = self
                .best_effort_persist_local_timeline_floor(&timeline.timeline_key)
                .await
            {
                warn!(
                    component = "shutdown",
                    event = "timeline_flush_failed",
                    result = "degraded",
                    reason = %error,
                    timeline_key = %timeline.timeline_key
                );
            }
        }

        for generator_id in local_generator_ids {
            if let Err(error) = self
                .best_effort_persist_local_generator_floor(generator_id)
                .await
            {
                warn!(
                    component = "shutdown",
                    event = "generator_flush_failed",
                    result = "degraded",
                    reason = %error,
                    generator_id
                );
            }
        }

        self.best_effort_transfer_local_timelines_before_shutdown(&local_timelines, &candidates)
            .await;
    }

    fn invalidate_local_runtime_after_shutdown(&self) {
        self.generator_runtime.clear_leases();
        self.generator_runtime.clear_dedicated_claims();
        self.timeline_runtime.clear();
    }

    async fn collect_shutdown_inventory(
        &self,
    ) -> Result<(Vec<TimelineRoute>, HashSet<u32>, ShutdownTransferCandidates), TsoError> {
        let mut local_timelines = Vec::new();
        let mut local_generator_ids = HashSet::new();
        let mut candidates = ShutdownTransferCandidates::default();
        let mut seen_candidates = HashSet::new();
        let mut cursor = None;
        let page_size = self.config.max_timeline_runtime_entries.clamp(1, 256);

        loop {
            let page = self
                .metadata
                .list_timeline_filters_page(cursor.as_deref(), page_size)
                .await?;
            if page.records.is_empty() {
                break;
            }

            for timeline in page.records {
                if self.is_local_endpoint(&timeline.route.owner_worker_endpoint) {
                    local_generator_ids.insert(timeline.route.generator_id);
                    local_timelines.push(timeline.route);
                    continue;
                }

                let dedupe_key = (
                    timeline.route.owner_worker_endpoint.clone(),
                    timeline.route.generator_id,
                );
                if !seen_candidates.insert(dedupe_key) {
                    continue;
                }

                let candidate = (
                    timeline.route.owner_worker_endpoint.clone(),
                    timeline.route.generator_id,
                );
                match timeline.route.resource_tier {
                    ResourceTier::Shared => candidates.shared.push(candidate),
                    ResourceTier::Warm => candidates.warm.push(candidate),
                    ResourceTier::Dedicated => candidates.dedicated.push(candidate),
                }
            }

            cursor = page.next_start_after_timeline_key;
            if cursor.is_none() {
                break;
            }
        }

        Ok((local_timelines, local_generator_ids, candidates))
    }

    async fn best_effort_transfer_local_timelines_before_shutdown(
        &self,
        timelines: &[TimelineRoute],
        candidates: &ShutdownTransferCandidates,
    ) {
        if candidates.shared.is_empty()
            && candidates.warm.is_empty()
            && candidates.dedicated.is_empty()
        {
            return;
        }
        let mut transfer_state = ShutdownTransferState::default();

        for timeline in timelines {
            if !self.is_local_endpoint(&timeline.owner_worker_endpoint) {
                continue;
            }
            let Some((target_endpoint, target_generator_id)) = Self::shutdown_transfer_target(
                candidates,
                timeline.resource_tier,
                &mut transfer_state,
            ) else {
                continue;
            };

            if let Err(error) = self
                .transfer_timeline_for_rpc(
                    &timeline.timeline_key,
                    target_endpoint.clone(),
                    Some(target_generator_id),
                    TransferReason::Rebalance,
                )
                .await
            {
                warn!(
                    component = "shutdown",
                    event = "timeline_transfer_failed",
                    result = "degraded",
                    reason = %error,
                    timeline_key = %timeline.timeline_key,
                    target_owner_endpoint = %target_endpoint,
                    target_generator_id
                );
            }
        }
    }

    fn shutdown_transfer_target(
        candidates: &ShutdownTransferCandidates,
        resource_tier: ResourceTier,
        state: &mut ShutdownTransferState,
    ) -> Option<(String, u32)> {
        let (pool, cursor) = match resource_tier {
            ResourceTier::Shared => (&candidates.shared, &mut state.shared),
            ResourceTier::Warm => (&candidates.warm, &mut state.warm),
            ResourceTier::Dedicated => (&candidates.dedicated, &mut state.dedicated),
        };
        if pool.is_empty() {
            return None;
        }
        let selected = pool[*cursor % pool.len()].clone();
        *cursor = cursor.saturating_add(1);
        Some(selected)
    }

    async fn best_effort_persist_local_timeline_floor(
        &self,
        timeline_key: &str,
    ) -> Result<(), TsoError> {
        let local_last_issued = match self.timeline_runtime.timeline_handle(timeline_key) {
            Some(handle) => handle.lock().await.last_issued_tso,
            None => None,
        };
        let Some(local_last_issued) = local_last_issued else {
            return Ok(());
        };

        let Some((mut record, revision)) = self.metadata.load_timeline(timeline_key).await? else {
            return Ok(());
        };
        if !self.is_local_endpoint(&record.route.owner_worker_endpoint) {
            return Ok(());
        }

        let desired_last_graceful = record
            .last_graceful_issued
            .map(|current| current.max(local_last_issued))
            .unwrap_or(local_last_issued);
        let desired_recovery_floor = record
            .recovery_floor_tso
            .map(|current| current.max(desired_last_graceful))
            .unwrap_or(desired_last_graceful);

        if record.last_graceful_issued == Some(desired_last_graceful)
            && record.recovery_floor_tso == Some(desired_recovery_floor)
        {
            return Ok(());
        }

        record.last_graceful_issued = Some(desired_last_graceful);
        record.recovery_floor_tso = Some(desired_recovery_floor);
        record.updated_at_ms = self.clock.now_ms();
        match self
            .metadata
            .compare_exchange_timeline(timeline_key, revision, &record)
            .await
        {
            Ok(_) | Err(TsoError::CasFailed) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn best_effort_persist_local_generator_floor(
        &self,
        generator_id: u32,
    ) -> Result<(), TsoError> {
        let Some(local_last_issued) = self
            .lookup_generator(generator_id)?
            .current_last_issued_tso()?
        else {
            return Ok(());
        };
        let Some((mut record, revision)) = self.metadata.load_generator(generator_id).await? else {
            return Ok(());
        };
        if !self.is_local_generator_owner(&record) {
            return Ok(());
        }

        let desired_last_issued = record
            .last_issued_tso
            .map(|current| current.max(local_last_issued))
            .unwrap_or(local_last_issued);
        if record.last_issued_tso == Some(desired_last_issued) {
            return Ok(());
        }

        record.last_issued_tso = Some(desired_last_issued);
        record.updated_at_ms = self.clock.now_ms();
        match self
            .metadata
            .compare_exchange_generator(generator_id, revision, &record)
            .await
        {
            Ok(_) | Err(TsoError::CasFailed) => Ok(()),
            Err(error) => Err(error),
        }
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

#[derive(Debug, Default)]
struct ShutdownTransferCandidates {
    shared: Vec<(String, u32)>,
    warm: Vec<(String, u32)>,
    dedicated: Vec<(String, u32)>,
}

#[derive(Debug, Default)]
struct ShutdownTransferState {
    shared: usize,
    warm: usize,
    dedicated: usize,
}

impl Drop for TsoService {
    fn drop(&mut self) {
        self.background.abort_all();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{ShutdownTransferCandidates, ShutdownTransferState, TsoService};
    use crate::metadata::{GeneratorLeaseAuthority, MemoryMetadataStore, TimelineAuthority};
    use crate::{AllocateTimestampsRequest, ManualClock, ResourceTier, TsoConfig, TsoError};

    fn required_test_config(config: TsoConfig) -> TsoConfig {
        crate::test_tls::required_grpc_tls_test_config(config, 100)
    }

    fn with_worker(mut config: TsoConfig, worker_id: &str) -> TsoConfig {
        config.worker_id = worker_id.to_owned();
        config.advertise_endpoint = format!("{worker_id}:50051");
        required_test_config(config)
    }

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

    #[tokio::test]
    async fn service_new_uses_effective_instance_id_when_config_instance_is_blank() {
        let config = required_test_config(TsoConfig {
            advertise_endpoint: "worker-a:50051".into(),
            ..TsoConfig::default()
        });

        let service = TsoService::new(
            config,
            Arc::new(ManualClock::new(1_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();

        assert!(service.health().instance_id.starts_with("worker-a:50051#"));

        service.shutdown().await;
    }

    #[test]
    fn service_new_rejects_default_worker_id_for_etcd_metadata() {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            etcd_endpoints: vec!["127.0.0.1:2379".into()],
            advertise_endpoint: "10.0.0.10:50051".into(),
            safety_gap_ms: 1,
            ..TsoConfig::default()
        };
        let config = crate::test_tls::required_grpc_tls_test_config(config, 100);

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

    #[tokio::test]
    async fn service_shutdown_persists_local_timeline_graceful_floor() {
        let clock = Arc::new(ManualClock::new(40_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            with_worker(TsoConfig::default(), "worker-a"),
            clock,
            metadata,
        )
        .unwrap();

        let route = service
            .ensure_timeline("shutdown.flush.timeline")
            .await
            .unwrap();
        let allocation = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "shutdown-flush".into(),
            })
            .await
            .unwrap();
        let last_issued = allocation.ranges.last().unwrap().end_tso;

        let before = service
            .get_timeline_record(&route.timeline_key)
            .await
            .unwrap();
        assert_eq!(before.last_graceful_issued, None);

        service.shutdown().await;

        let after = service
            .get_timeline_record(&route.timeline_key)
            .await
            .unwrap();
        assert_eq!(after.last_graceful_issued, Some(last_issued));
        assert_eq!(after.recovery_floor_tso, Some(last_issued));
    }

    #[tokio::test]
    async fn service_shutdown_blocks_new_ensure_and_allocate_work() {
        let clock = Arc::new(ManualClock::new(40_500));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            with_worker(TsoConfig::default(), "worker-a"),
            clock,
            metadata,
        )
        .unwrap();

        let route = service
            .ensure_timeline("shutdown.gate.timeline")
            .await
            .unwrap();

        service.shutdown().await;

        assert_eq!(
            service
                .ensure_timeline("shutdown.gate.new")
                .await
                .unwrap_err(),
            TsoError::ServiceShuttingDown
        );
        assert_eq!(
            service
                .allocate_timestamps(AllocateTimestampsRequest {
                    timeline_key: route.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route.epoch,
                    expected_route_version: route.route_version,
                    client_request_id: "shutdown-gate".into(),
                })
                .await
                .unwrap_err(),
            TsoError::ServiceShuttingDown
        );
    }

    #[tokio::test]
    async fn service_shutdown_clears_local_runtime_and_cached_leases() {
        let clock = Arc::new(ManualClock::new(40_750));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            with_worker(TsoConfig::default(), "worker-a"),
            clock,
            metadata,
        )
        .unwrap();

        let route = service
            .ensure_timeline("shutdown.clear-runtime")
            .await
            .unwrap();
        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "shutdown-clear-runtime".into(),
            })
            .await
            .unwrap();

        assert!(service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .is_some());
        assert!(service
            .generator_runtime
            .lease_state(route.generator_id)
            .is_some());

        service.shutdown().await;

        assert!(service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .is_none());
        assert!(service
            .generator_runtime
            .lease_state(route.generator_id)
            .is_none());
    }

    #[tokio::test]
    async fn service_shutdown_persists_local_generator_floor_without_extending_lease() {
        let clock = Arc::new(ManualClock::new(41_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            with_worker(TsoConfig::default(), "worker-a"),
            clock,
            metadata.clone(),
        )
        .unwrap();

        let route = service
            .ensure_timeline("shutdown.flush.generator")
            .await
            .unwrap();
        let allocation = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "shutdown-generator-flush".into(),
            })
            .await
            .unwrap();
        let last_issued = allocation.ranges.last().unwrap().end_tso;

        let (mut record, revision) = metadata
            .load_generator(route.generator_id)
            .await
            .unwrap()
            .unwrap();
        let original_expiry = record.lease_expire_at_ms;
        record.last_issued_tso = None;
        record.issued_upper_bound = None;
        metadata
            .compare_exchange_generator(route.generator_id, revision, &record)
            .await
            .unwrap();

        service.shutdown().await;

        let flushed = metadata
            .load_generator(route.generator_id)
            .await
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(flushed.last_issued_tso, Some(last_issued));
        assert_eq!(flushed.lease_expire_at_ms, original_expiry);
        assert_eq!(flushed.issued_upper_bound, None);
    }

    #[tokio::test]
    async fn service_shutdown_transfers_local_timeline_when_remote_candidate_exists() {
        let clock = Arc::new(ManualClock::new(42_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let config = TsoConfig {
            max_future_borrow_ms: 2_000,
            ..TsoConfig::default()
        };
        let service_a = TsoService::new(
            with_worker(config.clone(), "worker-a"),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();
        let service_b = TsoService::new(
            with_worker(config, "worker-b"),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();

        let remote_anchor = service_b
            .ensure_timeline("shutdown.transfer.anchor")
            .await
            .unwrap();
        let target = service_a
            .ensure_timeline("shutdown.transfer.target")
            .await
            .unwrap();
        let before = service_a
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: target.timeline_key.clone(),
                count: 1,
                expected_epoch: target.epoch,
                expected_route_version: target.route_version,
                client_request_id: "shutdown-transfer".into(),
            })
            .await
            .unwrap();
        let before_last = before.ranges.last().unwrap().end_tso;

        service_a.shutdown().await;
        clock.set(43_001);

        let moved = service_b
            .get_timeline_route(&target.timeline_key)
            .await
            .unwrap();
        assert_eq!(moved.owner_worker_endpoint, service_b.advertise_endpoint());
        assert_eq!(moved.generator_id, remote_anchor.generator_id);
        assert!(moved.route_version > target.route_version);

        let resumed = service_b
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: moved.timeline_key.clone(),
                count: 1,
                expected_epoch: moved.epoch,
                expected_route_version: moved.route_version,
                client_request_id: "shutdown-transfer-resume".into(),
            })
            .await
            .unwrap();
        let resumed_start = resumed.ranges.first().unwrap().start_tso;
        assert!(resumed_start > before_last);

        let persisted = metadata
            .load_timeline(&target.timeline_key)
            .await
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(
            persisted.route.owner_worker_endpoint,
            service_b.advertise_endpoint()
        );
        assert_eq!(persisted.route.generator_id, remote_anchor.generator_id);
        assert!(persisted.recovery_floor_tso.is_some());

        service_b.shutdown().await;
    }

    #[test]
    fn shutdown_transfer_target_rotates_across_same_tier_candidates() {
        let candidates = ShutdownTransferCandidates {
            shared: vec![("worker-b:50051".into(), 1), ("worker-c:50051".into(), 2)],
            warm: Vec::new(),
            dedicated: Vec::new(),
        };
        let mut state = ShutdownTransferState::default();

        assert_eq!(
            TsoService::shutdown_transfer_target(&candidates, ResourceTier::Shared, &mut state),
            Some(("worker-b:50051".into(), 1))
        );
        assert_eq!(
            TsoService::shutdown_transfer_target(&candidates, ResourceTier::Shared, &mut state),
            Some(("worker-c:50051".into(), 2))
        );
        assert_eq!(
            TsoService::shutdown_transfer_target(&candidates, ResourceTier::Shared, &mut state),
            Some(("worker-b:50051".into(), 1))
        );
    }
}
