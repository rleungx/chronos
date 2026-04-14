mod serve;

use std::sync::Arc;

use crate::plane::RequestCancellation;
use tokio::sync::Mutex;

use crate::metadata::TimelineRecord;
use crate::timeline_state::build_timeline_state;
use crate::{
    metrics, AllocateTimestampsRequest, AllocateTimestampsResponse, ResourceTier,
    TimelineLifecycleState, TimelineRoute, TsoError,
};

use super::TsoService;

impl TsoService {
    fn validate_batch_for_route(&self, count: u32, route: &TimelineRoute) -> Result<(), TsoError> {
        let max = match route.resource_tier {
            ResourceTier::Shared => self
                .config
                .max_batch_per_request
                .min(crate::SEQUENCE_CAPACITY),
            ResourceTier::Warm | ResourceTier::Dedicated => self.config.max_batch_per_request,
        };

        if count > max {
            return Err(TsoError::BatchTooLarge {
                requested: count,
                max,
            });
        }
        Ok(())
    }

    fn effective_future_borrow_ms_for_route(&self, route: &TimelineRoute) -> u64 {
        match route.resource_tier {
            ResourceTier::Shared | ResourceTier::Warm | ResourceTier::Dedicated => {
                self.config.max_future_borrow_ms
            }
        }
    }

    pub async fn ensure_timeline(&self, timeline_key: &str) -> Result<TimelineRoute, TsoError> {
        self.ensure_timeline_with_tier(timeline_key, self.config.default_resource_tier)
            .await
    }

    pub async fn ensure_timeline_with_tier(
        &self,
        timeline_key: &str,
        resource_tier: ResourceTier,
    ) -> Result<TimelineRoute, TsoError> {
        loop {
            let cached_timeline = self.timeline_runtime.timeline_handle(timeline_key);
            if let Some(timeline_handle) = cached_timeline {
                let timeline = timeline_handle.lock().await;
                if self.is_local_endpoint(&timeline.route.owner_worker_endpoint)
                    && Self::timeline_is_ready(timeline.state)
                {
                    return Ok(timeline.route.clone());
                }
            }

            match self.load_timeline_with_singleflight(timeline_key).await? {
                Some((record, revision)) => {
                    if self.is_local_endpoint(&record.route.owner_worker_endpoint) {
                        match self
                            .activate_local_timeline_record(timeline_key, record.clone(), revision)
                            .await
                        {
                            Ok((record, revision)) => {
                                self.upsert_timeline_cache_from_record(
                                    timeline_key,
                                    &record,
                                    revision,
                                )
                                .await?;
                                return Ok(record.route);
                            }
                            Err(TsoError::NotGeneratorOwner { .. }) => {
                                self.restore_dedicated_claim(timeline_key, &record)?;
                                return Ok(record.route);
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    return Ok(record.route);
                }
                None => {
                    let generator_id = self.pick_generator_id(timeline_key, resource_tier)?;
                    self.ensure_generator_lease(generator_id).await?;
                    let route = TimelineRoute {
                        timeline_key: timeline_key.to_owned(),
                        generator_id,
                        epoch: 1,
                        route_version: 1,
                        resource_tier,
                        owner_worker_endpoint: self.config.advertise_endpoint.clone(),
                    };

                    let record = TimelineRecord {
                        route: route.clone(),
                        state: TimelineLifecycleState::Active,
                        recovery_floor_tso: None,
                        issued_upper_bound: None,
                        last_graceful_issued: None,
                        lease_expire_at_ms: None,
                        updated_at_ms: self.clock.now_ms(),
                    };

                    match self.metadata.create_timeline(timeline_key, &record).await {
                        Ok(revision) => {
                            let timeline =
                                Arc::new(Mutex::new(build_timeline_state(&record, revision, None)));
                            let _ =
                                self.best_effort_insert_timeline_cache(timeline_key, timeline)?;
                            return Ok(route);
                        }
                        Err(TsoError::MetadataAlreadyExists) => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
        }
    }

    pub async fn allocate_timestamps(
        &self,
        request: AllocateTimestampsRequest,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        self.allocate_timestamps_with_cancellation(request, None)
            .await
    }

    pub(crate) async fn allocate_timestamps_with_cancellation(
        &self,
        request: AllocateTimestampsRequest,
        cancellation: Option<RequestCancellation>,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        let _timer = metrics::TSO_ALLOCATE_LATENCY.start_timer();

        if request.count == 0 {
            return Err(TsoError::InvalidCount);
        }
        if request.count > self.config.max_batch_per_request {
            return Err(TsoError::BatchTooLarge {
                requested: request.count,
                max: self.config.max_batch_per_request,
            });
        }

        loop {
            Self::check_request_cancellation(cancellation.as_ref())?;
            if let Some(response) = self
                .try_allocate_from_cached_timeline(&request, cancellation.clone())
                .await?
            {
                return Ok(response);
            }

            let (timeline_record, revision) = self
                .load_timeline_with_singleflight(&request.timeline_key)
                .await?
                .ok_or_else(|| TsoError::TimelineNotFound {
                    timeline_key: request.timeline_key.clone(),
                })?;

            if timeline_record.route.route_version != request.expected_route_version {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::RouteVersionMismatch {
                    expected: request.expected_route_version,
                    actual: timeline_record.route.route_version,
                });
            }
            if timeline_record.route.epoch != request.expected_epoch {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::EpochMismatch {
                    expected: request.expected_epoch,
                    actual: timeline_record.route.epoch,
                });
            }

            if !self.is_local_endpoint(&timeline_record.route.owner_worker_endpoint) {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::NotTimelineOwner {
                    owner_worker_endpoint: timeline_record.route.owner_worker_endpoint,
                });
            }
            self.validate_batch_for_route(request.count, &timeline_record.route)?;

            let (timeline_record, _revision) = match self
                .activate_local_timeline_record(&request.timeline_key, timeline_record, revision)
                .await
            {
                Ok(value) => value,
                Err(TsoError::GeneratorLeaseExpired { .. }) => {
                    return Err(TsoError::LeaseExpired {
                        timeline_key: request.timeline_key.clone(),
                    });
                }
                Err(error) => return Err(error),
            };

            if !Self::timeline_is_ready(timeline_record.state) {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::TimelineNotReady {
                    timeline_key: request.timeline_key.clone(),
                    state: timeline_record.state,
                });
            }

            let issued_upper_bound = self
                .ensure_generator_lease_for_allocation_with_cancellation(
                    &request.timeline_key,
                    timeline_record.route.generator_id,
                    cancellation.clone(),
                )
                .await?;
            let (timeline_record, revision) = self
                .load_timeline_with_singleflight(&request.timeline_key)
                .await?
                .ok_or_else(|| TsoError::TimelineNotFound {
                    timeline_key: request.timeline_key.clone(),
                })?;
            if timeline_record.route.route_version != request.expected_route_version {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::RouteVersionMismatch {
                    expected: request.expected_route_version,
                    actual: timeline_record.route.route_version,
                });
            }
            if timeline_record.route.epoch != request.expected_epoch {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::EpochMismatch {
                    expected: request.expected_epoch,
                    actual: timeline_record.route.epoch,
                });
            }
            if !self.is_local_endpoint(&timeline_record.route.owner_worker_endpoint) {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::NotTimelineOwner {
                    owner_worker_endpoint: timeline_record.route.owner_worker_endpoint,
                });
            }
            if !Self::timeline_is_ready(timeline_record.state) {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::TimelineNotReady {
                    timeline_key: request.timeline_key.clone(),
                    state: timeline_record.state,
                });
            }
            let timeline_state_handle = self
                .timeline_state_handle_from_record(
                    &request.timeline_key,
                    &timeline_record,
                    revision,
                )
                .await?;
            let generator_id = timeline_record.route.generator_id;
            if let Some(response) = self
                .try_serve_timeline_state_handle_with_guard(
                    &request,
                    timeline_state_handle,
                    generator_id,
                    self.clock.now_ms(),
                    issued_upper_bound,
                    cancellation.clone(),
                )
                .await?
            {
                return Ok(response);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::sync::{broadcast, oneshot};
    use tokio::time::{timeout, Duration};

    use crate::metadata::{
        ControlPlaneStore, GeneratorBatchOp, GeneratorLeaseAuthority, GeneratorRecord,
        MemoryMetadataStore, RouteUpdateSource, TimelineAuthority, TimelineBatchOp, TimelineRecord,
    };
    use crate::{
        AllocateTimestampsRequest, Clock, ManualClock, ResourceTier, TimelineLifecycleState,
        TsoConfig, TsoError, TsoSecurityMode, TsoService,
    };

    #[derive(Clone)]
    struct BlockingGeneratorLoadStore {
        inner: Arc<MemoryMetadataStore>,
        block_on_load: Arc<AtomicBool>,
        load_started_tx: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
        release_load_rx: Arc<std::sync::Mutex<Option<oneshot::Receiver<()>>>>,
    }

    impl BlockingGeneratorLoadStore {
        fn new(
            inner: Arc<MemoryMetadataStore>,
            load_started_tx: oneshot::Sender<()>,
            release_load_rx: oneshot::Receiver<()>,
        ) -> Self {
            Self {
                inner,
                block_on_load: Arc::new(AtomicBool::new(false)),
                load_started_tx: Arc::new(std::sync::Mutex::new(Some(load_started_tx))),
                release_load_rx: Arc::new(std::sync::Mutex::new(Some(release_load_rx))),
            }
        }

        fn arm_blocking_load(&self) {
            self.block_on_load.store(true, Ordering::Release);
        }
    }

    #[async_trait]
    impl TimelineAuthority for BlockingGeneratorLoadStore {
        async fn load_timeline(
            &self,
            timeline_key: &str,
        ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
            self.inner.load_timeline(timeline_key).await
        }

        async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
            self.inner.list_timelines().await
        }

        async fn create_timeline(
            &self,
            timeline_key: &str,
            record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            self.inner.create_timeline(timeline_key, record).await
        }

        async fn compare_exchange_timeline(
            &self,
            timeline_key: &str,
            expected_revision: u64,
            record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            self.inner
                .compare_exchange_timeline(timeline_key, expected_revision, record)
                .await
        }

        async fn compare_exchange_timelines(
            &self,
            operations: &[TimelineBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            self.inner.compare_exchange_timelines(operations).await
        }
    }

    #[async_trait]
    impl GeneratorLeaseAuthority for BlockingGeneratorLoadStore {
        async fn load_generator(
            &self,
            generator_id: u32,
        ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
            if self.block_on_load.swap(false, Ordering::AcqRel) {
                let started_tx = self.load_started_tx.lock().unwrap().take();
                if let Some(started_tx) = started_tx {
                    let _ = started_tx.send(());
                }
                let release_load_rx = self.release_load_rx.lock().unwrap().take();
                if let Some(release_load_rx) = release_load_rx {
                    let _ = release_load_rx.await;
                }
            }
            self.inner.load_generator(generator_id).await
        }

        async fn create_generator(
            &self,
            generator_id: u32,
            record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            self.inner.create_generator(generator_id, record).await
        }

        async fn compare_exchange_generator(
            &self,
            generator_id: u32,
            expected_revision: u64,
            record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            self.inner
                .compare_exchange_generator(generator_id, expected_revision, record)
                .await
        }

        async fn compare_exchange_generators(
            &self,
            operations: &[GeneratorBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            self.inner.compare_exchange_generators(operations).await
        }
    }

    impl RouteUpdateSource for BlockingGeneratorLoadStore {
        fn subscribe_route_updates(
            &self,
        ) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
            self.inner.subscribe_route_updates()
        }
    }

    #[async_trait]
    impl ControlPlaneStore for BlockingGeneratorLoadStore {}

    #[derive(Clone)]
    struct BlockingTimelineLoadStore {
        inner: Arc<MemoryMetadataStore>,
        block_on_load: Arc<AtomicBool>,
        active_loads: Arc<AtomicUsize>,
        max_parallel_loads: Arc<AtomicUsize>,
        load_started_tx: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
        release_load_rx: Arc<std::sync::Mutex<Option<oneshot::Receiver<()>>>>,
    }

    impl BlockingTimelineLoadStore {
        fn new(
            inner: Arc<MemoryMetadataStore>,
            load_started_tx: oneshot::Sender<()>,
            release_load_rx: oneshot::Receiver<()>,
        ) -> Self {
            Self {
                inner,
                block_on_load: Arc::new(AtomicBool::new(false)),
                active_loads: Arc::new(AtomicUsize::new(0)),
                max_parallel_loads: Arc::new(AtomicUsize::new(0)),
                load_started_tx: Arc::new(std::sync::Mutex::new(Some(load_started_tx))),
                release_load_rx: Arc::new(std::sync::Mutex::new(Some(release_load_rx))),
            }
        }

        fn active_loads(&self) -> usize {
            self.active_loads.load(Ordering::Acquire)
        }

        fn max_parallel_loads(&self) -> usize {
            self.max_parallel_loads.load(Ordering::Acquire)
        }

        fn arm_blocking_load(&self) {
            self.block_on_load.store(true, Ordering::Release);
        }
    }

    #[async_trait]
    impl TimelineAuthority for BlockingTimelineLoadStore {
        async fn load_timeline(
            &self,
            timeline_key: &str,
        ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
            let active_loads = self.active_loads.fetch_add(1, Ordering::AcqRel) + 1;
            self.max_parallel_loads
                .fetch_max(active_loads, Ordering::AcqRel);

            if self.block_on_load.swap(false, Ordering::AcqRel) {
                let started_tx = self.load_started_tx.lock().unwrap().take();
                if let Some(started_tx) = started_tx {
                    let _ = started_tx.send(());
                }
                let release_load_rx = self.release_load_rx.lock().unwrap().take();
                if let Some(release_load_rx) = release_load_rx {
                    let _ = release_load_rx.await;
                }
            }

            let result = self.inner.load_timeline(timeline_key).await;
            self.active_loads.fetch_sub(1, Ordering::AcqRel);
            result
        }

        async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
            self.inner.list_timelines().await
        }

        async fn create_timeline(
            &self,
            timeline_key: &str,
            record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            self.inner.create_timeline(timeline_key, record).await
        }

        async fn compare_exchange_timeline(
            &self,
            timeline_key: &str,
            expected_revision: u64,
            record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            self.inner
                .compare_exchange_timeline(timeline_key, expected_revision, record)
                .await
        }

        async fn compare_exchange_timelines(
            &self,
            operations: &[TimelineBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            self.inner.compare_exchange_timelines(operations).await
        }
    }

    #[async_trait]
    impl GeneratorLeaseAuthority for BlockingTimelineLoadStore {
        async fn load_generator(
            &self,
            generator_id: u32,
        ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
            self.inner.load_generator(generator_id).await
        }

        async fn create_generator(
            &self,
            generator_id: u32,
            record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            self.inner.create_generator(generator_id, record).await
        }

        async fn compare_exchange_generator(
            &self,
            generator_id: u32,
            expected_revision: u64,
            record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            self.inner
                .compare_exchange_generator(generator_id, expected_revision, record)
                .await
        }

        async fn compare_exchange_generators(
            &self,
            operations: &[GeneratorBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            self.inner.compare_exchange_generators(operations).await
        }
    }

    impl RouteUpdateSource for BlockingTimelineLoadStore {
        fn subscribe_route_updates(
            &self,
        ) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
            self.inner.subscribe_route_updates()
        }
    }

    #[async_trait]
    impl ControlPlaneStore for BlockingTimelineLoadStore {}

    #[derive(Clone)]
    struct SilentRouteUpdateStore {
        inner: Arc<MemoryMetadataStore>,
        route_updates: broadcast::Sender<crate::metadata::RouteUpdateSignal>,
    }

    impl SilentRouteUpdateStore {
        fn new(inner: Arc<MemoryMetadataStore>) -> Self {
            let (route_updates, _) = broadcast::channel(16);
            Self {
                inner,
                route_updates,
            }
        }
    }

    #[async_trait]
    impl TimelineAuthority for SilentRouteUpdateStore {
        async fn load_timeline(
            &self,
            timeline_key: &str,
        ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
            self.inner.load_timeline(timeline_key).await
        }

        async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
            self.inner.list_timelines().await
        }

        async fn create_timeline(
            &self,
            timeline_key: &str,
            record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            self.inner.create_timeline(timeline_key, record).await
        }

        async fn compare_exchange_timeline(
            &self,
            timeline_key: &str,
            expected_revision: u64,
            record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            self.inner
                .compare_exchange_timeline(timeline_key, expected_revision, record)
                .await
        }

        async fn compare_exchange_timelines(
            &self,
            operations: &[TimelineBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            self.inner.compare_exchange_timelines(operations).await
        }
    }

    #[async_trait]
    impl GeneratorLeaseAuthority for SilentRouteUpdateStore {
        async fn load_generator(
            &self,
            generator_id: u32,
        ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
            self.inner.load_generator(generator_id).await
        }

        async fn create_generator(
            &self,
            generator_id: u32,
            record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            self.inner.create_generator(generator_id, record).await
        }

        async fn compare_exchange_generator(
            &self,
            generator_id: u32,
            expected_revision: u64,
            record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            self.inner
                .compare_exchange_generator(generator_id, expected_revision, record)
                .await
        }

        async fn compare_exchange_generators(
            &self,
            operations: &[GeneratorBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            self.inner.compare_exchange_generators(operations).await
        }
    }

    impl RouteUpdateSource for SilentRouteUpdateStore {
        fn subscribe_route_updates(
            &self,
        ) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
            self.route_updates.subscribe()
        }
    }

    #[async_trait]
    impl ControlPlaneStore for SilentRouteUpdateStore {}

    fn required_test_config(config: TsoConfig) -> TsoConfig {
        TsoConfig {
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some("server.crt".into()),
            grpc_tls_key_file: Some("server.key".into()),
            grpc_client_ca_file: Some("ca.pem".into()),
            grpc_request_timeout_ms: Some(100),
            grpc_max_request_bytes: Some(1024),
            grpc_max_concurrent_requests: Some(16),
            ..config
        }
    }

    fn with_worker(mut config: TsoConfig, worker_id: &str) -> TsoConfig {
        config.worker_id = worker_id.to_owned();
        config.advertise_endpoint = format!("{worker_id}:50051");
        required_test_config(config)
    }

    #[tokio::test]
    async fn allocation_recovering_local_timeline_activates_and_upserts_cache() {
        let clock = Arc::new(ManualClock::new(22_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();

        let route = service
            .ensure_timeline("allocation.recovering.timeline")
            .await
            .unwrap();
        let (mut record, revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        record.state = TimelineLifecycleState::Recovering;
        let recovering_revision = metadata
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();
        service.clear_timeline_cache(&route.timeline_key);

        let response = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "recovering-local-allocate".to_string(),
            })
            .await
            .unwrap();
        let allocated_tso = response.ranges.last().unwrap().end_tso;

        let (persisted, persisted_revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.state, TimelineLifecycleState::Active);
        assert!(persisted_revision > recovering_revision);

        let cache_handle = service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .expect("allocation should repopulate timeline cache");
        let cached = cache_handle.lock().await;
        assert_eq!(cached.state, TimelineLifecycleState::Active);
        assert_eq!(cached.route, persisted.route);
        assert_eq!(cached.revision, persisted_revision);
        assert_eq!(cached.last_issued_tso, Some(allocated_tso));
    }

    #[tokio::test]
    async fn cold_cache_ensure_timeline_serializes_inflight_timeline_loads() {
        let clock = Arc::new(ManualClock::new(22_500));
        let inner = Arc::new(MemoryMetadataStore::new());
        let route = crate::TimelineRoute {
            timeline_key: "allocation.timeline-load.singleflight".into(),
            generator_id: 7,
            owner_worker_endpoint: "127.0.0.1:59999".into(),
            epoch: 1,
            route_version: 1,
            resource_tier: crate::ResourceTier::Shared,
        };
        inner
            .create_timeline(
                &route.timeline_key,
                &TimelineRecord {
                    route: route.clone(),
                    state: TimelineLifecycleState::Active,
                    recovery_floor_tso: None,
                    issued_upper_bound: None,
                    last_graceful_issued: None,
                    lease_expire_at_ms: None,
                    updated_at_ms: clock.now_ms(),
                },
            )
            .await
            .unwrap();

        let (load_started_tx, load_started_rx) = oneshot::channel();
        let (release_load_tx, release_load_rx) = oneshot::channel();
        let metadata = Arc::new(BlockingTimelineLoadStore::new(
            inner,
            load_started_tx,
            release_load_rx,
        ));
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();

        metadata.arm_blocking_load();

        let first_service = service.clone();
        let first_key = route.timeline_key.clone();
        let first = tokio::spawn(async move { first_service.ensure_timeline(&first_key).await });

        load_started_rx
            .await
            .expect("the first cold-cache load should enter metadata");
        assert_eq!(metadata.active_loads(), 1);

        let second_service = service.clone();
        let second_key = route.timeline_key.clone();
        let second = tokio::spawn(async move { second_service.ensure_timeline(&second_key).await });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(metadata.active_loads(), 1);
        assert_eq!(metadata.max_parallel_loads(), 1);
        assert!(!second.is_finished());

        release_load_tx.send(()).unwrap();

        assert_eq!(first.await.unwrap().unwrap(), route);
        assert_eq!(second.await.unwrap().unwrap(), route);
        assert_eq!(metadata.max_parallel_loads(), 1);
    }

    #[tokio::test]
    async fn distinct_cold_cache_timeline_loads_respect_global_load_limit() {
        let clock = Arc::new(ManualClock::new(22_600));
        let inner = Arc::new(MemoryMetadataStore::new());
        let route_a = crate::TimelineRoute {
            timeline_key: "allocation.timeline-load.limit.a".into(),
            generator_id: 7,
            owner_worker_endpoint: "127.0.0.1:59999".into(),
            epoch: 1,
            route_version: 1,
            resource_tier: crate::ResourceTier::Shared,
        };
        let route_b = crate::TimelineRoute {
            timeline_key: "allocation.timeline-load.limit.b".into(),
            generator_id: 8,
            owner_worker_endpoint: "127.0.0.1:59999".into(),
            epoch: 1,
            route_version: 1,
            resource_tier: crate::ResourceTier::Shared,
        };
        for route in [&route_a, &route_b] {
            inner
                .create_timeline(
                    &route.timeline_key,
                    &TimelineRecord {
                        route: route.clone(),
                        state: TimelineLifecycleState::Active,
                        recovery_floor_tso: None,
                        issued_upper_bound: None,
                        last_graceful_issued: None,
                        lease_expire_at_ms: None,
                        updated_at_ms: clock.now_ms(),
                    },
                )
                .await
                .unwrap();
        }

        let (load_started_tx, load_started_rx) = oneshot::channel();
        let (release_load_tx, release_load_rx) = oneshot::channel();
        let metadata = Arc::new(BlockingTimelineLoadStore::new(
            inner,
            load_started_tx,
            release_load_rx,
        ));
        let service = TsoService::new(
            required_test_config(TsoConfig {
                max_concurrent_timeline_loads: 1,
                ..TsoConfig::default()
            }),
            clock,
            metadata.clone(),
        )
        .unwrap();

        metadata.arm_blocking_load();

        let first_service = service.clone();
        let first_key = route_a.timeline_key.clone();
        let first = tokio::spawn(async move { first_service.ensure_timeline(&first_key).await });

        load_started_rx
            .await
            .expect("the first cold-cache load should enter metadata");
        assert_eq!(metadata.active_loads(), 1);

        let second_service = service.clone();
        let second_key = route_b.timeline_key.clone();
        let second = tokio::spawn(async move { second_service.ensure_timeline(&second_key).await });

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(metadata.active_loads(), 1);
        assert_eq!(metadata.max_parallel_loads(), 1);
        assert!(!second.is_finished());

        release_load_tx.send(()).unwrap();

        assert_eq!(first.await.unwrap().unwrap(), route_a);
        assert_eq!(second.await.unwrap().unwrap(), route_b);
        assert_eq!(metadata.max_parallel_loads(), 1);
    }

    #[tokio::test]
    async fn cached_allocation_revalidates_authoritative_route_without_route_updates() {
        let clock = Arc::new(ManualClock::new(30_000));
        let inner = Arc::new(MemoryMetadataStore::new());
        let metadata = Arc::new(SilentRouteUpdateStore::new(inner.clone()));
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock.clone(),
            metadata,
        )
        .unwrap();

        let route = service
            .ensure_timeline("allocation.stale-route.timeline")
            .await
            .unwrap();
        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "seed-stale-route".to_string(),
            })
            .await
            .unwrap();

        let (mut record, revision) = inner
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        record.route.route_version += 1;
        let actual_route_version = record.route.route_version;
        record.updated_at_ms = clock.now_ms();
        inner
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();

        let error = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "stale-route-request".to_string(),
            })
            .await
            .unwrap_err();

        assert_eq!(
            error,
            TsoError::RouteVersionMismatch {
                expected: route.route_version,
                actual: actual_route_version,
            }
        );
    }

    #[tokio::test]
    async fn cached_allocation_revalidates_authoritative_state_without_route_updates() {
        let clock = Arc::new(ManualClock::new(31_000));
        let inner = Arc::new(MemoryMetadataStore::new());
        let metadata = Arc::new(SilentRouteUpdateStore::new(inner.clone()));
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock.clone(),
            metadata,
        )
        .unwrap();

        let route = service
            .ensure_timeline("allocation.stale-state.timeline")
            .await
            .unwrap();
        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "seed-stale-state".to_string(),
            })
            .await
            .unwrap();

        let (mut record, revision) = inner
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        record.state = TimelineLifecycleState::Draining;
        record.updated_at_ms = clock.now_ms();
        inner
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();

        let error = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "stale-state-request".to_string(),
            })
            .await
            .unwrap_err();

        assert_eq!(
            error,
            TsoError::TimelineNotReady {
                timeline_key: route.timeline_key,
                state: TimelineLifecycleState::Draining,
            }
        );
    }

    #[tokio::test]
    async fn allocation_refreshes_now_ms_before_lease_validation_after_timeline_load() {
        let clock = Arc::new(ManualClock::new(40_000));
        let inner = Arc::new(MemoryMetadataStore::new());
        let (load_started_tx, load_started_rx) = oneshot::channel();
        let (release_load_tx, release_load_rx) = oneshot::channel();
        let metadata = Arc::new(BlockingTimelineLoadStore::new(
            inner.clone(),
            load_started_tx,
            release_load_rx,
        ));
        let service = TsoService::new(
            required_test_config(TsoConfig {
                generator_lease_ttl_ms: 250,
                lease_ttl_ms: 250,
                ..TsoConfig::default()
            }),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();

        let route = service
            .ensure_timeline("allocation.refresh-now.timeline")
            .await
            .unwrap();
        let lease_expire_at_ms = service
            .generator_runtime
            .lease_state(route.generator_id)
            .expect("generator lease should exist")
            .lease_expire_at_ms;

        service.clear_timeline_cache(&route.timeline_key);
        clock.set(lease_expire_at_ms.saturating_sub(1));
        metadata.arm_blocking_load();

        let service_clone = service.clone();
        let route_clone = route.clone();
        let allocate_task = tokio::spawn(async move {
            service_clone
                .allocate_timestamps(AllocateTimestampsRequest {
                    timeline_key: route_clone.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route_clone.epoch,
                    expected_route_version: route_clone.route_version,
                    client_request_id: "refresh-now-after-load".to_string(),
                })
                .await
        });

        load_started_rx
            .await
            .expect("timeline load should start before lease validation");
        clock.set(lease_expire_at_ms + 1);
        release_load_tx.send(()).unwrap();

        let error = allocate_task.await.unwrap().unwrap_err();
        assert_eq!(
            error,
            TsoError::LeaseExpired {
                timeline_key: route.timeline_key,
            }
        );
    }

    #[tokio::test]
    async fn allocation_rechecks_generator_lease_time_after_slow_generator_load() {
        let clock = Arc::new(ManualClock::new(41_000));
        let inner = Arc::new(MemoryMetadataStore::new());
        let (load_started_tx, load_started_rx) = oneshot::channel();
        let (release_load_tx, release_load_rx) = oneshot::channel();
        let metadata = Arc::new(BlockingGeneratorLoadStore::new(
            inner.clone(),
            load_started_tx,
            release_load_rx,
        ));
        let service = TsoService::new(
            required_test_config(TsoConfig {
                generator_lease_ttl_ms: 250,
                lease_ttl_ms: 250,
                ..TsoConfig::default()
            }),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();

        let route = service
            .ensure_timeline("allocation.refresh-generator-load.timeline")
            .await
            .unwrap();
        let lease_expire_at_ms = service
            .generator_runtime
            .lease_state(route.generator_id)
            .expect("generator lease should exist")
            .lease_expire_at_ms;

        service.clear_timeline_cache(&route.timeline_key);
        service.generator_runtime.remove_lease(route.generator_id);
        clock.set(lease_expire_at_ms.saturating_sub(1));
        metadata.arm_blocking_load();

        let service_clone = service.clone();
        let route_clone = route.clone();
        let allocate_task = tokio::spawn(async move {
            service_clone
                .allocate_timestamps(AllocateTimestampsRequest {
                    timeline_key: route_clone.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route_clone.epoch,
                    expected_route_version: route_clone.route_version,
                    client_request_id: "refresh-after-generator-load".to_string(),
                })
                .await
        });

        load_started_rx
            .await
            .expect("generator load should start before lease validation returns");
        clock.set(lease_expire_at_ms + 1);
        release_load_tx.send(()).unwrap();

        let error = allocate_task.await.unwrap().unwrap_err();
        assert_eq!(
            error,
            TsoError::LeaseExpired {
                timeline_key: route.timeline_key,
            }
        );
    }

    #[tokio::test]
    async fn allocation_does_not_hold_cached_timeline_lock_while_waiting_for_generator_load() {
        let clock = Arc::new(ManualClock::new(23_000));
        let inner = Arc::new(MemoryMetadataStore::new());
        let (load_started_tx, load_started_rx) = oneshot::channel();
        let (release_load_tx, release_load_rx) = oneshot::channel();
        let metadata = Arc::new(BlockingGeneratorLoadStore::new(
            inner,
            load_started_tx,
            release_load_rx,
        ));
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();

        let route = service
            .ensure_timeline("allocation.lock.scope.timeline")
            .await
            .unwrap();
        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        service.generator_runtime.remove_lease(route.generator_id);
        metadata.arm_blocking_load();

        let timeline_handle = service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .expect("timeline should be cached after ensure");

        let allocate_service = service.clone();
        let allocate_route = route.clone();
        let allocate_task = tokio::spawn(async move {
            allocate_service
                .allocate_timestamps(AllocateTimestampsRequest {
                    timeline_key: allocate_route.timeline_key,
                    count: 1,
                    expected_epoch: allocate_route.epoch,
                    expected_route_version: allocate_route.route_version,
                    client_request_id: "allocation-lock-scope".to_string(),
                })
                .await
        });

        load_started_rx
            .await
            .expect("allocation should block in load_generator");

        let timeline_guard = timeout(Duration::from_millis(50), timeline_handle.lock())
            .await
            .expect("cached timeline lock should be released while generator load waits");
        drop(timeline_guard);

        release_load_tx.send(()).unwrap();
        let response = allocate_task.await.unwrap().unwrap();
        assert_eq!(response.timeline_key, route.timeline_key);
    }

    #[tokio::test]
    async fn allocation_degrades_to_transient_timeline_state_when_runtime_cache_is_saturated() {
        let clock = Arc::new(ManualClock::new(24_500));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig {
                max_timeline_runtime_entries: 1,
                ..TsoConfig::default()
            }),
            clock,
            metadata.clone(),
        )
        .unwrap();

        let route_a = service
            .ensure_timeline("allocation.degrade.a")
            .await
            .unwrap();
        let busy_handle = service
            .timeline_runtime
            .timeline_handle(&route_a.timeline_key)
            .expect("seeded timeline should be cached");

        let route_b = crate::TimelineRoute {
            timeline_key: "allocation.degrade.b".into(),
            generator_id: route_a.generator_id,
            owner_worker_endpoint: service.advertise_endpoint().to_string(),
            epoch: 1,
            route_version: 1,
            resource_tier: crate::ResourceTier::Shared,
        };
        let record_b = TimelineRecord {
            route: route_b.clone(),
            state: TimelineLifecycleState::Active,
            recovery_floor_tso: None,
            issued_upper_bound: None,
            last_graceful_issued: None,
            lease_expire_at_ms: None,
            updated_at_ms: service.clock.now_ms(),
        };
        metadata
            .create_timeline(&route_b.timeline_key, &record_b)
            .await
            .unwrap();

        let degraded_before = crate::metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
            .with_label_values(&["degraded_uncached"])
            .get();

        let response = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_b.timeline_key.clone(),
                count: 1,
                expected_epoch: route_b.epoch,
                expected_route_version: route_b.route_version,
                client_request_id: "allocation-degrade-b".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(response.timeline_key, route_b.timeline_key);
        assert_eq!(service.timeline_runtime.timeline_count(), 1);
        assert!(service
            .timeline_runtime
            .timeline_handle(&route_b.timeline_key)
            .is_none());
        assert!(
            crate::metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                .with_label_values(&["degraded_uncached"])
                .get()
                > degraded_before
        );

        drop(busy_handle);
    }

    #[tokio::test]
    async fn allocation_clock_backwards_remains_terminal_error() {
        let clock = Arc::new(ManualClock::new(1_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig {
                max_clock_rewind_ms: 10,
                max_future_borrow_ms: 0,
                ..TsoConfig::default()
            }),
            clock.clone(),
            metadata,
        )
        .unwrap();

        let route = service
            .ensure_timeline("allocation.clock-terminal.timeline")
            .await
            .unwrap();
        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "clock-terminal-seed".to_string(),
            })
            .await
            .unwrap();

        clock.set(975);

        let error = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key,
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "clock-terminal-second".to_string(),
            })
            .await
            .expect_err("allocation should still fail immediately on large clock rewind");

        assert_eq!(error, TsoError::ClockBackwards { delta_ms: 25 });
    }

    #[tokio::test]
    async fn shared_tier_rejects_batches_above_single_ms_capacity() {
        let service = TsoService::new(
            with_worker(
                TsoConfig {
                    max_batch_per_request: crate::SEQUENCE_CAPACITY + 32,
                    ..TsoConfig::default()
                },
                "worker-a",
            ),
            Arc::new(ManualClock::new(50_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();

        let route = service
            .ensure_timeline("shared.noisy-neighbor")
            .await
            .unwrap();
        assert_eq!(route.resource_tier, ResourceTier::Shared);

        let error = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: crate::SEQUENCE_CAPACITY + 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "shared-batch-cap".into(),
            })
            .await
            .unwrap_err();

        assert_eq!(
            error,
            TsoError::BatchTooLarge {
                requested: crate::SEQUENCE_CAPACITY + 1,
                max: crate::SEQUENCE_CAPACITY,
            }
        );
    }

    #[tokio::test]
    async fn dedicated_tier_keeps_global_batch_limit() {
        let global_limit = crate::SEQUENCE_CAPACITY + 32;
        let service = TsoService::new(
            with_worker(
                TsoConfig {
                    default_resource_tier: ResourceTier::Dedicated,
                    max_batch_per_request: global_limit,
                    max_future_borrow_ms: 2_000,
                    ..TsoConfig::default()
                },
                "worker-a",
            ),
            Arc::new(ManualClock::new(51_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();

        let route = service
            .ensure_timeline("dedicated.batch-limit")
            .await
            .unwrap();
        assert_eq!(route.resource_tier, ResourceTier::Dedicated);

        let response = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: global_limit,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "dedicated-global-cap".into(),
            })
            .await
            .unwrap();

        assert!(!response.ranges.is_empty());
    }

    #[tokio::test]
    async fn ensure_timeline_create_degrades_when_runtime_cache_is_saturated() {
        let clock = Arc::new(ManualClock::new(2_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig {
                max_timeline_runtime_entries: 1,
                ..TsoConfig::default()
            }),
            clock,
            metadata,
        )
        .unwrap();

        let route_a = service.ensure_timeline("ensure.degrade.a").await.unwrap();
        let busy_handle = service
            .timeline_runtime
            .timeline_handle(&route_a.timeline_key)
            .expect("seeded timeline should be cached");

        let degraded_before = crate::metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
            .with_label_values(&["degraded_uncached"])
            .get();

        let route_b = service.ensure_timeline("ensure.degrade.b").await.unwrap();

        assert_eq!(route_b.timeline_key, "ensure.degrade.b");
        assert_eq!(service.timeline_runtime.timeline_count(), 1);
        assert!(service
            .timeline_runtime
            .timeline_handle(&route_b.timeline_key)
            .is_none());
        assert!(
            crate::metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                .with_label_values(&["degraded_uncached"])
                .get()
                > degraded_before
        );

        drop(busy_handle);
    }

    #[tokio::test]
    async fn metadata_path_rechecks_post_handle_generator_before_serving() {
        let clock = Arc::new(ManualClock::new(2_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();

        let route = service
            .ensure_timeline("allocation.post-handle.guard.timeline")
            .await
            .unwrap();
        service.clear_timeline_cache(&route.timeline_key);

        let (record, revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        let timeline_state_handle = service
            .timeline_state_handle_from_record(&route.timeline_key, &record, revision)
            .await
            .unwrap();

        {
            let mut timeline_state = timeline_state_handle.lock().await;
            timeline_state.route.generator_id = route.generator_id + 1;
        }

        let response = service
            .try_serve_timeline_state_handle_with_guard(
                &AllocateTimestampsRequest {
                    timeline_key: route.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route.epoch,
                    expected_route_version: route.route_version,
                    client_request_id: "post-handle-guard".to_string(),
                },
                timeline_state_handle,
                route.generator_id,
                clock.now_ms(),
                None,
                None,
            )
            .await
            .unwrap();

        assert!(
            response.is_none(),
            "post-handle guard should force a retry when generator changes before serve"
        );
    }
}
