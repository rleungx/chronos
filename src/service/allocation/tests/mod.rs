use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{timeout, Duration};

use super::serve::{AllocationPath, CachedServeGuardOptions};
use crate::metadata::{
    AllocationRequestFingerprint, ControlPlaneStore, GeneratorBatchOp, GeneratorLeaseAuthority,
    GeneratorRecord, MemoryMetadataStore, RequestRecord, RequestRecordAuthority,
    RequestRecordState, RouteUpdateSource, TimelineAuthority, TimelineBatchOp, TimelineRecord,
};
use crate::plane::RequestCancellation;
use crate::{
    AllocateTimestampsRequest, Clock, ManualClock, ResourceTier, TimelineLifecycleState, TsoConfig,
    TsoError, TsoService, MAX_GENERATORS,
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
    fn subscribe_route_updates(&self) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
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
    fn subscribe_route_updates(&self) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
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
    fn subscribe_route_updates(&self) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
        self.route_updates.subscribe()
    }
}

#[async_trait]
impl ControlPlaneStore for SilentRouteUpdateStore {}

#[derive(Clone)]
struct RequestRecordCasFailureStore {
    inner: Arc<MemoryMetadataStore>,
    fail_next_request_cas: Arc<AtomicBool>,
    complete_next_request_cas_then_report_cas_failed: Arc<AtomicBool>,
}

impl RequestRecordCasFailureStore {
    fn new(inner: Arc<MemoryMetadataStore>) -> Self {
        Self {
            inner,
            fail_next_request_cas: Arc::new(AtomicBool::new(false)),
            complete_next_request_cas_then_report_cas_failed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn fail_next_request_cas(&self) {
        self.fail_next_request_cas.store(true, Ordering::Release);
    }

    fn complete_next_request_cas_then_report_cas_failed(&self) {
        self.complete_next_request_cas_then_report_cas_failed
            .store(true, Ordering::Release);
    }
}

#[async_trait]
impl TimelineAuthority for RequestRecordCasFailureStore {
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
impl GeneratorLeaseAuthority for RequestRecordCasFailureStore {
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

#[async_trait]
impl RequestRecordAuthority for RequestRecordCasFailureStore {
    async fn load_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
    ) -> Result<Option<(RequestRecord, u64)>, TsoError> {
        self.inner
            .load_request_record(timeline_key, client_request_id)
            .await
    }

    async fn create_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> Result<u64, TsoError> {
        self.inner
            .create_request_record(timeline_key, client_request_id, record)
            .await
    }

    async fn compare_exchange_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        expected_revision: u64,
        record: &RequestRecord,
    ) -> Result<u64, TsoError> {
        if self.fail_next_request_cas.swap(false, Ordering::AcqRel) {
            return Err(TsoError::Internal(
                "injected request record CAS failure".to_string(),
            ));
        }
        if self
            .complete_next_request_cas_then_report_cas_failed
            .swap(false, Ordering::AcqRel)
        {
            self.inner
                .compare_exchange_request_record(
                    timeline_key,
                    client_request_id,
                    expected_revision,
                    record,
                )
                .await?;
            return Err(TsoError::CasFailed);
        }
        self.inner
            .compare_exchange_request_record(
                timeline_key,
                client_request_id,
                expected_revision,
                record,
            )
            .await
    }

    async fn compare_delete_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        expected_revision: u64,
    ) -> Result<(), TsoError> {
        self.inner
            .compare_delete_request_record(timeline_key, client_request_id, expected_revision)
            .await
    }

    async fn prune_completed_request_records(
        &self,
        older_than_ms: u64,
        limit: usize,
    ) -> Result<usize, TsoError> {
        self.inner
            .prune_completed_request_records(older_than_ms, limit)
            .await
    }
}

impl RouteUpdateSource for RequestRecordCasFailureStore {
    fn subscribe_route_updates(&self) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
        self.inner.subscribe_route_updates()
    }
}

#[async_trait]
impl ControlPlaneStore for RequestRecordCasFailureStore {
    fn request_records(&self) -> Option<&dyn RequestRecordAuthority> {
        Some(self)
    }
}

fn required_test_config(config: TsoConfig) -> TsoConfig {
    crate::test_tls::required_grpc_tls_test_config(config, 100)
}

fn with_worker(mut config: TsoConfig, worker_id: &str) -> TsoConfig {
    config.worker_id = worker_id.to_owned();
    config.advertise_endpoint = format!("{worker_id}:50051");
    required_test_config(config)
}

mod cache_and_revalidation;
mod idempotency;
mod quota_and_fairness;
mod runtime_cache;
