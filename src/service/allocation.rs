mod serve;

use std::sync::Arc;

use self::serve::{AllocationPath, CachedServeGuardOptions};
use crate::plane::RequestCancellation;
use std::time::Instant;
use tokio::sync::Mutex;

use crate::metadata::{
    AllocationRequestFingerprint, AllocationResponseRecord, RequestRecord, RequestRecordState,
    TimelineRecord,
};
use crate::recovery::record_recovery_event;
use crate::timeline_state::build_timeline_state;
use crate::{
    metrics, AllocateTimestampsRequest, AllocateTimestampsResponse, ResourceTier,
    TimelineLifecycleState, TimelineRoute, TsoError,
};

use super::TsoService;

struct PendingIdempotentAllocation {
    revision: u64,
    fingerprint: AllocationRequestFingerprint,
}

enum IdempotentAllocationPreparation {
    Disabled,
    Replay(AllocateTimestampsResponse),
    Pending(PendingIdempotentAllocation),
}

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

    fn effective_future_borrow_ms_for_timeline_state(
        &self,
        timeline_state: &crate::runtime::TimelineState,
        now_ms: u64,
    ) -> u64 {
        let base = self.effective_future_borrow_ms_for_route(&timeline_state.route);
        let Some(recovery_floor_tso) = timeline_state.recovery_floor_tso else {
            return base;
        };
        let recovery_physical_ms = crate::decode_tso(recovery_floor_tso).physical_ms;
        if recovery_physical_ms <= now_ms {
            return base;
        }

        let catchup_gap_ms = recovery_physical_ms - now_ms;
        base.max(catchup_gap_ms.min(self.config.recovery_catchup_budget_ms))
    }

    fn timeline_quota_capacity_for_route(&self, route: &TimelineRoute) -> Option<f64> {
        match route.resource_tier {
            ResourceTier::Shared => Some(
                self.config
                    .max_batch_per_request
                    .min(crate::SEQUENCE_CAPACITY) as f64,
            ),
            ResourceTier::Warm => Some(self.config.max_batch_per_request as f64),
            ResourceTier::Dedicated => None,
        }
    }

    fn timeline_quota_wait_ms_for_route(&self, route: &TimelineRoute, deficit: f64) -> u64 {
        let window_ms = self.effective_future_borrow_ms_for_route(route).max(1) as f64;
        let capacity = self
            .timeline_quota_capacity_for_route(route)
            .unwrap_or(0.0)
            .max(1.0);
        let refill_per_ms = capacity / window_ms;
        (deficit / refill_per_ms).ceil().max(1.0) as u64
    }

    fn charge_timeline_quota(
        &self,
        timeline_state: &mut crate::runtime::TimelineState,
        count: u32,
        now_ms: u64,
    ) -> Result<Option<f64>, TsoError> {
        let Some(capacity) = self.timeline_quota_capacity_for_route(&timeline_state.route) else {
            return Ok(None);
        };

        let window_ms = self
            .effective_future_borrow_ms_for_timeline_state(timeline_state, now_ms)
            .max(1) as f64;
        let refill_per_ms = capacity / window_ms;
        let elapsed_ms = timeline_state
            .timeline_quota_last_refill_ms
            .map(|last| now_ms.saturating_sub(last) as f64)
            .unwrap_or(0.0);
        let available = timeline_state
            .timeline_quota_tokens
            .unwrap_or(capacity)
            .min(capacity)
            + elapsed_ms * refill_per_ms;
        let available = available.min(capacity);
        let requested = count as f64;

        if requested > available {
            let wait_ms =
                self.timeline_quota_wait_ms_for_route(&timeline_state.route, requested - available);
            return Err(TsoError::FutureBorrowExceeded {
                requested_physical_ms: now_ms + wait_ms,
                allowed_physical_ms: now_ms,
            });
        }

        timeline_state.timeline_quota_tokens = Some((available - requested).max(0.0));
        timeline_state.timeline_quota_last_refill_ms = Some(now_ms);
        Ok(Some(requested))
    }

    fn refund_timeline_quota(
        &self,
        timeline_state: &mut crate::runtime::TimelineState,
        charged: Option<f64>,
    ) {
        let Some(charged) = charged else {
            return;
        };
        let Some(capacity) = self.timeline_quota_capacity_for_route(&timeline_state.route) else {
            return;
        };
        let available = timeline_state.timeline_quota_tokens.unwrap_or(capacity);
        timeline_state.timeline_quota_tokens = Some((available + charged).min(capacity));
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
            self.reject_new_work_if_shutting_down()?;
            let cached_timeline = self.timeline_runtime.timeline_handle(timeline_key);
            if let Some(timeline_handle) = cached_timeline {
                let timeline = timeline_handle.lock().await;
                if self.is_local_endpoint(&timeline.route.owner_worker_endpoint)
                    && Self::timeline_is_ready(timeline.state)
                {
                    return Ok(timeline.route.clone());
                }
            }

            match self
                .load_timeline_with_singleflight_and_cancellation(timeline_key, None)
                .await?
            {
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
                        schema_version: 1,
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

        let idempotency = self.prepare_idempotent_allocation(&request).await?;
        if let IdempotentAllocationPreparation::Replay(response) = idempotency {
            return Ok(response);
        }

        let response = self
            .allocate_timestamps_after_validation(request.clone(), cancellation)
            .await;
        self.finish_idempotent_allocation(&request, idempotency, response)
            .await
    }

    async fn allocate_timestamps_after_validation(
        &self,
        request: AllocateTimestampsRequest,
        cancellation: Option<RequestCancellation>,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        loop {
            self.reject_new_work_if_shutting_down()?;
            Self::check_request_cancellation(cancellation.as_ref())?;
            if let Some(response) = self
                .try_allocate_from_cached_timeline(&request, cancellation.clone())
                .await?
            {
                return Ok(response);
            }

            let metadata_load_started = Instant::now();
            let (timeline_record, revision) = self
                .load_timeline_with_singleflight_and_cancellation(
                    &request.timeline_key,
                    cancellation.clone(),
                )
                .await?
                .ok_or_else(|| TsoError::TimelineNotFound {
                    timeline_key: request.timeline_key.clone(),
                })?;
            Self::record_allocation_stage_latency(
                AllocationPath::Metadata,
                "metadata_load",
                metadata_load_started,
            );

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

            let activate_local_started = Instant::now();
            let (timeline_record, activated_revision) = match self
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
            Self::record_allocation_stage_latency(
                AllocationPath::Metadata,
                "activate_local",
                activate_local_started,
            );

            if !Self::timeline_is_ready(timeline_record.state) {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::TimelineNotReady {
                    timeline_key: request.timeline_key.clone(),
                    state: timeline_record.state,
                });
            }

            let lease_ensure_started = Instant::now();
            let issued_upper_bound = self
                .ensure_generator_lease_for_allocation_with_cancellation(
                    &request.timeline_key,
                    timeline_record.route.generator_id,
                    cancellation.clone(),
                )
                .await?;
            Self::record_allocation_stage_latency(
                AllocationPath::Metadata,
                "lease_ensure",
                lease_ensure_started,
            );
            let timeline_state_handle = self
                .timeline_state_handle_from_record(
                    &request.timeline_key,
                    &timeline_record,
                    activated_revision,
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
                    CachedServeGuardOptions {
                        cancellation: cancellation.clone(),
                        revalidate_authority: true,
                        path: AllocationPath::Metadata,
                    },
                )
                .await?
            {
                return Ok(response);
            }
        }
    }

    fn idempotency_enabled(request: &AllocateTimestampsRequest) -> bool {
        !request.client_request_id.trim().is_empty()
    }

    fn allocation_request_fingerprint(
        request: &AllocateTimestampsRequest,
    ) -> AllocationRequestFingerprint {
        AllocationRequestFingerprint {
            count: request.count,
        }
    }

    fn pending_request_record(
        fingerprint: AllocationRequestFingerprint,
        updated_at_ms: u64,
    ) -> RequestRecord {
        RequestRecord {
            schema_version: 1,
            fingerprint,
            state: RequestRecordState::Pending,
            response: None,
            updated_at_ms,
        }
    }

    fn completed_request_record(
        fingerprint: AllocationRequestFingerprint,
        response: &AllocateTimestampsResponse,
        updated_at_ms: u64,
    ) -> RequestRecord {
        RequestRecord {
            schema_version: 1,
            fingerprint,
            state: RequestRecordState::Completed,
            response: Some(AllocationResponseRecord {
                generator_id: response.generator_id,
                epoch: response.epoch,
                route_version: response.route_version,
                ranges: response.ranges.clone(),
            }),
            updated_at_ms,
        }
    }

    fn record_idempotency_outcome(outcome: &'static str) {
        metrics::TSO_REQUEST_IDEMPOTENCY_TOTAL
            .with_label_values(&[outcome])
            .inc();
    }

    fn pending_record_is_stale(&self, record: &RequestRecord, now_ms: u64) -> bool {
        record.state == RequestRecordState::Pending
            && now_ms.saturating_sub(record.updated_at_ms)
                >= self.config.request_record_pending_timeout_ms
    }

    fn validate_idempotent_record_fingerprint(
        request: &AllocateTimestampsRequest,
        record: &RequestRecord,
    ) -> Result<(), TsoError> {
        if record.fingerprint == Self::allocation_request_fingerprint(request) {
            return Ok(());
        }

        Self::record_idempotency_outcome("conflict");
        Err(TsoError::ClientRequestConflict {
            timeline_key: request.timeline_key.clone(),
            client_request_id: request.client_request_id.clone(),
        })
    }

    async fn prepare_idempotent_allocation(
        &self,
        request: &AllocateTimestampsRequest,
    ) -> Result<IdempotentAllocationPreparation, TsoError> {
        if !Self::idempotency_enabled(request) {
            Self::record_idempotency_outcome("disabled");
            return Ok(IdempotentAllocationPreparation::Disabled);
        }
        let Some(request_records) = self.metadata.request_records() else {
            Self::record_idempotency_outcome("unsupported");
            return Ok(IdempotentAllocationPreparation::Disabled);
        };

        let fingerprint = Self::allocation_request_fingerprint(request);
        loop {
            let now_ms = self.clock.now_ms();
            let pending_record = Self::pending_request_record(fingerprint.clone(), now_ms);
            match request_records
                .create_request_record(
                    &request.timeline_key,
                    &request.client_request_id,
                    &pending_record,
                )
                .await
            {
                Ok(revision) => {
                    Self::record_idempotency_outcome("pending_created");
                    return Ok(IdempotentAllocationPreparation::Pending(
                        PendingIdempotentAllocation {
                            revision,
                            fingerprint,
                        },
                    ));
                }
                Err(TsoError::MetadataAlreadyExists) => {}
                Err(error) => {
                    Self::record_idempotency_outcome("prepare_error");
                    return Err(error);
                }
            }

            let Some((record, revision)) = request_records
                .load_request_record(&request.timeline_key, &request.client_request_id)
                .await?
            else {
                continue;
            };
            Self::validate_idempotent_record_fingerprint(request, &record)?;

            if let Some(response) = record.completed_response(&request.timeline_key)? {
                Self::record_idempotency_outcome("replay");
                return Ok(IdempotentAllocationPreparation::Replay(response));
            }

            if self.pending_record_is_stale(&record, now_ms) {
                match request_records
                    .compare_delete_request_record(
                        &request.timeline_key,
                        &request.client_request_id,
                        revision,
                    )
                    .await
                {
                    Ok(()) => {
                        Self::record_idempotency_outcome("stale_pending_deleted");
                        continue;
                    }
                    Err(TsoError::CasFailed) | Err(TsoError::TimelineNotFound { .. }) => {
                        continue;
                    }
                    Err(error) => {
                        Self::record_idempotency_outcome("stale_pending_delete_error");
                        return Err(error);
                    }
                }
            }

            Self::record_idempotency_outcome("pending_in_progress");
            return Err(TsoError::ClientRequestInProgress {
                timeline_key: request.timeline_key.clone(),
                client_request_id: request.client_request_id.clone(),
            });
        }
    }

    async fn finish_idempotent_allocation(
        &self,
        request: &AllocateTimestampsRequest,
        preparation: IdempotentAllocationPreparation,
        allocation_result: Result<AllocateTimestampsResponse, TsoError>,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        let IdempotentAllocationPreparation::Pending(pending) = preparation else {
            return allocation_result;
        };
        let Some(request_records) = self.metadata.request_records() else {
            return allocation_result;
        };

        let response = match allocation_result {
            Ok(response) => response,
            Err(error) => {
                if request_records
                    .compare_delete_request_record(
                        &request.timeline_key,
                        &request.client_request_id,
                        pending.revision,
                    )
                    .await
                    .is_err()
                {
                    record_recovery_event(
                        "service",
                        "request_record_pending_cleanup",
                        "metadata_error",
                    );
                    Self::record_idempotency_outcome("pending_cleanup_error");
                } else {
                    Self::record_idempotency_outcome("pending_cleaned");
                }
                return Err(error);
            }
        };

        let completed_record = Self::completed_request_record(
            pending.fingerprint.clone(),
            &response,
            self.clock.now_ms(),
        );

        match request_records
            .compare_exchange_request_record(
                &request.timeline_key,
                &request.client_request_id,
                pending.revision,
                &completed_record,
            )
            .await
        {
            Ok(_) => {
                Self::record_idempotency_outcome("completed");
                Ok(response)
            }
            Err(TsoError::CasFailed) | Err(TsoError::TimelineNotFound { .. }) => {
                self.recover_idempotent_completion_after_cas_failure(
                    request,
                    completed_record,
                    response,
                )
                .await
            }
            Err(error) => {
                Self::record_idempotency_outcome("complete_error");
                record_recovery_event("service", "request_record_complete", "metadata_error");
                Err(error)
            }
        }
    }

    async fn recover_idempotent_completion_after_cas_failure(
        &self,
        request: &AllocateTimestampsRequest,
        completed_record: RequestRecord,
        response: AllocateTimestampsResponse,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        let Some(request_records) = self.metadata.request_records() else {
            return Ok(response);
        };

        match request_records
            .load_request_record(&request.timeline_key, &request.client_request_id)
            .await?
        {
            Some((record, revision)) => {
                Self::validate_idempotent_record_fingerprint(request, &record)?;
                if let Some(existing_response) = record.completed_response(&request.timeline_key)? {
                    Self::record_idempotency_outcome("complete_race_replay");
                    return Ok(existing_response);
                }
                match request_records
                    .compare_exchange_request_record(
                        &request.timeline_key,
                        &request.client_request_id,
                        revision,
                        &completed_record,
                    )
                    .await
                {
                    Ok(_) => {
                        Self::record_idempotency_outcome("complete_race_repaired");
                        Ok(response)
                    }
                    Err(TsoError::CasFailed) => {
                        Self::record_idempotency_outcome("complete_race_lost");
                        Err(TsoError::ClientRequestInProgress {
                            timeline_key: request.timeline_key.clone(),
                            client_request_id: request.client_request_id.clone(),
                        })
                    }
                    Err(error) => {
                        Self::record_idempotency_outcome("complete_repair_error");
                        Err(error)
                    }
                }
            }
            None => match request_records
                .create_request_record(
                    &request.timeline_key,
                    &request.client_request_id,
                    &completed_record,
                )
                .await
            {
                Ok(_) => {
                    Self::record_idempotency_outcome("complete_missing_recreated");
                    Ok(response)
                }
                Err(TsoError::MetadataAlreadyExists) => Err(TsoError::ClientRequestInProgress {
                    timeline_key: request.timeline_key.clone(),
                    client_request_id: request.client_request_id.clone(),
                }),
                Err(error) => {
                    Self::record_idempotency_outcome("complete_recreate_error");
                    Err(error)
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
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
        AllocateTimestampsRequest, Clock, ManualClock, ResourceTier, TimelineLifecycleState,
        TsoConfig, TsoError, TsoService, MAX_GENERATORS,
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
        fn subscribe_route_updates(
            &self,
        ) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
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

    #[tokio::test]
    async fn repeated_client_request_id_returns_recorded_allocation_response() {
        let clock = Arc::new(ManualClock::new(21_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service =
            TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();
        let route = service
            .ensure_timeline("allocation.idempotent")
            .await
            .unwrap();
        let request = AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 2,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "same-logical-request".to_string(),
        };

        let first = service.allocate_timestamps(request.clone()).await.unwrap();
        let second = service.allocate_timestamps(request).await.unwrap();

        assert_eq!(second, first);
    }

    #[tokio::test]
    async fn repeated_client_request_id_with_different_fingerprint_is_rejected() {
        let clock = Arc::new(ManualClock::new(21_500));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service =
            TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();
        let route = service
            .ensure_timeline("allocation.idempotent.conflict")
            .await
            .unwrap();

        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "conflicting-logical-request".to_string(),
            })
            .await
            .unwrap();
        let conflict = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 2,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "conflicting-logical-request".to_string(),
            })
            .await
            .unwrap_err();

        assert!(matches!(
            conflict,
            TsoError::ClientRequestConflict { timeline_key, .. }
                if timeline_key == route.timeline_key
        ));
    }

    #[tokio::test]
    async fn pending_client_request_id_is_rejected_until_timeout() {
        let clock = Arc::new(ManualClock::new(21_800));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig {
                request_record_pending_timeout_ms: 100,
                request_record_retention_ms: 1_000,
                ..TsoConfig::default()
            }),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();
        let route = service
            .ensure_timeline("allocation.idempotent.pending")
            .await
            .unwrap();
        metadata
            .create_request_record(
                &route.timeline_key,
                "pending-logical-request",
                &RequestRecord {
                    schema_version: 1,
                    fingerprint: AllocationRequestFingerprint { count: 1 },
                    state: RequestRecordState::Pending,
                    response: None,
                    updated_at_ms: clock.now_ms(),
                },
            )
            .await
            .unwrap();

        let pending = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "pending-logical-request".to_string(),
            })
            .await
            .unwrap_err();
        assert!(matches!(
            pending,
            TsoError::ClientRequestInProgress { timeline_key, .. }
                if timeline_key == route.timeline_key
        ));

        clock.advance(101);
        let recovered = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "pending-logical-request".to_string(),
            })
            .await
            .unwrap();
        let replayed = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "pending-logical-request".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(replayed, recovered);
    }

    #[tokio::test]
    async fn idempotent_allocation_fails_closed_when_completion_record_cannot_be_persisted() {
        let clock = Arc::new(ManualClock::new(21_900));
        let inner = Arc::new(MemoryMetadataStore::new());
        let metadata = Arc::new(RequestRecordCasFailureStore::new(inner.clone()));
        let service = TsoService::new(
            required_test_config(TsoConfig {
                request_record_pending_timeout_ms: 1_000,
                request_record_retention_ms: 5_000,
                ..TsoConfig::default()
            }),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let route = service
            .ensure_timeline("allocation.idempotent.completion-cas-failure")
            .await
            .unwrap();
        let request = AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "completion-cas-failure".to_string(),
        };

        metadata.fail_next_request_cas();
        let error = service
            .allocate_timestamps(request.clone())
            .await
            .expect_err("allocation must not return a response that was not recorded");
        assert!(matches!(
            error,
            TsoError::Internal(message)
                if message.contains("injected request record CAS failure")
        ));

        let (record, _) = inner
            .load_request_record(&route.timeline_key, &request.client_request_id)
            .await
            .unwrap()
            .expect("failed completion should leave the request pending");
        assert_eq!(record.state, RequestRecordState::Pending);
        assert!(record.response.is_none());

        let retry_error = service
            .allocate_timestamps(request)
            .await
            .expect_err("pending request should fail closed until the pending timeout");
        assert!(matches!(
            retry_error,
            TsoError::ClientRequestInProgress { timeline_key, .. }
                if timeline_key == route.timeline_key
        ));
    }

    #[tokio::test]
    async fn idempotent_completion_replays_recorded_response_after_cas_race() {
        let clock = Arc::new(ManualClock::new(21_950));
        let inner = Arc::new(MemoryMetadataStore::new());
        let metadata = Arc::new(RequestRecordCasFailureStore::new(inner.clone()));
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let route = service
            .ensure_timeline("allocation.idempotent.completion-cas-race")
            .await
            .unwrap();
        let request = AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 2,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "completion-cas-race".to_string(),
        };

        metadata.complete_next_request_cas_then_report_cas_failed();
        let response = service.allocate_timestamps(request.clone()).await.unwrap();
        let replayed = service.allocate_timestamps(request.clone()).await.unwrap();

        assert_eq!(replayed, response);
        let (record, _) = inner
            .load_request_record(&route.timeline_key, &request.client_request_id)
            .await
            .unwrap()
            .expect("CAS race should leave a completed request record");
        assert_eq!(record.state, RequestRecordState::Completed);
        assert_eq!(
            record
                .completed_response(&route.timeline_key)
                .unwrap()
                .as_ref(),
            Some(&response)
        );
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
                    schema_version: 1,
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
                        schema_version: 1,
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
    async fn timeline_load_limiter_respects_request_cancellation() {
        let clock = Arc::new(ManualClock::new(22_700));
        let inner = Arc::new(MemoryMetadataStore::new());
        for (timeline_key, generator_id) in [
            ("allocation.timeline-load.cancel.a", 7),
            ("allocation.timeline-load.cancel.b", 8),
        ] {
            inner
                .create_timeline(
                    timeline_key,
                    &TimelineRecord {
                        schema_version: 1,
                        route: crate::TimelineRoute {
                            timeline_key: timeline_key.into(),
                            generator_id,
                            owner_worker_endpoint: "127.0.0.1:59999".into(),
                            epoch: 1,
                            route_version: 1,
                            resource_tier: crate::ResourceTier::Shared,
                        },
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
        let first = tokio::spawn(async move {
            first_service
                .load_timeline_with_singleflight_and_cancellation(
                    "allocation.timeline-load.cancel.a",
                    None,
                )
                .await
        });

        load_started_rx
            .await
            .expect("the first cold-cache load should enter metadata");
        assert_eq!(metadata.active_loads(), 1);

        let cancellation = RequestCancellation::new();
        let cancelled_service = service.clone();
        let cancelled_request = cancellation.clone();
        let second = tokio::spawn(async move {
            cancelled_service
                .load_timeline_with_singleflight_and_cancellation(
                    "allocation.timeline-load.cancel.b",
                    Some(cancelled_request),
                )
                .await
        });

        tokio::time::sleep(Duration::from_millis(25)).await;
        cancellation.cancel();

        let error = second.await.unwrap().unwrap_err();
        assert_eq!(error, TsoError::RequestCancelled);
        assert_eq!(metadata.max_parallel_loads(), 1);

        release_load_tx.send(()).unwrap();
        first.await.unwrap().unwrap();
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
        service.generator_runtime.remove_lease(route.generator_id);

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
        service.generator_runtime.remove_lease(route.generator_id);

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
    async fn post_lease_guard_revalidates_authoritative_route_before_serving() {
        let clock = Arc::new(ManualClock::new(31_500));
        let inner = Arc::new(MemoryMetadataStore::new());
        let metadata = Arc::new(SilentRouteUpdateStore::new(inner.clone()));
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock.clone(),
            metadata,
        )
        .unwrap();

        let route = service
            .ensure_timeline("allocation.post-lease-cutover.timeline")
            .await
            .unwrap();
        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "seed-post-lease-cutover".to_string(),
            })
            .await
            .unwrap();

        let timeline_state_handle = service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .expect("timeline should be cached after seed allocation");
        let issued_upper_bound = service
            .ensure_generator_lease_for_allocation_with_cancellation(
                &route.timeline_key,
                route.generator_id,
                None,
            )
            .await
            .unwrap();

        let (mut record, revision) = inner
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        record.route.route_version += 1;
        record.updated_at_ms = clock.now_ms();
        inner
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();

        let response = service
            .try_serve_timeline_state_handle_with_guard(
                &AllocateTimestampsRequest {
                    timeline_key: route.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route.epoch,
                    expected_route_version: route.route_version,
                    client_request_id: "post-lease-cutover".to_string(),
                },
                timeline_state_handle,
                route.generator_id,
                clock.now_ms(),
                issued_upper_bound,
                CachedServeGuardOptions {
                    cancellation: None,
                    revalidate_authority: true,
                    path: AllocationPath::Metadata,
                },
            )
            .await
            .unwrap();

        assert!(
            response.is_none(),
            "post-lease guard should fail closed after cutover"
        );
        assert!(service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .is_none());
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
    async fn allocation_fails_closed_after_cutover_without_second_full_reload() {
        let clock = Arc::new(ManualClock::new(40_500));
        let inner = Arc::new(MemoryMetadataStore::new());
        let metadata = Arc::new(SilentRouteUpdateStore::new(inner.clone()));
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock.clone(),
            metadata,
        )
        .unwrap();

        let route = service
            .ensure_timeline("allocation.slow-path-cutover.timeline")
            .await
            .unwrap();
        service.clear_timeline_cache(&route.timeline_key);

        let issued_upper_bound = service
            .ensure_generator_lease_for_allocation_with_cancellation(
                &route.timeline_key,
                route.generator_id,
                None,
            )
            .await
            .unwrap();

        let (mut record, revision) = inner
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        record.route.route_version += 1;
        record.updated_at_ms = clock.now_ms();
        inner
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();

        let stale_record = TimelineRecord {
            route: route.clone(),
            updated_at_ms: clock.now_ms(),
            ..record.clone()
        };
        let timeline_state_handle = service
            .timeline_state_handle_from_record(&route.timeline_key, &stale_record, revision)
            .await
            .unwrap();

        let response = service
            .try_serve_timeline_state_handle_with_guard(
                &AllocateTimestampsRequest {
                    timeline_key: route.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route.epoch,
                    expected_route_version: route.route_version,
                    client_request_id: "slow-path-cutover".to_string(),
                },
                timeline_state_handle,
                route.generator_id,
                clock.now_ms(),
                issued_upper_bound,
                CachedServeGuardOptions {
                    cancellation: None,
                    revalidate_authority: true,
                    path: AllocationPath::Metadata,
                },
            )
            .await
            .unwrap();

        assert!(response.is_none());
        assert!(service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .is_none());
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
    async fn cached_allocation_with_valid_local_lease_skips_metadata_reload() {
        let clock = Arc::new(ManualClock::new(23_500));
        let inner = Arc::new(MemoryMetadataStore::new());
        let (load_started_tx, load_started_rx) = oneshot::channel();
        let (_release_load_tx, release_load_rx) = oneshot::channel();
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

        let route = service
            .ensure_timeline("allocation.cached-hot-path.timeline")
            .await
            .unwrap();
        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "seed-cached-hot-path".to_string(),
            })
            .await
            .unwrap();

        metadata.arm_blocking_load();

        let response = timeout(
            Duration::from_millis(50),
            service.allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "cached-hot-path".to_string(),
            }),
        )
        .await
        .expect("cached allocation should complete without metadata blocking")
        .unwrap();

        assert_eq!(response.timeline_key, route.timeline_key);
        assert!(
            timeout(Duration::from_millis(25), load_started_rx)
                .await
                .is_err(),
            "cached hot path should not trigger timeline metadata reload"
        );
    }

    #[tokio::test]
    async fn cached_allocation_with_unready_local_lease_reloads_metadata() {
        let clock = Arc::new(ManualClock::new(23_500));
        let inner = Arc::new(MemoryMetadataStore::new());
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

        let route = service
            .ensure_timeline("allocation.cached-unready-lease.timeline")
            .await
            .unwrap();
        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "seed-cached-unready-lease".to_string(),
            })
            .await
            .unwrap();

        let lease = service
            .generator_runtime
            .lease_state(route.generator_id)
            .expect("generator lease should exist");
        service.generator_runtime.remove_lease(route.generator_id);
        service
            .generator_runtime
            .upsert_lease(route.generator_id, lease);
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
                    client_request_id: "cached-unready-lease".to_string(),
                })
                .await
        });

        load_started_rx
            .await
            .expect("unready lease should force a metadata reload");
        release_load_tx.send(()).unwrap();
        let response = allocate_task.await.unwrap().unwrap();
        assert_eq!(response.timeline_key, route.timeline_key);
    }

    #[tokio::test]
    async fn cached_guard_fails_closed_when_timeline_state_drifts_before_serve() {
        let clock = Arc::new(ManualClock::new(24_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service =
            TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();

        let route = service
            .ensure_timeline("allocation.cached-state-drift.timeline")
            .await
            .unwrap();
        let timeline_state_handle = service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .expect("timeline should be cached after ensure");

        {
            let mut timeline_state = timeline_state_handle.lock().await;
            timeline_state.state = TimelineLifecycleState::Draining;
        }

        let error = service
            .try_serve_timeline_state_handle_with_guard(
                &AllocateTimestampsRequest {
                    timeline_key: route.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route.epoch,
                    expected_route_version: route.route_version,
                    client_request_id: "cached-state-drift".to_string(),
                },
                timeline_state_handle,
                route.generator_id,
                service.clock.now_ms(),
                service
                    .valid_generator_lease_upper_bound(route.generator_id, service.clock.now_ms()),
                CachedServeGuardOptions {
                    cancellation: None,
                    revalidate_authority: false,
                    path: AllocationPath::Cached,
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error,
            TsoError::TimelineNotReady {
                timeline_key: route.timeline_key.clone(),
                state: TimelineLifecycleState::Draining,
            }
        );
        assert!(service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .is_none());
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
            schema_version: 1,
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
    async fn shared_allocation_waits_for_generator_admission_gate() {
        let service = TsoService::new(
            with_worker(TsoConfig::default(), "worker-a"),
            Arc::new(ManualClock::new(52_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();

        let route = service
            .ensure_timeline("shared.admission.wait")
            .await
            .unwrap();
        assert_eq!(route.resource_tier, ResourceTier::Shared);

        let permit = service.generator_admission_gates[route.generator_id as usize]
            .clone()
            .acquire_owned()
            .await
            .unwrap();

        let service_clone = service.clone();
        let route_clone = route.clone();
        let mut allocate_task = tokio::spawn(async move {
            service_clone
                .allocate_timestamps(AllocateTimestampsRequest {
                    timeline_key: route_clone.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route_clone.epoch,
                    expected_route_version: route_clone.route_version,
                    client_request_id: "shared-admission-wait".to_string(),
                })
                .await
        });

        assert!(
            timeout(Duration::from_millis(50), &mut allocate_task)
                .await
                .is_err(),
            "shared allocation should wait while the generator admission gate is held"
        );

        drop(permit);
        let response = allocate_task.await.unwrap().unwrap();
        assert_eq!(response.timeline_key, route.timeline_key);
    }

    async fn ensure_shared_timeline_on_generator(
        service: &TsoService,
        generator_id: u32,
        prefix: &str,
    ) -> crate::TimelineRoute {
        for attempt in 0..(MAX_GENERATORS as usize * 8).max(8) {
            let route = service
                .ensure_timeline(&format!("{prefix}.{attempt}"))
                .await
                .unwrap();
            if route.resource_tier == ResourceTier::Shared && route.generator_id == generator_id {
                return route;
            }
        }
        panic!("failed to find shared timeline for generator {generator_id}");
    }

    #[tokio::test]
    async fn shared_large_batch_repeat_waits_behind_other_timeline() {
        let service = TsoService::new(
            with_worker(
                TsoConfig {
                    shared_generators: 1,
                    warm_generators: 0,
                    max_batch_per_request: 8,
                    ..TsoConfig::default()
                },
                "worker-a",
            ),
            Arc::new(ManualClock::new(54_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();

        let route_a = service.ensure_timeline("shared.fairness.a").await.unwrap();
        let route_b = ensure_shared_timeline_on_generator(
            &service,
            route_a.generator_id,
            "shared.fairness.b",
        )
        .await;

        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_a.timeline_key.clone(),
                count: 4,
                expected_epoch: route_a.epoch,
                expected_route_version: route_a.route_version,
                client_request_id: "shared-large-first".into(),
            })
            .await
            .unwrap();

        let permit = service.generator_admission_gates[route_a.generator_id as usize]
            .clone()
            .acquire_owned()
            .await
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let service_clone = service.clone();
        let route_b_clone = route_b.clone();
        let tx_b = tx.clone();
        let mut blocked_task = tokio::spawn(async move {
            let response = service_clone
                .allocate_timestamps(AllocateTimestampsRequest {
                    timeline_key: route_b_clone.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route_b_clone.epoch,
                    expected_route_version: route_b_clone.route_version,
                    client_request_id: "shared-other-waiter".into(),
                })
                .await;
            if let Ok(response) = &response {
                let _ = tx_b.send(response.timeline_key.clone());
            }
            response
        });

        assert!(timeout(Duration::from_millis(50), &mut blocked_task)
            .await
            .is_err());

        let service_clone = service.clone();
        let route_a_clone = route_a.clone();
        let tx_a = tx.clone();
        let mut repeat_task = tokio::spawn(async move {
            let response = service_clone
                .allocate_timestamps(AllocateTimestampsRequest {
                    timeline_key: route_a_clone.timeline_key.clone(),
                    count: 4,
                    expected_epoch: route_a_clone.epoch,
                    expected_route_version: route_a_clone.route_version,
                    client_request_id: "shared-large-repeat".into(),
                })
                .await;
            if let Ok(response) = &response {
                let _ = tx_a.send(response.timeline_key.clone());
            }
            response
        });

        assert!(timeout(Duration::from_millis(50), &mut repeat_task)
            .await
            .is_err());

        drop(permit);
        assert_eq!(rx.recv().await.unwrap(), route_b.timeline_key);
        assert_eq!(rx.recv().await.unwrap(), route_a.timeline_key);
        assert_eq!(
            blocked_task.await.unwrap().unwrap().timeline_key,
            route_b.timeline_key
        );
        assert_eq!(
            repeat_task.await.unwrap().unwrap().timeline_key,
            route_a.timeline_key
        );
    }

    #[tokio::test]
    async fn shared_small_batch_repeat_waits_behind_other_timeline() {
        let service = TsoService::new(
            with_worker(
                TsoConfig {
                    shared_generators: 1,
                    warm_generators: 0,
                    max_batch_per_request: 8,
                    ..TsoConfig::default()
                },
                "worker-a",
            ),
            Arc::new(ManualClock::new(55_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();

        let route_a = service.ensure_timeline("shared.small.a").await.unwrap();
        let route_b =
            ensure_shared_timeline_on_generator(&service, route_a.generator_id, "shared.small.b")
                .await;

        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_a.timeline_key.clone(),
                count: 4,
                expected_epoch: route_a.epoch,
                expected_route_version: route_a.route_version,
                client_request_id: "shared-large-seed".into(),
            })
            .await
            .unwrap();

        let permit = service.generator_admission_gates[route_a.generator_id as usize]
            .clone()
            .acquire_owned()
            .await
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let service_clone = service.clone();
        let route_b_clone = route_b.clone();
        let tx_b = tx.clone();
        let mut blocked_b = tokio::spawn(async move {
            let response = service_clone
                .allocate_timestamps(AllocateTimestampsRequest {
                    timeline_key: route_b_clone.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route_b_clone.epoch,
                    expected_route_version: route_b_clone.route_version,
                    client_request_id: "shared-small-waiter".into(),
                })
                .await;
            if let Ok(response) = &response {
                let _ = tx_b.send(response.timeline_key.clone());
            }
            response
        });
        assert!(timeout(Duration::from_millis(50), &mut blocked_b)
            .await
            .is_err());

        let service_clone = service.clone();
        let route_a_clone = route_a.clone();
        let tx_a = tx.clone();
        let mut repeat_a = tokio::spawn(async move {
            let response = service_clone
                .allocate_timestamps(AllocateTimestampsRequest {
                    timeline_key: route_a_clone.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route_a_clone.epoch,
                    expected_route_version: route_a_clone.route_version,
                    client_request_id: "shared-small-repeat".into(),
                })
                .await;
            if let Ok(response) = &response {
                let _ = tx_a.send(response.timeline_key.clone());
            }
            response
        });
        assert!(timeout(Duration::from_millis(50), &mut repeat_a)
            .await
            .is_err());

        drop(permit);
        assert_eq!(rx.recv().await.unwrap(), route_b.timeline_key);
        assert_eq!(rx.recv().await.unwrap(), route_a.timeline_key);
        assert_eq!(
            blocked_b.await.unwrap().unwrap().timeline_key,
            route_b.timeline_key
        );
        assert_eq!(
            repeat_a.await.unwrap().unwrap().timeline_key,
            route_a.timeline_key
        );
    }

    async fn ensure_warm_timeline_on_generator(
        service: &TsoService,
        generator_id: u32,
        prefix: &str,
    ) -> crate::TimelineRoute {
        for attempt in 0..(MAX_GENERATORS as usize * 8).max(8) {
            let route = service
                .ensure_timeline(&format!("{prefix}.{attempt}"))
                .await
                .unwrap();
            if route.resource_tier == ResourceTier::Warm && route.generator_id == generator_id {
                return route;
            }
        }
        panic!("failed to find warm timeline for generator {generator_id}");
    }

    #[tokio::test]
    async fn warm_repeat_winner_waits_behind_other_timeline() {
        let service = TsoService::new(
            with_worker(
                TsoConfig {
                    default_resource_tier: ResourceTier::Warm,
                    shared_generators: 0,
                    warm_generators: 1,
                    max_batch_per_request: 8,
                    ..TsoConfig::default()
                },
                "worker-a",
            ),
            Arc::new(ManualClock::new(56_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();

        let route_a = service.ensure_timeline("warm.fairness.a").await.unwrap();
        let route_b =
            ensure_warm_timeline_on_generator(&service, route_a.generator_id, "warm.fairness.b")
                .await;

        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_a.timeline_key.clone(),
                count: 1,
                expected_epoch: route_a.epoch,
                expected_route_version: route_a.route_version,
                client_request_id: "warm-first".into(),
            })
            .await
            .unwrap();

        let permit = service.generator_admission_gates[route_a.generator_id as usize]
            .clone()
            .acquire_owned()
            .await
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let service_clone = service.clone();
        let route_b_clone = route_b.clone();
        let tx_b = tx.clone();
        let mut blocked_task = tokio::spawn(async move {
            let response = service_clone
                .allocate_timestamps(AllocateTimestampsRequest {
                    timeline_key: route_b_clone.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route_b_clone.epoch,
                    expected_route_version: route_b_clone.route_version,
                    client_request_id: "warm-waiter".into(),
                })
                .await;
            if let Ok(response) = &response {
                let _ = tx_b.send(response.timeline_key.clone());
            }
            response
        });

        assert!(timeout(Duration::from_millis(50), &mut blocked_task)
            .await
            .is_err());

        let service_clone = service.clone();
        let route_a_clone = route_a.clone();
        let tx_a = tx.clone();
        let mut repeat_task = tokio::spawn(async move {
            let response = service_clone
                .allocate_timestamps(AllocateTimestampsRequest {
                    timeline_key: route_a_clone.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route_a_clone.epoch,
                    expected_route_version: route_a_clone.route_version,
                    client_request_id: "warm-repeat".into(),
                })
                .await;
            if let Ok(response) = &response {
                let _ = tx_a.send(response.timeline_key.clone());
            }
            response
        });

        assert!(timeout(Duration::from_millis(50), &mut repeat_task)
            .await
            .is_err());

        drop(permit);
        assert_eq!(rx.recv().await.unwrap(), route_b.timeline_key);
        assert_eq!(rx.recv().await.unwrap(), route_a.timeline_key);
        assert_eq!(
            blocked_task.await.unwrap().unwrap().timeline_key,
            route_b.timeline_key
        );
        assert_eq!(
            repeat_task.await.unwrap().unwrap().timeline_key,
            route_a.timeline_key
        );
    }

    #[tokio::test]
    async fn shared_waiters_are_served_in_fifo_timeline_order() {
        let service = TsoService::new(
            with_worker(
                TsoConfig {
                    shared_generators: 1,
                    warm_generators: 0,
                    max_batch_per_request: 8,
                    ..TsoConfig::default()
                },
                "worker-a",
            ),
            Arc::new(ManualClock::new(57_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();

        let route_a = service.ensure_timeline("shared.queue.a").await.unwrap();
        let route_b =
            ensure_shared_timeline_on_generator(&service, route_a.generator_id, "shared.queue.b")
                .await;
        let route_c =
            ensure_shared_timeline_on_generator(&service, route_a.generator_id, "shared.queue.c")
                .await;

        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_a.timeline_key.clone(),
                count: 1,
                expected_epoch: route_a.epoch,
                expected_route_version: route_a.route_version,
                client_request_id: "shared-queue-seed".into(),
            })
            .await
            .unwrap();

        let permit = service.generator_admission_gates[route_a.generator_id as usize]
            .clone()
            .acquire_owned()
            .await
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();

        for (route, request_id) in [
            (route_b.clone(), "shared-queue-b"),
            (route_c.clone(), "shared-queue-c"),
        ] {
            let service_clone = service.clone();
            let tx_clone = tx.clone();
            tokio::spawn(async move {
                let response = service_clone
                    .allocate_timestamps(AllocateTimestampsRequest {
                        timeline_key: route.timeline_key.clone(),
                        count: 1,
                        expected_epoch: route.epoch,
                        expected_route_version: route.route_version,
                        client_request_id: request_id.into(),
                    })
                    .await
                    .unwrap();
                let _ = tx_clone.send(response.timeline_key);
            });
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(permit);

        assert_eq!(rx.recv().await.unwrap(), route_b.timeline_key);
        assert_eq!(rx.recv().await.unwrap(), route_c.timeline_key);
    }

    #[tokio::test]
    async fn shared_timeline_hits_per_timeline_quota_before_repeated_large_allocation() {
        let service = TsoService::new(
            with_worker(
                TsoConfig {
                    shared_generators: 1,
                    warm_generators: 0,
                    max_batch_per_request: 8,
                    max_future_borrow_ms: 2_000,
                    ..TsoConfig::default()
                },
                "worker-a",
            ),
            Arc::new(ManualClock::new(58_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();

        let route = service.ensure_timeline("shared.quota.a").await.unwrap();

        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 8,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "shared-quota-first".into(),
            })
            .await
            .unwrap();

        let error = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 8,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "shared-quota-repeat".into(),
            })
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TsoError::FutureBorrowExceeded {
                requested_physical_ms,
                allowed_physical_ms,
            } if requested_physical_ms > allowed_physical_ms
        ));
    }

    #[tokio::test]
    async fn shared_timeline_quota_does_not_block_peer_timeline() {
        let service = TsoService::new(
            with_worker(
                TsoConfig {
                    shared_generators: 1,
                    warm_generators: 0,
                    max_batch_per_request: 8,
                    max_future_borrow_ms: 2_000,
                    ..TsoConfig::default()
                },
                "worker-a",
            ),
            Arc::new(ManualClock::new(59_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();

        let route_a = service
            .ensure_timeline("shared.quota.peer.a")
            .await
            .unwrap();
        let route_b = ensure_shared_timeline_on_generator(
            &service,
            route_a.generator_id,
            "shared.quota.peer.b",
        )
        .await;

        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_a.timeline_key.clone(),
                count: 8,
                expected_epoch: route_a.epoch,
                expected_route_version: route_a.route_version,
                client_request_id: "shared-peer-seed".into(),
            })
            .await
            .unwrap();

        let peer_response = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_b.timeline_key.clone(),
                count: 1,
                expected_epoch: route_b.epoch,
                expected_route_version: route_b.route_version,
                client_request_id: "shared-peer-alloc".into(),
            })
            .await
            .unwrap();

        assert_eq!(peer_response.timeline_key, route_b.timeline_key);
    }

    #[tokio::test]
    async fn warm_timeline_hits_per_timeline_quota_before_repeated_allocation() {
        let service = TsoService::new(
            with_worker(
                TsoConfig {
                    default_resource_tier: ResourceTier::Warm,
                    shared_generators: 0,
                    warm_generators: 1,
                    max_batch_per_request: 8,
                    max_future_borrow_ms: 2_000,
                    ..TsoConfig::default()
                },
                "worker-a",
            ),
            Arc::new(ManualClock::new(60_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();

        let route = service.ensure_timeline("warm.quota.a").await.unwrap();

        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 8,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "warm-quota-first".into(),
            })
            .await
            .unwrap();

        let error = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 8,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "warm-quota-repeat".into(),
            })
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TsoError::FutureBorrowExceeded {
                requested_physical_ms,
                allowed_physical_ms,
            } if requested_physical_ms > allowed_physical_ms
        ));
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
    async fn dedicated_allocation_bypasses_generator_admission_gate() {
        let service = TsoService::new(
            with_worker(
                TsoConfig {
                    default_resource_tier: ResourceTier::Dedicated,
                    ..TsoConfig::default()
                },
                "worker-a",
            ),
            Arc::new(ManualClock::new(53_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();

        let route = service
            .ensure_timeline("dedicated.admission.bypass")
            .await
            .unwrap();
        assert_eq!(route.resource_tier, ResourceTier::Dedicated);

        let permit = service.generator_admission_gates[route.generator_id as usize]
            .clone()
            .acquire_owned()
            .await
            .unwrap();

        let response = timeout(
            Duration::from_millis(100),
            service.allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "dedicated-admission-bypass".to_string(),
            }),
        )
        .await
        .expect("dedicated allocation should bypass the shared admission gate")
        .unwrap();

        drop(permit);
        assert_eq!(response.timeline_key, route.timeline_key);
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
                CachedServeGuardOptions {
                    cancellation: None,
                    revalidate_authority: true,
                    path: AllocationPath::Metadata,
                },
            )
            .await
            .unwrap();

        assert!(
            response.is_none(),
            "post-handle guard should force a retry when generator changes before serve"
        );
        assert!(service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .is_none());
    }
}
