mod coordination;
mod renewal;

use std::cmp::max;
use std::collections::VecDeque;

use tokio::time::Instant;

use crate::metadata::{GeneratorBatchOp, GeneratorRecord};
use crate::plane::RequestCancellation;
use crate::planning::generator_recovery_floor_tso;
use crate::recovery::record_recovery_event;
use crate::runtime::GeneratorLeaseState;
use crate::TsoError;

pub(in crate::service) use coordination::GeneratorLeaseCoordinator;
use renewal::GeneratorLeaseRefreshPlan;
pub(in crate::service) use renewal::GeneratorLeaseRefreshReason;

use super::TsoService;

fn runtime_lease_state_from_record(
    revision: u64,
    record: &GeneratorRecord,
) -> Result<GeneratorLeaseState, TsoError> {
    Ok(GeneratorLeaseState {
        revision,
        owner_instance_id: record.owner_instance_id.clone(),
        generator_lease_token: record.generator_lease_token,
        lease_expire_at_ms: record
            .lease_expire_at_ms
            .ok_or(TsoError::GeneratorLeaseExpired {
                generator_id: record.generator_id,
            })?,
        last_persisted_tso: record.last_issued_tso,
        issued_upper_bound: record.issued_upper_bound,
    })
}

impl TsoService {
    pub(super) async fn ensure_generator_lease(&self, generator_id: u32) -> Result<(), TsoError> {
        self.ensure_generator_lease_with_cancellation(generator_id, None)
            .await
    }

    pub(super) async fn ensure_generator_lease_with_cancellation(
        &self,
        generator_id: u32,
        cancellation: Option<RequestCancellation>,
    ) -> Result<(), TsoError> {
        let _flight = self
            .acquire_generator_lease_singleflight_with_cancellation(
                generator_id,
                cancellation.clone(),
            )
            .await?;
        Self::check_request_cancellation(cancellation.as_ref())?;
        if self.is_generator_lease_valid(generator_id, self.clock.now_ms()) {
            return Ok(());
        }
        self.ensure_generator_lease_after_singleflight(generator_id, cancellation)
            .await
    }

    async fn ensure_generator_lease_after_singleflight(
        &self,
        generator_id: u32,
        cancellation: Option<RequestCancellation>,
    ) -> Result<(), TsoError> {
        self.ensure_generator_id_range(generator_id)?;
        if !self.owns_generator_id(generator_id) {
            return Err(TsoError::GeneratorNotOwnedByThisWorker {
                generator_id,
                modulo: self.config.generator_ownership_modulo,
                remainder: self.config.generator_ownership_remainder,
            });
        }

        if self.is_generator_lease_valid(generator_id, self.clock.now_ms()) {
            return Ok(());
        }

        let deadline = Instant::now() + self.metadata_contention_retry_budget();
        let mut contention_retries: u32 = 0;
        loop {
            Self::check_request_cancellation(cancellation.as_ref())?;
            match self.metadata.load_generator(generator_id).await? {
                Some((record, revision)) => {
                    let now_ms = self.clock.now_ms();
                    let lease_expire_at_ms = record.lease_expire_at_ms;
                    let generator_floor_tso = generator_recovery_floor_tso(&record);
                    let takeover_ready = lease_expire_at_ms.is_some_and(|exp| {
                        crate::lease_expired_with_safety_gap(exp, now_ms, self.config.safety_gap_ms)
                    });
                    let is_local_owner = self.is_local_generator_owner(&record);
                    if is_local_owner && !takeover_ready {
                        let Some(lease_expire_at_ms) = lease_expire_at_ms else {
                            self.clear_generator_ownership_drift(generator_id);
                            return Err(TsoError::GeneratorLeaseExpired { generator_id });
                        };
                        if lease_expire_at_ms > now_ms {
                            if let Some(generator_floor_tso) = generator_floor_tso {
                                self.lookup_generator(generator_id)?
                                    .init_after_floor(generator_floor_tso)?;
                            }
                            self.generator_runtime.mark_generator_ready_for_lease(
                                generator_id,
                                record.generator_lease_token,
                            );
                            self.generator_runtime.upsert_lease(
                                generator_id,
                                crate::runtime::GeneratorLeaseState {
                                    revision,
                                    owner_instance_id: record.owner_instance_id.clone(),
                                    generator_lease_token: record.generator_lease_token,
                                    lease_expire_at_ms,
                                    last_persisted_tso: generator_floor_tso,
                                    issued_upper_bound: record.issued_upper_bound,
                                },
                            );
                            self.clear_generator_ownership_drift(generator_id);
                            return Ok(());
                        }
                        self.clear_generator_ownership_drift(generator_id);
                        return Err(TsoError::GeneratorLeaseExpired { generator_id });
                    }

                    if !is_local_owner {
                        if let Some(exp) = lease_expire_at_ms.filter(|exp| *exp > now_ms) {
                            if self.is_local_endpoint(&record.owner_worker_endpoint) {
                                self.observe_contended_local_generator_ownership_drift(
                                    generator_id,
                                    &record.owner_instance_id,
                                    exp,
                                    now_ms,
                                );
                            } else {
                                self.clear_generator_ownership_drift(generator_id);
                            }
                        } else {
                            self.clear_generator_ownership_drift(generator_id);
                        }
                    }

                    if takeover_ready {
                        let mut record = record;
                        let new_lease_expire_at_ms = now_ms + self.config.generator_lease_ttl_ms;
                        if let Some(generator_floor_tso) = generator_floor_tso {
                            let floor_cursor =
                                crate::next_cursor_after(generator_floor_tso, generator_id)?;
                            let upper_bound_base_ms = max(now_ms, floor_cursor.physical_ms);
                            record.last_issued_tso = Some(generator_floor_tso);
                            record.issued_upper_bound = self
                                .compute_generator_upper_bound(generator_id, upper_bound_base_ms);
                        } else {
                            record.issued_upper_bound =
                                self.compute_generator_upper_bound(generator_id, now_ms);
                        }
                        record.owner_worker_endpoint = self.config.advertise_endpoint.clone();
                        record.owner_instance_id = self.local_instance_id().to_owned();
                        record.generator_lease_token =
                            record.generator_lease_token.saturating_add(1).max(1);
                        record.lease_expire_at_ms = Some(new_lease_expire_at_ms);
                        record.updated_at_ms = now_ms;
                        match self
                            .metadata
                            .compare_exchange_generator(generator_id, revision, &record)
                            .await
                        {
                            Ok(new_rev) => {
                                if let Some(generator_floor_tso) =
                                    generator_recovery_floor_tso(&record)
                                {
                                    self.lookup_generator(generator_id)?
                                        .init_after_floor(generator_floor_tso)?;
                                }
                                self.generator_runtime.mark_generator_ready_for_lease(
                                    generator_id,
                                    record.generator_lease_token,
                                );
                                self.generator_runtime.upsert_lease(
                                    generator_id,
                                    crate::runtime::GeneratorLeaseState {
                                        revision: new_rev,
                                        owner_instance_id: record.owner_instance_id.clone(),
                                        generator_lease_token: record.generator_lease_token,
                                        lease_expire_at_ms: new_lease_expire_at_ms,
                                        last_persisted_tso: record.last_issued_tso,
                                        issued_upper_bound: record.issued_upper_bound,
                                    },
                                );
                                self.clear_generator_ownership_drift(generator_id);
                                return Ok(());
                            }
                            Err(TsoError::CasFailed) => {
                                contention_retries = contention_retries.saturating_add(1);
                                self.backoff_after_metadata_contention(
                                    contention_retries,
                                    deadline,
                                )
                                .await?;
                                Self::check_request_cancellation(cancellation.as_ref())?;
                                continue;
                            }
                            Err(e) => return Err(e),
                        }
                    }

                    return Err(TsoError::NotGeneratorOwner {
                        generator_id,
                        owner_worker_endpoint: record.owner_worker_endpoint,
                    });
                }
                None => {
                    let now_ms = self.clock.now_ms();
                    let lease_expire_at_ms = now_ms + self.config.generator_lease_ttl_ms;
                    let record = GeneratorRecord {
                        schema_version: 1,
                        generator_id,
                        owner_worker_endpoint: self.config.advertise_endpoint.clone(),
                        owner_instance_id: self.local_instance_id().to_owned(),
                        generator_lease_token: 1,
                        lease_expire_at_ms: Some(lease_expire_at_ms),
                        last_issued_tso: None,
                        issued_upper_bound: self
                            .compute_generator_upper_bound(generator_id, now_ms),
                        updated_at_ms: now_ms,
                    };
                    match self.metadata.create_generator(generator_id, &record).await {
                        Ok(revision) => {
                            self.generator_runtime.mark_generator_ready_for_lease(
                                generator_id,
                                record.generator_lease_token,
                            );
                            self.generator_runtime.upsert_lease(
                                generator_id,
                                crate::runtime::GeneratorLeaseState {
                                    revision,
                                    owner_instance_id: record.owner_instance_id.clone(),
                                    generator_lease_token: record.generator_lease_token,
                                    lease_expire_at_ms,
                                    last_persisted_tso: None,
                                    issued_upper_bound: record.issued_upper_bound,
                                },
                            );
                            self.clear_generator_ownership_drift(generator_id);
                            return Ok(());
                        }
                        Err(TsoError::MetadataAlreadyExists) => {
                            contention_retries = contention_retries.saturating_add(1);
                            self.backoff_after_metadata_contention(contention_retries, deadline)
                                .await?;
                            Self::check_request_cancellation(cancellation.as_ref())?;
                            continue;
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
    }

    pub(super) async fn ensure_generator_lease_for_allocation_with_cancellation(
        &self,
        timeline_key: &str,
        generator_id: u32,
        cancellation: Option<RequestCancellation>,
    ) -> Result<Option<u64>, TsoError> {
        let now_ms = self.clock.now_ms();
        if let Some(issued_upper_bound) =
            self.valid_generator_lease_upper_bound(generator_id, now_ms)
        {
            return Ok(Some(issued_upper_bound));
        }

        Self::check_request_cancellation(cancellation.as_ref())?;

        let _flight = self
            .acquire_generator_lease_singleflight_with_cancellation(
                generator_id,
                cancellation.clone(),
            )
            .await?;
        Self::check_request_cancellation(cancellation.as_ref())?;
        let now_ms = self.clock.now_ms();
        if let Some(issued_upper_bound) =
            self.valid_generator_lease_upper_bound(generator_id, now_ms)
        {
            return Ok(Some(issued_upper_bound));
        }

        let record = self
            .metadata
            .load_generator(generator_id)
            .await?
            .map(|(record, _)| record)
            .ok_or_else(|| TsoError::LeaseExpired {
                timeline_key: timeline_key.to_owned(),
            })?;
        let now_ms = self.clock.now_ms();
        let Some(exp) = record.lease_expire_at_ms else {
            crate::metrics::TSO_LEASE_EXPIRED_TOTAL.inc();
            self.clear_generator_ownership_drift(generator_id);
            return Err(TsoError::LeaseExpired {
                timeline_key: timeline_key.to_owned(),
            });
        };
        if !self.is_local_generator_owner(&record) || exp <= now_ms {
            crate::metrics::TSO_LEASE_EXPIRED_TOTAL.inc();
            if self.is_local_endpoint(&record.owner_worker_endpoint) && exp > now_ms {
                self.observe_contended_local_generator_ownership_drift(
                    generator_id,
                    &record.owner_instance_id,
                    exp,
                    now_ms,
                );
            } else {
                self.clear_generator_ownership_drift(generator_id);
            }
            return Err(TsoError::LeaseExpired {
                timeline_key: timeline_key.to_owned(),
            });
        }

        self.ensure_generator_lease_after_singleflight(generator_id, cancellation)
            .await?;
        let now_ms = self.clock.now_ms();
        Ok(self.valid_generator_lease_upper_bound(generator_id, now_ms))
    }

    pub(super) async fn refresh_generator_lease(&self, generator_id: u32) -> Result<(), TsoError> {
        self.refresh_generator_lease_inner(generator_id, GeneratorLeaseRefreshReason::Maintenance)
            .await
    }

    pub(super) async fn refresh_generator_lease_inner(
        &self,
        generator_id: u32,
        reason: GeneratorLeaseRefreshReason,
    ) -> Result<(), TsoError> {
        self.refresh_generator_lease_inner_with_cancellation(generator_id, reason, None)
            .await
    }

    pub(super) async fn refresh_generator_lease_inner_with_cancellation(
        &self,
        generator_id: u32,
        reason: GeneratorLeaseRefreshReason,
        cancellation: Option<RequestCancellation>,
    ) -> Result<(), TsoError> {
        let _flight = self
            .acquire_generator_lease_singleflight_with_cancellation(
                generator_id,
                cancellation.clone(),
            )
            .await?;
        Self::check_request_cancellation(cancellation.as_ref())?;
        let now_ms = self.clock.now_ms();
        self.refresh_generator_lease_after_singleflight_with_cancellation(
            generator_id,
            now_ms,
            reason,
            cancellation,
        )
        .await
    }

    // The caller must hold this generator's lease singleflight for the full await.
    async fn refresh_generator_lease_after_singleflight_with_cancellation(
        &self,
        generator_id: u32,
        now_ms: u64,
        reason: GeneratorLeaseRefreshReason,
        cancellation: Option<RequestCancellation>,
    ) -> Result<(), TsoError> {
        let lease_state = self.generator_runtime.lease_state(generator_id);
        let Some(lease_state) = lease_state else {
            return Ok(());
        };

        Self::check_request_cancellation(cancellation.as_ref())?;

        if lease_state.lease_expire_at_ms <= now_ms {
            return Err(TsoError::GeneratorLeaseExpired { generator_id });
        }

        let ttl = self.config.generator_lease_ttl_ms;
        let local_last = self
            .lookup_generator(generator_id)?
            .current_last_issued_tso()?;
        let plan = GeneratorLeaseRefreshPlan::build(
            lease_state.lease_expire_at_ms,
            lease_state.last_persisted_tso,
            lease_state.issued_upper_bound,
            local_last,
            now_ms,
            ttl,
            self.config.pre_borrow_ms,
        );

        if !plan.needs_write(reason) {
            return Ok(());
        }

        let next_upper_bound = plan
            .should_extend_upper_bound(reason)
            .then(|| {
                self.compute_generator_upper_bound(generator_id, plan.next_upper_bound_base_ms())
            })
            .flatten();
        let refreshed_lease_expire_at_ms = now_ms + ttl;

        let record = GeneratorRecord {
            schema_version: 1,
            generator_id,
            owner_worker_endpoint: self.config.advertise_endpoint.clone(),
            owner_instance_id: lease_state.owner_instance_id.clone(),
            generator_lease_token: lease_state.generator_lease_token,
            lease_expire_at_ms: Some(
                plan.record_lease_expire_at_ms(refreshed_lease_expire_at_ms, reason),
            ),
            last_issued_tso: plan.candidate_last(),
            issued_upper_bound: plan.record_issued_upper_bound(next_upper_bound, reason),
            updated_at_ms: now_ms,
        };
        match self
            .metadata
            .compare_exchange_generator(generator_id, lease_state.revision, &record)
            .await
        {
            Ok(new_rev) => {
                let state = runtime_lease_state_from_record(new_rev, &record)?;
                self.generator_runtime
                    .mark_generator_ready_for_lease(generator_id, record.generator_lease_token);
                self.generator_runtime.upsert_lease(generator_id, state);
                self.clear_generator_ownership_drift(generator_id);
                Ok(())
            }
            Err(TsoError::CasFailed) => {
                self.generator_runtime.remove_lease(generator_id);
                let result = self
                    .ensure_generator_lease_after_singleflight(generator_id, cancellation)
                    .await;
                if result.is_err() && self.generator_runtime.lease_state(generator_id).is_none() {
                    // Keep the entry visible to background maintenance, but do not restore its
                    // ready token: the CAS conflict means ownership may have changed remotely.
                    self.generator_runtime
                        .upsert_lease(generator_id, lease_state);
                }
                result
            }
            Err(e) => Err(e),
        }
    }

    pub(super) async fn refresh_generator_lease_if_unchanged_with_cancellation(
        &self,
        generator_id: u32,
        observed_upper_bound: Option<u64>,
        cancellation: Option<RequestCancellation>,
    ) -> Result<(), TsoError> {
        let _flight = self
            .acquire_generator_lease_singleflight_with_cancellation(
                generator_id,
                cancellation.clone(),
            )
            .await?;
        Self::check_request_cancellation(cancellation.as_ref())?;
        let now_ms = self.clock.now_ms();
        if let Some(lease_state) = self.generator_runtime.lease_state(generator_id) {
            if lease_state.lease_expire_at_ms > now_ms
                && lease_state.issued_upper_bound != observed_upper_bound
            {
                return Ok(());
            }
        } else {
            return self
                .ensure_generator_lease_after_singleflight(generator_id, cancellation)
                .await;
        }

        self.refresh_generator_lease_after_singleflight_with_cancellation(
            generator_id,
            now_ms,
            GeneratorLeaseRefreshReason::HorizonRequired,
            cancellation,
        )
        .await
    }

    pub(super) async fn refresh_generator_leases_batch(&self, generator_ids: Vec<u32>) {
        if generator_ids.is_empty() {
            return;
        }

        let mut operations = Vec::new();
        for generator_id in generator_ids {
            let now_ms = self.clock.now_ms();
            let lease_state = self.generator_runtime.lease_state(generator_id);
            let Some(lease_state) = lease_state else {
                continue;
            };
            if lease_state.lease_expire_at_ms <= now_ms {
                record_recovery_event(
                    "service",
                    "batch_generator_refresh",
                    "expired_lease_tracked",
                );
                continue;
            }

            let ttl = self.config.generator_lease_ttl_ms;
            let local_last = match self
                .lookup_generator(generator_id)
                .and_then(|generator| generator.current_last_issued_tso())
            {
                Ok(value) => value,
                Err(_) => continue,
            };
            let plan = GeneratorLeaseRefreshPlan::build(
                lease_state.lease_expire_at_ms,
                lease_state.last_persisted_tso,
                lease_state.issued_upper_bound,
                local_last,
                now_ms,
                ttl,
                self.config.pre_borrow_ms,
            );
            if !plan.needs_write(GeneratorLeaseRefreshReason::Maintenance) {
                continue;
            }

            let next_upper_bound = plan
                .should_extend_upper_bound(GeneratorLeaseRefreshReason::Maintenance)
                .then(|| {
                    self.compute_generator_upper_bound(
                        generator_id,
                        plan.next_upper_bound_base_ms(),
                    )
                })
                .flatten();
            let refreshed_lease_expire_at_ms = now_ms + ttl;

            let record = GeneratorRecord {
                schema_version: 1,
                generator_id,
                owner_worker_endpoint: self.config.advertise_endpoint.clone(),
                owner_instance_id: lease_state.owner_instance_id.clone(),
                generator_lease_token: lease_state.generator_lease_token,
                lease_expire_at_ms: Some(plan.record_lease_expire_at_ms(
                    refreshed_lease_expire_at_ms,
                    GeneratorLeaseRefreshReason::Maintenance,
                )),
                last_issued_tso: plan.candidate_last(),
                issued_upper_bound: plan.record_issued_upper_bound(
                    next_upper_bound,
                    GeneratorLeaseRefreshReason::Maintenance,
                ),
                updated_at_ms: now_ms,
            };
            operations.push(GeneratorBatchOp {
                generator_id,
                previous_revision: lease_state.revision,
                record,
            });
        }

        if operations.is_empty() {
            return;
        }

        let mut pending_batches = VecDeque::from([operations]);
        while let Some(mut batch_operations) = pending_batches.pop_front() {
            match self
                .metadata
                .compare_exchange_generators(&batch_operations)
                .await
            {
                Ok(revisions) => {
                    if revisions.len() != batch_operations.len() {
                        record_recovery_event(
                            "service",
                            "batch_generator_refresh",
                            "invalid_revision_count",
                        );
                        continue;
                    }
                    for (operation, revision) in
                        batch_operations.into_iter().zip(revisions.into_iter())
                    {
                        let generator_id = operation.generator_id;
                        let Ok(state) =
                            runtime_lease_state_from_record(revision, &operation.record)
                        else {
                            record_recovery_event(
                                "service",
                                "batch_generator_refresh",
                                "invalid_lease_record",
                            );
                            continue;
                        };
                        self.generator_runtime.mark_generator_ready_for_lease(
                            generator_id,
                            state.generator_lease_token,
                        );
                        self.generator_runtime.upsert_lease(generator_id, state);
                        self.clear_generator_ownership_drift(generator_id);
                    }
                }
                Err(TsoError::CasFailed) if batch_operations.len() > 1 => {
                    record_recovery_event("service", "batch_generator_refresh", "batch_cas_split");
                    let split_at = batch_operations.len() / 2;
                    let right_operations = batch_operations.split_off(split_at);
                    pending_batches.push_front(right_operations);
                    pending_batches.push_front(batch_operations);
                }
                Err(TsoError::CasFailed) => {
                    record_recovery_event(
                        "service",
                        "batch_generator_refresh",
                        "generator_cas_conflict",
                    );
                    let generator_id = batch_operations[0].generator_id;
                    if self.refresh_generator_lease(generator_id).await.is_err() {
                        record_recovery_event(
                            "service",
                            "batch_generator_refresh",
                            "conflict_refresh_failed",
                        );
                    }
                }
                Err(_) => {
                    record_recovery_event(
                        "service",
                        "batch_generator_refresh",
                        "batch_store_failed",
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use tokio::sync::broadcast;
    use tokio::time::{sleep, timeout, Duration};

    use crate::metadata::MemoryMetadataStore;
    use crate::metadata::{
        ControlPlaneStore, GeneratorBatchOp, GeneratorLeaseAuthority, GeneratorRecord,
        RouteUpdateSource, TimelineAuthority, TimelineBatchOp, TimelineRecord,
    };
    use crate::{
        decode_tso, encode_tso, AllocateTimestampsRequest, Clock, ManualClock,
        OwnershipDriftEvidence, TsoConfig, TsoError, TsoService, WorkerReadinessSink,
    };

    use super::GeneratorLeaseRefreshReason;

    #[derive(Default)]
    struct TestReadinessSink {
        started: StdMutex<Vec<OwnershipDriftEvidence>>,
        cleared: AtomicUsize,
    }

    impl WorkerReadinessSink for TestReadinessSink {
        fn ownership_drift_started(&self, evidence: OwnershipDriftEvidence) {
            self.started.lock().unwrap().push(evidence);
        }

        fn ownership_drift_cleared(&self) {
            self.cleared.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn assert_runtime_matches_record(
        service: &TsoService,
        generator_id: u32,
        record: &GeneratorRecord,
        revision: u64,
    ) {
        let runtime = service
            .generator_runtime
            .lease_state(generator_id)
            .expect("runtime lease should exist");
        assert_eq!(runtime.revision, revision);
        assert_eq!(runtime.owner_instance_id, record.owner_instance_id);
        assert_eq!(runtime.generator_lease_token, record.generator_lease_token);
        assert_eq!(Some(runtime.lease_expire_at_ms), record.lease_expire_at_ms);
        assert_eq!(runtime.last_persisted_tso, record.last_issued_tso);
        assert_eq!(runtime.issued_upper_bound, record.issued_upper_bound);
    }

    fn required_test_config(config: TsoConfig) -> TsoConfig {
        let config = crate::test_tls::required_grpc_tls_test_config(config, 1);
        TsoConfig {
            advertise_endpoint: "127.0.0.1:50051".into(),
            instance_id: "lease-instance".into(),
            ..config
        }
    }

    async fn persist_expired_restart_fixture(
        metadata: &MemoryMetadataStore,
        timeline_key: &str,
        owner_instance_id: &str,
    ) -> (crate::TimelineRoute, GeneratorRecord, u64) {
        let route = crate::TimelineRoute {
            timeline_key: timeline_key.into(),
            generator_id: 0,
            epoch: 1,
            route_version: 1,
            resource_tier: crate::ResourceTier::Shared,
            owner_worker_endpoint: "127.0.0.1:50051".into(),
        };
        metadata
            .create_timeline(
                timeline_key,
                &TimelineRecord {
                    schema_version: 1,
                    route: route.clone(),
                    state: crate::TimelineLifecycleState::Recovering,
                    recovery_floor_tso: None,
                    issued_upper_bound: None,
                    last_graceful_issued: None,
                    lease_expire_at_ms: None,
                    updated_at_ms: 700,
                },
            )
            .await
            .unwrap();
        let generator = GeneratorRecord {
            schema_version: 1,
            generator_id: route.generator_id,
            owner_worker_endpoint: "127.0.0.1:50051".into(),
            owner_instance_id: owner_instance_id.into(),
            generator_lease_token: 7,
            lease_expire_at_ms: Some(800),
            last_issued_tso: Some(encode_tso(900, route.generator_id, 0).unwrap()),
            issued_upper_bound: Some(encode_tso(950, route.generator_id, 0).unwrap()),
            updated_at_ms: 700,
        };
        let revision = metadata
            .create_generator(route.generator_id, &generator)
            .await
            .unwrap();
        (route, generator, revision)
    }

    #[tokio::test]
    async fn local_generator_restart_obeys_inclusive_takeover_boundary() {
        for (case, now_ms, expected_ok, expected_takeover) in [
            ("valid", 799, true, false),
            ("pre-gap", 899, false, false),
            ("exact-boundary", 900, true, true),
        ] {
            let metadata = Arc::new(MemoryMetadataStore::new());
            let (route, before, before_revision) =
                persist_expired_restart_fixture(&metadata, case, "lease-instance").await;
            let service = TsoService::new(
                required_test_config(TsoConfig {
                    generator_lease_ttl_ms: 500,
                    lease_ttl_ms: 500,
                    safety_gap_ms: 100,
                    ..TsoConfig::default()
                }),
                Arc::new(ManualClock::new(now_ms)),
                metadata.clone(),
            )
            .unwrap();
            service.background.begin_shutdown();
            service.background.drain_tasks().await;

            let result = service.ensure_generator_lease(route.generator_id).await;
            assert_eq!(result.is_ok(), expected_ok, "case={case}: {result:?}");
            let (after, after_revision) = metadata
                .load_generator(route.generator_id)
                .await
                .unwrap()
                .unwrap();
            if expected_takeover {
                assert!(after_revision > before_revision, "case={case}");
                assert_eq!(
                    after.generator_lease_token,
                    before.generator_lease_token + 1,
                    "case={case}"
                );
            } else {
                assert_eq!(
                    (after, after_revision),
                    (before, before_revision),
                    "case={case}: valid reuse and pre-gap rejection must not mutate metadata"
                );
            }
        }
    }

    #[tokio::test]
    async fn same_instance_restart_recovers_expired_local_generator_lease() {
        let config = required_test_config(TsoConfig {
            generator_lease_ttl_ms: 500,
            lease_ttl_ms: 500,
            safety_gap_ms: 100,
            ..TsoConfig::default()
        });
        let clock = Arc::new(ManualClock::new(1_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let (route, before, before_revision) =
            persist_expired_restart_fixture(&metadata, "restart.local-expired", "lease-instance")
                .await;
        let service = TsoService::new(config.clone(), clock.clone(), metadata.clone()).unwrap();
        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        let sink = Arc::new(TestReadinessSink::default());
        service.set_worker_readiness_sink(sink.clone());
        service.observe_contended_local_generator_ownership_drift(0, "contender", 10, 0);
        service.observe_contended_local_generator_ownership_drift(0, "contender", 20, 1);

        let ensure_result = service.ensure_timeline(&route.timeline_key).await;
        let request = AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "restart-local-expired".into(),
        };
        let allocation_result = service.allocate_timestamps(request.clone()).await;
        let (after_first_attempt, after_first_revision) = metadata
            .load_generator(route.generator_id)
            .await
            .unwrap()
            .unwrap();

        let control_metadata = Arc::new(MemoryMetadataStore::new());
        let (control_route, control_before, control_before_revision) =
            persist_expired_restart_fixture(
                &control_metadata,
                "restart.remote-expired",
                "previous-instance",
            )
            .await;
        let control =
            TsoService::new(config, clock, control_metadata.clone()).expect("control service");
        control.background.begin_shutdown();
        control.background.drain_tasks().await;
        assert_eq!(
            control
                .ensure_timeline(&control_route.timeline_key)
                .await
                .unwrap(),
            control_route
        );
        let control_response = control
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: control_route.timeline_key.clone(),
                count: 1,
                expected_epoch: control_route.epoch,
                expected_route_version: control_route.route_version,
                client_request_id: "restart-remote-expired".into(),
            })
            .await
            .unwrap();
        let (control_after, control_after_revision) = control_metadata
            .load_generator(control_route.generator_id)
            .await
            .unwrap()
            .unwrap();
        assert!(control_after_revision > control_before_revision);
        assert_eq!(
            control_after.generator_lease_token,
            control_before.generator_lease_token + 1
        );
        assert_eq!(
            control_after.owner_worker_endpoint,
            before.owner_worker_endpoint
        );
        assert_eq!(control_after.owner_instance_id, "lease-instance");
        assert!(control_response.ranges[0].start_tso > control_before.issued_upper_bound.unwrap());

        assert!(
            ensure_result.is_ok(),
            "same-instance restart must recover after expiry+safety; ensure={ensure_result:?} \
             allocation={allocation_result:?}"
        );
        let response = allocation_result.unwrap();
        assert!(after_first_revision > before_revision);
        assert_eq!(
            after_first_attempt.generator_lease_token,
            before.generator_lease_token + 1
        );
        assert_eq!(
            after_first_attempt.owner_worker_endpoint,
            before.owner_worker_endpoint
        );
        assert_eq!(
            after_first_attempt.owner_instance_id,
            before.owner_instance_id
        );
        assert!(after_first_attempt.last_issued_tso >= before.issued_upper_bound);
        assert!(after_first_attempt.issued_upper_bound >= before.issued_upper_bound);
        assert!(response.ranges[0].start_tso > before.issued_upper_bound.unwrap());
        assert!(!service.ownership_drift.is_active(0));
        assert_eq!(sink.cleared.load(Ordering::Acquire), 1);

        service
            .ensure_generator_lease(route.generator_id)
            .await
            .unwrap();
        let replay = service.allocate_timestamps(request).await.unwrap();
        let (after_repeat, after_repeat_revision) = metadata
            .load_generator(route.generator_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay, response);
        assert_eq!(
            (after_repeat.generator_lease_token, after_repeat_revision),
            (
                after_first_attempt.generator_lease_token,
                after_first_revision
            ),
            "repeated ensure and idempotent allocation must not take over twice"
        );
    }

    #[derive(Clone)]
    struct AlwaysCasFailLeaseStore;

    #[async_trait]
    impl TimelineAuthority for AlwaysCasFailLeaseStore {
        async fn load_timeline(
            &self,
            _timeline_key: &str,
        ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
            Ok(None)
        }

        async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
            Ok(Vec::new())
        }

        async fn create_timeline(
            &self,
            _timeline_key: &str,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timeline(
            &self,
            _timeline_key: &str,
            _expected_revision: u64,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timelines(
            &self,
            _operations: &[TimelineBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl GeneratorLeaseAuthority for AlwaysCasFailLeaseStore {
        async fn load_generator(
            &self,
            generator_id: u32,
        ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
            Ok(Some((
                GeneratorRecord {
                    schema_version: 1,
                    generator_id,
                    owner_worker_endpoint: "remote-endpoint:50051".into(),
                    owner_instance_id: "remote-instance".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(0),
                    last_issued_tso: None,
                    issued_upper_bound: None,
                    updated_at_ms: 0,
                },
                1,
            )))
        }

        async fn create_generator(
            &self,
            _generator_id: u32,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_generator(
            &self,
            _generator_id: u32,
            _expected_revision: u64,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            Err(TsoError::CasFailed)
        }

        async fn compare_exchange_generators(
            &self,
            _operations: &[GeneratorBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            Err(TsoError::CasFailed)
        }
    }

    impl RouteUpdateSource for AlwaysCasFailLeaseStore {
        fn subscribe_route_updates(
            &self,
        ) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
    }

    #[async_trait]
    impl ControlPlaneStore for AlwaysCasFailLeaseStore {}

    #[derive(Clone)]
    struct RetryAdvancingLeaseStore {
        clock: Arc<ManualClock>,
        compare_exchange_calls: Arc<AtomicUsize>,
    }

    impl RetryAdvancingLeaseStore {
        fn new(clock: Arc<ManualClock>) -> Self {
            Self {
                clock,
                compare_exchange_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl TimelineAuthority for RetryAdvancingLeaseStore {
        async fn load_timeline(
            &self,
            _timeline_key: &str,
        ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
            Ok(None)
        }

        async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
            Ok(Vec::new())
        }

        async fn create_timeline(
            &self,
            _timeline_key: &str,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timeline(
            &self,
            _timeline_key: &str,
            _expected_revision: u64,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timelines(
            &self,
            _operations: &[TimelineBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl GeneratorLeaseAuthority for RetryAdvancingLeaseStore {
        async fn load_generator(
            &self,
            generator_id: u32,
        ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
            Ok(Some((
                GeneratorRecord {
                    schema_version: 1,
                    generator_id,
                    owner_worker_endpoint: "remote-endpoint".into(),
                    owner_instance_id: "remote-instance".into(),
                    generator_lease_token: 7,
                    lease_expire_at_ms: Some(0),
                    last_issued_tso: None,
                    issued_upper_bound: None,
                    updated_at_ms: 0,
                },
                1,
            )))
        }

        async fn create_generator(
            &self,
            _generator_id: u32,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_generator(
            &self,
            _generator_id: u32,
            _expected_revision: u64,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            let attempt = self.compare_exchange_calls.fetch_add(1, Ordering::AcqRel);
            if attempt == 0 {
                self.clock.advance(30);
                Err(TsoError::CasFailed)
            } else {
                Ok(2)
            }
        }

        async fn compare_exchange_generators(
            &self,
            _operations: &[GeneratorBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            Err(TsoError::CasFailed)
        }
    }

    impl RouteUpdateSource for RetryAdvancingLeaseStore {
        fn subscribe_route_updates(
            &self,
        ) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
    }

    #[async_trait]
    impl ControlPlaneStore for RetryAdvancingLeaseStore {}

    #[derive(Clone)]
    struct DelayedLeaseAcquireStore {
        load_calls: Arc<AtomicUsize>,
        compare_exchange_calls: Arc<AtomicUsize>,
    }

    impl DelayedLeaseAcquireStore {
        fn new() -> Self {
            Self {
                load_calls: Arc::new(AtomicUsize::new(0)),
                compare_exchange_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl TimelineAuthority for DelayedLeaseAcquireStore {
        async fn load_timeline(
            &self,
            _timeline_key: &str,
        ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
            Ok(None)
        }

        async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
            Ok(Vec::new())
        }

        async fn create_timeline(
            &self,
            _timeline_key: &str,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timeline(
            &self,
            _timeline_key: &str,
            _expected_revision: u64,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timelines(
            &self,
            _operations: &[TimelineBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl GeneratorLeaseAuthority for DelayedLeaseAcquireStore {
        async fn load_generator(
            &self,
            generator_id: u32,
        ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
            self.load_calls.fetch_add(1, Ordering::AcqRel);
            Ok(Some((
                GeneratorRecord {
                    schema_version: 1,
                    generator_id,
                    owner_worker_endpoint: "remote-endpoint".into(),
                    owner_instance_id: "remote-instance".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(0),
                    last_issued_tso: None,
                    issued_upper_bound: None,
                    updated_at_ms: 0,
                },
                1,
            )))
        }

        async fn create_generator(
            &self,
            _generator_id: u32,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_generator(
            &self,
            _generator_id: u32,
            _expected_revision: u64,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            self.compare_exchange_calls.fetch_add(1, Ordering::AcqRel);
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(2)
        }

        async fn compare_exchange_generators(
            &self,
            _operations: &[GeneratorBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            Ok(Vec::new())
        }
    }

    impl RouteUpdateSource for DelayedLeaseAcquireStore {
        fn subscribe_route_updates(
            &self,
        ) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
    }

    #[async_trait]
    impl ControlPlaneStore for DelayedLeaseAcquireStore {}

    #[derive(Clone)]
    struct SlowLoadAdvancingLeaseStore {
        clock: Arc<ManualClock>,
        load_calls: Arc<AtomicUsize>,
    }

    impl SlowLoadAdvancingLeaseStore {
        fn new(clock: Arc<ManualClock>) -> Self {
            Self {
                clock,
                load_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl TimelineAuthority for SlowLoadAdvancingLeaseStore {
        async fn load_timeline(
            &self,
            _timeline_key: &str,
        ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
            Ok(None)
        }

        async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
            Ok(Vec::new())
        }

        async fn create_timeline(
            &self,
            _timeline_key: &str,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timeline(
            &self,
            _timeline_key: &str,
            _expected_revision: u64,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timelines(
            &self,
            _operations: &[TimelineBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl GeneratorLeaseAuthority for SlowLoadAdvancingLeaseStore {
        async fn load_generator(
            &self,
            generator_id: u32,
        ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
            let attempt = self.load_calls.fetch_add(1, Ordering::AcqRel);
            if attempt == 0 {
                self.clock.advance(30);
            }

            Ok(Some((
                GeneratorRecord {
                    schema_version: 1,
                    generator_id,
                    owner_worker_endpoint: "remote-endpoint".into(),
                    owner_instance_id: "remote-instance".into(),
                    generator_lease_token: 9,
                    lease_expire_at_ms: Some(0),
                    last_issued_tso: None,
                    issued_upper_bound: None,
                    updated_at_ms: 0,
                },
                1,
            )))
        }

        async fn create_generator(
            &self,
            _generator_id: u32,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_generator(
            &self,
            _generator_id: u32,
            _expected_revision: u64,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            Ok(2)
        }

        async fn compare_exchange_generators(
            &self,
            _operations: &[GeneratorBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            Ok(Vec::new())
        }
    }

    impl RouteUpdateSource for SlowLoadAdvancingLeaseStore {
        fn subscribe_route_updates(
            &self,
        ) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
    }

    #[async_trait]
    impl ControlPlaneStore for SlowLoadAdvancingLeaseStore {}

    #[tokio::test]
    async fn ensure_generator_lease_retries_are_bounded_under_metadata_contention() {
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            Arc::new(ManualClock::new(50_000)),
            Arc::new(AlwaysCasFailLeaseStore),
        )
        .unwrap();

        service.background.begin_shutdown();
        service.background.drain_tasks().await;

        let result = timeout(
            Duration::from_millis(200),
            service.ensure_generator_lease(0),
        )
        .await
        .expect("ensure_generator_lease should stop retrying within the request budget")
        .expect_err("contention exhaustion should surface as CasFailed");
        assert!(matches!(result, TsoError::CasFailed));
    }

    #[tokio::test]
    async fn local_takeover_failure_keeps_active_drift() {
        let mut failure_config = required_test_config(TsoConfig::default());
        failure_config.advertise_endpoint = "remote-endpoint:50051".into();
        failure_config.instance_id = "remote-instance".into();
        let failed_service = TsoService::new(
            failure_config,
            Arc::new(ManualClock::new(50_000)),
            Arc::new(AlwaysCasFailLeaseStore),
        )
        .unwrap();
        failed_service.background.begin_shutdown();
        failed_service.background.drain_tasks().await;
        let failed_sink = Arc::new(TestReadinessSink::default());
        failed_service.set_worker_readiness_sink(failed_sink.clone());
        failed_service.observe_contended_local_generator_ownership_drift(0, "contender", 10, 0);
        failed_service.observe_contended_local_generator_ownership_drift(0, "contender", 20, 1);
        assert!(failed_service.ownership_drift.is_active(0));

        let cancellation = crate::plane::RequestCancellation::new();
        cancellation.cancel();
        assert!(matches!(
            failed_service
                .ensure_generator_lease_with_cancellation(0, Some(cancellation))
                .await,
            Err(TsoError::RequestCancelled)
        ));
        assert!(failed_service.ownership_drift.is_active(0));
        let error = failed_service.ensure_generator_lease(0).await.unwrap_err();
        assert!(matches!(error, TsoError::CasFailed));
        assert!(failed_service.ownership_drift.is_active(0));
        assert_eq!(failed_sink.cleared.load(Ordering::Acquire), 0);
        assert_eq!(
            failed_service.next_generator_with_ownership_drift(50_000),
            Some(0)
        );
    }

    #[tokio::test]
    async fn ensure_generator_lease_reloads_now_ms_between_retries() {
        let clock = Arc::new(ManualClock::new(100));
        let metadata = Arc::new(RetryAdvancingLeaseStore::new(clock.clone()));
        let service = TsoService::new(
            required_test_config(TsoConfig {
                grpc_request_timeout_ms: Some(100),
                generator_lease_ttl_ms: 50,
                lease_ttl_ms: 50,
                generator_maintenance_interval_ms: 10,
                ..TsoConfig::default()
            }),
            clock,
            metadata.clone(),
        )
        .unwrap();

        service.background.begin_shutdown();
        service.background.drain_tasks().await;

        service.ensure_generator_lease(0).await.unwrap();

        let lease_state = service
            .generator_runtime
            .lease_state(0)
            .expect("generator lease should be cached after success");
        assert_eq!(lease_state.revision, 2);
        assert_eq!(lease_state.lease_expire_at_ms, 180);
        assert_eq!(
            metadata.compare_exchange_calls.load(Ordering::Acquire),
            2,
            "test should force one retry before succeeding"
        );
    }

    #[tokio::test]
    async fn ensure_generator_lease_reloads_now_ms_after_slow_load() {
        let clock = Arc::new(ManualClock::new(100));
        let metadata = Arc::new(SlowLoadAdvancingLeaseStore::new(clock.clone()));
        let service = TsoService::new(
            required_test_config(TsoConfig {
                grpc_request_timeout_ms: Some(100),
                generator_lease_ttl_ms: 50,
                lease_ttl_ms: 50,
                generator_maintenance_interval_ms: 10,
                ..TsoConfig::default()
            }),
            clock,
            metadata.clone(),
        )
        .unwrap();

        service.background.begin_shutdown();
        service.background.drain_tasks().await;

        service.ensure_generator_lease(0).await.unwrap();

        let lease_state = service
            .generator_runtime
            .lease_state(0)
            .expect("generator lease should be cached after success");
        assert_eq!(lease_state.revision, 2);
        assert_eq!(lease_state.lease_expire_at_ms, 180);
        assert_eq!(
            metadata.load_calls.load(Ordering::Acquire),
            1,
            "test should exercise a single slow load without retries"
        );
    }

    #[derive(Clone)]
    struct FallbackAdvancingLeaseStore {
        clock: Arc<ManualClock>,
        compare_exchange_calls: Arc<AtomicUsize>,
    }

    impl FallbackAdvancingLeaseStore {
        fn new(clock: Arc<ManualClock>) -> Self {
            Self {
                clock,
                compare_exchange_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl TimelineAuthority for FallbackAdvancingLeaseStore {
        async fn load_timeline(
            &self,
            _timeline_key: &str,
        ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
            Ok(None)
        }

        async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
            Ok(Vec::new())
        }

        async fn create_timeline(
            &self,
            _timeline_key: &str,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timeline(
            &self,
            _timeline_key: &str,
            _expected_revision: u64,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timelines(
            &self,
            _operations: &[TimelineBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl GeneratorLeaseAuthority for FallbackAdvancingLeaseStore {
        async fn load_generator(
            &self,
            generator_id: u32,
        ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
            Ok(Some((
                GeneratorRecord {
                    schema_version: 1,
                    generator_id,
                    owner_worker_endpoint: "127.0.0.1:50051".into(),
                    owner_instance_id: "lease-instance".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(150),
                    last_issued_tso: None,
                    issued_upper_bound: None,
                    updated_at_ms: 100,
                },
                1,
            )))
        }

        async fn create_generator(
            &self,
            _generator_id: u32,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_generator(
            &self,
            _generator_id: u32,
            _expected_revision: u64,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            self.compare_exchange_calls.fetch_add(1, Ordering::AcqRel);
            Ok(2)
        }

        async fn compare_exchange_generators(
            &self,
            _operations: &[GeneratorBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            self.clock.advance(30);
            Err(TsoError::CasFailed)
        }
    }

    impl RouteUpdateSource for FallbackAdvancingLeaseStore {
        fn subscribe_route_updates(
            &self,
        ) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
    }

    #[async_trait]
    impl ControlPlaneStore for FallbackAdvancingLeaseStore {}

    #[derive(Clone, Default)]
    struct ConflictIsolatingLeaseStore {
        batch_calls: Arc<StdMutex<Vec<Vec<u32>>>>,
        single_calls: Arc<StdMutex<Vec<u32>>>,
    }

    #[async_trait]
    impl TimelineAuthority for ConflictIsolatingLeaseStore {
        async fn load_timeline(
            &self,
            _timeline_key: &str,
        ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
            Ok(None)
        }

        async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
            Ok(Vec::new())
        }

        async fn create_timeline(
            &self,
            _timeline_key: &str,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timeline(
            &self,
            _timeline_key: &str,
            _expected_revision: u64,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timelines(
            &self,
            _operations: &[TimelineBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl GeneratorLeaseAuthority for ConflictIsolatingLeaseStore {
        async fn load_generator(
            &self,
            generator_id: u32,
        ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
            Ok(Some((
                GeneratorRecord {
                    schema_version: 1,
                    generator_id,
                    owner_worker_endpoint: "127.0.0.1:50051".into(),
                    owner_instance_id: "lease-instance".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(150),
                    last_issued_tso: None,
                    issued_upper_bound: None,
                    updated_at_ms: 100,
                },
                1,
            )))
        }

        async fn create_generator(
            &self,
            _generator_id: u32,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_generator(
            &self,
            generator_id: u32,
            expected_revision: u64,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            self.single_calls.lock().unwrap().push(generator_id);
            Ok(expected_revision + 1)
        }

        async fn compare_exchange_generators(
            &self,
            operations: &[GeneratorBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            let generator_ids = operations
                .iter()
                .map(|operation| operation.generator_id)
                .collect::<Vec<_>>();
            self.batch_calls.lock().unwrap().push(generator_ids.clone());
            if generator_ids.contains(&1) {
                Err(TsoError::CasFailed)
            } else {
                Ok(operations
                    .iter()
                    .map(|operation| operation.previous_revision + 1)
                    .collect())
            }
        }
    }

    impl RouteUpdateSource for ConflictIsolatingLeaseStore {
        fn subscribe_route_updates(
            &self,
        ) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
    }

    #[async_trait]
    impl ControlPlaneStore for ConflictIsolatingLeaseStore {}

    #[tokio::test]
    async fn refresh_generator_leases_batch_reloads_now_ms_for_fallback_refreshes() {
        let clock = Arc::new(ManualClock::new(100));
        let metadata = Arc::new(FallbackAdvancingLeaseStore::new(clock.clone()));
        let service = TsoService::new(
            required_test_config(TsoConfig {
                generator_lease_ttl_ms: 50,
                lease_ttl_ms: 50,
                generator_maintenance_interval_ms: 10,
                ..TsoConfig::default()
            }),
            clock,
            metadata.clone(),
        )
        .unwrap();

        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        service
            .generator_runtime
            .mark_generator_ready_for_lease(0, 1);
        service.generator_runtime.upsert_lease(
            0,
            crate::runtime::GeneratorLeaseState {
                revision: 1,
                owner_instance_id: "lease-instance".into(),
                generator_lease_token: 1,
                lease_expire_at_ms: 150,
                last_persisted_tso: None,
                issued_upper_bound: None,
            },
        );

        service.refresh_generator_leases_batch(vec![0]).await;

        let lease_state = service.generator_runtime.lease_state(0).unwrap();
        assert_eq!(lease_state.lease_expire_at_ms, 180);
        assert_eq!(metadata.compare_exchange_calls.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn refresh_generator_leases_batch_isolates_one_conflicting_generator() {
        let metadata = Arc::new(ConflictIsolatingLeaseStore::default());
        let service = TsoService::new(
            required_test_config(TsoConfig {
                generator_lease_ttl_ms: 50,
                lease_ttl_ms: 50,
                generator_maintenance_interval_ms: 10,
                ..TsoConfig::default()
            }),
            Arc::new(ManualClock::new(100)),
            metadata.clone(),
        )
        .unwrap();

        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        for generator_id in 0..4 {
            service
                .generator_runtime
                .mark_generator_ready_for_lease(generator_id, 1);
            service.generator_runtime.upsert_lease(
                generator_id,
                crate::runtime::GeneratorLeaseState {
                    revision: 1,
                    owner_instance_id: "lease-instance".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: 150,
                    last_persisted_tso: None,
                    issued_upper_bound: None,
                },
            );
        }

        service
            .refresh_generator_leases_batch(vec![0, 1, 2, 3])
            .await;

        assert_eq!(*metadata.single_calls.lock().unwrap(), vec![1]);
        let batch_calls = metadata.batch_calls.lock().unwrap();
        assert_eq!(batch_calls.first(), Some(&vec![0, 1, 2, 3]));
        assert!(batch_calls
            .iter()
            .any(|ids| ids.len() > 1 && !ids.contains(&1)));
        for generator_id in 0..4 {
            assert_eq!(
                service
                    .generator_runtime
                    .lease_state(generator_id)
                    .unwrap()
                    .revision,
                2
            );
        }
    }

    #[tokio::test]
    async fn default_horizon_is_refreshed_before_allocation_stales_a_batch() {
        let clock = Arc::new(ManualClock::new(1_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let config = TsoConfig::default();
        assert_eq!(config.generator_maintenance_interval_ms, 200);
        assert_eq!(config.pre_borrow_ms, 100);
        let service = TsoService::new(
            required_test_config(config),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();

        // Let the interval's immediate first tick observe an empty runtime before acquiring a lease.
        sleep(Duration::from_millis(10)).await;
        let route = service
            .ensure_timeline("default-horizon-cadence-red")
            .await
            .unwrap();
        let (initial_record, initial_revision) = metadata
            .load_generator(route.generator_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            initial_record
                .issued_upper_bound
                .map(decode_tso)
                .map(|tso| tso.physical_ms),
            Some(1_100)
        );

        clock.set(1_050);
        sleep(Duration::from_millis(70)).await;
        let (before_allocation, before_allocation_revision) = metadata
            .load_generator(route.generator_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            before_allocation
                .issued_upper_bound
                .map(decode_tso)
                .map(|tso| tso.physical_ms),
            Some(1_200),
            "background maintenance must extend the issued horizon before allocation"
        );
        service.background.begin_shutdown();
        service.background.drain_tasks().await;

        clock.set(1_101);
        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key,
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "default-horizon-cadence-red-request".into(),
            })
            .await
            .unwrap();
        let (_, after_allocation_revision) = metadata
            .load_generator(route.generator_id)
            .await
            .unwrap()
            .unwrap();

        let stale_batch_result = metadata
            .compare_exchange_generators(&[GeneratorBatchOp {
                generator_id: route.generator_id,
                previous_revision: before_allocation_revision,
                record: before_allocation.clone(),
            }])
            .await;
        let observed = (
            before_allocation_revision > initial_revision,
            after_allocation_revision > before_allocation_revision,
            stale_batch_result.is_ok(),
        );
        assert_eq!(
            observed,
            (true, false, true),
            "expected proactive batch refresh, no synchronous allocation CAS, and no stale batch; \
             initial={initial_record:?} before_allocation={before_allocation:?} \
             stale_batch_result={stale_batch_result:?}"
        );
    }

    #[tokio::test]
    async fn checkpoint_only_refresh_does_not_move_persisted_or_runtime_lease_state() {
        let clock = Arc::new(ManualClock::new(1_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();
        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        service.ensure_generator_lease(0).await.unwrap();

        let (before_record, before_revision) = metadata.load_generator(0).await.unwrap().unwrap();
        let before_runtime = service.generator_runtime.lease_state(0).unwrap();
        service
            .lookup_generator(0)
            .unwrap()
            .allocate_after(
                1,
                None,
                clock.now_ms(),
                service.config.max_future_borrow_ms,
                service.config.max_clock_rewind_ms,
                before_record.issued_upper_bound,
            )
            .unwrap();
        clock.set(1_010);

        service.refresh_generator_lease(0).await.unwrap();

        let (after_record, after_revision) = metadata.load_generator(0).await.unwrap().unwrap();
        let after_runtime = service.generator_runtime.lease_state(0).unwrap();
        assert_eq!(
            (
                after_revision,
                after_record.lease_expire_at_ms,
                after_record.last_issued_tso,
                after_record.issued_upper_bound,
                after_runtime.lease_expire_at_ms,
                after_runtime.last_persisted_tso,
                after_runtime.issued_upper_bound,
            ),
            (
                before_revision,
                before_record.lease_expire_at_ms,
                before_record.last_issued_tso,
                before_record.issued_upper_bound,
                before_runtime.lease_expire_at_ms,
                before_runtime.last_persisted_tso,
                before_runtime.issued_upper_bound,
            ),
            "checkpoint-only maintenance must not write or advance E/U/L"
        );
    }

    #[tokio::test]
    async fn single_and_batch_refresh_runtime_match_the_persisted_plan() {
        let clock = Arc::new(ManualClock::new(1_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();
        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        service.ensure_generator_lease(0).await.unwrap();
        service.ensure_generator_lease(1).await.unwrap();
        let (single_before, _) = metadata.load_generator(0).await.unwrap().unwrap();
        let (batch_before, _) = metadata.load_generator(1).await.unwrap().unwrap();

        clock.set(1_050);
        service
            .refresh_generator_lease_inner(0, GeneratorLeaseRefreshReason::HorizonRequired)
            .await
            .unwrap();
        service.refresh_generator_leases_batch(vec![1]).await;

        let (single_record, single_revision) = metadata.load_generator(0).await.unwrap().unwrap();
        let (batch_record, batch_revision) = metadata.load_generator(1).await.unwrap().unwrap();
        assert_eq!(
            single_record.lease_expire_at_ms,
            single_before.lease_expire_at_ms
        );
        assert_eq!(
            batch_record.lease_expire_at_ms,
            batch_before.lease_expire_at_ms
        );
        assert_ne!(
            single_record.issued_upper_bound,
            single_before.issued_upper_bound
        );
        assert_ne!(
            batch_record.issued_upper_bound,
            batch_before.issued_upper_bound
        );
        assert_runtime_matches_record(&service, 0, &single_record, single_revision);
        assert_runtime_matches_record(&service, 1, &batch_record, batch_revision);
    }

    #[tokio::test]
    async fn refresh_generator_leases_batch_keeps_expired_lease_tracked() {
        let metadata = Arc::new(MemoryMetadataStore::new());
        metadata
            .create_generator(
                0,
                &GeneratorRecord {
                    schema_version: 1,
                    generator_id: 0,
                    owner_worker_endpoint: "127.0.0.1:50051".into(),
                    owner_instance_id: "lease-instance".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(90),
                    last_issued_tso: None,
                    issued_upper_bound: None,
                    updated_at_ms: 50,
                },
            )
            .await
            .unwrap();
        let service = TsoService::new(
            required_test_config(TsoConfig {
                generator_lease_ttl_ms: 50,
                lease_ttl_ms: 50,
                generator_maintenance_interval_ms: 10,
                safety_gap_ms: 50,
                ..TsoConfig::default()
            }),
            Arc::new(ManualClock::new(100)),
            metadata,
        )
        .unwrap();

        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        service
            .generator_runtime
            .mark_generator_ready_for_lease(0, 1);
        service.generator_runtime.upsert_lease(
            0,
            crate::runtime::GeneratorLeaseState {
                revision: 1,
                owner_instance_id: "lease-instance".into(),
                generator_lease_token: 1,
                lease_expire_at_ms: 90,
                last_persisted_tso: None,
                issued_upper_bound: None,
            },
        );

        service.refresh_generator_leases_batch(vec![0]).await;

        assert!(service.generator_runtime.lease_state(0).is_some());
        assert_eq!(service.generator_runtime.lease_keys(), vec![0]);
    }

    #[tokio::test]
    async fn failed_conflict_reload_keeps_lease_tracked_but_not_ready() {
        let service = TsoService::new(
            required_test_config(TsoConfig {
                generator_lease_ttl_ms: 50,
                lease_ttl_ms: 50,
                generator_maintenance_interval_ms: 10,
                grpc_request_timeout_ms: Some(1),
                ..TsoConfig::default()
            }),
            Arc::new(ManualClock::new(100)),
            Arc::new(AlwaysCasFailLeaseStore),
        )
        .unwrap();

        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        service
            .generator_runtime
            .mark_generator_ready_for_lease(0, 1);
        service.generator_runtime.upsert_lease(
            0,
            crate::runtime::GeneratorLeaseState {
                revision: 1,
                owner_instance_id: "lease-instance".into(),
                generator_lease_token: 1,
                lease_expire_at_ms: 150,
                last_persisted_tso: None,
                issued_upper_bound: None,
            },
        );

        let error = service
            .refresh_generator_lease_inner(0, GeneratorLeaseRefreshReason::HorizonRequired)
            .await
            .unwrap_err();

        assert!(matches!(error, TsoError::CasFailed));
        assert!(service.generator_runtime.lease_state(0).is_some());
        assert!(!service.is_generator_lease_valid(0, 100));
    }

    #[tokio::test]
    async fn refresh_wrapper_reloads_now_ms_after_waiting_for_singleflight() {
        let metadata = Arc::new(MemoryMetadataStore::new());
        metadata
            .create_generator(
                0,
                &GeneratorRecord {
                    schema_version: 1,
                    generator_id: 0,
                    owner_worker_endpoint: "127.0.0.1:50051".into(),
                    owner_instance_id: "lease-instance".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(300),
                    last_issued_tso: None,
                    issued_upper_bound: None,
                    updated_at_ms: 100,
                },
            )
            .await
            .unwrap();
        let clock = Arc::new(ManualClock::new(100));
        let service = TsoService::new(
            required_test_config(TsoConfig {
                generator_lease_ttl_ms: 50,
                lease_ttl_ms: 50,
                generator_maintenance_interval_ms: 10,
                ..TsoConfig::default()
            }),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();

        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        service
            .generator_runtime
            .mark_generator_ready_for_lease(0, 1);
        service.generator_runtime.upsert_lease(
            0,
            crate::runtime::GeneratorLeaseState {
                revision: 1,
                owner_instance_id: "lease-instance".into(),
                generator_lease_token: 1,
                lease_expire_at_ms: 300,
                last_persisted_tso: None,
                issued_upper_bound: None,
            },
        );

        let held_flight = service
            .acquire_generator_lease_singleflight_with_cancellation(0, None)
            .await
            .unwrap();
        let refresh =
            service.refresh_generator_lease_inner(0, GeneratorLeaseRefreshReason::HorizonRequired);
        tokio::pin!(refresh);
        tokio::select! {
            result = &mut refresh => panic!("refresh unexpectedly bypassed held flight: {result:?}"),
            _ = sleep(Duration::from_millis(20)) => {}
        }
        clock.set(125);
        drop(held_flight);
        timeout(Duration::from_millis(100), &mut refresh)
            .await
            .expect("refresh should proceed after the held flight releases")
            .unwrap();

        let (record, _) = metadata.load_generator(0).await.unwrap().unwrap();
        assert_eq!(record.updated_at_ms, 125);
        assert_eq!(record.lease_expire_at_ms, Some(300));
        assert_eq!(
            record
                .issued_upper_bound
                .map(decode_tso)
                .map(|tso| tso.physical_ms),
            Some(225)
        );
        let runtime = service.generator_runtime.lease_state(0).unwrap();
        assert_eq!(runtime.lease_expire_at_ms, 300);
    }

    #[tokio::test]
    async fn force_refresh_conflict_does_not_reacquire_held_singleflight() {
        let metadata = Arc::new(MemoryMetadataStore::new());
        let old_upper_bound = encode_tso(120, 0, 0).unwrap();
        let authoritative_upper_bound = encode_tso(140, 0, 0).unwrap();
        metadata
            .create_generator(
                0,
                &GeneratorRecord {
                    schema_version: 1,
                    generator_id: 0,
                    owner_worker_endpoint: "127.0.0.1:50051".into(),
                    owner_instance_id: "lease-instance".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(150),
                    last_issued_tso: None,
                    issued_upper_bound: Some(old_upper_bound),
                    updated_at_ms: 100,
                },
            )
            .await
            .unwrap();
        let service = TsoService::new(
            required_test_config(TsoConfig {
                generator_lease_ttl_ms: 50,
                lease_ttl_ms: 50,
                generator_maintenance_interval_ms: 10,
                ..TsoConfig::default()
            }),
            Arc::new(ManualClock::new(100)),
            metadata.clone(),
        )
        .unwrap();

        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        service
            .generator_runtime
            .mark_generator_ready_for_lease(0, 1);
        service.generator_runtime.upsert_lease(
            0,
            crate::runtime::GeneratorLeaseState {
                revision: 1,
                owner_instance_id: "lease-instance".into(),
                generator_lease_token: 1,
                lease_expire_at_ms: 150,
                last_persisted_tso: None,
                issued_upper_bound: Some(old_upper_bound),
            },
        );
        let (mut authoritative, revision) = metadata.load_generator(0).await.unwrap().unwrap();
        authoritative.lease_expire_at_ms = Some(180);
        authoritative.issued_upper_bound = Some(authoritative_upper_bound);
        authoritative.updated_at_ms = 130;
        let authoritative_revision = metadata
            .compare_exchange_generator(0, revision, &authoritative)
            .await
            .unwrap();

        let nested_refresh = timeout(
            Duration::from_millis(100),
            service.refresh_generator_lease_if_unchanged_with_cancellation(
                0,
                Some(old_upper_bound),
                None,
            ),
        )
        .await;
        timeout(
            Duration::from_millis(100),
            service.ensure_generator_lease(0),
        )
        .await
        .expect("the same metadata store must support a non-nested reload")
        .unwrap();
        assert!(
            nested_refresh.is_ok(),
            "CAS conflict reload must not wait on its already-held singleflight"
        );
        nested_refresh.unwrap().unwrap();

        let reloaded = service.generator_runtime.lease_state(0).unwrap();
        assert_eq!(reloaded.revision, authoritative_revision);
        assert_eq!(reloaded.lease_expire_at_ms, 180);
        assert_eq!(reloaded.issued_upper_bound, Some(authoritative_upper_bound));
    }

    #[tokio::test]
    async fn force_refresh_if_unchanged_coalesces_upper_bound_refreshes() {
        let metadata = Arc::new(DelayedLeaseAcquireStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig {
                generator_lease_ttl_ms: 50,
                lease_ttl_ms: 50,
                generator_maintenance_interval_ms: 10,
                ..TsoConfig::default()
            }),
            Arc::new(ManualClock::new(70_000)),
            metadata.clone(),
        )
        .unwrap();

        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        service
            .generator_runtime
            .mark_generator_ready_for_lease(0, 1);
        service.generator_runtime.upsert_lease(
            0,
            crate::runtime::GeneratorLeaseState {
                revision: 1,
                owner_instance_id: "lease-instance".into(),
                generator_lease_token: 1,
                lease_expire_at_ms: 70_050,
                last_persisted_tso: None,
                issued_upper_bound: Some(1),
            },
        );

        let service_a = service.clone();
        let service_b = service.clone();
        let task_a = tokio::spawn(async move {
            service_a
                .refresh_generator_lease_if_unchanged_with_cancellation(0, Some(1), None)
                .await
        });
        let task_b = tokio::spawn(async move {
            service_b
                .refresh_generator_lease_if_unchanged_with_cancellation(0, Some(1), None)
                .await
        });

        task_a.await.unwrap().unwrap();
        task_b.await.unwrap().unwrap();

        assert_eq!(metadata.compare_exchange_calls.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn concurrent_generator_lease_ensure_coalesces_remote_refresh() {
        let metadata = Arc::new(DelayedLeaseAcquireStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            Arc::new(ManualClock::new(70_000)),
            metadata.clone(),
        )
        .unwrap();

        service.background.begin_shutdown();
        service.background.drain_tasks().await;

        let service_a = service.clone();
        let service_b = service.clone();
        let task_a = tokio::spawn(async move { service_a.ensure_generator_lease(0).await });
        let task_b = tokio::spawn(async move { service_b.ensure_generator_lease(0).await });

        task_a.await.unwrap().unwrap();
        task_b.await.unwrap().unwrap();

        assert_eq!(metadata.load_calls.load(Ordering::Acquire), 1);
        assert_eq!(metadata.compare_exchange_calls.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn contested_local_endpoint_does_not_trigger_drift_without_renewal() {
        let clock = Arc::new(ManualClock::new(100));
        let metadata = Arc::new(MemoryMetadataStore::new());
        metadata
            .create_generator(
                0,
                &GeneratorRecord {
                    schema_version: 1,
                    generator_id: 0,
                    owner_worker_endpoint: "127.0.0.1:50051".into(),
                    owner_instance_id: "other-instance".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(200),
                    last_issued_tso: None,
                    issued_upper_bound: None,
                    updated_at_ms: 100,
                },
            )
            .await
            .unwrap();
        let service =
            TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();
        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        let sink = Arc::new(TestReadinessSink::default());
        service.set_worker_readiness_sink(sink.clone());

        let error = service.ensure_generator_lease(0).await.unwrap_err();

        assert!(matches!(error, TsoError::NotGeneratorOwner { .. }));
        assert!(sink.started.lock().unwrap().is_empty());
        assert_eq!(sink.cleared.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn contested_local_endpoint_triggers_drift_after_expiry_advances() {
        let clock = Arc::new(ManualClock::new(100));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let record = GeneratorRecord {
            schema_version: 1,
            generator_id: 0,
            owner_worker_endpoint: "127.0.0.1:50051".into(),
            owner_instance_id: "other-instance".into(),
            generator_lease_token: 1,
            lease_expire_at_ms: Some(200),
            last_issued_tso: None,
            issued_upper_bound: None,
            updated_at_ms: 100,
        };
        let revision = metadata.create_generator(0, &record).await.unwrap();
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();
        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        let sink = Arc::new(TestReadinessSink::default());
        service.set_worker_readiness_sink(sink.clone());

        let _ = service.ensure_generator_lease(0).await.unwrap_err();
        let mut renewed = record.clone();
        renewed.generator_lease_token = 2;
        renewed.lease_expire_at_ms = Some(240);
        renewed.updated_at_ms = 120;
        metadata
            .compare_exchange_generator(0, revision, &renewed)
            .await
            .unwrap();

        let error = service.ensure_generator_lease(0).await.unwrap_err();

        assert!(matches!(error, TsoError::NotGeneratorOwner { .. }));
        let started = sink.started.lock().unwrap();
        assert_eq!(started.len(), 1);
        assert_eq!(started[0].generator_id, 0);
        assert_eq!(started[0].contending_instance_id, "other-instance");
    }

    #[tokio::test]
    async fn drift_clears_after_local_lease_is_acquired() {
        let clock = Arc::new(ManualClock::new(100));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let record = GeneratorRecord {
            schema_version: 1,
            generator_id: 0,
            owner_worker_endpoint: "127.0.0.1:50051".into(),
            owner_instance_id: "other-instance".into(),
            generator_lease_token: 1,
            lease_expire_at_ms: Some(200),
            last_issued_tso: None,
            issued_upper_bound: None,
            updated_at_ms: 100,
        };
        let revision = metadata.create_generator(0, &record).await.unwrap();
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();
        service.background.begin_shutdown();
        service.background.drain_tasks().await;
        let sink = Arc::new(TestReadinessSink::default());
        service.set_worker_readiness_sink(sink.clone());

        let _ = service.ensure_generator_lease(0).await.unwrap_err();
        let mut renewed = record.clone();
        renewed.generator_lease_token = 2;
        renewed.lease_expire_at_ms = Some(240);
        renewed.updated_at_ms = 120;
        let renewed_revision = metadata
            .compare_exchange_generator(0, revision, &renewed)
            .await
            .unwrap();
        let _ = service.ensure_generator_lease(0).await.unwrap_err();
        clock.set(300);

        service.ensure_generator_lease(0).await.unwrap();

        {
            let started = sink.started.lock().unwrap();
            assert_eq!(started.len(), 1);
        }
        assert_eq!(sink.cleared.load(Ordering::Acquire), 1);
        let (owned, owned_revision) = metadata.load_generator(0).await.unwrap().unwrap();
        assert!(owned_revision > renewed_revision);
        assert_eq!(owned.owner_instance_id, "lease-instance");
        assert_eq!(owned.owner_worker_endpoint, "127.0.0.1:50051");
    }

    #[tokio::test]
    async fn background_retry_reacquires_drifted_generator_and_clears_signal() {
        let clock = Arc::new(ManualClock::new(100));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let record = GeneratorRecord {
            schema_version: 1,
            generator_id: 0,
            owner_worker_endpoint: "127.0.0.1:50051".into(),
            owner_instance_id: "other-instance".into(),
            generator_lease_token: 1,
            lease_expire_at_ms: Some(200),
            last_issued_tso: None,
            issued_upper_bound: None,
            updated_at_ms: 100,
        };
        let revision = metadata.create_generator(0, &record).await.unwrap();
        let service = TsoService::new(
            required_test_config(TsoConfig {
                generator_maintenance_interval_ms: 10,
                ..TsoConfig::default()
            }),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();
        let sink = Arc::new(TestReadinessSink::default());
        service.set_worker_readiness_sink(sink.clone());

        let _ = service.ensure_generator_lease(0).await.unwrap_err();
        let mut renewed = record.clone();
        renewed.generator_lease_token = 2;
        renewed.lease_expire_at_ms = Some(240);
        renewed.updated_at_ms = 120;
        metadata
            .compare_exchange_generator(0, revision, &renewed)
            .await
            .unwrap();
        let _ = service.ensure_generator_lease(0).await.unwrap_err();
        clock.set(300);

        timeout(Duration::from_millis(200), async {
            loop {
                if sink.cleared.load(Ordering::Acquire) > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background retry should clear drift after reacquire");

        let (owned, _) = metadata.load_generator(0).await.unwrap().unwrap();
        assert_eq!(owned.owner_instance_id, "lease-instance");
        service.background.begin_shutdown();
        service.background.drain_tasks().await;
    }
}
