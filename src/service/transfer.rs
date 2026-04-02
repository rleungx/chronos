use std::sync::Arc;

mod telemetry;
mod validation;

use tokio::sync::Mutex;

use crate::metadata::TimelineRecord;
use crate::planning::{
    generator_recovery_floor_tso, resource_tier_for_generator_id, safe_floor_for_transfer_reason,
    should_release_claimed_dedicated_generator,
    should_release_previous_dedicated_generator_after_route_change, TransferPlan,
};
use crate::timeline_state::build_timeline_state;
use crate::{ResourceTier, TimelineLifecycleState, TimelineRoute, TransferReason, TsoError};
use telemetry::{
    log_failover_blocked, log_transfer_completed, log_transfer_failed,
    log_transfer_recovery_floor_computed, log_transfer_requested, FailoverBlocker,
};
use validation::validate_transfer_target;

use super::TsoService;

type ClaimedDedicated = Option<(u32, bool)>;

struct TransferPreparation {
    previous_route: TimelineRoute,
    old_generator_id: u32,
    safe_floor: Option<u64>,
    now: u64,
}

struct TransferTargetSelection {
    target_resource_tier: ResourceTier,
    new_generator_id: u32,
    claimed_dedicated: ClaimedDedicated,
}

struct TransferCompletion<'a> {
    previous_route: &'a TimelineRoute,
    transfer: &'a TransferPlan,
    target_resource_tier: ResourceTier,
    new_generator_id: u32,
    safe_floor: Option<u64>,
}

impl TsoService {
    fn release_claimed_dedicated_if_needed(
        &self,
        timeline_key: &str,
        previous_route: &TimelineRoute,
        claimed_dedicated: ClaimedDedicated,
    ) {
        if let Some((generator_id, claimed)) = claimed_dedicated {
            if should_release_claimed_dedicated_generator(
                claimed,
                previous_route.resource_tier,
                previous_route.generator_id,
                generator_id,
            ) {
                self.release_dedicated(generator_id, timeline_key);
            }
        }
    }

    async fn prepare_transfer_application(
        &self,
        timeline_key: &str,
        record: &mut TimelineRecord,
        transfer: &TransferPlan,
    ) -> Result<TransferPreparation, TsoError> {
        let previous_route = record.route.clone();
        let old_generator_id = previous_route.generator_id;
        let now = self.clock.now_ms();
        let previous_generator_record = self.metadata.load_generator(old_generator_id).await?;
        let previous_generator_floor_tso = previous_generator_record
            .as_ref()
            .and_then(|(generator_record, _)| generator_recovery_floor_tso(generator_record));

        if transfer.reason == TransferReason::Failover {
            if let Some((generator_record, _)) = previous_generator_record.as_ref() {
                let exp = generator_record.lease_expire_at_ms.unwrap_or(0);
                if !crate::lease_expired_with_safety_gap(exp, now, self.config.safety_gap_ms) {
                    let blocker = FailoverBlocker::LeaseNotExpired;
                    log_failover_blocked(timeline_key, old_generator_id, Some(exp), blocker);
                    return Err(TsoError::FailoverRequiresExpiredLease {
                        timeline_key: timeline_key.to_owned(),
                        lease_expire_at_ms: exp,
                    });
                }
            }
            if previous_generator_floor_tso.is_none() {
                let blocker = FailoverBlocker::RecoveryFloorMissing;
                log_failover_blocked(timeline_key, old_generator_id, None, blocker);
                return Err(TsoError::FailoverMissingRecoveryFloor {
                    timeline_key: timeline_key.to_owned(),
                    generator_id: old_generator_id,
                });
            }
        }

        if transfer.reason != TransferReason::Failover
            && self.is_local_endpoint(&previous_route.owner_worker_endpoint)
        {
            if let Some(timeline_handle) = self.timeline_runtime.timeline_handle(timeline_key) {
                let timeline = timeline_handle.lock().await;
                if let Some(last_issued) = timeline.last_issued_tso {
                    record.last_graceful_issued = Some(last_issued);
                }
            }
        }

        let safe_floor = safe_floor_for_transfer_reason(
            transfer.reason,
            record.last_graceful_issued,
            previous_generator_floor_tso,
        );
        log_transfer_recovery_floor_computed(
            timeline_key,
            old_generator_id,
            transfer.reason,
            safe_floor,
        );

        Ok(TransferPreparation {
            previous_route,
            old_generator_id,
            safe_floor,
            now,
        })
    }

    async fn select_transfer_target(
        &self,
        timeline_key: &str,
        previous_route: &TimelineRoute,
        transfer: &TransferPlan,
        safe_floor: Option<u64>,
    ) -> Result<TransferTargetSelection, TsoError> {
        let mut target_resource_tier = transfer.resource_tier;
        let mut claimed_dedicated = None;
        let mut new_generator_id = match (target_resource_tier, transfer.generator_id) {
            (_, Some(generator_id)) => {
                self.ensure_generator_id_range(generator_id)?;
                if target_resource_tier == ResourceTier::Dedicated {
                    let claimed = self.claim_specific_dedicated(generator_id, timeline_key)?;
                    claimed_dedicated = Some((generator_id, claimed));
                }
                generator_id
            }
            (ResourceTier::Dedicated, None) => {
                let (generator_id, claimed) = self.claim_dedicated(timeline_key)?;
                claimed_dedicated = Some((generator_id, claimed));
                generator_id
            }
            (_, None) => self.pick_generator_id(timeline_key, target_resource_tier)?,
        };

        if target_resource_tier != ResourceTier::Dedicated {
            if let Some(recovery_floor_tso) = safe_floor {
                let required_jump_ms = self
                    .required_jump_ms_for_generator(new_generator_id, recovery_floor_tso)
                    .await?;
                if required_jump_ms > self.config.shared_jump_ahead_threshold_ms {
                    if transfer.generator_id.is_some() {
                        let error = TsoError::SharedGeneratorJumpAheadTooLarge {
                            generator_id: new_generator_id,
                            required_jump_ms,
                            threshold_ms: self.config.shared_jump_ahead_threshold_ms,
                        };
                        log_transfer_failed(
                            timeline_key,
                            &transfer.owner_endpoint,
                            Some(new_generator_id),
                            transfer.reason,
                            &error,
                        );
                        return Err(error);
                    }
                    let (isolated_generator_id, claimed) = self.claim_dedicated(timeline_key)?;
                    claimed_dedicated = Some((isolated_generator_id, claimed));
                    new_generator_id = isolated_generator_id;
                    target_resource_tier = ResourceTier::Dedicated;
                }
            }
        }

        if self.is_local_endpoint(&transfer.owner_endpoint)
            && !self.owns_generator_id(new_generator_id)
        {
            self.release_claimed_dedicated_if_needed(
                timeline_key,
                previous_route,
                claimed_dedicated,
            );
            let error = TsoError::GeneratorNotOwnedByThisWorker {
                generator_id: new_generator_id,
                modulo: self.config.generator_ownership_modulo,
                remainder: self.config.generator_ownership_remainder,
            };
            log_transfer_failed(
                timeline_key,
                &transfer.owner_endpoint,
                Some(new_generator_id),
                transfer.reason,
                &error,
            );
            return Err(error);
        }

        Ok(TransferTargetSelection {
            target_resource_tier,
            new_generator_id,
            claimed_dedicated,
        })
    }

    async fn synchronize_transfer_completion(
        &self,
        timeline_key: &str,
        record: TimelineRecord,
        new_revision: u64,
        completion: TransferCompletion<'_>,
    ) -> Result<(TimelineRoute, TimelineLifecycleState), TsoError> {
        if should_release_previous_dedicated_generator_after_route_change(
            completion.previous_route.resource_tier,
            completion.previous_route.generator_id,
            completion.target_resource_tier,
            completion.new_generator_id,
        ) {
            self.release_dedicated(completion.previous_route.generator_id, timeline_key);
        }

        let (record, final_revision) =
            if self.is_local_endpoint(&completion.transfer.owner_endpoint) {
                self.ensure_generator_lease(completion.new_generator_id)
                    .await?;
                self.activate_local_timeline_record(timeline_key, record, new_revision)
                    .await?
            } else {
                (record, new_revision)
            };

        let route = record.route.clone();
        if self.is_local_endpoint(&completion.transfer.owner_endpoint) {
            let timeline = Arc::new(Mutex::new(build_timeline_state(
                &record,
                final_revision,
                completion.safe_floor,
            )));
            let _ = self.best_effort_insert_timeline_cache(timeline_key, timeline)?;
        } else {
            self.clear_timeline_cache(timeline_key);
            if completion.target_resource_tier == ResourceTier::Dedicated {
                self.release_dedicated(completion.new_generator_id, timeline_key);
            }
        }

        log_transfer_completed(
            timeline_key,
            completion.transfer.reason,
            completion.previous_route,
            &route,
            record.state,
        );

        Ok((route, record.state))
    }

    pub async fn transfer_timeline_for_rpc(
        &self,
        timeline_key: &str,
        target_owner_endpoint: String,
        target_generator_id: Option<u32>,
        reason: TransferReason,
    ) -> Result<(TimelineRoute, u32, TimelineLifecycleState), TsoError> {
        log_transfer_requested(
            timeline_key,
            &target_owner_endpoint,
            target_generator_id,
            reason,
        );
        if let Err(error) = validate_transfer_target(
            &self.config.advertise_endpoint,
            timeline_key,
            &target_owner_endpoint,
            target_generator_id,
        ) {
            log_transfer_failed(
                timeline_key,
                &target_owner_endpoint,
                target_generator_id,
                reason,
                &error,
            );
            return Err(error);
        }
        let (record, revision) = self
            .metadata
            .load_timeline(timeline_key)
            .await?
            .ok_or_else(|| TsoError::TimelineNotFound {
                timeline_key: timeline_key.to_owned(),
            })?;

        let resource_tier = match target_generator_id {
            Some(generator_id) => resource_tier_for_generator_id(
                generator_id,
                self.config.shared_generators,
                self.config.warm_generators,
            )?,
            None => record.route.resource_tier,
        };
        let transfer = TransferPlan {
            resource_tier,
            owner_endpoint: target_owner_endpoint,
            generator_id: target_generator_id,
            reason,
        };

        self.apply_transfer_plan(timeline_key, record, revision, transfer)
            .await
    }

    pub(super) async fn apply_transfer_plan(
        &self,
        timeline_key: &str,
        mut record: TimelineRecord,
        revision: u64,
        transfer: TransferPlan,
    ) -> Result<(TimelineRoute, u32, TimelineLifecycleState), TsoError> {
        let prepared = self
            .prepare_transfer_application(timeline_key, &mut record, &transfer)
            .await?;
        let target = self
            .select_transfer_target(
                timeline_key,
                &prepared.previous_route,
                &transfer,
                prepared.safe_floor,
            )
            .await?;

        record.route.generator_id = target.new_generator_id;
        record.route.resource_tier = target.target_resource_tier;
        record.route.epoch += 1;
        record.route.route_version += 1;
        record.route.owner_worker_endpoint = transfer.owner_endpoint.clone();
        record.state = TimelineLifecycleState::Recovering;
        record.recovery_floor_tso = prepared.safe_floor;
        record.issued_upper_bound = None;
        record.lease_expire_at_ms = None;
        record.updated_at_ms = prepared.now;

        let new_rev = match self
            .metadata
            .compare_exchange_timeline(timeline_key, revision, &record)
            .await
        {
            Ok(rev) => rev,
            Err(e) => {
                self.release_claimed_dedicated_if_needed(
                    timeline_key,
                    &prepared.previous_route,
                    target.claimed_dedicated,
                );
                if matches!(e, TsoError::CasFailed) {
                    log_transfer_failed(
                        timeline_key,
                        &transfer.owner_endpoint,
                        Some(target.new_generator_id),
                        transfer.reason,
                        &e,
                    );
                }
                return Err(e);
            }
        };
        let (route, state) = self
            .synchronize_transfer_completion(
                timeline_key,
                record,
                new_rev,
                TransferCompletion {
                    previous_route: &prepared.previous_route,
                    transfer: &transfer,
                    target_resource_tier: target.target_resource_tier,
                    new_generator_id: target.new_generator_id,
                    safe_floor: prepared.safe_floor,
                },
            )
            .await?;
        Ok((route, prepared.old_generator_id, state))
    }

    pub async fn transfer_timeline(
        &self,
        timeline_key: &str,
        target_resource_tier: ResourceTier,
        target_owner_endpoint: String,
        target_generator_id: Option<u32>,
    ) -> Result<TimelineRoute, TsoError> {
        if let Err(error) = validate_transfer_target(
            &self.config.advertise_endpoint,
            timeline_key,
            &target_owner_endpoint,
            target_generator_id,
        ) {
            log_transfer_failed(
                timeline_key,
                &target_owner_endpoint,
                target_generator_id,
                TransferReason::Manual,
                &error,
            );
            return Err(error);
        }
        log_transfer_requested(
            timeline_key,
            &target_owner_endpoint,
            target_generator_id,
            TransferReason::Manual,
        );
        let (record, revision) = self
            .metadata
            .load_timeline(timeline_key)
            .await?
            .ok_or_else(|| TsoError::TimelineNotFound {
                timeline_key: timeline_key.to_owned(),
            })?;
        let transfer = TransferPlan {
            resource_tier: target_resource_tier,
            owner_endpoint: target_owner_endpoint,
            generator_id: target_generator_id,
            reason: TransferReason::Manual,
        };
        let (route, _, _) = self
            .apply_transfer_plan(timeline_key, record, revision, transfer)
            .await?;
        Ok(route)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::metadata::MemoryMetadataStore;
    use crate::{ManualClock, ResourceTier, TsoConfig, TsoSecurityMode, TsoService};

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
    async fn local_transfer_degrades_when_runtime_cache_is_saturated() {
        let clock = Arc::new(ManualClock::new(31_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            with_worker(
                TsoConfig {
                    max_timeline_runtime_entries: 1,
                    ..TsoConfig::default()
                },
                "worker-a",
            ),
            clock,
            metadata,
        )
        .unwrap();

        let anchor = service
            .ensure_timeline("transfer-degrade.anchor")
            .await
            .unwrap();
        let busy_handle = service
            .timeline_runtime
            .timeline_handle(&anchor.timeline_key)
            .expect("anchor timeline should be cached");

        let route = service
            .ensure_timeline("transfer-degrade.target")
            .await
            .unwrap();
        service.clear_timeline_cache(&route.timeline_key);

        let degraded_before = crate::metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
            .with_label_values(&["degraded_uncached"])
            .get();

        let transferred = service
            .transfer_timeline(
                &route.timeline_key,
                ResourceTier::Shared,
                "worker-a:50051".to_string(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(transferred.timeline_key, route.timeline_key);
        assert_eq!(service.timeline_runtime.timeline_count(), 1);
        assert!(service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .is_none());
        assert!(
            crate::metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                .with_label_values(&["degraded_uncached"])
                .get()
                > degraded_before
        );

        drop(busy_handle);
    }
}
