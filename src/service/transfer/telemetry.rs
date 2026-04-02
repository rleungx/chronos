use tracing::{info, warn};

use crate::metrics;
use crate::{TimelineLifecycleState, TimelineRoute, TransferReason, TsoError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct OperatorActionEvent {
    pub(super) event: &'static str,
    pub(super) action_outcome: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TransferEventFamily {
    Requested,
    Failed,
    Completed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FailoverBlocker {
    LeaseNotExpired,
    RecoveryFloorMissing,
}

impl FailoverBlocker {
    pub(super) fn action_blocker(self) -> &'static str {
        match self {
            Self::LeaseNotExpired => "LeaseNotExpired",
            Self::RecoveryFloorMissing => "RecoveryFloorMissing",
        }
    }

    pub(super) fn next_step(self) -> &'static str {
        match self {
            Self::LeaseNotExpired => "WaitForLeaseExpiry",
            Self::RecoveryFloorMissing => "PersistRecoveryFloor",
        }
    }
}

pub(super) fn operator_action_event(
    reason: TransferReason,
    family: TransferEventFamily,
) -> OperatorActionEvent {
    match family {
        TransferEventFamily::Requested => OperatorActionEvent {
            event: "transfer_requested",
            action_outcome: "started",
        },
        TransferEventFamily::Failed => OperatorActionEvent {
            event: "transfer_failed",
            action_outcome: "failed",
        },
        TransferEventFamily::Completed => OperatorActionEvent {
            event: if reason == TransferReason::Failover {
                "failover_completed"
            } else {
                "cutover_committed"
            },
            action_outcome: "succeeded",
        },
    }
}

pub(super) fn failover_blocked_event(blocker: FailoverBlocker) -> OperatorActionEvent {
    OperatorActionEvent {
        event: match blocker {
            FailoverBlocker::LeaseNotExpired => "failover_blocked_lease_not_expired",
            FailoverBlocker::RecoveryFloorMissing => "failover_blocked_recovery_floor_missing",
        },
        action_outcome: "blocked",
    }
}

pub(super) fn transfer_action_kind(reason: TransferReason) -> &'static str {
    match reason {
        TransferReason::Failover => "failover",
        TransferReason::Manual | TransferReason::Rebalance | TransferReason::Hotspot => "transfer",
    }
}

pub(super) fn transfer_reason_label(reason: TransferReason) -> &'static str {
    match reason {
        TransferReason::Rebalance => "rebalance",
        TransferReason::Hotspot => "hotspot",
        TransferReason::Failover => "failover",
        TransferReason::Manual => "manual",
    }
}

pub(super) fn record_transfer_outcome(reason: TransferReason, outcome: &'static str) {
    metrics::TSO_TRANSFER_OUTCOMES_TOTAL
        .with_label_values(&[
            transfer_action_kind(reason),
            transfer_reason_label(reason),
            outcome,
        ])
        .inc();
}

pub(super) fn log_transfer_requested(
    timeline_key: &str,
    target_owner_endpoint: &str,
    target_generator_id: Option<u32>,
    reason: TransferReason,
) {
    let action = operator_action_event(reason, TransferEventFamily::Requested);
    record_transfer_outcome(reason, action.action_outcome);
    info!(
        component = "transfer_failover",
        event = action.event,
        result = "success",
        action_kind = transfer_action_kind(reason),
        action_outcome = action.action_outcome,
        transfer_reason = transfer_reason_label(reason),
        timeline_key,
        target_owner_endpoint = %target_owner_endpoint,
        target_generator_id = target_generator_id.unwrap_or_default(),
        target_generator_id_explicit = target_generator_id.is_some()
    );
}

pub(super) fn failed_reason_label(error: &TsoError) -> &'static str {
    match error {
        TsoError::TargetGeneratorIdRequired { .. } => "TargetGeneratorIdRequired",
        TsoError::SharedGeneratorJumpAheadTooLarge { .. } => "SharedGeneratorJumpAheadTooLarge",
        TsoError::GeneratorNotOwnedByThisWorker { .. } => "GeneratorNotOwnedByThisWorker",
        TsoError::CasFailed => "CasFailed",
        _ => "TransferFailed",
    }
}

pub(super) fn log_transfer_failed(
    timeline_key: &str,
    target_owner_endpoint: &str,
    target_generator_id: Option<u32>,
    reason: TransferReason,
    error: &TsoError,
) {
    let action = operator_action_event(reason, TransferEventFamily::Failed);
    record_transfer_outcome(reason, action.action_outcome);
    warn!(
        component = "transfer_failover",
        event = action.event,
        result = "failure",
        action_kind = transfer_action_kind(reason),
        action_outcome = action.action_outcome,
        transfer_reason = transfer_reason_label(reason),
        failure_reason = failed_reason_label(error),
        timeline_key,
        target_owner_endpoint = %target_owner_endpoint,
        target_generator_id = target_generator_id.unwrap_or_default(),
        target_generator_id_explicit = target_generator_id.is_some(),
        error = %error
    );
}

pub(super) fn log_transfer_recovery_floor_computed(
    timeline_key: &str,
    source_generator_id: u32,
    reason: TransferReason,
    recovery_floor_tso: Option<u64>,
) {
    info!(
        component = "transfer_failover",
        event = "recovery_floor_computed",
        result = "success",
        action_kind = transfer_action_kind(reason),
        action_phase = "recovery_floor_computed",
        transfer_reason = transfer_reason_label(reason),
        timeline_key,
        source_generator_id,
        recovery_floor_tso = recovery_floor_tso.unwrap_or_default()
    );
}

pub(super) fn log_failover_blocked(
    timeline_key: &str,
    generator_id: u32,
    lease_expire_at_ms: Option<u64>,
    blocker: FailoverBlocker,
) {
    let action = failover_blocked_event(blocker);
    record_transfer_outcome(TransferReason::Failover, "blocked");
    match lease_expire_at_ms {
        Some(exp) => warn!(
            component = "transfer_failover",
            event = action.event,
            result = "failure",
            action_kind = "failover",
            action_outcome = action.action_outcome,
            transfer_reason = transfer_reason_label(TransferReason::Failover),
            action_blocker = blocker.action_blocker(),
            next_step = blocker.next_step(),
            timeline_key,
            generator_id,
            lease_expire_at_ms = exp
        ),
        None => warn!(
            component = "transfer_failover",
            event = action.event,
            result = "failure",
            action_kind = "failover",
            action_outcome = action.action_outcome,
            transfer_reason = transfer_reason_label(TransferReason::Failover),
            action_blocker = blocker.action_blocker(),
            next_step = blocker.next_step(),
            timeline_key,
            generator_id
        ),
    }
}

pub(super) fn log_transfer_completed(
    timeline_key: &str,
    reason: TransferReason,
    previous_route: &TimelineRoute,
    route: &TimelineRoute,
    timeline_state: TimelineLifecycleState,
) {
    let action = operator_action_event(reason, TransferEventFamily::Completed);
    info!(
        component = "transfer_failover",
        event = action.event,
        result = "success",
        action_kind = transfer_action_kind(reason),
        action_outcome = action.action_outcome,
        transfer_reason = transfer_reason_label(reason),
        timeline_key,
        source_owner_endpoint = %previous_route.owner_worker_endpoint,
        source_generator_id = previous_route.generator_id,
        target_owner_endpoint = %route.owner_worker_endpoint,
        target_generator_id = route.generator_id,
        epoch = route.epoch,
        route_version = route.route_version,
        timeline_state = %timeline_state
    );
    record_transfer_outcome(reason, action.action_outcome);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        failed_reason_label, failover_blocked_event, operator_action_event,
        record_transfer_outcome, transfer_action_kind, transfer_reason_label, FailoverBlocker,
        TransferEventFamily,
    };
    use crate::metadata::{MemoryMetadataStore, TimelineAuthority};
    use crate::planning::TransferPlan;
    use crate::{
        metrics, AllocateTimestampsRequest, ManualClock, ResourceTier, TransferReason, TsoConfig,
        TsoError, TsoSecurityMode, TsoService,
    };

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

    #[test]
    fn manual_transfer_outcome_uses_stable_labels() {
        let before = metrics::TSO_TRANSFER_OUTCOMES_TOTAL
            .with_label_values(&["transfer", "manual", "started"])
            .get();

        record_transfer_outcome(TransferReason::Manual, "started");

        assert!(
            metrics::TSO_TRANSFER_OUTCOMES_TOTAL
                .with_label_values(&["transfer", "manual", "started"])
                .get()
                > before
        );
    }

    #[test]
    fn failover_outcome_uses_stable_labels() {
        let before = metrics::TSO_TRANSFER_OUTCOMES_TOTAL
            .with_label_values(&["failover", "failover", "blocked"])
            .get();

        record_transfer_outcome(TransferReason::Failover, "blocked");

        assert!(
            metrics::TSO_TRANSFER_OUTCOMES_TOTAL
                .with_label_values(&["failover", "failover", "blocked"])
                .get()
                > before
        );
    }

    #[test]
    fn transfer_reason_labels_match_operator_vocabulary() {
        assert_eq!(transfer_action_kind(TransferReason::Manual), "transfer");
        assert_eq!(transfer_reason_label(TransferReason::Manual), "manual");
        assert_eq!(transfer_action_kind(TransferReason::Rebalance), "transfer");
        assert_eq!(
            transfer_reason_label(TransferReason::Rebalance),
            "rebalance"
        );
        assert_eq!(transfer_action_kind(TransferReason::Hotspot), "transfer");
        assert_eq!(transfer_reason_label(TransferReason::Hotspot), "hotspot");
        assert_eq!(transfer_action_kind(TransferReason::Failover), "failover");
        assert_eq!(transfer_reason_label(TransferReason::Failover), "failover");
    }

    #[test]
    fn failover_blockers_use_rpc_aligned_labels_and_blocked_outcome() {
        let lease_blocked = failover_blocked_event(FailoverBlocker::LeaseNotExpired);
        let floor_blocked = failover_blocked_event(FailoverBlocker::RecoveryFloorMissing);

        assert_eq!(lease_blocked.event, "failover_blocked_lease_not_expired");
        assert_eq!(lease_blocked.action_outcome, "blocked");
        assert_eq!(
            floor_blocked.event,
            "failover_blocked_recovery_floor_missing"
        );
        assert_eq!(floor_blocked.action_outcome, "blocked");
        assert_eq!(
            FailoverBlocker::LeaseNotExpired.action_blocker(),
            "LeaseNotExpired"
        );
        assert_eq!(
            FailoverBlocker::LeaseNotExpired.next_step(),
            "WaitForLeaseExpiry"
        );
        assert_eq!(
            FailoverBlocker::RecoveryFloorMissing.action_blocker(),
            "RecoveryFloorMissing"
        );
        assert_eq!(
            FailoverBlocker::RecoveryFloorMissing.next_step(),
            "PersistRecoveryFloor"
        );
    }

    #[test]
    fn target_generator_validation_uses_stable_failed_reason() {
        let error = TsoError::TargetGeneratorIdRequired {
            timeline_key: "timeline-a".to_string(),
        };

        assert_eq!(
            operator_action_event(TransferReason::Manual, TransferEventFamily::Failed).event,
            "transfer_failed"
        );
        assert_eq!(
            operator_action_event(TransferReason::Manual, TransferEventFamily::Failed)
                .action_outcome,
            "failed"
        );
        assert_eq!(failed_reason_label(&error), "TargetGeneratorIdRequired");
    }

    #[test]
    fn failed_reason_labels_lock_known_reasons_and_generic_fallback() {
        assert_eq!(
            failed_reason_label(&TsoError::SharedGeneratorJumpAheadTooLarge {
                generator_id: 7,
                required_jump_ms: 900,
                threshold_ms: 100,
            }),
            "SharedGeneratorJumpAheadTooLarge"
        );
        assert_eq!(
            failed_reason_label(&TsoError::GeneratorNotOwnedByThisWorker {
                generator_id: 3,
                modulo: 4,
                remainder: 1,
            }),
            "GeneratorNotOwnedByThisWorker"
        );
        assert_eq!(failed_reason_label(&TsoError::CasFailed), "CasFailed");
        assert_eq!(
            failed_reason_label(&TsoError::TimelineNotFound {
                timeline_key: "missing".to_string(),
            }),
            "TransferFailed"
        );
    }

    #[tokio::test]
    async fn rpc_transfer_missing_target_generator_records_terminal_failed_outcome() {
        let clock = Arc::new(ManualClock::new(10_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service =
            TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();
        service
            .ensure_timeline("transfer.failed.contract")
            .await
            .unwrap();

        let started_before = metrics::TSO_TRANSFER_OUTCOMES_TOTAL
            .with_label_values(&["transfer", "manual", "started"])
            .get();
        let failed_before = metrics::TSO_TRANSFER_OUTCOMES_TOTAL
            .with_label_values(&["transfer", "manual", "failed"])
            .get();

        let error = service
            .transfer_timeline_for_rpc(
                "transfer.failed.contract",
                "remote-endpoint:50051".to_string(),
                None,
                TransferReason::Manual,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, TsoError::TargetGeneratorIdRequired { .. }));
        assert!(
            metrics::TSO_TRANSFER_OUTCOMES_TOTAL
                .with_label_values(&["transfer", "manual", "started"])
                .get()
                > started_before
        );
        assert!(
            metrics::TSO_TRANSFER_OUTCOMES_TOTAL
                .with_label_values(&["transfer", "manual", "failed"])
                .get()
                > failed_before
        );
    }

    #[tokio::test]
    async fn generator_not_owned_failure_records_terminal_failed_outcome() {
        let clock = Arc::new(ManualClock::new(11_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig {
                shared_generators: 2,
                generator_ownership_modulo: 2,
                generator_ownership_remainder: 0,
                advertise_endpoint: "worker-a:50051".to_string(),
                ..TsoConfig::default()
            }),
            clock,
            metadata,
        )
        .unwrap();
        let route = service
            .ensure_timeline("transfer.failed.generator-owner")
            .await
            .unwrap();

        let started_before = metrics::TSO_TRANSFER_OUTCOMES_TOTAL
            .with_label_values(&["transfer", "manual", "started"])
            .get();
        let failed_before = metrics::TSO_TRANSFER_OUTCOMES_TOTAL
            .with_label_values(&["transfer", "manual", "failed"])
            .get();

        let error = service
            .transfer_timeline(
                &route.timeline_key,
                ResourceTier::Shared,
                service.advertise_endpoint().to_string(),
                Some(1),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TsoError::GeneratorNotOwnedByThisWorker {
                generator_id: 1,
                ..
            }
        ));
        assert!(
            metrics::TSO_TRANSFER_OUTCOMES_TOTAL
                .with_label_values(&["transfer", "manual", "started"])
                .get()
                > started_before
        );
        assert!(
            metrics::TSO_TRANSFER_OUTCOMES_TOTAL
                .with_label_values(&["transfer", "manual", "failed"])
                .get()
                > failed_before
        );
    }

    #[tokio::test]
    async fn shared_jump_ahead_failure_records_terminal_failed_outcome() {
        let clock = Arc::new(ManualClock::new(1_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig {
                shared_generators: 1,
                warm_generators: 0,
                shared_jump_ahead_threshold_ms: 100,
                max_future_borrow_ms: 10_000,
                ..TsoConfig::default()
            }),
            clock.clone(),
            metadata,
        )
        .unwrap();

        let shared = service
            .ensure_timeline("shared-floor.anchor")
            .await
            .unwrap();
        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: shared.timeline_key.clone(),
                count: 1,
                expected_epoch: shared.epoch,
                expected_route_version: shared.route_version,
                client_request_id: "shared-floor-seed".to_string(),
            })
            .await
            .unwrap();

        clock.set(20_000);
        let dedicated = service
            .ensure_timeline_with_tier("hot.timeline", ResourceTier::Dedicated)
            .await
            .unwrap();
        service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: dedicated.timeline_key.clone(),
                count: 1,
                expected_epoch: dedicated.epoch,
                expected_route_version: dedicated.route_version,
                client_request_id: "hot-seed".to_string(),
            })
            .await
            .unwrap();

        let started_before = metrics::TSO_TRANSFER_OUTCOMES_TOTAL
            .with_label_values(&["transfer", "manual", "started"])
            .get();
        let failed_before = metrics::TSO_TRANSFER_OUTCOMES_TOTAL
            .with_label_values(&["transfer", "manual", "failed"])
            .get();

        let error = service
            .transfer_timeline(
                &dedicated.timeline_key,
                ResourceTier::Shared,
                service.advertise_endpoint().to_string(),
                Some(shared.generator_id),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TsoError::SharedGeneratorJumpAheadTooLarge {
                generator_id: 0,
                ..
            }
        ));
        assert!(
            metrics::TSO_TRANSFER_OUTCOMES_TOTAL
                .with_label_values(&["transfer", "manual", "started"])
                .get()
                > started_before
        );
        assert!(
            metrics::TSO_TRANSFER_OUTCOMES_TOTAL
                .with_label_values(&["transfer", "manual", "failed"])
                .get()
                > failed_before
        );
    }

    #[tokio::test]
    async fn cas_failure_records_terminal_failed_outcome() {
        let clock = Arc::new(ManualClock::new(12_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig {
                shared_generators: 2,
                warm_generators: 0,
                ..TsoConfig::default()
            }),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let route = service
            .ensure_timeline("transfer.failed.cas")
            .await
            .unwrap();
        let (record, revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();

        let mut concurrent = record.clone();
        concurrent.updated_at_ms += 1;
        metadata
            .compare_exchange_timeline(&route.timeline_key, revision, &concurrent)
            .await
            .unwrap();

        let failed_before = metrics::TSO_TRANSFER_OUTCOMES_TOTAL
            .with_label_values(&["transfer", "manual", "failed"])
            .get();

        let error = service
            .apply_transfer_plan(
                &route.timeline_key,
                record,
                revision,
                TransferPlan {
                    resource_tier: ResourceTier::Shared,
                    owner_endpoint: service.advertise_endpoint().to_string(),
                    generator_id: Some(1),
                    reason: TransferReason::Manual,
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(error, TsoError::CasFailed));
        assert!(
            metrics::TSO_TRANSFER_OUTCOMES_TOTAL
                .with_label_values(&["transfer", "manual", "failed"])
                .get()
                > failed_before
        );
    }

    #[tokio::test]
    async fn non_cas_compare_exchange_errors_do_not_record_failed_outcome() {
        let clock = Arc::new(ManualClock::new(13_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig {
                shared_generators: 2,
                warm_generators: 0,
                ..TsoConfig::default()
            }),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let route = service
            .ensure_timeline("transfer.failed.noncas.anchor")
            .await
            .unwrap();
        let (record, revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();

        let failed_before = metrics::TSO_TRANSFER_OUTCOMES_TOTAL
            .with_label_values(&["transfer", "hotspot", "failed"])
            .get();

        let error = service
            .apply_transfer_plan(
                "transfer.failed.noncas.missing",
                record,
                revision,
                TransferPlan {
                    resource_tier: ResourceTier::Shared,
                    owner_endpoint: service.advertise_endpoint().to_string(),
                    generator_id: Some(route.generator_id),
                    reason: TransferReason::Hotspot,
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(error, TsoError::TimelineNotFound { .. }));
        assert_eq!(
            metrics::TSO_TRANSFER_OUTCOMES_TOTAL
                .with_label_values(&["transfer", "hotspot", "failed"])
                .get(),
            failed_before
        );
    }
}
