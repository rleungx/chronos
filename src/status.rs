use crate::metadata::{GeneratorRecord, TimelineRecord};
use crate::{HealthInfo, TimelineLifecycleState, TimelineRoute, TsoConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerReadinessState {
    Ready,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerReadinessReason {
    Serving,
    IdentityLeaseLost,
    ShuttingDown,
    OwnershipDrift,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkerStatusIdentity {
    pub(crate) worker_id: String,
    pub(crate) instance_id: String,
    pub(crate) advertise_endpoint: String,
}

impl WorkerStatusIdentity {
    pub(crate) fn from_health_info(health: &HealthInfo) -> Self {
        Self {
            worker_id: health.worker_id.clone(),
            instance_id: health.instance_id.clone(),
            advertise_endpoint: health.advertise_endpoint.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkerStatusSnapshot {
    pub(crate) identity: WorkerStatusIdentity,
    pub(crate) readiness_state: WorkerReadinessState,
    pub(crate) readiness_reason: WorkerReadinessReason,
    pub(crate) identity_lease_healthy: bool,
}

impl WorkerStatusSnapshot {
    pub(crate) fn serving(identity: WorkerStatusIdentity) -> Self {
        Self {
            identity,
            readiness_state: WorkerReadinessState::Ready,
            readiness_reason: WorkerReadinessReason::Serving,
            identity_lease_healthy: true,
        }
    }

    pub(crate) fn mark_shutting_down(&mut self) {
        self.readiness_state = WorkerReadinessState::Degraded;
        if self.readiness_reason != WorkerReadinessReason::IdentityLeaseLost {
            self.readiness_reason = WorkerReadinessReason::ShuttingDown;
        }
    }

    pub(crate) fn mark_identity_lease_lost(&mut self) {
        self.readiness_state = WorkerReadinessState::Degraded;
        self.readiness_reason = WorkerReadinessReason::IdentityLeaseLost;
        self.identity_lease_healthy = false;
    }

    pub(crate) fn mark_ownership_drift(&mut self) {
        if matches!(
            self.readiness_reason,
            WorkerReadinessReason::IdentityLeaseLost | WorkerReadinessReason::ShuttingDown
        ) {
            return;
        }
        self.readiness_state = WorkerReadinessState::Degraded;
        self.readiness_reason = WorkerReadinessReason::OwnershipDrift;
    }

    pub(crate) fn clear_ownership_drift(&mut self) {
        if self.readiness_reason == WorkerReadinessReason::OwnershipDrift {
            self.readiness_state = WorkerReadinessState::Ready;
            self.readiness_reason = WorkerReadinessReason::Serving;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimelineFailoverReadiness {
    NotApplicable,
    Eligible,
    WaitingForLeaseExpiry,
    BlockedMissingRecoveryFloor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimelineStatusSnapshot {
    pub(crate) route: TimelineRoute,
    pub(crate) state: TimelineLifecycleState,
    pub(crate) recovery_floor_tso: Option<u64>,
    pub(crate) issued_upper_bound: Option<u64>,
    pub(crate) last_graceful_issued: Option<u64>,
    pub(crate) lease_expire_at_ms: Option<u64>,
    pub(crate) owner_instance_id: Option<String>,
    pub(crate) updated_at_ms: u64,
    pub(crate) failover_readiness: TimelineFailoverReadiness,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimelineStatusListPage {
    pub(crate) statuses: Vec<TimelineStatusSnapshot>,
    pub(crate) next_start_after: Option<String>,
}

pub(crate) fn matched_generator_record<'a>(
    timeline: &TimelineRecord,
    generator: Option<&'a GeneratorRecord>,
) -> Option<&'a GeneratorRecord> {
    generator.filter(|generator| {
        generator.generator_id == timeline.route.generator_id
            && generator.owner_worker_endpoint == timeline.route.owner_worker_endpoint
    })
}

pub(crate) fn build_timeline_status_snapshot(
    config: &TsoConfig,
    now_ms: u64,
    timeline: &TimelineRecord,
    generator: Option<&GeneratorRecord>,
) -> TimelineStatusSnapshot {
    let matched_generator = matched_generator_record(timeline, generator);
    let updated_at_ms = matched_generator
        .map(|generator| timeline.updated_at_ms.max(generator.updated_at_ms))
        .unwrap_or(timeline.updated_at_ms);

    TimelineStatusSnapshot {
        route: timeline.route.clone(),
        state: timeline.state,
        recovery_floor_tso: timeline.recovery_floor_tso,
        issued_upper_bound: timeline.issued_upper_bound,
        last_graceful_issued: timeline.last_graceful_issued,
        lease_expire_at_ms: matched_generator.and_then(|generator| generator.lease_expire_at_ms),
        owner_instance_id: matched_generator.and_then(|generator| {
            (!generator.owner_instance_id.is_empty()).then_some(generator.owner_instance_id.clone())
        }),
        updated_at_ms,
        failover_readiness: derive_failover_readiness(
            config.safety_gap_ms,
            now_ms,
            timeline.recovery_floor_tso,
            matched_generator.and_then(|generator| generator.lease_expire_at_ms),
        ),
    }
}

fn derive_failover_readiness(
    safety_gap_ms: u64,
    now_ms: u64,
    recovery_floor_tso: Option<u64>,
    lease_expire_at_ms: Option<u64>,
) -> TimelineFailoverReadiness {
    let Some(lease_expire_at_ms) = lease_expire_at_ms else {
        return TimelineFailoverReadiness::NotApplicable;
    };
    let lease_expired =
        crate::lease_expired_with_safety_gap(lease_expire_at_ms, now_ms, safety_gap_ms);
    match (lease_expired, recovery_floor_tso) {
        (true, Some(_)) => TimelineFailoverReadiness::Eligible,
        (true, None) => TimelineFailoverReadiness::BlockedMissingRecoveryFloor,
        (false, Some(_)) => TimelineFailoverReadiness::WaitingForLeaseExpiry,
        (false, None) => TimelineFailoverReadiness::NotApplicable,
    }
}

#[cfg(test)]
mod tests {
    use crate::metadata::{GeneratorRecord, TimelineRecord};
    use crate::{ResourceTier, TimelineLifecycleState, TimelineRoute, TsoConfig};

    use super::{
        build_timeline_status_snapshot, matched_generator_record, TimelineFailoverReadiness,
        WorkerReadinessReason, WorkerReadinessState, WorkerStatusIdentity, WorkerStatusSnapshot,
    };

    fn sample_route() -> TimelineRoute {
        TimelineRoute {
            timeline_key: "timeline-a".into(),
            generator_id: 7,
            epoch: 3,
            route_version: 9,
            resource_tier: ResourceTier::Shared,
            owner_worker_endpoint: "worker-a:50051".into(),
        }
    }

    fn sample_timeline_record() -> TimelineRecord {
        TimelineRecord {
            route: sample_route(),
            state: TimelineLifecycleState::Active,
            recovery_floor_tso: None,
            issued_upper_bound: Some(88),
            last_graceful_issued: Some(77),
            lease_expire_at_ms: Some(66),
            updated_at_ms: 10,
        }
    }

    fn sample_generator_record() -> GeneratorRecord {
        GeneratorRecord {
            generator_id: 7,
            owner_worker_endpoint: "worker-a:50051".into(),
            owner_instance_id: "instance-a".into(),
            generator_lease_token: 1,
            lease_expire_at_ms: Some(123),
            last_issued_tso: Some(99),
            issued_upper_bound: Some(100),
            updated_at_ms: 12,
        }
    }

    #[test]
    fn worker_serving_snapshot_is_ready_and_healthy() {
        let identity = WorkerStatusIdentity {
            worker_id: "worker-a".into(),
            instance_id: "instance-a".into(),
            advertise_endpoint: "worker-a:50051".into(),
        };

        let snapshot = WorkerStatusSnapshot::serving(identity.clone());

        assert_eq!(snapshot.identity, identity);
        assert_eq!(snapshot.readiness_state, WorkerReadinessState::Ready);
        assert_eq!(snapshot.readiness_reason, WorkerReadinessReason::Serving);
        assert!(snapshot.identity_lease_healthy);
    }

    #[test]
    fn worker_shutting_down_snapshot_is_degraded_but_keeps_lease_health() {
        let identity = WorkerStatusIdentity {
            worker_id: "worker-a".into(),
            instance_id: "instance-a".into(),
            advertise_endpoint: "worker-a:50051".into(),
        };
        let mut snapshot = WorkerStatusSnapshot::serving(identity);

        snapshot.mark_shutting_down();

        assert_eq!(snapshot.readiness_state, WorkerReadinessState::Degraded);
        assert_eq!(
            snapshot.readiness_reason,
            WorkerReadinessReason::ShuttingDown
        );
        assert!(snapshot.identity_lease_healthy);
    }

    #[test]
    fn worker_identity_lease_loss_snapshot_is_degraded_and_unhealthy() {
        let identity = WorkerStatusIdentity {
            worker_id: "worker-a".into(),
            instance_id: "instance-a".into(),
            advertise_endpoint: "worker-a:50051".into(),
        };
        let mut snapshot = WorkerStatusSnapshot::serving(identity);

        snapshot.mark_identity_lease_lost();

        assert_eq!(snapshot.readiness_state, WorkerReadinessState::Degraded);
        assert_eq!(
            snapshot.readiness_reason,
            WorkerReadinessReason::IdentityLeaseLost
        );
        assert!(!snapshot.identity_lease_healthy);
    }

    #[test]
    fn worker_shutting_down_does_not_override_identity_lease_loss() {
        let identity = WorkerStatusIdentity {
            worker_id: "worker-a".into(),
            instance_id: "instance-a".into(),
            advertise_endpoint: "worker-a:50051".into(),
        };
        let mut snapshot = WorkerStatusSnapshot::serving(identity);

        snapshot.mark_identity_lease_lost();
        snapshot.mark_shutting_down();

        assert_eq!(snapshot.readiness_state, WorkerReadinessState::Degraded);
        assert_eq!(
            snapshot.readiness_reason,
            WorkerReadinessReason::IdentityLeaseLost
        );
        assert!(!snapshot.identity_lease_healthy);
    }

    #[test]
    fn worker_identity_lease_loss_reason_is_not_overridden_by_shutdown() {
        let identity = WorkerStatusIdentity {
            worker_id: "worker-a".into(),
            instance_id: "instance-a".into(),
            advertise_endpoint: "worker-a:50051".into(),
        };
        let mut snapshot = WorkerStatusSnapshot::serving(identity);

        snapshot.mark_identity_lease_lost();
        snapshot.mark_shutting_down();

        assert_eq!(snapshot.readiness_state, WorkerReadinessState::Degraded);
        assert_eq!(
            snapshot.readiness_reason,
            WorkerReadinessReason::IdentityLeaseLost
        );
        assert!(!snapshot.identity_lease_healthy);
    }

    #[test]
    fn worker_ownership_drift_marks_degraded() {
        let identity = WorkerStatusIdentity {
            worker_id: "worker-a".into(),
            instance_id: "instance-a".into(),
            advertise_endpoint: "worker-a:50051".into(),
        };
        let mut snapshot = WorkerStatusSnapshot::serving(identity);

        snapshot.mark_ownership_drift();

        assert_eq!(snapshot.readiness_state, WorkerReadinessState::Degraded);
        assert_eq!(
            snapshot.readiness_reason,
            WorkerReadinessReason::OwnershipDrift
        );
        assert!(snapshot.identity_lease_healthy);
    }

    #[test]
    fn worker_ownership_drift_does_not_override_identity_lease_loss() {
        let identity = WorkerStatusIdentity {
            worker_id: "worker-a".into(),
            instance_id: "instance-a".into(),
            advertise_endpoint: "worker-a:50051".into(),
        };
        let mut snapshot = WorkerStatusSnapshot::serving(identity);

        snapshot.mark_identity_lease_lost();
        snapshot.mark_ownership_drift();

        assert_eq!(snapshot.readiness_state, WorkerReadinessState::Degraded);
        assert_eq!(
            snapshot.readiness_reason,
            WorkerReadinessReason::IdentityLeaseLost
        );
        assert!(!snapshot.identity_lease_healthy);
    }

    #[test]
    fn worker_clear_ownership_drift_restores_serving_only_from_drift() {
        let identity = WorkerStatusIdentity {
            worker_id: "worker-a".into(),
            instance_id: "instance-a".into(),
            advertise_endpoint: "worker-a:50051".into(),
        };
        let mut snapshot = WorkerStatusSnapshot::serving(identity);

        snapshot.mark_ownership_drift();
        snapshot.clear_ownership_drift();

        assert_eq!(snapshot.readiness_state, WorkerReadinessState::Ready);
        assert_eq!(snapshot.readiness_reason, WorkerReadinessReason::Serving);
        assert!(snapshot.identity_lease_healthy);
    }

    #[test]
    fn matched_generator_requires_same_owner_endpoint() {
        let timeline = sample_timeline_record();
        let mut mismatched_generator = sample_generator_record();
        mismatched_generator.owner_worker_endpoint = "worker-b:50051".into();

        assert!(matched_generator_record(&timeline, Some(&mismatched_generator)).is_none());
        assert!(matched_generator_record(&timeline, Some(&sample_generator_record())).is_some());
    }

    #[test]
    fn timeline_status_snapshot_prefers_matched_generator_fields() {
        let config = TsoConfig::default();
        let timeline = sample_timeline_record();
        let generator = sample_generator_record();

        let snapshot = build_timeline_status_snapshot(&config, 200, &timeline, Some(&generator));

        assert_eq!(snapshot.route, timeline.route);
        assert_eq!(snapshot.state, TimelineLifecycleState::Active);
        assert_eq!(snapshot.issued_upper_bound, timeline.issued_upper_bound);
        assert_eq!(snapshot.last_graceful_issued, timeline.last_graceful_issued);
        assert_eq!(snapshot.lease_expire_at_ms, generator.lease_expire_at_ms);
        assert_eq!(snapshot.owner_instance_id.as_deref(), Some("instance-a"));
        assert_eq!(snapshot.updated_at_ms, 12);
    }

    #[test]
    fn timeline_status_snapshot_degrades_when_generator_is_mismatched() {
        let config = TsoConfig::default();
        let timeline = sample_timeline_record();
        let mut generator = sample_generator_record();
        generator.owner_worker_endpoint = "worker-b:50051".into();

        let snapshot = build_timeline_status_snapshot(&config, 200, &timeline, Some(&generator));

        assert_eq!(snapshot.lease_expire_at_ms, None);
        assert_eq!(snapshot.owner_instance_id, None);
        assert_eq!(snapshot.updated_at_ms, timeline.updated_at_ms);
        assert_eq!(
            snapshot.failover_readiness,
            TimelineFailoverReadiness::NotApplicable
        );
    }

    #[test]
    fn timeline_status_snapshot_marks_failover_eligible_after_lease_expiry() {
        let config = TsoConfig {
            safety_gap_ms: 10,
            ..TsoConfig::default()
        };
        let mut timeline = sample_timeline_record();
        timeline.recovery_floor_tso = Some(1000);
        let mut generator = sample_generator_record();
        generator.lease_expire_at_ms = Some(80);

        let snapshot = build_timeline_status_snapshot(&config, 100, &timeline, Some(&generator));

        assert_eq!(
            snapshot.failover_readiness,
            TimelineFailoverReadiness::Eligible
        );
    }

    #[test]
    fn timeline_status_snapshot_marks_missing_floor_as_blocked_after_lease_expiry() {
        let config = TsoConfig {
            safety_gap_ms: 10,
            ..TsoConfig::default()
        };
        let timeline = sample_timeline_record();
        let mut generator = sample_generator_record();
        generator.lease_expire_at_ms = Some(80);

        let snapshot = build_timeline_status_snapshot(&config, 100, &timeline, Some(&generator));

        assert_eq!(
            snapshot.failover_readiness,
            TimelineFailoverReadiness::BlockedMissingRecoveryFloor
        );
    }
}
