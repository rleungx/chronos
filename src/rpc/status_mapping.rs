use prost_types::Timestamp;

use crate::proto::v1::{
    GetTimelineStatusResponse, HealthResponse, ListTimelineStatusesResponse,
    ResourceTier as ProtoResourceTier, TimelineFailoverReadiness as ProtoTimelineFailoverReadiness,
    TimelineRoute as ProtoTimelineRoute, TimelineStatus as ProtoTimelineStatus,
    WorkerReadinessReason as ProtoWorkerReadinessReason,
    WorkerReadinessState as ProtoWorkerReadinessState,
};
use crate::status::{
    TimelineFailoverReadiness, TimelineStatusSnapshot, WorkerReadinessReason, WorkerReadinessState,
    WorkerStatusSnapshot,
};
use crate::{ResourceTier, TimelineLifecycleState, TimelineRoute};

pub(super) fn proto_worker_readiness_state(
    state: WorkerReadinessState,
) -> ProtoWorkerReadinessState {
    match state {
        WorkerReadinessState::Ready => ProtoWorkerReadinessState::Ready,
        WorkerReadinessState::Degraded => ProtoWorkerReadinessState::Degraded,
    }
}

pub(super) fn proto_worker_readiness_reason(
    reason: WorkerReadinessReason,
) -> ProtoWorkerReadinessReason {
    match reason {
        WorkerReadinessReason::Serving => ProtoWorkerReadinessReason::Serving,
        WorkerReadinessReason::StartupPreflightFailed => {
            ProtoWorkerReadinessReason::StartupPreflightFailed
        }
        WorkerReadinessReason::MetadataStartupProbeFailed => {
            ProtoWorkerReadinessReason::MetadataStartupProbeFailed
        }
        WorkerReadinessReason::IdentityLeaseAcquireFailed => {
            ProtoWorkerReadinessReason::IdentityLeaseAcquireFailed
        }
        WorkerReadinessReason::IdentityLeaseLost => ProtoWorkerReadinessReason::IdentityLeaseLost,
        WorkerReadinessReason::ShuttingDown => ProtoWorkerReadinessReason::ShuttingDown,
        WorkerReadinessReason::Draining => ProtoWorkerReadinessReason::Draining,
        WorkerReadinessReason::OwnershipDrift => ProtoWorkerReadinessReason::OwnershipDrift,
    }
}

pub(super) fn proto_timeline_route(
    route: TimelineRoute,
    route_cache_ttl_ms: u32,
) -> ProtoTimelineRoute {
    ProtoTimelineRoute {
        timeline_key: route.timeline_key,
        generator_id: route.generator_id,
        owner_worker_endpoint: route.owner_worker_endpoint,
        epoch: route.epoch,
        route_version: route.route_version,
        resource_tier: proto_resource_tier(route.resource_tier),
        cache_ttl_ms: route_cache_ttl_ms,
    }
}

pub(super) fn health_response(
    worker_status: WorkerStatusSnapshot,
    server_time: Timestamp,
) -> HealthResponse {
    HealthResponse {
        service: "chronos".to_string(),
        instance_id: worker_status.identity.instance_id,
        server_time: Some(server_time),
        worker_id: worker_status.identity.worker_id,
        advertise_endpoint: worker_status.identity.advertise_endpoint,
        readiness_state: proto_worker_readiness_state(worker_status.readiness_state) as i32,
        readiness_reason: proto_worker_readiness_reason(worker_status.readiness_reason) as i32,
        identity_lease_healthy: worker_status.identity_lease_healthy,
        build_version: crate::build_info::build_version().to_string(),
        build_commit: crate::build_info::build_commit().to_string(),
        mixed_version_contract_id: crate::build_info::mixed_version_contract_id().to_string(),
    }
}

pub(super) fn get_timeline_status_response(
    status: TimelineStatusSnapshot,
    route_cache_ttl_ms: u32,
) -> GetTimelineStatusResponse {
    GetTimelineStatusResponse {
        status: Some(proto_timeline_status(status, route_cache_ttl_ms)),
    }
}

pub(super) fn list_timeline_statuses_response(
    statuses: Vec<TimelineStatusSnapshot>,
    next_page_token: String,
    route_cache_ttl_ms: u32,
) -> ListTimelineStatusesResponse {
    ListTimelineStatusesResponse {
        statuses: statuses
            .into_iter()
            .map(|status| proto_timeline_status(status, route_cache_ttl_ms))
            .collect(),
        next_page_token,
    }
}

fn proto_timeline_status(
    status: TimelineStatusSnapshot,
    route_cache_ttl_ms: u32,
) -> ProtoTimelineStatus {
    ProtoTimelineStatus {
        route: Some(proto_timeline_route(status.route, route_cache_ttl_ms)),
        state: proto_timeline_state(status.state),
        recovery_floor_tso: status.recovery_floor_tso,
        issued_upper_bound: status.issued_upper_bound,
        last_graceful_issued: status.last_graceful_issued,
        lease_expire_at_ms: status.lease_expire_at_ms,
        owner_instance_id: status.owner_instance_id,
        updated_at_ms: status.updated_at_ms,
        failover_readiness: proto_timeline_failover_readiness(status.failover_readiness) as i32,
    }
}

fn proto_timeline_failover_readiness(
    readiness: TimelineFailoverReadiness,
) -> ProtoTimelineFailoverReadiness {
    match readiness {
        TimelineFailoverReadiness::NotApplicable => ProtoTimelineFailoverReadiness::NotApplicable,
        TimelineFailoverReadiness::Eligible => ProtoTimelineFailoverReadiness::Eligible,
        TimelineFailoverReadiness::WaitingForLeaseExpiry => {
            ProtoTimelineFailoverReadiness::WaitingForLeaseExpiry
        }
        TimelineFailoverReadiness::BlockedMissingRecoveryFloor => {
            ProtoTimelineFailoverReadiness::BlockedMissingRecoveryFloor
        }
    }
}

fn proto_timeline_state(state: TimelineLifecycleState) -> i32 {
    match state {
        TimelineLifecycleState::Creating => crate::proto::v1::TimelineState::Creating as i32,
        TimelineLifecycleState::Active => crate::proto::v1::TimelineState::Active as i32,
        TimelineLifecycleState::Draining => crate::proto::v1::TimelineState::Draining as i32,
        TimelineLifecycleState::Locked => crate::proto::v1::TimelineState::Locked as i32,
        TimelineLifecycleState::Recovering => crate::proto::v1::TimelineState::Recovering as i32,
    }
}

fn proto_resource_tier(resource_tier: ResourceTier) -> i32 {
    match resource_tier {
        ResourceTier::Shared => ProtoResourceTier::Shared as i32,
        ResourceTier::Warm => ProtoResourceTier::Warm as i32,
        ResourceTier::Dedicated => ProtoResourceTier::Dedicated as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::{TimelineFailoverReadiness, WorkerStatusIdentity, WorkerStatusSnapshot};

    #[test]
    fn health_response_maps_worker_identity_and_readiness() {
        let response = health_response(
            WorkerStatusSnapshot {
                identity: WorkerStatusIdentity {
                    worker_id: "worker-a".into(),
                    instance_id: "instance-a".into(),
                    advertise_endpoint: "worker-a:50051".into(),
                },
                readiness_state: WorkerReadinessState::Degraded,
                readiness_reason: WorkerReadinessReason::IdentityLeaseLost,
                identity_lease_healthy: false,
            },
            Timestamp {
                seconds: 12,
                nanos: 0,
            },
        );

        assert_eq!(response.service, "chronos");
        assert_eq!(response.instance_id, "instance-a");
        assert_eq!(response.worker_id, "worker-a");
        assert_eq!(response.advertise_endpoint, "worker-a:50051");
        assert_eq!(
            response.readiness_state,
            ProtoWorkerReadinessState::Degraded as i32
        );
        assert_eq!(
            response.readiness_reason,
            ProtoWorkerReadinessReason::IdentityLeaseLost as i32
        );
        assert!(!response.identity_lease_healthy);
        assert_eq!(response.server_time.expect("server_time").seconds, 12);
        assert_eq!(response.build_version, crate::build_info::build_version());
        assert_eq!(response.build_commit, crate::build_info::build_commit());
        assert_eq!(
            response.mixed_version_contract_id,
            crate::build_info::mixed_version_contract_id()
        );
    }

    #[test]
    fn timeline_status_responses_keep_route_ttl_and_failover_mapping() {
        let status = TimelineStatusSnapshot {
            route: TimelineRoute {
                timeline_key: "timeline-a".into(),
                generator_id: 7,
                owner_worker_endpoint: "worker-a:50051".into(),
                epoch: 4,
                route_version: 9,
                resource_tier: ResourceTier::Warm,
            },
            state: TimelineLifecycleState::Recovering,
            recovery_floor_tso: Some(42),
            issued_upper_bound: Some(88),
            last_graceful_issued: Some(77),
            lease_expire_at_ms: Some(100),
            owner_instance_id: Some("instance-a".into()),
            updated_at_ms: 12,
            failover_readiness: TimelineFailoverReadiness::Eligible,
        };

        let point_read = get_timeline_status_response(status.clone(), 0);
        let list = list_timeline_statuses_response(vec![status], "next".into(), 0);
        let mapped = point_read.status.expect("status should be present");
        let route = mapped.route.expect("route should be present");

        assert_eq!(route.timeline_key, "timeline-a");
        assert_eq!(route.cache_ttl_ms, 0);
        assert_eq!(
            mapped.state,
            crate::proto::v1::TimelineState::Recovering as i32
        );
        assert_eq!(
            mapped.failover_readiness,
            ProtoTimelineFailoverReadiness::Eligible as i32
        );
        assert_eq!(list.statuses.len(), 1);
        assert_eq!(list.next_page_token, "next");
    }
}
