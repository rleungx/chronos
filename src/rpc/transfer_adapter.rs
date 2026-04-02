use tonic::Status;

use crate::proto::v1::{
    TimelineState as ProtoTimelineState, TransferTimelineRequest, TransferTimelineResponse,
};
use crate::{TimelineLifecycleState, TimelineRoute, TransferReason, TsoControlPlane};

use super::translation;

pub(super) async fn transfer_timeline_response(
    control_plane: &TsoControlPlane,
    request: TransferTimelineRequest,
) -> Result<TransferTimelineResponse, Status> {
    let request = normalize_transfer_request(control_plane.advertise_endpoint(), request);
    let (route, old_generator_id, state) = control_plane
        .transfer_timeline_for_rpc(
            &request.timeline_key,
            request.target_owner,
            request.target_generator_id,
            request.reason,
        )
        .await
        .map_err(translation::map_tso_error)?;

    Ok(build_transfer_timeline_response(
        route,
        old_generator_id,
        state,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedTransferTimelineRequest {
    timeline_key: String,
    target_owner: String,
    target_generator_id: Option<u32>,
    reason: TransferReason,
}

fn normalize_transfer_request(
    advertise_endpoint: &str,
    request: TransferTimelineRequest,
) -> NormalizedTransferTimelineRequest {
    NormalizedTransferTimelineRequest {
        timeline_key: request.timeline_key,
        target_owner: resolve_target_owner(advertise_endpoint, request.target_worker_id),
        target_generator_id: request.target_generator_id,
        reason: parse_transfer_reason(request.reason),
    }
}

fn resolve_target_owner(advertise_endpoint: &str, target_worker_id: Option<String>) -> String {
    target_worker_id.unwrap_or_else(|| advertise_endpoint.to_string())
}

fn parse_transfer_reason(reason: i32) -> TransferReason {
    match reason {
        1 => TransferReason::Rebalance,
        2 => TransferReason::Hotspot,
        3 => TransferReason::Failover,
        _ => TransferReason::Manual,
    }
}

fn build_transfer_timeline_response(
    route: TimelineRoute,
    old_generator_id: u32,
    state: TimelineLifecycleState,
) -> TransferTimelineResponse {
    TransferTimelineResponse {
        timeline_key: route.timeline_key,
        old_generator_id,
        new_generator_id: route.generator_id,
        new_epoch: route.epoch,
        route_version: route.route_version,
        state: proto_timeline_state(state),
    }
}

fn proto_timeline_state(state: TimelineLifecycleState) -> i32 {
    match state {
        TimelineLifecycleState::Creating => ProtoTimelineState::Creating as i32,
        TimelineLifecycleState::Active => ProtoTimelineState::Active as i32,
        TimelineLifecycleState::Draining => ProtoTimelineState::Draining as i32,
        TimelineLifecycleState::Locked => ProtoTimelineState::Locked as i32,
        TimelineLifecycleState::Recovering => ProtoTimelineState::Recovering as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceTier;

    #[test]
    fn normalize_transfer_request_defaults_target_owner_and_manual_reason() {
        let normalized = normalize_transfer_request(
            "worker-a:50051",
            TransferTimelineRequest {
                timeline_key: "timeline-a".into(),
                target_generator_id: Some(7),
                target_worker_id: None,
                reason: 0,
            },
        );

        assert_eq!(normalized.timeline_key, "timeline-a");
        assert_eq!(normalized.target_owner, "worker-a:50051");
        assert_eq!(normalized.target_generator_id, Some(7));
        assert_eq!(normalized.reason, TransferReason::Manual);
    }

    #[test]
    fn transfer_response_keeps_route_epoch_and_state_surface() {
        let response = build_transfer_timeline_response(
            TimelineRoute {
                timeline_key: "timeline-a".into(),
                generator_id: 9,
                owner_worker_endpoint: "worker-b:50051".into(),
                epoch: 4,
                route_version: 12,
                resource_tier: ResourceTier::Warm,
            },
            7,
            TimelineLifecycleState::Draining,
        );

        assert_eq!(response.timeline_key, "timeline-a");
        assert_eq!(response.old_generator_id, 7);
        assert_eq!(response.new_generator_id, 9);
        assert_eq!(response.new_epoch, 4);
        assert_eq!(response.route_version, 12);
        assert_eq!(response.state, ProtoTimelineState::Draining as i32);
    }
}
