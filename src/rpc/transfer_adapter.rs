use tonic::Status;

use crate::proto::v1::{TransferTimelineRequest, TransferTimelineResponse};
use crate::{TimelineLifecycleState, TimelineRoute, TransferReason, TsoControlPlane, TsoError};

use super::{status_mapping, translation};

pub(super) async fn transfer_timeline_response(
    control_plane: &TsoControlPlane,
    request: TransferTimelineRequest,
) -> Result<TransferTimelineResponse, Status> {
    let request = normalize_transfer_request(control_plane.advertise_endpoint(), request)
        .map_err(translation::map_tso_error)?;
    let (route, old_generator_id, state) = control_plane
        .transfer_timeline_for_rpc(
            &request.timeline_key,
            request.target_owner_endpoint,
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
    target_owner_endpoint: String,
    target_generator_id: Option<u32>,
    reason: TransferReason,
}

fn normalize_transfer_request(
    advertise_endpoint: &str,
    request: TransferTimelineRequest,
) -> Result<NormalizedTransferTimelineRequest, TsoError> {
    Ok(NormalizedTransferTimelineRequest {
        timeline_key: request.timeline_key,
        target_owner_endpoint: resolve_target_owner_endpoint(
            advertise_endpoint,
            request.target_worker_id,
        )?,
        target_generator_id: request.target_generator_id,
        reason: parse_transfer_reason(request.reason)?,
    })
}

fn resolve_target_owner_endpoint(
    advertise_endpoint: &str,
    raw_target_owner_endpoint: Option<String>,
) -> Result<String, TsoError> {
    match raw_target_owner_endpoint {
        Some(raw_target_owner_endpoint) => {
            let target_owner_endpoint = raw_target_owner_endpoint.trim();
            if target_owner_endpoint.is_empty() {
                return Err(TsoError::InvalidTargetOwnerEndpoint);
            }
            Ok(target_owner_endpoint.to_owned())
        }
        None => Ok(advertise_endpoint.to_string()),
    }
}

fn parse_transfer_reason(reason: i32) -> Result<TransferReason, TsoError> {
    match reason {
        1 => Ok(TransferReason::Rebalance),
        2 => Ok(TransferReason::Hotspot),
        3 => Ok(TransferReason::Failover),
        4 => Ok(TransferReason::Manual),
        _ => Err(TsoError::InvalidTransferReason { value: reason }),
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
        state: status_mapping::proto_timeline_state(state),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::v1::TimelineState as ProtoTimelineState;
    use crate::ResourceTier;

    #[test]
    fn normalize_transfer_request_defaults_target_owner_and_manual_reason() {
        let normalized = normalize_transfer_request(
            "worker-a:50051",
            TransferTimelineRequest {
                timeline_key: "timeline-a".into(),
                target_generator_id: Some(7),
                target_worker_id: None,
                reason: 4,
            },
        )
        .unwrap();

        assert_eq!(normalized.timeline_key, "timeline-a");
        assert_eq!(normalized.target_owner_endpoint, "worker-a:50051");
        assert_eq!(normalized.target_generator_id, Some(7));
        assert_eq!(normalized.reason, TransferReason::Manual);
    }

    #[test]
    fn normalize_transfer_request_rejects_blank_target_owner_endpoint() {
        let error = normalize_transfer_request(
            "worker-a:50051",
            TransferTimelineRequest {
                timeline_key: "timeline-a".into(),
                target_generator_id: Some(7),
                target_worker_id: Some("   ".into()),
                reason: 4,
            },
        )
        .unwrap_err();

        assert!(matches!(error, TsoError::InvalidTargetOwnerEndpoint));
    }

    #[test]
    fn normalize_transfer_request_rejects_invalid_transfer_reason() {
        let error = normalize_transfer_request(
            "worker-a:50051",
            TransferTimelineRequest {
                timeline_key: "timeline-a".into(),
                target_generator_id: Some(7),
                target_worker_id: None,
                reason: 0,
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TsoError::InvalidTransferReason { value: 0 }
        ));
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
