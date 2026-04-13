use tonic::{Request, Response, Status};

use crate::proto::v1::ResourceTier as ProtoResourceTier;
use crate::proto::v1::{
    timeline_route_service_server::TimelineRouteService,
    timestamp_service_server::TimestampService, AllocateTimestampsRequest,
    AllocateTimestampsResponse, EnsureTimelineRequest, EnsureTimelineResponse,
    GetTimelineRouteRequest, GetTimelineRouteResponse,
};
use crate::timeline_proxy::TimelineScopedAllocator;
use crate::{
    AllocateTimestampsRequest as InternalAllocateTimestampsRequest, ResourceTier, TimestampRange,
    TsoControlPlane, TsoDataPlane,
};

use super::{status_mapping, translation};

pub struct TsoRouteService {
    control_plane: TsoControlPlane,
}

impl TsoRouteService {
    pub fn new(control_plane: TsoControlPlane) -> Self {
        Self { control_plane }
    }
}

fn decode_resource_tier(resource_tier: i32) -> ResourceTier {
    match ProtoResourceTier::try_from(resource_tier) {
        Ok(ProtoResourceTier::Shared) => ResourceTier::Shared,
        Ok(ProtoResourceTier::Warm) => ResourceTier::Warm,
        Ok(ProtoResourceTier::Dedicated) => ResourceTier::Dedicated,
        _ => ResourceTier::Shared,
    }
}

#[tonic::async_trait]
impl TimelineRouteService for TsoRouteService {
    async fn get_timeline_route(
        &self,
        request: Request<GetTimelineRouteRequest>,
    ) -> Result<Response<GetTimelineRouteResponse>, Status> {
        let req = request.into_inner();
        match self
            .control_plane
            .get_timeline_route(&req.timeline_key)
            .await
        {
            Ok(route) => Ok(Response::new(GetTimelineRouteResponse {
                route: Some(status_mapping::proto_timeline_route(route)),
            })),
            Err(e) => Err(translation::map_tso_error(e)),
        }
    }

    async fn ensure_timeline(
        &self,
        request: Request<EnsureTimelineRequest>,
    ) -> Result<Response<EnsureTimelineResponse>, Status> {
        let req = request.into_inner();
        let tier = decode_resource_tier(req.desired_resource_tier);

        match self
            .control_plane
            .ensure_timeline_with_tier(&req.timeline_key, tier)
            .await
        {
            Ok(route) => Ok(Response::new(EnsureTimelineResponse {
                route: Some(status_mapping::proto_timeline_route(route)),
            })),
            Err(e) => Err(translation::map_tso_error(e)),
        }
    }
}

pub struct TsoTimestampService {
    allocator: TimelineScopedAllocator,
}

impl TsoTimestampService {
    pub fn new(data_plane: TsoDataPlane) -> Self {
        Self {
            allocator: TimelineScopedAllocator::new(data_plane),
        }
    }
}

#[tonic::async_trait]
impl TimestampService for TsoTimestampService {
    async fn allocate_timestamps(
        &self,
        request: Request<AllocateTimestampsRequest>,
    ) -> Result<Response<AllocateTimestampsResponse>, Status> {
        Ok(Response::new(
            allocate_timestamps_response(&self.allocator, request.into_inner()).await?,
        ))
    }
}

async fn allocate_timestamps_response(
    allocator: &TimelineScopedAllocator,
    request: AllocateTimestampsRequest,
) -> Result<AllocateTimestampsResponse, Status> {
    let response = allocator
        .allocate_timestamps_with_timeout(
            InternalAllocateTimestampsRequest {
                timeline_key: request.timeline_key.clone(),
                count: request.count,
                expected_epoch: request.expected_epoch,
                expected_route_version: request.expected_route_version,
                client_request_id: request.client_request_id,
            },
            request.request_timeout_ms,
        )
        .await
        .map_err(translation::map_timeline_proxy_error)?;

    Ok(AllocateTimestampsResponse {
        timeline_key: request.timeline_key,
        generator_id: response.generator_id,
        epoch: response.epoch,
        route_version: response.route_version,
        ranges: proto_timestamp_ranges(response.ranges),
    })
}

fn proto_timestamp_ranges(ranges: Vec<TimestampRange>) -> Vec<crate::proto::v1::TimestampRange> {
    ranges
        .into_iter()
        .map(|range| crate::proto::v1::TimestampRange {
            start_tso: range.start_tso,
            end_tso: range.end_tso,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_resource_tier_defaults_unknown_values_to_shared() {
        assert_eq!(decode_resource_tier(-1), ResourceTier::Shared);
        assert_eq!(
            decode_resource_tier(ProtoResourceTier::Warm as i32),
            ResourceTier::Warm
        );
        assert_eq!(
            decode_resource_tier(ProtoResourceTier::Dedicated as i32),
            ResourceTier::Dedicated
        );
    }

    #[test]
    fn proto_timestamp_ranges_keep_start_and_end_bounds() {
        let ranges = proto_timestamp_ranges(vec![
            TimestampRange {
                start_tso: 10,
                end_tso: 19,
            },
            TimestampRange {
                start_tso: 20,
                end_tso: 29,
            },
        ]);

        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].start_tso, 10);
        assert_eq!(ranges[0].end_tso, 19);
        assert_eq!(ranges[1].start_tso, 20);
        assert_eq!(ranges[1].end_tso, 29);
    }
}
