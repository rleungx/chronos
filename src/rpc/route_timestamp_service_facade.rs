use tonic::{Request, Response, Status};

use crate::proto::v1::ResourceTier as ProtoResourceTier;
use crate::proto::v1::{
    timeline_route_service_server::TimelineRouteService,
    timestamp_service_server::TimestampService, AllocateTimestampsRequest,
    AllocateTimestampsResponse, EnsureTimelineRequest, EnsureTimelineResponse,
    GetTimelineRouteRequest, GetTimelineRouteResponse, WatchTimelineRoutesRequest,
};
use crate::timeline_proxy::TimelineScopedAllocator;
use crate::{ResourceTier, TsoControlPlane, TsoDataPlane};

use super::{status_mapping, translation, watch_allocate_entry};

pub struct TsoRouteService {
    control_plane: TsoControlPlane,
    route_cache_ttl_ms: u32,
}

impl TsoRouteService {
    pub fn new(control_plane: TsoControlPlane) -> Self {
        let route_cache_ttl_ms = control_plane.route_cache_ttl_ms();
        Self {
            control_plane,
            route_cache_ttl_ms,
        }
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
                route: Some(status_mapping::proto_timeline_route(
                    route,
                    self.route_cache_ttl_ms,
                )),
            })),
            Err(e) => Err(translation::map_tso_error(e)),
        }
    }

    type WatchTimelineRoutesStream = watch_allocate_entry::WatchTimelineRoutesStream;

    async fn watch_timeline_routes(
        &self,
        request: Request<WatchTimelineRoutesRequest>,
    ) -> Result<Response<Self::WatchTimelineRoutesStream>, Status> {
        Ok(Response::new(
            watch_allocate_entry::watch_timeline_routes_stream(
                &self.control_plane,
                self.route_cache_ttl_ms,
                request.into_inner(),
            )
            .await?,
        ))
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
                route: Some(status_mapping::proto_timeline_route(
                    route,
                    self.route_cache_ttl_ms,
                )),
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
            watch_allocate_entry::allocate_timestamps_response(
                &self.allocator,
                request.into_inner(),
            )
            .await?,
        ))
    }
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
}
