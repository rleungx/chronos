use std::pin::Pin;

use futures::Stream;
use tonic::{Request, Response, Status};

use crate::authz::PeerCertAuthorizer;
use crate::config::TsoConfigValidationError;
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
    TsoControlPlane, TsoDataPlane, TsoError,
};

use super::{status_mapping, translation};

pub struct TsoRouteService {
    control_plane: TsoControlPlane,
    authorizer: PeerCertAuthorizer,
}

impl TsoRouteService {
    pub fn new(control_plane: TsoControlPlane) -> Self {
        Self {
            control_plane,
            authorizer: PeerCertAuthorizer::disabled("TimelineRouteService"),
        }
    }

    pub fn with_allowlist(
        control_plane: TsoControlPlane,
        allowlist: &[String],
    ) -> Result<Self, TsoConfigValidationError> {
        Ok(Self {
            control_plane,
            authorizer: PeerCertAuthorizer::from_allowlist("TimelineRouteService", allowlist)
                .map_err(TsoConfigValidationError::Security)?,
        })
    }

    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        self.authorizer.authorize(request)
    }
}

fn decode_resource_tier(resource_tier: i32) -> Result<ResourceTier, TsoError> {
    match ProtoResourceTier::try_from(resource_tier) {
        Ok(ProtoResourceTier::Shared) => Ok(ResourceTier::Shared),
        Ok(ProtoResourceTier::Warm) => Ok(ResourceTier::Warm),
        Ok(ProtoResourceTier::Dedicated) => Ok(ResourceTier::Dedicated),
        Ok(ProtoResourceTier::Unspecified) | Err(_) => Err(TsoError::InvalidResourceTier {
            value: resource_tier,
        }),
    }
}

#[tonic::async_trait]
impl TimelineRouteService for TsoRouteService {
    async fn get_timeline_route(
        &self,
        request: Request<GetTimelineRouteRequest>,
    ) -> Result<Response<GetTimelineRouteResponse>, Status> {
        self.authorize(&request)?;
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
        self.authorize(&request)?;
        let req = request.into_inner();
        let tier =
            decode_resource_tier(req.desired_resource_tier).map_err(translation::map_tso_error)?;

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
    authorizer: PeerCertAuthorizer,
}

impl TsoTimestampService {
    pub fn new(data_plane: TsoDataPlane) -> Self {
        Self {
            allocator: TimelineScopedAllocator::new(data_plane),
            authorizer: PeerCertAuthorizer::disabled("TimestampService"),
        }
    }

    pub fn with_allowlist(
        data_plane: TsoDataPlane,
        allowlist: &[String],
    ) -> Result<Self, TsoConfigValidationError> {
        Ok(Self {
            allocator: TimelineScopedAllocator::new(data_plane),
            authorizer: PeerCertAuthorizer::from_allowlist("TimestampService", allowlist)
                .map_err(TsoConfigValidationError::Security)?,
        })
    }

    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        self.authorizer.authorize(request)
    }
}

#[tonic::async_trait]
impl TimestampService for TsoTimestampService {
    type AllocateTimestampsStreamStream =
        Pin<Box<dyn Stream<Item = Result<AllocateTimestampsResponse, Status>> + Send + 'static>>;

    async fn allocate_timestamps(
        &self,
        request: Request<AllocateTimestampsRequest>,
    ) -> Result<Response<AllocateTimestampsResponse>, Status> {
        self.authorize(&request)?;
        Ok(Response::new(
            allocate_timestamps_response(&self.allocator, request.into_inner()).await?,
        ))
    }

    async fn allocate_timestamps_stream(
        &self,
        request: Request<tonic::Streaming<AllocateTimestampsRequest>>,
    ) -> Result<Response<Self::AllocateTimestampsStreamStream>, Status> {
        self.authorize(&request)?;
        let allocator = self.allocator.clone();
        let responses = futures::stream::try_unfold(
            (request.into_inner(), allocator),
            |(mut requests, allocator)| async move {
                let Some(request) = requests.message().await? else {
                    return Ok(None);
                };
                let response = allocate_timestamps_response(&allocator, request).await?;
                Ok(Some((response, (requests, allocator))))
            },
        );
        Ok(Response::new(Box::pin(responses)))
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
    use std::sync::Arc;
    use tonic::Code;

    #[test]
    fn decode_resource_tier_rejects_invalid_values() {
        assert!(decode_resource_tier(-1).is_err());
        assert!(decode_resource_tier(ProtoResourceTier::Unspecified as i32).is_err());
        assert_eq!(
            decode_resource_tier(ProtoResourceTier::Warm as i32).unwrap(),
            ResourceTier::Warm
        );
        assert_eq!(
            decode_resource_tier(ProtoResourceTier::Dedicated as i32).unwrap(),
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

    #[tokio::test]
    async fn route_service_allowlist_requires_peer_certificate() {
        let service = crate::TsoService::new(
            crate::test_tls::required_grpc_tls_test_config(crate::TsoConfig::default(), 100),
            Arc::new(crate::ManualClock::new(1)),
            Arc::new(crate::metadata::MemoryMetadataStore::new()),
        )
        .unwrap();
        let route_service = TsoRouteService::with_allowlist(
            service.control_plane(),
            &[crate::test_tls::placeholder_client_cert_fingerprint().into()],
        )
        .unwrap();

        let error = route_service
            .get_timeline_route(Request::new(GetTimelineRouteRequest {
                timeline_key: "auth.route".into(),
            }))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::Unauthenticated);
    }

    #[tokio::test]
    async fn timestamp_service_allowlist_requires_peer_certificate() {
        let service = crate::TsoService::new(
            crate::test_tls::required_grpc_tls_test_config(crate::TsoConfig::default(), 100),
            Arc::new(crate::ManualClock::new(1)),
            Arc::new(crate::metadata::MemoryMetadataStore::new()),
        )
        .unwrap();
        let timestamp_service = TsoTimestampService::with_allowlist(
            service.data_plane(),
            &[crate::test_tls::placeholder_client_cert_fingerprint().into()],
        )
        .unwrap();

        let error = timestamp_service
            .allocate_timestamps(Request::new(AllocateTimestampsRequest {
                timeline_key: "auth.timestamp".into(),
                count: 1,
                expected_epoch: 1,
                expected_route_version: 1,
                client_request_id: "auth".into(),
                request_timeout_ms: 0,
            }))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::Unauthenticated);
    }
}
