use prost::Message;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status};

use crate::proto::v1::{
    timeline_control_service_server::TimelineControlService, timeline_route_event,
    timeline_route_service_server::TimelineRouteService,
    timestamp_service_server::TimestampService, AllocateTimestampsRequest,
    AllocateTimestampsResponse, EnsureTimelineRequest, EnsureTimelineResponse, ErrorCode,
    ErrorDetail, GetTimelineRouteRequest, GetTimelineRouteResponse, HealthResponse,
    TimelineRouteEvent, TransferTimelineRequest, TransferTimelineResponse, WatchKeepalive,
    WatchTimelineRoutesRequest,
};

use crate::proto::v1::ResourceTier as ProtoResourceTier;
use crate::proto::v1::TimelineState as ProtoTimelineState;
use crate::proto::v1::TimelineRoute as ProtoTimelineRoute;
use crate::timeline_proxy::{TimelineProxyError, TimelineScopedAllocator};
use crate::{
    metrics, ResourceTier, TimelineLifecycleState, TransferReason, TsoControlPlane, TsoDataPlane,
    TsoError,
};

const WATCH_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const WATCH_ROUTE_SEND_TIMEOUT: Duration = Duration::from_millis(250);

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

fn current_timestamp() -> prost_types::Timestamp {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch");
    prost_types::Timestamp {
        seconds: since_epoch.as_secs() as i64,
        nanos: 0,
    }
}

fn encode_resource_tier(resource_tier: ResourceTier) -> i32 {
    match resource_tier {
        ResourceTier::Shared => ProtoResourceTier::Shared as i32,
        ResourceTier::Warm => ProtoResourceTier::Warm as i32,
        ResourceTier::Dedicated => ProtoResourceTier::Dedicated as i32,
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

fn encode_timeline_state(state: TimelineLifecycleState) -> i32 {
    match state {
        TimelineLifecycleState::Creating => ProtoTimelineState::Creating as i32,
        TimelineLifecycleState::Active => ProtoTimelineState::Active as i32,
        TimelineLifecycleState::Draining => ProtoTimelineState::Draining as i32,
        TimelineLifecycleState::Locked => ProtoTimelineState::Locked as i32,
        TimelineLifecycleState::Recovering => ProtoTimelineState::Recovering as i32,
    }
}

fn build_proto_route(route: crate::TimelineRoute, route_cache_ttl_ms: u32) -> ProtoTimelineRoute {
    ProtoTimelineRoute {
        timeline_key: route.timeline_key,
        generator_id: route.generator_id,
        owner_worker_endpoint: route.owner_worker_endpoint,
        epoch: route.epoch,
        route_version: route.route_version,
        resource_tier: encode_resource_tier(route.resource_tier),
        cache_ttl_ms: route_cache_ttl_ms,
    }
}

fn build_route_event(route: crate::TimelineRoute, route_cache_ttl_ms: u32) -> TimelineRouteEvent {
    TimelineRouteEvent {
        event: Some(timeline_route_event::Event::Route(build_proto_route(
            route,
            route_cache_ttl_ms,
        ))),
    }
}

fn build_keepalive_event() -> TimelineRouteEvent {
    TimelineRouteEvent {
        event: Some(timeline_route_event::Event::Keepalive(WatchKeepalive {
            server_time: Some(current_timestamp()),
        })),
    }
}

fn try_send_keepalive_event(
    event_tx: &tokio::sync::mpsc::Sender<Result<TimelineRouteEvent, Status>>,
) -> bool {
    match event_tx.try_send(Ok(build_keepalive_event())) {
        Ok(()) => true,
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            metrics::TSO_WATCH_KEEPALIVE_DROPPED_TOTAL.inc();
            true
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
    }
}

async fn send_watch_event_with_timeout(
    event_tx: &tokio::sync::mpsc::Sender<Result<TimelineRouteEvent, Status>>,
    event: Result<TimelineRouteEvent, Status>,
) -> bool {
    match tokio::time::timeout(WATCH_ROUTE_SEND_TIMEOUT, event_tx.send(event)).await {
        Ok(Ok(())) => true,
        Ok(Err(_)) => false,
        Err(_) => {
            metrics::TSO_WATCH_ROUTE_SEND_TIMEOUT_TOTAL.inc();
            let _ = event_tx.try_send(Err(Status::resource_exhausted(
                "watch consumer backpressure",
            )));
            false
        }
    }
}

fn build_timestamp_ranges(
    ranges: Vec<crate::TimestampRange>,
) -> Vec<crate::proto::v1::TimestampRange> {
    ranges
        .into_iter()
        .map(|range| crate::proto::v1::TimestampRange {
            start_tso: range.start_tso,
            end_tso: range.end_tso,
        })
        .collect()
}

fn parse_transfer_reason(reason: i32) -> TransferReason {
    match reason {
        1 => TransferReason::Rebalance,
        2 => TransferReason::Hotspot,
        3 => TransferReason::Failover,
        _ => TransferReason::Manual,
    }
}

async fn allocate_with_optional_timeout(
    allocator: &TimelineScopedAllocator,
    request: crate::AllocateTimestampsRequest,
    timeout_ms: u32,
) -> Result<crate::AllocateTimestampsResponse, Status> {
    allocator
        .allocate_timestamps_with_timeout(request, timeout_ms)
        .await
        .map_err(map_timeline_proxy_error)
}

async fn load_initial_routes(
    control_plane: &TsoControlPlane,
    filter_keys: &HashSet<String>,
    delivered_versions: &mut HashMap<String, u64>,
) -> Result<Vec<crate::TimelineRoute>, Status> {
    let mut snapshot_keys = filter_keys.clone();
    snapshot_keys.extend(delivered_versions.keys().cloned());

    let mut initial_routes = Vec::new();
    for timeline_key in snapshot_keys {
        match control_plane.get_timeline_route(&timeline_key).await {
            Ok(route) => {
                let known_version = delivered_versions.get(&timeline_key).copied().unwrap_or(0);
                if route.route_version > known_version {
                    delivered_versions.insert(timeline_key, route.route_version);
                    initial_routes.push(route);
                }
            }
            Err(TsoError::TimelineNotFound { .. }) => {}
            Err(error) => return Err(map_tso_error(error)),
        }
    }

    Ok(initial_routes)
}

async fn send_route_events(
    event_tx: &tokio::sync::mpsc::Sender<Result<TimelineRouteEvent, Status>>,
    routes: Vec<crate::TimelineRoute>,
    route_cache_ttl_ms: u32,
) -> bool {
    for route in routes {
        if !send_watch_event_with_timeout(
            event_tx,
            Ok(build_route_event(route, route_cache_ttl_ms)),
        )
        .await
        {
            return false;
        }
    }
    true
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
                route: Some(build_proto_route(route, self.route_cache_ttl_ms)),
            })),
            Err(e) => Err(map_tso_error(e)),
        }
    }

    type WatchTimelineRoutesStream = ReceiverStream<Result<TimelineRouteEvent, Status>>;

    async fn watch_timeline_routes(
        &self,
        request: Request<WatchTimelineRoutesRequest>,
    ) -> Result<Response<Self::WatchTimelineRoutesStream>, Status> {
        let req = request.into_inner();
        let filter_keys: HashSet<String> = req.timeline_keys.into_iter().collect();
        let mut delivered_versions = req.known_route_versions;
        let mut rx = self.control_plane.subscribe_route_changes();
        let initial_routes =
            load_initial_routes(&self.control_plane, &filter_keys, &mut delivered_versions).await?;
        let control_plane = self.control_plane.clone();
        let route_cache_ttl_ms = self.route_cache_ttl_ms;
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            let mut keepalive_interval = tokio::time::interval(WATCH_KEEPALIVE_INTERVAL);
            keepalive_interval.tick().await;
            if !send_route_events(&event_tx, initial_routes, route_cache_ttl_ms).await {
                return;
            }
            loop {
                tokio::select! {
                    res = rx.recv() => {
                        match res {
                            Ok(route) => {
                                if !filter_keys.is_empty() && !filter_keys.contains(route.timeline_key.as_str()) {
                                    continue;
                                }
                                if let Some(known) = delivered_versions.get(route.timeline_key.as_str()) {
                                    if route.route_version <= *known {
                                        continue;
                                    }
                                }
                                delivered_versions.insert(route.timeline_key.clone(), route.route_version);
                                if !send_watch_event_with_timeout(
                                    &event_tx,
                                    Ok(build_route_event(route, route_cache_ttl_ms)),
                                )
                                .await
                                {
                                    break;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                let routes = match load_initial_routes(
                                    &control_plane,
                                    &filter_keys,
                                    &mut delivered_versions,
                                ).await {
                                    Ok(routes) => routes,
                                    Err(status) => {
                                        let _ = send_watch_event_with_timeout(&event_tx, Err(status)).await;
                                        break;
                                    }
                                };
                                if !send_route_events(&event_tx, routes, route_cache_ttl_ms).await {
                                    break;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    _ = keepalive_interval.tick() => {
                        if !try_send_keepalive_event(&event_tx) {
                            break;
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(event_rx)))
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
                route: Some(build_proto_route(route, self.route_cache_ttl_ms)),
            })),
            Err(e) => Err(map_tso_error(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn route_event_send_times_out_when_watch_queue_is_full() {
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(1);
        event_tx.try_send(Ok(build_keepalive_event())).unwrap();

        let before = metrics::TSO_WATCH_ROUTE_SEND_TIMEOUT_TOTAL.get();
        let delivered = send_watch_event_with_timeout(
            &event_tx,
            Ok(build_keepalive_event()),
        )
        .await;

        assert!(!delivered);
        assert!(metrics::TSO_WATCH_ROUTE_SEND_TIMEOUT_TOTAL.get() >= before + 1);
    }

    #[tokio::test]
    async fn route_event_timeout_only_best_effort_delivers_resource_exhausted() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
        event_tx.try_send(Ok(build_keepalive_event())).unwrap();

        let delivered = send_watch_event_with_timeout(
            &event_tx,
            Ok(build_keepalive_event()),
        )
        .await;

        assert!(!delivered);

        let first = event_rx.recv().await.expect("expected queued event");
        match first {
            Ok(event) => {
                assert!(matches!(event.event, Some(timeline_route_event::Event::Keepalive(_))));
            }
            Err(status) => panic!("unexpected first event status: {status}"),
        }

        let maybe_second = tokio::time::timeout(Duration::from_millis(50), event_rx.recv()).await;
        if let Ok(Some(Err(status))) = maybe_second {
            assert_eq!(status.code(), Code::ResourceExhausted);
            assert!(status.message().contains("backpressure"));
        }
    }

    #[test]
    fn keepalive_is_dropped_when_watch_queue_is_full() {
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(1);
        event_tx.try_send(Ok(build_keepalive_event())).unwrap();

        let before = metrics::TSO_WATCH_KEEPALIVE_DROPPED_TOTAL.get();
        let keep_running = try_send_keepalive_event(&event_tx);

        assert!(keep_running);
        assert!(metrics::TSO_WATCH_KEEPALIVE_DROPPED_TOTAL.get() >= before + 1);
    }

    #[test]
    fn keepalive_success_does_not_increment_drop_metric() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
        let before = metrics::TSO_WATCH_KEEPALIVE_DROPPED_TOTAL.get();

        let keep_running = try_send_keepalive_event(&event_tx);

        assert!(keep_running);
        assert_eq!(metrics::TSO_WATCH_KEEPALIVE_DROPPED_TOTAL.get(), before);
        assert!(event_rx.try_recv().is_ok());
    }

    #[test]
    fn keepalive_returns_false_when_watch_queue_is_closed() {
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);
        drop(event_rx);

        assert!(!try_send_keepalive_event(&event_tx));
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
        let req = request.into_inner();
        let inner_req = crate::AllocateTimestampsRequest {
            timeline_key: req.timeline_key,
            count: req.count,
            expected_epoch: req.expected_epoch,
            expected_route_version: req.expected_route_version,
            client_request_id: req.client_request_id,
        };

        match allocate_with_optional_timeout(&self.allocator, inner_req, req.request_timeout_ms)
            .await
        {
            Ok(resp) => Ok(Response::new(AllocateTimestampsResponse {
                timeline_key: resp.timeline_key,
                generator_id: resp.generator_id,
                epoch: resp.epoch,
                route_version: resp.route_version,
                ranges: build_timestamp_ranges(resp.ranges),
            })),
            Err(status) => Err(status),
        }
    }
}

pub struct TsoControlService {
    control_plane: TsoControlPlane,
}

impl TsoControlService {
    pub fn new(control_plane: TsoControlPlane) -> Self {
        Self { control_plane }
    }
}

#[tonic::async_trait]
impl TimelineControlService for TsoControlService {
    async fn transfer_timeline(
        &self,
        request: Request<TransferTimelineRequest>,
    ) -> Result<Response<TransferTimelineResponse>, Status> {
        let req = request.into_inner();

        let target_owner = if let Some(tw) = req.target_worker_id {
            tw
        } else {
            self.control_plane.advertise_endpoint().to_string()
        };

        let target_gen_id = req.target_generator_id;
        let reason = parse_transfer_reason(req.reason);

        match self
            .control_plane
            .transfer_timeline_for_rpc(&req.timeline_key, target_owner, target_gen_id, reason)
            .await
        {
            Ok((route, old_generator_id, state)) => Ok(Response::new(TransferTimelineResponse {
                timeline_key: route.timeline_key,
                old_generator_id,
                new_generator_id: route.generator_id,
                new_epoch: route.epoch,
                route_version: route.route_version,
                state: encode_timeline_state(state),
            })),
            Err(e) => Err(map_tso_error(e)),
        }
    }

    async fn health(&self, _request: Request<()>) -> Result<Response<HealthResponse>, Status> {
        let health_info = self.control_plane.health();
        Ok(Response::new(HealthResponse {
            service: "chronos-tso".to_string(),
            instance_id: health_info.instance_id,
            server_time: Some(current_timestamp()),
        }))
    }
}

fn error_detail_code(err: &TsoError) -> ErrorCode {
    match err {
        TsoError::TimelineNotFound { .. } => ErrorCode::TimelineNotFound,
        TsoError::RouteVersionMismatch { .. } => ErrorCode::RouteVersionMismatch,
        TsoError::EpochMismatch { .. } => ErrorCode::EpochMismatch,
        TsoError::LeaseExpired { .. } | TsoError::GeneratorLeaseExpired { .. } => {
            ErrorCode::LeaseExpired
        }
        TsoError::NotTimelineOwner { .. }
        | TsoError::NotGeneratorOwner { .. }
        | TsoError::GeneratorNotOwnedByThisWorker { .. } => ErrorCode::NotTimelineOwner,
        TsoError::BatchTooLarge { .. }
        | TsoError::InvalidCount
        | TsoError::TargetGeneratorIdRequired { .. }
        | TsoError::SharedGeneratorJumpAheadTooLarge { .. }
        | TsoError::GeneratorOwnershipMisconfigured { .. }
        | TsoError::GeneratorIdOutOfRange { .. }
        | TsoError::TsoOverflow => ErrorCode::InvalidArgument,
        TsoError::FutureBorrowExceeded { .. }
        | TsoError::IssuedUpperBoundExceeded { .. }
        | TsoError::GeneratorPoolExhausted => ErrorCode::RateLimited,
        TsoError::FailoverRequiresExpiredLease { .. } | TsoError::CasFailed => {
            ErrorCode::TemporarilyUnavailable
        }
        TsoError::FailoverMissingRecoveryFloor { .. } => ErrorCode::TemporarilyUnavailable,
        TsoError::TimelineNotReady { .. } => ErrorCode::TemporarilyUnavailable,
        TsoError::TimelineIngressSaturated { .. } => ErrorCode::TemporarilyUnavailable,
        TsoError::TimelineRuntimeCacheSaturated { .. } => ErrorCode::TemporarilyUnavailable,
        TsoError::ClockBackwards { .. }
        | TsoError::MetadataAlreadyExists
        | TsoError::Internal(_) => ErrorCode::Internal,
    }
}

fn build_error_detail(err: &TsoError) -> ErrorDetail {
    let mut detail = ErrorDetail {
        code: error_detail_code(err) as i32,
        message: err.to_string(),
        current_epoch: 0,
        current_route_version: 0,
        redirect_endpoint: String::new(),
    };

    match err {
        TsoError::RouteVersionMismatch { actual, .. } => {
            detail.current_route_version = *actual;
        }
        TsoError::EpochMismatch { actual, .. } => {
            detail.current_epoch = *actual;
        }
        TsoError::NotTimelineOwner {
            owner_worker_endpoint,
        }
        | TsoError::NotGeneratorOwner {
            owner_worker_endpoint,
            ..
        } => {
            detail.redirect_endpoint = owner_worker_endpoint.clone();
        }
        _ => {}
    }

    detail
}

fn status_with_error_detail(code: Code, err: TsoError) -> Status {
    let detail = build_error_detail(&err);
    let mut encoded_detail = Vec::new();
    detail
        .encode(&mut encoded_detail)
        .expect("ErrorDetail encoding should succeed");
    Status::with_details(
        code,
        err.to_string(),
        prost::bytes::Bytes::from(encoded_detail),
    )
}

fn map_tso_error(err: TsoError) -> Status {
    match err {
        TsoError::TimelineNotFound { .. } => status_with_error_detail(Code::NotFound, err),
        TsoError::RouteVersionMismatch { .. } | TsoError::EpochMismatch { .. } => {
            status_with_error_detail(Code::FailedPrecondition, err)
        }
        TsoError::LeaseExpired { .. } => status_with_error_detail(Code::Unavailable, err),
        TsoError::TimelineNotReady { .. } => status_with_error_detail(Code::Unavailable, err),
        TsoError::TimelineIngressSaturated { .. } => {
            status_with_error_detail(Code::Unavailable, err)
        }
        TsoError::TimelineRuntimeCacheSaturated { .. } => {
            status_with_error_detail(Code::Unavailable, err)
        }
        TsoError::FailoverRequiresExpiredLease { .. } => {
            status_with_error_detail(Code::FailedPrecondition, err)
        }
        TsoError::FailoverMissingRecoveryFloor { .. } => {
            status_with_error_detail(Code::FailedPrecondition, err)
        }
        TsoError::NotTimelineOwner { .. } => {
            status_with_error_detail(Code::FailedPrecondition, err)
        }
        TsoError::GeneratorLeaseExpired { .. } => status_with_error_detail(Code::Unavailable, err),
        TsoError::NotGeneratorOwner { .. }
        | TsoError::GeneratorNotOwnedByThisWorker { .. }
        | TsoError::GeneratorOwnershipMisconfigured { .. } => {
            status_with_error_detail(Code::FailedPrecondition, err)
        }
        TsoError::ClockBackwards { .. } => status_with_error_detail(Code::Internal, err),
        TsoError::BatchTooLarge { .. } | TsoError::InvalidCount => {
            status_with_error_detail(Code::InvalidArgument, err)
        }
        TsoError::TargetGeneratorIdRequired { .. } => {
            status_with_error_detail(Code::InvalidArgument, err)
        }
        TsoError::SharedGeneratorJumpAheadTooLarge { .. } => {
            status_with_error_detail(Code::FailedPrecondition, err)
        }
        TsoError::FutureBorrowExceeded { .. } => {
            status_with_error_detail(Code::ResourceExhausted, err)
        }
        TsoError::IssuedUpperBoundExceeded { .. } => {
            status_with_error_detail(Code::ResourceExhausted, err)
        }
        TsoError::GeneratorPoolExhausted => status_with_error_detail(Code::ResourceExhausted, err),
        TsoError::GeneratorIdOutOfRange { .. } => status_with_error_detail(Code::OutOfRange, err),
        TsoError::TsoOverflow => status_with_error_detail(Code::OutOfRange, err),
        TsoError::MetadataAlreadyExists => status_with_error_detail(Code::AlreadyExists, err),
        TsoError::CasFailed => status_with_error_detail(Code::Aborted, err),
        TsoError::Internal(_) => status_with_error_detail(Code::Internal, err),
    }
}

fn map_timeline_proxy_error(err: TimelineProxyError) -> Status {
    match err {
        TimelineProxyError::Tso(err) => map_tso_error(err),
        TimelineProxyError::TimedOut => Status::deadline_exceeded("AllocateTimestamps timed out"),
    }
}
