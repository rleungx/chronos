//! Application-facing Chronos client.
//!
//! The intended usage model is:
//!
//! 1. Bind one client to one timeline during initialization.
//! 2. Call `allocate_timestamps(count)` during normal operation.
//!
//! Internal route management stays inside the client. The client ensures the timeline,
//! fetches the current route, caches it, and refreshes it on stale-route errors.
//! Stale-route conditions such as owner, epoch, or route-version mismatch are retried
//! internally once after a route refresh. Other RPC failures are returned to the caller.
//!
//! ```no_run
//! use chronos::{Client, ClientConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::connect(
//!     "127.0.0.1:50051",
//!     "orders.primary",
//! )
//! .await?;
//!
//! let ranges = client.allocate_timestamps(1).await?;
//! println!("tso={}", ranges[0].start_tso);
//! # Ok(())
//! # }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use prost::Message;
use thiserror::Error;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::proto::v1::{
    timeline_route_service_client::TimelineRouteServiceClient,
    timestamp_service_client::TimestampServiceClient, AllocateTimestampsRequest,
    EnsureTimelineRequest, ErrorCode, ErrorDetail, GetTimelineRouteRequest, ResourceTier,
    TimelineRoute, TimestampRange,
};

#[derive(Debug, Clone)]
pub struct ClientConfig {
    timeline_key: String,
    desired_resource_tier: ResourceTier,
    request_timeout_ms: u32,
}

impl ClientConfig {
    /// Creates a config for a single bound timeline.
    pub fn new(timeline_key: impl Into<String>) -> Self {
        Self {
            timeline_key: timeline_key.into(),
            desired_resource_tier: ResourceTier::Shared,
            request_timeout_ms: 0,
        }
    }

    /// Sets the resource tier used when the client ensures the timeline.
    pub fn with_desired_resource_tier(mut self, desired_resource_tier: ResourceTier) -> Self {
        self.desired_resource_tier = desired_resource_tier;
        self
    }

    /// Sets the per-request timeout forwarded to `AllocateTimestamps`.
    pub fn with_request_timeout_ms(mut self, request_timeout_ms: u32) -> Self {
        self.request_timeout_ms = request_timeout_ms;
        self
    }

}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid or unreachable endpoint {endpoint}: {source}")]
    Endpoint {
        endpoint: String,
        #[source]
        source: tonic::transport::Error,
    },
    /// A gRPC error returned by Chronos after client-side refresh/retry handling.
    ///
    /// Route staleness is handled internally. Errors returned here are terminal for the
    /// current call unless the application chooses to apply its own higher-level retry policy.
    #[error(transparent)]
    Rpc(Box<Status>),
    #[error("chronos returned no route from {operation}")]
    MissingRoute { operation: &'static str },
}

impl From<Status> for ClientError {
    fn from(status: Status) -> Self {
        Self::Rpc(Box::new(status))
    }
}

/// A client bound to a single timeline.
///
/// Applications should create one client per logical timeline and then call
/// `allocate_timestamps` as needed. Route ownership changes are handled internally.
pub struct Client {
    route_client: Mutex<TimelineRouteServiceClient<Channel>>,
    tso_client: Mutex<TimestampServiceClient<Channel>>,
    owner_endpoint: Mutex<String>,
    route: Mutex<TimelineRoute>,
    config: ClientConfig,
    request_id: AtomicU64,
}

impl Client {
    /// Connects to a Chronos server and initializes the bound timeline.
    pub async fn connect(
        endpoint: impl Into<String>,
        timeline_key: impl Into<String>,
    ) -> Result<Self, ClientError> {
        Self::connect_with_config(endpoint, ClientConfig::new(timeline_key)).await
    }

    /// Connects to a Chronos server with explicit client configuration.
    pub async fn connect_with_config(
        endpoint: impl Into<String>,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        let endpoint = endpoint.into();
        let channel = Endpoint::from_shared(normalize_endpoint(&endpoint))
            .map_err(|source| ClientError::Endpoint {
                endpoint: endpoint.clone(),
                source,
            })?
            .connect()
            .await
            .map_err(|source| ClientError::Endpoint {
                endpoint: endpoint.clone(),
                source,
            })?;
        Self::with_channel(channel, config).await
    }

    pub async fn with_channel(
        channel: Channel,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        let mut route_client = TimelineRouteServiceClient::new(channel.clone());
        let ensured_route = route_client
            .ensure_timeline(tonic::Request::new(EnsureTimelineRequest {
                timeline_key: config.timeline_key.clone(),
                desired_resource_tier: config.desired_resource_tier as i32,
            }))
            .await?
            .into_inner()
            .route
            .ok_or(ClientError::MissingRoute {
                operation: "ensure_timeline",
            })?;
        let tso_client = TimestampServiceClient::new(connect_channel(
            &ensured_route.owner_worker_endpoint,
        )?);

        Ok(Self {
            route_client: Mutex::new(route_client),
            tso_client: Mutex::new(tso_client),
            owner_endpoint: Mutex::new(ensured_route.owner_worker_endpoint.clone()),
            route: Mutex::new(ensured_route),
            config,
            request_id: AtomicU64::new(1),
        })
    }

    /// Allocates one or more timestamp ranges from the bound timeline.
    ///
    /// This method refreshes the cached route and retries once when Chronos reports that the
    /// current route is stale. Other failures are returned as `ClientError`.
    pub async fn allocate_timestamps(
        &self,
        count: u32,
    ) -> Result<Vec<TimestampRange>, ClientError> {
        let route = self.route.lock().await.clone();
        match self.allocate_once(&route, count).await {
            Ok(ranges) => Ok(ranges),
            Err(status) if is_stale_route_error(&status) => {
                let route = self.refresh_route().await?;
                self.allocate_once(&route, count)
                    .await
                    .map_err(ClientError::from)
            }
            Err(status) => Err(status.into()),
        }
    }

    async fn refresh_route(&self) -> Result<TimelineRoute, ClientError> {
        let route = self
            .route_client
            .lock()
            .await
            .get_timeline_route(tonic::Request::new(GetTimelineRouteRequest {
                timeline_key: self.config.timeline_key.clone(),
            }))
            .await?
            .into_inner()
            .route
            .ok_or(ClientError::MissingRoute {
                operation: "get_timeline_route",
            })?;
        self.ensure_owner_client(&route.owner_worker_endpoint).await?;
        *self.route.lock().await = route.clone();
        Ok(route)
    }

    async fn ensure_owner_client(&self, owner_endpoint: &str) -> Result<(), ClientError> {
        let mut current_owner = self.owner_endpoint.lock().await;
        if current_owner.as_str() == owner_endpoint {
            return Ok(());
        }

        let channel = connect_channel(owner_endpoint)?;
        *self.tso_client.lock().await = TimestampServiceClient::new(channel);
        *current_owner = owner_endpoint.to_string();
        Ok(())
    }

    async fn allocate_once(
        &self,
        route: &TimelineRoute,
        count: u32,
    ) -> Result<Vec<TimestampRange>, Status> {
        Ok(self
            .tso_client
            .lock()
            .await
            .allocate_timestamps(tonic::Request::new(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: format!(
                    "{}-{}",
                    self.config.timeline_key,
                    self.request_id.fetch_add(1, Ordering::Relaxed)
                ),
                request_timeout_ms: self.config.request_timeout_ms,
            }))
            .await?
            .into_inner()
            .ranges)
    }
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

fn connect_channel(endpoint: &str) -> Result<Channel, ClientError> {
    let normalized = normalize_endpoint(endpoint);
    let endpoint = Endpoint::from_shared(normalized).map_err(|source| ClientError::Endpoint {
            endpoint: endpoint.to_string(),
            source,
        })?;
    Ok(endpoint.connect_lazy())
}

fn is_stale_route_error(status: &Status) -> bool {
    if status.code() != Code::FailedPrecondition {
        return false;
    }

    let Ok(detail) = ErrorDetail::decode(status.details()) else {
        return false;
    };

    matches!(
        ErrorCode::try_from(detail.code).ok(),
        Some(ErrorCode::NotTimelineOwner)
            | Some(ErrorCode::RouteVersionMismatch)
            | Some(ErrorCode::EpochMismatch)
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex as StdMutex};

    use prost::Message;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Code, Request, Response};
    use tonic::transport::Server;

    use crate::metadata::MemoryMetadataStore;
    use crate::proto::v1::{
        timeline_route_service_server::TimelineRouteServiceServer,
        timestamp_service_server::TimestampServiceServer,
        AllocateTimestampsResponse, EnsureTimelineRequest, EnsureTimelineResponse, GetTimelineRouteRequest,
        GetTimelineRouteResponse, TimelineRoute,
    };
    use crate::rpc::{TsoRouteService, TsoTimestampService};
    use crate::{ManualClock, TsoConfig, TsoSecurityMode, TsoService};

    use super::*;

    async fn spawn_test_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");

        let metadata = Arc::new(MemoryMetadataStore::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let service = TsoService::new(
            TsoConfig {
                advertise_endpoint: addr.to_string(),
                ..TsoConfig::default().with_security_mode(TsoSecurityMode::DevInsecure)
            },
            clock,
            metadata,
        )
        .expect("service should start");

        let route_service = TsoRouteService::new(service.control_plane());
        let timestamp_service = TsoTimestampService::new(service.data_plane());
        let incoming = TcpListenerStream::new(listener);

        tokio::spawn(async move {
            Server::builder()
                .add_service(TimelineRouteServiceServer::new(route_service))
                .add_service(TimestampServiceServer::new(timestamp_service))
                .serve_with_incoming(incoming)
                .await
                .expect("server should serve");
        });

        addr.to_string()
    }

    #[derive(Clone)]
    struct FakeRouteService {
        route: Arc<StdMutex<TimelineRoute>>,
    }

    #[tonic::async_trait]
    impl crate::proto::v1::timeline_route_service_server::TimelineRouteService for FakeRouteService {
        async fn get_timeline_route(
            &self,
            request: Request<GetTimelineRouteRequest>,
        ) -> Result<Response<GetTimelineRouteResponse>, Status> {
            let mut route = self.route.lock().unwrap().clone();
            route.timeline_key = request.into_inner().timeline_key;
            Ok(Response::new(GetTimelineRouteResponse { route: Some(route) }))
        }

        async fn ensure_timeline(
            &self,
            request: Request<EnsureTimelineRequest>,
        ) -> Result<Response<EnsureTimelineResponse>, Status> {
            let mut route = self.route.lock().unwrap();
            route.timeline_key = request.into_inner().timeline_key;
            Ok(Response::new(EnsureTimelineResponse {
                route: Some(route.clone()),
            }))
        }
    }

    #[derive(Clone)]
    struct FakeTimestampService {
        route: Arc<StdMutex<TimelineRoute>>,
    }

    #[tonic::async_trait]
    impl crate::proto::v1::timestamp_service_server::TimestampService for FakeTimestampService {
        async fn allocate_timestamps(
            &self,
            request: Request<AllocateTimestampsRequest>,
        ) -> Result<Response<AllocateTimestampsResponse>, Status> {
            let request = request.into_inner();
            let route = self.route.lock().unwrap().clone();
            if request.expected_epoch != route.epoch
                || request.expected_route_version != route.route_version
            {
                let detail = ErrorDetail {
                    code: ErrorCode::RouteVersionMismatch as i32,
                    message: "stale route".into(),
                    current_epoch: route.epoch,
                    current_route_version: route.route_version,
                    redirect_endpoint: route.owner_worker_endpoint.clone(),
                    action_blocker: 0,
                    next_step: 0,
                }
                .encode_to_vec();
                return Err(Status::with_details(
                    Code::FailedPrecondition,
                    "stale route",
                    prost::bytes::Bytes::from(detail),
                ));
            }

            Ok(Response::new(AllocateTimestampsResponse {
                timeline_key: request.timeline_key,
                generator_id: route.generator_id,
                epoch: route.epoch,
                route_version: route.route_version,
                ranges: vec![crate::proto::v1::TimestampRange {
                    start_tso: 100,
                    end_tso: 100 + request.count as u64 - 1,
                }],
            }))
        }
    }

    async fn spawn_route_only_server(route: TimelineRoute) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have local addr");
        let incoming = TcpListenerStream::new(listener);
        let route_service = FakeRouteService {
            route: Arc::new(StdMutex::new(route)),
        };

        tokio::spawn(async move {
            Server::builder()
                .add_service(TimelineRouteServiceServer::new(route_service))
                .serve_with_incoming(incoming)
                .await
                .expect("server should serve");
        });

        addr.to_string()
    }

    async fn spawn_timestamp_only_server(route: TimelineRoute) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have local addr");
        let incoming = TcpListenerStream::new(listener);
        let timestamp_service = FakeTimestampService {
            route: Arc::new(StdMutex::new(route)),
        };

        tokio::spawn(async move {
            Server::builder()
                .add_service(TimestampServiceServer::new(timestamp_service))
                .serve_with_incoming(incoming)
                .await
                .expect("server should serve");
        });

        addr.to_string()
    }

    #[tokio::test]
    async fn connect_initializes_timeline_and_allocation_only_needs_count() {
        let endpoint = spawn_test_server().await;
        let client = Client::connect(endpoint, "orders.primary")
            .await
            .expect("client should connect");

        let ranges = client
            .allocate_timestamps(1)
            .await
            .expect("allocation should succeed");

        assert_eq!(ranges.len(), 1);
    }

    #[tokio::test]
    async fn allocate_refreshes_stale_cached_route_and_retries() {
        let endpoint = spawn_test_server().await;
        let client = Client::connect(endpoint, "orders.primary")
            .await
            .expect("client should connect");

        {
            let mut route = client.route.lock().await;
            route.route_version += 1;
        }

        let ranges = client
            .allocate_timestamps(1)
            .await
            .expect("allocation should recover after route refresh");

        assert_eq!(ranges.len(), 1);
    }

    #[tokio::test]
    async fn allocate_routes_to_owner_endpoint_from_route_response() {
        let owner_addr = spawn_timestamp_only_server(TimelineRoute {
            timeline_key: "orders.primary".into(),
            generator_id: 7,
            owner_worker_endpoint: "127.0.0.1:0".into(),
            epoch: 3,
            route_version: 11,
            resource_tier: ResourceTier::Shared as i32,
        })
        .await;

        let route_addr = spawn_route_only_server(TimelineRoute {
            timeline_key: "orders.primary".into(),
            generator_id: 7,
            owner_worker_endpoint: owner_addr.clone(),
            epoch: 3,
            route_version: 11,
            resource_tier: ResourceTier::Shared as i32,
        })
        .await;

        let client = Client::connect(route_addr, "orders.primary")
            .await
            .expect("client should connect");

        let ranges = client
            .allocate_timestamps(1)
            .await
            .expect("allocation should succeed against owner endpoint");

        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start_tso, 100);
    }

    #[test]
    fn normalize_endpoint_accepts_bare_host_port() {
        assert_eq!(
            normalize_endpoint("127.0.0.1:50051"),
            "http://127.0.0.1:50051"
        );
        assert_eq!(
            normalize_endpoint("https://chronos.internal:50051"),
            "https://chronos.internal:50051"
        );
    }
}
