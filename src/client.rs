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
//! internally after route refresh. Other RPC failures are returned to the caller.
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
use std::time::Duration;

use prost::Message;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Code, Status};

use crate::proto::v1::{
    timeline_route_service_client::TimelineRouteServiceClient,
    timestamp_service_client::TimestampServiceClient, AllocateTimestampsRequest,
    EnsureTimelineRequest, ErrorCode, ErrorDetail, GetTimelineRouteRequest, ResourceTier,
    TimelineRoute, TimestampRange,
};

const DEFAULT_STALE_ROUTE_RETRY_ATTEMPTS: u32 = 3;
const DEFAULT_STALE_ROUTE_RETRY_BACKOFF_MS: u64 = 5;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    timeline_key: String,
    desired_resource_tier: ResourceTier,
    request_timeout_ms: u32,
    stale_route_retry_attempts: u32,
    stale_route_retry_backoff_ms: u64,
    transport: ClientTransportConfig,
}

#[derive(Debug, Clone, Default)]
pub struct ClientTransportConfig {
    insecure: bool,
    ca_pem: Option<Vec<u8>>,
    client_cert_pem: Option<Vec<u8>>,
    client_key_pem: Option<Vec<u8>>,
    domain_name: Option<String>,
}

impl ClientConfig {
    /// Creates a config for a single bound timeline.
    pub fn new(timeline_key: impl Into<String>) -> Self {
        Self {
            timeline_key: timeline_key.into(),
            desired_resource_tier: ResourceTier::Shared,
            request_timeout_ms: 0,
            stale_route_retry_attempts: DEFAULT_STALE_ROUTE_RETRY_ATTEMPTS,
            stale_route_retry_backoff_ms: DEFAULT_STALE_ROUTE_RETRY_BACKOFF_MS,
            transport: ClientTransportConfig::default(),
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

    /// Sets how many stale-route refresh retries a single allocation may perform.
    pub fn with_stale_route_retry_attempts(mut self, stale_route_retry_attempts: u32) -> Self {
        self.stale_route_retry_attempts = stale_route_retry_attempts;
        self
    }

    /// Sets the backoff between stale-route refresh retries.
    pub fn with_stale_route_retry_backoff_ms(mut self, stale_route_retry_backoff_ms: u64) -> Self {
        self.stale_route_retry_backoff_ms = stale_route_retry_backoff_ms;
        self
    }

    /// Sets the secure transport configuration used for initial route connect and owner reconnects.
    pub fn with_transport(mut self, transport: ClientTransportConfig) -> Self {
        self.transport = transport;
        self
    }
}

impl ClientTransportConfig {
    /// Uses explicit plaintext transport instead of TLS.
    pub fn with_insecure(mut self, insecure: bool) -> Self {
        self.insecure = insecure;
        self
    }

    /// Supplies PEM-encoded CA roots for TLS server verification.
    pub fn with_ca_pem(mut self, ca_pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem = Some(ca_pem.into());
        self
    }

    /// Supplies PEM-encoded client certificate and private key for mutual TLS.
    pub fn with_client_identity_pem(
        mut self,
        client_cert_pem: impl Into<Vec<u8>>,
        client_key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_cert_pem = Some(client_cert_pem.into());
        self.client_key_pem = Some(client_key_pem.into());
        self
    }

    /// Overrides the TLS server name / authority used during verification.
    pub fn with_domain_name(mut self, domain_name: impl Into<String>) -> Self {
        self.domain_name = Some(domain_name.into());
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
    #[error("invalid client transport configuration: {message}")]
    InvalidTransportConfig { message: String },
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
    tso_client: RwLock<TimestampServiceClient<Channel>>,
    owner_endpoint: Mutex<String>,
    route: RwLock<TimelineRoute>,
    route_refresh: Mutex<()>,
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
        let channel = build_endpoint(&endpoint, &config.transport)?
            .connect()
            .await
            .map_err(|source| ClientError::Endpoint {
                endpoint: endpoint.clone(),
                source,
            })?;
        Self::with_channel(channel, config).await
    }

    pub async fn with_channel(channel: Channel, config: ClientConfig) -> Result<Self, ClientError> {
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
            &config.transport,
        )?);

        Ok(Self {
            route_client: Mutex::new(route_client),
            tso_client: RwLock::new(tso_client),
            owner_endpoint: Mutex::new(ensured_route.owner_worker_endpoint.clone()),
            route: RwLock::new(ensured_route),
            route_refresh: Mutex::new(()),
            config,
            request_id: AtomicU64::new(1),
        })
    }

    /// Allocates one or more timestamp ranges from the bound timeline.
    ///
    /// This method refreshes the cached route and retries when Chronos reports that the
    /// current route is stale. Other failures are returned as `ClientError`.
    pub async fn allocate_timestamps(
        &self,
        count: u32,
    ) -> Result<Vec<TimestampRange>, ClientError> {
        let client_request_id = self.next_client_request_id();
        let mut stale_retries = 0;

        loop {
            let route = self.route.read().await.clone();
            let tso_client = self.tso_client.read().await.clone();
            match self
                .allocate_once(tso_client, &route, count, &client_request_id)
                .await
            {
                Ok(ranges) => return Ok(ranges),
                Err(status)
                    if is_stale_route_error(&status)
                        && stale_retries < self.config.stale_route_retry_attempts =>
                {
                    stale_retries += 1;
                    self.refresh_route_if_unchanged(&route).await?;
                    if self.config.stale_route_retry_backoff_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(
                            self.config.stale_route_retry_backoff_ms,
                        ))
                        .await;
                    }
                }
                Err(status) => return Err(status.into()),
            }
        }
    }

    async fn refresh_route_if_unchanged(
        &self,
        observed_route: &TimelineRoute,
    ) -> Result<TimelineRoute, ClientError> {
        let _refresh_guard = self.route_refresh.lock().await;
        let current_route = self.route.read().await.clone();
        if !same_route_identity(&current_route, observed_route) {
            self.ensure_owner_client(&current_route.owner_worker_endpoint)
                .await?;
            return Ok(current_route);
        }

        self.refresh_route().await
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
        self.ensure_owner_client(&route.owner_worker_endpoint)
            .await?;
        *self.route.write().await = route.clone();
        Ok(route)
    }

    async fn ensure_owner_client(&self, owner_endpoint: &str) -> Result<(), ClientError> {
        let mut current_owner = self.owner_endpoint.lock().await;
        if current_owner.as_str() == owner_endpoint {
            return Ok(());
        }

        let channel = connect_channel(owner_endpoint, &self.config.transport)?;
        *self.tso_client.write().await = TimestampServiceClient::new(channel);
        *current_owner = owner_endpoint.to_string();
        Ok(())
    }

    async fn allocate_once(
        &self,
        mut tso_client: TimestampServiceClient<Channel>,
        route: &TimelineRoute,
        count: u32,
        client_request_id: &str,
    ) -> Result<Vec<TimestampRange>, Status> {
        self.allocate_with_client(&mut tso_client, route, count, client_request_id)
            .await
    }

    async fn allocate_with_client(
        &self,
        tso_client: &mut TimestampServiceClient<Channel>,
        route: &TimelineRoute,
        count: u32,
        client_request_id: &str,
    ) -> Result<Vec<TimestampRange>, Status> {
        Ok(tso_client
            .allocate_timestamps(tonic::Request::new(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: client_request_id.to_string(),
                request_timeout_ms: self.config.request_timeout_ms,
            }))
            .await?
            .into_inner()
            .ranges)
    }

    fn next_client_request_id(&self) -> String {
        format!(
            "{}-{}",
            self.config.timeline_key,
            self.request_id.fetch_add(1, Ordering::Relaxed)
        )
    }
}

fn same_route_identity(left: &TimelineRoute, right: &TimelineRoute) -> bool {
    left.timeline_key == right.timeline_key
        && left.generator_id == right.generator_id
        && left.owner_worker_endpoint == right.owner_worker_endpoint
        && left.epoch == right.epoch
        && left.route_version == right.route_version
        && left.resource_tier == right.resource_tier
}

fn normalize_endpoint(endpoint: &str, insecure: bool) -> String {
    if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        let scheme = if insecure { "http" } else { "https" };
        format!("{scheme}://{endpoint}")
    }
}

fn build_endpoint(
    endpoint: &str,
    transport: &ClientTransportConfig,
) -> Result<Endpoint, ClientError> {
    validate_transport_config(transport)?;
    let normalized = normalize_endpoint(endpoint, transport.insecure);
    let endpoint_label = endpoint.to_string();
    let mut endpoint =
        Endpoint::from_shared(normalized.clone()).map_err(|source| ClientError::Endpoint {
            endpoint: endpoint_label.clone(),
            source,
        })?;
    if should_use_tls(&normalized, transport) {
        let mut tls = ClientTlsConfig::new();
        if let Some(ca_pem) = &transport.ca_pem {
            tls = tls.ca_certificate(Certificate::from_pem(ca_pem.clone()));
        }
        if let (Some(cert_pem), Some(key_pem)) =
            (&transport.client_cert_pem, &transport.client_key_pem)
        {
            tls = tls.identity(Identity::from_pem(cert_pem.clone(), key_pem.clone()));
        }
        if let Some(domain_name) = &transport.domain_name {
            tls = tls.domain_name(domain_name.clone());
        }
        endpoint = endpoint
            .tls_config(tls)
            .map_err(|source| ClientError::Endpoint {
                endpoint: endpoint_label,
                source,
            })?;
    }
    Ok(endpoint)
}

fn connect_channel(
    endpoint: &str,
    transport: &ClientTransportConfig,
) -> Result<Channel, ClientError> {
    Ok(build_endpoint(endpoint, transport)?.connect_lazy())
}

fn should_use_tls(endpoint: &str, transport: &ClientTransportConfig) -> bool {
    !transport.insecure && !endpoint.starts_with("http://")
}

fn validate_transport_config(transport: &ClientTransportConfig) -> Result<(), ClientError> {
    let has_cert = transport.client_cert_pem.is_some();
    let has_key = transport.client_key_pem.is_some();
    if has_cert != has_key {
        return Err(ClientError::InvalidTransportConfig {
            message: "client certificate and private key must be configured together".into(),
        });
    }
    Ok(())
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
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Mutex as StdMutex};

    use prost::Message;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use tonic::{Code, Request, Response};

    use crate::metadata::MemoryMetadataStore;
    use crate::proto::v1::{
        timeline_route_service_server::TimelineRouteServiceServer,
        timestamp_service_server::TimestampServiceServer, AllocateTimestampsResponse,
        EnsureTimelineRequest, EnsureTimelineResponse, GetTimelineRouteRequest,
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
        get_calls: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl crate::proto::v1::timeline_route_service_server::TimelineRouteService for FakeRouteService {
        async fn get_timeline_route(
            &self,
            request: Request<GetTimelineRouteRequest>,
        ) -> Result<Response<GetTimelineRouteResponse>, Status> {
            self.get_calls.fetch_add(1, AtomicOrdering::AcqRel);
            let mut route = self.route.lock().unwrap().clone();
            route.timeline_key = request.into_inner().timeline_key;
            Ok(Response::new(GetTimelineRouteResponse {
                route: Some(route),
            }))
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
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let incoming = TcpListenerStream::new(listener);
        let route_service = FakeRouteService {
            route: Arc::new(StdMutex::new(route)),
            get_calls: Arc::new(AtomicUsize::new(0)),
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

    async fn spawn_counting_route_only_server(route: TimelineRoute) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
        let incoming = TcpListenerStream::new(listener);
        let get_calls = Arc::new(AtomicUsize::new(0));
        let route_service = FakeRouteService {
            route: Arc::new(StdMutex::new(route)),
            get_calls: Arc::clone(&get_calls),
        };

        tokio::spawn(async move {
            Server::builder()
                .add_service(TimelineRouteServiceServer::new(route_service))
                .serve_with_incoming(incoming)
                .await
                .expect("server should serve");
        });

        (addr.to_string(), get_calls)
    }

    async fn spawn_timestamp_only_server(route: TimelineRoute) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener
            .local_addr()
            .expect("listener should have local addr");
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
        let client = Client::connect_with_config(
            endpoint,
            ClientConfig::new("orders.primary")
                .with_transport(ClientTransportConfig::default().with_insecure(true)),
        )
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
        let client = Client::connect_with_config(
            endpoint,
            ClientConfig::new("orders.primary")
                .with_transport(ClientTransportConfig::default().with_insecure(true)),
        )
        .await
        .expect("client should connect");

        {
            let mut route = client.route.write().await;
            route.route_version += 1;
        }

        let ranges = client
            .allocate_timestamps(1)
            .await
            .expect("allocation should recover after route refresh");

        assert_eq!(ranges.len(), 1);
    }

    #[tokio::test]
    async fn repeated_stale_route_refresh_reuses_newer_cached_route() {
        let (endpoint, get_calls) = spawn_counting_route_only_server(TimelineRoute {
            timeline_key: "orders.primary".into(),
            generator_id: 7,
            owner_worker_endpoint: "127.0.0.1:0".into(),
            epoch: 3,
            route_version: 11,
            resource_tier: ResourceTier::Shared as i32,
        })
        .await;
        let client = Client::connect_with_config(
            endpoint,
            ClientConfig::new("orders.primary")
                .with_transport(ClientTransportConfig::default().with_insecure(true)),
        )
        .await
        .expect("client should connect");

        {
            let mut route = client.route.write().await;
            route.route_version = 10;
        }
        let stale_observation = client.route.read().await.clone();

        client
            .refresh_route_if_unchanged(&stale_observation)
            .await
            .expect("first refresh should load from control plane");
        client
            .refresh_route_if_unchanged(&stale_observation)
            .await
            .expect("second refresh should reuse cached route");

        assert_eq!(get_calls.load(AtomicOrdering::Acquire), 1);
    }

    #[tokio::test]
    async fn concurrent_allocations_recover_from_shared_stale_cached_route() {
        let endpoint = spawn_test_server().await;
        let client = Arc::new(
            Client::connect_with_config(
                endpoint,
                ClientConfig::new("orders.primary")
                    .with_transport(ClientTransportConfig::default().with_insecure(true)),
            )
            .await
            .expect("client should connect"),
        );

        {
            let mut route = client.route.write().await;
            route.route_version += 1;
        }

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let client = Arc::clone(&client);
            tasks.push(tokio::spawn(async move {
                client
                    .allocate_timestamps(1)
                    .await
                    .expect("allocation should recover after shared stale route");
            }));
        }

        for task in tasks {
            task.await.expect("task should complete");
        }
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

        let client = Client::connect_with_config(
            route_addr,
            ClientConfig::new("orders.primary")
                .with_transport(ClientTransportConfig::default().with_insecure(true)),
        )
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
    fn normalize_endpoint_defaults_to_secure_and_respects_explicit_insecure() {
        assert_eq!(
            normalize_endpoint("127.0.0.1:50051", false),
            "https://127.0.0.1:50051"
        );
        assert_eq!(
            normalize_endpoint("127.0.0.1:50051", true),
            "http://127.0.0.1:50051"
        );
        assert_eq!(
            normalize_endpoint("https://chronos.internal:50051", false),
            "https://chronos.internal:50051"
        );
    }

    #[test]
    fn transport_config_rejects_partial_client_identity() {
        validate_transport_config(
            &ClientTransportConfig::default()
                .with_ca_pem(b"ca".to_vec())
                .with_domain_name("chronos.internal"),
        )
        .expect("complete secure transport settings should validate");

        let error = validate_transport_config(&ClientTransportConfig {
            client_cert_pem: Some(b"cert".to_vec()),
            ..ClientTransportConfig::default()
        })
        .expect_err("partial client identity should be rejected");
        assert!(matches!(error, ClientError::InvalidTransportConfig { .. }));
    }
}
