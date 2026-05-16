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
//! use chronos::{Client, ClientConfig, ClientTransportConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ClientConfig::new("orders.primary")
//!     .with_transport(ClientTransportConfig::default().with_insecure(true));
//! let client = Client::connect_with_config(
//!     "127.0.0.1:50051",
//!     config,
//! )
//! .await?;
//!
//! let ranges = client.allocate_timestamps(1).await?;
//! println!("tso={}", ranges[0].start_tso);
//! # Ok(())
//! # }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock as StdRwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::sync::Mutex;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Code, Request, Status};

use crate::proto::v1::{
    timeline_route_service_client::TimelineRouteServiceClient,
    timestamp_service_client::TimestampServiceClient, AllocateTimestampsRequest,
    EnsureTimelineRequest, ErrorCode, GetTimelineRouteRequest, ResourceTier, TimelineRoute,
    TimestampRange,
};
use crate::rpc::decode_error_detail_from_status_details;

const DEFAULT_STALE_ROUTE_RETRY_ATTEMPTS: u32 = 3;
const DEFAULT_STALE_ROUTE_RETRY_BACKOFF_MS: u64 = 5;
const DEFAULT_REQUEST_TIMEOUT_MS: u32 = 250;
static CLIENT_SCOPE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct ClientConfig {
    timeline_key: String,
    desired_resource_tier: ResourceTier,
    request_timeout_ms: u32,
    stale_route_retry_attempts: u32,
    stale_route_retry_backoff_ms: u64,
    idempotency_enabled: bool,
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
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            stale_route_retry_attempts: DEFAULT_STALE_ROUTE_RETRY_ATTEMPTS,
            stale_route_retry_backoff_ms: DEFAULT_STALE_ROUTE_RETRY_BACKOFF_MS,
            idempotency_enabled: false,
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

    /// Enables server-side request-record idempotency for each allocation.
    ///
    /// This adds metadata I/O on authoritative stores such as etcd, so the default keeps the
    /// allocation hot path non-idempotent. Enable it when callers need replay protection for
    /// ambiguous client-side retries.
    pub fn with_idempotency_enabled(mut self, idempotency_enabled: bool) -> Self {
        self.idempotency_enabled = idempotency_enabled;
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
    #[error("chronos returned invalid route from {operation}: {message}")]
    InvalidRoute {
        operation: &'static str,
        message: String,
    },
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
    route_snapshot: StdRwLock<RouteSnapshot>,
    route_refresh: Mutex<()>,
    config: ClientConfig,
    idempotency_scope: String,
    request_id: AtomicU64,
}

#[derive(Clone)]
struct RouteSnapshot {
    route: TimelineRoute,
    owner_endpoint: String,
    tso_client: TimestampServiceClient<Channel>,
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
        let ensure_request = request_with_timeout(
            EnsureTimelineRequest {
                timeline_key: config.timeline_key.clone(),
                desired_resource_tier: config.desired_resource_tier as i32,
            },
            config.request_timeout_ms,
        );
        let ensured_route = require_route(
            "ensure_timeline",
            route_client
                .ensure_timeline(ensure_request)
                .await?
                .into_inner()
                .route,
        )?;
        let route_snapshot = route_snapshot_for(&ensured_route, None, &config.transport)?;

        Ok(Self {
            route_client: Mutex::new(route_client),
            route_snapshot: StdRwLock::new(route_snapshot),
            route_refresh: Mutex::new(()),
            config,
            idempotency_scope: new_idempotency_scope(),
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
            let snapshot = self.route_snapshot();
            match self
                .allocate_once(
                    snapshot.tso_client,
                    &snapshot.route,
                    count,
                    &client_request_id,
                )
                .await
            {
                Ok(ranges) => return Ok(ranges),
                Err(status)
                    if is_stale_route_error(&status)
                        && stale_retries < self.config.stale_route_retry_attempts =>
                {
                    stale_retries += 1;
                    self.refresh_route_if_unchanged(&snapshot.route).await?;
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
        let current = self.route_snapshot();
        if !same_route_identity(&current.route, observed_route) {
            return Ok(current.route);
        }

        self.refresh_route().await
    }

    async fn refresh_route(&self) -> Result<TimelineRoute, ClientError> {
        let route = require_route(
            "get_timeline_route",
            self.route_client
                .lock()
                .await
                .get_timeline_route(request_with_timeout(
                    GetTimelineRouteRequest {
                        timeline_key: self.config.timeline_key.clone(),
                    },
                    self.config.request_timeout_ms,
                ))
                .await?
                .into_inner()
                .route,
        )?;
        self.replace_route_snapshot(route.clone())?;
        Ok(route)
    }

    fn route_snapshot(&self) -> RouteSnapshot {
        match self.route_snapshot.read() {
            Ok(snapshot) => snapshot.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn replace_route_snapshot(&self, route: TimelineRoute) -> Result<(), ClientError> {
        let current = self.route_snapshot();
        let next = route_snapshot_for(&route, Some(&current), &self.config.transport)?;
        match self.route_snapshot.write() {
            Ok(mut snapshot) => {
                *snapshot = next;
            }
            Err(poisoned) => {
                *poisoned.into_inner() = next;
            }
        }
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
        let request = request_with_timeout(
            AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: client_request_id.to_string(),
                request_timeout_ms: self.config.request_timeout_ms,
            },
            self.config.request_timeout_ms,
        );
        Ok(tso_client
            .allocate_timestamps(request)
            .await?
            .into_inner()
            .ranges)
    }

    fn next_client_request_id(&self) -> String {
        if self.config.idempotency_enabled {
            format!(
                "{}-{}-{}",
                self.config.timeline_key,
                self.idempotency_scope,
                self.request_id.fetch_add(1, Ordering::Relaxed)
            )
        } else {
            String::new()
        }
    }
}

fn new_idempotency_scope() -> String {
    let counter = CLIENT_SCOPE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{:x}-{:x}-{:x}", std::process::id(), now_ns, counter)
}

fn request_with_timeout<T>(message: T, timeout_ms: u32) -> Request<T> {
    let mut request = Request::new(message);
    if timeout_ms > 0 {
        request.set_timeout(Duration::from_millis(timeout_ms as u64));
    }
    request
}

fn same_route_identity(left: &TimelineRoute, right: &TimelineRoute) -> bool {
    left.timeline_key == right.timeline_key
        && left.generator_id == right.generator_id
        && left.owner_worker_endpoint == right.owner_worker_endpoint
        && left.epoch == right.epoch
        && left.route_version == right.route_version
        && left.resource_tier == right.resource_tier
}

fn require_route(
    operation: &'static str,
    route: Option<TimelineRoute>,
) -> Result<TimelineRoute, ClientError> {
    let route = route.ok_or(ClientError::MissingRoute { operation })?;
    if route.owner_worker_endpoint.is_empty() {
        return Err(ClientError::InvalidRoute {
            operation,
            message: "empty owner endpoint".into(),
        });
    }
    Ok(route)
}

fn route_snapshot_for(
    route: &TimelineRoute,
    current: Option<&RouteSnapshot>,
    transport: &ClientTransportConfig,
) -> Result<RouteSnapshot, ClientError> {
    let owner_endpoint = route.owner_worker_endpoint.clone();
    let tso_client = match current {
        Some(snapshot) if snapshot.owner_endpoint == owner_endpoint => snapshot.tso_client.clone(),
        _ => TimestampServiceClient::new(connect_channel(&owner_endpoint, transport)?),
    };
    Ok(RouteSnapshot {
        route: route.clone(),
        owner_endpoint,
        tso_client,
    })
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
    if endpoint.starts_with("http://") && !transport.insecure {
        return Err(ClientError::InvalidTransportConfig {
            message: "http:// endpoints require ClientTransportConfig::with_insecure(true)".into(),
        });
    }
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

    let Some(detail) = decode_error_detail_from_status_details(status.details()) else {
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

    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use tonic::{Code, Request, Response};

    use crate::metadata::MemoryMetadataStore;
    use crate::proto::v1::{
        timeline_route_service_server::TimelineRouteServiceServer,
        timestamp_service_server::TimestampServiceServer, AllocateTimestampsResponse,
        EnsureTimelineRequest, EnsureTimelineResponse, ErrorDetail, GetTimelineRouteRequest,
        GetTimelineRouteResponse, TimelineRoute,
    };
    use crate::rpc::{encode_error_detail_status, TsoRouteService, TsoTimestampService};
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
                };
                let encoded_status =
                    encode_error_detail_status(Code::FailedPrecondition, "stale route", detail);
                return Err(Status::with_details(
                    Code::FailedPrecondition,
                    "stale route",
                    encoded_status.into(),
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

    fn mutate_cached_route(client: &Client, mutate: impl FnOnce(&mut TimelineRoute)) {
        match client.route_snapshot.write() {
            Ok(mut snapshot) => mutate(&mut snapshot.route),
            Err(poisoned) => mutate(&mut poisoned.into_inner().route),
        }
    }

    fn cached_route(client: &Client) -> TimelineRoute {
        match client.route_snapshot.read() {
            Ok(snapshot) => snapshot.route.clone(),
            Err(poisoned) => poisoned.into_inner().route.clone(),
        }
    }

    fn test_route(owner_worker_endpoint: impl Into<String>) -> TimelineRoute {
        TimelineRoute {
            timeline_key: "orders.primary".into(),
            generator_id: 7,
            owner_worker_endpoint: owner_worker_endpoint.into(),
            epoch: 3,
            route_version: 11,
            resource_tier: ResourceTier::Shared as i32,
        }
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

        mutate_cached_route(&client, |route| {
            route.route_version += 1;
        });

        let ranges = client
            .allocate_timestamps(1)
            .await
            .expect("allocation should recover after route refresh");

        assert_eq!(ranges.len(), 1);
    }

    #[tokio::test]
    async fn repeated_stale_route_refresh_reuses_newer_cached_route() {
        let (endpoint, get_calls) =
            spawn_counting_route_only_server(test_route("127.0.0.1:0")).await;
        let client = Client::connect_with_config(
            endpoint,
            ClientConfig::new("orders.primary")
                .with_transport(ClientTransportConfig::default().with_insecure(true)),
        )
        .await
        .expect("client should connect");

        mutate_cached_route(&client, |route| {
            route.route_version = 10;
        });
        let stale_observation = cached_route(&client);

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

        mutate_cached_route(&client, |route| {
            route.route_version += 1;
        });

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
        let owner_addr = spawn_timestamp_only_server(test_route("127.0.0.1:0")).await;

        let route_addr = spawn_route_only_server(test_route(owner_addr.clone())).await;

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

    #[tokio::test]
    async fn connect_rejects_route_with_empty_owner_endpoint() {
        let route_addr = spawn_route_only_server(test_route("")).await;

        let error = match Client::connect_with_config(
            route_addr,
            ClientConfig::new("orders.primary")
                .with_transport(ClientTransportConfig::default().with_insecure(true)),
        )
        .await
        {
            Ok(_) => panic!("client should reject an invalid route"),
            Err(error) => error,
        };

        assert!(matches!(error, ClientError::InvalidRoute { .. }));
    }

    #[test]
    fn client_config_disables_request_record_idempotency_by_default() {
        let default_config = ClientConfig::new("orders.primary");
        assert!(!default_config.idempotency_enabled);

        let idempotent_config = ClientConfig::new("orders.primary").with_idempotency_enabled(true);
        assert!(idempotent_config.idempotency_enabled);
    }

    #[test]
    fn request_timeout_sets_grpc_timeout_metadata() {
        let request = request_with_timeout(
            GetTimelineRouteRequest {
                timeline_key: "orders.primary".into(),
            },
            1500,
        );
        assert!(request.metadata().contains_key("grpc-timeout"));

        let request = request_with_timeout(
            GetTimelineRouteRequest {
                timeline_key: "orders.primary".into(),
            },
            0,
        );
        assert!(!request.metadata().contains_key("grpc-timeout"));
    }

    #[test]
    fn idempotency_scope_is_unique_per_client_instance() {
        let first = new_idempotency_scope();
        let second = new_idempotency_scope();

        assert_ne!(first, second);
        assert!(first.contains('-'));
        assert!(second.contains('-'));
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
    fn stale_route_error_requires_chronos_route_detail() {
        let status = Status::failed_precondition("non-route precondition failed");

        assert!(!is_stale_route_error(&status));
    }

    #[test]
    fn http_endpoint_requires_explicit_insecure_transport() {
        assert!(matches!(
            build_endpoint("http://127.0.0.1:50051", &ClientTransportConfig::default()),
            Err(ClientError::InvalidTransportConfig { .. })
        ));

        build_endpoint(
            "http://127.0.0.1:50051",
            &ClientTransportConfig::default().with_insecure(true),
        )
        .expect("explicit insecure transport should accept http endpoint");
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
