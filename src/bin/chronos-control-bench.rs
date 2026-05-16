use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::Barrier;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

use chronos::proto::v1::{
    timeline_control_service_client::TimelineControlServiceClient,
    timeline_route_service_client::TimelineRouteServiceClient,
    timeline_status_service_client::TimelineStatusServiceClient,
    timestamp_service_client::TimestampServiceClient, AllocateTimestampsRequest,
    EnsureTimelineRequest, ErrorCode, ErrorDetail, GetTimelineRouteRequest,
    ListTimelineStatusesRequest, ResourceTier, TimelineRoute, TimelineState,
    TimelineTransferReason, TransferTimelineRequest,
};

#[path = "support/config_parse.rs"]
mod support_config_parse;
#[path = "support/endpoint.rs"]
mod support_endpoint;
#[path = "support/env.rs"]
mod support_env;
#[path = "support/stats.rs"]
mod support_stats;

use support_config_parse::{parse_boolish, parse_csv_string_list, parse_csv_u32_list};
use support_endpoint::{normalize_endpoint, normalize_endpoint_or_fallback};
use support_env::{env_or, env_or_string};
use support_stats::percentile;

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const REBALANCE_ALLOCATE_RETRY_ATTEMPTS: usize = 8;
const REBALANCE_ALLOCATE_RETRY_BACKOFF_MS: u64 = 25;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    StatusScan,
    StatusScanFiltered,
    AllocateDuringRebalance,
}

impl Scenario {
    fn as_str(self) -> &'static str {
        match self {
            Self::StatusScan => "status_scan",
            Self::StatusScanFiltered => "status_scan_filtered",
            Self::AllocateDuringRebalance => "allocate_during_rebalance",
        }
    }
}

#[derive(Clone, Debug)]
struct RouteSnapshot {
    timeline_key: String,
    generator_id: u32,
    epoch: u64,
    route_version: u64,
    owner_worker_endpoint: String,
}

#[derive(Clone, Debug, Default)]
struct StatusFilters {
    states: Vec<i32>,
    owner_worker_endpoint: Option<String>,
}

#[derive(Clone, Debug)]
struct BenchConfig {
    endpoint: String,
    timeline_namespace: String,
    scenario: Scenario,
    concurrency: usize,
    timeline_count: usize,
    duration_secs: u64,
    warmup_secs: u64,
    seed_timelines: bool,
    resource_tier: ResourceTier,
    page_size: u32,
    status_filters: StatusFilters,
    filter_states_display: String,
    allocate_batch: u32,
    transfer_interval_ms: u64,
    transfer_target_generators: Vec<u32>,
    transfer_target_owner_endpoints: Vec<String>,
    route_to_owners: bool,
    allocate_request_timeout_ms: u64,
    idempotency_enabled: bool,
}

#[derive(Default)]
struct StatusWorkerStats {
    requests: u64,
    pages: u64,
    statuses: u64,
    scans: u64,
    rpc_latencies_us: Vec<u64>,
    scan_latencies_us: Vec<u64>,
}

#[derive(Default)]
struct RebalanceWorkerStats {
    allocate_requests_total: u64,
    allocate_success_total: u64,
    allocate_tsos_total: u64,
    allocate_latencies_us: Vec<u64>,
    route_refresh_total: u64,
    route_refresh_latencies_us: Vec<u64>,
    monotonicity_violations_total: u64,
    error_counts: HashMap<String, u64>,
}

#[derive(Default)]
struct RebalanceDriverStats {
    transfer_attempts_total: u64,
    transfer_success_total: u64,
    transfer_failed_total: u64,
    transfer_latencies_us: Vec<u64>,
}

#[derive(Default)]
struct RebalanceBenchStats {
    allocate_requests_total: u64,
    allocate_success_total: u64,
    allocate_tsos_total: u64,
    allocate_latencies_us: Vec<u64>,
    route_refresh_total: u64,
    route_refresh_latencies_us: Vec<u64>,
    monotonicity_violations_total: u64,
    error_counts: HashMap<String, u64>,
    transfer_attempts_total: u64,
    transfer_success_total: u64,
    transfer_failed_total: u64,
    transfer_latencies_us: Vec<u64>,
}

fn parse_scenario(value: &str) -> AppResult<Scenario> {
    match value.trim().to_ascii_lowercase().as_str() {
        "status_scan" => Ok(Scenario::StatusScan),
        "status_scan_filtered" => Ok(Scenario::StatusScanFiltered),
        "allocate_during_rebalance" => Ok(Scenario::AllocateDuringRebalance),
        other => Err(format!("unsupported control bench scenario: {other}").into()),
    }
}

fn parse_resource_tier(value: &str) -> ResourceTier {
    match value.to_ascii_lowercase().as_str() {
        "shared" => ResourceTier::Shared,
        "warm" => ResourceTier::Warm,
        "dedicated" => ResourceTier::Dedicated,
        _ => ResourceTier::Shared,
    }
}

fn parse_timeline_state_token(token: &str) -> AppResult<i32> {
    match token.trim().to_ascii_lowercase().as_str() {
        "creating" => Ok(TimelineState::Creating as i32),
        "active" => Ok(TimelineState::Active as i32),
        "draining" => Ok(TimelineState::Draining as i32),
        "locked" => Ok(TimelineState::Locked as i32),
        "recovering" => Ok(TimelineState::Recovering as i32),
        other => Err(format!("unsupported timeline state filter: {other}").into()),
    }
}

fn parse_filter_states_csv(value: &str) -> AppResult<Vec<i32>> {
    let mut parsed = Vec::new();
    for token in value.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        parsed.push(parse_timeline_state_token(trimmed)?);
    }
    parsed.sort_unstable();
    parsed.dedup();
    Ok(parsed)
}

fn load_config() -> AppResult<BenchConfig> {
    let endpoint = normalize_endpoint(&env_or_string(
        "CHRONOS_CONTROL_BENCH_ENDPOINT",
        "http://[::1]:50051",
    ));
    let timeline_namespace = env_or_string("CHRONOS_CONTROL_BENCH_NAMESPACE", "controlbench");
    let scenario = parse_scenario(&env_or_string(
        "CHRONOS_CONTROL_BENCH_SCENARIO",
        "status_scan",
    ))?;
    let concurrency = env_or("CHRONOS_CONTROL_BENCH_CONCURRENCY", 16usize);
    let timeline_count = env_or("CHRONOS_CONTROL_BENCH_TIMELINES", concurrency.max(1));
    let duration_secs = env_or("CHRONOS_CONTROL_BENCH_DURATION_SECS", 10u64);
    let warmup_secs = env_or("CHRONOS_CONTROL_BENCH_WARMUP_SECS", 3u64);
    let seed_timelines =
        parse_boolish(&env_or_string("CHRONOS_CONTROL_BENCH_SEED_TIMELINES", "1"))?;
    let resource_tier = parse_resource_tier(&env_or_string(
        "CHRONOS_CONTROL_BENCH_RESOURCE_TIER",
        "shared",
    ));
    let page_size = env_or("CHRONOS_CONTROL_BENCH_PAGE_SIZE", 200u32);
    let filter_states_display = env_or_string("CHRONOS_CONTROL_BENCH_FILTER_STATES", "");
    let owner_worker_endpoint = env_or_string("CHRONOS_CONTROL_BENCH_FILTER_OWNER_ENDPOINT", "");
    let allocate_batch = env_or("CHRONOS_CONTROL_BENCH_ALLOCATE_BATCH", 1u32);
    let transfer_interval_ms = env_or("CHRONOS_CONTROL_BENCH_TRANSFER_INTERVAL_MS", 250u64);
    let transfer_target_generators = parse_csv_u32_list(&env_or_string(
        "CHRONOS_CONTROL_BENCH_TRANSFER_TARGET_GENERATORS",
        "0,1,2,3",
    ))?;
    let transfer_target_owner_endpoints = parse_csv_string_list(&env_or_string(
        "CHRONOS_CONTROL_BENCH_TRANSFER_TARGET_OWNER_ENDPOINTS",
        "",
    ));
    let route_to_owners =
        parse_boolish(&env_or_string("CHRONOS_CONTROL_BENCH_ROUTE_TO_OWNERS", "0"))?;
    let allocate_request_timeout_ms = env_or(
        "CHRONOS_CONTROL_BENCH_ALLOCATE_REQUEST_TIMEOUT_MS",
        10000u64,
    );
    let idempotency_enabled =
        parse_boolish(&env_or_string("CHRONOS_CONTROL_BENCH_IDEMPOTENCY", "0"))?;

    Ok(BenchConfig {
        endpoint,
        timeline_namespace,
        scenario,
        concurrency,
        timeline_count,
        duration_secs,
        warmup_secs,
        seed_timelines,
        resource_tier,
        page_size,
        status_filters: StatusFilters {
            states: parse_filter_states_csv(&filter_states_display)?,
            owner_worker_endpoint: (!owner_worker_endpoint.is_empty())
                .then_some(owner_worker_endpoint),
        },
        filter_states_display,
        allocate_batch,
        transfer_interval_ms,
        transfer_target_generators,
        transfer_target_owner_endpoints,
        route_to_owners,
        allocate_request_timeout_ms,
        idempotency_enabled,
    })
}

fn route_snapshot_from_proto(route: TimelineRoute) -> RouteSnapshot {
    RouteSnapshot {
        timeline_key: route.timeline_key,
        generator_id: route.generator_id,
        epoch: route.epoch,
        route_version: route.route_version,
        owner_worker_endpoint: route.owner_worker_endpoint,
    }
}

async fn ensure_seed_routes(
    config: &BenchConfig,
    channel: Channel,
) -> AppResult<Vec<RouteSnapshot>> {
    let mut client = TimelineRouteServiceClient::new(channel);
    let mut routes = Vec::with_capacity(config.timeline_count);
    for idx in 0..config.timeline_count {
        let timeline_key = format!(
            "{}.{}.{}",
            config.timeline_namespace,
            config.scenario.as_str(),
            idx
        );
        let route = client
            .ensure_timeline(Request::new(EnsureTimelineRequest {
                timeline_key,
                desired_resource_tier: config.resource_tier as i32,
            }))
            .await?
            .into_inner()
            .route
            .ok_or("missing route from ensure_timeline")?;
        routes.push(route_snapshot_from_proto(route));
    }
    Ok(routes)
}

async fn connect_channel(endpoint: String) -> AppResult<Channel> {
    let channel = Channel::from_shared(endpoint.clone())
        .map_err(|error| format!("invalid bench endpoint {endpoint}: {error}"))?;
    channel
        .connect()
        .await
        .map_err(|error| format!("failed to connect bench endpoint {endpoint}: {error}").into())
}

async fn ensure_timestamp_client(
    clients: &mut BTreeMap<String, TimestampServiceClient<Channel>>,
    endpoint: &str,
) -> AppResult<()> {
    if !clients.contains_key(endpoint) {
        let channel = connect_channel(endpoint.to_owned()).await?;
        clients.insert(endpoint.to_owned(), TimestampServiceClient::new(channel));
    }
    Ok(())
}

async fn ensure_control_client(
    clients: &mut BTreeMap<String, TimelineControlServiceClient<Channel>>,
    endpoint: &str,
) -> AppResult<()> {
    if !clients.contains_key(endpoint) {
        let channel = connect_channel(endpoint.to_owned()).await?;
        clients.insert(
            endpoint.to_owned(),
            TimelineControlServiceClient::new(channel),
        );
    }
    Ok(())
}

fn allocation_endpoint(
    route: &RouteSnapshot,
    fallback_endpoint: &str,
    route_to_owners: bool,
) -> String {
    if route_to_owners {
        normalize_endpoint_or_fallback(&route.owner_worker_endpoint, fallback_endpoint)
    } else {
        fallback_endpoint.to_owned()
    }
}

fn target_control_endpoint(
    target_owner_endpoint: Option<&str>,
    fallback_endpoint: &str,
    route_to_owners: bool,
) -> String {
    if route_to_owners {
        target_owner_endpoint
            .map(|endpoint| normalize_endpoint_or_fallback(endpoint, fallback_endpoint))
            .unwrap_or_else(|| fallback_endpoint.to_owned())
    } else {
        fallback_endpoint.to_owned()
    }
}

fn configured_allocation_endpoints(config: &BenchConfig, routes: &[RouteSnapshot]) -> Vec<String> {
    let mut endpoints = BTreeSet::new();
    for route in routes {
        endpoints.insert(allocation_endpoint(
            route,
            &config.endpoint,
            config.route_to_owners,
        ));
    }
    if config.route_to_owners {
        for endpoint in &config.transfer_target_owner_endpoints {
            endpoints.insert(normalize_endpoint(endpoint));
        }
    }
    endpoints.into_iter().collect()
}

fn route_owner_endpoint_for_generator(
    transfer_target_owner_endpoints: &[String],
    generator_id: u32,
) -> Option<String> {
    if transfer_target_owner_endpoints.is_empty() {
        None
    } else {
        let index = generator_id as usize % transfer_target_owner_endpoints.len();
        Some(transfer_target_owner_endpoints[index].clone())
    }
}

fn endpoint_key(endpoint: &str) -> String {
    normalize_endpoint(endpoint)
        .trim_end_matches('/')
        .to_ascii_lowercase()
}

fn next_target_owner_endpoint(
    current_owner_endpoint: &str,
    transfer_target_owner_endpoints: &[String],
) -> Option<String> {
    if transfer_target_owner_endpoints.is_empty() {
        return None;
    }

    let current_key = endpoint_key(current_owner_endpoint);
    let next_index = transfer_target_owner_endpoints
        .iter()
        .position(|endpoint| endpoint_key(endpoint) == current_key)
        .map(|index| {
            if transfer_target_owner_endpoints.len() == 1 {
                index
            } else {
                (index + 1) % transfer_target_owner_endpoints.len()
            }
        })
        .unwrap_or(0);
    Some(transfer_target_owner_endpoints[next_index].clone())
}

async fn list_timeline_statuses_page(
    client: &mut TimelineStatusServiceClient<Channel>,
    filters: &StatusFilters,
    page_size: u32,
    page_token: String,
) -> AppResult<chronos::proto::v1::ListTimelineStatusesResponse> {
    Ok(client
        .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
            states: filters.states.clone(),
            owner_worker_endpoint: filters.owner_worker_endpoint.clone(),
            page_size,
            page_token,
        }))
        .await?
        .into_inner())
}

async fn run_status_scan_bench(config: &BenchConfig) -> AppResult<StatusWorkerStats> {
    let barrier = Arc::new(Barrier::new(config.concurrency + 1));
    let warmup_until = Instant::now() + Duration::from_secs(config.warmup_secs);
    let measure_until = warmup_until + Duration::from_secs(config.duration_secs);
    let mut handles = Vec::with_capacity(config.concurrency);

    for _worker_idx in 0..config.concurrency {
        let barrier = barrier.clone();
        let endpoint = config.endpoint.clone();
        let page_size = config.page_size;
        let filters = config.status_filters.clone();
        handles.push(tokio::spawn(async move {
            let channel = connect_channel(endpoint.clone()).await?;
            let mut client = TimelineStatusServiceClient::new(channel);
            let mut stats = StatusWorkerStats::default();

            barrier.wait().await;
            loop {
                let scan_start = Instant::now();
                if scan_start >= measure_until {
                    break;
                }

                let mut page_token = String::new();

                loop {
                    let request_start = Instant::now();
                    let response =
                        list_timeline_statuses_page(&mut client, &filters, page_size, page_token)
                            .await?;
                    let request_elapsed = request_start.elapsed().as_micros() as u64;
                    let record_request = request_start >= warmup_until;

                    if record_request {
                        stats.requests += 1;
                        stats.pages += 1;
                        stats.statuses += response.statuses.len() as u64;
                        stats.rpc_latencies_us.push(request_elapsed);
                    }

                    if response.next_page_token.is_empty() {
                        break;
                    }
                    page_token = response.next_page_token;
                }

                if scan_start < measure_until && Instant::now() >= warmup_until {
                    stats.scans += 1;
                    stats
                        .scan_latencies_us
                        .push(scan_start.elapsed().as_micros() as u64);
                }
            }

            Ok::<StatusWorkerStats, Box<dyn Error + Send + Sync>>(stats)
        }));
    }

    barrier.wait().await;
    let _started = Instant::now();
    let mut total = StatusWorkerStats::default();
    for handle in handles {
        let stats = handle.await??;
        total.requests += stats.requests;
        total.pages += stats.pages;
        total.statuses += stats.statuses;
        total.scans += stats.scans;
        total.rpc_latencies_us.extend(stats.rpc_latencies_us);
        total.scan_latencies_us.extend(stats.scan_latencies_us);
    }
    Ok(total)
}

fn record_error_count(counts: &mut HashMap<String, u64>, label: &str) {
    *counts.entry(label.to_string()).or_insert(0) += 1;
}

fn error_code_label(error_code: i32) -> Option<&'static str> {
    match ErrorCode::try_from(error_code).ok() {
        Some(ErrorCode::NotTimelineOwner) => Some("not_timeline_owner"),
        Some(ErrorCode::RouteVersionMismatch) => Some("route_version_mismatch"),
        Some(ErrorCode::EpochMismatch) => Some("epoch_mismatch"),
        Some(ErrorCode::LeaseExpired) => Some("lease_expired"),
        Some(ErrorCode::RateLimited) => Some("rate_limited"),
        Some(ErrorCode::TemporarilyUnavailable) => Some("temporarily_unavailable"),
        Some(ErrorCode::TimelineNotFound) => Some("timeline_not_found"),
        Some(ErrorCode::InvalidArgument) => Some("invalid_argument"),
        Some(ErrorCode::Internal) => Some("internal"),
        Some(ErrorCode::Unspecified) | None => None,
    }
}

fn status_code_label(code: Code) -> &'static str {
    match code {
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "transport_unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
        Code::Ok => "ok",
    }
}

fn allocation_error_label(status: &Status, detail: Option<&ErrorDetail>) -> &'static str {
    detail
        .and_then(|detail| error_code_label(detail.code))
        .unwrap_or_else(|| status_code_label(status.code()))
}

fn is_transient_rebalance_error(status: &Status, detail: Option<&ErrorDetail>) -> bool {
    if matches!(
        detail.and_then(|detail| ErrorCode::try_from(detail.code).ok()),
        Some(ErrorCode::LeaseExpired)
            | Some(ErrorCode::TemporarilyUnavailable)
            | Some(ErrorCode::RouteVersionMismatch)
            | Some(ErrorCode::EpochMismatch)
            | Some(ErrorCode::NotTimelineOwner)
            | Some(ErrorCode::RateLimited)
    ) {
        return true;
    }

    matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted
    )
}

fn should_refresh_route_after_rebalance_error(
    status: &Status,
    detail: Option<&ErrorDetail>,
) -> bool {
    if matches!(
        detail.and_then(|detail| ErrorCode::try_from(detail.code).ok()),
        Some(ErrorCode::LeaseExpired)
            | Some(ErrorCode::TemporarilyUnavailable)
            | Some(ErrorCode::RouteVersionMismatch)
            | Some(ErrorCode::EpochMismatch)
            | Some(ErrorCode::NotTimelineOwner)
    ) {
        return true;
    }

    matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded)
}

fn rebalance_retry_backoff_ms(status: &Status, detail: Option<&ErrorDetail>) -> u64 {
    match detail.and_then(|detail| ErrorCode::try_from(detail.code).ok()) {
        Some(ErrorCode::RateLimited) => 5,
        _ if status.code() == Code::ResourceExhausted => 5,
        _ => REBALANCE_ALLOCATE_RETRY_BACKOFF_MS,
    }
}

fn decode_error_detail(status: &Status) -> Option<ErrorDetail> {
    if status.details().is_empty() {
        return None;
    }
    chronos::rpc::decode_error_detail_from_status_details(status.details())
        .filter(|detail| detail.code != ErrorCode::Unspecified as i32)
}

fn count_tsos(response: &chronos::proto::v1::AllocateTimestampsResponse) -> u64 {
    response
        .ranges
        .iter()
        .map(|range| range.end_tso - range.start_tso + 1)
        .sum()
}

fn record_monotonicity(
    last_end_by_timeline: &mut HashMap<String, u64>,
    response: &chronos::proto::v1::AllocateTimestampsResponse,
) -> u64 {
    let Some(first) = response.ranges.first() else {
        return 0;
    };
    let Some(last) = response.ranges.last() else {
        return 0;
    };

    match last_end_by_timeline.get_mut(&response.timeline_key) {
        Some(previous_end) => {
            let violation = u64::from(first.start_tso <= *previous_end);
            if last.end_tso > *previous_end {
                *previous_end = last.end_tso;
            }
            violation
        }
        None => {
            last_end_by_timeline.insert(response.timeline_key.clone(), last.end_tso);
            0
        }
    }
}

async fn refresh_route(
    client: &mut TimelineRouteServiceClient<Channel>,
    timeline_key: &str,
) -> AppResult<RouteSnapshot> {
    let route = client
        .get_timeline_route(Request::new(GetTimelineRouteRequest {
            timeline_key: timeline_key.to_owned(),
        }))
        .await?
        .into_inner()
        .route
        .ok_or("missing route from get_timeline_route")?;
    Ok(route_snapshot_from_proto(route))
}

fn next_target_generator(current: u32, targets: &[u32]) -> Option<u32> {
    if targets.is_empty() {
        return None;
    }
    if let Some(index) = targets.iter().position(|target| *target == current) {
        if targets.len() == 1 {
            return Some(current);
        }
        return Some(targets[(index + 1) % targets.len()]);
    }
    Some(targets[0])
}

async fn run_allocate_during_rebalance_bench(
    config: &BenchConfig,
    seeded_routes: &[RouteSnapshot],
) -> AppResult<RebalanceBenchStats> {
    if seeded_routes.is_empty() {
        return Err("allocate_during_rebalance requires seeded timelines".into());
    }
    if config.transfer_target_generators.is_empty()
        && config.transfer_target_owner_endpoints.is_empty()
    {
        return Err(
            "allocate_during_rebalance requires target generators or target owner endpoints".into(),
        );
    }

    let allocator_workers = config.concurrency.min(seeded_routes.len());
    let barrier = Arc::new(Barrier::new(allocator_workers + 2));
    let warmup_until = Instant::now() + Duration::from_secs(config.warmup_secs);
    let measure_until = warmup_until + Duration::from_secs(config.duration_secs);
    let route_snapshots = Arc::new(DashMap::<String, RouteSnapshot>::new());
    let route_keys = Arc::new(
        seeded_routes
            .iter()
            .map(|route| route.timeline_key.clone())
            .collect::<Vec<_>>(),
    );
    let seeded_routes_for_driver = seeded_routes.to_vec();
    for route in seeded_routes {
        route_snapshots.insert(route.timeline_key.clone(), route.clone());
    }

    let mut worker_handles = Vec::with_capacity(allocator_workers);
    for worker_idx in 0..allocator_workers {
        let barrier = barrier.clone();
        let endpoint = config.endpoint.clone();
        let route_to_owners = config.route_to_owners;
        let allocation_endpoints = configured_allocation_endpoints(config, seeded_routes);
        let allocate_batch = config.allocate_batch;
        let allocate_request_timeout_ms = config.allocate_request_timeout_ms;
        let idempotency_enabled = config.idempotency_enabled;
        let route_snapshots = route_snapshots.clone();
        let route_keys = route_keys.clone();
        worker_handles.push(tokio::spawn(async move {
            let channel = connect_channel(endpoint.clone()).await?;
            let mut route_client = TimelineRouteServiceClient::new(channel);
            let mut timestamp_clients = BTreeMap::new();
            for allocation_endpoint in allocation_endpoints {
                ensure_timestamp_client(&mut timestamp_clients, &allocation_endpoint).await?;
            }
            let mut stats = RebalanceWorkerStats::default();
            let mut last_end_by_timeline = HashMap::new();
            let worker_keys = route_keys
                .iter()
                .skip(worker_idx)
                .step_by(allocator_workers)
                .cloned()
                .collect::<Vec<_>>();
            let mut route_idx = 0usize;
            let mut request_ordinal = 0u64;

            barrier.wait().await;
            loop {
                let request_start = Instant::now();
                if request_start >= measure_until {
                    break;
                }

                let timeline_key = &worker_keys[route_idx % worker_keys.len()];
                route_idx = (route_idx + 1) % worker_keys.len();
                request_ordinal = request_ordinal.saturating_add(1);
                let route = route_snapshots
                    .get(timeline_key)
                    .ok_or("missing route snapshot")?
                    .clone();

                let logical_result: Result<chronos::proto::v1::AllocateTimestampsResponse, ()> =
                    async {
                        let mut current_route = route.clone();

                        for attempt in 0..=REBALANCE_ALLOCATE_RETRY_ATTEMPTS {
                            let request = AllocateTimestampsRequest {
                                timeline_key: current_route.timeline_key.clone(),
                                count: allocate_batch,
                                expected_epoch: current_route.epoch,
                                expected_route_version: current_route.route_version,
                                client_request_id: if idempotency_enabled {
                                    format!("rebalance-{worker_idx}-{request_ordinal}-{attempt}")
                                } else {
                                    String::new()
                                },
                                request_timeout_ms: allocate_request_timeout_ms.min(u32::MAX as u64)
                                    as u32,
                            };
                            let allocation_endpoint =
                                allocation_endpoint(&current_route, &endpoint, route_to_owners);
                            ensure_timestamp_client(&mut timestamp_clients, &allocation_endpoint)
                                .await
                                .map_err(|_| ())?;
                            let timestamp_client = timestamp_clients
                                .get_mut(&allocation_endpoint)
                                .ok_or(())
                                .map_err(|_| ())?;

                            match timestamp_client
                                .allocate_timestamps(Request::new(request))
                                .await
                            {
                                Ok(response) => return Ok(response.into_inner()),
                                Err(status) => {
                                    let detail = decode_error_detail(&status);
                                    record_error_count(
                                        &mut stats.error_counts,
                                        allocation_error_label(&status, detail.as_ref()),
                                    );

                                    if !is_transient_rebalance_error(&status, detail.as_ref())
                                        || attempt == REBALANCE_ALLOCATE_RETRY_ATTEMPTS
                                    {
                                        return Err(());
                                    }

                                    if should_refresh_route_after_rebalance_error(
                                        &status,
                                        detail.as_ref(),
                                    ) {
                                        let refresh_start = Instant::now();
                                        let refreshed = match refresh_route(
                                            &mut route_client,
                                            &current_route.timeline_key,
                                        )
                                        .await
                                        {
                                            Ok(refreshed) => refreshed,
                                            Err(_) => {
                                                record_error_count(
                                                    &mut stats.error_counts,
                                                    "route_refresh_failed",
                                                );
                                                return Err(());
                                            }
                                        };
                                        let refresh_elapsed =
                                            refresh_start.elapsed().as_micros() as u64;
                                        route_snapshots.insert(
                                            current_route.timeline_key.clone(),
                                            refreshed.clone(),
                                        );
                                        current_route = refreshed;

                                        if request_start >= warmup_until {
                                            stats.route_refresh_total += 1;
                                            stats.route_refresh_latencies_us.push(refresh_elapsed);
                                        }
                                    }
                                    tokio::time::sleep(Duration::from_millis(
                                        rebalance_retry_backoff_ms(&status, detail.as_ref()),
                                    ))
                                    .await;
                                }
                            }
                        }

                        Err(())
                    }
                    .await;

                let elapsed = request_start.elapsed().as_micros() as u64;
                if request_start >= warmup_until {
                    stats.allocate_requests_total += 1;
                    stats.allocate_latencies_us.push(elapsed);
                }

                if let Ok(response) = logical_result {
                    if request_start >= warmup_until {
                        stats.allocate_success_total += 1;
                        stats.allocate_tsos_total += count_tsos(&response);
                        stats.monotonicity_violations_total +=
                            record_monotonicity(&mut last_end_by_timeline, &response);
                    }

                    if let Some(mut route) = route_snapshots.get_mut(&response.timeline_key) {
                        route.epoch = response.epoch;
                        route.route_version = response.route_version;
                        route.generator_id = response.generator_id;
                    }
                }
            }

            Ok::<RebalanceWorkerStats, Box<dyn Error + Send + Sync>>(stats)
        }));
    }

    let barrier_driver = barrier.clone();
    let endpoint = config.endpoint.clone();
    let route_keys_for_driver = route_keys.clone();
    let transfer_targets = config.transfer_target_generators.clone();
    let transfer_target_owner_endpoints = config.transfer_target_owner_endpoints.clone();
    let transfer_interval_ms = config.transfer_interval_ms;
    let route_to_owners = config.route_to_owners;
    let driver_handle = tokio::spawn(async move {
        let mut driver_routes = seeded_routes_for_driver
            .iter()
            .map(|route| (route.timeline_key.clone(), route.clone()))
            .collect::<HashMap<_, _>>();
        let mut stats = RebalanceDriverStats::default();
        let mut transfer_idx = 0usize;

        barrier_driver.wait().await;
        let mut control_clients = BTreeMap::new();
        ensure_control_client(&mut control_clients, &endpoint).await?;
        let mut next_tick = Instant::now();
        loop {
            next_tick += Duration::from_millis(transfer_interval_ms);
            tokio::time::sleep_until(tokio::time::Instant::from_std(next_tick)).await;
            let transfer_start = Instant::now();
            if transfer_start >= measure_until {
                break;
            }

            let timeline_key = &route_keys_for_driver[transfer_idx % route_keys_for_driver.len()];
            transfer_idx = (transfer_idx + 1) % route_keys_for_driver.len();
            let current = driver_routes
                .get(timeline_key)
                .ok_or("missing driver route snapshot")?
                .clone();
            let (target_generator_id, target_worker_id) =
                if transfer_targets.is_empty() && !transfer_target_owner_endpoints.is_empty() {
                    (
                        None,
                        next_target_owner_endpoint(
                            &current.owner_worker_endpoint,
                            &transfer_target_owner_endpoints,
                        ),
                    )
                } else {
                    let Some(target_generator_id) =
                        next_target_generator(current.generator_id, &transfer_targets)
                    else {
                        continue;
                    };
                    (
                        Some(target_generator_id),
                        route_owner_endpoint_for_generator(
                            &transfer_target_owner_endpoints,
                            target_generator_id,
                        ),
                    )
                };
            let control_endpoint =
                target_control_endpoint(target_worker_id.as_deref(), &endpoint, route_to_owners);
            ensure_control_client(&mut control_clients, &control_endpoint).await?;
            let control_client = control_clients
                .get_mut(&control_endpoint)
                .ok_or("missing control client")?;

            let result = control_client
                .transfer_timeline(Request::new(TransferTimelineRequest {
                    timeline_key: timeline_key.clone(),
                    target_generator_id,
                    target_worker_id,
                    reason: TimelineTransferReason::Rebalance as i32,
                }))
                .await;
            let elapsed = transfer_start.elapsed().as_micros() as u64;

            if transfer_start >= warmup_until {
                stats.transfer_attempts_total += 1;
            }

            match result {
                Ok(response) => {
                    let response = response.into_inner();
                    if transfer_start >= warmup_until {
                        stats.transfer_success_total += 1;
                        stats.transfer_latencies_us.push(elapsed);
                    }
                    if let Some(route) = driver_routes.get_mut(timeline_key) {
                        route.generator_id = response.new_generator_id;
                        route.epoch = response.new_epoch;
                        route.route_version = response.route_version;
                    }
                }
                Err(_) => {
                    if transfer_start >= warmup_until {
                        stats.transfer_failed_total += 1;
                    }
                }
            }
        }

        Ok::<RebalanceDriverStats, Box<dyn Error + Send + Sync>>(stats)
    });

    barrier.wait().await;

    let mut total = RebalanceBenchStats::default();
    for handle in worker_handles {
        let stats = handle.await??;
        total.allocate_requests_total += stats.allocate_requests_total;
        total.allocate_success_total += stats.allocate_success_total;
        total.allocate_tsos_total += stats.allocate_tsos_total;
        total
            .allocate_latencies_us
            .extend(stats.allocate_latencies_us);
        total.route_refresh_total += stats.route_refresh_total;
        total
            .route_refresh_latencies_us
            .extend(stats.route_refresh_latencies_us);
        total.monotonicity_violations_total += stats.monotonicity_violations_total;
        for (label, count) in stats.error_counts {
            *total.error_counts.entry(label).or_insert(0) += count;
        }
    }

    let driver_stats = driver_handle.await??;
    total.transfer_attempts_total += driver_stats.transfer_attempts_total;
    total.transfer_success_total += driver_stats.transfer_success_total;
    total.transfer_failed_total += driver_stats.transfer_failed_total;
    total
        .transfer_latencies_us
        .extend(driver_stats.transfer_latencies_us);

    Ok(total)
}

fn print_rebalance_summary(config: &BenchConfig, stats: RebalanceBenchStats) {
    let elapsed = config.duration_secs as f64;
    let mut allocate_latencies = stats.allocate_latencies_us;
    let mut transfer_latencies = stats.transfer_latencies_us;
    let mut refresh_latencies = stats.route_refresh_latencies_us;
    allocate_latencies.sort_unstable();
    transfer_latencies.sort_unstable();
    refresh_latencies.sort_unstable();

    println!("scenario={}", config.scenario.as_str());
    println!("endpoint={}", config.endpoint);
    println!("timeline_namespace={}", config.timeline_namespace);
    println!("concurrency={}", config.concurrency);
    println!("timeline_count={}", config.timeline_count);
    println!("duration_secs={}", config.duration_secs);
    println!("warmup_secs={}", config.warmup_secs);
    println!("allocate_batch={}", config.allocate_batch);
    println!(
        "allocate_request_timeout_ms={}",
        config.allocate_request_timeout_ms
    );
    println!("idempotency_enabled={}", config.idempotency_enabled);
    println!("route_to_owners={}", config.route_to_owners);
    println!("transfer_interval_ms={}", config.transfer_interval_ms);
    println!(
        "transfer_target_generators={}",
        config
            .transfer_target_generators
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!(
        "transfer_target_owner_endpoints={}",
        config.transfer_target_owner_endpoints.join(",")
    );
    println!("allocate_requests_total={}", stats.allocate_requests_total);
    println!("allocate_success_total={}", stats.allocate_success_total);
    println!(
        "allocate_failed_total={}",
        stats
            .allocate_requests_total
            .saturating_sub(stats.allocate_success_total)
    );
    println!("allocate_tsos_total={}", stats.allocate_tsos_total);
    println!(
        "allocate_req_per_sec={:.2}",
        stats.allocate_requests_total as f64 / elapsed
    );
    println!(
        "allocate_success_per_sec={:.2}",
        stats.allocate_success_total as f64 / elapsed
    );
    println!(
        "allocate_tso_per_sec={:.2}",
        stats.allocate_tsos_total as f64 / elapsed
    );
    println!(
        "allocate_latency_p50_us={}",
        percentile(&allocate_latencies, 0.50)
    );
    println!(
        "allocate_latency_p95_us={}",
        percentile(&allocate_latencies, 0.95)
    );
    println!(
        "allocate_latency_p99_us={}",
        percentile(&allocate_latencies, 0.99)
    );
    println!(
        "allocate_latency_p999_us={}",
        percentile(&allocate_latencies, 0.999)
    );
    println!(
        "allocate_latency_max_us={}",
        allocate_latencies.last().copied().unwrap_or(0)
    );
    println!("route_refresh_total={}", stats.route_refresh_total);
    println!(
        "route_refresh_p50_us={}",
        percentile(&refresh_latencies, 0.50)
    );
    println!(
        "route_refresh_p95_us={}",
        percentile(&refresh_latencies, 0.95)
    );
    println!(
        "route_refresh_p99_us={}",
        percentile(&refresh_latencies, 0.99)
    );
    println!(
        "route_refresh_p999_us={}",
        percentile(&refresh_latencies, 0.999)
    );
    println!(
        "route_refresh_max_us={}",
        refresh_latencies.last().copied().unwrap_or(0)
    );
    println!("transfer_attempts_total={}", stats.transfer_attempts_total);
    println!("transfer_success_total={}", stats.transfer_success_total);
    println!("transfer_failed_total={}", stats.transfer_failed_total);
    println!(
        "transfer_latency_p50_us={}",
        percentile(&transfer_latencies, 0.50)
    );
    println!(
        "transfer_latency_p95_us={}",
        percentile(&transfer_latencies, 0.95)
    );
    println!(
        "transfer_latency_p99_us={}",
        percentile(&transfer_latencies, 0.99)
    );
    println!(
        "transfer_latency_p999_us={}",
        percentile(&transfer_latencies, 0.999)
    );
    println!(
        "transfer_latency_max_us={}",
        transfer_latencies.last().copied().unwrap_or(0)
    );
    println!(
        "monotonicity_violations_total={}",
        stats.monotonicity_violations_total
    );
    let mut error_labels = stats.error_counts.into_iter().collect::<Vec<_>>();
    error_labels.sort_by(|left, right| left.0.cmp(&right.0));
    for (label, count) in error_labels {
        println!("allocate_error_{}_total={}", label, count);
    }
}

fn print_status_summary(config: &BenchConfig, stats: StatusWorkerStats) {
    let elapsed = config.duration_secs as f64;
    let mut rpc_latencies = stats.rpc_latencies_us;
    let mut scan_latencies = stats.scan_latencies_us;
    rpc_latencies.sort_unstable();
    scan_latencies.sort_unstable();

    println!("scenario={}", config.scenario.as_str());
    println!("endpoint={}", config.endpoint);
    println!("timeline_namespace={}", config.timeline_namespace);
    println!("concurrency={}", config.concurrency);
    println!("timeline_count={}", config.timeline_count);
    println!("duration_secs={}", config.duration_secs);
    println!("warmup_secs={}", config.warmup_secs);
    println!("page_size={}", config.page_size);
    println!("filter_states={}", config.filter_states_display);
    println!(
        "filter_owner_endpoint={}",
        config
            .status_filters
            .owner_worker_endpoint
            .as_deref()
            .unwrap_or("")
    );
    println!("requests={}", stats.requests);
    println!("pages={}", stats.pages);
    println!("statuses={}", stats.statuses);
    println!("scans={}", stats.scans);
    println!("req_per_sec={:.2}", stats.requests as f64 / elapsed);
    println!("statuses_per_sec={:.2}", stats.statuses as f64 / elapsed);
    println!("rpc_latency_p50_us={}", percentile(&rpc_latencies, 0.50));
    println!("rpc_latency_p95_us={}", percentile(&rpc_latencies, 0.95));
    println!("rpc_latency_p99_us={}", percentile(&rpc_latencies, 0.99));
    println!("rpc_latency_p999_us={}", percentile(&rpc_latencies, 0.999));
    println!(
        "rpc_latency_max_us={}",
        rpc_latencies.last().copied().unwrap_or(0)
    );
    println!("scan_latency_p50_us={}", percentile(&scan_latencies, 0.50));
    println!("scan_latency_p95_us={}", percentile(&scan_latencies, 0.95));
    println!("scan_latency_p99_us={}", percentile(&scan_latencies, 0.99));
    println!(
        "scan_latency_p999_us={}",
        percentile(&scan_latencies, 0.999)
    );
    println!(
        "scan_latency_max_us={}",
        scan_latencies.last().copied().unwrap_or(0)
    );
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let config = load_config()?;
    let seed_channel = connect_channel(config.endpoint.clone()).await?;
    let seeded_routes = if config.seed_timelines {
        ensure_seed_routes(&config, seed_channel.clone()).await?
    } else {
        Vec::new()
    };
    match config.scenario {
        Scenario::StatusScan | Scenario::StatusScanFiltered => {
            let stats = run_status_scan_bench(&config).await?;
            print_status_summary(&config, stats);
        }
        Scenario::AllocateDuringRebalance => {
            let stats = run_allocate_during_rebalance_bench(&config, &seeded_routes).await?;
            print_rebalance_summary(&config, stats);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_scenario_accepts_supported_names() {
        assert_eq!(parse_scenario("status_scan").unwrap(), Scenario::StatusScan);
        assert_eq!(
            parse_scenario("status_scan_filtered").unwrap(),
            Scenario::StatusScanFiltered
        );
        assert_eq!(
            parse_scenario("allocate_during_rebalance").unwrap(),
            Scenario::AllocateDuringRebalance
        );
    }

    #[test]
    fn parse_filter_states_csv_maps_and_dedups() {
        let parsed = parse_filter_states_csv("active, recovering,active").unwrap();
        assert_eq!(
            parsed,
            vec![
                TimelineState::Active as i32,
                TimelineState::Recovering as i32
            ]
        );
    }

    #[test]
    fn rebalance_retry_classifier_treats_rate_limit_as_transient() {
        let detail = ErrorDetail {
            code: ErrorCode::RateLimited as i32,
            message: "backpressure".into(),
            current_epoch: 0,
            current_route_version: 0,
            redirect_endpoint: String::new(),
            action_blocker: 0,
            next_step: 0,
        };
        let status = Status::resource_exhausted("backpressure");

        assert_eq!(
            allocation_error_label(&status, Some(&detail)),
            "rate_limited"
        );
        assert!(is_transient_rebalance_error(&status, Some(&detail)));
        assert!(!should_refresh_route_after_rebalance_error(
            &status,
            Some(&detail)
        ));
        assert_eq!(rebalance_retry_backoff_ms(&status, Some(&detail)), 5);
    }

    #[test]
    fn rebalance_retry_classifier_treats_deadline_without_detail_as_transient() {
        let status = Status::deadline_exceeded("AllocateTimestamps timed out");

        assert!(decode_error_detail(&status).is_none());
        assert_eq!(allocation_error_label(&status, None), "deadline_exceeded");
        assert!(is_transient_rebalance_error(&status, None));
        assert!(should_refresh_route_after_rebalance_error(&status, None));
        assert_eq!(
            rebalance_retry_backoff_ms(&status, None),
            REBALANCE_ALLOCATE_RETRY_BACKOFF_MS
        );
    }

    #[test]
    fn next_target_generator_rotates_and_avoids_current_when_possible() {
        assert_eq!(next_target_generator(1, &[0, 1, 2]), Some(2));
        assert_eq!(next_target_generator(9, &[0, 1, 2]), Some(0));
        assert_eq!(next_target_generator(4, &[4]), Some(4));
    }
}
