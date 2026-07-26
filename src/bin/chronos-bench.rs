use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use prost::Message;
use tokio::sync::Barrier;
use tokio::time::sleep;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

use chronos::proto::v1::{
    timeline_route_service_client::TimelineRouteServiceClient,
    timestamp_service_client::TimestampServiceClient, AllocateTimestampsRequest,
    AllocateTimestampsResponse, EnsureTimelineRequest, ErrorCode, ErrorDetail, ResourceTier,
};

#[path = "support/bool_env.rs"]
mod support_bool_env;
#[path = "support/endpoint.rs"]
mod support_endpoint;
#[path = "support/endpoint_list.rs"]
mod support_endpoint_list;
#[path = "support/env.rs"]
mod support_env;
#[path = "support/stats.rs"]
mod support_stats;

use support_bool_env::env_bool_or;
use support_endpoint::{normalize_endpoint, normalize_endpoint_or_fallback};
use support_endpoint_list::{parse_endpoint_filter, parse_endpoint_list};
use support_env::{env_or, env_or_string};
use support_stats::percentile;

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type ChannelPools = BTreeMap<String, Vec<Channel>>;
type ClientPools = BTreeMap<String, Vec<TimestampServiceClient<Channel>>>;

#[derive(Clone)]
struct BenchConfig {
    endpoint: String,
    control_endpoints: Vec<String>,
    concurrency: usize,
    timeline_count: usize,
    batch: u32,
    duration_secs: u64,
    warmup_secs: u64,
    request_timeout_ms: u64,
    client_timeout_ms: u64,
    connect_timeout_ms: u64,
    connect_retry_interval_ms: u64,
    allocation_connection_pool_size: usize,
    worker_index_offset: usize,
    resource_tier: ResourceTier,
    scenario: String,
    idempotency_enabled: bool,
    route_to_owners: bool,
    owner_endpoint_filter: BTreeSet<String>,
    owner_affinity: bool,
    probe_only: bool,
    probe_request_id: String,
    cold_probe_enabled: bool,
    cold_probe_concurrency: usize,
    cold_request_timeout_ms: u64,
    cold_client_timeout_ms: u64,
}

#[derive(Clone)]
struct BenchRoute {
    timeline_key: String,
    epoch: u64,
    route_version: u64,
    owner_worker_endpoint: String,
}

#[derive(Clone)]
struct BenchAffinityGroup {
    endpoint: String,
    route_indexes: Vec<usize>,
}

impl BenchRoute {
    fn allocation_endpoint(&self, fallback_endpoint: &str, route_to_owners: bool) -> String {
        if route_to_owners {
            normalize_endpoint_or_fallback(&self.owner_worker_endpoint, fallback_endpoint)
        } else {
            fallback_endpoint.to_owned()
        }
    }
}

#[derive(Default)]
struct WorkerStats {
    requests: u64,
    tsos: u64,
    failed: u64,
    measured_failed: u64,
    failure_reasons: BTreeMap<String, u64>,
    measured_failure_reasons: BTreeMap<String, u64>,
    latencies_us: Vec<u64>,
}

#[derive(Default)]
struct ColdProbeStats {
    requests: u64,
    success: u64,
    failed: u64,
    tsos: u64,
    failure_reasons: BTreeMap<String, u64>,
    latencies_us: Vec<u64>,
}

struct ProbeResult {
    timeline_key: String,
    generator_id: u32,
    first_tso: u64,
    last_tso: u64,
    response_protobuf_hex: String,
    idempotency_replay_verified: bool,
}

fn parse_resource_tier(value: &str) -> ResourceTier {
    match value.to_ascii_lowercase().as_str() {
        "shared" => ResourceTier::Shared,
        "warm" => ResourceTier::Warm,
        "dedicated" => ResourceTier::Dedicated,
        _ => ResourceTier::Shared,
    }
}

fn load_config() -> BenchConfig {
    let endpoint = normalize_endpoint(&env_or_string(
        "CHRONOS_BENCH_ENDPOINT",
        "http://[::1]:50051",
    ));
    let control_endpoints = parse_endpoint_list("CHRONOS_BENCH_CONTROL_ENDPOINTS", &endpoint);
    let concurrency = env_or("CHRONOS_BENCH_CONCURRENCY", 32usize);
    let timeline_count = env_or("CHRONOS_BENCH_TIMELINES", concurrency.max(1));
    let batch = env_or("CHRONOS_BENCH_BATCH", 1u32);
    let duration_secs = env_or("CHRONOS_BENCH_DURATION_SECS", 10u64);
    let warmup_secs = env_or("CHRONOS_BENCH_WARMUP_SECS", 3u64);
    let request_timeout_ms = env_or("CHRONOS_BENCH_REQUEST_TIMEOUT_MS", 1000u64);
    let default_client_timeout_ms = if request_timeout_ms == 0 {
        1000
    } else {
        request_timeout_ms
    };
    let client_timeout_ms = env_or("CHRONOS_BENCH_CLIENT_TIMEOUT_MS", default_client_timeout_ms);
    let connect_timeout_ms = env_or(
        "CHRONOS_BENCH_CONNECT_TIMEOUT_MS",
        client_timeout_ms.max(15_000),
    );
    let connect_retry_interval_ms = env_or("CHRONOS_BENCH_CONNECT_RETRY_INTERVAL_MS", 100u64);
    let allocation_connection_pool_size =
        env_or("CHRONOS_BENCH_ALLOCATION_CONNECTION_POOL_SIZE", 1usize).max(1);
    let worker_index_offset = env_or("CHRONOS_BENCH_WORKER_INDEX_OFFSET", 0usize);
    let resource_tier =
        parse_resource_tier(&env_or_string("CHRONOS_BENCH_RESOURCE_TIER", "shared"));
    let scenario = env_or_string("CHRONOS_BENCH_SCENARIO", "round_robin");
    let idempotency_enabled = env_bool_or("CHRONOS_BENCH_IDEMPOTENCY", false);
    let route_to_owners = env_bool_or("CHRONOS_BENCH_ROUTE_TO_OWNERS", false);
    let owner_endpoint_filter = parse_endpoint_filter("CHRONOS_BENCH_OWNER_ENDPOINT_FILTER");
    let owner_affinity = env_bool_or("CHRONOS_BENCH_OWNER_AFFINITY", route_to_owners);
    let probe_only = env_bool_or("CHRONOS_BENCH_PROBE_ONLY", false);
    let probe_request_id = env_or_string("CHRONOS_BENCH_PROBE_REQUEST_ID", "probe");
    let cold_probe_enabled = env_bool_or("CHRONOS_BENCH_COLD_PROBE", false);
    let cold_probe_concurrency = env_or(
        "CHRONOS_BENCH_COLD_PROBE_CONCURRENCY",
        timeline_count.clamp(1, 16),
    );
    let cold_request_timeout_ms =
        env_or("CHRONOS_BENCH_COLD_REQUEST_TIMEOUT_MS", request_timeout_ms);
    let cold_client_timeout_ms = env_or("CHRONOS_BENCH_COLD_CLIENT_TIMEOUT_MS", client_timeout_ms);
    BenchConfig {
        endpoint,
        control_endpoints,
        concurrency,
        timeline_count,
        batch,
        duration_secs,
        warmup_secs,
        request_timeout_ms,
        client_timeout_ms,
        connect_timeout_ms,
        connect_retry_interval_ms,
        allocation_connection_pool_size,
        worker_index_offset,
        resource_tier,
        scenario,
        idempotency_enabled,
        route_to_owners,
        owner_endpoint_filter,
        owner_affinity,
        probe_only,
        probe_request_id,
        cold_probe_enabled,
        cold_probe_concurrency,
        cold_request_timeout_ms,
        cold_client_timeout_ms,
    }
}

async fn ensure_timelines(config: &BenchConfig) -> AppResult<Vec<BenchRoute>> {
    let mut clients = Vec::with_capacity(config.control_endpoints.len());
    for endpoint in &config.control_endpoints {
        clients.push(TimelineRouteServiceClient::new(
            connect_channel(
                endpoint.clone(),
                config.connect_timeout_ms,
                config.connect_retry_interval_ms,
            )
            .await?,
        ));
    }
    let mut routes = Vec::with_capacity(config.timeline_count);
    for idx in 0..config.timeline_count {
        let timeline_key = format!("bench.{}.{}", config.scenario, idx);
        let client_index = idx % clients.len();
        let client = &mut clients[client_index];
        let response = client
            .ensure_timeline(Request::new(EnsureTimelineRequest {
                timeline_key: timeline_key.clone(),
                desired_resource_tier: config.resource_tier as i32,
            }))
            .await?
            .into_inner();
        let route = response.route.ok_or("missing route")?;
        routes.push(BenchRoute {
            timeline_key: route.timeline_key,
            epoch: route.epoch,
            route_version: route.route_version,
            owner_worker_endpoint: route.owner_worker_endpoint,
        });
    }
    Ok(routes)
}

fn filter_routes_to_owner_endpoints(
    routes: Vec<BenchRoute>,
    owner_endpoint_filter: &BTreeSet<String>,
    fallback_endpoint: &str,
) -> AppResult<Vec<BenchRoute>> {
    if owner_endpoint_filter.is_empty() {
        return Ok(routes);
    }
    let filtered = routes
        .into_iter()
        .filter(|route| {
            owner_endpoint_filter.contains(&route.allocation_endpoint(fallback_endpoint, true))
        })
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        return Err(format!(
            "owner endpoint filter matched no routes: {}",
            owner_endpoint_filter
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(",")
        )
        .into());
    }
    Ok(filtered)
}

async fn connect_channel(
    endpoint: String,
    connect_timeout_ms: u64,
    retry_interval_ms: u64,
) -> AppResult<Channel> {
    let channel = Channel::from_shared(endpoint.clone())
        .map_err(|error| format!("invalid bench endpoint {endpoint}: {error}"))?;
    let connect_timeout = Duration::from_millis(connect_timeout_ms.max(1));
    let retry_interval = Duration::from_millis(retry_interval_ms.max(1));
    let per_attempt_cap = Duration::from_millis(2_000);
    let deadline = Instant::now() + connect_timeout;
    let mut attempts = 0u64;
    let mut last_error = String::from("not attempted");

    loop {
        attempts += 1;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let attempt_timeout = remaining.min(per_attempt_cap).max(Duration::from_millis(1));
        match tokio::time::timeout(
            attempt_timeout + Duration::from_millis(100),
            channel.clone().connect_timeout(attempt_timeout).connect(),
        )
        .await
        {
            Ok(Ok(channel)) => return Ok(channel),
            Ok(Err(error)) => {
                last_error = format!("{error:?}");
            }
            Err(_) => {
                last_error = format!("connect attempt exceeded {}ms", attempt_timeout.as_millis());
            }
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining <= retry_interval {
            break;
        }
        sleep(retry_interval).await;
    }

    Err(format!(
        "failed to connect bench endpoint {endpoint} within {connect_timeout_ms}ms after {attempts} attempt(s): {last_error}"
    )
    .into())
}

async fn connect_channel_pools(
    endpoints: Vec<String>,
    pool_size: usize,
    connect_timeout_ms: u64,
    retry_interval_ms: u64,
) -> AppResult<ChannelPools> {
    let mut channels = BTreeMap::new();
    for endpoint in endpoints {
        let mut pool = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            pool.push(
                connect_channel(endpoint.clone(), connect_timeout_ms, retry_interval_ms).await?,
            );
        }
        channels.insert(endpoint, pool);
    }
    Ok(channels)
}

fn clients_from_channel_pools(channels: &ChannelPools) -> ClientPools {
    channels
        .iter()
        .map(|(endpoint, pool)| {
            (
                endpoint.clone(),
                pool.iter()
                    .cloned()
                    .map(TimestampServiceClient::new)
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

fn pooled_client_mut<'a>(
    clients: &'a mut ClientPools,
    endpoint: &str,
    worker_idx: usize,
) -> AppResult<&'a mut TimestampServiceClient<Channel>> {
    let pool = clients
        .get_mut(endpoint)
        .ok_or_else(|| format!("missing allocation client pool for {endpoint}"))?;
    if pool.is_empty() {
        return Err(format!("empty allocation client pool for {endpoint}").into());
    }
    let index = worker_idx % pool.len();
    Ok(&mut pool[index])
}

fn record_count(counts: &mut BTreeMap<String, u64>, label: &str) {
    *counts.entry(label.to_owned()).or_default() += 1;
}

fn merge_counts(target: &mut BTreeMap<String, u64>, source: BTreeMap<String, u64>) {
    for (label, count) in source {
        *target.entry(label).or_default() += count;
    }
}

fn format_counts(counts: &BTreeMap<String, u64>) -> String {
    if counts.is_empty() {
        return "none".to_owned();
    }
    counts
        .iter()
        .map(|(label, count)| format!("{label}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn operation_is_measured(completed_at: Instant, warmup_until: Instant) -> bool {
    completed_at >= warmup_until
}

fn decode_error_detail(status: &Status) -> Option<ErrorDetail> {
    chronos::rpc::decode_error_detail_from_status_details(status.details())
}

fn error_code_label(error_code: i32) -> Option<&'static str> {
    match ErrorCode::try_from(error_code).ok()? {
        ErrorCode::NotTimelineOwner => Some("not_timeline_owner"),
        ErrorCode::RouteVersionMismatch => Some("route_version_mismatch"),
        ErrorCode::EpochMismatch => Some("epoch_mismatch"),
        ErrorCode::LeaseExpired => Some("lease_expired"),
        ErrorCode::RateLimited => Some("rate_limited"),
        ErrorCode::TemporarilyUnavailable => Some("temporarily_unavailable"),
        ErrorCode::TimelineNotFound => Some("timeline_not_found"),
        ErrorCode::InvalidArgument => Some("invalid_argument"),
        ErrorCode::Internal => Some("internal"),
        ErrorCode::Unspecified => None,
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

fn allocation_error_label(status: &Status) -> String {
    decode_error_detail(status)
        .and_then(|detail| error_code_label(detail.code))
        .unwrap_or_else(|| status_code_label(status.code()))
        .to_owned()
}

fn probe_bounds(
    response: &AllocateTimestampsResponse,
    expected_count: u32,
) -> AppResult<(u64, u64)> {
    let first_tso = response
        .ranges
        .first()
        .map(|range| range.start_tso)
        .ok_or("probe allocation returned no ranges")?;
    let last_tso = response
        .ranges
        .last()
        .map(|range| range.end_tso)
        .ok_or("probe allocation returned no ranges")?;
    let mut allocated = 0u64;
    for range in &response.ranges {
        if range.end_tso < range.start_tso {
            return Err("probe allocation returned an invalid range".into());
        }
        allocated = allocated
            .checked_add(range.end_tso - range.start_tso + 1)
            .ok_or("probe allocation range count overflowed")?;
    }
    if allocated != expected_count as u64 {
        return Err(format!(
            "probe allocation returned {allocated} timestamps, expected {expected_count}"
        )
        .into());
    }
    Ok((first_tso, last_tso))
}

fn verify_idempotent_replay(
    original: &AllocateTimestampsResponse,
    replay: &AllocateTimestampsResponse,
) -> AppResult<()> {
    if original != replay {
        return Err("probe idempotency replay returned a different allocation response".into());
    }
    Ok(())
}

fn protobuf_hex(message: &impl Message) -> String {
    message
        .encode_to_vec()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn allocate_timestamps_with_timeout(
    client: &mut TimestampServiceClient<Channel>,
    request: Request<AllocateTimestampsRequest>,
    client_timeout_ms: u64,
) -> Result<tonic::Response<AllocateTimestampsResponse>, String> {
    if client_timeout_ms == 0 {
        return client
            .allocate_timestamps(request)
            .await
            .map_err(|status| allocation_error_label(&status));
    }

    match tokio::time::timeout(
        Duration::from_millis(client_timeout_ms),
        client.allocate_timestamps(request),
    )
    .await
    {
        Ok(result) => result.map_err(|status| allocation_error_label(&status)),
        Err(_) => Err("client_timeout".to_owned()),
    }
}

fn route_owner_distribution(routes: &[BenchRoute]) -> (usize, usize, usize, String) {
    let mut counts = BTreeMap::<String, usize>::new();
    for route in routes {
        let owner = route.owner_worker_endpoint.trim();
        let owner = if owner.is_empty() { "unknown" } else { owner };
        *counts.entry(owner.to_owned()).or_default() += 1;
    }
    let owner_count = counts.len();
    let min_timelines = counts.values().copied().min().unwrap_or(0);
    let max_timelines = counts.values().copied().max().unwrap_or(0);
    let display = counts
        .into_iter()
        .map(|(owner, count)| format!("{owner}:{count}"))
        .collect::<Vec<_>>()
        .join(",");
    (owner_count, min_timelines, max_timelines, display)
}

fn allocation_endpoints(
    routes: &[BenchRoute],
    fallback_endpoint: &str,
    route_to_owners: bool,
) -> Vec<String> {
    let mut endpoints = BTreeSet::new();
    for route in routes {
        endpoints.insert(route.allocation_endpoint(fallback_endpoint, route_to_owners));
    }
    endpoints.into_iter().collect()
}

fn affinity_groups(
    routes: &[BenchRoute],
    fallback_endpoint: &str,
    route_to_owners: bool,
) -> Vec<BenchAffinityGroup> {
    let mut groups = BTreeMap::<String, Vec<usize>>::new();
    for (idx, route) in routes.iter().enumerate() {
        groups
            .entry(route.allocation_endpoint(fallback_endpoint, route_to_owners))
            .or_default()
            .push(idx);
    }
    groups
        .into_iter()
        .map(|(endpoint, route_indexes)| BenchAffinityGroup {
            endpoint,
            route_indexes,
        })
        .collect()
}

fn affinity_group_assignment(global_worker_idx: usize, group_count: usize) -> (usize, usize) {
    debug_assert!(group_count > 0);
    (
        global_worker_idx % group_count,
        global_worker_idx / group_count,
    )
}

async fn run_probe(
    config: &BenchConfig,
    routes: &[BenchRoute],
    allocation_channels: &ChannelPools,
) -> AppResult<ProbeResult> {
    if routes.len() != 1 {
        return Err(format!(
            "probe-only mode requires exactly one timeline, got {}",
            routes.len()
        )
        .into());
    }
    if !config.idempotency_enabled || config.probe_request_id.is_empty() {
        return Err(
            "probe-only mode requires CHRONOS_BENCH_IDEMPOTENCY=true and a non-empty CHRONOS_BENCH_PROBE_REQUEST_ID"
                .into(),
        );
    }

    let route = &routes[0];
    let allocation_endpoint = route.allocation_endpoint(&config.endpoint, config.route_to_owners);
    let mut clients = clients_from_channel_pools(allocation_channels);
    let client = pooled_client_mut(&mut clients, &allocation_endpoint, 0)?;
    let allocation_request = AllocateTimestampsRequest {
        timeline_key: route.timeline_key.clone(),
        count: config.batch,
        expected_epoch: route.epoch,
        expected_route_version: route.route_version,
        client_request_id: if config.idempotency_enabled {
            config.probe_request_id.clone()
        } else {
            String::new()
        },
        request_timeout_ms: config.request_timeout_ms.min(u32::MAX as u64) as u32,
    };
    let response = allocate_timestamps_with_timeout(
        client,
        Request::new(allocation_request.clone()),
        config.client_timeout_ms,
    )
    .await
    .map_err(|reason| format!("probe allocation failed: {reason}"))?
    .into_inner();
    let (first_tso, last_tso) = probe_bounds(&response, config.batch)?;

    let replay = allocate_timestamps_with_timeout(
        client,
        Request::new(allocation_request),
        config.client_timeout_ms,
    )
    .await
    .map_err(|reason| format!("probe idempotency replay failed: {reason}"))?
    .into_inner();
    verify_idempotent_replay(&response, &replay)?;
    let response_protobuf_hex = protobuf_hex(&response);

    Ok(ProbeResult {
        timeline_key: response.timeline_key,
        generator_id: response.generator_id,
        first_tso,
        last_tso,
        response_protobuf_hex,
        idempotency_replay_verified: true,
    })
}

async fn run_cold_probe(
    config: &BenchConfig,
    routes: Arc<Vec<BenchRoute>>,
    allocation_channels: Arc<ChannelPools>,
) -> AppResult<ColdProbeStats> {
    if !config.cold_probe_enabled || routes.is_empty() {
        return Ok(ColdProbeStats::default());
    }

    let worker_count = config.cold_probe_concurrency.min(routes.len()).max(1);
    let mut handles = Vec::with_capacity(worker_count);

    for worker_idx in 0..worker_count {
        let routes = routes.clone();
        let endpoint = config.endpoint.clone();
        let route_to_owners = config.route_to_owners;
        let allocation_channels = allocation_channels.clone();
        let idempotency_enabled = config.idempotency_enabled;
        let request_timeout_ms = config.cold_request_timeout_ms;
        let client_timeout_ms = config.cold_client_timeout_ms;
        handles.push(tokio::spawn(async move {
            let mut clients = clients_from_channel_pools(allocation_channels.as_ref());

            let mut stats = ColdProbeStats::default();
            for route_idx in (worker_idx..routes.len()).step_by(worker_count) {
                let route = &routes[route_idx];
                let allocation_endpoint = route.allocation_endpoint(&endpoint, route_to_owners);
                let client = pooled_client_mut(&mut clients, &allocation_endpoint, worker_idx)?;
                let start = Instant::now();
                let request = Request::new(AllocateTimestampsRequest {
                    timeline_key: route.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route.epoch,
                    expected_route_version: route.route_version,
                    client_request_id: if idempotency_enabled {
                        format!("cold-probe-{worker_idx}-{route_idx}")
                    } else {
                        String::new()
                    },
                    request_timeout_ms: request_timeout_ms.min(u32::MAX as u64) as u32,
                });
                let result =
                    allocate_timestamps_with_timeout(client, request, client_timeout_ms).await;
                let elapsed = start.elapsed().as_micros() as u64;
                stats.requests += 1;
                stats.latencies_us.push(elapsed);
                match result {
                    Ok(response) => {
                        stats.success += 1;
                        stats.tsos += response
                            .into_inner()
                            .ranges
                            .iter()
                            .map(|range| range.end_tso - range.start_tso + 1)
                            .sum::<u64>();
                    }
                    Err(reason) => {
                        stats.failed += 1;
                        record_count(&mut stats.failure_reasons, &reason);
                    }
                }
            }
            Ok::<ColdProbeStats, Box<dyn Error + Send + Sync>>(stats)
        }));
    }

    let mut total = ColdProbeStats::default();
    for handle in handles {
        let stats = handle.await??;
        total.requests += stats.requests;
        total.success += stats.success;
        total.failed += stats.failed;
        total.tsos += stats.tsos;
        merge_counts(&mut total.failure_reasons, stats.failure_reasons);
        total.latencies_us.extend(stats.latencies_us);
    }
    total.latencies_us.sort_unstable();
    Ok(total)
}

fn main() -> AppResult<()> {
    chronos::process_runtime::build_multi_thread_runtime(
        "CHRONOS_BENCH_RUNTIME_WORKER_THREADS",
        "chronos-bench-runtime",
    )?
    .block_on(run())
}

async fn run() -> AppResult<()> {
    let config = load_config();
    let routes = Arc::new(filter_routes_to_owner_endpoints(
        ensure_timelines(&config).await?,
        &config.owner_endpoint_filter,
        &config.endpoint,
    )?);
    let (
        route_owner_endpoints,
        route_owner_min_timelines,
        route_owner_max_timelines,
        route_owner_counts,
    ) = route_owner_distribution(&routes);
    let allocation_channels = Arc::new(
        connect_channel_pools(
            allocation_endpoints(&routes, &config.endpoint, config.route_to_owners),
            config.allocation_connection_pool_size,
            config.connect_timeout_ms,
            config.connect_retry_interval_ms,
        )
        .await?,
    );
    if config.probe_only {
        let probe = run_probe(&config, &routes, &allocation_channels).await?;
        println!("scenario={}", config.scenario);
        println!("probe_timeline_key={}", probe.timeline_key);
        println!("probe_generator_id={}", probe.generator_id);
        println!("probe_first_tso={}", probe.first_tso);
        println!("probe_last_tso={}", probe.last_tso);
        println!(
            "probe_response_protobuf_hex={}",
            probe.response_protobuf_hex
        );
        println!(
            "probe_idempotency_replay_verified={}",
            probe.idempotency_replay_verified
        );
        return Ok(());
    }
    let cold_probe_stats =
        run_cold_probe(&config, routes.clone(), allocation_channels.clone()).await?;
    let affinity_groups = Arc::new(if config.owner_affinity {
        affinity_groups(&routes, &config.endpoint, config.route_to_owners)
    } else {
        Vec::new()
    });

    let barrier = Arc::new(Barrier::new(config.concurrency + 1));
    let id_gen = Arc::new(AtomicU64::new(1));
    let warmup_until = Instant::now() + Duration::from_secs(config.warmup_secs);
    let measure_until = warmup_until + Duration::from_secs(config.duration_secs);

    let mut handles = Vec::with_capacity(config.concurrency);
    for worker_idx in 0..config.concurrency {
        let global_worker_idx = config.worker_index_offset + worker_idx;
        let barrier = barrier.clone();
        let routes = routes.clone();
        let endpoint = config.endpoint.clone();
        let route_to_owners = config.route_to_owners;
        let owner_affinity = config.owner_affinity;
        let affinity_groups = affinity_groups.clone();
        let allocation_channels = allocation_channels.clone();
        let id_gen = id_gen.clone();
        let batch = config.batch;
        let idempotency_enabled = config.idempotency_enabled;
        let request_timeout_ms = config.request_timeout_ms;
        let client_timeout_ms = config.client_timeout_ms;
        handles.push(tokio::spawn(async move {
            let mut clients = clients_from_channel_pools(allocation_channels.as_ref());
            let mut stats = WorkerStats::default();
            let mut route_idx = global_worker_idx % routes.len();
            let affinity_group = if owner_affinity && !affinity_groups.is_empty() {
                let (group_idx, _) =
                    affinity_group_assignment(global_worker_idx, affinity_groups.len());
                Some(affinity_groups[group_idx].clone())
            } else {
                None
            };
            let mut affinity_route_idx = affinity_group
                .as_ref()
                .map(|group| {
                    let (_, owner_worker_idx) =
                        affinity_group_assignment(global_worker_idx, affinity_groups.len());
                    owner_worker_idx % group.route_indexes.len()
                })
                .unwrap_or(0);

            barrier.wait().await;
            loop {
                let now = Instant::now();
                if now >= measure_until {
                    break;
                }

                let (route, allocation_endpoint) = if let Some(group) = &affinity_group {
                    let route = &routes[group.route_indexes[affinity_route_idx]];
                    affinity_route_idx = (affinity_route_idx + 1) % group.route_indexes.len();
                    (route, group.endpoint.clone())
                } else {
                    let route = &routes[route_idx];
                    route_idx = (route_idx + 1) % routes.len();
                    (route, route.allocation_endpoint(&endpoint, route_to_owners))
                };
                let client =
                    pooled_client_mut(&mut clients, &allocation_endpoint, global_worker_idx)?;
                let client_request_id = if idempotency_enabled {
                    format!(
                        "{}-{}",
                        global_worker_idx,
                        id_gen.fetch_add(1, Ordering::Relaxed)
                    )
                } else {
                    String::new()
                };

                let start = Instant::now();
                let request = Request::new(AllocateTimestampsRequest {
                    timeline_key: route.timeline_key.clone(),
                    count: batch,
                    expected_epoch: route.epoch,
                    expected_route_version: route.route_version,
                    client_request_id,
                    request_timeout_ms: request_timeout_ms.min(u32::MAX as u64) as u32,
                });
                let result =
                    allocate_timestamps_with_timeout(client, request, client_timeout_ms).await;
                let elapsed = start.elapsed().as_micros() as u64;
                let measured = operation_is_measured(Instant::now(), warmup_until);
                match result {
                    Ok(_) if measured => {
                        stats.requests += 1;
                        stats.tsos += batch as u64;
                        stats.latencies_us.push(elapsed);
                    }
                    Ok(_) => {}
                    Err(reason) => {
                        stats.failed += 1;
                        record_count(&mut stats.failure_reasons, &reason);
                        if measured {
                            stats.measured_failed += 1;
                            record_count(&mut stats.measured_failure_reasons, &reason);
                        }
                    }
                }
            }
            Ok::<WorkerStats, Box<dyn Error + Send + Sync>>(stats)
        }));
    }

    barrier.wait().await;
    let started = Instant::now();

    let mut total_requests = 0u64;
    let mut total_tsos = 0u64;
    let mut total_failed = 0u64;
    let mut measured_failed = 0u64;
    let mut failure_reasons = BTreeMap::new();
    let mut measured_failure_reasons = BTreeMap::new();
    let mut latencies = Vec::new();
    for handle in handles {
        let stats = handle.await??;
        total_requests += stats.requests;
        total_tsos += stats.tsos;
        total_failed += stats.failed;
        measured_failed += stats.measured_failed;
        merge_counts(&mut failure_reasons, stats.failure_reasons);
        merge_counts(
            &mut measured_failure_reasons,
            stats.measured_failure_reasons,
        );
        latencies.extend(stats.latencies_us);
    }
    let elapsed = started.elapsed().as_secs_f64() - config.warmup_secs as f64;
    latencies.sort_unstable();

    println!("scenario={}", config.scenario);
    println!("endpoint={}", config.endpoint);
    println!("control_endpoints={}", config.control_endpoints.join(","));
    println!("route_to_owners={}", config.route_to_owners);
    println!(
        "owner_endpoint_filter={}",
        if config.owner_endpoint_filter.is_empty() {
            "none".to_owned()
        } else {
            config
                .owner_endpoint_filter
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(",")
        }
    );
    println!("route_owner_endpoints={}", route_owner_endpoints);
    println!("route_owner_min_timelines={}", route_owner_min_timelines);
    println!("route_owner_max_timelines={}", route_owner_max_timelines);
    println!("route_owner_counts={}", route_owner_counts);
    println!("concurrency={}", config.concurrency);
    println!("timelines={}", routes.len());
    println!("batch={}", config.batch);
    println!("idempotency_enabled={}", config.idempotency_enabled);
    println!("owner_affinity={}", config.owner_affinity);
    println!("request_timeout_ms={}", config.request_timeout_ms);
    println!("client_timeout_ms={}", config.client_timeout_ms);
    println!("connect_timeout_ms={}", config.connect_timeout_ms);
    println!(
        "connect_retry_interval_ms={}",
        config.connect_retry_interval_ms
    );
    println!(
        "allocation_connection_pool_size={}",
        config.allocation_connection_pool_size
    );
    println!("worker_index_offset={}", config.worker_index_offset);
    println!("cold_probe_enabled={}", config.cold_probe_enabled);
    println!("cold_probe_concurrency={}", config.cold_probe_concurrency);
    println!("cold_request_timeout_ms={}", config.cold_request_timeout_ms);
    println!("cold_client_timeout_ms={}", config.cold_client_timeout_ms);
    println!("cold_probe_requests={}", cold_probe_stats.requests);
    println!("cold_probe_success_total={}", cold_probe_stats.success);
    println!("cold_probe_failed_total={}", cold_probe_stats.failed);
    println!(
        "cold_probe_failure_reasons={}",
        format_counts(&cold_probe_stats.failure_reasons)
    );
    println!("cold_probe_tsos={}", cold_probe_stats.tsos);
    println!(
        "cold_probe_latency_p50_us={}",
        percentile(&cold_probe_stats.latencies_us, 0.50)
    );
    println!(
        "cold_probe_latency_p95_us={}",
        percentile(&cold_probe_stats.latencies_us, 0.95)
    );
    println!(
        "cold_probe_latency_p99_us={}",
        percentile(&cold_probe_stats.latencies_us, 0.99)
    );
    println!(
        "cold_probe_latency_p999_us={}",
        percentile(&cold_probe_stats.latencies_us, 0.999)
    );
    println!(
        "cold_probe_latency_max_us={}",
        cold_probe_stats.latencies_us.last().copied().unwrap_or(0)
    );
    println!("duration_secs={}", config.duration_secs);
    println!("requests={}", total_requests);
    println!("tsos={}", total_tsos);
    println!("allocation_failed_total={}", total_failed);
    println!("allocation_measured_failed_total={}", measured_failed);
    println!(
        "allocation_failure_reasons={}",
        format_counts(&failure_reasons)
    );
    println!(
        "allocation_measured_failure_reasons={}",
        format_counts(&measured_failure_reasons)
    );
    println!("req_per_sec={:.2}", total_requests as f64 / elapsed);
    println!("tso_per_sec={:.2}", total_tsos as f64 / elapsed);
    println!("latency_p50_us={}", percentile(&latencies, 0.50));
    println!("latency_p95_us={}", percentile(&latencies, 0.95));
    println!("latency_p99_us={}", percentile(&latencies, 0.99));
    println!("latency_p999_us={}", percentile(&latencies, 0.999));
    println!("latency_max_us={}", latencies.last().copied().unwrap_or(0));

    Ok(())
}

#[cfg(test)]
mod tests {
    use prost::bytes::Bytes;

    use super::*;

    #[test]
    fn allocation_error_label_prefers_structured_error_detail() {
        let detail = ErrorDetail {
            code: ErrorCode::RouteVersionMismatch as i32,
            message: "route version mismatch".to_owned(),
            current_epoch: 0,
            current_route_version: 42,
            redirect_endpoint: String::new(),
            action_blocker: 0,
            next_step: 0,
        };
        let status = Status::with_details(
            Code::FailedPrecondition,
            "route version mismatch",
            Bytes::from(detail.encode_to_vec()),
        );

        assert_eq!(allocation_error_label(&status), "route_version_mismatch");
    }

    #[test]
    fn allocation_error_label_falls_back_to_transport_code() {
        let status = Status::unavailable("connection refused");

        assert_eq!(allocation_error_label(&status), "transport_unavailable");
    }

    #[test]
    fn probe_bounds_require_the_expected_allocation_count() {
        let response = AllocateTimestampsResponse {
            timeline_key: "bench.restore.0".to_owned(),
            generator_id: 7,
            epoch: 1,
            route_version: 1,
            ranges: vec![chronos::proto::v1::TimestampRange {
                start_tso: 100,
                end_tso: 101,
            }],
        };

        assert_eq!(probe_bounds(&response, 2).unwrap(), (100, 101));
        assert!(probe_bounds(&response, 1).is_err());
    }

    #[test]
    fn probe_idempotency_replay_rejects_a_different_range() {
        let original = AllocateTimestampsResponse {
            timeline_key: "bench.restore.0".to_owned(),
            generator_id: 7,
            epoch: 1,
            route_version: 1,
            ranges: vec![chronos::proto::v1::TimestampRange {
                start_tso: 100,
                end_tso: 100,
            }],
        };
        let mut replay = original.clone();

        assert!(verify_idempotent_replay(&original, &replay).is_ok());
        replay.ranges[0].start_tso = 101;
        replay.ranges[0].end_tso = 101;
        assert!(verify_idempotent_replay(&original, &replay).is_err());
    }

    #[test]
    fn probe_response_hex_covers_the_complete_protobuf_response() {
        let response = AllocateTimestampsResponse {
            timeline_key: "timeline-a".to_owned(),
            generator_id: 7,
            epoch: 9,
            route_version: 11,
            ranges: vec![chronos::proto::v1::TimestampRange {
                start_tso: 100,
                end_tso: 101,
            }],
        };
        let encoded = protobuf_hex(&response);
        assert!(!encoded.is_empty());

        let bytes = encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|digits| {
                u8::from_str_radix(std::str::from_utf8(digits).expect("hex must be UTF-8"), 16)
                    .expect("hex byte")
            })
            .collect::<Vec<_>>();
        let decoded =
            AllocateTimestampsResponse::decode(bytes.as_slice()).expect("decode probe response");
        assert_eq!(decoded, response);
    }

    #[test]
    fn format_counts_is_stable_and_empty_safe() {
        let mut counts = BTreeMap::new();

        assert_eq!(format_counts(&counts), "none");

        record_count(&mut counts, "temporarily_unavailable");
        record_count(&mut counts, "client_timeout");
        record_count(&mut counts, "temporarily_unavailable");

        assert_eq!(
            format_counts(&counts),
            "client_timeout:1,temporarily_unavailable:2"
        );
    }

    #[test]
    fn operation_completing_after_warmup_is_measured() {
        let warmup_until = Instant::now() + Duration::from_secs(1);

        assert!(!operation_is_measured(
            warmup_until - Duration::from_millis(1),
            warmup_until
        ));
        assert!(operation_is_measured(warmup_until, warmup_until));
        assert!(operation_is_measured(
            warmup_until + Duration::from_millis(1),
            warmup_until
        ));
    }

    #[test]
    fn owner_affinity_distribution_uses_global_worker_offsets() {
        let mut group_counts = [0usize; 3];
        for offset in [0usize, 4, 8] {
            for worker_idx in 0..4 {
                let global_worker_idx = offset + worker_idx;
                let (group_idx, _) = affinity_group_assignment(global_worker_idx, 3);
                group_counts[group_idx] += 1;
            }
        }

        assert_eq!(group_counts, [4, 4, 4]);
    }

    #[test]
    fn owner_endpoint_filter_keeps_only_matching_routes() {
        let routes = vec![
            BenchRoute {
                timeline_key: "bench.a".to_owned(),
                epoch: 1,
                route_version: 1,
                owner_worker_endpoint: "127.0.0.1:50051".to_owned(),
            },
            BenchRoute {
                timeline_key: "bench.b".to_owned(),
                epoch: 1,
                route_version: 1,
                owner_worker_endpoint: "127.0.0.1:50052".to_owned(),
            },
        ];
        let filter = BTreeSet::from(["http://127.0.0.1:50052".to_owned()]);

        let filtered =
            filter_routes_to_owner_endpoints(routes, &filter, "http://127.0.0.1:50051").unwrap();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].timeline_key, "bench.b");
    }
}
