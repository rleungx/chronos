use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Barrier;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::Request;

use chronos::proto::v1::{
    timeline_route_event, timeline_route_service_client::TimelineRouteServiceClient,
    timeline_status_service_client::TimelineStatusServiceClient, EnsureTimelineRequest,
    ListTimelineStatusesRequest, ResourceTier, TimelineState, WatchTimelineRoutesRequest,
};

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    StatusScan,
    StatusScanFiltered,
    WatchAllSnapshot,
    WatchFilteredSnapshot,
}

impl Scenario {
    fn as_str(self) -> &'static str {
        match self {
            Self::StatusScan => "status_scan",
            Self::StatusScanFiltered => "status_scan_filtered",
            Self::WatchAllSnapshot => "watch_all_snapshot",
            Self::WatchFilteredSnapshot => "watch_filtered_snapshot",
        }
    }
}

#[derive(Clone, Debug, Default)]
struct StatusFilters {
    states: Vec<i32>,
    owner_worker_endpoint: Option<String>,
}

#[derive(Clone, Debug)]
struct BenchConfig {
    endpoint: String,
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
    watch_filter_keys: Vec<String>,
    watch_snapshot_timeout_ms: u64,
    sdk_instance_id: String,
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
struct WatchWorkerStats {
    sessions: u64,
    routes_observed: u64,
    timeouts: u64,
    stream_errors: u64,
    incomplete_keys_total: u64,
    stream_open_time_us: Vec<u64>,
    snapshot_transfer_time_us: Vec<u64>,
    snapshot_end_to_end_time_us: Vec<u64>,
}

struct SnapshotTracker {
    expected: Arc<HashSet<String>>,
    seen: HashSet<String>,
}

impl SnapshotTracker {
    fn new(expected: Arc<HashSet<String>>) -> Self {
        Self {
            expected,
            seen: HashSet::new(),
        }
    }

    fn on_route(&mut self, timeline_key: &str) -> bool {
        if !self.expected.contains(timeline_key) {
            return false;
        }
        self.seen.insert(timeline_key.to_owned());
        self.is_complete()
    }

    fn is_complete(&self) -> bool {
        self.seen.len() == self.expected.len()
    }

    fn incomplete_count(&self) -> usize {
        self.expected.len().saturating_sub(self.seen.len())
    }
}

fn env_or<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<T>().ok())
        .unwrap_or(default)
}

fn env_or_string(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn parse_boolish(value: &str) -> AppResult<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(format!("invalid boolean value: {other}").into()),
    }
}

fn parse_scenario(value: &str) -> AppResult<Scenario> {
    match value.trim().to_ascii_lowercase().as_str() {
        "status_scan" => Ok(Scenario::StatusScan),
        "status_scan_filtered" => Ok(Scenario::StatusScanFiltered),
        "watch_all_snapshot" => Ok(Scenario::WatchAllSnapshot),
        "watch_filtered_snapshot" => Ok(Scenario::WatchFilteredSnapshot),
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

fn parse_csv_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn load_config() -> AppResult<BenchConfig> {
    let endpoint = env_or_string("CHRONOS_CONTROL_BENCH_ENDPOINT", "http://[::1]:50051");
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
    let watch_filter_keys = parse_csv_list(&env_or_string(
        "CHRONOS_CONTROL_BENCH_WATCH_FILTER_KEYS",
        "",
    ));
    let watch_snapshot_timeout_ms =
        env_or("CHRONOS_CONTROL_BENCH_WATCH_SNAPSHOT_TIMEOUT_MS", 2_000u64);
    let sdk_instance_id = env_or_string("CHRONOS_CONTROL_BENCH_SDK_INSTANCE_ID", "control-bench");

    Ok(BenchConfig {
        endpoint,
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
        watch_filter_keys,
        watch_snapshot_timeout_ms,
        sdk_instance_id,
    })
}

async fn connect_channel(endpoint: String) -> AppResult<Channel> {
    let channel = Channel::from_shared(endpoint.clone())
        .map_err(|error| format!("invalid bench endpoint {endpoint}: {error}"))?;
    channel
        .connect()
        .await
        .map_err(|error| format!("failed to connect bench endpoint {endpoint}: {error}").into())
}

async fn ensure_seed_timelines(config: &BenchConfig, channel: Channel) -> AppResult<Vec<String>> {
    let mut client = TimelineRouteServiceClient::new(channel);
    let mut timeline_keys = Vec::with_capacity(config.timeline_count);
    for idx in 0..config.timeline_count {
        let timeline_key = format!("controlbench.{}.{}", config.scenario.as_str(), idx);
        client
            .ensure_timeline(Request::new(EnsureTimelineRequest {
                timeline_key: timeline_key.clone(),
                desired_resource_tier: config.resource_tier as i32,
            }))
            .await?;
        timeline_keys.push(timeline_key);
    }
    Ok(timeline_keys)
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

async fn fetch_all_status_keys(channel: Channel, page_size: u32) -> AppResult<HashSet<String>> {
    let mut client = TimelineStatusServiceClient::new(channel);
    let filters = StatusFilters::default();
    let mut keys = HashSet::new();
    let mut page_token = String::new();

    loop {
        let response =
            list_timeline_statuses_page(&mut client, &filters, page_size, page_token).await?;
        for status in response.statuses {
            let route = status.route.ok_or("timeline status missing route")?;
            keys.insert(route.timeline_key);
        }
        if response.next_page_token.is_empty() {
            break;
        }
        page_token = response.next_page_token;
    }

    Ok(keys)
}

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * pct).round() as usize;
    sorted[idx]
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
            let channel = connect_channel(endpoint).await?;
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

#[derive(Clone)]
enum WatchScenario {
    All { expected_keys: Arc<HashSet<String>> },
    Filtered { expected_keys: Arc<HashSet<String>> },
}

impl WatchScenario {
    fn expected_keys(&self) -> Arc<HashSet<String>> {
        match self {
            Self::All { expected_keys } | Self::Filtered { expected_keys } => expected_keys.clone(),
        }
    }

    fn build_request(&self, sdk_instance_id: String) -> WatchTimelineRoutesRequest {
        match self {
            Self::All { .. } => WatchTimelineRoutesRequest {
                timeline_keys: Vec::new(),
                known_route_versions: HashMap::new(),
                sdk_instance_id,
            },
            Self::Filtered { expected_keys } => WatchTimelineRoutesRequest {
                timeline_keys: expected_keys.iter().cloned().collect(),
                known_route_versions: expected_keys
                    .iter()
                    .cloned()
                    .map(|timeline_key| (timeline_key, 0))
                    .collect(),
                sdk_instance_id,
            },
        }
    }
}

async fn run_watch_snapshot_bench(
    config: &BenchConfig,
    scenario: WatchScenario,
) -> AppResult<WatchWorkerStats> {
    let barrier = Arc::new(Barrier::new(config.concurrency + 1));
    let warmup_until = Instant::now() + Duration::from_secs(config.warmup_secs);
    let measure_until = warmup_until + Duration::from_secs(config.duration_secs);
    let timeout = Duration::from_millis(config.watch_snapshot_timeout_ms);
    let mut handles = Vec::with_capacity(config.concurrency);

    for worker_idx in 0..config.concurrency {
        let barrier = barrier.clone();
        let endpoint = config.endpoint.clone();
        let scenario = scenario.clone();
        let sdk_base = config.sdk_instance_id.clone();
        handles.push(tokio::spawn(async move {
            let channel = connect_channel(endpoint).await?;
            let mut client = TimelineRouteServiceClient::new(channel);
            let mut stats = WatchWorkerStats::default();
            let mut session_ordinal = 0u64;

            barrier.wait().await;
            loop {
                let session_start = Instant::now();
                if session_start >= measure_until {
                    break;
                }

                let sdk_instance_id = format!("{sdk_base}-{worker_idx}-{session_ordinal}");
                session_ordinal = session_ordinal.saturating_add(1);

                let request = scenario.build_request(sdk_instance_id);
                let expected_keys = scenario.expected_keys();
                let mut tracker = SnapshotTracker::new(expected_keys.clone());
                let stream_open_start = Instant::now();
                let stream = match client.watch_timeline_routes(Request::new(request)).await {
                    Ok(response) => response.into_inner(),
                    Err(_) => {
                        if session_start < measure_until && Instant::now() >= warmup_until {
                            stats.stream_errors += 1;
                        }
                        continue;
                    }
                };
                let stream_open_elapsed = stream_open_start.elapsed().as_micros() as u64;
                let snapshot_transfer_start = Instant::now();

                let routes_observed =
                    tokio::time::timeout(timeout, consume_snapshot(stream, &mut tracker)).await;
                let record_session =
                    session_start < measure_until && Instant::now() >= warmup_until;

                match routes_observed {
                    Ok(Ok(routes_observed)) => {
                        if record_session {
                            stats.sessions += 1;
                            stats.routes_observed += routes_observed;
                            stats.stream_open_time_us.push(stream_open_elapsed);
                            stats
                                .snapshot_transfer_time_us
                                .push(snapshot_transfer_start.elapsed().as_micros() as u64);
                            stats
                                .snapshot_end_to_end_time_us
                                .push(session_start.elapsed().as_micros() as u64);
                        }
                    }
                    Ok(Err(_)) => {
                        if record_session {
                            stats.stream_errors += 1;
                            stats.incomplete_keys_total += tracker.incomplete_count() as u64;
                        }
                    }
                    Err(_) => {
                        if record_session {
                            stats.timeouts += 1;
                            stats.incomplete_keys_total += tracker.incomplete_count() as u64;
                        }
                    }
                }
            }

            Ok::<WatchWorkerStats, Box<dyn Error + Send + Sync>>(stats)
        }));
    }

    barrier.wait().await;
    let mut total = WatchWorkerStats::default();
    for handle in handles {
        let stats = handle.await??;
        total.sessions += stats.sessions;
        total.routes_observed += stats.routes_observed;
        total.timeouts += stats.timeouts;
        total.stream_errors += stats.stream_errors;
        total.incomplete_keys_total += stats.incomplete_keys_total;
        total.stream_open_time_us.extend(stats.stream_open_time_us);
        total
            .snapshot_transfer_time_us
            .extend(stats.snapshot_transfer_time_us);
        total
            .snapshot_end_to_end_time_us
            .extend(stats.snapshot_end_to_end_time_us);
    }
    Ok(total)
}

async fn consume_snapshot(
    mut stream: tonic::Streaming<chronos::proto::v1::TimelineRouteEvent>,
    tracker: &mut SnapshotTracker,
) -> AppResult<u64> {
    if tracker.is_complete() {
        return Ok(0);
    }

    let mut routes_observed = 0u64;
    while let Some(event) = stream.next().await {
        let event = event?;
        match event.event {
            Some(timeline_route_event::Event::Route(route)) => {
                routes_observed += 1;
                if tracker.on_route(&route.timeline_key) {
                    return Ok(routes_observed);
                }
            }
            Some(timeline_route_event::Event::Tombstone(_)) => {}
            Some(timeline_route_event::Event::Keepalive(_)) => {}
            None => {}
        }
    }

    Err("watch stream ended before snapshot completed".into())
}

fn print_status_summary(config: &BenchConfig, stats: StatusWorkerStats) {
    let elapsed = config.duration_secs as f64;
    let mut rpc_latencies = stats.rpc_latencies_us;
    let mut scan_latencies = stats.scan_latencies_us;
    rpc_latencies.sort_unstable();
    scan_latencies.sort_unstable();

    println!("scenario={}", config.scenario.as_str());
    println!("endpoint={}", config.endpoint);
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
    println!(
        "rpc_latency_max_us={}",
        rpc_latencies.last().copied().unwrap_or(0)
    );
    println!("scan_latency_p50_us={}", percentile(&scan_latencies, 0.50));
    println!("scan_latency_p95_us={}", percentile(&scan_latencies, 0.95));
    println!("scan_latency_p99_us={}", percentile(&scan_latencies, 0.99));
    println!(
        "scan_latency_max_us={}",
        scan_latencies.last().copied().unwrap_or(0)
    );
}

fn print_watch_summary(
    config: &BenchConfig,
    expected_keys: &HashSet<String>,
    stats: WatchWorkerStats,
) {
    let elapsed = config.duration_secs as f64;
    let mut stream_open_times = stats.stream_open_time_us;
    let mut snapshot_transfer_times = stats.snapshot_transfer_time_us;
    let mut snapshot_end_to_end_times = stats.snapshot_end_to_end_time_us;
    stream_open_times.sort_unstable();
    snapshot_transfer_times.sort_unstable();
    snapshot_end_to_end_times.sort_unstable();

    println!("scenario={}", config.scenario.as_str());
    println!("endpoint={}", config.endpoint);
    println!("concurrency={}", config.concurrency);
    println!("timeline_count={}", config.timeline_count);
    println!("duration_secs={}", config.duration_secs);
    println!("warmup_secs={}", config.warmup_secs);
    println!("expected_keys={}", expected_keys.len());
    println!("sessions={}", stats.sessions);
    println!("routes_observed={}", stats.routes_observed);
    println!("timeouts={}", stats.timeouts);
    println!("stream_errors={}", stats.stream_errors);
    println!("incomplete_keys_total={}", stats.incomplete_keys_total);
    println!("sessions_per_sec={:.2}", stats.sessions as f64 / elapsed);
    println!(
        "stream_open_p50_us={}",
        percentile(&stream_open_times, 0.50)
    );
    println!(
        "stream_open_p95_us={}",
        percentile(&stream_open_times, 0.95)
    );
    println!(
        "stream_open_p99_us={}",
        percentile(&stream_open_times, 0.99)
    );
    println!(
        "stream_open_max_us={}",
        stream_open_times.last().copied().unwrap_or(0)
    );
    println!(
        "snapshot_transfer_p50_us={}",
        percentile(&snapshot_transfer_times, 0.50)
    );
    println!(
        "snapshot_transfer_p95_us={}",
        percentile(&snapshot_transfer_times, 0.95)
    );
    println!(
        "snapshot_transfer_p99_us={}",
        percentile(&snapshot_transfer_times, 0.99)
    );
    println!(
        "snapshot_transfer_max_us={}",
        snapshot_transfer_times.last().copied().unwrap_or(0)
    );
    println!(
        "snapshot_end_to_end_p50_us={}",
        percentile(&snapshot_end_to_end_times, 0.50)
    );
    println!(
        "snapshot_end_to_end_p95_us={}",
        percentile(&snapshot_end_to_end_times, 0.95)
    );
    println!(
        "snapshot_end_to_end_p99_us={}",
        percentile(&snapshot_end_to_end_times, 0.99)
    );
    println!(
        "snapshot_end_to_end_max_us={}",
        snapshot_end_to_end_times.last().copied().unwrap_or(0)
    );
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let config = load_config()?;
    let seed_channel = connect_channel(config.endpoint.clone()).await?;
    let seeded_keys = if config.seed_timelines {
        ensure_seed_timelines(&config, seed_channel.clone()).await?
    } else {
        Vec::new()
    };

    match config.scenario {
        Scenario::StatusScan | Scenario::StatusScanFiltered => {
            let stats = run_status_scan_bench(&config).await?;
            print_status_summary(&config, stats);
        }
        Scenario::WatchAllSnapshot => {
            let expected_keys =
                Arc::new(fetch_all_status_keys(seed_channel.clone(), config.page_size).await?);
            let stats = run_watch_snapshot_bench(
                &config,
                WatchScenario::All {
                    expected_keys: expected_keys.clone(),
                },
            )
            .await?;
            print_watch_summary(&config, &expected_keys, stats);
        }
        Scenario::WatchFilteredSnapshot => {
            let expected_keys: HashSet<String> = if !config.watch_filter_keys.is_empty() {
                config.watch_filter_keys.iter().cloned().collect()
            } else if !seeded_keys.is_empty() {
                seeded_keys.into_iter().collect()
            } else {
                return Err(
                    "watch_filtered_snapshot requires seeded timelines or CHRONOS_CONTROL_BENCH_WATCH_FILTER_KEYS"
                        .into(),
                );
            };
            let expected_keys = Arc::new(expected_keys);
            let stats = run_watch_snapshot_bench(
                &config,
                WatchScenario::Filtered {
                    expected_keys: expected_keys.clone(),
                },
            )
            .await?;
            print_watch_summary(&config, &expected_keys, stats);
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
            parse_scenario("watch_all_snapshot").unwrap(),
            Scenario::WatchAllSnapshot
        );
        assert_eq!(
            parse_scenario("watch_filtered_snapshot").unwrap(),
            Scenario::WatchFilteredSnapshot
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
    fn snapshot_tracker_completes_only_after_all_expected_routes() {
        let expected = Arc::new(HashSet::from([
            "timeline-a".to_string(),
            "timeline-b".to_string(),
        ]));
        let mut tracker = SnapshotTracker::new(expected);

        assert!(!tracker.on_route("timeline-a"));
        assert!(!tracker.on_route("timeline-a"));
        assert!(tracker.on_route("timeline-b"));
    }

    #[test]
    fn percentile_handles_empty_and_simple_slices() {
        assert_eq!(percentile(&[], 0.95), 0);
        assert_eq!(percentile(&[10, 20, 30], 0.50), 20);
        assert_eq!(percentile(&[10, 20, 30], 0.99), 30);
    }
}
