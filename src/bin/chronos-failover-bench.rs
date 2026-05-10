use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io::Read;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prost::Message;
use tokio::sync::{Barrier, Mutex};
use tokio::time::{sleep, timeout};
use tonic::{transport::Channel, Code, Request, Status};

use chronos::proto::v1::{
    timeline_control_service_client::TimelineControlServiceClient,
    timeline_route_service_client::TimelineRouteServiceClient,
    timestamp_service_client::TimestampServiceClient, AllocateTimestampsRequest,
    EnsureTimelineRequest, ErrorCode, ErrorDetail, GetTimelineRouteRequest, ResourceTier,
    TimelineRoute, TimelineTransferReason, TransferTimelineRequest, WorkerReadinessState,
};

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Debug)]
struct BenchConfig {
    etcd_endpoints: Vec<String>,
    timeline_namespace: String,
    timeline_key: String,
    duration_secs: u64,
    warmup_secs: u64,
    allocate_batch: u32,
    safety_gap_ms: u64,
    failover_poll_interval_ms: u64,
    failover_timeout_secs: u64,
    allocate_request_timeout_ms: u64,
    route_refresh_timeout_ms: u64,
    auto_failover_enabled: bool,
    auto_failover_interval_ms: u64,
    auto_failover_batch_size: usize,
    owner_a: SpawnConfig,
    owner_b: SpawnConfig,
}

#[derive(Clone, Debug)]
struct SpawnConfig {
    instance_id: String,
    worker_id: String,
    bind_addr: SocketAddr,
    advertise_endpoint: String,
    metrics_bind_addr: SocketAddr,
}

#[derive(Default)]
struct FailoverBenchStats {
    allocate_requests_total: u64,
    allocate_success_total: u64,
    allocate_failed_total: u64,
    allocate_tsos_total: u64,
    allocate_latencies_us: Vec<u64>,
    route_refresh_total: u64,
    route_refresh_latencies_us: Vec<u64>,
    failover_attempts_total: u64,
    failover_success_total: u64,
    failover_blocked_total: u64,
    failover_other_failures_total: u64,
    failover_latencies_us: Vec<u64>,
    monotonicity_violations_total: u64,
    first_success_after_kill_ms: Option<u64>,
    error_counts: HashMap<String, u64>,
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

fn parse_endpoints_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn free_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("loopback bind should succeed");
    let addr = listener
        .local_addr()
        .expect("loopback listener should expose local addr");
    drop(listener);
    addr
}

fn local_advertise_endpoint(_alias: &str, bind_addr: SocketAddr) -> String {
    bind_addr.to_string()
}

fn timeline_key(config: &BenchConfig) -> String {
    format!(
        "{}.allocate_during_failover.{}",
        config.timeline_namespace, config.timeline_key
    )
}

fn load_config() -> BenchConfig {
    let duration_secs = env_or("CHRONOS_FAILOVER_BENCH_DURATION_SECS", 10u64);
    let warmup_secs = env_or("CHRONOS_FAILOVER_BENCH_WARMUP_SECS", 2u64);
    let allocate_batch = env_or("CHRONOS_FAILOVER_BENCH_ALLOCATE_BATCH", 1u32);
    let safety_gap_ms = env_or("CHRONOS_FAILOVER_BENCH_SAFETY_GAP_MS", 200u64);
    let failover_poll_interval_ms =
        env_or("CHRONOS_FAILOVER_BENCH_FAILOVER_POLL_INTERVAL_MS", 100u64);
    let failover_timeout_secs = env_or("CHRONOS_FAILOVER_BENCH_FAILOVER_TIMEOUT_SECS", 15u64);
    let allocate_request_timeout_ms = env_or(
        "CHRONOS_FAILOVER_BENCH_ALLOCATE_REQUEST_TIMEOUT_MS",
        2_000u64,
    );
    let route_refresh_timeout_ms =
        env_or("CHRONOS_FAILOVER_BENCH_ROUTE_REFRESH_TIMEOUT_MS", 500u64);
    let auto_failover_enabled = env_or("CHRONOS_FAILOVER_BENCH_AUTO_FAILOVER_ENABLED", false);
    let auto_failover_interval_ms =
        env_or("CHRONOS_FAILOVER_BENCH_AUTO_FAILOVER_INTERVAL_MS", 100u64);
    let auto_failover_batch_size =
        env_or("CHRONOS_FAILOVER_BENCH_AUTO_FAILOVER_BATCH_SIZE", 16usize);
    let timeline_namespace = env_or_string(
        "CHRONOS_FAILOVER_BENCH_NAMESPACE",
        &format!(
            "failoverbench-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time should be monotonic")
                .as_nanos()
        ),
    );
    let timeline_key = env_or_string("CHRONOS_FAILOVER_BENCH_TIMELINE_KEY", "timeline");
    let etcd_endpoints = parse_endpoints_csv(&env_or_string(
        "CHRONOS_TEST_ETCD_ENDPOINTS",
        "127.0.0.1:2379",
    ));

    let bind_a = free_loopback_addr();
    let bind_b = free_loopback_addr();

    BenchConfig {
        etcd_endpoints,
        timeline_namespace,
        timeline_key,
        duration_secs,
        warmup_secs,
        allocate_batch,
        safety_gap_ms,
        failover_poll_interval_ms,
        failover_timeout_secs,
        allocate_request_timeout_ms,
        route_refresh_timeout_ms,
        auto_failover_enabled,
        auto_failover_interval_ms,
        auto_failover_batch_size,
        owner_a: SpawnConfig {
            instance_id: "bench-instance-a".to_string(),
            worker_id: "worker-a".to_string(),
            bind_addr: bind_a,
            advertise_endpoint: local_advertise_endpoint("chronos-failover-a", bind_a),
            metrics_bind_addr: free_loopback_addr(),
        },
        owner_b: SpawnConfig {
            instance_id: "bench-instance-b".to_string(),
            worker_id: "worker-b".to_string(),
            bind_addr: bind_b,
            advertise_endpoint: local_advertise_endpoint("chronos-failover-b", bind_b),
            metrics_bind_addr: free_loopback_addr(),
        },
    }
}

fn chronos_bin() -> AppResult<PathBuf> {
    let mut bin = std::env::current_exe()?;
    bin.pop();
    bin.push("chronos");
    Ok(bin)
}

fn endpoint_uri(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

fn run_make_target(target: &str) -> AppResult<()> {
    let status = Command::new("make")
        .arg(target)
        .env("ETCD_ENDPOINTS", config_endpoints_env())
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("make {target} failed with status {status}").into())
    }
}

fn wait_for_make_target(target: &str, attempts: u64, interval: Duration) -> AppResult<()> {
    for _ in 0..attempts {
        if run_make_target(target).is_ok() {
            return Ok(());
        }
        std::thread::sleep(interval);
    }

    Err(format!("make {target} did not succeed after {attempts} attempts").into())
}

fn config_endpoints_env() -> String {
    env_or_string("CHRONOS_TEST_ETCD_ENDPOINTS", "127.0.0.1:2379")
}

fn spawn_chronos_process(config: &BenchConfig, spawn: &SpawnConfig) -> AppResult<Child> {
    let chronos = chronos_bin()?;
    Ok(Command::new(chronos)
        .env("CHRONOS_METADATA", "etcd")
        .env("CHRONOS_ETCD_ENDPOINTS", config.etcd_endpoints.join(","))
        .env(
            "CHRONOS_ETCD_PREFIX",
            format!("/{}", config.timeline_namespace),
        )
        .env("CHRONOS_SECURITY_MODE", "dev-insecure")
        .env("CHRONOS_SAFETY_GAP_MS", config.safety_gap_ms.to_string())
        .env("CHRONOS_WORKER_ID", &spawn.worker_id)
        .env("CHRONOS_INSTANCE_ID", &spawn.instance_id)
        .env("CHRONOS_BIND_ADDR", spawn.bind_addr.to_string())
        .env("CHRONOS_ADVERTISE_ENDPOINT", &spawn.advertise_endpoint)
        .env(
            "CHRONOS_AUTO_FAILOVER_ENABLED",
            config.auto_failover_enabled.to_string(),
        )
        .env(
            "CHRONOS_AUTO_FAILOVER_INTERVAL_MS",
            config.auto_failover_interval_ms.to_string(),
        )
        .env(
            "CHRONOS_AUTO_FAILOVER_BATCH_SIZE",
            config.auto_failover_batch_size.to_string(),
        )
        .env(
            "CHRONOS_METRICS_BIND_ADDR",
            spawn.metrics_bind_addr.to_string(),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?)
}

fn read_child_stderr(child: &mut Child) -> String {
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        pipe.read_to_string(&mut stderr)
            .expect("child stderr should read");
    }
    stderr
}

fn terminate_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn owner_endpoint_addr(config: &BenchConfig, owner_endpoint: &str) -> AppResult<SocketAddr> {
    if owner_endpoint == config.owner_a.advertise_endpoint {
        return Ok(config.owner_a.bind_addr);
    }
    if owner_endpoint == config.owner_b.advertise_endpoint {
        return Ok(config.owner_b.bind_addr);
    }
    Ok(owner_endpoint.parse::<SocketAddr>()?)
}

async fn wait_for_post_failover_allocation_success(
    config: &BenchConfig,
    route: &TimelineRoute,
    allocate_batch: u32,
    request_timeout_ms: u64,
    timeout_secs: u64,
    kill_instant: Arc<Mutex<Option<Instant>>>,
    first_success_after_kill: Arc<Mutex<Option<u64>>>,
) -> AppResult<()> {
    timeout(Duration::from_secs(timeout_secs), async {
        loop {
            match allocate_timestamps_raw(
                owner_endpoint_addr(config, &route.owner_worker_endpoint)?,
                route,
                "failover-post-success-probe",
                allocate_batch,
                request_timeout_ms,
            )
            .await
            {
                Ok(response) if !response.ranges.is_empty() => {
                    if let Some(kill_at) = *kill_instant.lock().await {
                        let mut first = first_success_after_kill.lock().await;
                        if first.is_none() {
                            *first = Some(kill_at.elapsed().as_millis() as u64);
                        }
                    }
                    return Ok::<(), Box<dyn Error + Send + Sync>>(());
                }
                Ok(_) | Err(_) => sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await??;
    Ok(())
}

async fn wait_for_ready(endpoint: SocketAddr, child: &mut Child) -> AppResult<()> {
    timeout(Duration::from_secs(15), async {
        loop {
            if let Some(status) = child.try_wait()? {
                let stderr = read_child_stderr(child);
                return Err(format!(
                    "chronos process exited before readiness: status={status} stderr={stderr}"
                )
                .into());
            }

            if let Ok(mut client) =
                TimelineControlServiceClient::connect(endpoint_uri(endpoint)).await
            {
                if let Ok(response) = client.health(()).await {
                    if response.into_inner().readiness_state == WorkerReadinessState::Ready as i32 {
                        return Ok(());
                    }
                }
            }

            sleep(Duration::from_millis(100)).await;
        }
    })
    .await?
}

async fn wait_for_auto_failover_route(
    standby_endpoint: SocketAddr,
    timeline_key: &str,
    target_owner_endpoint: &str,
    poll_interval_ms: u64,
    timeout_secs: u64,
    failover_started: Instant,
) -> Result<(TimelineRoute, FailoverBenchStats), Box<dyn Error + Send + Sync>> {
    let mut stats = FailoverBenchStats::default();
    timeout(Duration::from_secs(timeout_secs), async {
        loop {
            stats.failover_attempts_total += 1;
            match get_timeline_route(standby_endpoint, timeline_key).await {
                Ok(route) if route.owner_worker_endpoint == target_owner_endpoint => {
                    stats.failover_success_total += 1;
                    stats
                        .failover_latencies_us
                        .push(failover_started.elapsed().as_micros() as u64);
                    eprintln!(
                        "phase=auto_failover_observed timeline_key={} route_version={} epoch={}",
                        route.timeline_key, route.route_version, route.epoch
                    );
                    return Ok((route, stats));
                }
                Ok(_) => {
                    stats.failover_blocked_total += 1;
                }
                Err(_) => {
                    stats.failover_other_failures_total += 1;
                }
            }
            sleep(Duration::from_millis(poll_interval_ms)).await;
        }
    })
    .await?
}

async fn route_client(endpoint: SocketAddr) -> AppResult<TimelineRouteServiceClient<Channel>> {
    Ok(TimelineRouteServiceClient::connect(endpoint_uri(endpoint)).await?)
}

async fn control_client(endpoint: SocketAddr) -> AppResult<TimelineControlServiceClient<Channel>> {
    Ok(TimelineControlServiceClient::connect(endpoint_uri(endpoint)).await?)
}

async fn timestamp_client(endpoint: SocketAddr) -> AppResult<TimestampServiceClient<Channel>> {
    Ok(TimestampServiceClient::connect(endpoint_uri(endpoint)).await?)
}

async fn ensure_timeline(endpoint: SocketAddr, timeline_key: &str) -> AppResult<TimelineRoute> {
    Ok(route_client(endpoint)
        .await?
        .ensure_timeline(Request::new(EnsureTimelineRequest {
            timeline_key: timeline_key.to_string(),
            desired_resource_tier: ResourceTier::Shared as i32,
        }))
        .await?
        .into_inner()
        .route
        .ok_or("ensure_timeline should return route")?)
}

async fn get_timeline_route(endpoint: SocketAddr, timeline_key: &str) -> AppResult<TimelineRoute> {
    get_timeline_route_with_timeout(endpoint, timeline_key, Duration::from_secs(2)).await
}

async fn get_timeline_route_with_timeout(
    endpoint: SocketAddr,
    timeline_key: &str,
    timeout_duration: Duration,
) -> AppResult<TimelineRoute> {
    Ok(timeout(timeout_duration, async {
        route_client(endpoint)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?
            .get_timeline_route(Request::new(GetTimelineRouteRequest {
                timeline_key: timeline_key.to_string(),
            }))
            .await
    })
    .await??
    .into_inner()
    .route
    .ok_or("get_timeline_route should return route")?)
}

async fn allocate_timestamps_raw(
    endpoint: SocketAddr,
    route: &TimelineRoute,
    client_request_id: &str,
    count: u32,
    request_timeout_ms: u64,
) -> Result<chronos::proto::v1::AllocateTimestampsResponse, tonic::Status> {
    let mut client = timestamp_client(endpoint)
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let response = client
        .allocate_timestamps(Request::new(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: client_request_id.to_string(),
            request_timeout_ms: request_timeout_ms as u32,
        }))
        .await?;
    Ok(response.into_inner())
}

async fn failover_timeline(
    endpoint: SocketAddr,
    timeline_key: &str,
    target_worker_endpoint: &str,
) -> Result<chronos::proto::v1::TransferTimelineResponse, tonic::Status> {
    let mut client = control_client(endpoint)
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let response = client
        .transfer_timeline(Request::new(TransferTimelineRequest {
            timeline_key: timeline_key.to_string(),
            target_generator_id: None,
            target_worker_id: Some(target_worker_endpoint.to_string()),
            reason: TimelineTransferReason::Failover as i32,
        }))
        .await?;
    Ok(response.into_inner())
}

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * pct).round() as usize;
    sorted[idx]
}

fn decode_error_detail(status: &Status) -> Option<ErrorDetail> {
    ErrorDetail::decode(status.details()).ok()
}

fn error_label(status: &Status) -> &'static str {
    match decode_error_detail(status).and_then(|detail| ErrorCode::try_from(detail.code).ok()) {
        Some(ErrorCode::LeaseExpired) => "lease_expired",
        Some(ErrorCode::TemporarilyUnavailable) => "temporarily_unavailable",
        Some(ErrorCode::NotTimelineOwner) => "not_timeline_owner",
        Some(ErrorCode::RouteVersionMismatch) => "route_version_mismatch",
        Some(ErrorCode::EpochMismatch) => "epoch_mismatch",
        _ => match status.code() {
            Code::Unavailable => "transport_unavailable",
            Code::FailedPrecondition => "failed_precondition",
            Code::Aborted => "aborted",
            _ => "other",
        },
    }
}

fn count_tsos(response: &chronos::proto::v1::AllocateTimestampsResponse) -> u64 {
    response
        .ranges
        .iter()
        .map(|range| range.end_tso - range.start_tso + 1)
        .sum()
}

fn last_tso(response: &chronos::proto::v1::AllocateTimestampsResponse) -> Option<u64> {
    response.ranges.last().map(|range| range.end_tso)
}

fn select_leader_and_standby(
    initial_owner: SocketAddr,
    owner_a: SocketAddr,
    owner_b: SocketAddr,
) -> (SocketAddr, SocketAddr) {
    if initial_owner == owner_a {
        (owner_a, owner_b)
    } else {
        (owner_b, owner_a)
    }
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let config = load_config();
    let timeline_key = timeline_key(&config);
    run_make_target("etcd-reset")?;
    run_make_target("etcd-up")?;
    wait_for_make_target("etcd-health", 60, Duration::from_secs(1))?;

    let mut owner_a = spawn_chronos_process(&config, &config.owner_a)?;
    let mut owner_b = spawn_chronos_process(&config, &config.owner_b)?;

    let result: AppResult<()> = async {
        wait_for_ready(config.owner_a.bind_addr, &mut owner_a).await?;
        eprintln!("phase=owner_a_ready endpoint={}", config.owner_a.bind_addr);
        wait_for_ready(config.owner_b.bind_addr, &mut owner_b).await?;
        eprintln!("phase=owner_b_ready endpoint={}", config.owner_b.bind_addr);

        let owner_a = Arc::new(Mutex::new(owner_a));
        let owner_b = Arc::new(Mutex::new(owner_b));

        let initial_route = ensure_timeline(config.owner_a.bind_addr, &timeline_key).await?;
        eprintln!(
            "phase=timeline_ensured timeline_key={} owner={}",
            timeline_key, initial_route.owner_worker_endpoint
        );
        let route = Arc::new(Mutex::new(initial_route.clone()));
        let (leader_endpoint, standby_endpoint) = select_leader_and_standby(
            owner_endpoint_addr(&config, &initial_route.owner_worker_endpoint)?,
            config.owner_a.bind_addr,
            config.owner_b.bind_addr,
        );
        let leader_child = if leader_endpoint == config.owner_a.bind_addr {
            owner_a.clone()
        } else {
            owner_b.clone()
        };
        let standby_child = if leader_endpoint == config.owner_a.bind_addr {
            owner_b.clone()
        } else {
            owner_a.clone()
        };
        let barrier = Arc::new(Barrier::new(2));
        let kill_instant = Arc::new(Mutex::new(None::<Instant>));
        let first_success_after_kill = Arc::new(Mutex::new(None::<u64>));

        let allocator_route = route.clone();
        let allocator_barrier = barrier.clone();
        let allocator_kill_instant = kill_instant.clone();
        let allocator_first_success = first_success_after_kill.clone();
        let timeline_key_for_alloc = timeline_key.clone();
        let allocate_batch = config.allocate_batch;
        let duration_secs = config.duration_secs;
        let warmup_secs = config.warmup_secs;
        let allocate_request_timeout_ms = config.allocate_request_timeout_ms;
        let route_refresh_timeout_ms = config.route_refresh_timeout_ms;
        let owner_a_endpoint = config.owner_a.bind_addr;
        let owner_b_endpoint = config.owner_b.bind_addr;
        let allocator_config = config.clone();

        let allocator_handle = tokio::spawn(async move {
            let warmup_until = Instant::now() + Duration::from_secs(warmup_secs);
            let measure_until = warmup_until + Duration::from_secs(duration_secs);
            let mut stats = FailoverBenchStats::default();
            let mut last_success_tso = None;
            let mut ordinal = 0u64;

            allocator_barrier.wait().await;
            loop {
                let request_start = Instant::now();
                if request_start >= measure_until {
                    break;
                }

                ordinal = ordinal.saturating_add(1);
                let route_snapshot = allocator_route.lock().await.clone();
                let owner_endpoint =
                    owner_endpoint_addr(&allocator_config, &route_snapshot.owner_worker_endpoint)?;
                let allocate_result = allocate_timestamps_raw(
                    owner_endpoint,
                    &route_snapshot,
                    &format!("failover-bench-{ordinal}"),
                    allocate_batch,
                    allocate_request_timeout_ms,
                )
                .await;
                let elapsed = request_start.elapsed().as_micros() as u64;

                if request_start >= warmup_until {
                    stats.allocate_requests_total += 1;
                    stats.allocate_latencies_us.push(elapsed);
                }

                match allocate_result {
                    Ok(response) => {
                        if request_start >= warmup_until {
                            stats.allocate_success_total += 1;
                            stats.allocate_tsos_total += count_tsos(&response);
                            if let Some(last_tso) = last_tso(&response) {
                                if let Some(previous) = last_success_tso {
                                    if last_tso <= previous {
                                        stats.monotonicity_violations_total += 1;
                                    }
                                }
                                last_success_tso = Some(last_tso);
                            }
                            if let Some(kill_at) = *allocator_kill_instant.lock().await {
                                let mut first = allocator_first_success.lock().await;
                                if first.is_none() {
                                    *first = Some(kill_at.elapsed().as_millis() as u64);
                                }
                            }
                        }
                    }
                    Err(status) => {
                        if request_start >= warmup_until {
                            stats.allocate_failed_total += 1;
                            *stats
                                .error_counts
                                .entry(error_label(&status).to_string())
                                .or_insert(0) += 1;
                        }
                        let refresh_start = Instant::now();
                        if let Ok(refreshed) = get_timeline_route_with_timeout(
                            owner_b_endpoint,
                            &timeline_key_for_alloc,
                            Duration::from_millis(route_refresh_timeout_ms.max(100)),
                        )
                        .await
                        {
                            *allocator_route.lock().await = refreshed;
                            if request_start >= warmup_until {
                                stats.route_refresh_total += 1;
                                stats
                                    .route_refresh_latencies_us
                                    .push(refresh_start.elapsed().as_micros() as u64);
                            }
                        } else if let Ok(refreshed) = get_timeline_route_with_timeout(
                            owner_a_endpoint,
                            &timeline_key_for_alloc,
                            Duration::from_millis(route_refresh_timeout_ms.max(100)),
                        )
                        .await
                        {
                            *allocator_route.lock().await = refreshed;
                            if request_start >= warmup_until {
                                stats.route_refresh_total += 1;
                                stats
                                    .route_refresh_latencies_us
                                    .push(refresh_start.elapsed().as_micros() as u64);
                            }
                        }
                    }
                }
            }

            Ok::<FailoverBenchStats, Box<dyn Error + Send + Sync>>(stats)
        });

        let driver_barrier = barrier.clone();
        let timeline_key_for_failover = timeline_key.clone();
        let owner_endpoint_str = if standby_endpoint == config.owner_a.bind_addr {
            config.owner_a.advertise_endpoint.clone()
        } else {
            config.owner_b.advertise_endpoint.clone()
        };
        let failover_timeout_secs = config.failover_timeout_secs;
        let failover_poll_interval_ms = config.failover_poll_interval_ms;
        let driver_allocate_batch = config.allocate_batch;
        let driver_request_timeout_ms = config.allocate_request_timeout_ms;
        let driver_kill_instant = kill_instant.clone();
        let driver_first_success_after_kill = first_success_after_kill.clone();
        let driver_leader_child = leader_child.clone();
        let driver_config = config.clone();
        let driver_stats_handle = tokio::spawn(async move {
            let mut stats = FailoverBenchStats::default();
            driver_barrier.wait().await;
            sleep(Duration::from_secs(warmup_secs)).await;
            *kill_instant.lock().await = Some(Instant::now());
            terminate_child(&mut *driver_leader_child.lock().await);
            eprintln!("phase=leader_killed endpoint={}", leader_endpoint);

            if driver_config.auto_failover_enabled {
                let (refreshed, observed_stats) = wait_for_auto_failover_route(
                    standby_endpoint,
                    &timeline_key_for_failover,
                    &owner_endpoint_str,
                    failover_poll_interval_ms,
                    failover_timeout_secs,
                    driver_kill_instant
                        .lock()
                        .await
                        .expect("kill instant must be set before auto failover polling"),
                )
                .await?;
                *route.lock().await = refreshed;
                wait_for_post_failover_allocation_success(
                    &driver_config,
                    &route.lock().await.clone(),
                    driver_allocate_batch,
                    driver_request_timeout_ms,
                    failover_timeout_secs,
                    driver_kill_instant.clone(),
                    driver_first_success_after_kill.clone(),
                )
                .await?;
                return Ok::<FailoverBenchStats, Box<dyn Error + Send + Sync>>(observed_stats);
            }

            timeout(Duration::from_secs(failover_timeout_secs), async {
                loop {
                    let transfer_start = Instant::now();
                    stats.failover_attempts_total += 1;
                    match failover_timeline(
                        standby_endpoint,
                        &timeline_key_for_failover,
                        &owner_endpoint_str,
                    )
                    .await
                    {
                        Ok(response) => {
                            stats.failover_success_total += 1;
                            stats
                                .failover_latencies_us
                                .push(transfer_start.elapsed().as_micros() as u64);
                            eprintln!(
                                "phase=failover_succeeded timeline_key={} route_version={} epoch={}",
                                response.timeline_key, response.route_version, response.new_epoch
                            );
                            let refreshed = get_timeline_route(standby_endpoint, &response.timeline_key)
                                .await
                                .map_err(|error| {
                                    Box::<dyn Error + Send + Sync>::from(error.to_string())
                                })?;
                            *route.lock().await = refreshed;
                            wait_for_post_failover_allocation_success(
                                &driver_config,
                                &route.lock().await.clone(),
                                driver_allocate_batch,
                                driver_request_timeout_ms,
                                failover_timeout_secs,
                                driver_kill_instant.clone(),
                                driver_first_success_after_kill.clone(),
                            )
                            .await?;
                            return Ok::<FailoverBenchStats, Box<dyn Error + Send + Sync>>(stats);
                        }
                        Err(status) if status.code() == Code::FailedPrecondition => {
                            stats.failover_blocked_total += 1;
                            sleep(Duration::from_millis(failover_poll_interval_ms)).await;
                        }
                        Err(_) => {
                            stats.failover_other_failures_total += 1;
                            sleep(Duration::from_millis(failover_poll_interval_ms)).await;
                        }
                    }
                }
            })
            .await?
        });

        let mut stats = allocator_handle.await??;
        let driver_stats = driver_stats_handle.await??;
        stats.failover_attempts_total = driver_stats.failover_attempts_total;
        stats.failover_success_total = driver_stats.failover_success_total;
        stats.failover_blocked_total = driver_stats.failover_blocked_total;
        stats.failover_other_failures_total = driver_stats.failover_other_failures_total;
        stats.failover_latencies_us = driver_stats.failover_latencies_us;
        stats.first_success_after_kill_ms = *first_success_after_kill.lock().await;

        terminate_child(&mut *leader_child.lock().await);
        terminate_child(&mut *standby_child.lock().await);

        let elapsed = duration_secs as f64;
        let mut allocate_latencies = stats.allocate_latencies_us;
        let mut refresh_latencies = stats.route_refresh_latencies_us;
        let mut failover_latencies = stats.failover_latencies_us;
        allocate_latencies.sort_unstable();
        refresh_latencies.sort_unstable();
        failover_latencies.sort_unstable();

        println!("scenario=allocate_during_failover");
        println!("endpoint_a={}", config.owner_a.bind_addr);
        println!("endpoint_b={}", config.owner_b.bind_addr);
        println!("timeline_namespace={}", config.timeline_namespace);
        println!("timeline_key={}", timeline_key);
        println!("duration_secs={}", config.duration_secs);
        println!("warmup_secs={}", config.warmup_secs);
        println!("allocate_batch={}", config.allocate_batch);
        println!(
            "allocate_request_timeout_ms={}",
            config.allocate_request_timeout_ms
        );
        println!(
            "route_refresh_timeout_ms={}",
            config.route_refresh_timeout_ms
        );
        println!("safety_gap_ms={}", config.safety_gap_ms);
        println!("auto_failover_enabled={}", config.auto_failover_enabled);
        println!(
            "auto_failover_interval_ms={}",
            config.auto_failover_interval_ms
        );
        println!(
            "auto_failover_batch_size={}",
            config.auto_failover_batch_size
        );
        println!("allocate_requests_total={}", stats.allocate_requests_total);
        println!("allocate_success_total={}", stats.allocate_success_total);
        println!("allocate_failed_total={}", stats.allocate_failed_total);
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
        println!("failover_attempts_total={}", stats.failover_attempts_total);
        println!("failover_success_total={}", stats.failover_success_total);
        println!("failover_blocked_total={}", stats.failover_blocked_total);
        println!(
            "failover_other_failures_total={}",
            stats.failover_other_failures_total
        );
        println!(
            "failover_latency_p50_us={}",
            percentile(&failover_latencies, 0.50)
        );
        println!(
            "failover_latency_p95_us={}",
            percentile(&failover_latencies, 0.95)
        );
        println!(
            "failover_latency_p99_us={}",
            percentile(&failover_latencies, 0.99)
        );
        println!(
            "failover_latency_p999_us={}",
            percentile(&failover_latencies, 0.999)
        );
        println!(
            "first_success_after_kill_ms={}",
            stats.first_success_after_kill_ms.unwrap_or(0)
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

        Ok(())
    }
    .await;

    let _ = run_make_target("etcd-reset");
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronos::proto::v1::{OperatorActionBlocker, OperatorActionNextStep};

    #[test]
    fn parse_endpoints_csv_skips_empty_entries() {
        assert_eq!(
            parse_endpoints_csv("127.0.0.1:2379, ,127.0.0.1:2380"),
            vec!["127.0.0.1:2379", "127.0.0.1:2380"]
        );
    }

    #[test]
    fn select_leader_and_standby_tracks_actual_owner() {
        let owner_a: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let owner_b: SocketAddr = "127.0.0.1:50052".parse().unwrap();

        assert_eq!(
            select_leader_and_standby(owner_a, owner_a, owner_b),
            (owner_a, owner_b)
        );
        assert_eq!(
            select_leader_and_standby(owner_b, owner_a, owner_b),
            (owner_b, owner_a)
        );
    }

    #[test]
    fn error_label_maps_known_error_codes() {
        let detail = ErrorDetail {
            code: ErrorCode::LeaseExpired as i32,
            message: "lease expired".to_string(),
            current_epoch: 0,
            current_route_version: 0,
            redirect_endpoint: String::new(),
            action_blocker: OperatorActionBlocker::Unspecified as i32,
            next_step: OperatorActionNextStep::Unspecified as i32,
        };
        let status = Status::with_details(
            Code::FailedPrecondition,
            "lease expired",
            detail.encode_to_vec().into(),
        );
        assert_eq!(error_label(&status), "lease_expired");
    }

    #[test]
    fn owner_endpoint_addr_maps_localhost_subdomains_to_child_bind_addresses() {
        let owner_a_bind: SocketAddr = "127.0.0.1:50051".parse().unwrap();
        let owner_b_bind: SocketAddr = "127.0.0.1:50052".parse().unwrap();
        let config = BenchConfig {
            etcd_endpoints: vec!["127.0.0.1:2379".to_string()],
            timeline_namespace: "ns".to_string(),
            timeline_key: "key".to_string(),
            duration_secs: 1,
            warmup_secs: 0,
            allocate_batch: 1,
            safety_gap_ms: 1,
            failover_poll_interval_ms: 100,
            failover_timeout_secs: 1,
            allocate_request_timeout_ms: 100,
            route_refresh_timeout_ms: 100,
            auto_failover_enabled: false,
            auto_failover_interval_ms: 100,
            auto_failover_batch_size: 16,
            owner_a: SpawnConfig {
                instance_id: "a".to_string(),
                worker_id: "worker-a".to_string(),
                bind_addr: owner_a_bind,
                advertise_endpoint: "chronos-failover-a.localhost:50051".to_string(),
                metrics_bind_addr: "127.0.0.1:9898".parse().unwrap(),
            },
            owner_b: SpawnConfig {
                instance_id: "b".to_string(),
                worker_id: "worker-b".to_string(),
                bind_addr: owner_b_bind,
                advertise_endpoint: "chronos-failover-b.localhost:50052".to_string(),
                metrics_bind_addr: "127.0.0.1:9899".parse().unwrap(),
            },
        };

        assert_eq!(
            owner_endpoint_addr(&config, "chronos-failover-a.localhost:50051").unwrap(),
            owner_a_bind
        );
        assert_eq!(
            owner_endpoint_addr(&config, "chronos-failover-b.localhost:50052").unwrap(),
            owner_b_bind
        );
    }
}
