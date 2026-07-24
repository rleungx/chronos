use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, timeout};
use tonic::transport::Channel;
use tonic::Request;

use chronos::proto::v1::{
    timeline_control_service_client::TimelineControlServiceClient,
    timeline_route_service_client::TimelineRouteServiceClient,
    timestamp_service_client::TimestampServiceClient, AllocateTimestampsRequest,
    EnsureTimelineRequest, GetTimelineRouteRequest, HealthResponse, ResourceTier,
    TimelineTransferReason, TransferTimelineRequest,
};

#[path = "support/env.rs"]
mod support_env;

use support_env::{env_or, env_or_string};

type AppResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const PHASES: [&str; 7] = [
    "old_only",
    "replace_0",
    "mixed_1",
    "replace_1",
    "mixed_2",
    "replace_2",
    "new_only",
];

#[derive(Clone, Debug)]
struct Config {
    endpoints: Vec<String>,
    command_file: PathBuf,
    ack_file: PathBuf,
    timeline_key: String,
    request_timeout_ms: u64,
    poll_interval_ms: u64,
    max_runtime_secs: u64,
}

#[derive(Clone, Debug)]
struct Route {
    timeline_key: String,
    epoch: u64,
    route_version: u64,
    owner_worker_endpoint: String,
}

#[derive(Clone, Debug)]
struct PhaseCommand {
    sequence: u64,
    phase: String,
    target_endpoint: Option<String>,
}

#[derive(Debug)]
struct PendingAck {
    command: PhaseCommand,
    health: Option<HealthResponse>,
}

#[derive(Debug, Default)]
struct PhaseStats {
    requests: u64,
    success: u64,
    failed: u64,
    first_tso: Option<u64>,
    last_tso: Option<u64>,
    max_success_gap_ms: u64,
    last_success_at: Option<Instant>,
}

#[derive(Clone, Debug)]
struct RoutePhase {
    route: Route,
    phase: Option<String>,
}

#[derive(Clone, Debug)]
struct ServingObservation {
    ordinal: u64,
    phase: String,
    owner_worker_endpoint: String,
    last_tso: u64,
}

struct BenchStats {
    stages: BTreeMap<String, PhaseStats>,
    ordinal: u64,
    last_tso: Option<u64>,
    monotonicity_violations: u64,
    last_success_at: Option<Instant>,
    global_max_success_gap_ms: u64,
    latest_serving: Option<ServingObservation>,
}

impl Default for BenchStats {
    fn default() -> Self {
        Self {
            stages: PHASES
                .iter()
                .map(|phase| ((*phase).to_owned(), PhaseStats::default()))
                .collect(),
            ordinal: 0,
            last_tso: None,
            monotonicity_violations: 0,
            last_success_at: None,
            global_max_success_gap_ms: 0,
            latest_serving: None,
        }
    }
}

fn parse_endpoints(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn endpoint_uri(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or_default()
}

fn load_config() -> AppResult<Config> {
    let endpoints = parse_endpoints(&env_or_string(
        "CHRONOS_UPGRADE_BENCH_ENDPOINTS",
        "127.0.0.1:52051,127.0.0.1:52052,127.0.0.1:52053",
    ));
    if endpoints.len() != 3 {
        return Err(format!(
            "CHRONOS_UPGRADE_BENCH_ENDPOINTS must contain exactly 3 endpoints, got {}",
            endpoints.len()
        )
        .into());
    }
    Ok(Config {
        endpoints,
        command_file: PathBuf::from(env_or_string(
            "CHRONOS_UPGRADE_BENCH_COMMAND_FILE",
            "artifacts/rolling-upgrade/command.txt",
        )),
        ack_file: PathBuf::from(env_or_string(
            "CHRONOS_UPGRADE_BENCH_ACK_FILE",
            "artifacts/rolling-upgrade/ack.txt",
        )),
        timeline_key: env_or_string(
            "CHRONOS_UPGRADE_BENCH_TIMELINE_KEY",
            "rolling-upgrade.timeline",
        ),
        request_timeout_ms: env_or("CHRONOS_UPGRADE_BENCH_REQUEST_TIMEOUT_MS", 500u64),
        poll_interval_ms: env_or("CHRONOS_UPGRADE_BENCH_POLL_INTERVAL_MS", 25u64),
        max_runtime_secs: env_or("CHRONOS_UPGRADE_BENCH_MAX_RUNTIME_SECS", 180u64),
    })
}

fn parse_command(path: &Path) -> AppResult<Option<PhaseCommand>> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut fields = content.trim().splitn(3, '|');
    let sequence = fields
        .next()
        .ok_or("upgrade command is missing sequence")?
        .parse::<u64>()?;
    let phase = fields
        .next()
        .ok_or("upgrade command is missing phase")?
        .to_owned();
    let target = fields
        .next()
        .ok_or("upgrade command is missing target endpoint")?;
    if phase != "stop" && !PHASES.contains(&phase.as_str()) {
        return Err(format!("unsupported upgrade phase: {phase}").into());
    }
    Ok(Some(PhaseCommand {
        sequence,
        phase,
        target_endpoint: (target != "-").then(|| target.to_owned()),
    }))
}

fn write_ack(path: &Path, pending: &PendingAck, route: &Route, serving_tso: u64) -> AppResult<()> {
    let temp = path.with_extension("tmp");
    let (instance_id, worker_id, build_version, build_commit) = pending
        .health
        .as_ref()
        .map(|health| {
            (
                health.instance_id.as_str(),
                health.worker_id.as_str(),
                health.build_version.as_str(),
                health.build_commit.as_str(),
            )
        })
        .unwrap_or(("", "", "", ""));
    fs::write(
        &temp,
        format!(
            "sequence={}\nstatus=serving\nphase={}\ntarget_endpoint={}\n\
             observed_owner_endpoint={}\ninstance_id={}\nworker_id={}\n\
             build_version={}\nbuild_commit={}\nserving_tso={}\nacknowledged_at_ms={}\n",
            pending.command.sequence,
            pending.command.phase,
            pending.command.target_endpoint.as_deref().unwrap_or(""),
            route.owner_worker_endpoint,
            instance_id,
            worker_id,
            build_version,
            build_commit,
            serving_tso,
            now_ms(),
        ),
    )?;
    fs::rename(temp, path)?;
    Ok(())
}

async fn connect_channel(endpoint: &str) -> AppResult<Channel> {
    Ok(timeout(
        Duration::from_secs(2),
        Channel::from_shared(endpoint_uri(endpoint))?.connect(),
    )
    .await??)
}

async fn health(endpoint: &str) -> AppResult<HealthResponse> {
    let mut client = TimelineControlServiceClient::new(connect_channel(endpoint).await?);
    Ok(
        timeout(Duration::from_secs(2), client.health(Request::new(())))
            .await??
            .into_inner(),
    )
}

async fn ensure_timeline(endpoint: &str, timeline_key: &str) -> AppResult<Route> {
    let mut client = TimelineRouteServiceClient::new(connect_channel(endpoint).await?);
    let route = timeout(
        Duration::from_secs(5),
        client.ensure_timeline(Request::new(EnsureTimelineRequest {
            timeline_key: timeline_key.to_owned(),
            desired_resource_tier: ResourceTier::Shared as i32,
        })),
    )
    .await??
    .into_inner()
    .route
    .ok_or("ensure timeline returned no route")?;
    Ok(Route {
        timeline_key: route.timeline_key,
        epoch: route.epoch,
        route_version: route.route_version,
        owner_worker_endpoint: route.owner_worker_endpoint,
    })
}

async fn refresh_route(endpoints: &[String], timeline_key: &str) -> AppResult<Route> {
    let mut last_error = None;
    for endpoint in endpoints {
        let result: AppResult<Route> = async {
            let mut client = TimelineRouteServiceClient::new(connect_channel(endpoint).await?);
            let route = timeout(
                Duration::from_secs(2),
                client.get_timeline_route(Request::new(GetTimelineRouteRequest {
                    timeline_key: timeline_key.to_owned(),
                })),
            )
            .await??
            .into_inner()
            .route
            .ok_or("get timeline route returned no route")?;
            Ok(Route {
                timeline_key: route.timeline_key,
                epoch: route.epoch,
                route_version: route.route_version,
                owner_worker_endpoint: route.owner_worker_endpoint,
            })
        }
        .await;
        match result {
            Ok(route) => return Ok(route),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| "no control endpoints configured".into()))
}

async fn try_transfer_to(config: &Config, route: &Route, target_endpoint: &str) -> Route {
    let attempt: AppResult<()> = async {
        let mut client = TimelineControlServiceClient::new(connect_channel(target_endpoint).await?);
        timeout(
            Duration::from_millis(500),
            client.transfer_timeline(Request::new(TransferTimelineRequest {
                timeline_key: route.timeline_key.clone(),
                target_generator_id: None,
                target_worker_id: Some(target_endpoint.to_owned()),
                reason: TimelineTransferReason::Manual as i32,
            })),
        )
        .await??;
        Ok(())
    }
    .await;
    let _ = attempt;
    refresh_route(&config.endpoints, &route.timeline_key)
        .await
        .unwrap_or_else(|_| route.clone())
}

async fn allocate(
    clients: &mut BTreeMap<String, TimestampServiceClient<Channel>>,
    route: &Route,
    ordinal: u64,
    request_timeout_ms: u64,
) -> AppResult<chronos::proto::v1::AllocateTimestampsResponse> {
    if !clients.contains_key(&route.owner_worker_endpoint) {
        clients.insert(
            route.owner_worker_endpoint.clone(),
            TimestampServiceClient::new(connect_channel(&route.owner_worker_endpoint).await?),
        );
    }
    let client = clients
        .get_mut(&route.owner_worker_endpoint)
        .ok_or("timestamp client was not initialized")?;
    Ok(timeout(
        Duration::from_millis(request_timeout_ms.max(1) + 250),
        client.allocate_timestamps(Request::new(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: format!("rolling-upgrade-{ordinal}"),
            request_timeout_ms: request_timeout_ms.min(u32::MAX as u64) as u32,
        })),
    )
    .await??
    .into_inner())
}

#[async_trait]
trait AllocationStep: Send {
    async fn step(&mut self) -> AppResult<()>;
}

struct NetworkAllocator {
    config: Config,
    route_phase: Arc<RwLock<RoutePhase>>,
    stats: Arc<Mutex<BenchStats>>,
    clients: BTreeMap<String, TimestampServiceClient<Channel>>,
}

#[async_trait]
impl AllocationStep for NetworkAllocator {
    async fn step(&mut self) -> AppResult<()> {
        let snapshot = self.route_phase.read().await.clone();
        let Some(phase) = snapshot.phase else {
            sleep(Duration::from_millis(self.config.poll_interval_ms)).await;
            return Ok(());
        };
        let ordinal = {
            let mut stats = self.stats.lock().await;
            stats.ordinal = stats.ordinal.saturating_add(1);
            let stage = stats
                .stages
                .get_mut(&phase)
                .ok_or_else(|| format!("missing stats for phase {phase}"))?;
            stage.requests = stage.requests.saturating_add(1);
            stats.ordinal
        };
        match allocate(
            &mut self.clients,
            &snapshot.route,
            ordinal,
            self.config.request_timeout_ms,
        )
        .await
        {
            Ok(response) => {
                let first = response
                    .ranges
                    .first()
                    .ok_or("allocation response has no ranges")?
                    .start_tso;
                let last = response
                    .ranges
                    .last()
                    .ok_or("allocation response has no ranges")?
                    .end_tso;
                let success_at = Instant::now();
                {
                    let mut stats = self.stats.lock().await;
                    if first > last || stats.last_tso.is_some_and(|previous| first <= previous) {
                        stats.monotonicity_violations =
                            stats.monotonicity_violations.saturating_add(1);
                    }
                    stats.last_tso =
                        Some(stats.last_tso.map_or(last, |previous| previous.max(last)));
                    if let Some(previous) = stats.last_success_at {
                        stats.global_max_success_gap_ms = stats
                            .global_max_success_gap_ms
                            .max(success_at.duration_since(previous).as_millis() as u64);
                    }
                    stats.last_success_at = Some(success_at);
                    stats.latest_serving = Some(ServingObservation {
                        ordinal,
                        phase: phase.clone(),
                        owner_worker_endpoint: snapshot.route.owner_worker_endpoint.clone(),
                        last_tso: last,
                    });
                    let stage = stats
                        .stages
                        .get_mut(&phase)
                        .ok_or_else(|| format!("missing stats for phase {phase}"))?;
                    stage.success = stage.success.saturating_add(1);
                    stage.first_tso.get_or_insert(first);
                    stage.last_tso = Some(stage.last_tso.map_or(last, |value| value.max(last)));
                    if let Some(previous) = stage.last_success_at {
                        stage.max_success_gap_ms = stage
                            .max_success_gap_ms
                            .max(success_at.duration_since(previous).as_millis() as u64);
                    }
                    stage.last_success_at = Some(success_at);
                }
                let mut shared = self.route_phase.write().await;
                if shared.route.owner_worker_endpoint == snapshot.route.owner_worker_endpoint
                    && response.route_version >= shared.route.route_version
                {
                    shared.route.epoch = response.epoch;
                    shared.route.route_version = response.route_version;
                }
            }
            Err(_) => {
                let mut stats = self.stats.lock().await;
                let stage = stats
                    .stages
                    .get_mut(&phase)
                    .ok_or_else(|| format!("missing stats for phase {phase}"))?;
                stage.failed = stage.failed.saturating_add(1);
            }
        }
        Ok(())
    }
}

async fn run_allocation_loop<S: AllocationStep>(
    mut allocator: S,
    stop: Arc<AtomicBool>,
) -> AppResult<()> {
    while !stop.load(Ordering::Acquire) {
        allocator.step().await?;
    }
    Ok(())
}

async fn wait_for_serving_observation(
    stats: &Arc<Mutex<BenchStats>>,
    minimum_ordinal: u64,
    expected_phase: &str,
    target_endpoint: Option<&str>,
    deadline: Instant,
) -> AppResult<ServingObservation> {
    loop {
        if Instant::now() >= deadline {
            return Err(format!(
                "no {expected_phase} serving allocation observed after ordinal {minimum_ordinal} for target {}",
                target_endpoint.unwrap_or("any")
            )
            .into());
        }
        if let Some(observation) = stats.lock().await.latest_serving.clone() {
            if observation.ordinal > minimum_ordinal
                && observation.phase == expected_phase
                && target_endpoint.is_none_or(|target| target == observation.owner_worker_endpoint)
            {
                return Ok(observation);
            }
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn run_command_driver(
    config: &Config,
    route_phase: Arc<RwLock<RoutePhase>>,
    stats: Arc<Mutex<BenchStats>>,
    stop: Arc<AtomicBool>,
) -> AppResult<u64> {
    let started = Instant::now();
    let mut current_sequence = 0u64;
    let mut command_count = 0u64;
    loop {
        if started.elapsed() >= Duration::from_secs(config.max_runtime_secs) {
            return Err(format!(
                "upgrade bench exceeded {}s runtime without a stop command",
                config.max_runtime_secs
            )
            .into());
        }
        let Some(command) = parse_command(&config.command_file)? else {
            sleep(Duration::from_millis(config.poll_interval_ms)).await;
            continue;
        };
        if command.sequence <= current_sequence {
            sleep(Duration::from_millis(config.poll_interval_ms)).await;
            continue;
        }
        if command.phase == "stop" {
            stop.store(true, Ordering::Release);
            return Ok(command_count);
        }

        let baseline_ordinal = stats.lock().await.ordinal;
        {
            let mut shared = route_phase.write().await;
            shared.phase = Some(command.phase.clone());
        }
        let observed_health = if let Some(target) = &command.target_endpoint {
            Some(health(target).await?)
        } else {
            None
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        if let Some(target) = &command.target_endpoint {
            loop {
                let route = route_phase.read().await.route.clone();
                if route.owner_worker_endpoint == *target {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(format!(
                        "command {} did not transfer the timeline to {} within 20s",
                        command.sequence, target
                    )
                    .into());
                }
                let refreshed = try_transfer_to(config, &route, target).await;
                let mut shared = route_phase.write().await;
                if refreshed.route_version >= shared.route.route_version {
                    shared.route = refreshed;
                }
                drop(shared);
                sleep(Duration::from_millis(25)).await;
            }
        }
        let observation = wait_for_serving_observation(
            &stats,
            baseline_ordinal,
            &command.phase,
            command.target_endpoint.as_deref(),
            deadline,
        )
        .await?;
        let route = route_phase.read().await.route.clone();
        write_ack(
            &config.ack_file,
            &PendingAck {
                command: command.clone(),
                health: observed_health,
            },
            &route,
            observation.last_tso,
        )?;
        current_sequence = command.sequence;
        command_count = command_count.saturating_add(1);
    }
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let config = load_config()?;
    let started = Instant::now();
    let route = 'wait_for_timeline: loop {
        for endpoint in &config.endpoints {
            if let Ok(route) = ensure_timeline(endpoint, &config.timeline_key).await {
                break 'wait_for_timeline route;
            }
        }
        if started.elapsed() >= Duration::from_secs(30) {
            return Err("no upgrade endpoint could ensure the timeline within 30s".into());
        }
        sleep(Duration::from_millis(100)).await;
    };

    let route_phase = Arc::new(RwLock::new(RoutePhase { route, phase: None }));
    let stats = Arc::new(Mutex::new(BenchStats::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let allocator_handle = tokio::spawn(run_allocation_loop(
        NetworkAllocator {
            config: config.clone(),
            route_phase: route_phase.clone(),
            stats: stats.clone(),
            clients: BTreeMap::new(),
        },
        stop.clone(),
    ));
    let driver_result = run_command_driver(&config, route_phase, stats.clone(), stop.clone()).await;
    stop.store(true, Ordering::Release);
    let allocator_result = allocator_handle.await?;
    let command_count = driver_result?;
    allocator_result?;

    let stats = stats.lock().await;
    println!("result=success");
    println!("timeline_key={}", config.timeline_key);
    println!("command_count={command_count}");
    println!(
        "monotonicity_violations_total={}",
        stats.monotonicity_violations
    );
    println!(
        "global_max_success_gap_ms={}",
        stats.global_max_success_gap_ms
    );
    println!(
        "global_first_tso={}",
        stats
            .stages
            .values()
            .filter_map(|stats| stats.first_tso)
            .min()
            .unwrap_or(0)
    );
    println!("global_last_tso={}", stats.last_tso.unwrap_or(0));
    for phase in PHASES {
        let phase_stats = stats
            .stages
            .get(phase)
            .ok_or_else(|| format!("missing final stats for phase {phase}"))?;
        println!("{phase}_requests_total={}", phase_stats.requests);
        println!("{phase}_success_total={}", phase_stats.success);
        println!("{phase}_failed_total={}", phase_stats.failed);
        println!("{phase}_first_tso={}", phase_stats.first_tso.unwrap_or(0));
        println!("{phase}_last_tso={}", phase_stats.last_tso.unwrap_or(0));
        println!(
            "{phase}_max_success_gap_ms={}",
            phase_stats.max_success_gap_ms
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    struct CountingAllocator {
        progress: Arc<AtomicU64>,
    }

    #[async_trait]
    impl AllocationStep for CountingAllocator {
        async fn step(&mut self) -> AppResult<()> {
            self.progress.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
            Ok(())
        }
    }

    fn command_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "chronos-upgrade-bench-{name}-{}-{}",
            std::process::id(),
            now_ms()
        ))
    }

    #[test]
    fn command_parser_accepts_known_phase_and_optional_target() {
        let command = command_path("valid");
        fs::write(&command, "7|mixed_1|127.0.0.1:52051\n").expect("write command");
        let parsed = parse_command(&command)
            .expect("parse command")
            .expect("command should exist");
        assert_eq!(parsed.sequence, 7);
        assert_eq!(parsed.phase, "mixed_1");
        assert_eq!(parsed.target_endpoint.as_deref(), Some("127.0.0.1:52051"));

        fs::write(&command, "8|replace_1|-\n").expect("write command");
        assert!(parse_command(&command)
            .expect("parse command")
            .expect("command should exist")
            .target_endpoint
            .is_none());
        fs::remove_file(command).expect("remove command");
    }

    #[test]
    fn command_parser_rejects_unknown_phase() {
        let command = command_path("invalid");
        fs::write(&command, "1|rollback|-\n").expect("write command");
        assert!(parse_command(&command).is_err());
        fs::remove_file(command).expect("remove command");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pending_control_future_does_not_stop_allocation_progress() {
        let stop = Arc::new(AtomicBool::new(false));
        let progress = Arc::new(AtomicU64::new(0));
        let allocator = tokio::spawn(run_allocation_loop(
            CountingAllocator {
                progress: progress.clone(),
            },
            stop.clone(),
        ));
        let control = tokio::spawn(std::future::pending::<()>());

        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert!(
            progress.load(Ordering::Relaxed) > 1,
            "allocator must continue stepping while the control future is permanently pending"
        );

        stop.store(true, Ordering::Release);
        allocator
            .await
            .expect("allocator task should join")
            .expect("allocator loop should succeed");
        control.abort();
    }
}
