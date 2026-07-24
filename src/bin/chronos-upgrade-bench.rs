use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    started_at: Instant,
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

#[tokio::main]
async fn main() -> AppResult<()> {
    let config = load_config()?;
    let started = Instant::now();
    let mut route = 'wait_for_timeline: loop {
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

    let mut stages = PHASES
        .iter()
        .map(|phase| ((*phase).to_owned(), PhaseStats::default()))
        .collect::<BTreeMap<_, _>>();
    let mut current_phase = None::<String>;
    let mut current_sequence = 0u64;
    let mut pending_ack = None::<PendingAck>;
    let mut clients = BTreeMap::new();
    let mut ordinal = 0u64;
    let mut last_tso = None::<u64>;
    let mut monotonicity_violations = 0u64;
    let mut last_success_at = None::<Instant>;
    let mut global_max_success_gap_ms = 0u64;
    let mut command_count = 0u64;
    let mut stop_requested = false;

    while !stop_requested {
        if started.elapsed() >= Duration::from_secs(config.max_runtime_secs) {
            return Err(format!(
                "upgrade bench exceeded {}s runtime without a stop command",
                config.max_runtime_secs
            )
            .into());
        }

        if let Some(command) = parse_command(&config.command_file)? {
            if command.sequence > current_sequence {
                if command.phase == "stop" {
                    stop_requested = true;
                    continue;
                }
                let observed_health = if let Some(target) = &command.target_endpoint {
                    Some(health(target).await?)
                } else {
                    None
                };
                current_sequence = command.sequence;
                current_phase = Some(command.phase.clone());
                pending_ack = Some(PendingAck {
                    command,
                    health: observed_health,
                    started_at: Instant::now(),
                });
                command_count = command_count.saturating_add(1);
            }
        }

        let Some(phase) = current_phase.as_deref() else {
            sleep(Duration::from_millis(config.poll_interval_ms)).await;
            continue;
        };
        ordinal = ordinal.saturating_add(1);
        let stats = stages
            .get_mut(phase)
            .ok_or_else(|| format!("missing stats for phase {phase}"))?;
        if let Some(pending) = pending_ack.as_ref() {
            if let Some(target) = &pending.command.target_endpoint {
                if route.owner_worker_endpoint != *target {
                    if pending.started_at.elapsed() >= Duration::from_secs(20) {
                        return Err(format!(
                            "command {} did not transfer the timeline to {} within 20s",
                            pending.command.sequence, target
                        )
                        .into());
                    }
                    let previous_owner = route.owner_worker_endpoint.clone();
                    route = try_transfer_to(&config, &route, target).await;
                    if route.owner_worker_endpoint != previous_owner {
                        clients.clear();
                    }
                }
            }
        }
        stats.requests = stats.requests.saturating_add(1);
        match allocate(&mut clients, &route, ordinal, config.request_timeout_ms).await {
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
                if first > last || last_tso.is_some_and(|previous| first <= previous) {
                    monotonicity_violations = monotonicity_violations.saturating_add(1);
                }
                last_tso = Some(last_tso.map_or(last, |previous| previous.max(last)));
                route.epoch = response.epoch;
                route.route_version = response.route_version;
                stats.success = stats.success.saturating_add(1);
                stats.first_tso.get_or_insert(first);
                stats.last_tso = Some(stats.last_tso.map_or(last, |value| value.max(last)));
                let success_at = Instant::now();
                if let Some(previous) = last_success_at {
                    global_max_success_gap_ms = global_max_success_gap_ms
                        .max(success_at.duration_since(previous).as_millis() as u64);
                }
                last_success_at = Some(success_at);
                if let Some(previous) = stats.last_success_at {
                    stats.max_success_gap_ms = stats
                        .max_success_gap_ms
                        .max(success_at.duration_since(previous).as_millis() as u64);
                }
                stats.last_success_at = Some(success_at);
                let command_is_serving = pending_ack.as_ref().is_some_and(|pending| {
                    pending
                        .command
                        .target_endpoint
                        .as_ref()
                        .is_none_or(|target| target == &route.owner_worker_endpoint)
                });
                if command_is_serving {
                    let pending = pending_ack
                        .take()
                        .expect("serving command must have pending acknowledgement");
                    write_ack(&config.ack_file, &pending, &route, last)?;
                }
            }
            Err(_) => {
                stats.failed = stats.failed.saturating_add(1);
                if let Ok(refreshed) = refresh_route(&config.endpoints, &config.timeline_key).await
                {
                    if refreshed.owner_worker_endpoint != route.owner_worker_endpoint {
                        clients.clear();
                    }
                    route = refreshed;
                } else {
                    sleep(Duration::from_millis(config.poll_interval_ms)).await;
                }
            }
        }
    }

    println!("result=success");
    println!("timeline_key={}", config.timeline_key);
    println!("command_count={command_count}");
    println!("monotonicity_violations_total={monotonicity_violations}");
    println!("global_max_success_gap_ms={global_max_success_gap_ms}");
    println!(
        "global_first_tso={}",
        stages
            .values()
            .filter_map(|stats| stats.first_tso)
            .min()
            .unwrap_or(0)
    );
    println!("global_last_tso={}", last_tso.unwrap_or(0));
    for phase in PHASES {
        let stats = stages
            .get(phase)
            .ok_or_else(|| format!("missing final stats for phase {phase}"))?;
        println!("{phase}_requests_total={}", stats.requests);
        println!("{phase}_success_total={}", stats.success);
        println!("{phase}_failed_total={}", stats.failed);
        println!("{phase}_first_tso={}", stats.first_tso.unwrap_or(0));
        println!("{phase}_last_tso={}", stats.last_tso.unwrap_or(0));
        println!("{phase}_max_success_gap_ms={}", stats.max_success_gap_ms);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
