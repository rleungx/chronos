use std::env;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Barrier;
use tonic::transport::Channel;
use tonic::Request;

use chronos_tso::proto::v1::{
    timeline_route_service_client::TimelineRouteServiceClient,
    timestamp_service_client::TimestampServiceClient, AllocateTimestampsRequest,
    EnsureTimelineRequest, ResourceTier,
};

type AppResult<T> = Result<T, Box<dyn Error>>;

#[derive(Clone)]
struct BenchConfig {
    endpoint: String,
    concurrency: usize,
    timeline_count: usize,
    batch: u32,
    duration_secs: u64,
    warmup_secs: u64,
    resource_tier: ResourceTier,
    scenario: String,
}

#[derive(Default)]
struct WorkerStats {
    requests: u64,
    tsos: u64,
    latencies_us: Vec<u64>,
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

fn parse_resource_tier(value: &str) -> ResourceTier {
    match value.to_ascii_lowercase().as_str() {
        "shared" => ResourceTier::Shared,
        "warm" => ResourceTier::Warm,
        "dedicated" => ResourceTier::Dedicated,
        _ => ResourceTier::Shared,
    }
}

fn load_config() -> BenchConfig {
    let endpoint = env_or_string("CHRONOS_BENCH_ENDPOINT", "http://[::1]:50051");
    let concurrency = env_or("CHRONOS_BENCH_CONCURRENCY", 32usize);
    let timeline_count = env_or("CHRONOS_BENCH_TIMELINES", concurrency.max(1));
    let batch = env_or("CHRONOS_BENCH_BATCH", 1u32);
    let duration_secs = env_or("CHRONOS_BENCH_DURATION_SECS", 10u64);
    let warmup_secs = env_or("CHRONOS_BENCH_WARMUP_SECS", 3u64);
    let resource_tier = parse_resource_tier(&env_or_string("CHRONOS_BENCH_RESOURCE_TIER", "shared"));
    let scenario = env_or_string("CHRONOS_BENCH_SCENARIO", "round_robin");
    BenchConfig {
        endpoint,
        concurrency,
        timeline_count,
        batch,
        duration_secs,
        warmup_secs,
        resource_tier,
        scenario,
    }
}

async fn ensure_timelines(config: &BenchConfig, channel: Channel) -> AppResult<Vec<(String, u64, u64)>> {
    let mut client = TimelineRouteServiceClient::new(channel);
    let mut routes = Vec::with_capacity(config.timeline_count);
    for idx in 0..config.timeline_count {
        let timeline_key = format!("bench.{}.{}", config.scenario, idx);
        let response = client
            .ensure_timeline(Request::new(EnsureTimelineRequest {
                timeline_key: timeline_key.clone(),
                desired_resource_tier: config.resource_tier as i32,
            }))
            .await?
            .into_inner();
        let route = response.route.ok_or("missing route")?;
        routes.push((route.timeline_key, route.epoch, route.route_version));
    }
    Ok(routes)
}

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * pct).round() as usize;
    sorted[idx]
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let config = load_config();
    let channel = Channel::from_shared(config.endpoint.clone())?.connect().await?;
    let routes = Arc::new(ensure_timelines(&config, channel.clone()).await?);

    let barrier = Arc::new(Barrier::new(config.concurrency + 1));
    let id_gen = Arc::new(AtomicU64::new(1));
    let warmup_until = Instant::now() + Duration::from_secs(config.warmup_secs);
    let measure_until = warmup_until + Duration::from_secs(config.duration_secs);

    let mut handles = Vec::with_capacity(config.concurrency);
    for worker_idx in 0..config.concurrency {
        let barrier = barrier.clone();
        let routes = routes.clone();
        let endpoint = config.endpoint.clone();
        let id_gen = id_gen.clone();
        let batch = config.batch;
        handles.push(tokio::spawn(async move {
            let channel = Channel::from_shared(endpoint).unwrap().connect().await.unwrap();
            let mut client = TimestampServiceClient::new(channel);
            let mut stats = WorkerStats::default();
            let mut route_idx = worker_idx % routes.len();

            barrier.wait().await;
            loop {
                let now = Instant::now();
                if now >= measure_until {
                    break;
                }

                let route = &routes[route_idx];
                route_idx = (route_idx + 1) % routes.len();

                let start = Instant::now();
                let result = client
                    .allocate_timestamps(Request::new(AllocateTimestampsRequest {
                        timeline_key: route.0.clone(),
                        count: batch,
                        expected_epoch: route.1,
                        expected_route_version: route.2,
                        client_request_id: format!("{}-{}", worker_idx, id_gen.fetch_add(1, Ordering::Relaxed)),
                        request_timeout_ms: 0,
                    }))
                    .await;
                let elapsed = start.elapsed().as_micros() as u64;
                result.unwrap();

                if start >= warmup_until {
                    stats.requests += 1;
                    stats.tsos += batch as u64;
                    stats.latencies_us.push(elapsed);
                }
            }
            stats
        }));
    }

    barrier.wait().await;
    let started = Instant::now();

    let mut total_requests = 0u64;
    let mut total_tsos = 0u64;
    let mut latencies = Vec::new();
    for handle in handles {
        let stats = handle.await?;
        total_requests += stats.requests;
        total_tsos += stats.tsos;
        latencies.extend(stats.latencies_us);
    }
    let elapsed = started.elapsed().as_secs_f64() - config.warmup_secs as f64;
    latencies.sort_unstable();

    println!("scenario={}", config.scenario);
    println!("endpoint={}", config.endpoint);
    println!("concurrency={}", config.concurrency);
    println!("timelines={}", config.timeline_count);
    println!("batch={}", config.batch);
    println!("duration_secs={}", config.duration_secs);
    println!("requests={}", total_requests);
    println!("tsos={}", total_tsos);
    println!("req_per_sec={:.2}", total_requests as f64 / elapsed);
    println!("tso_per_sec={:.2}", total_tsos as f64 / elapsed);
    println!("latency_p50_us={}", percentile(&latencies, 0.50));
    println!("latency_p95_us={}", percentile(&latencies, 0.95));
    println!("latency_p99_us={}", percentile(&latencies, 0.99));
    println!("latency_max_us={}", latencies.last().copied().unwrap_or(0));

    Ok(())
}
