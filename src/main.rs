use std::net::SocketAddr;
use std::sync::Arc;
use std::{env, error::Error};
use tonic::transport::Server;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request as HyperRequest, Response as HyperResponse, StatusCode};
use prometheus::{Encoder, TextEncoder};

use chronos_tso::proto::v1::{
    timeline_control_service_server::TimelineControlServiceServer,
    timeline_route_service_server::TimelineRouteServiceServer,
    timestamp_service_server::TimestampServiceServer,
};
use chronos_tso::rpc::{TsoControlService, TsoRouteService, TsoTimestampService};
use chronos_tso::{ResourceTier, SystemClock, TsoConfig, TsoService};

use chronos_tso::metadata::{EtcdMetadataStore, MemoryMetadataStore};

const DEFAULT_BIND_ADDR: &str = "[::1]:50051";
const DEFAULT_METADATA_KIND: &str = "memory";
const DEFAULT_METRICS_BIND_ADDR: &str = "127.0.0.1:9898";
const DEFAULT_ETCD_PREFIX: &str = "/chronos-tso";

type AppResult<T> = Result<T, Box<dyn Error>>;

async fn metrics_handler(req: HyperRequest<Body>) -> Result<HyperResponse<Body>, hyper::Error> {
    match req.uri().path() {
        "/healthz" => Ok(HyperResponse::builder()
            .status(StatusCode::OK)
            .body(Body::from("ok"))
            .unwrap()),
        "/metrics" => {
            let encoder = TextEncoder::new();
            let metric_families = prometheus::gather();
            let mut buffer = Vec::new();
            encoder.encode(&metric_families, &mut buffer).unwrap_or(());
            Ok(HyperResponse::builder()
                .status(StatusCode::OK)
                .header("Content-Type", encoder.format_type())
                .body(Body::from(buffer))
                .unwrap())
        }
        _ => Ok(HyperResponse::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("not found"))
            .unwrap()),
    }
}

async fn serve_metrics(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let make_svc =
        make_service_fn(|_conn| async { Ok::<_, hyper::Error>(service_fn(metrics_handler)) });
    hyper::Server::bind(&addr).serve(make_svc).await?;
    Ok(())
}

fn read_env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn apply_string_env(key: &str, target: &mut String) {
    if let Ok(value) = env::var(key) {
        *target = value;
    }
}

fn apply_parsed_env<T>(key: &str, target: &mut T) -> AppResult<()>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    if let Ok(value) = env::var(key) {
        *target = value.parse()?;
    }
    Ok(())
}

fn parse_resource_tier(value: &str) -> Result<ResourceTier, Box<dyn Error>> {
    match value.to_ascii_lowercase().as_str() {
        "shared" => Ok(ResourceTier::Shared),
        "warm" => Ok(ResourceTier::Warm),
        "dedicated" => Ok(ResourceTier::Dedicated),
        _ => Err(format!("invalid resource tier: {}", value).into()),
    }
}

fn load_tso_config() -> AppResult<TsoConfig> {
    let mut config = TsoConfig::default();

    apply_string_env("CHRONOS_TSO_WORKER_ID", &mut config.worker_id);
    apply_string_env("CHRONOS_TSO_INSTANCE_ID", &mut config.instance_id);
    apply_string_env(
        "CHRONOS_TSO_ADVERTISE_ENDPOINT",
        &mut config.advertise_endpoint,
    );
    apply_parsed_env(
        "CHRONOS_TSO_SHARED_GENERATORS",
        &mut config.shared_generators,
    )?;
    apply_parsed_env("CHRONOS_TSO_WARM_GENERATORS", &mut config.warm_generators)?;
    apply_parsed_env(
        "CHRONOS_TSO_MAX_BATCH_PER_REQUEST",
        &mut config.max_batch_per_request,
    )?;
    apply_parsed_env(
        "CHRONOS_TSO_MAX_FUTURE_BORROW_MS",
        &mut config.max_future_borrow_ms,
    )?;
    apply_parsed_env(
        "CHRONOS_TSO_MAX_CLOCK_REWIND_MS",
        &mut config.max_clock_rewind_ms,
    )?;
    apply_parsed_env("CHRONOS_TSO_LEASE_TTL_MS", &mut config.lease_ttl_ms)?;
    apply_parsed_env(
        "CHRONOS_TSO_GENERATOR_LEASE_TTL_MS",
        &mut config.generator_lease_ttl_ms,
    )?;
    apply_parsed_env(
        "CHRONOS_TSO_GENERATOR_MAINTENANCE_INTERVAL_MS",
        &mut config.generator_maintenance_interval_ms,
    )?;
    apply_parsed_env(
        "CHRONOS_TSO_GENERATOR_OWNERSHIP_MODULO",
        &mut config.generator_ownership_modulo,
    )?;
    apply_parsed_env(
        "CHRONOS_TSO_GENERATOR_OWNERSHIP_REMAINDER",
        &mut config.generator_ownership_remainder,
    )?;
    apply_parsed_env("CHRONOS_TSO_PRE_BORROW_MS", &mut config.pre_borrow_ms)?;
    apply_parsed_env(
        "CHRONOS_TSO_SHARED_JUMP_AHEAD_THRESHOLD_MS",
        &mut config.shared_jump_ahead_threshold_ms,
    )?;
    apply_parsed_env("CHRONOS_TSO_SAFETY_GAP_MS", &mut config.safety_gap_ms)?;
    apply_parsed_env(
        "CHRONOS_TSO_ROUTE_CACHE_TTL_MS",
        &mut config.route_cache_ttl_ms,
    )?;
    apply_parsed_env(
        "CHRONOS_TSO_MAX_TIMELINE_PROXY_LANES",
        &mut config.max_timeline_proxy_lanes,
    )?;
    apply_parsed_env(
        "CHRONOS_TSO_MAX_TIMELINE_RUNTIME_ENTRIES",
        &mut config.max_timeline_runtime_entries,
    )?;
    if let Ok(value) = env::var("CHRONOS_TSO_DEFAULT_RESOURCE_TIER") {
        config.default_resource_tier = parse_resource_tier(&value)?;
    }

    Ok(config)
}

async fn build_tso_service(
    config: &TsoConfig,
    clock: Arc<SystemClock>,
) -> AppResult<Arc<TsoService>> {
    match read_env_or_default("CHRONOS_TSO_METADATA", DEFAULT_METADATA_KIND)
        .to_ascii_lowercase()
        .as_str()
    {
        "memory" => {
            let metadata = Arc::new(MemoryMetadataStore::new());
            Ok(TsoService::new(config.clone(), clock, metadata)?)
        }
        "etcd" => {
            let endpoints = env::var("CHRONOS_TSO_ETCD_ENDPOINTS")?;
            let endpoints = endpoints
                .split(',')
                .map(|endpoint| endpoint.trim().to_string())
                .filter(|endpoint| !endpoint.is_empty())
                .collect();
            let prefix = read_env_or_default("CHRONOS_TSO_ETCD_PREFIX", DEFAULT_ETCD_PREFIX);
            let metadata = Arc::new(EtcdMetadataStore::new(endpoints, prefix).await?);
            Ok(TsoService::new(config.clone(), clock, metadata)?)
        }
        other => Err(format!("unknown metadata store: {}", other).into()),
    }
}

fn spawn_metrics_server(metrics_addr: SocketAddr) {
    tokio::spawn(async move {
        if let Err(error) = serve_metrics(metrics_addr).await {
            eprintln!("metrics server failed: {}", error);
        }
    });
}

#[tokio::main]
async fn main() -> AppResult<()> {
    println!("Starting Chronos TSO Server...");

    let clock = Arc::new(SystemClock);
    let config = load_tso_config()?;
    let service = build_tso_service(&config, clock.clone()).await?;

    let route_service = TsoRouteService::new(service.control_plane());
    let timestamp_service = TsoTimestampService::new(service.data_plane());
    let control_service = TsoControlService::new(service.control_plane());

    let metrics_addr: SocketAddr =
        read_env_or_default("CHRONOS_TSO_METRICS_BIND_ADDR", DEFAULT_METRICS_BIND_ADDR).parse()?;
    spawn_metrics_server(metrics_addr);

    let addr: SocketAddr =
        read_env_or_default("CHRONOS_TSO_BIND_ADDR", DEFAULT_BIND_ADDR).parse()?;
    println!("Listening on {}", addr);

    Server::builder()
        .add_service(TimelineRouteServiceServer::new(route_service))
        .add_service(TimestampServiceServer::new(timestamp_service))
        .add_service(TimelineControlServiceServer::new(control_service))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clear_tso_env() {
        for key in [
            "CHRONOS_TSO_ROUTE_CACHE_TTL_MS",
            "CHRONOS_TSO_DEFAULT_RESOURCE_TIER",
            "CHRONOS_TSO_WORKER_ID",
            "CHRONOS_TSO_INSTANCE_ID",
            "CHRONOS_TSO_ADVERTISE_ENDPOINT",
        ] {
            unsafe { env::remove_var(key) };
        }
    }

    #[test]
    fn load_tso_config_reads_route_cache_ttl_variants_from_env() {
        clear_tso_env();
        unsafe { env::set_var("CHRONOS_TSO_ROUTE_CACHE_TTL_MS", "1234") };

        let config = load_tso_config().unwrap();
        assert_eq!(config.route_cache_ttl_ms, 1234);

        clear_tso_env();
        unsafe { env::set_var("CHRONOS_TSO_ROUTE_CACHE_TTL_MS", "0") };

        let config = load_tso_config().unwrap();
        assert_eq!(config.route_cache_ttl_ms, 0);

        clear_tso_env();
    }
}
