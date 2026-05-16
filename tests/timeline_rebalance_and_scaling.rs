#[path = "common/config.rs"]
mod common_config;
#[path = "common/etcd_endpoints.rs"]
mod common_etcd_endpoints;
#[path = "common/etcd_prefix.rs"]
mod common_etcd_prefix;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use common_config::required_test_config;
use common_etcd_endpoints::test_etcd_endpoints;
use common_etcd_prefix::unique_test_etcd_prefix;
use tokio::time::{timeout, Duration};

use chronos::{
    decode_tso, encode_tso,
    metadata::{EtcdMetadataStore, GeneratorLeaseAuthority, MemoryMetadataStore},
    AllocateTimestampsRequest, AllocateTimestampsResponse, ManualClock, ResourceTier,
    TimelineLifecycleState, TimelineRoute, TransferReason, TsoConfig, TsoError, TsoService,
    MAX_PHYSICAL_MS, SEQUENCE_CAPACITY,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IssuedSpan {
    first_tso: u64,
    last_tso: u64,
}

async fn service_with_config(
    mut config: TsoConfig,
    now_ms: u64,
) -> (Arc<ManualClock>, Arc<TsoService>) {
    config.max_future_borrow_ms = 10000;
    let clock = Arc::new(ManualClock::new(now_ms));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(required_test_config(config), clock.clone(), metadata).unwrap();
    (clock, service)
}

fn memory_two_worker_services(
    base: TsoConfig,
    clock: Arc<ManualClock>,
    metadata: Arc<MemoryMetadataStore>,
) -> (Arc<TsoService>, Arc<TsoService>) {
    let service_a = TsoService::new(
        with_worker(base.clone(), "worker-a"),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();
    let service_b = TsoService::new(with_worker(base, "worker-b"), clock, metadata).unwrap();
    (service_a, service_b)
}

fn worker_endpoint(worker_id: &str) -> String {
    format!("{worker_id}:50051")
}

async fn real_etcd_store(prefix: &str) -> Arc<EtcdMetadataStore> {
    Arc::new(
        EtcdMetadataStore::from_raw_endpoints_unchecked(test_etcd_endpoints(), prefix.to_string())
            .await
            .expect("etcd store should start"),
    )
}

async fn overwrite_generator_record<M, F>(metadata: &Arc<M>, generator_id: u32, mutator: F)
where
    M: GeneratorLeaseAuthority + ?Sized,
    F: FnOnce(&mut chronos::metadata::GeneratorRecord),
{
    let (mut record, revision) = metadata
        .load_generator(generator_id)
        .await
        .unwrap()
        .unwrap();
    mutator(&mut record);
    metadata
        .compare_exchange_generator(generator_id, revision, &record)
        .await
        .unwrap();
}

fn with_worker(mut config: TsoConfig, worker_id: &str) -> TsoConfig {
    config.worker_id = worker_id.to_owned();
    config.advertise_endpoint = worker_endpoint(worker_id);
    config.max_future_borrow_ms = 10000;
    required_test_config(config)
}

fn with_instance_id(mut config: TsoConfig, instance_id: &str) -> TsoConfig {
    config.instance_id = instance_id.to_owned();
    config
}

fn request(
    route: &TimelineRoute,
    client_request_id: String,
    count: u32,
) -> AllocateTimestampsRequest {
    AllocateTimestampsRequest {
        timeline_key: route.timeline_key.clone(),
        count,
        expected_epoch: route.epoch,
        expected_route_version: route.route_version,
        client_request_id,
    }
}

fn response_tsos(response: &AllocateTimestampsResponse) -> Vec<u64> {
    let mut tsos = Vec::new();
    for range in &response.ranges {
        for tso in range.start_tso..=range.end_tso {
            tsos.push(tso);
        }
    }
    tsos
}

async fn issue_once_for_routes(
    service: &Arc<TsoService>,
    routes: &[TimelineRoute],
    phase: &str,
    count: u32,
) -> (HashMap<String, IssuedSpan>, HashSet<u64>) {
    let mut spans = HashMap::new();
    let mut all_tsos = HashSet::new();
    for route in routes {
        let response = service
            .allocate_timestamps(request(
                route,
                format!("{}-{}", phase, route.timeline_key),
                count,
            ))
            .await
            .unwrap();
        let tsos = response_tsos(&response);
        let first_tso = *tsos.first().unwrap();
        let last_tso = *tsos.last().unwrap();
        spans.insert(
            route.timeline_key.clone(),
            IssuedSpan {
                first_tso,
                last_tso,
            },
        );
        for tso in tsos {
            assert!(
                all_tsos.insert(tso),
                "duplicate tso {} in phase {}",
                tso,
                phase
            );
        }
    }
    (spans, all_tsos)
}

fn generator_usage(routes: &[TimelineRoute]) -> HashMap<u32, usize> {
    let mut usage = HashMap::new();
    for route in routes {
        *usage.entry(route.generator_id).or_insert(0) += 1;
    }
    usage
}

fn sorted_usage_values(usage: &HashMap<u32, usize>) -> Vec<usize> {
    let mut values: Vec<_> = usage.values().copied().collect();
    values.sort_unstable();
    values
}

async fn assert_stale_routes_are_rejected(
    service: &Arc<TsoService>,
    routes: &[TimelineRoute],
    phase: &str,
) {
    for route in routes {
        let stale = service
            .allocate_timestamps(request(
                route,
                format!("stale-{}-{}", phase, route.timeline_key),
                1,
            ))
            .await;
        match stale {
            Err(TsoError::RouteVersionMismatch { expected, actual }) => {
                assert_eq!(expected, route.route_version);
                assert!(actual > expected);
            }
            other => panic!(
                "unexpected stale route result for {}: {:?}",
                route.timeline_key, other
            ),
        }
    }
}

fn assert_all_timelines_advanced(
    before: &HashMap<String, IssuedSpan>,
    after: &HashMap<String, IssuedSpan>,
) {
    for (timeline_key, before_span) in before {
        let after_span = after.get(timeline_key).unwrap();
        assert!(
            before_span.last_tso < after_span.first_tso,
            "timeline {} regressed: before_last={} after_first={}",
            timeline_key,
            before_span.last_tso,
            after_span.first_tso
        );
    }
}

async fn transfer_routes_to_targets(
    service: &Arc<TsoService>,
    routes: &[TimelineRoute],
    target_of: impl Fn(usize, &TimelineRoute) -> u32,
) -> Vec<TimelineRoute> {
    let mut refreshed = Vec::with_capacity(routes.len());
    let endpoint = service.advertise_endpoint().to_string();
    for (idx, route) in routes.iter().enumerate() {
        refreshed.push(
            service
                .transfer_timeline(
                    &route.timeline_key,
                    ResourceTier::Shared,
                    endpoint.clone(),
                    Some(target_of(idx, route)),
                )
                .await
                .unwrap(),
        );
    }
    refreshed
}

#[path = "timeline_rebalance_and_scaling/distribution.rs"]
mod distribution;
#[path = "timeline_rebalance_and_scaling/failover.rs"]
mod failover;
#[path = "timeline_rebalance_and_scaling/recovery_floor.rs"]
mod recovery_floor;
#[path = "timeline_rebalance_and_scaling/restart_and_routes.rs"]
mod restart_and_routes;
#[path = "timeline_rebalance_and_scaling/runtime_cache.rs"]
mod runtime_cache;
