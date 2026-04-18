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
    // P2: Increase future borrow for tests to accommodate clock jumps
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

#[tokio::test]
async fn shared_timelines_are_evenly_distributed_across_shared_generators() {
    let shared_generators = 8;
    let config = TsoConfig {
        shared_generators,
        warm_generators: 0,
        ..TsoConfig::default()
    };
    let (_clock, service) = service_with_config(config, 100).await;

    let mut covered = BTreeSet::new();
    let mut counts = vec![0usize; shared_generators as usize];
    for idx in 0..1024 {
        let route = service
            .ensure_timeline(&format!("shared-coverage-{}", idx))
            .await
            .unwrap();
        covered.insert(route.generator_id);
        counts[route.generator_id as usize] += 1;
    }

    assert_eq!(covered.len(), shared_generators as usize);
    let max_bucket = *counts.iter().max().unwrap();
    let avg_bucket = 1024usize / shared_generators as usize;
    assert!(max_bucket < avg_bucket * 2);
}

#[tokio::test]
async fn allocations_remain_globally_unique_across_many_timelines() {
    let config = TsoConfig {
        shared_generators: 8,
        warm_generators: 0,
        max_batch_per_request: 32,
        max_future_borrow_ms: 10000,
        default_resource_tier: ResourceTier::Shared,
        ..TsoConfig::default()
    };
    let (_clock, service) = service_with_config(config, 1_000).await;

    let timeline_count = 16usize;
    let per_timeline_threads = 4usize;
    let allocations_per_thread = 32usize;

    let mut routes = Vec::with_capacity(timeline_count);
    for idx in 0..timeline_count {
        routes.push(
            service
                .ensure_timeline(&format!("orders-{}", idx))
                .await
                .unwrap(),
        );
    }

    let mut handles = vec![];
    for route in routes.clone() {
        for thread_idx in 0..per_timeline_threads {
            let service = service.clone();
            let route = route.clone();
            handles.push(tokio::spawn(async move {
                let mut local = Vec::with_capacity(allocations_per_thread * 2);
                for alloc_idx in 0..allocations_per_thread {
                    let response = service
                        .allocate_timestamps(request(
                            &route,
                            format!("{}-{}-{}", route.timeline_key, thread_idx, alloc_idx),
                            2,
                        ))
                        .await
                        .unwrap();
                    local.extend(response_tsos(&response));
                }
                (route.timeline_key.clone(), local)
            }));
        }
    }

    let expected_total = timeline_count * per_timeline_threads * allocations_per_thread * 2;
    let mut global = HashSet::with_capacity(expected_total);
    let mut per_timeline: HashMap<String, Vec<u64>> = HashMap::new();
    for handle in handles {
        let (timeline_key, tsos) = handle.await.unwrap();
        for tso in tsos {
            assert!(global.insert(tso), "duplicate tso {}", tso);
            per_timeline
                .entry(timeline_key.clone())
                .or_default()
                .push(tso);
        }
    }

    assert_eq!(global.len(), expected_total);
    assert_eq!(per_timeline.len(), timeline_count);
    for values in per_timeline.values_mut() {
        values.sort_unstable();
        for pair in values.windows(2) {
            assert!(pair[0] < pair[1]);
        }
    }
}

#[tokio::test]
async fn stale_routes_are_rejected_after_transfer_and_refreshed_routes_continue_monotonicity() {
    let config = TsoConfig {
        shared_generators: 4,
        warm_generators: 0,
        max_future_borrow_ms: 10000,
        ..TsoConfig::default()
    };
    let (_clock, service) = service_with_config(config, 2_000).await;

    let route = service.ensure_timeline("timeline.transfer").await.unwrap();
    let before = service
        .allocate_timestamps(request(&route, "before-transfer".to_owned(), 4))
        .await
        .unwrap();
    let before_last = before.ranges.last().unwrap().end_tso;

    let refreshed = service
        .transfer_timeline(
            "timeline.transfer",
            ResourceTier::Shared,
            service.advertise_endpoint().to_string(),
            Some(1),
        )
        .await
        .unwrap();
    assert!(refreshed.route_version > route.route_version);
    assert!(refreshed.epoch > route.epoch);

    assert_stale_routes_are_rejected(&service, std::slice::from_ref(&route), "single-transfer")
        .await;

    let after = service
        .allocate_timestamps(request(&refreshed, "after-transfer".to_owned(), 2))
        .await
        .unwrap();
    let after_first = after.ranges.first().unwrap().start_tso;
    assert!(before_last < after_first);
}

#[tokio::test]
async fn dedicated_timelines_use_isolated_generators_instead_of_shared_tier() {
    let config = TsoConfig {
        shared_generators: 4,
        warm_generators: 2,
        max_future_borrow_ms: 10000,
        ..TsoConfig::default()
    };
    let (_clock, service) = service_with_config(config, 500).await;

    let mut shared_routes = vec![];
    for idx in 0..32 {
        shared_routes.push(
            service
                .ensure_timeline(&format!("shared-{}", idx))
                .await
                .unwrap(),
        );
    }
    let dedicated_a = service
        .ensure_timeline_with_tier("dedicated-a", ResourceTier::Dedicated)
        .await
        .unwrap();
    let dedicated_b = service
        .ensure_timeline_with_tier("dedicated-b", ResourceTier::Dedicated)
        .await
        .unwrap();

    assert!(dedicated_a.generator_id >= 6);
    assert!(dedicated_b.generator_id >= 6);
    assert_ne!(dedicated_a.generator_id, dedicated_b.generator_id);
    assert!(shared_routes.iter().all(|route| route.generator_id < 4));

    let first = service
        .allocate_timestamps(request(&dedicated_a, "dedicated-first".to_owned(), 1))
        .await
        .unwrap();
    let second = service
        .allocate_timestamps(request(&dedicated_a, "dedicated-second".to_owned(), 1))
        .await
        .unwrap();
    assert!(first.ranges[0].end_tso < second.ranges[0].start_tso);
}

#[tokio::test]
async fn rewound_clock_can_continue_on_existing_physical_slot_with_zero_future_borrow() {
    let config = TsoConfig {
        shared_generators: 1,
        warm_generators: 0,
        max_future_borrow_ms: 0,
        max_clock_rewind_ms: 500,
        ..TsoConfig::default()
    };
    let clock = Arc::new(ManualClock::new(10_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(required_test_config(config), clock.clone(), metadata).unwrap();

    let route = service
        .ensure_timeline("clock.rewind.timeline")
        .await
        .unwrap();
    let first = service
        .allocate_timestamps(request(&route, "rewind-before".to_string(), 1))
        .await
        .unwrap();
    let first_tso = first.ranges[0].start_tso;

    clock.set(9_900);
    let second = service
        .allocate_timestamps(request(&route, "rewind-after".to_string(), 1))
        .await
        .unwrap();
    let second_tso = second.ranges[0].start_tso;

    assert!(second_tso > first_tso);
    assert_eq!(
        decode_tso(first_tso).physical_ms,
        decode_tso(second_tso).physical_ms
    );
}

#[tokio::test]
async fn large_safe_floor_jump_auto_isolates_into_dedicated_generator() {
    let config = TsoConfig {
        shared_generators: 1,
        warm_generators: 0,
        shared_jump_ahead_threshold_ms: 100,
        max_future_borrow_ms: 10_000,
        ..TsoConfig::default()
    };
    let clock = Arc::new(ManualClock::new(1_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(required_test_config(config), clock.clone(), metadata).unwrap();

    let shared = service
        .ensure_timeline("shared-floor.anchor")
        .await
        .unwrap();
    service
        .allocate_timestamps(request(&shared, "shared-floor-seed".to_string(), 1))
        .await
        .unwrap();

    clock.set(20_000);
    let dedicated = service
        .ensure_timeline_with_tier("hot.timeline", ResourceTier::Dedicated)
        .await
        .unwrap();
    service
        .allocate_timestamps(request(&dedicated, "hot-seed".to_string(), 1))
        .await
        .unwrap();

    let moved = service
        .transfer_timeline(
            &dedicated.timeline_key,
            ResourceTier::Shared,
            service.advertise_endpoint().to_string(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(moved.resource_tier, ResourceTier::Dedicated);
    assert!(moved.generator_id >= 1);
}

#[tokio::test]
async fn splitting_shared_capacity_rebalances_timelines_without_tso_regression() {
    let config = TsoConfig {
        shared_generators: 4,
        warm_generators: 0,
        max_batch_per_request: 16,
        max_future_borrow_ms: 10000,
        default_resource_tier: ResourceTier::Shared,
        ..TsoConfig::default()
    };
    let (_clock, service) = service_with_config(config, 3_000).await;

    let mut initial_routes = vec![];
    for idx in 0..32 {
        initial_routes.push(
            service
                .ensure_timeline(&format!("split-{}", idx))
                .await
                .unwrap(),
        );
    }
    let compacted_routes =
        transfer_routes_to_targets(&service, &initial_routes, |idx, _| (idx % 2) as u32).await;
    let compacted_usage = generator_usage(&compacted_routes);
    assert_eq!(compacted_usage.len(), 2);

    let (before_spans, before_tsos) =
        issue_once_for_routes(&service, &compacted_routes, "before-split", 8).await;

    let split_routes =
        transfer_routes_to_targets(&service, &compacted_routes, |idx, _| (idx % 4) as u32).await;
    let split_usage = generator_usage(&split_routes);
    assert_eq!(split_usage.len(), 4);
    assert!(
        sorted_usage_values(&split_usage).last().unwrap()
            < sorted_usage_values(&compacted_usage).last().unwrap()
    );

    assert_stale_routes_are_rejected(&service, &compacted_routes, "split").await;

    let (after_spans, after_tsos) =
        issue_once_for_routes(&service, &split_routes, "after-split", 8).await;
    assert_all_timelines_advanced(&before_spans, &after_spans);

    let mut all_tsos = before_tsos;
    for tso in after_tsos {
        assert!(all_tsos.insert(tso), "duplicate tso after split: {}", tso);
    }
}

#[tokio::test]
async fn merging_shared_capacity_compacts_timelines_without_tso_regression() {
    let config = TsoConfig {
        shared_generators: 4,
        warm_generators: 0,
        max_batch_per_request: 16,
        max_future_borrow_ms: 10000,
        default_resource_tier: ResourceTier::Shared,
        ..TsoConfig::default()
    };
    let (_clock, service) = service_with_config(config, 5_000).await;

    let mut initial_routes = vec![];
    for idx in 0..32 {
        initial_routes.push(
            service
                .ensure_timeline(&format!("merge-{}", idx))
                .await
                .unwrap(),
        );
    }
    let expanded_routes =
        transfer_routes_to_targets(&service, &initial_routes, |idx, _| (idx % 4) as u32).await;
    let expanded_usage = generator_usage(&expanded_routes);
    assert_eq!(expanded_usage.len(), 4);

    let (before_spans, before_tsos) =
        issue_once_for_routes(&service, &expanded_routes, "before-merge", 8).await;

    let merged_routes =
        transfer_routes_to_targets(&service, &expanded_routes, |idx, _| (idx % 2) as u32).await;
    let merged_usage = generator_usage(&merged_routes);
    assert_eq!(merged_usage.len(), 2);
    assert!(
        sorted_usage_values(&merged_usage).last().unwrap()
            > sorted_usage_values(&expanded_usage).last().unwrap()
    );

    assert_stale_routes_are_rejected(&service, &expanded_routes, "merge").await;

    let (after_spans, after_tsos) =
        issue_once_for_routes(&service, &merged_routes, "after-merge", 8).await;
    assert_all_timelines_advanced(&before_spans, &after_spans);

    let mut all_tsos = before_tsos;
    for tso in after_tsos {
        assert!(all_tsos.insert(tso), "duplicate tso after merge: {}", tso);
    }
}

#[tokio::test]
async fn decoded_tso_reveals_parallel_timelines_using_distinct_generators() {
    let config = TsoConfig {
        shared_generators: 8,
        warm_generators: 0,
        max_future_borrow_ms: 10000,
        ..TsoConfig::default()
    };
    let (_clock, service) = service_with_config(config, 300).await;

    let alpha = service.ensure_timeline("alpha").await.unwrap();
    let mut beta = service.ensure_timeline("beta").await.unwrap();
    if beta.generator_id == alpha.generator_id {
        for idx in 0..1024usize {
            let candidate = service
                .ensure_timeline(&format!("beta-{}", idx))
                .await
                .unwrap();
            if candidate.generator_id != alpha.generator_id {
                beta = candidate;
                break;
            }
        }
    }
    assert_ne!(alpha.generator_id, beta.generator_id);

    let alpha_tso = service
        .allocate_timestamps(request(&alpha, "alpha".to_owned(), 1))
        .await
        .unwrap()
        .ranges[0]
        .start_tso;
    let beta_tso = service
        .allocate_timestamps(request(&beta, "beta".to_owned(), 1))
        .await
        .unwrap()
        .ranges[0]
        .start_tso;

    let alpha_decoded = decode_tso(alpha_tso);
    let beta_decoded = decode_tso(beta_tso);
    assert_eq!(alpha_decoded.generator_id, alpha.generator_id);
    assert_eq!(beta_decoded.generator_id, beta.generator_id);
}

#[test]
fn single_format_tso_decoding_matches_encoder() {
    let tso = chronos::encode_tso(1_234, 4097, 99).unwrap();
    let decoded = decode_tso(tso);
    assert_eq!(decoded.physical_ms, 1_234);
    assert_eq!(decoded.generator_id, 4097);
    assert_eq!(decoded.sequence, 99);
}

#[tokio::test]
async fn restarting_service_restores_last_issued_floor_from_metadata() {
    let config = TsoConfig {
        shared_generators: 4,
        warm_generators: 0,
        max_batch_per_request: 16,
        max_future_borrow_ms: 10000,
        default_resource_tier: ResourceTier::Shared,
        lease_ttl_ms: 5,
        generator_maintenance_interval_ms: 1,
        ..TsoConfig::default()
    };
    let clock = Arc::new(ManualClock::new(10_000));
    let metadata = Arc::new(MemoryMetadataStore::new());

    let service_before = TsoService::new(
        with_worker(config.clone(), "worker-a"),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();
    let route_before = service_before
        .ensure_timeline("restart.timeline")
        .await
        .unwrap();
    let first = service_before
        .allocate_timestamps(request(&route_before, "before-restart".to_owned(), 8))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;
    clock.advance(10);

    let service_after = TsoService::new(with_worker(config, "worker-b"), clock, metadata).unwrap();
    let old_route = service_after
        .get_timeline_route("restart.timeline")
        .await
        .unwrap();
    let target_generator_id = if old_route.generator_id == 0 { 1 } else { 0 };
    let (route_after, _, _) = service_after
        .control_plane()
        .transfer_timeline_for_rpc(
            "restart.timeline",
            service_after.advertise_endpoint().to_string(),
            Some(target_generator_id),
            TransferReason::Failover,
        )
        .await
        .unwrap();
    let second = service_after
        .allocate_timestamps(request(&route_after, "after-restart".to_owned(), 4))
        .await
        .unwrap();
    let second_first = second.ranges.first().unwrap().start_tso;

    assert!(first_last < second_first);
}

#[tokio::test]
async fn restarting_service_with_same_instance_id_reuses_local_lease() {
    let config = with_instance_id(
        with_worker(
            TsoConfig {
                shared_generators: 4,
                warm_generators: 0,
                max_batch_per_request: 16,
                max_future_borrow_ms: 10000,
                default_resource_tier: ResourceTier::Shared,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        "instance-a-stable",
    );
    let clock = Arc::new(ManualClock::new(11_000));
    let metadata = Arc::new(MemoryMetadataStore::new());

    let service_before = TsoService::new(
        required_test_config(config.clone()),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();
    let route_before = service_before
        .ensure_timeline("restart.same-instance.timeline")
        .await
        .unwrap();
    let first = service_before
        .allocate_timestamps(request(
            &route_before,
            "before-restart-stable".to_owned(),
            4,
        ))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;
    drop(service_before);

    let service_after = TsoService::new(required_test_config(config), clock, metadata).unwrap();
    let route_after = service_after
        .get_timeline_route("restart.same-instance.timeline")
        .await
        .unwrap();
    let second = service_after
        .allocate_timestamps(request(&route_after, "after-restart-stable".to_owned(), 2))
        .await
        .unwrap();
    let second_first = second.ranges.first().unwrap().start_tso;

    assert!(first_last < second_first);
}

#[tokio::test]
async fn transferred_route_is_reloaded_from_metadata_after_restart() {
    let config = TsoConfig {
        shared_generators: 4,
        warm_generators: 0,
        max_batch_per_request: 16,
        max_future_borrow_ms: 10000,
        default_resource_tier: ResourceTier::Shared,
        ..TsoConfig::default()
    };
    let clock = Arc::new(ManualClock::new(12_000));
    let metadata = Arc::new(MemoryMetadataStore::new());

    let service_before = TsoService::new(
        required_test_config(config.clone()),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();
    let route_before = service_before
        .ensure_timeline("restart.transfer.timeline")
        .await
        .unwrap();
    let target_generator_id = if route_before.generator_id == 0 { 1 } else { 0 };
    let transferred = service_before
        .transfer_timeline(
            &route_before.timeline_key,
            route_before.resource_tier,
            service_before.advertise_endpoint().to_string(),
            Some(target_generator_id),
        )
        .await
        .unwrap();
    drop(service_before);

    let service_after = TsoService::new(required_test_config(config), clock, metadata).unwrap();
    let reloaded = service_after
        .get_timeline_route("restart.transfer.timeline")
        .await
        .unwrap();

    assert_eq!(reloaded.generator_id, transferred.generator_id);
    assert_eq!(reloaded.route_version, transferred.route_version);
    assert_eq!(reloaded.epoch, transferred.epoch);
}

#[tokio::test]
async fn stale_cached_route_from_another_service_returns_route_mismatch_and_refreshes_cache() {
    let base = TsoConfig {
        shared_generators: 4,
        warm_generators: 0,
        max_batch_per_request: 16,
        max_future_borrow_ms: 10000,
        default_resource_tier: ResourceTier::Shared,
        ..TsoConfig::default()
    };
    let clock = Arc::new(ManualClock::new(20_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let (service_a, service_b) = memory_two_worker_services(base, clock.clone(), metadata);

    let route_a = service_a
        .ensure_timeline("shared-metadata.timeline")
        .await
        .unwrap();
    let mut route_updates = service_a.subscribe_route_changes();
    let _route_b = service_b
        .get_timeline_route("shared-metadata.timeline")
        .await
        .unwrap();

    let transferred = service_b
        .transfer_timeline(
            &route_a.timeline_key,
            route_a.resource_tier,
            worker_endpoint("worker-b"),
            Some(1),
        )
        .await
        .unwrap();
    assert_eq!(
        transferred.owner_worker_endpoint,
        worker_endpoint("worker-b")
    );

    let mut route_update_seen = false;
    for _ in 0..5 {
        let route_update = timeout(Duration::from_millis(500), route_updates.recv())
            .await
            .expect("expected route update after transfer")
            .expect("route watch should remain open");
        if route_update.timeline_key == transferred.timeline_key
            && route_update.route_version >= transferred.route_version
        {
            route_update_seen = true;
            break;
        }
    }
    assert!(
        route_update_seen,
        "did not observe transferred route update"
    );

    let stale_result = service_a
        .allocate_timestamps(request(&route_a, "stale-cached-route".to_owned(), 1))
        .await;
    match stale_result {
        Err(TsoError::RouteVersionMismatch { expected, actual }) => {
            assert_eq!(expected, route_a.route_version);
            assert_eq!(actual, transferred.route_version);
        }
        other => panic!("unexpected stale route result: {:?}", other),
    }

    let old_owner_result = service_a
        .allocate_timestamps(request(
            &transferred,
            "old-owner-after-transfer".to_owned(),
            1,
        ))
        .await;
    match old_owner_result {
        Err(TsoError::NotTimelineOwner {
            owner_worker_endpoint,
            ..
        }) => {
            assert_eq!(owner_worker_endpoint, worker_endpoint("worker-b"));
        }
        other => panic!("unexpected old owner result: {:?}", other),
    }

    let fresh = service_b
        .allocate_timestamps(request(&transferred, "fresh-after-refresh".to_owned(), 1))
        .await
        .unwrap();
    assert_eq!(fresh.route_version, transferred.route_version);
}

#[tokio::test]
async fn concurrent_ensure_returns_single_metadata_record_for_same_timeline() {
    let clock = Arc::new(ManualClock::new(21_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let (service_a, service_b) =
        memory_two_worker_services(TsoConfig::default(), clock, metadata.clone());

    let h1 = tokio::spawn({
        let s = service_a.clone();
        async move { s.ensure_timeline("same.timeline").await.unwrap() }
    });
    let h2 = tokio::spawn({
        let s = service_b.clone();
        async move { s.ensure_timeline("same.timeline").await.unwrap() }
    });

    let (route_a, route_b) = tokio::join!(h1, h2);
    let route_a = route_a.unwrap();
    let route_b = route_b.unwrap();

    assert_eq!(route_a.timeline_key, route_b.timeline_key);
    assert_eq!(route_a.generator_id, route_b.generator_id);
    assert_eq!(route_a.epoch, route_b.epoch);
    assert_eq!(route_a.route_version, route_b.route_version);
    assert_eq!(route_a.owner_worker_endpoint, route_b.owner_worker_endpoint);
}

#[tokio::test]
async fn reused_endpoint_does_not_bypass_instance_fencing() {
    let base = TsoConfig {
        shared_generators: 4,
        warm_generators: 0,
        max_batch_per_request: 8,
        max_future_borrow_ms: 10000,
        default_resource_tier: ResourceTier::Shared,
        ..TsoConfig::default()
    };
    let clock = Arc::new(ManualClock::new(22_000));
    let metadata = Arc::new(MemoryMetadataStore::new());

    let shared_owner = with_worker(base.clone(), "shared-owner");
    let service_a = TsoService::new(shared_owner.clone(), clock.clone(), metadata.clone()).unwrap();
    let service_b = TsoService::new(shared_owner, clock, metadata).unwrap();

    let route_a = service_a
        .ensure_timeline("concurrent.shared.timeline")
        .await
        .unwrap();
    let route_b = service_b
        .get_timeline_route("concurrent.shared.timeline")
        .await
        .unwrap();

    let success = service_a
        .allocate_timestamps(request(&route_a, "owner-a".to_owned(), 4))
        .await
        .unwrap();
    assert_eq!(success.ranges.len(), 1);

    let rejected = service_b
        .allocate_timestamps(request(&route_b, "owner-b".to_owned(), 1))
        .await;
    match rejected {
        Err(TsoError::LeaseExpired { .. }) | Err(TsoError::NotGeneratorOwner { .. }) => {}
        other => panic!("unexpected fencing result: {:?}", other),
    }
}

#[tokio::test]
async fn different_worker_cannot_allocate_timeline_without_transfer() {
    let clock = Arc::new(ManualClock::new(23_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let (service_a, service_b) = memory_two_worker_services(TsoConfig::default(), clock, metadata);

    let route = service_a
        .ensure_timeline("owner-fence.timeline")
        .await
        .unwrap();
    let route_from_b = service_b
        .get_timeline_route("owner-fence.timeline")
        .await
        .unwrap();
    assert_eq!(route.owner_worker_endpoint, worker_endpoint("worker-a"));
    assert_eq!(
        route_from_b.owner_worker_endpoint,
        worker_endpoint("worker-a")
    );

    let result = service_b
        .allocate_timestamps(request(&route_from_b, "not-owner".to_owned(), 1))
        .await;
    match result {
        Err(TsoError::NotTimelineOwner {
            owner_worker_endpoint,
            ..
        }) => {
            assert_eq!(owner_worker_endpoint, worker_endpoint("worker-a"));
        }
        other => panic!("unexpected not owner result: {:?}", other),
    }
}

#[tokio::test]
async fn expired_lease_requires_failover_before_issuing_more_tsos() {
    let base = TsoConfig {
        lease_ttl_ms: 5,
        generator_maintenance_interval_ms: 1,
        max_future_borrow_ms: 10000,
        ..TsoConfig::default()
    };
    let shared_generators = base.shared_generators;
    let clock = Arc::new(ManualClock::new(24_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service_a = TsoService::new(
        with_worker(base.clone(), "worker-a"),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();
    let service_b =
        TsoService::new(with_worker(base, "worker-b"), clock.clone(), metadata).unwrap();

    let route_a = service_a
        .ensure_timeline("lease-failover.timeline")
        .await
        .unwrap();
    let first = service_a
        .allocate_timestamps(request(&route_a, "before-expire".to_owned(), 2))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;

    clock.advance(10); // Definitely past 5ms lease
    let expired = service_a
        .allocate_timestamps(request(&route_a, "after-expire".to_owned(), 1))
        .await;
    match expired {
        Err(TsoError::LeaseExpired { .. }) => {}
        other => panic!("unexpected expired lease result: {:?}", other),
    }

    let target_generator_id = (route_a.generator_id + 1) % shared_generators;
    let transferred = service_b
        .transfer_timeline(
            &route_a.timeline_key,
            route_a.resource_tier,
            worker_endpoint("worker-b"),
            Some(target_generator_id),
        )
        .await
        .unwrap();
    assert_eq!(
        transferred.owner_worker_endpoint,
        worker_endpoint("worker-b")
    );

    let second = service_b
        .allocate_timestamps(request(&transferred, "after-failover".to_owned(), 1))
        .await
        .unwrap();
    assert!(first_last < second.ranges[0].start_tso);
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_expired_lease_requires_failover_before_issuing_more_tsos() {
    let base = TsoConfig {
        lease_ttl_ms: 5,
        generator_maintenance_interval_ms: 1,
        max_future_borrow_ms: 10_000,
        ..TsoConfig::default()
    };
    let shared_generators = base.shared_generators;
    let clock = Arc::new(ManualClock::new(24_000));
    let prefix = unique_test_etcd_prefix("lease-failover");
    let service_a = TsoService::new(
        with_worker(base.clone(), "worker-a"),
        clock.clone(),
        real_etcd_store(&prefix).await,
    )
    .unwrap();
    let service_b = TsoService::new(
        with_worker(base, "worker-b"),
        clock.clone(),
        real_etcd_store(&prefix).await,
    )
    .unwrap();

    let route_a = service_a
        .ensure_timeline("lease-failover.timeline")
        .await
        .unwrap();
    let first = service_a
        .allocate_timestamps(request(&route_a, "before-expire".to_owned(), 2))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;

    clock.advance(10);
    let expired = service_a
        .allocate_timestamps(request(&route_a, "after-expire".to_owned(), 1))
        .await;
    match expired {
        Err(TsoError::LeaseExpired { .. }) => {}
        other => panic!("unexpected expired lease result: {:?}", other),
    }

    let target_generator_id = (route_a.generator_id + 1) % shared_generators;
    let transferred = service_b
        .transfer_timeline(
            &route_a.timeline_key,
            route_a.resource_tier,
            worker_endpoint("worker-b"),
            Some(target_generator_id),
        )
        .await
        .unwrap();
    assert_eq!(
        transferred.owner_worker_endpoint,
        worker_endpoint("worker-b")
    );

    let second = service_b
        .allocate_timestamps(request(&transferred, "after-failover".to_owned(), 1))
        .await
        .unwrap();
    assert!(first_last < second.ranges[0].start_tso);
}

#[tokio::test]
async fn failover_is_blocked_until_expiry_plus_safety_gap_with_skewed_clocks() {
    let safety_gap_ms = 5;
    let base = TsoConfig {
        lease_ttl_ms: 5,
        generator_maintenance_interval_ms: 1,
        safety_gap_ms,
        max_future_borrow_ms: 10_000,
        ..TsoConfig::default()
    };
    let shared_generators = base.shared_generators;
    let clock_a = Arc::new(ManualClock::new(25_000));
    let clock_b = Arc::new(ManualClock::new(25_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service_a = TsoService::new(
        with_worker(base.clone(), "worker-a"),
        clock_a.clone(),
        metadata.clone(),
    )
    .unwrap();
    let service_b =
        TsoService::new(with_worker(base, "worker-b"), clock_b.clone(), metadata).unwrap();

    let route_a = service_a
        .ensure_timeline("lease-failover-skew.timeline")
        .await
        .unwrap();
    let first = service_a
        .allocate_timestamps(request(&route_a, "before-skewed-failover".to_owned(), 2))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;
    let lease_expire_at_ms = service_a
        .load_generator_record(route_a.generator_id)
        .await
        .unwrap()
        .lease_expire_at_ms
        .unwrap();

    let target_generator_id = (route_a.generator_id + 1) % shared_generators;

    clock_b.set(lease_expire_at_ms + safety_gap_ms - 1);
    let blocked = service_b
        .control_plane()
        .transfer_timeline_for_rpc(
            &route_a.timeline_key,
            worker_endpoint("worker-b"),
            Some(target_generator_id),
            TransferReason::Failover,
        )
        .await;
    match blocked {
        Err(TsoError::FailoverRequiresExpiredLease {
            timeline_key,
            lease_expire_at_ms: actual_expire_at_ms,
        }) => {
            assert_eq!(timeline_key, route_a.timeline_key);
            assert_eq!(actual_expire_at_ms, lease_expire_at_ms);
        }
        other => panic!("unexpected skew-blocked failover result: {:?}", other),
    }

    clock_b.set(lease_expire_at_ms + safety_gap_ms);
    let transferred = service_b
        .control_plane()
        .transfer_timeline_for_rpc(
            &route_a.timeline_key,
            worker_endpoint("worker-b"),
            Some(target_generator_id),
            TransferReason::Failover,
        )
        .await
        .unwrap()
        .0;

    let second = service_b
        .allocate_timestamps(request(&transferred, "after-skewed-failover".to_owned(), 1))
        .await
        .unwrap();
    assert!(first_last < second.ranges[0].start_tso);
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_failover_is_blocked_until_expiry_plus_safety_gap_with_skewed_clocks() {
    let safety_gap_ms = 5;
    let base = TsoConfig {
        lease_ttl_ms: 5,
        generator_maintenance_interval_ms: 1,
        safety_gap_ms,
        max_future_borrow_ms: 10_000,
        ..TsoConfig::default()
    };
    let shared_generators = base.shared_generators;
    let clock_a = Arc::new(ManualClock::new(25_000));
    let clock_b = Arc::new(ManualClock::new(25_000));
    let prefix = unique_test_etcd_prefix("lease-failover-skew");
    let service_a = TsoService::new(
        with_worker(base.clone(), "worker-a"),
        clock_a.clone(),
        real_etcd_store(&prefix).await,
    )
    .unwrap();
    let service_b = TsoService::new(
        with_worker(base, "worker-b"),
        clock_b.clone(),
        real_etcd_store(&prefix).await,
    )
    .unwrap();

    let route_a = service_a
        .ensure_timeline("lease-failover-skew.timeline")
        .await
        .unwrap();
    let first = service_a
        .allocate_timestamps(request(&route_a, "before-skewed-failover".to_owned(), 2))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;
    let lease_expire_at_ms = service_a
        .load_generator_record(route_a.generator_id)
        .await
        .unwrap()
        .lease_expire_at_ms
        .unwrap();

    let target_generator_id = (route_a.generator_id + 1) % shared_generators;

    clock_b.set(lease_expire_at_ms + safety_gap_ms - 1);
    let blocked = service_b
        .control_plane()
        .transfer_timeline_for_rpc(
            &route_a.timeline_key,
            worker_endpoint("worker-b"),
            Some(target_generator_id),
            TransferReason::Failover,
        )
        .await;
    match blocked {
        Err(TsoError::FailoverRequiresExpiredLease {
            timeline_key,
            lease_expire_at_ms: actual_expire_at_ms,
        }) => {
            assert_eq!(timeline_key, route_a.timeline_key);
            assert_eq!(actual_expire_at_ms, lease_expire_at_ms);
        }
        other => panic!("unexpected etcd skew-blocked failover result: {:?}", other),
    }

    clock_b.set(lease_expire_at_ms + safety_gap_ms);
    let transferred = service_b
        .control_plane()
        .transfer_timeline_for_rpc(
            &route_a.timeline_key,
            worker_endpoint("worker-b"),
            Some(target_generator_id),
            TransferReason::Failover,
        )
        .await
        .unwrap()
        .0;

    let second = service_b
        .allocate_timestamps(request(&transferred, "after-skewed-failover".to_owned(), 1))
        .await
        .unwrap();
    assert!(first_last < second.ranges[0].start_tso);
}

#[tokio::test]
async fn multihop_failover_at_capacity_horizon_uses_upper_bound_and_fails_closed() {
    let pre_borrow_ms = 1_000;
    let now_ms = MAX_PHYSICAL_MS - pre_borrow_ms;
    let base = TsoConfig {
        lease_ttl_ms: 20_000,
        generator_maintenance_interval_ms: 10_000,
        max_future_borrow_ms: 10_000,
        pre_borrow_ms,
        generator_ownership_modulo: 2,
        generator_ownership_remainder: 0,
        shared_generators: 32,
        ..TsoConfig::default()
    };
    let metadata = Arc::new(MemoryMetadataStore::new());
    let clock = Arc::new(ManualClock::new(now_ms));

    let service_a = TsoService::new(
        with_worker(base.clone(), "worker-a"),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();
    let service_b = TsoService::new(
        with_worker(
            TsoConfig {
                generator_ownership_modulo: 2,
                generator_ownership_remainder: 1,
                ..base
            },
            "worker-b",
        ),
        clock,
        metadata.clone(),
    )
    .unwrap();

    let route_a = service_a
        .ensure_timeline("horizon.failover.timeline")
        .await
        .unwrap();
    let first = service_a
        .allocate_timestamps(request(&route_a, "before-horizon-hop-1".to_owned(), 2))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;
    let a_upper_bound = encode_tso(MAX_PHYSICAL_MS, route_a.generator_id, SEQUENCE_CAPACITY - 1)
        .expect("horizon upper bound should encode");

    overwrite_generator_record(&metadata, route_a.generator_id, |record| {
        record.last_issued_tso = Some(first_last);
        record.issued_upper_bound = Some(a_upper_bound);
        record.lease_expire_at_ms = Some(now_ms - 1);
    })
    .await;

    let generator_b = route_a.generator_id + 1;
    let transferred_b = service_b
        .control_plane()
        .transfer_timeline_for_rpc(
            &route_a.timeline_key,
            worker_endpoint("worker-b"),
            Some(generator_b),
            TransferReason::Failover,
        )
        .await
        .unwrap()
        .0;

    let persisted_after_hop_1 = service_b
        .get_timeline_record(&route_a.timeline_key)
        .await
        .unwrap();
    let recovery_floor_hop_1 = persisted_after_hop_1
        .recovery_floor_tso
        .expect("failover should persist a recovery floor");
    assert_eq!(recovery_floor_hop_1, a_upper_bound);
    assert_eq!(
        decode_tso(recovery_floor_hop_1).physical_ms,
        MAX_PHYSICAL_MS
    );
    assert_eq!(persisted_after_hop_1.issued_upper_bound, None);

    let after_hop_1 = service_b
        .allocate_timestamps(request(&transferred_b, "after-horizon-hop-1".to_owned(), 1))
        .await
        .unwrap();
    let b_first = after_hop_1.ranges[0].start_tso;
    let b_first_decoded = decode_tso(b_first);
    assert_eq!(b_first_decoded.physical_ms, MAX_PHYSICAL_MS);
    assert_eq!(b_first_decoded.generator_id, generator_b);
    assert!(first_last < b_first);

    let b_upper_bound = encode_tso(MAX_PHYSICAL_MS, generator_b, SEQUENCE_CAPACITY - 1)
        .expect("horizon upper bound should encode");
    overwrite_generator_record(&metadata, generator_b, |record| {
        record.issued_upper_bound = Some(b_upper_bound);
        record.lease_expire_at_ms = Some(now_ms - 1);
    })
    .await;

    let transferred_a = service_a
        .control_plane()
        .transfer_timeline_for_rpc(
            &route_a.timeline_key,
            worker_endpoint("worker-a"),
            Some(route_a.generator_id),
            TransferReason::Failover,
        )
        .await
        .unwrap()
        .0;

    let persisted_after_hop_2 = service_a
        .get_timeline_record(&route_a.timeline_key)
        .await
        .unwrap();
    let recovery_floor_hop_2 = persisted_after_hop_2
        .recovery_floor_tso
        .expect("second hop should persist a recovery floor");
    assert_eq!(recovery_floor_hop_2, b_upper_bound);
    assert!(recovery_floor_hop_1 < recovery_floor_hop_2);

    let error = service_a
        .allocate_timestamps(request(&transferred_a, "after-horizon-hop-2".to_owned(), 1))
        .await
        .unwrap_err();
    assert!(matches!(error, TsoError::TsoOverflow));
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_multihop_failover_at_capacity_horizon_uses_upper_bound_and_fails_closed() {
    let pre_borrow_ms = 1_000;
    let now_ms = MAX_PHYSICAL_MS - pre_borrow_ms;
    let base = TsoConfig {
        lease_ttl_ms: 20_000,
        generator_maintenance_interval_ms: 10_000,
        max_future_borrow_ms: 10_000,
        pre_borrow_ms,
        generator_ownership_modulo: 2,
        generator_ownership_remainder: 0,
        shared_generators: 32,
        ..TsoConfig::default()
    };
    let prefix = unique_test_etcd_prefix("horizon-multihop");
    let clock = Arc::new(ManualClock::new(now_ms));

    let service_a = TsoService::new(
        with_worker(base.clone(), "worker-a"),
        clock.clone(),
        real_etcd_store(&prefix).await,
    )
    .unwrap();
    let service_b = TsoService::new(
        with_worker(
            TsoConfig {
                generator_ownership_modulo: 2,
                generator_ownership_remainder: 1,
                ..base
            },
            "worker-b",
        ),
        clock,
        real_etcd_store(&prefix).await,
    )
    .unwrap();

    let route_a = service_a
        .ensure_timeline("horizon.failover.timeline")
        .await
        .unwrap();
    let first = service_a
        .allocate_timestamps(request(&route_a, "before-horizon-hop-1".to_owned(), 2))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;
    let a_upper_bound = encode_tso(MAX_PHYSICAL_MS, route_a.generator_id, SEQUENCE_CAPACITY - 1)
        .expect("horizon upper bound should encode");

    let metadata_a = real_etcd_store(&prefix).await;
    overwrite_generator_record(&metadata_a, route_a.generator_id, |record| {
        record.last_issued_tso = Some(first_last);
        record.issued_upper_bound = Some(a_upper_bound);
        record.lease_expire_at_ms = Some(now_ms - 1);
    })
    .await;

    let generator_b = route_a.generator_id + 1;
    let transferred_b = service_b
        .control_plane()
        .transfer_timeline_for_rpc(
            &route_a.timeline_key,
            worker_endpoint("worker-b"),
            Some(generator_b),
            TransferReason::Failover,
        )
        .await
        .unwrap()
        .0;

    let persisted_after_hop_1 = service_b
        .get_timeline_record(&route_a.timeline_key)
        .await
        .unwrap();
    let recovery_floor_hop_1 = persisted_after_hop_1
        .recovery_floor_tso
        .expect("failover should persist a recovery floor");
    assert_eq!(recovery_floor_hop_1, a_upper_bound);
    assert_eq!(
        decode_tso(recovery_floor_hop_1).physical_ms,
        MAX_PHYSICAL_MS
    );
    assert_eq!(persisted_after_hop_1.issued_upper_bound, None);

    let after_hop_1 = service_b
        .allocate_timestamps(request(&transferred_b, "after-horizon-hop-1".to_owned(), 1))
        .await
        .unwrap();
    let b_first = after_hop_1.ranges[0].start_tso;
    let b_first_decoded = decode_tso(b_first);
    assert_eq!(b_first_decoded.physical_ms, MAX_PHYSICAL_MS);
    assert_eq!(b_first_decoded.generator_id, generator_b);
    assert!(first_last < b_first);

    let b_upper_bound = encode_tso(MAX_PHYSICAL_MS, generator_b, SEQUENCE_CAPACITY - 1)
        .expect("horizon upper bound should encode");
    overwrite_generator_record(&metadata_a, generator_b, |record| {
        record.issued_upper_bound = Some(b_upper_bound);
        record.lease_expire_at_ms = Some(now_ms - 1);
    })
    .await;

    let transferred_a = service_a
        .control_plane()
        .transfer_timeline_for_rpc(
            &route_a.timeline_key,
            worker_endpoint("worker-a"),
            Some(route_a.generator_id),
            TransferReason::Failover,
        )
        .await
        .unwrap()
        .0;

    let persisted_after_hop_2 = service_a
        .get_timeline_record(&route_a.timeline_key)
        .await
        .unwrap();
    let recovery_floor_hop_2 = persisted_after_hop_2
        .recovery_floor_tso
        .expect("second hop should persist a recovery floor");
    assert_eq!(recovery_floor_hop_2, b_upper_bound);
    assert!(recovery_floor_hop_1 < recovery_floor_hop_2);

    let error = service_a
        .allocate_timestamps(request(&transferred_a, "after-horizon-hop-2".to_owned(), 1))
        .await
        .unwrap_err();
    assert!(matches!(error, TsoError::TsoOverflow));
}

#[tokio::test]
async fn failover_recovery_floor_survives_remote_restart_before_first_allocation() {
    let base = TsoConfig {
        lease_ttl_ms: 5,
        generator_maintenance_interval_ms: 1,
        max_future_borrow_ms: 10_000,
        ..TsoConfig::default()
    };
    let shared_generators = base.shared_generators;
    let clock = Arc::new(ManualClock::new(26_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let (service_a, _) = memory_two_worker_services(base.clone(), clock.clone(), metadata.clone());

    let route_a = service_a
        .ensure_timeline("failover.restart.floor.timeline")
        .await
        .unwrap();
    let first = service_a
        .allocate_timestamps(request(&route_a, "before-failover-restart".to_owned(), 1))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;
    let previous_generator = service_a
        .load_generator_record(route_a.generator_id)
        .await
        .unwrap();
    let previous_upper_bound = previous_generator.issued_upper_bound.unwrap();

    clock.advance(10);
    let target_generator_id = (route_a.generator_id + 1) % shared_generators;
    let transferred = service_a
        .control_plane()
        .transfer_timeline_for_rpc(
            &route_a.timeline_key,
            worker_endpoint("worker-b"),
            Some(target_generator_id),
            TransferReason::Failover,
        )
        .await
        .unwrap()
        .0;

    let persisted = service_a
        .get_timeline_record(&route_a.timeline_key)
        .await
        .unwrap();
    let persisted_recovery_floor = persisted
        .recovery_floor_tso
        .expect("failover should persist a recovery floor");
    assert!(previous_upper_bound <= persisted_recovery_floor);

    let service_b = TsoService::new(with_worker(base, "worker-b"), clock, metadata).unwrap();
    let route_b = service_b
        .get_timeline_route("failover.restart.floor.timeline")
        .await
        .unwrap();
    assert_eq!(route_b.route_version, transferred.route_version);

    let after = service_b
        .allocate_timestamps(request(&route_b, "after-failover-restart".to_owned(), 1))
        .await
        .unwrap();
    let resumed = after.ranges[0].start_tso;

    assert!(first_last < resumed);
    assert!(persisted_recovery_floor < resumed);
}

#[tokio::test]
async fn failover_recovery_floor_uses_recovery_catchup_budget_when_future_borrow_is_tight() {
    let base = TsoConfig {
        lease_ttl_ms: 5,
        generator_lease_ttl_ms: 5,
        generator_maintenance_interval_ms: 1,
        max_future_borrow_ms: 10,
        recovery_catchup_budget_ms: 2_000,
        ..TsoConfig::default()
    };
    let shared_generators = base.shared_generators;
    let metadata = Arc::new(MemoryMetadataStore::new());
    let mut config_a = with_worker(base.clone(), "worker-a");
    config_a.max_future_borrow_ms = base.max_future_borrow_ms;
    config_a.recovery_catchup_budget_ms = base.recovery_catchup_budget_ms;
    let mut config_b = with_worker(base.clone(), "worker-b");
    config_b.max_future_borrow_ms = base.max_future_borrow_ms;
    config_b.recovery_catchup_budget_ms = base.recovery_catchup_budget_ms;

    let fast_clock = Arc::new(ManualClock::new(30_000));
    let service_a = TsoService::new(config_a, fast_clock.clone(), metadata.clone()).unwrap();

    let route_a = service_a
        .ensure_timeline("failover.recovery.catchup.timeline")
        .await
        .unwrap();
    let before = service_a
        .allocate_timestamps(request(&route_a, "before-catchup-failover".to_owned(), 1))
        .await
        .unwrap();
    let before_last = before.ranges.last().unwrap().end_tso;

    fast_clock.advance(1_000);
    let target_generator_id = (route_a.generator_id + 1) % shared_generators;
    let transferred = service_a
        .control_plane()
        .transfer_timeline_for_rpc(
            &route_a.timeline_key,
            worker_endpoint("worker-b"),
            Some(target_generator_id),
            TransferReason::Failover,
        )
        .await
        .unwrap()
        .0;

    let slow_clock = Arc::new(ManualClock::new(30_050));
    let service_b = TsoService::new(config_b, slow_clock, metadata).unwrap();
    let after = service_b
        .allocate_timestamps(request(
            &transferred,
            "after-catchup-failover".to_owned(),
            1,
        ))
        .await
        .unwrap();
    let resumed = after.ranges[0].start_tso;

    assert!(before_last < resumed);
}

#[tokio::test]
async fn failover_rejects_transfer_beyond_recovery_catchup_budget() {
    let base = TsoConfig {
        lease_ttl_ms: 5,
        generator_lease_ttl_ms: 5,
        generator_maintenance_interval_ms: 1,
        max_future_borrow_ms: 10,
        recovery_catchup_budget_ms: 50,
        ..TsoConfig::default()
    };
    let shared_generators = base.shared_generators;
    let metadata = Arc::new(MemoryMetadataStore::new());
    let mut config_a = with_worker(base.clone(), "worker-a");
    config_a.max_future_borrow_ms = base.max_future_borrow_ms;
    config_a.recovery_catchup_budget_ms = base.recovery_catchup_budget_ms;
    let mut config_b = with_worker(base.clone(), "worker-b");
    config_b.max_future_borrow_ms = base.max_future_borrow_ms;
    config_b.recovery_catchup_budget_ms = base.recovery_catchup_budget_ms;
    let fast_clock = Arc::new(ManualClock::new(31_000));
    let service_a = TsoService::new(config_a, fast_clock.clone(), metadata.clone()).unwrap();

    let route_a = service_a
        .ensure_timeline("failover.recovery.catchup.limit")
        .await
        .unwrap();
    service_a
        .allocate_timestamps(request(&route_a, "before-catchup-limit".to_owned(), 1))
        .await
        .unwrap();

    fast_clock.advance(1_000);
    let forced_upper_bound = encode_tso(32_000, route_a.generator_id, SEQUENCE_CAPACITY - 1)
        .expect("forced recovery floor should encode");
    overwrite_generator_record(&metadata, route_a.generator_id, |record| {
        record.issued_upper_bound = Some(forced_upper_bound);
        record.lease_expire_at_ms = Some(31_999);
    })
    .await;
    let target_generator_id = (route_a.generator_id + 1) % shared_generators;
    let transferred = service_a
        .control_plane()
        .transfer_timeline_for_rpc(
            &route_a.timeline_key,
            worker_endpoint("worker-b"),
            Some(target_generator_id),
            TransferReason::Failover,
        )
        .await
        .unwrap()
        .0;

    let slow_clock = Arc::new(ManualClock::new(31_050));
    let service_b = TsoService::new(config_b, slow_clock, metadata).unwrap();
    let error = service_b
        .allocate_timestamps(request(&transferred, "after-catchup-limit".to_owned(), 1))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        TsoError::TimelineNotReady {
            state: TimelineLifecycleState::Recovering,
            ..
        }
    ));
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_failover_recovery_floor_survives_remote_restart_before_first_allocation() {
    let base = TsoConfig {
        lease_ttl_ms: 5,
        generator_maintenance_interval_ms: 1,
        max_future_borrow_ms: 10_000,
        ..TsoConfig::default()
    };
    let shared_generators = base.shared_generators;
    let clock = Arc::new(ManualClock::new(26_000));
    let prefix = unique_test_etcd_prefix("failover-restart-floor");

    let service_a = TsoService::new(
        with_worker(base.clone(), "worker-a"),
        clock.clone(),
        real_etcd_store(&prefix).await,
    )
    .unwrap();

    let route_a = service_a
        .ensure_timeline("failover.restart.floor.timeline")
        .await
        .unwrap();
    let first = service_a
        .allocate_timestamps(request(&route_a, "before-failover-restart".to_owned(), 1))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;
    let previous_generator = service_a
        .load_generator_record(route_a.generator_id)
        .await
        .unwrap();
    let previous_upper_bound = previous_generator.issued_upper_bound.unwrap();

    clock.advance(10);
    let target_generator_id = (route_a.generator_id + 1) % shared_generators;
    let transferred = service_a
        .control_plane()
        .transfer_timeline_for_rpc(
            &route_a.timeline_key,
            worker_endpoint("worker-b"),
            Some(target_generator_id),
            TransferReason::Failover,
        )
        .await
        .unwrap()
        .0;

    let persisted = service_a
        .get_timeline_record(&route_a.timeline_key)
        .await
        .unwrap();
    let persisted_recovery_floor = persisted
        .recovery_floor_tso
        .expect("failover should persist a recovery floor");
    assert!(previous_upper_bound <= persisted_recovery_floor);

    let service_b = TsoService::new(
        with_worker(base, "worker-b"),
        clock,
        real_etcd_store(&prefix).await,
    )
    .unwrap();
    let route_b = service_b
        .get_timeline_route("failover.restart.floor.timeline")
        .await
        .unwrap();
    assert_eq!(route_b.route_version, transferred.route_version);

    let after = service_b
        .allocate_timestamps(request(&route_b, "after-failover-restart".to_owned(), 1))
        .await
        .unwrap();
    let resumed = after.ranges[0].start_tso;

    assert!(first_last < resumed);
    assert!(persisted_recovery_floor < resumed);
}

#[tokio::test]
async fn remote_transfer_recovery_floor_uses_max_of_graceful_and_generator_floor() {
    let base = TsoConfig {
        lease_ttl_ms: 100,
        generator_maintenance_interval_ms: 10,
        max_future_borrow_ms: 10_000,
        ..TsoConfig::default()
    };
    let shared_generators = base.shared_generators;
    let clock = Arc::new(ManualClock::new(27_000));
    let metadata = Arc::new(MemoryMetadataStore::new());

    let service_a = TsoService::new(
        with_worker(base.clone(), "worker-a"),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();

    let route_a = service_a
        .ensure_timeline("transfer.restart.floor.timeline")
        .await
        .unwrap();
    let first = service_a
        .allocate_timestamps(request(&route_a, "before-transfer-restart".to_owned(), 1))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;
    let previous_generator = service_a
        .load_generator_record(route_a.generator_id)
        .await
        .unwrap();
    let previous_upper_bound = previous_generator.issued_upper_bound.unwrap();

    let target_generator_id = (route_a.generator_id + 1) % shared_generators;
    let transferred = service_a
        .transfer_timeline(
            &route_a.timeline_key,
            route_a.resource_tier,
            worker_endpoint("worker-b"),
            Some(target_generator_id),
        )
        .await
        .unwrap();

    let persisted = service_a
        .get_timeline_record(&route_a.timeline_key)
        .await
        .unwrap();
    assert_eq!(persisted.last_graceful_issued, Some(first_last));
    let persisted_recovery_floor = persisted
        .recovery_floor_tso
        .expect("transfer should persist a recovery floor");
    assert!(previous_upper_bound <= persisted_recovery_floor);

    let service_b = TsoService::new(with_worker(base, "worker-b"), clock, metadata).unwrap();
    let route_b = service_b
        .get_timeline_route("transfer.restart.floor.timeline")
        .await
        .unwrap();
    assert_eq!(route_b.route_version, transferred.route_version);

    let after = service_b
        .allocate_timestamps(request(&route_b, "after-transfer-restart".to_owned(), 1))
        .await
        .unwrap();
    let resumed = after.ranges[0].start_tso;

    assert!(first_last < resumed);
    assert!(persisted_recovery_floor < resumed);
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_remote_transfer_recovery_floor_uses_max_of_graceful_and_generator_floor() {
    let base = TsoConfig {
        lease_ttl_ms: 100,
        generator_maintenance_interval_ms: 10,
        max_future_borrow_ms: 10_000,
        ..TsoConfig::default()
    };
    let shared_generators = base.shared_generators;
    let clock = Arc::new(ManualClock::new(27_000));
    let prefix = unique_test_etcd_prefix("transfer-restart-floor");

    let service_a = TsoService::new(
        with_worker(base.clone(), "worker-a"),
        clock.clone(),
        real_etcd_store(&prefix).await,
    )
    .unwrap();

    let route_a = service_a
        .ensure_timeline("transfer.restart.floor.timeline")
        .await
        .unwrap();
    let first = service_a
        .allocate_timestamps(request(&route_a, "before-transfer-restart".to_owned(), 1))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;
    let previous_generator = service_a
        .load_generator_record(route_a.generator_id)
        .await
        .unwrap();
    let previous_upper_bound = previous_generator.issued_upper_bound.unwrap();

    let target_generator_id = (route_a.generator_id + 1) % shared_generators;
    let transferred = service_a
        .transfer_timeline(
            &route_a.timeline_key,
            route_a.resource_tier,
            worker_endpoint("worker-b"),
            Some(target_generator_id),
        )
        .await
        .unwrap();

    let persisted = service_a
        .get_timeline_record(&route_a.timeline_key)
        .await
        .unwrap();
    assert_eq!(persisted.last_graceful_issued, Some(first_last));
    let persisted_recovery_floor = persisted
        .recovery_floor_tso
        .expect("transfer should persist a recovery floor");
    assert!(previous_upper_bound <= persisted_recovery_floor);

    let service_b = TsoService::new(
        with_worker(base, "worker-b"),
        clock,
        real_etcd_store(&prefix).await,
    )
    .unwrap();
    let route_b = service_b
        .get_timeline_route("transfer.restart.floor.timeline")
        .await
        .unwrap();
    assert_eq!(route_b.route_version, transferred.route_version);

    let after = service_b
        .allocate_timestamps(request(&route_b, "after-transfer-restart".to_owned(), 1))
        .await
        .unwrap();
    let resumed = after.ranges[0].start_tso;

    assert!(first_last < resumed);
    assert!(persisted_recovery_floor < resumed);
}

#[tokio::test]
async fn failover_requires_persisted_previous_generator_floor() {
    let base = TsoConfig {
        lease_ttl_ms: 5,
        generator_maintenance_interval_ms: 1,
        max_future_borrow_ms: 10_000,
        ..TsoConfig::default()
    };
    let shared_generators = base.shared_generators;
    let clock = Arc::new(ManualClock::new(28_000));
    let metadata = Arc::new(MemoryMetadataStore::new());

    let service_a = TsoService::new(
        with_worker(base.clone(), "worker-a"),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();
    let service_b = TsoService::new(
        with_worker(base, "worker-b"),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();

    let route = service_a
        .ensure_timeline("failover.missing.floor.timeline")
        .await
        .unwrap();

    let (mut generator_record, revision) = metadata
        .load_generator(route.generator_id)
        .await
        .unwrap()
        .unwrap();
    generator_record.last_issued_tso = None;
    generator_record.issued_upper_bound = None;
    metadata
        .compare_exchange_generator(route.generator_id, revision, &generator_record)
        .await
        .unwrap();

    clock.advance(10);
    let result = service_b
        .control_plane()
        .transfer_timeline_for_rpc(
            &route.timeline_key,
            worker_endpoint("worker-b"),
            Some((route.generator_id + 1) % shared_generators),
            TransferReason::Failover,
        )
        .await;

    match result {
        Err(TsoError::FailoverMissingRecoveryFloor {
            timeline_key,
            generator_id,
        }) => {
            assert_eq!(timeline_key, route.timeline_key);
            assert_eq!(generator_id, route.generator_id);
        }
        other => panic!("unexpected failover result: {:?}", other),
    }
}

#[tokio::test]
async fn timeline_runtime_cache_respects_capacity() {
    let config = with_worker(
        TsoConfig {
            max_timeline_runtime_entries: 1,
            ..TsoConfig::default()
        },
        "worker-a",
    );
    let clock = Arc::new(ManualClock::new(29_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(required_test_config(config), clock, metadata).unwrap();

    let first = service.ensure_timeline("runtime-cap.a").await.unwrap();
    assert_eq!(service.health().timeline_count, 1);

    let second = service.ensure_timeline("runtime-cap.b").await.unwrap();
    assert_eq!(service.health().timeline_count, 1);

    assert_ne!(first.timeline_key, second.timeline_key);
}

#[tokio::test]
async fn evicted_timeline_reloads_without_tso_regression() {
    let config = with_worker(
        TsoConfig {
            max_timeline_runtime_entries: 1,
            ..TsoConfig::default()
        },
        "worker-a",
    );
    let clock = Arc::new(ManualClock::new(30_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(required_test_config(config), clock, metadata).unwrap();

    let route_a = service.ensure_timeline("runtime-evict.a").await.unwrap();
    let first = service
        .allocate_timestamps(request(&route_a, "runtime-evict-a-1".to_owned(), 4))
        .await
        .unwrap();
    let first_last = first.ranges.last().unwrap().end_tso;
    assert_eq!(service.health().timeline_count, 1);

    let route_b = service.ensure_timeline("runtime-evict.b").await.unwrap();
    let _ = service
        .allocate_timestamps(request(&route_b, "runtime-evict-b-1".to_owned(), 1))
        .await
        .unwrap();
    assert_eq!(service.health().timeline_count, 1);

    let second = service
        .allocate_timestamps(request(&route_a, "runtime-evict-a-2".to_owned(), 2))
        .await
        .unwrap();
    let second_first = second.ranges.first().unwrap().start_tso;

    assert!(first_last < second_first);
    assert_eq!(service.health().timeline_count, 1);
}

#[tokio::test]
async fn owner_can_renew_lease_before_expiry() {
    let config = with_worker(
        TsoConfig {
            lease_ttl_ms: 100,
            generator_maintenance_interval_ms: 10,
            ..TsoConfig::default()
        },
        "worker-a",
    );
    let clock = Arc::new(ManualClock::new(25_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(required_test_config(config), clock.clone(), metadata).unwrap();

    let route = service.ensure_timeline("renew.timeline").await.unwrap();
    let before = service
        .load_generator_record(route.generator_id)
        .await
        .unwrap();
    clock.advance(10);
    service
        .renew_timeline_lease("renew.timeline")
        .await
        .unwrap();
    let after = service
        .load_generator_record(route.generator_id)
        .await
        .unwrap();

    assert!(after.lease_expire_at_ms.unwrap() > before.lease_expire_at_ms.unwrap());
    assert_eq!(after.owner_worker_endpoint, worker_endpoint("worker-a"));
}
