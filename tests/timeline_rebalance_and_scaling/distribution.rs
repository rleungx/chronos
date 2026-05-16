use super::*;

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
        max_batch_per_request: 256,
        max_future_borrow_ms: 10000,
        lease_ttl_ms: 1_000_000,
        generator_lease_ttl_ms: 1_000_000,
        generator_maintenance_interval_ms: 1_000,
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
