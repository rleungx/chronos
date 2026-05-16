use super::*;

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
