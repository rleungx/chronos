use super::*;

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
