use super::*;

#[tokio::test]
async fn allocation_recovering_local_timeline_activates_and_upserts_cache() {
    let clock = Arc::new(ManualClock::new(22_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(
        required_test_config(TsoConfig::default()),
        clock,
        metadata.clone(),
    )
    .unwrap();

    let route = service
        .ensure_timeline("allocation.recovering.timeline")
        .await
        .unwrap();
    let (mut record, revision) = metadata
        .load_timeline(&route.timeline_key)
        .await
        .unwrap()
        .unwrap();
    record.state = TimelineLifecycleState::Recovering;
    let recovering_revision = metadata
        .compare_exchange_timeline(&route.timeline_key, revision, &record)
        .await
        .unwrap();
    service.clear_timeline_cache(&route.timeline_key);

    let response = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "recovering-local-allocate".to_string(),
        })
        .await
        .unwrap();
    let allocated_tso = response.ranges.last().unwrap().end_tso;

    let (persisted, persisted_revision) = metadata
        .load_timeline(&route.timeline_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, TimelineLifecycleState::Active);
    assert!(persisted_revision > recovering_revision);

    let cache_handle = service
        .timeline_runtime
        .timeline_handle(&route.timeline_key)
        .expect("allocation should repopulate timeline cache");
    let cached = cache_handle.lock().await;
    assert_eq!(cached.state, TimelineLifecycleState::Active);
    assert_eq!(cached.route, persisted.route);
    assert_eq!(cached.revision, persisted_revision);
    assert_eq!(cached.last_issued_tso, Some(allocated_tso));
}

#[tokio::test]
async fn cold_cache_ensure_timeline_serializes_inflight_timeline_loads() {
    let clock = Arc::new(ManualClock::new(22_500));
    let inner = Arc::new(MemoryMetadataStore::new());
    let route = crate::TimelineRoute {
        timeline_key: "allocation.timeline-load.singleflight".into(),
        generator_id: 7,
        owner_worker_endpoint: "127.0.0.1:59999".into(),
        epoch: 1,
        route_version: 1,
        resource_tier: crate::ResourceTier::Shared,
    };
    inner
        .create_timeline(
            &route.timeline_key,
            &TimelineRecord {
                schema_version: 1,
                route: route.clone(),
                state: TimelineLifecycleState::Active,
                recovery_floor_tso: None,
                issued_upper_bound: None,
                last_graceful_issued: None,
                lease_expire_at_ms: None,
                updated_at_ms: clock.now_ms(),
            },
        )
        .await
        .unwrap();

    let (load_started_tx, load_started_rx) = oneshot::channel();
    let (release_load_tx, release_load_rx) = oneshot::channel();
    let metadata = Arc::new(BlockingTimelineLoadStore::new(
        inner,
        load_started_tx,
        release_load_rx,
    ));
    let service = TsoService::new(
        required_test_config(TsoConfig::default()),
        clock,
        metadata.clone(),
    )
    .unwrap();

    metadata.arm_blocking_load();

    let first_service = service.clone();
    let first_key = route.timeline_key.clone();
    let first = tokio::spawn(async move { first_service.ensure_timeline(&first_key).await });

    load_started_rx
        .await
        .expect("the first cold-cache load should enter metadata");
    assert_eq!(metadata.active_loads(), 1);

    let second_service = service.clone();
    let second_key = route.timeline_key.clone();
    let second = tokio::spawn(async move { second_service.ensure_timeline(&second_key).await });

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(metadata.active_loads(), 1);
    assert_eq!(metadata.max_parallel_loads(), 1);
    assert!(!second.is_finished());

    release_load_tx.send(()).unwrap();

    assert_eq!(first.await.unwrap().unwrap(), route);
    assert_eq!(second.await.unwrap().unwrap(), route);
    assert_eq!(metadata.max_parallel_loads(), 1);
}

#[tokio::test]
async fn distinct_cold_cache_timeline_loads_respect_global_load_limit() {
    let clock = Arc::new(ManualClock::new(22_600));
    let inner = Arc::new(MemoryMetadataStore::new());
    let route_a = crate::TimelineRoute {
        timeline_key: "allocation.timeline-load.limit.a".into(),
        generator_id: 7,
        owner_worker_endpoint: "127.0.0.1:59999".into(),
        epoch: 1,
        route_version: 1,
        resource_tier: crate::ResourceTier::Shared,
    };
    let route_b = crate::TimelineRoute {
        timeline_key: "allocation.timeline-load.limit.b".into(),
        generator_id: 8,
        owner_worker_endpoint: "127.0.0.1:59999".into(),
        epoch: 1,
        route_version: 1,
        resource_tier: crate::ResourceTier::Shared,
    };
    for route in [&route_a, &route_b] {
        inner
            .create_timeline(
                &route.timeline_key,
                &TimelineRecord {
                    schema_version: 1,
                    route: route.clone(),
                    state: TimelineLifecycleState::Active,
                    recovery_floor_tso: None,
                    issued_upper_bound: None,
                    last_graceful_issued: None,
                    lease_expire_at_ms: None,
                    updated_at_ms: clock.now_ms(),
                },
            )
            .await
            .unwrap();
    }

    let (load_started_tx, load_started_rx) = oneshot::channel();
    let (release_load_tx, release_load_rx) = oneshot::channel();
    let metadata = Arc::new(BlockingTimelineLoadStore::new(
        inner,
        load_started_tx,
        release_load_rx,
    ));
    let service = TsoService::new(
        required_test_config(TsoConfig {
            max_concurrent_timeline_loads: 1,
            ..TsoConfig::default()
        }),
        clock,
        metadata.clone(),
    )
    .unwrap();

    metadata.arm_blocking_load();

    let first_service = service.clone();
    let first_key = route_a.timeline_key.clone();
    let first = tokio::spawn(async move { first_service.ensure_timeline(&first_key).await });

    load_started_rx
        .await
        .expect("the first cold-cache load should enter metadata");
    assert_eq!(metadata.active_loads(), 1);

    let second_service = service.clone();
    let second_key = route_b.timeline_key.clone();
    let second = tokio::spawn(async move { second_service.ensure_timeline(&second_key).await });

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(metadata.active_loads(), 1);
    assert_eq!(metadata.max_parallel_loads(), 1);
    assert!(!second.is_finished());

    release_load_tx.send(()).unwrap();

    assert_eq!(first.await.unwrap().unwrap(), route_a);
    assert_eq!(second.await.unwrap().unwrap(), route_b);
    assert_eq!(metadata.max_parallel_loads(), 1);
}

#[tokio::test]
async fn timeline_load_limiter_respects_request_cancellation() {
    let clock = Arc::new(ManualClock::new(22_700));
    let inner = Arc::new(MemoryMetadataStore::new());
    for (timeline_key, generator_id) in [
        ("allocation.timeline-load.cancel.a", 7),
        ("allocation.timeline-load.cancel.b", 8),
    ] {
        inner
            .create_timeline(
                timeline_key,
                &TimelineRecord {
                    schema_version: 1,
                    route: crate::TimelineRoute {
                        timeline_key: timeline_key.into(),
                        generator_id,
                        owner_worker_endpoint: "127.0.0.1:59999".into(),
                        epoch: 1,
                        route_version: 1,
                        resource_tier: crate::ResourceTier::Shared,
                    },
                    state: TimelineLifecycleState::Active,
                    recovery_floor_tso: None,
                    issued_upper_bound: None,
                    last_graceful_issued: None,
                    lease_expire_at_ms: None,
                    updated_at_ms: clock.now_ms(),
                },
            )
            .await
            .unwrap();
    }

    let (load_started_tx, load_started_rx) = oneshot::channel();
    let (release_load_tx, release_load_rx) = oneshot::channel();
    let metadata = Arc::new(BlockingTimelineLoadStore::new(
        inner,
        load_started_tx,
        release_load_rx,
    ));
    let service = TsoService::new(
        required_test_config(TsoConfig {
            max_concurrent_timeline_loads: 1,
            ..TsoConfig::default()
        }),
        clock,
        metadata.clone(),
    )
    .unwrap();

    metadata.arm_blocking_load();

    let first_service = service.clone();
    let first = tokio::spawn(async move {
        first_service
            .load_timeline_with_singleflight_and_cancellation(
                "allocation.timeline-load.cancel.a",
                None,
            )
            .await
    });

    load_started_rx
        .await
        .expect("the first cold-cache load should enter metadata");
    assert_eq!(metadata.active_loads(), 1);

    let cancellation = RequestCancellation::new();
    let cancelled_service = service.clone();
    let cancelled_request = cancellation.clone();
    let second = tokio::spawn(async move {
        cancelled_service
            .load_timeline_with_singleflight_and_cancellation(
                "allocation.timeline-load.cancel.b",
                Some(cancelled_request),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(25)).await;
    cancellation.cancel();

    let error = second.await.unwrap().unwrap_err();
    assert_eq!(error, TsoError::RequestCancelled);
    assert_eq!(metadata.max_parallel_loads(), 1);

    release_load_tx.send(()).unwrap();
    first.await.unwrap().unwrap();
}

#[tokio::test]
async fn cached_allocation_revalidates_authoritative_route_without_route_updates() {
    let clock = Arc::new(ManualClock::new(30_000));
    let inner = Arc::new(MemoryMetadataStore::new());
    let metadata = Arc::new(SilentRouteUpdateStore::new(inner.clone()));
    let service = TsoService::new(
        required_test_config(TsoConfig::default()),
        clock.clone(),
        metadata,
    )
    .unwrap();

    let route = service
        .ensure_timeline("allocation.stale-route.timeline")
        .await
        .unwrap();
    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "seed-stale-route".to_string(),
        })
        .await
        .unwrap();
    service.generator_runtime.remove_lease(route.generator_id);

    let (mut record, revision) = inner
        .load_timeline(&route.timeline_key)
        .await
        .unwrap()
        .unwrap();
    record.route.route_version += 1;
    let actual_route_version = record.route.route_version;
    record.updated_at_ms = clock.now_ms();
    inner
        .compare_exchange_timeline(&route.timeline_key, revision, &record)
        .await
        .unwrap();

    let error = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "stale-route-request".to_string(),
        })
        .await
        .unwrap_err();

    assert_eq!(
        error,
        TsoError::RouteVersionMismatch {
            expected: route.route_version,
            actual: actual_route_version,
        }
    );
}

#[tokio::test]
async fn cached_allocation_revalidates_authoritative_state_without_route_updates() {
    let clock = Arc::new(ManualClock::new(31_000));
    let inner = Arc::new(MemoryMetadataStore::new());
    let metadata = Arc::new(SilentRouteUpdateStore::new(inner.clone()));
    let service = TsoService::new(
        required_test_config(TsoConfig::default()),
        clock.clone(),
        metadata,
    )
    .unwrap();

    let route = service
        .ensure_timeline("allocation.stale-state.timeline")
        .await
        .unwrap();
    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "seed-stale-state".to_string(),
        })
        .await
        .unwrap();
    service.generator_runtime.remove_lease(route.generator_id);

    let (mut record, revision) = inner
        .load_timeline(&route.timeline_key)
        .await
        .unwrap()
        .unwrap();
    record.state = TimelineLifecycleState::Draining;
    record.updated_at_ms = clock.now_ms();
    inner
        .compare_exchange_timeline(&route.timeline_key, revision, &record)
        .await
        .unwrap();

    let error = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "stale-state-request".to_string(),
        })
        .await
        .unwrap_err();

    assert_eq!(
        error,
        TsoError::TimelineNotReady {
            timeline_key: route.timeline_key,
            state: TimelineLifecycleState::Draining,
        }
    );
}

#[tokio::test]
async fn post_lease_guard_revalidates_authoritative_route_before_serving() {
    let clock = Arc::new(ManualClock::new(31_500));
    let inner = Arc::new(MemoryMetadataStore::new());
    let metadata = Arc::new(SilentRouteUpdateStore::new(inner.clone()));
    let service = TsoService::new(
        required_test_config(TsoConfig::default()),
        clock.clone(),
        metadata,
    )
    .unwrap();

    let route = service
        .ensure_timeline("allocation.post-lease-cutover.timeline")
        .await
        .unwrap();
    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "seed-post-lease-cutover".to_string(),
        })
        .await
        .unwrap();

    let timeline_state_handle = service
        .timeline_runtime
        .timeline_handle(&route.timeline_key)
        .expect("timeline should be cached after seed allocation");
    let issued_upper_bound = service
        .ensure_generator_lease_for_allocation_with_cancellation(
            &route.timeline_key,
            route.generator_id,
            None,
        )
        .await
        .unwrap();

    let (mut record, revision) = inner
        .load_timeline(&route.timeline_key)
        .await
        .unwrap()
        .unwrap();
    record.route.route_version += 1;
    record.updated_at_ms = clock.now_ms();
    inner
        .compare_exchange_timeline(&route.timeline_key, revision, &record)
        .await
        .unwrap();

    let response = service
        .try_serve_timeline_state_handle_with_guard(
            &AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "post-lease-cutover".to_string(),
            },
            timeline_state_handle,
            route.generator_id,
            clock.now_ms(),
            issued_upper_bound,
            CachedServeGuardOptions {
                cancellation: None,
                revalidate_authority: true,
                path: AllocationPath::Metadata,
            },
        )
        .await
        .unwrap();

    assert!(
        response.is_none(),
        "post-lease guard should fail closed after cutover"
    );
    assert!(service
        .timeline_runtime
        .timeline_handle(&route.timeline_key)
        .is_none());
}

#[tokio::test]
async fn allocation_refreshes_now_ms_before_lease_validation_after_timeline_load() {
    let clock = Arc::new(ManualClock::new(40_000));
    let inner = Arc::new(MemoryMetadataStore::new());
    let (load_started_tx, load_started_rx) = oneshot::channel();
    let (release_load_tx, release_load_rx) = oneshot::channel();
    let metadata = Arc::new(BlockingTimelineLoadStore::new(
        inner.clone(),
        load_started_tx,
        release_load_rx,
    ));
    let service = TsoService::new(
        required_test_config(TsoConfig {
            generator_lease_ttl_ms: 250,
            lease_ttl_ms: 250,
            ..TsoConfig::default()
        }),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();

    let route = service
        .ensure_timeline("allocation.refresh-now.timeline")
        .await
        .unwrap();
    let lease_expire_at_ms = service
        .generator_runtime
        .lease_state(route.generator_id)
        .expect("generator lease should exist")
        .lease_expire_at_ms;

    service.clear_timeline_cache(&route.timeline_key);
    clock.set(lease_expire_at_ms.saturating_sub(1));
    metadata.arm_blocking_load();

    let service_clone = service.clone();
    let route_clone = route.clone();
    let allocate_task = tokio::spawn(async move {
        service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_clone.timeline_key.clone(),
                count: 1,
                expected_epoch: route_clone.epoch,
                expected_route_version: route_clone.route_version,
                client_request_id: "refresh-now-after-load".to_string(),
            })
            .await
    });

    load_started_rx
        .await
        .expect("timeline load should start before lease validation");
    clock.set(lease_expire_at_ms + 1);
    release_load_tx.send(()).unwrap();

    let error = allocate_task.await.unwrap().unwrap_err();
    assert_eq!(
        error,
        TsoError::LeaseExpired {
            timeline_key: route.timeline_key,
        }
    );
}

#[tokio::test]
async fn allocation_fails_closed_after_cutover_without_second_full_reload() {
    let clock = Arc::new(ManualClock::new(40_500));
    let inner = Arc::new(MemoryMetadataStore::new());
    let metadata = Arc::new(SilentRouteUpdateStore::new(inner.clone()));
    let service = TsoService::new(
        required_test_config(TsoConfig::default()),
        clock.clone(),
        metadata,
    )
    .unwrap();

    let route = service
        .ensure_timeline("allocation.slow-path-cutover.timeline")
        .await
        .unwrap();
    service.clear_timeline_cache(&route.timeline_key);

    let issued_upper_bound = service
        .ensure_generator_lease_for_allocation_with_cancellation(
            &route.timeline_key,
            route.generator_id,
            None,
        )
        .await
        .unwrap();

    let (mut record, revision) = inner
        .load_timeline(&route.timeline_key)
        .await
        .unwrap()
        .unwrap();
    record.route.route_version += 1;
    record.updated_at_ms = clock.now_ms();
    inner
        .compare_exchange_timeline(&route.timeline_key, revision, &record)
        .await
        .unwrap();

    let stale_record = TimelineRecord {
        route: route.clone(),
        updated_at_ms: clock.now_ms(),
        ..record.clone()
    };
    let timeline_state_handle = service
        .timeline_state_handle_from_record(&route.timeline_key, &stale_record, revision)
        .await
        .unwrap();

    let response = service
        .try_serve_timeline_state_handle_with_guard(
            &AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "slow-path-cutover".to_string(),
            },
            timeline_state_handle,
            route.generator_id,
            clock.now_ms(),
            issued_upper_bound,
            CachedServeGuardOptions {
                cancellation: None,
                revalidate_authority: true,
                path: AllocationPath::Metadata,
            },
        )
        .await
        .unwrap();

    assert!(response.is_none());
    assert!(service
        .timeline_runtime
        .timeline_handle(&route.timeline_key)
        .is_none());
}

#[tokio::test]
async fn allocation_rechecks_generator_lease_time_after_slow_generator_load() {
    let clock = Arc::new(ManualClock::new(41_000));
    let inner = Arc::new(MemoryMetadataStore::new());
    let (load_started_tx, load_started_rx) = oneshot::channel();
    let (release_load_tx, release_load_rx) = oneshot::channel();
    let metadata = Arc::new(BlockingGeneratorLoadStore::new(
        inner.clone(),
        load_started_tx,
        release_load_rx,
    ));
    let service = TsoService::new(
        required_test_config(TsoConfig {
            generator_lease_ttl_ms: 250,
            lease_ttl_ms: 250,
            ..TsoConfig::default()
        }),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();

    let route = service
        .ensure_timeline("allocation.refresh-generator-load.timeline")
        .await
        .unwrap();
    let lease_expire_at_ms = service
        .generator_runtime
        .lease_state(route.generator_id)
        .expect("generator lease should exist")
        .lease_expire_at_ms;

    service.clear_timeline_cache(&route.timeline_key);
    service.generator_runtime.remove_lease(route.generator_id);
    clock.set(lease_expire_at_ms.saturating_sub(1));
    metadata.arm_blocking_load();

    let service_clone = service.clone();
    let route_clone = route.clone();
    let allocate_task = tokio::spawn(async move {
        service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_clone.timeline_key.clone(),
                count: 1,
                expected_epoch: route_clone.epoch,
                expected_route_version: route_clone.route_version,
                client_request_id: "refresh-after-generator-load".to_string(),
            })
            .await
    });

    load_started_rx
        .await
        .expect("generator load should start before lease validation returns");
    clock.set(lease_expire_at_ms + 1);
    release_load_tx.send(()).unwrap();

    let error = allocate_task.await.unwrap().unwrap_err();
    assert_eq!(
        error,
        TsoError::LeaseExpired {
            timeline_key: route.timeline_key,
        }
    );
}

#[tokio::test]
async fn allocation_does_not_hold_cached_timeline_lock_while_waiting_for_generator_load() {
    let clock = Arc::new(ManualClock::new(23_000));
    let inner = Arc::new(MemoryMetadataStore::new());
    let (load_started_tx, load_started_rx) = oneshot::channel();
    let (release_load_tx, release_load_rx) = oneshot::channel();
    let metadata = Arc::new(BlockingGeneratorLoadStore::new(
        inner,
        load_started_tx,
        release_load_rx,
    ));
    let service = TsoService::new(
        required_test_config(TsoConfig::default()),
        clock,
        metadata.clone(),
    )
    .unwrap();

    let route = service
        .ensure_timeline("allocation.lock.scope.timeline")
        .await
        .unwrap();
    service.background.begin_shutdown();
    service.background.drain_tasks().await;
    service.generator_runtime.remove_lease(route.generator_id);
    metadata.arm_blocking_load();

    let timeline_handle = service
        .timeline_runtime
        .timeline_handle(&route.timeline_key)
        .expect("timeline should be cached after ensure");

    let allocate_service = service.clone();
    let allocate_route = route.clone();
    let allocate_task = tokio::spawn(async move {
        allocate_service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: allocate_route.timeline_key,
                count: 1,
                expected_epoch: allocate_route.epoch,
                expected_route_version: allocate_route.route_version,
                client_request_id: "allocation-lock-scope".to_string(),
            })
            .await
    });

    load_started_rx
        .await
        .expect("allocation should block in load_generator");

    let timeline_guard = timeout(Duration::from_millis(50), timeline_handle.lock())
        .await
        .expect("cached timeline lock should be released while generator load waits");
    drop(timeline_guard);

    release_load_tx.send(()).unwrap();
    let response = allocate_task.await.unwrap().unwrap();
    assert_eq!(response.timeline_key, route.timeline_key);
}

#[tokio::test]
async fn cached_allocation_with_valid_local_lease_skips_metadata_reload() {
    let clock = Arc::new(ManualClock::new(23_500));
    let inner = Arc::new(MemoryMetadataStore::new());
    let (load_started_tx, load_started_rx) = oneshot::channel();
    let (_release_load_tx, release_load_rx) = oneshot::channel();
    let metadata = Arc::new(BlockingTimelineLoadStore::new(
        inner,
        load_started_tx,
        release_load_rx,
    ));
    let service = TsoService::new(
        required_test_config(TsoConfig::default()),
        clock,
        metadata.clone(),
    )
    .unwrap();

    let route = service
        .ensure_timeline("allocation.cached-hot-path.timeline")
        .await
        .unwrap();
    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "seed-cached-hot-path".to_string(),
        })
        .await
        .unwrap();

    metadata.arm_blocking_load();

    let response = timeout(
        Duration::from_millis(50),
        service.allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "cached-hot-path".to_string(),
        }),
    )
    .await
    .expect("cached allocation should complete without metadata blocking")
    .unwrap();

    assert_eq!(response.timeline_key, route.timeline_key);
    assert!(
        timeout(Duration::from_millis(25), load_started_rx)
            .await
            .is_err(),
        "cached hot path should not trigger timeline metadata reload"
    );
}

#[tokio::test]
async fn cached_allocation_with_unready_local_lease_reloads_metadata() {
    let clock = Arc::new(ManualClock::new(23_500));
    let inner = Arc::new(MemoryMetadataStore::new());
    let (load_started_tx, load_started_rx) = oneshot::channel();
    let (release_load_tx, release_load_rx) = oneshot::channel();
    let metadata = Arc::new(BlockingTimelineLoadStore::new(
        inner,
        load_started_tx,
        release_load_rx,
    ));
    let service = TsoService::new(
        required_test_config(TsoConfig::default()),
        clock,
        metadata.clone(),
    )
    .unwrap();

    let route = service
        .ensure_timeline("allocation.cached-unready-lease.timeline")
        .await
        .unwrap();
    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "seed-cached-unready-lease".to_string(),
        })
        .await
        .unwrap();

    let lease = service
        .generator_runtime
        .lease_state(route.generator_id)
        .expect("generator lease should exist");
    service.generator_runtime.remove_lease(route.generator_id);
    service
        .generator_runtime
        .upsert_lease(route.generator_id, lease);
    metadata.arm_blocking_load();

    let service_clone = service.clone();
    let route_clone = route.clone();
    let allocate_task = tokio::spawn(async move {
        service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_clone.timeline_key.clone(),
                count: 1,
                expected_epoch: route_clone.epoch,
                expected_route_version: route_clone.route_version,
                client_request_id: "cached-unready-lease".to_string(),
            })
            .await
    });

    load_started_rx
        .await
        .expect("unready lease should force a metadata reload");
    release_load_tx.send(()).unwrap();
    let response = allocate_task.await.unwrap().unwrap();
    assert_eq!(response.timeline_key, route.timeline_key);
}

#[tokio::test]
async fn cached_guard_fails_closed_when_timeline_state_drifts_before_serve() {
    let clock = Arc::new(ManualClock::new(24_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service =
        TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();

    let route = service
        .ensure_timeline("allocation.cached-state-drift.timeline")
        .await
        .unwrap();
    let timeline_state_handle = service
        .timeline_runtime
        .timeline_handle(&route.timeline_key)
        .expect("timeline should be cached after ensure");

    {
        let mut timeline_state = timeline_state_handle.lock().await;
        timeline_state.state = TimelineLifecycleState::Draining;
    }

    let error = service
        .try_serve_timeline_state_handle_with_guard(
            &AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "cached-state-drift".to_string(),
            },
            timeline_state_handle,
            route.generator_id,
            service.clock.now_ms(),
            service.valid_generator_lease_upper_bound(route.generator_id, service.clock.now_ms()),
            CachedServeGuardOptions {
                cancellation: None,
                revalidate_authority: false,
                path: AllocationPath::Cached,
            },
        )
        .await
        .unwrap_err();

    assert_eq!(
        error,
        TsoError::TimelineNotReady {
            timeline_key: route.timeline_key.clone(),
            state: TimelineLifecycleState::Draining,
        }
    );
    assert!(service
        .timeline_runtime
        .timeline_handle(&route.timeline_key)
        .is_none());
}

#[tokio::test]
async fn allocation_degrades_to_transient_timeline_state_when_runtime_cache_is_saturated() {
    let clock = Arc::new(ManualClock::new(24_500));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(
        required_test_config(TsoConfig {
            max_timeline_runtime_entries: 1,
            ..TsoConfig::default()
        }),
        clock,
        metadata.clone(),
    )
    .unwrap();

    let route_a = service
        .ensure_timeline("allocation.degrade.a")
        .await
        .unwrap();
    let busy_handle = service
        .timeline_runtime
        .timeline_handle(&route_a.timeline_key)
        .expect("seeded timeline should be cached");

    let route_b = crate::TimelineRoute {
        timeline_key: "allocation.degrade.b".into(),
        generator_id: route_a.generator_id,
        owner_worker_endpoint: service.advertise_endpoint().to_string(),
        epoch: 1,
        route_version: 1,
        resource_tier: crate::ResourceTier::Shared,
    };
    let record_b = TimelineRecord {
        schema_version: 1,
        route: route_b.clone(),
        state: TimelineLifecycleState::Active,
        recovery_floor_tso: None,
        issued_upper_bound: None,
        last_graceful_issued: None,
        lease_expire_at_ms: None,
        updated_at_ms: service.clock.now_ms(),
    };
    metadata
        .create_timeline(&route_b.timeline_key, &record_b)
        .await
        .unwrap();

    let degraded_before = crate::metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
        .with_label_values(&["degraded_uncached"])
        .get();

    let response = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route_b.timeline_key.clone(),
            count: 1,
            expected_epoch: route_b.epoch,
            expected_route_version: route_b.route_version,
            client_request_id: "allocation-degrade-b".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(response.timeline_key, route_b.timeline_key);
    assert_eq!(service.timeline_runtime.timeline_count(), 1);
    assert!(service
        .timeline_runtime
        .timeline_handle(&route_b.timeline_key)
        .is_none());
    assert!(
        crate::metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
            .with_label_values(&["degraded_uncached"])
            .get()
            > degraded_before
    );

    drop(busy_handle);
}

#[tokio::test]
async fn allocation_clock_backwards_remains_terminal_error() {
    let clock = Arc::new(ManualClock::new(1_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(
        required_test_config(TsoConfig {
            max_clock_rewind_ms: 10,
            max_future_borrow_ms: 0,
            ..TsoConfig::default()
        }),
        clock.clone(),
        metadata,
    )
    .unwrap();

    let route = service
        .ensure_timeline("allocation.clock-terminal.timeline")
        .await
        .unwrap();
    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "clock-terminal-seed".to_string(),
        })
        .await
        .unwrap();

    clock.set(975);

    let error = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key,
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "clock-terminal-second".to_string(),
        })
        .await
        .expect_err("allocation should still fail immediately on large clock rewind");

    assert_eq!(error, TsoError::ClockBackwards { delta_ms: 25 });
}
