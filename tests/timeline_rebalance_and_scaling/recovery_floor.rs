use super::*;

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

    let source_clock = Arc::new(ManualClock::new(30_000));
    let service_a = TsoService::new(config_a, source_clock.clone(), metadata.clone()).unwrap();

    let route_a = service_a
        .ensure_timeline("failover.recovery.catchup.timeline")
        .await
        .unwrap();
    let before = service_a
        .allocate_timestamps(request(&route_a, "before-catchup-failover".to_owned(), 1))
        .await
        .unwrap();
    let before_last = before.ranges.last().unwrap().end_tso;

    source_clock.advance(1_000);
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
    let source_clock = Arc::new(ManualClock::new(31_000));
    let service_a = TsoService::new(config_a, source_clock.clone(), metadata.clone()).unwrap();

    let route_a = service_a
        .ensure_timeline("failover.recovery.catchup.limit")
        .await
        .unwrap();
    service_a
        .allocate_timestamps(request(&route_a, "before-catchup-limit".to_owned(), 1))
        .await
        .unwrap();

    source_clock.advance(1_000);
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
