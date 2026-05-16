use super::*;

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
