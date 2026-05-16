use super::*;

#[tokio::test]
async fn shared_tier_rejects_batches_above_single_ms_capacity() {
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                max_batch_per_request: crate::SEQUENCE_CAPACITY + 32,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(50_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route = service
        .ensure_timeline("shared.noisy-neighbor")
        .await
        .unwrap();
    assert_eq!(route.resource_tier, ResourceTier::Shared);

    let error = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: crate::SEQUENCE_CAPACITY + 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "shared-batch-cap".into(),
        })
        .await
        .unwrap_err();

    assert_eq!(
        error,
        TsoError::BatchTooLarge {
            requested: crate::SEQUENCE_CAPACITY + 1,
            max: crate::SEQUENCE_CAPACITY,
        }
    );
}

#[tokio::test]
async fn shared_allocation_waits_for_generator_admission_gate() {
    let service = TsoService::new(
        with_worker(TsoConfig::default(), "worker-a"),
        Arc::new(ManualClock::new(52_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route = service
        .ensure_timeline("shared.admission.wait")
        .await
        .unwrap();
    assert_eq!(route.resource_tier, ResourceTier::Shared);

    let permit = service.generator_admission_gates[route.generator_id as usize]
        .clone()
        .acquire_owned()
        .await
        .unwrap();

    let service_clone = service.clone();
    let route_clone = route.clone();
    let mut allocate_task = tokio::spawn(async move {
        service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_clone.timeline_key.clone(),
                count: 1,
                expected_epoch: route_clone.epoch,
                expected_route_version: route_clone.route_version,
                client_request_id: "shared-admission-wait".to_string(),
            })
            .await
    });

    assert!(
        timeout(Duration::from_millis(50), &mut allocate_task)
            .await
            .is_err(),
        "shared allocation should wait while the generator admission gate is held"
    );

    drop(permit);
    let response = allocate_task.await.unwrap().unwrap();
    assert_eq!(response.timeline_key, route.timeline_key);
}

#[tokio::test]
async fn cancelled_shared_allocation_refunds_timeline_quota() {
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                shared_generators: 1,
                warm_generators: 0,
                max_batch_per_request: 8,
                max_future_borrow_ms: 2_000,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(52_500)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route = service
        .ensure_timeline("shared.cancel.refund")
        .await
        .unwrap();
    let permit = service.generator_admission_gates[route.generator_id as usize]
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    let cancellation = RequestCancellation::new();
    let service_clone = service.clone();
    let route_clone = route.clone();
    let cancellation_clone = cancellation.clone();
    let allocate_task = tokio::spawn(async move {
        service_clone
            .allocate_timestamps_with_cancellation(
                AllocateTimestampsRequest {
                    timeline_key: route_clone.timeline_key.clone(),
                    count: 8,
                    expected_epoch: route_clone.epoch,
                    expected_route_version: route_clone.route_version,
                    client_request_id: "shared-cancel-refund-cancelled".into(),
                },
                Some(cancellation_clone),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(25)).await;
    cancellation.cancel();
    let error = allocate_task.await.unwrap().unwrap_err();
    assert_eq!(error, TsoError::RequestCancelled);
    drop(permit);

    let response = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 8,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "shared-cancel-refund-after".into(),
        })
        .await
        .unwrap();
    assert_eq!(response.timeline_key, route.timeline_key);
}

async fn ensure_shared_timeline_on_generator(
    service: &TsoService,
    generator_id: u32,
    prefix: &str,
) -> crate::TimelineRoute {
    for attempt in 0..(MAX_GENERATORS as usize * 8).max(8) {
        let route = service
            .ensure_timeline(&format!("{prefix}.{attempt}"))
            .await
            .unwrap();
        if route.resource_tier == ResourceTier::Shared && route.generator_id == generator_id {
            return route;
        }
    }
    panic!("failed to find shared timeline for generator {generator_id}");
}

#[tokio::test]
async fn dropped_shared_allocation_releases_generator_admission_turn() {
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                shared_generators: 1,
                warm_generators: 0,
                max_batch_per_request: 8,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(53_500)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route_a = service.ensure_timeline("shared.drop-turn.a").await.unwrap();
    let route_b =
        ensure_shared_timeline_on_generator(&service, route_a.generator_id, "shared.drop-turn.b")
            .await;
    let permit = service.generator_admission_gates[route_a.generator_id as usize]
        .clone()
        .acquire_owned()
        .await
        .unwrap();

    let service_clone = service.clone();
    let route_a_clone = route_a.clone();
    let blocked_task = tokio::spawn(async move {
        service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_a_clone.timeline_key.clone(),
                count: 1,
                expected_epoch: route_a_clone.epoch,
                expected_route_version: route_a_clone.route_version,
                client_request_id: "shared-drop-turn-aborted".into(),
            })
            .await
    });

    for _ in 0..20 {
        let active = service.generator_fairness_trackers[route_a.generator_id as usize]
            .lock()
            .unwrap()
            .active_timeline_key
            .clone();
        if active.as_deref() == Some(route_a.timeline_key.as_str()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        service.generator_fairness_trackers[route_a.generator_id as usize]
            .lock()
            .unwrap()
            .active_timeline_key
            .as_deref(),
        Some(route_a.timeline_key.as_str())
    );

    blocked_task.abort();
    let _ = blocked_task.await;
    drop(permit);

    timeout(
        Duration::from_millis(100),
        service.allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route_b.timeline_key.clone(),
            count: 1,
            expected_epoch: route_b.epoch,
            expected_route_version: route_b.route_version,
            client_request_id: "shared-drop-turn-next".into(),
        }),
    )
    .await
    .expect("next timeline should not be blocked by aborted admission turn")
    .unwrap();
}

#[tokio::test]
async fn dropped_shared_allocation_removes_queued_generator_waiter() {
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                shared_generators: 1,
                warm_generators: 0,
                max_batch_per_request: 8,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(53_750)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route_a = service
        .ensure_timeline("shared.drop-queued.a")
        .await
        .unwrap();
    let route_b =
        ensure_shared_timeline_on_generator(&service, route_a.generator_id, "shared.drop-queued.b")
            .await;
    let route_c =
        ensure_shared_timeline_on_generator(&service, route_a.generator_id, "shared.drop-queued.c")
            .await;
    let permit = service.generator_admission_gates[route_a.generator_id as usize]
        .clone()
        .acquire_owned()
        .await
        .unwrap();

    let service_clone = service.clone();
    let route_a_clone = route_a.clone();
    let active_task = tokio::spawn(async move {
        service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_a_clone.timeline_key.clone(),
                count: 1,
                expected_epoch: route_a_clone.epoch,
                expected_route_version: route_a_clone.route_version,
                client_request_id: "shared-drop-queued-active".into(),
            })
            .await
    });

    for _ in 0..20 {
        let active = service.generator_fairness_trackers[route_a.generator_id as usize]
            .lock()
            .unwrap()
            .active_timeline_key
            .clone();
        if active.as_deref() == Some(route_a.timeline_key.as_str()) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        service.generator_fairness_trackers[route_a.generator_id as usize]
            .lock()
            .unwrap()
            .active_timeline_key
            .as_deref(),
        Some(route_a.timeline_key.as_str())
    );

    let service_clone = service.clone();
    let route_b_clone = route_b.clone();
    let queued_task = tokio::spawn(async move {
        service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_b_clone.timeline_key.clone(),
                count: 1,
                expected_epoch: route_b_clone.epoch,
                expected_route_version: route_b_clone.route_version,
                client_request_id: "shared-drop-queued-aborted".into(),
            })
            .await
    });

    for _ in 0..20 {
        let queued = service.generator_fairness_trackers[route_a.generator_id as usize]
            .lock()
            .unwrap()
            .wait_queue
            .iter()
            .any(|queued| queued.timeline_key == route_b.timeline_key);
        if queued {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        service.generator_fairness_trackers[route_a.generator_id as usize]
            .lock()
            .unwrap()
            .wait_queue
            .iter()
            .any(|queued| queued.timeline_key == route_b.timeline_key),
        "route_b should be queued behind the active route"
    );

    queued_task.abort();
    let _ = queued_task.await;
    assert!(
        !service.generator_fairness_trackers[route_a.generator_id as usize]
            .lock()
            .unwrap()
            .wait_queue
            .iter()
            .any(|queued| queued.timeline_key == route_b.timeline_key),
        "aborted queued waiter should be removed from generator fairness queue"
    );

    drop(permit);
    active_task.await.unwrap().unwrap();

    timeout(
        Duration::from_millis(100),
        service.allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route_c.timeline_key.clone(),
            count: 1,
            expected_epoch: route_c.epoch,
            expected_route_version: route_c.route_version,
            client_request_id: "shared-drop-queued-next".into(),
        }),
    )
    .await
    .expect("next timeline should not be blocked by aborted queued waiter")
    .unwrap();
}

#[tokio::test]
async fn shared_large_batch_repeat_waits_behind_other_timeline() {
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                shared_generators: 1,
                warm_generators: 0,
                max_batch_per_request: 8,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(54_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route_a = service.ensure_timeline("shared.fairness.a").await.unwrap();
    let route_b =
        ensure_shared_timeline_on_generator(&service, route_a.generator_id, "shared.fairness.b")
            .await;

    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route_a.timeline_key.clone(),
            count: 4,
            expected_epoch: route_a.epoch,
            expected_route_version: route_a.route_version,
            client_request_id: "shared-large-first".into(),
        })
        .await
        .unwrap();

    let permit = service.generator_admission_gates[route_a.generator_id as usize]
        .clone()
        .acquire_owned()
        .await
        .unwrap();

    let (tx, mut rx) = mpsc::unbounded_channel();
    let service_clone = service.clone();
    let route_b_clone = route_b.clone();
    let tx_b = tx.clone();
    let mut blocked_task = tokio::spawn(async move {
        let response = service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_b_clone.timeline_key.clone(),
                count: 1,
                expected_epoch: route_b_clone.epoch,
                expected_route_version: route_b_clone.route_version,
                client_request_id: "shared-other-waiter".into(),
            })
            .await;
        if let Ok(response) = &response {
            let _ = tx_b.send(response.timeline_key.clone());
        }
        response
    });

    assert!(timeout(Duration::from_millis(50), &mut blocked_task)
        .await
        .is_err());

    let service_clone = service.clone();
    let route_a_clone = route_a.clone();
    let tx_a = tx.clone();
    let mut repeat_task = tokio::spawn(async move {
        let response = service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_a_clone.timeline_key.clone(),
                count: 4,
                expected_epoch: route_a_clone.epoch,
                expected_route_version: route_a_clone.route_version,
                client_request_id: "shared-large-repeat".into(),
            })
            .await;
        if let Ok(response) = &response {
            let _ = tx_a.send(response.timeline_key.clone());
        }
        response
    });

    assert!(timeout(Duration::from_millis(50), &mut repeat_task)
        .await
        .is_err());

    drop(permit);
    assert_eq!(rx.recv().await.unwrap(), route_b.timeline_key);
    assert_eq!(rx.recv().await.unwrap(), route_a.timeline_key);
    assert_eq!(
        blocked_task.await.unwrap().unwrap().timeline_key,
        route_b.timeline_key
    );
    assert_eq!(
        repeat_task.await.unwrap().unwrap().timeline_key,
        route_a.timeline_key
    );
}

#[tokio::test]
async fn shared_small_batch_repeat_waits_behind_other_timeline() {
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                shared_generators: 1,
                warm_generators: 0,
                max_batch_per_request: 8,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(55_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route_a = service.ensure_timeline("shared.small.a").await.unwrap();
    let route_b =
        ensure_shared_timeline_on_generator(&service, route_a.generator_id, "shared.small.b").await;

    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route_a.timeline_key.clone(),
            count: 4,
            expected_epoch: route_a.epoch,
            expected_route_version: route_a.route_version,
            client_request_id: "shared-large-seed".into(),
        })
        .await
        .unwrap();

    let permit = service.generator_admission_gates[route_a.generator_id as usize]
        .clone()
        .acquire_owned()
        .await
        .unwrap();

    let (tx, mut rx) = mpsc::unbounded_channel();
    let service_clone = service.clone();
    let route_b_clone = route_b.clone();
    let tx_b = tx.clone();
    let mut blocked_b = tokio::spawn(async move {
        let response = service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_b_clone.timeline_key.clone(),
                count: 1,
                expected_epoch: route_b_clone.epoch,
                expected_route_version: route_b_clone.route_version,
                client_request_id: "shared-small-waiter".into(),
            })
            .await;
        if let Ok(response) = &response {
            let _ = tx_b.send(response.timeline_key.clone());
        }
        response
    });
    assert!(timeout(Duration::from_millis(50), &mut blocked_b)
        .await
        .is_err());

    let service_clone = service.clone();
    let route_a_clone = route_a.clone();
    let tx_a = tx.clone();
    let mut repeat_a = tokio::spawn(async move {
        let response = service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_a_clone.timeline_key.clone(),
                count: 1,
                expected_epoch: route_a_clone.epoch,
                expected_route_version: route_a_clone.route_version,
                client_request_id: "shared-small-repeat".into(),
            })
            .await;
        if let Ok(response) = &response {
            let _ = tx_a.send(response.timeline_key.clone());
        }
        response
    });
    assert!(timeout(Duration::from_millis(50), &mut repeat_a)
        .await
        .is_err());

    drop(permit);
    assert_eq!(rx.recv().await.unwrap(), route_b.timeline_key);
    assert_eq!(rx.recv().await.unwrap(), route_a.timeline_key);
    assert_eq!(
        blocked_b.await.unwrap().unwrap().timeline_key,
        route_b.timeline_key
    );
    assert_eq!(
        repeat_a.await.unwrap().unwrap().timeline_key,
        route_a.timeline_key
    );
}

async fn ensure_warm_timeline_on_generator(
    service: &TsoService,
    generator_id: u32,
    prefix: &str,
) -> crate::TimelineRoute {
    for attempt in 0..(MAX_GENERATORS as usize * 8).max(8) {
        let route = service
            .ensure_timeline(&format!("{prefix}.{attempt}"))
            .await
            .unwrap();
        if route.resource_tier == ResourceTier::Warm && route.generator_id == generator_id {
            return route;
        }
    }
    panic!("failed to find warm timeline for generator {generator_id}");
}

#[tokio::test]
async fn warm_repeat_winner_waits_behind_other_timeline() {
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                default_resource_tier: ResourceTier::Warm,
                shared_generators: 0,
                warm_generators: 1,
                max_batch_per_request: 8,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(56_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route_a = service.ensure_timeline("warm.fairness.a").await.unwrap();
    let route_b =
        ensure_warm_timeline_on_generator(&service, route_a.generator_id, "warm.fairness.b").await;

    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route_a.timeline_key.clone(),
            count: 1,
            expected_epoch: route_a.epoch,
            expected_route_version: route_a.route_version,
            client_request_id: "warm-first".into(),
        })
        .await
        .unwrap();

    let permit = service.generator_admission_gates[route_a.generator_id as usize]
        .clone()
        .acquire_owned()
        .await
        .unwrap();

    let (tx, mut rx) = mpsc::unbounded_channel();
    let service_clone = service.clone();
    let route_b_clone = route_b.clone();
    let tx_b = tx.clone();
    let mut blocked_task = tokio::spawn(async move {
        let response = service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_b_clone.timeline_key.clone(),
                count: 1,
                expected_epoch: route_b_clone.epoch,
                expected_route_version: route_b_clone.route_version,
                client_request_id: "warm-waiter".into(),
            })
            .await;
        if let Ok(response) = &response {
            let _ = tx_b.send(response.timeline_key.clone());
        }
        response
    });

    assert!(timeout(Duration::from_millis(50), &mut blocked_task)
        .await
        .is_err());

    let service_clone = service.clone();
    let route_a_clone = route_a.clone();
    let tx_a = tx.clone();
    let mut repeat_task = tokio::spawn(async move {
        let response = service_clone
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route_a_clone.timeline_key.clone(),
                count: 1,
                expected_epoch: route_a_clone.epoch,
                expected_route_version: route_a_clone.route_version,
                client_request_id: "warm-repeat".into(),
            })
            .await;
        if let Ok(response) = &response {
            let _ = tx_a.send(response.timeline_key.clone());
        }
        response
    });

    assert!(timeout(Duration::from_millis(50), &mut repeat_task)
        .await
        .is_err());

    drop(permit);
    assert_eq!(rx.recv().await.unwrap(), route_b.timeline_key);
    assert_eq!(rx.recv().await.unwrap(), route_a.timeline_key);
    assert_eq!(
        blocked_task.await.unwrap().unwrap().timeline_key,
        route_b.timeline_key
    );
    assert_eq!(
        repeat_task.await.unwrap().unwrap().timeline_key,
        route_a.timeline_key
    );
}

#[tokio::test]
async fn shared_waiters_are_served_in_fifo_timeline_order() {
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                shared_generators: 1,
                warm_generators: 0,
                max_batch_per_request: 8,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(57_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route_a = service.ensure_timeline("shared.queue.a").await.unwrap();
    let route_b =
        ensure_shared_timeline_on_generator(&service, route_a.generator_id, "shared.queue.b").await;
    let route_c =
        ensure_shared_timeline_on_generator(&service, route_a.generator_id, "shared.queue.c").await;

    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route_a.timeline_key.clone(),
            count: 1,
            expected_epoch: route_a.epoch,
            expected_route_version: route_a.route_version,
            client_request_id: "shared-queue-seed".into(),
        })
        .await
        .unwrap();

    let permit = service.generator_admission_gates[route_a.generator_id as usize]
        .clone()
        .acquire_owned()
        .await
        .unwrap();

    let (tx, mut rx) = mpsc::unbounded_channel();

    for (route, request_id) in [
        (route_b.clone(), "shared-queue-b"),
        (route_c.clone(), "shared-queue-c"),
    ] {
        let service_clone = service.clone();
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let response = service_clone
                .allocate_timestamps(AllocateTimestampsRequest {
                    timeline_key: route.timeline_key.clone(),
                    count: 1,
                    expected_epoch: route.epoch,
                    expected_route_version: route.route_version,
                    client_request_id: request_id.into(),
                })
                .await
                .unwrap();
            let _ = tx_clone.send(response.timeline_key);
        });
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(permit);

    assert_eq!(rx.recv().await.unwrap(), route_b.timeline_key);
    assert_eq!(rx.recv().await.unwrap(), route_c.timeline_key);
}

#[tokio::test]
async fn shared_timeline_hits_per_timeline_quota_before_repeated_large_allocation() {
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                shared_generators: 1,
                warm_generators: 0,
                max_batch_per_request: 8,
                max_future_borrow_ms: 2_000,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(58_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route = service.ensure_timeline("shared.quota.a").await.unwrap();

    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 8,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "shared-quota-first".into(),
        })
        .await
        .unwrap();

    let error = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 8,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "shared-quota-repeat".into(),
        })
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        TsoError::FutureBorrowExceeded {
            requested_physical_ms,
            allowed_physical_ms,
        } if requested_physical_ms > allowed_physical_ms
    ));
}

#[tokio::test]
async fn shared_timeline_quota_does_not_block_peer_timeline() {
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                shared_generators: 1,
                warm_generators: 0,
                max_batch_per_request: 8,
                max_future_borrow_ms: 2_000,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(59_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route_a = service
        .ensure_timeline("shared.quota.peer.a")
        .await
        .unwrap();
    let route_b =
        ensure_shared_timeline_on_generator(&service, route_a.generator_id, "shared.quota.peer.b")
            .await;

    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route_a.timeline_key.clone(),
            count: 8,
            expected_epoch: route_a.epoch,
            expected_route_version: route_a.route_version,
            client_request_id: "shared-peer-seed".into(),
        })
        .await
        .unwrap();

    let peer_response = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route_b.timeline_key.clone(),
            count: 1,
            expected_epoch: route_b.epoch,
            expected_route_version: route_b.route_version,
            client_request_id: "shared-peer-alloc".into(),
        })
        .await
        .unwrap();

    assert_eq!(peer_response.timeline_key, route_b.timeline_key);
}

#[tokio::test]
async fn warm_timeline_hits_per_timeline_quota_before_repeated_allocation() {
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                default_resource_tier: ResourceTier::Warm,
                shared_generators: 0,
                warm_generators: 1,
                max_batch_per_request: 8,
                max_future_borrow_ms: 2_000,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(60_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route = service.ensure_timeline("warm.quota.a").await.unwrap();

    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 8,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "warm-quota-first".into(),
        })
        .await
        .unwrap();

    let error = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 8,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "warm-quota-repeat".into(),
        })
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        TsoError::FutureBorrowExceeded {
            requested_physical_ms,
            allowed_physical_ms,
        } if requested_physical_ms > allowed_physical_ms
    ));
}

#[tokio::test]
async fn dedicated_tier_keeps_global_batch_limit() {
    let global_limit = crate::SEQUENCE_CAPACITY + 32;
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                default_resource_tier: ResourceTier::Dedicated,
                max_batch_per_request: global_limit,
                max_future_borrow_ms: 2_000,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(51_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route = service
        .ensure_timeline("dedicated.batch-limit")
        .await
        .unwrap();
    assert_eq!(route.resource_tier, ResourceTier::Dedicated);

    let response = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: global_limit,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "dedicated-global-cap".into(),
        })
        .await
        .unwrap();

    assert!(!response.ranges.is_empty());
}

#[tokio::test]
async fn dedicated_allocation_bypasses_generator_admission_gate() {
    let service = TsoService::new(
        with_worker(
            TsoConfig {
                default_resource_tier: ResourceTier::Dedicated,
                ..TsoConfig::default()
            },
            "worker-a",
        ),
        Arc::new(ManualClock::new(53_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let route = service
        .ensure_timeline("dedicated.admission.bypass")
        .await
        .unwrap();
    assert_eq!(route.resource_tier, ResourceTier::Dedicated);

    let permit = service.generator_admission_gates[route.generator_id as usize]
        .clone()
        .acquire_owned()
        .await
        .unwrap();

    let response = timeout(
        Duration::from_millis(100),
        service.allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "dedicated-admission-bypass".to_string(),
        }),
    )
    .await
    .expect("dedicated allocation should bypass the shared admission gate")
    .unwrap();

    drop(permit);
    assert_eq!(response.timeline_key, route.timeline_key);
}
