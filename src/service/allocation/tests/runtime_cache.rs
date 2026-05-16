use super::*;

#[tokio::test]
async fn ensure_timeline_create_degrades_when_runtime_cache_is_saturated() {
    let clock = Arc::new(ManualClock::new(2_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(
        required_test_config(TsoConfig {
            max_timeline_runtime_entries: 1,
            ..TsoConfig::default()
        }),
        clock,
        metadata,
    )
    .unwrap();

    let route_a = service.ensure_timeline("ensure.degrade.a").await.unwrap();
    let busy_handle = service
        .timeline_runtime
        .timeline_handle(&route_a.timeline_key)
        .expect("seeded timeline should be cached");

    let degraded_before = crate::metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
        .with_label_values(&["degraded_uncached"])
        .get();

    let route_b = service.ensure_timeline("ensure.degrade.b").await.unwrap();

    assert_eq!(route_b.timeline_key, "ensure.degrade.b");
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
async fn metadata_path_rechecks_post_handle_generator_before_serving() {
    let clock = Arc::new(ManualClock::new(2_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(
        required_test_config(TsoConfig::default()),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();

    let route = service
        .ensure_timeline("allocation.post-handle.guard.timeline")
        .await
        .unwrap();
    service.clear_timeline_cache(&route.timeline_key);

    let (record, revision) = metadata
        .load_timeline(&route.timeline_key)
        .await
        .unwrap()
        .unwrap();
    let timeline_state_handle = service
        .timeline_state_handle_from_record(&route.timeline_key, &record, revision)
        .await
        .unwrap();

    {
        let mut timeline_state = timeline_state_handle.lock().await;
        timeline_state.route.generator_id = route.generator_id + 1;
    }

    let response = service
        .try_serve_timeline_state_handle_with_guard(
            &AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "post-handle-guard".to_string(),
            },
            timeline_state_handle,
            route.generator_id,
            clock.now_ms(),
            None,
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
        "post-handle guard should force a retry when generator changes before serve"
    );
    assert!(service
        .timeline_runtime
        .timeline_handle(&route.timeline_key)
        .is_none());
}
