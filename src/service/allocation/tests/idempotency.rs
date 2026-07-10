use super::*;

#[tokio::test]
async fn repeated_client_request_id_returns_recorded_allocation_response() {
    let clock = Arc::new(ManualClock::new(21_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service =
        TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();
    let route = service
        .ensure_timeline("allocation.idempotent")
        .await
        .unwrap();
    let request = AllocateTimestampsRequest {
        timeline_key: route.timeline_key.clone(),
        count: 2,
        expected_epoch: route.epoch,
        expected_route_version: route.route_version,
        client_request_id: "same-logical-request".to_string(),
    };

    let first = service.allocate_timestamps(request.clone()).await.unwrap();
    let second = service.allocate_timestamps(request).await.unwrap();

    assert_eq!(second, first);
}

#[tokio::test]
async fn idempotency_records_are_isolated_for_legacy_path_collision_inputs() {
    let clock = Arc::new(ManualClock::new(21_250));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service =
        TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();
    let route_a = service.ensure_timeline("a/b").await.unwrap();
    let route_b = service.ensure_timeline("a").await.unwrap();

    let response_a = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route_a.timeline_key.clone(),
            count: 1,
            expected_epoch: route_a.epoch,
            expected_route_version: route_a.route_version,
            client_request_id: "c".into(),
        })
        .await
        .unwrap();
    let response_b = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route_b.timeline_key.clone(),
            count: 1,
            expected_epoch: route_b.epoch,
            expected_route_version: route_b.route_version,
            client_request_id: "b/c".into(),
        })
        .await
        .unwrap();

    assert_eq!(response_a.timeline_key, "a/b");
    assert_eq!(response_b.timeline_key, "a");
    assert_ne!(response_a.ranges, response_b.ranges);
}

#[tokio::test]
async fn repeated_client_request_id_with_different_fingerprint_is_rejected() {
    let clock = Arc::new(ManualClock::new(21_500));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service =
        TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();
    let route = service
        .ensure_timeline("allocation.idempotent.conflict")
        .await
        .unwrap();

    service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "conflicting-logical-request".to_string(),
        })
        .await
        .unwrap();
    let conflict = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 2,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "conflicting-logical-request".to_string(),
        })
        .await
        .unwrap_err();

    assert!(matches!(
        conflict,
        TsoError::ClientRequestConflict { timeline_key, .. }
            if timeline_key == route.timeline_key
    ));
}

#[tokio::test]
async fn pending_client_request_id_is_rejected_until_timeout() {
    let clock = Arc::new(ManualClock::new(21_800));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(
        required_test_config(TsoConfig {
            request_record_pending_timeout_ms: 100,
            request_record_retention_ms: 1_000,
            ..TsoConfig::default()
        }),
        clock.clone(),
        metadata.clone(),
    )
    .unwrap();
    let route = service
        .ensure_timeline("allocation.idempotent.pending")
        .await
        .unwrap();
    metadata
        .create_request_record(
            &route.timeline_key,
            "pending-logical-request",
            &RequestRecord {
                schema_version: 1,
                fingerprint: AllocationRequestFingerprint {
                    timeline_key: route.timeline_key.clone(),
                    count: 1,
                },
                state: RequestRecordState::Pending,
                response: None,
                updated_at_ms: clock.now_ms(),
            },
        )
        .await
        .unwrap();

    let pending = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "pending-logical-request".to_string(),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        pending,
        TsoError::ClientRequestInProgress { timeline_key, .. }
            if timeline_key == route.timeline_key
    ));

    clock.advance(101);
    let recovered = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "pending-logical-request".to_string(),
        })
        .await
        .unwrap();
    let replayed = service
        .allocate_timestamps(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "pending-logical-request".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(replayed, recovered);
}

#[tokio::test]
async fn idempotent_allocation_fails_closed_when_completion_record_cannot_be_persisted() {
    let clock = Arc::new(ManualClock::new(21_900));
    let inner = Arc::new(MemoryMetadataStore::new());
    let metadata = Arc::new(RequestRecordCasFailureStore::new(inner.clone()));
    let service = TsoService::new(
        required_test_config(TsoConfig {
            request_record_pending_timeout_ms: 1_000,
            request_record_retention_ms: 5_000,
            ..TsoConfig::default()
        }),
        clock,
        metadata.clone(),
    )
    .unwrap();
    let route = service
        .ensure_timeline("allocation.idempotent.completion-cas-failure")
        .await
        .unwrap();
    let request = AllocateTimestampsRequest {
        timeline_key: route.timeline_key.clone(),
        count: 1,
        expected_epoch: route.epoch,
        expected_route_version: route.route_version,
        client_request_id: "completion-cas-failure".to_string(),
    };

    metadata.fail_next_request_cas();
    let error = service
        .allocate_timestamps(request.clone())
        .await
        .expect_err("allocation must not return a response that was not recorded");
    assert!(matches!(
        error,
        TsoError::Internal(message)
            if message.contains("injected request record CAS failure")
    ));

    let (record, _) = inner
        .load_request_record(&route.timeline_key, &request.client_request_id)
        .await
        .unwrap()
        .expect("failed completion should leave the request pending");
    assert_eq!(record.state, RequestRecordState::Pending);
    assert!(record.response.is_none());

    let retry_error = service
        .allocate_timestamps(request)
        .await
        .expect_err("pending request should fail closed until the pending timeout");
    assert!(matches!(
        retry_error,
        TsoError::ClientRequestInProgress { timeline_key, .. }
            if timeline_key == route.timeline_key
    ));
}

#[tokio::test]
async fn idempotent_completion_replays_recorded_response_after_cas_race() {
    let clock = Arc::new(ManualClock::new(21_950));
    let inner = Arc::new(MemoryMetadataStore::new());
    let metadata = Arc::new(RequestRecordCasFailureStore::new(inner.clone()));
    let service = TsoService::new(
        required_test_config(TsoConfig::default()),
        clock,
        metadata.clone(),
    )
    .unwrap();
    let route = service
        .ensure_timeline("allocation.idempotent.completion-cas-race")
        .await
        .unwrap();
    let request = AllocateTimestampsRequest {
        timeline_key: route.timeline_key.clone(),
        count: 2,
        expected_epoch: route.epoch,
        expected_route_version: route.route_version,
        client_request_id: "completion-cas-race".to_string(),
    };

    metadata.complete_next_request_cas_then_report_cas_failed();
    let response = service.allocate_timestamps(request.clone()).await.unwrap();
    let replayed = service.allocate_timestamps(request.clone()).await.unwrap();

    assert_eq!(replayed, response);
    let (record, _) = inner
        .load_request_record(&route.timeline_key, &request.client_request_id)
        .await
        .unwrap()
        .expect("CAS race should leave a completed request record");
    assert_eq!(record.state, RequestRecordState::Completed);
    assert_eq!(
        record
            .completed_response(&route.timeline_key)
            .unwrap()
            .as_ref(),
        Some(&response)
    );
}
