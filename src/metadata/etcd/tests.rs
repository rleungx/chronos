use crate::metrics;
use crate::{ResourceTier, TimelineLifecycleState, TsoConfig};

use super::EtcdMetadataStore;
use super::{
    cluster_format::active_identity_formats_are_compatible, identity_claim_matches_record,
    identity_record_belongs_to_ownership_plan, parse_prev_route, parse_timeline_filter_record,
    route_update_for_watch_event, verify_instance_identity_lease_record,
    InstanceIdentityLeaseRecord, RouteOnlyTimelineRecord, TimelineRoute, TimelineRouteRecord,
};
use crate::metadata::types::{CURRENT_CLUSTER_FORMAT_VERSION, CURRENT_METADATA_SCHEMA_VERSION};
use crate::metadata::{
    AllocationRequestFingerprint, RequestRecord, RequestRecordState, RouteUpdateSignal,
    TimelineRecord,
};
use etcd_client::{PutOptions, Txn, TxnOp};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::Duration;

fn sample_route(generator_id: u32, route_version: u64) -> TimelineRoute {
    TimelineRoute {
        timeline_key: "timeline-a".into(),
        generator_id,
        epoch: 1,
        route_version,
        resource_tier: ResourceTier::Shared,
        owner_worker_endpoint: "worker-a:50051".into(),
    }
}

fn sample_request_record(state: RequestRecordState, updated_at_ms: u64) -> RequestRecord {
    RequestRecord {
        schema_version: 1,
        fingerprint: AllocationRequestFingerprint {
            timeline_key: "timeline".into(),
            count: 1,
        },
        state,
        response: None,
        updated_at_ms,
    }
}

fn test_instance_identity_record() -> InstanceIdentityLeaseRecord {
    InstanceIdentityLeaseRecord {
        instance_id: "instance-a".into(),
        worker_id: "worker-a".into(),
        advertise_endpoint: "worker-a:50051".into(),
        ownership_plan_id: "plan-a".into(),
        ownership_modulo: 2,
        cluster_format_version: CURRENT_CLUSTER_FORMAT_VERSION,
    }
}

async fn test_etcd_store(label: &str) -> (EtcdMetadataStore, String) {
    let endpoints = std::env::var("CHRONOS_TEST_ETCD_ENDPOINTS")
        .unwrap_or_else(|_| "127.0.0.1:2379".into())
        .split(',')
        .map(|endpoint| endpoint.trim().to_string())
        .filter(|endpoint| !endpoint.is_empty())
        .collect();
    let prefix = format!(
        "/chronos-test-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let store = EtcdMetadataStore::connect_with_options(
        endpoints,
        prefix.clone(),
        None,
        Duration::from_millis(super::retry::DEFAULT_ETCD_REQUEST_RETRY_BUDGET_MS),
    )
    .await
    .expect("etcd store should start");
    store.rebuild_timeline_status_indexes().await.unwrap();
    (store, prefix)
}

#[test]
fn request_record_prune_predicate_only_matches_old_completed_records() {
    assert!(super::request_record_is_prunable(
        &sample_request_record(RequestRecordState::Completed, 99),
        100
    ));
    assert!(!super::request_record_is_prunable(
        &sample_request_record(RequestRecordState::Completed, 100),
        100
    ));
    assert!(!super::request_record_is_prunable(
        &sample_request_record(RequestRecordState::Pending, 50),
        100
    ));
}

#[test]
fn next_etcd_key_after_advances_without_leaving_prefix_range() {
    let prefix = "/chronos/requests/";
    let key = b"/chronos/requests/timeline/request";
    let next = super::next_etcd_key_after(key);

    assert!(next.as_slice() > key.as_slice());
    assert!(next.as_slice() < super::prefix_range_end(prefix).as_slice());
}

#[test]
fn request_record_prune_fetch_limit_is_bounded() {
    assert_eq!(
        super::request_record_prune_fetch_limit(1),
        super::REQUEST_RECORD_PRUNE_MIN_FETCH_LIMIT as i64
    );
    assert_eq!(
        super::request_record_prune_fetch_limit(usize::MAX),
        super::REQUEST_RECORD_PRUNE_MAX_FETCH_LIMIT as i64
    );
}

#[test]
fn parse_prev_route_accepts_route_only_payload() {
    let route = sample_route(7, 1);
    let payload = serde_json::json!({
        "route": route,
    });

    assert_eq!(
        parse_prev_route(payload.to_string().as_bytes()),
        Some(sample_route(7, 1))
    );
}

#[test]
fn route_update_for_watch_event_skips_same_route_when_prev_route_is_known() {
    let route = sample_route(7, 1);
    let next = route.clone();
    let payload = serde_json::json!({ "route": route });

    assert_eq!(
        route_update_for_watch_event(Some(payload.to_string().as_bytes()), &next),
        None
    );
}

#[test]
fn route_update_for_watch_event_preserves_unknown_prev_as_changed() {
    let next = sample_route(7, 1);

    assert_eq!(
        route_update_for_watch_event(Some(br#"{not-json}"#), &next),
        Some(next.clone())
    );
    assert_eq!(route_update_for_watch_event(None, &next), Some(next));
}

#[test]
fn parse_timeline_filter_record_accepts_partial_payload() {
    let payload = serde_json::json!({
        "schema_version": 1,
        "route": sample_route(7, 2),
        "state": "Recovering"
    });

    let record = parse_timeline_filter_record(payload.to_string().as_bytes()).unwrap();
    assert_eq!(record.route, sample_route(7, 2));
    assert_eq!(record.state, crate::TimelineLifecycleState::Recovering);
}

#[test]
fn timeline_route_record_deserializes_from_full_timeline_payload() {
    let payload = serde_json::json!({
        "schema_version": 1,
        "route": sample_route(7, 3),
        "state": "Recovering",
        "recovery_floor_tso": 42,
        "issued_upper_bound": 88,
        "last_graceful_issued": 77,
        "lease_expire_at_ms": 66,
        "updated_at_ms": 10
    });

    let record: TimelineRouteRecord =
        serde_json::from_slice(payload.to_string().as_bytes()).unwrap();
    assert_eq!(record.route, sample_route(7, 3));
    assert!(record.validate_schema_version().is_ok());
}

#[test]
fn route_watch_projection_accepts_newer_schema_version() {
    let next = sample_route(7, 4);
    let payload = serde_json::json!({
        "schema_version": CURRENT_METADATA_SCHEMA_VERSION + 1,
        "route": next,
        "state": "Active",
        "issued_upper_bound": 88,
    });

    let record: RouteOnlyTimelineRecord =
        serde_json::from_slice(payload.to_string().as_bytes()).unwrap();
    assert_eq!(record.route, sample_route(7, 4));
}

#[test]
fn parse_timeline_filter_record_accepts_newer_schema_version() {
    let payload = serde_json::json!({
        "schema_version": CURRENT_METADATA_SCHEMA_VERSION + 1,
        "route": sample_route(7, 5),
        "state": "Recovering",
        "issued_upper_bound": 88,
    });

    let record = parse_timeline_filter_record(payload.to_string().as_bytes()).unwrap();
    assert_eq!(record.schema_version, CURRENT_METADATA_SCHEMA_VERSION + 1);
    assert!(record.validate_schema_version().is_ok());
}

#[test]
fn record_route_watch_resync_increments_metric() {
    let before = metrics::TSO_WATCH_RESYNC_TOTAL
        .with_label_values(&["watch_stream_error"])
        .get();

    super::record_route_watch_resync("watch_stream_error");

    assert!(
        metrics::TSO_WATCH_RESYNC_TOTAL
            .with_label_values(&["watch_stream_error"])
            .get()
            > before
    );
}

#[test]
fn route_watch_reconnect_backoff_is_bounded() {
    let first = super::route_watch_reconnect_backoff(1);
    let later = super::route_watch_reconnect_backoff(64);

    assert!(first >= Duration::from_millis(super::retry::ROUTE_WATCH_MIN_RECONNECT_BACKOFF_MS));
    assert!(later <= Duration::from_millis(super::retry::ROUTE_WATCH_MAX_RECONNECT_BACKOFF_MS));
}

#[tokio::test]
async fn retry_etcd_request_retries_retryable_errors_until_success() {
    let attempts = AtomicUsize::new(0);
    let result = super::retry_etcd_request("unit_retry_success", Duration::from_secs(1), || {
        let attempt = attempts.fetch_add(1, Ordering::AcqRel);
        async move {
            if attempt == 0 {
                Err(etcd_client::Error::EndpointError("endpoint down".into()))
            } else {
                Ok(7u32)
            }
        }
    })
    .await
    .expect("retryable error should be retried");

    assert_eq!(result, 7);
    assert_eq!(attempts.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn retry_etcd_request_respects_total_budget() {
    let result =
        super::retry_etcd_request("unit_retry_budget", Duration::from_millis(1), || async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            Ok::<_, etcd_client::Error>(())
        })
        .await;

    assert!(matches!(
        result,
        Err(etcd_client::Error::GRpcStatus(status))
            if status.code() == tonic::Code::DeadlineExceeded
    ));
}

#[test]
fn etcd_request_retry_budget_covers_configured_etcd_attempt_timeout() {
    let config = TsoConfig {
        grpc_request_timeout_ms: Some(100),
        etcd_timeout_ms: Some(1_500),
        ..TsoConfig::default()
    };

    assert_eq!(
        super::etcd_request_retry_budget(&config),
        Duration::from_millis(1_500)
    );
}

#[test]
fn etcd_request_retry_budget_uses_default_when_etcd_timeout_is_lower() {
    let config = TsoConfig {
        etcd_timeout_ms: Some(100),
        ..TsoConfig::default()
    };

    assert_eq!(
        super::etcd_request_retry_budget(&config),
        Duration::from_millis(super::retry::DEFAULT_ETCD_REQUEST_RETRY_BUDGET_MS)
    );
}

#[test]
fn etcd_request_retry_budget_uses_default_when_grpc_timeout_is_lower() {
    let config = TsoConfig {
        grpc_request_timeout_ms: Some(100),
        ..TsoConfig::default()
    };

    assert_eq!(
        super::etcd_request_retry_budget(&config),
        Duration::from_millis(super::retry::DEFAULT_ETCD_REQUEST_RETRY_BUDGET_MS)
    );
}

#[tokio::test]
async fn route_watch_reset_once_suppresses_duplicate_outage_resets() {
    let (tx, mut rx) = tokio::sync::broadcast::channel(4);
    let mut reset_sent = false;

    super::send_route_watch_reset_once(&tx, &mut reset_sent);
    super::send_route_watch_reset_once(&tx, &mut reset_sent);

    assert!(matches!(rx.recv().await.unwrap(), RouteUpdateSignal::Reset));
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[test]
fn verify_instance_identity_lease_record_accepts_exact_matching_lease_and_payload() {
    let payload = serde_json::json!({
        "instance_id": "instance-a",
        "worker_id": "worker-a",
        "advertise_endpoint": "worker-a:50051",
        "ownership_plan_id": "plan-a",
        "ownership_modulo": 2,
        "cluster_format_version": CURRENT_CLUSTER_FORMAT_VERSION
    });

    verify_instance_identity_lease_record(
        17,
        17,
        payload.to_string().as_bytes(),
        &test_instance_identity_record(),
    )
    .expect("matching lease record should verify");
}

#[test]
fn verify_instance_identity_lease_record_rejects_wrong_lease_id() {
    let payload = serde_json::json!({
        "instance_id": "instance-a",
        "worker_id": "worker-a",
        "advertise_endpoint": "worker-a:50051",
        "ownership_plan_id": "plan-a",
        "ownership_modulo": 2,
        "cluster_format_version": CURRENT_CLUSTER_FORMAT_VERSION
    });

    let error = verify_instance_identity_lease_record(
        17,
        18,
        payload.to_string().as_bytes(),
        &test_instance_identity_record(),
    )
    .expect_err("wrong lease id should fail verification");

    assert!(error.to_string().contains("expected 17"));
}

#[test]
fn verify_instance_identity_lease_record_rejects_invalid_payload() {
    let error = verify_instance_identity_lease_record(
        17,
        17,
        br#"{not-json}"#,
        &test_instance_identity_record(),
    )
    .expect_err("invalid payload should fail verification");

    assert!(error.to_string().contains("decode failed"));
}

#[test]
fn verify_instance_identity_lease_record_rejects_mismatched_identity_fields() {
    let payload = serde_json::json!({
        "instance_id": "instance-a",
        "worker_id": "worker-b",
        "advertise_endpoint": "worker-a:50051",
        "ownership_plan_id": "plan-a",
        "ownership_modulo": 2,
        "cluster_format_version": CURRENT_CLUSTER_FORMAT_VERSION
    });

    let error = verify_instance_identity_lease_record(
        17,
        17,
        payload.to_string().as_bytes(),
        &test_instance_identity_record(),
    )
    .expect_err("mismatched payload should fail verification");

    assert!(error.to_string().contains("mismatched identity record"));
}

#[test]
fn identity_claim_matches_record_accepts_matching_claim() {
    let expected = test_instance_identity_record();
    let payload = serde_json::to_vec(&expected).unwrap();

    assert!(identity_claim_matches_record(17, 17, &payload, &expected)
        .expect("matching claim should parse"));
}

#[test]
fn identity_claim_matches_record_rejects_other_lease_without_error() {
    let expected = test_instance_identity_record();
    let payload = serde_json::to_vec(&expected).unwrap();

    assert!(!identity_claim_matches_record(17, 18, &payload, &expected)
        .expect("other lease should not require payload parsing"));
}

#[test]
fn identity_claim_matches_record_rejects_mismatched_payload() {
    let expected = test_instance_identity_record();
    let payload = serde_json::to_vec(&InstanceIdentityLeaseRecord {
        worker_id: "worker-b".into(),
        ..test_instance_identity_record()
    })
    .unwrap();

    assert!(!identity_claim_matches_record(17, 17, &payload, &expected)
        .expect("mismatched claim payload should parse"));
}

#[test]
fn identity_claim_matches_record_rejects_invalid_own_payload() {
    let expected = test_instance_identity_record();

    let error = identity_claim_matches_record(17, 17, br#"{not-json}"#, &expected)
        .expect_err("invalid payload on the expected lease should fail verification");

    assert!(error
        .to_string()
        .contains("claim verification decode failed"));
}

#[test]
fn ownership_plan_membership_uses_identity_plan_and_treats_legacy_as_blocking() {
    let current = InstanceIdentityLeaseRecord {
        instance_id: "instance-a".into(),
        worker_id: "worker-a".into(),
        advertise_endpoint: "worker-a:50051".into(),
        ownership_plan_id: "plan-b".into(),
        ownership_modulo: 4,
        cluster_format_version: CURRENT_CLUSTER_FORMAT_VERSION,
    };
    assert!(identity_record_belongs_to_ownership_plan(
        &current, "plan-b", 4
    ));
    assert!(!identity_record_belongs_to_ownership_plan(
        &current, "plan-a", 2
    ));

    let legacy = InstanceIdentityLeaseRecord {
        ownership_plan_id: String::new(),
        ownership_modulo: 0,
        ..current
    };
    assert!(identity_record_belongs_to_ownership_plan(
        &legacy, "plan-a", 2
    ));
}

#[test]
fn active_identity_cluster_format_rejects_legacy_workers() {
    let current = test_instance_identity_record();
    active_identity_formats_are_compatible(std::slice::from_ref(&current)).unwrap();

    let legacy = InstanceIdentityLeaseRecord {
        cluster_format_version: 0,
        ..current
    };
    let error = active_identity_formats_are_compatible(&[legacy]).unwrap_err();
    assert!(error.to_string().contains("stop every old worker"));
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_route_watch_shutdown_completes() {
    let (store, _) = test_etcd_store("watch-shutdown").await;

    tokio::time::timeout(Duration::from_secs(5), store.shutdown_route_watch())
        .await
        .expect("route watch shutdown should complete");
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_expired_lease_lock_fences_stale_holder_writes() {
    let (store, prefix) = test_etcd_store("lock-fence").await;
    let lock = store
        .try_acquire_lease_lock(
            format!("{prefix}/cluster/fence_test_lock"),
            3,
            Vec::new(),
            "fence_test_lock",
        )
        .await
        .unwrap()
        .expect("test lock should be acquired");
    let stale_fence = lock.fence_compare();
    drop(lock);
    tokio::time::sleep(Duration::from_secs(4)).await;

    let response = store
        .etcd_txn(
            "fence_test_write",
            Txn::new().when(vec![stale_fence]).and_then(vec![TxnOp::put(
                format!("{prefix}/fenced_write").as_bytes(),
                "must-not-commit",
                None,
            )]),
        )
        .await
        .unwrap();
    assert!(!response.succeeded());
    store.shutdown_route_watch().await;
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_status_index_rebuild_upgrades_stale_marker() {
    let (store, _) = test_etcd_store("index-version").await;
    let record = TimelineRecord {
        schema_version: CURRENT_METADATA_SCHEMA_VERSION,
        route: sample_route(7, 1),
        state: TimelineLifecycleState::Active,
        recovery_floor_tso: None,
        issued_upper_bound: Some(10),
        last_graceful_issued: Some(9),
        lease_expire_at_ms: Some(100),
        updated_at_ms: 1,
    };
    store
        .create_timeline_with_indexes(&record.route.timeline_key, &record)
        .await
        .unwrap();

    let marker_key = store.timeline_status_index_marker_key();
    let owner_index_key = store.timeline_status_owner_index_key(&record);
    let mut client = store.client.clone();
    client.delete(owner_index_key.clone(), None).await.unwrap();
    client.put(marker_key.clone(), "1", None).await.unwrap();

    store.rebuild_timeline_status_indexes().await.unwrap();

    let marker = client.get(marker_key, None).await.unwrap();
    assert_eq!(
        marker.kvs().first().map(|kv| kv.value()),
        Some(super::CURRENT_STATUS_INDEX_VERSION.as_bytes())
    );
    assert!(!client
        .get(owner_index_key, None)
        .await
        .unwrap()
        .kvs()
        .is_empty());
    store.shutdown_route_watch().await;
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_cluster_format_initialization_rejects_active_legacy_identity() {
    let (store, _) = test_etcd_store("format-fence").await;
    let mut client = store.client.clone();
    let lease_id = client.lease_grant(30, None).await.unwrap().id();
    let legacy_payload = serde_json::json!({
        "instance_id": "legacy-instance",
        "worker_id": "legacy-worker",
        "advertise_endpoint": "legacy-worker:50051"
    });
    client
        .put(
            store.instance_identity_key("legacy-instance"),
            legacy_payload.to_string(),
            Some(PutOptions::new().with_lease(lease_id)),
        )
        .await
        .unwrap();

    let error = store
        .initialize_cluster_format_and_indexes()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("incompatible cluster format"));

    client.lease_revoke(lease_id).await.unwrap();
    store.shutdown_route_watch().await;
}
