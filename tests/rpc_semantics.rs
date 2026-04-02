use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Code, Request};

use chronos::lifecycle::TimelineLifecycleContract;
use chronos::metadata::{
    ControlPlaneStore, EtcdMetadataStore, GeneratorLeaseAuthority, MemoryMetadataStore,
    TimelineAuthority,
};
use chronos::proto::v1::{
    timeline_control_service_server::TimelineControlService, timeline_route_event,
    timeline_route_service_server::TimelineRouteService,
    timeline_status_service_server::TimelineStatusService,
    timestamp_service_server::TimestampService, AllocateTimestampsRequest as ProtoAllocateRequest,
    EnsureTimelineRequest, ErrorCode, ErrorDetail, GetTimelineRouteRequest,
    GetTimelineStatusRequest, ListTimelineStatusesRequest, OperatorActionBlocker,
    OperatorActionNextStep, TimelineRoute as ProtoTimelineRoute, TimelineState,
    TimelineTransferReason, TransferTimelineRequest, WatchTimelineRoutesRequest,
    WorkerReadinessReason, WorkerReadinessState,
};
use chronos::rpc::{
    HealthStatusHandle, TsoControlService, TsoRouteService, TsoTimelineStatusService,
    TsoTimestampService,
};
use chronos::{
    build_commit, build_version, mixed_version_contract_id, ManualClock, ResourceTier,
    TimelineLifecycleState, TransferReason, TsoConfig, TsoSecurityMode, TsoService,
};

fn required_test_config(config: TsoConfig) -> TsoConfig {
    TsoConfig {
        security_mode: Some(TsoSecurityMode::Required),
        grpc_tls_cert_file: Some("server.crt".into()),
        grpc_tls_key_file: Some("server.key".into()),
        grpc_client_ca_file: Some("ca.pem".into()),
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..config
    }
}

fn service_with_metadata<M>(
    config: TsoConfig,
    clock: Arc<ManualClock>,
    metadata: Arc<M>,
) -> Arc<TsoService>
where
    M: ControlPlaneStore + 'static,
{
    TsoService::new(required_test_config(config), clock, metadata).unwrap()
}

fn test_etcd_endpoints() -> Vec<String> {
    std::env::var("CHRONOS_TEST_ETCD_ENDPOINTS")
        .unwrap_or_else(|_| "127.0.0.1:2379".into())
        .split(',')
        .map(|endpoint| endpoint.trim().to_string())
        .filter(|endpoint| !endpoint.is_empty())
        .collect()
}

fn unique_test_etcd_prefix(label: &str) -> String {
    format!(
        "/chronos-test-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn test_endpoint(name: &str) -> String {
    format!("{name}:50051")
}

fn all_timeline_lifecycle_states() -> [TimelineLifecycleState; 5] {
    [
        TimelineLifecycleState::Creating,
        TimelineLifecycleState::Active,
        TimelineLifecycleState::Draining,
        TimelineLifecycleState::Locked,
        TimelineLifecycleState::Recovering,
    ]
}

fn proto_timeline_state(state: TimelineLifecycleState) -> i32 {
    match state {
        TimelineLifecycleState::Creating => TimelineState::Creating as i32,
        TimelineLifecycleState::Active => TimelineState::Active as i32,
        TimelineLifecycleState::Draining => TimelineState::Draining as i32,
        TimelineLifecycleState::Locked => TimelineState::Locked as i32,
        TimelineLifecycleState::Recovering => TimelineState::Recovering as i32,
    }
}

async fn real_etcd_store(prefix: &str) -> Arc<EtcdMetadataStore> {
    Arc::new(
        EtcdMetadataStore::from_raw_endpoints_unchecked(test_etcd_endpoints(), prefix.to_string())
            .await
            .expect("etcd store should start"),
    )
}

async fn assert_owner_filtered_inventory_supports_planned_drain<M>(
    clock: Arc<ManualClock>,
    metadata: Arc<M>,
) where
    M: ControlPlaneStore + 'static,
{
    let base = TsoConfig {
        shared_generators: 8,
        generator_ownership_modulo: 2,
        ..TsoConfig::default()
    };
    let service_a = service_with_metadata(
        TsoConfig {
            worker_id: "worker-a".into(),
            advertise_endpoint: test_endpoint("endpoint-a"),
            generator_ownership_remainder: 0,
            ..base.clone()
        },
        clock.clone(),
        metadata.clone(),
    );
    let service_b = service_with_metadata(
        TsoConfig {
            worker_id: "worker-b".into(),
            advertise_endpoint: test_endpoint("endpoint-b"),
            generator_ownership_remainder: 1,
            ..base
        },
        clock,
        metadata,
    );

    let route_service_a = TsoRouteService::new(service_a.control_plane());
    let control_service_b = TsoControlService::new(service_b.control_plane());
    let status_service_from_old_owner = TsoTimelineStatusService::new(service_a.control_plane());

    for timeline_key in ["ops.inventory.timeline-a", "ops.inventory.timeline-b"] {
        route_service_a
            .ensure_timeline(Request::new(EnsureTimelineRequest {
                timeline_key: timeline_key.to_string(),
                desired_resource_tier: chronos::proto::v1::ResourceTier::Shared as i32,
            }))
            .await
            .expect("ensure_timeline should succeed");
    }

    let inventory_before = status_service_from_old_owner
        .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
            states: vec![TimelineState::Active as i32],
            owner_worker_endpoint: Some(test_endpoint("endpoint-a")),
            page_size: 10,
            page_token: String::new(),
        }))
        .await
        .expect("list before transfer should succeed")
        .into_inner();
    let before_keys: Vec<_> = inventory_before
        .statuses
        .iter()
        .map(|status| {
            status
                .route
                .as_ref()
                .expect("route should be present")
                .timeline_key
                .clone()
        })
        .collect();
    assert_eq!(
        before_keys,
        vec![
            "ops.inventory.timeline-a".to_string(),
            "ops.inventory.timeline-b".to_string()
        ]
    );
    assert!(inventory_before.next_page_token.is_empty());

    let transfer = control_service_b
        .transfer_timeline(Request::new(TransferTimelineRequest {
            timeline_key: "ops.inventory.timeline-a".to_string(),
            target_generator_id: None,
            target_worker_id: Some(test_endpoint("endpoint-b")),
            reason: TimelineTransferReason::Manual as i32,
        }))
        .await
        .expect("manual transfer should succeed")
        .into_inner();
    assert_eq!(transfer.state, TimelineState::Active as i32);

    let source_inventory_after = status_service_from_old_owner
        .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
            states: vec![TimelineState::Active as i32],
            owner_worker_endpoint: Some(test_endpoint("endpoint-a")),
            page_size: 10,
            page_token: String::new(),
        }))
        .await
        .expect("list on old owner after transfer should succeed")
        .into_inner();
    let source_keys_after: Vec<_> = source_inventory_after
        .statuses
        .iter()
        .map(|status| {
            status
                .route
                .as_ref()
                .expect("route should be present")
                .timeline_key
                .clone()
        })
        .collect();
    assert_eq!(
        source_keys_after,
        vec!["ops.inventory.timeline-b".to_string()]
    );

    let target_inventory_after = status_service_from_old_owner
        .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
            states: vec![TimelineState::Active as i32],
            owner_worker_endpoint: Some(test_endpoint("endpoint-b")),
            page_size: 10,
            page_token: String::new(),
        }))
        .await
        .expect("list on old owner for new owner filter should succeed")
        .into_inner();
    assert_eq!(target_inventory_after.statuses.len(), 1);
    let list_row = target_inventory_after
        .statuses
        .first()
        .expect("target inventory row should exist");
    let list_route = list_row.route.as_ref().expect("route should be present");
    assert_eq!(list_route.timeline_key, "ops.inventory.timeline-a");
    assert_eq!(
        list_route.owner_worker_endpoint,
        test_endpoint("endpoint-b")
    );
    assert_eq!(list_route.generator_id, transfer.new_generator_id);
    assert_eq!(list_route.epoch, transfer.new_epoch);
    assert_eq!(list_route.route_version, transfer.route_version);
    assert_eq!(list_row.state, TimelineState::Active as i32);
    assert!(target_inventory_after.next_page_token.is_empty());

    let point_read = status_service_from_old_owner
        .get_timeline_status(Request::new(GetTimelineStatusRequest {
            timeline_key: "ops.inventory.timeline-a".to_string(),
        }))
        .await
        .expect("point read after transfer should succeed")
        .into_inner()
        .status
        .expect("point read should contain status");
    let point_route = point_read.route.as_ref().expect("route should be present");
    assert_eq!(point_route.timeline_key, list_route.timeline_key);
    assert_eq!(
        point_route.owner_worker_endpoint,
        list_route.owner_worker_endpoint
    );
    assert_eq!(point_route.generator_id, list_route.generator_id);
    assert_eq!(point_route.epoch, list_route.epoch);
    assert_eq!(point_route.route_version, list_route.route_version);
    assert_eq!(point_read.state, list_row.state);
}

async fn assert_list_timeline_statuses_paginates_across_pages<M>(
    clock: Arc<ManualClock>,
    metadata: Arc<M>,
) where
    M: ControlPlaneStore + 'static,
{
    let owner_worker_endpoint = test_endpoint("endpoint-a");
    let service = service_with_metadata(
        TsoConfig {
            worker_id: "worker-a".into(),
            advertise_endpoint: owner_worker_endpoint.clone(),
            ..TsoConfig::default()
        },
        clock,
        metadata,
    );
    let route_service = TsoRouteService::new(service.control_plane());
    let status_service = TsoTimelineStatusService::new(service.control_plane());

    for timeline_key in [
        "ops.pagination.timeline-c",
        "ops.pagination.timeline-a",
        "ops.pagination.timeline-b",
    ] {
        route_service
            .ensure_timeline(Request::new(EnsureTimelineRequest {
                timeline_key: timeline_key.to_string(),
                desired_resource_tier: chronos::proto::v1::ResourceTier::Shared as i32,
            }))
            .await
            .expect("ensure_timeline should succeed");
    }

    let first_page = status_service
        .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
            states: vec![TimelineState::Active as i32],
            owner_worker_endpoint: Some(owner_worker_endpoint.clone()),
            page_size: 1,
            page_token: String::new(),
        }))
        .await
        .expect("first pagination page should succeed")
        .into_inner();
    assert_eq!(first_page.statuses.len(), 1);
    let first_key = first_page.statuses[0]
        .route
        .as_ref()
        .expect("route should be present")
        .timeline_key
        .clone();
    assert_eq!(first_key, "ops.pagination.timeline-a");
    assert!(!first_page.next_page_token.is_empty());

    let second_page = status_service
        .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
            states: vec![TimelineState::Active as i32, TimelineState::Active as i32],
            owner_worker_endpoint: Some(owner_worker_endpoint.clone()),
            page_size: 1,
            page_token: first_page.next_page_token.clone(),
        }))
        .await
        .expect("second pagination page should succeed")
        .into_inner();
    assert_eq!(second_page.statuses.len(), 1);
    let second_key = second_page.statuses[0]
        .route
        .as_ref()
        .expect("route should be present")
        .timeline_key
        .clone();
    assert_eq!(second_key, "ops.pagination.timeline-b");
    assert!(!second_page.next_page_token.is_empty());

    let mismatched_filter_error = status_service
        .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
            states: vec![TimelineState::Active as i32],
            owner_worker_endpoint: Some(test_endpoint("endpoint-b")),
            page_size: 1,
            page_token: second_page.next_page_token.clone(),
        }))
        .await
        .expect_err("mismatched owner filter should reject the page token");
    assert_eq!(mismatched_filter_error.code(), Code::InvalidArgument);

    let third_page = status_service
        .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
            states: vec![TimelineState::Active as i32],
            owner_worker_endpoint: Some(owner_worker_endpoint),
            page_size: 1,
            page_token: second_page.next_page_token,
        }))
        .await
        .expect("third pagination page should succeed")
        .into_inner();
    assert_eq!(third_page.statuses.len(), 1);
    let third_key = third_page.statuses[0]
        .route
        .as_ref()
        .expect("route should be present")
        .timeline_key
        .clone();
    assert_eq!(third_key, "ops.pagination.timeline-c");
    assert!(third_page.next_page_token.is_empty());

    assert_eq!(
        vec![first_key, second_key, third_key],
        vec![
            "ops.pagination.timeline-a".to_string(),
            "ops.pagination.timeline-b".to_string(),
            "ops.pagination.timeline-c".to_string(),
        ]
    );
}

async fn next_route_event(
    stream: &mut ReceiverStream<Result<chronos::proto::v1::TimelineRouteEvent, tonic::Status>>,
    timeout: Duration,
) -> ProtoTimelineRoute {
    loop {
        let event = tokio::time::timeout(timeout, stream.next())
            .await
            .expect("expected route watch event before timeout")
            .expect("stream should yield an event")
            .expect("event should be ok");
        match event.event.expect("route event should be present") {
            timeline_route_event::Event::Route(route_event) => return route_event,
            timeline_route_event::Event::Keepalive(_) => continue,
            timeline_route_event::Event::Tombstone(other) => {
                panic!("unexpected tombstone route event: {:?}", other)
            }
        }
    }
}

async fn wait_for_latest_route_version(
    stream: &mut ReceiverStream<Result<chronos::proto::v1::TimelineRouteEvent, tonic::Status>>,
    expected_route_version: u64,
    expected_cache_ttl_ms: u32,
    deadline: Duration,
) {
    let end = tokio::time::Instant::now() + deadline;
    while tokio::time::Instant::now() < end {
        let remaining = end.saturating_duration_since(tokio::time::Instant::now());
        let event = match tokio::time::timeout(remaining.min(Duration::from_secs(1)), stream.next())
            .await
        {
            Ok(Some(Ok(event))) => event,
            Ok(Some(Err(error))) => panic!("route watch event should be ok: {:?}", error),
            Ok(None) => panic!("route watch stream closed before route version catch-up"),
            Err(_) => continue,
        };
        match event.event.expect("route event should be present") {
            timeline_route_event::Event::Route(route_event) => {
                assert_eq!(route_event.cache_ttl_ms, expected_cache_ttl_ms);
                if route_event.route_version == expected_route_version {
                    return;
                }
            }
            timeline_route_event::Event::Keepalive(_) => continue,
            timeline_route_event::Event::Tombstone(other) => {
                panic!("unexpected tombstone route event: {:?}", other)
            }
        }
    }
    panic!(
        "expected lagged resync to deliver latest route version {}",
        expected_route_version
    );
}

async fn wait_for_external_watch_ready(
    owner_service: &Arc<TsoService>,
    watcher_service: &Arc<TsoService>,
    label: &str,
) {
    let mut route_updates = watcher_service.subscribe_route_changes();
    for attempt in 0..5 {
        let warmup_key = format!("watch.ready.{label}.{attempt}");
        let route = owner_service.ensure_timeline(&warmup_key).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let received = tokio::time::timeout(
                remaining.min(Duration::from_millis(500)),
                route_updates.recv(),
            )
            .await;
            let Ok(Ok(update)) = received else {
                break;
            };
            if update.timeline_key == route.timeline_key {
                return;
            }
        }
    }
    panic!("watcher service never observed an external route update for {label}");
}

#[tokio::test]
async fn watch_timeline_routes_emits_initial_snapshot_for_stale_known_versions() {
    let clock = Arc::new(ManualClock::new(1_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(TsoConfig::default(), clock, metadata);
    let route = service
        .ensure_timeline("watch.initial.snapshot")
        .await
        .unwrap();
    let route_service = TsoRouteService::new(service.control_plane());

    let request = WatchTimelineRoutesRequest {
        timeline_keys: vec![route.timeline_key.clone()],
        known_route_versions: HashMap::from([(route.timeline_key.clone(), 0)]),
        sdk_instance_id: "sdk-1".to_string(),
    };
    let mut stream = route_service
        .watch_timeline_routes(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    let event = tokio::time::timeout(Duration::from_millis(200), stream.next())
        .await
        .expect("expected initial route event")
        .expect("stream should yield an event")
        .expect("event should be ok");

    match event.event.expect("route event should be present") {
        timeline_route_event::Event::Route(route_event) => {
            assert_eq!(route_event.timeline_key, route.timeline_key);
            assert_eq!(route_event.route_version, route.route_version);
            assert_eq!(route_event.epoch, route.epoch);
        }
        other => panic!("unexpected initial watch event: {:?}", other),
    }
}

#[tokio::test]
async fn watch_timeline_routes_watch_all_emits_authoritative_initial_inventory() {
    let clock = Arc::new(ManualClock::new(1_250));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(TsoConfig::default(), clock, metadata);
    let first = service
        .ensure_timeline("watch.all.inventory.a")
        .await
        .unwrap();
    let second = service
        .ensure_timeline("watch.all.inventory.b")
        .await
        .unwrap();
    let route_service = TsoRouteService::new(service.control_plane());

    let request = WatchTimelineRoutesRequest {
        timeline_keys: Vec::new(),
        known_route_versions: HashMap::new(),
        sdk_instance_id: "sdk-watch-all".to_string(),
    };
    let mut stream = route_service
        .watch_timeline_routes(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    let mut actual = vec![
        next_route_event(&mut stream, Duration::from_millis(200))
            .await
            .timeline_key,
        next_route_event(&mut stream, Duration::from_millis(200))
            .await
            .timeline_key,
    ];
    actual.sort();

    let mut expected = vec![first.timeline_key, second.timeline_key];
    expected.sort();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn watch_timeline_routes_filtered_watch_does_not_leak_request_external_known_keys() {
    let clock = Arc::new(ManualClock::new(1_375));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(TsoConfig::default(), clock, metadata);
    let allowed = service
        .ensure_timeline("watch.filtered.allowed")
        .await
        .unwrap();
    let outside = service
        .ensure_timeline("watch.filtered.outside")
        .await
        .unwrap();
    let route_service = TsoRouteService::new(service.control_plane());

    let request = WatchTimelineRoutesRequest {
        timeline_keys: vec![allowed.timeline_key.clone()],
        known_route_versions: HashMap::from([
            (allowed.timeline_key.clone(), 0),
            (outside.timeline_key.clone(), 0),
        ]),
        sdk_instance_id: "sdk-filtered-domain".to_string(),
    };
    let mut stream = route_service
        .watch_timeline_routes(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    let initial = next_route_event(&mut stream, Duration::from_millis(200)).await;
    assert_eq!(initial.timeline_key, allowed.timeline_key);
    assert_eq!(initial.route_version, allowed.route_version);

    let next = tokio::time::timeout(Duration::from_millis(100), stream.next()).await;
    assert!(
        next.is_err(),
        "filtered watch leaked a request-external timeline into the response"
    );
}

#[tokio::test]
async fn watch_timeline_routes_receives_cross_service_updates_from_shared_metadata() {
    let clock = Arc::new(ManualClock::new(1_500));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service_a = service_with_metadata(
        TsoConfig {
            worker_id: "worker-a".to_string(),
            advertise_endpoint: test_endpoint("endpoint-a"),
            ..TsoConfig::default()
        },
        clock.clone(),
        metadata.clone(),
    );
    let service_b = service_with_metadata(
        TsoConfig {
            worker_id: "worker-b".to_string(),
            advertise_endpoint: test_endpoint("endpoint-b"),
            ..TsoConfig::default()
        },
        clock,
        metadata,
    );
    let route_service = TsoRouteService::new(service_b.control_plane());
    let timeline_key = "watch.cross.service".to_string();

    let request = WatchTimelineRoutesRequest {
        timeline_keys: vec![timeline_key.clone()],
        known_route_versions: HashMap::new(),
        sdk_instance_id: "sdk-2".to_string(),
    };
    let mut stream = route_service
        .watch_timeline_routes(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    let route = service_a.ensure_timeline(&timeline_key).await.unwrap();
    let event = tokio::time::timeout(Duration::from_millis(200), stream.next())
        .await
        .expect("expected cross-service route event")
        .expect("stream should yield an event")
        .expect("event should be ok");

    match event.event.expect("route event should be present") {
        timeline_route_event::Event::Route(route_event) => {
            assert_eq!(route_event.timeline_key, route.timeline_key);
            assert_eq!(route_event.route_version, route.route_version);
            assert_eq!(
                route_event.owner_worker_endpoint,
                route.owner_worker_endpoint
            );
        }
        other => panic!("unexpected cross-service watch event: {:?}", other),
    }
}

#[tokio::test]
async fn watch_timeline_routes_ignores_authority_window_updates() {
    let clock = Arc::new(ManualClock::new(1_750));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(TsoConfig::default(), clock, metadata);
    let route_service = TsoRouteService::new(service.control_plane());
    let timeline_key = "watch.lease.renewal";

    let request = WatchTimelineRoutesRequest {
        timeline_keys: vec![timeline_key.to_string()],
        known_route_versions: HashMap::new(),
        sdk_instance_id: "sdk-lease".to_string(),
    };
    let mut stream = route_service
        .watch_timeline_routes(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    let route = service.ensure_timeline(timeline_key).await.unwrap();
    let event = tokio::time::timeout(Duration::from_millis(200), stream.next())
        .await
        .expect("expected initial route event")
        .expect("stream should yield an event")
        .expect("event should be ok");

    match event.event.expect("route event should be present") {
        timeline_route_event::Event::Route(route_event) => {
            assert_eq!(route_event.timeline_key, route.timeline_key);
            assert_eq!(route_event.route_version, route.route_version);
        }
        other => panic!("unexpected initial watch event: {:?}", other),
    }

    service.renew_timeline_lease(timeline_key).await.unwrap();
    let no_follow_up = tokio::time::timeout(Duration::from_millis(100), stream.next()).await;
    assert!(
        no_follow_up.is_err(),
        "lease renewal should not emit a route update"
    );
}

#[tokio::test]
async fn restarting_service_reclaims_dedicated_generator_claims() {
    let config = TsoConfig {
        shared_generators: 2,
        warm_generators: 0,
        ..TsoConfig::default()
    };
    let clock = Arc::new(ManualClock::new(2_000));
    let metadata = Arc::new(MemoryMetadataStore::new());

    let service_before = service_with_metadata(config.clone(), clock.clone(), metadata.clone());
    let existing = service_before
        .ensure_timeline_with_tier("dedicated.existing", ResourceTier::Dedicated)
        .await
        .unwrap();

    let service_after = service_with_metadata(config, clock, metadata);
    let reloaded = service_after
        .ensure_timeline_with_tier("dedicated.existing", ResourceTier::Dedicated)
        .await
        .unwrap();
    let new_route = service_after
        .ensure_timeline_with_tier("dedicated.new", ResourceTier::Dedicated)
        .await
        .unwrap();

    assert_eq!(reloaded.generator_id, existing.generator_id);
    assert_ne!(new_route.generator_id, existing.generator_id);
}

#[tokio::test]
async fn timestamp_rpc_returns_structured_error_details() {
    let clock = Arc::new(ManualClock::new(3_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(TsoConfig::default(), clock, metadata);
    let route = service.ensure_timeline("rpc.error.detail").await.unwrap();
    let timestamp_service = TsoTimestampService::new(service.data_plane());

    let error = timestamp_service
        .allocate_timestamps(Request::new(ProtoAllocateRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: 0,
            client_request_id: "rpc-error-detail".to_string(),
            request_timeout_ms: 0,
        }))
        .await
        .unwrap_err();

    let detail = ErrorDetail::decode(error.details()).expect("error detail should decode");
    assert_eq!(detail.code, ErrorCode::RouteVersionMismatch as i32);
    assert_eq!(detail.current_route_version, route.route_version);
    assert!(detail.message.contains("route version mismatch"));
}

#[tokio::test]
async fn timeline_public_surfaces_follow_lifecycle_contract_for_direct_unavailability_states() {
    for (index, state) in all_timeline_lifecycle_states().into_iter().enumerate() {
        if TimelineLifecycleContract::classify(state)
            .direct_public_unavailability()
            .is_none()
        {
            continue;
        }

        let clock = Arc::new(ManualClock::new(3_100 + index as u64));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = service_with_metadata(TsoConfig::default(), clock.clone(), metadata.clone());
        let route = service
            .ensure_timeline(format!("rpc.{state}.timeline").as_str())
            .await
            .unwrap();

        let (mut record, revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .expect("timeline should exist");
        record.state = state;
        metadata
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();

        let service_after = service_with_metadata(TsoConfig::default(), clock, metadata);
        let timestamp_service = TsoTimestampService::new(service_after.data_plane());
        let status_service = TsoTimelineStatusService::new(service_after.control_plane());

        let status = status_service
            .get_timeline_status(Request::new(GetTimelineStatusRequest {
                timeline_key: route.timeline_key.clone(),
            }))
            .await
            .unwrap()
            .into_inner()
            .status
            .expect("status should be present");
        assert_eq!(status.state, proto_timeline_state(state));

        let error = timestamp_service
            .allocate_timestamps(Request::new(ProtoAllocateRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: format!("rpc-{state}-unavailable"),
                request_timeout_ms: 0,
            }))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::Unavailable);
        let detail = ErrorDetail::decode(error.details()).expect("error detail should decode");
        assert_eq!(detail.code, ErrorCode::TemporarilyUnavailable as i32);
        assert!(detail.message.contains("timeline not ready"));
        assert!(detail.message.contains(&format!("state={state}")));
    }
}

#[tokio::test]
async fn transfer_timeline_rpc_reports_failover_lease_blocker_in_error_detail() {
    let config = TsoConfig {
        lease_ttl_ms: 5,
        generator_maintenance_interval_ms: 1,
        ..TsoConfig::default()
    };
    let shared_generators = config.shared_generators;
    let clock = Arc::new(ManualClock::new(3_200));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(config, clock, metadata);
    let control_service = TsoControlService::new(service.control_plane());
    let route = service
        .ensure_timeline("rpc.failover.lease.blocker")
        .await
        .unwrap();

    let error = control_service
        .transfer_timeline(Request::new(TransferTimelineRequest {
            timeline_key: route.timeline_key.clone(),
            target_generator_id: Some((route.generator_id + 1) % shared_generators),
            target_worker_id: Some(test_endpoint("endpoint-b")),
            reason: TimelineTransferReason::Failover as i32,
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::FailedPrecondition);
    let detail = ErrorDetail::decode(error.details()).expect("error detail should decode");
    assert_eq!(detail.code, ErrorCode::TemporarilyUnavailable as i32);
    assert_eq!(
        detail.action_blocker,
        OperatorActionBlocker::LeaseNotExpired as i32
    );
    assert_eq!(
        detail.next_step,
        OperatorActionNextStep::WaitForLeaseExpiry as i32
    );
    assert!(detail.message.contains("failover requires expired lease"));
}

#[tokio::test]
async fn transfer_timeline_rpc_reports_failover_floor_blocker_in_error_detail() {
    let config = TsoConfig {
        lease_ttl_ms: 5,
        generator_maintenance_interval_ms: 1,
        ..TsoConfig::default()
    };
    let shared_generators = config.shared_generators;
    let clock = Arc::new(ManualClock::new(3_300));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(config, clock.clone(), metadata.clone());
    let control_service = TsoControlService::new(service.control_plane());
    let route = service
        .ensure_timeline("rpc.failover.floor.blocker")
        .await
        .unwrap();

    let (mut generator_record, revision) = metadata
        .load_generator(route.generator_id)
        .await
        .unwrap()
        .expect("generator should exist");
    generator_record.last_issued_tso = None;
    generator_record.issued_upper_bound = None;
    metadata
        .compare_exchange_generator(route.generator_id, revision, &generator_record)
        .await
        .unwrap();

    clock.advance(10);
    let error = control_service
        .transfer_timeline(Request::new(TransferTimelineRequest {
            timeline_key: route.timeline_key.clone(),
            target_generator_id: Some((route.generator_id + 1) % shared_generators),
            target_worker_id: Some(test_endpoint("endpoint-b")),
            reason: TimelineTransferReason::Failover as i32,
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::FailedPrecondition);
    let detail = ErrorDetail::decode(error.details()).expect("error detail should decode");
    assert_eq!(detail.code, ErrorCode::TemporarilyUnavailable as i32);
    assert_eq!(
        detail.action_blocker,
        OperatorActionBlocker::RecoveryFloorMissing as i32
    );
    assert_eq!(
        detail.next_step,
        OperatorActionNextStep::PersistRecoveryFloor as i32
    );
    assert!(detail.message.contains("failover missing recovery floor"));
}

#[tokio::test]
async fn list_timeline_statuses_rpc_supports_owner_filtered_planned_drain_inventory() {
    let clock = Arc::new(ManualClock::new(3_400));
    let metadata = Arc::new(MemoryMetadataStore::new());
    assert_owner_filtered_inventory_supports_planned_drain(clock, metadata).await;
}

#[tokio::test]
async fn list_timeline_statuses_rpc_paginates_across_pages() {
    let clock = Arc::new(ManualClock::new(3_450));
    let metadata = Arc::new(MemoryMetadataStore::new());
    assert_list_timeline_statuses_paginates_across_pages(clock, metadata).await;
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_list_timeline_statuses_rpc_supports_owner_filtered_planned_drain_inventory() {
    let clock = Arc::new(ManualClock::new(3_400));
    let prefix = unique_test_etcd_prefix("owner-filtered-planned-drain");
    let metadata = real_etcd_store(&prefix).await;
    assert_owner_filtered_inventory_supports_planned_drain(clock, metadata).await;
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_list_timeline_statuses_rpc_paginates_across_pages() {
    let clock = Arc::new(ManualClock::new(3_450));
    let prefix = unique_test_etcd_prefix("list-pagination");
    let metadata = real_etcd_store(&prefix).await;
    assert_list_timeline_statuses_paginates_across_pages(clock, metadata).await;
}

#[tokio::test]
async fn health_rpc_reports_configured_instance_id() {
    let clock = Arc::new(ManualClock::new(3_500));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(
        TsoConfig {
            worker_id: "worker-a".to_string(),
            instance_id: "instance-a-stable".to_string(),
            advertise_endpoint: test_endpoint("endpoint-a"),
            ..TsoConfig::default()
        },
        clock,
        metadata,
    );
    let control_service = TsoControlService::new(service.control_plane());

    let response = control_service
        .health(Request::new(()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.instance_id, "instance-a-stable");
    assert_eq!(response.worker_id, "worker-a");
    assert_eq!(response.advertise_endpoint, test_endpoint("endpoint-a"));
    assert_eq!(response.readiness_state, WorkerReadinessState::Ready as i32);
    assert_eq!(
        response.readiness_reason,
        WorkerReadinessReason::Serving as i32
    );
    assert!(response.identity_lease_healthy);
}

#[tokio::test]
async fn health_rpc_derives_instance_id_from_advertise_endpoint_when_unset() {
    let clock = Arc::new(ManualClock::new(3_500));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(
        TsoConfig {
            worker_id: "worker-a".to_string(),
            advertise_endpoint: test_endpoint("endpoint-derived"),
            ..TsoConfig::default()
        },
        clock,
        metadata,
    );
    let control_service = TsoControlService::new(service.control_plane());

    let response = control_service
        .health(Request::new(()))
        .await
        .unwrap()
        .into_inner();
    assert!(response.instance_id.starts_with("endpoint-derived:50051#"));
    assert_eq!(response.worker_id, "worker-a");
    assert_eq!(
        response.advertise_endpoint,
        test_endpoint("endpoint-derived")
    );
    assert_eq!(response.readiness_state, WorkerReadinessState::Ready as i32);
    assert_eq!(
        response.readiness_reason,
        WorkerReadinessReason::Serving as i32
    );
    assert!(response.identity_lease_healthy);
    assert_eq!(response.build_version, build_version());
    assert_eq!(response.build_commit, build_commit());
    assert_eq!(
        response.mixed_version_contract_id,
        mixed_version_contract_id()
    );
}

#[tokio::test]
async fn health_rpc_reports_identity_lease_loss_as_degraded_and_unhealthy() {
    let clock = Arc::new(ManualClock::new(3_600));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(
        TsoConfig {
            worker_id: "worker-a".to_string(),
            instance_id: "instance-a-stable".to_string(),
            advertise_endpoint: test_endpoint("endpoint-a"),
            ..TsoConfig::default()
        },
        clock,
        metadata,
    );
    let health_status = HealthStatusHandle::serving(&service.health());
    health_status.mark_identity_lease_lost();
    let control_service =
        TsoControlService::with_health_status(service.control_plane(), health_status);

    let response = control_service
        .health(Request::new(()))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.instance_id, "instance-a-stable");
    assert_eq!(response.worker_id, "worker-a");
    assert_eq!(response.advertise_endpoint, test_endpoint("endpoint-a"));
    assert_eq!(
        response.readiness_state,
        WorkerReadinessState::Degraded as i32
    );
    assert_eq!(
        response.readiness_reason,
        WorkerReadinessReason::IdentityLeaseLost as i32
    );
    assert!(!response.identity_lease_healthy);
}

#[tokio::test]
async fn health_rpc_keeps_identity_lease_loss_reason_after_shutdown_signal() {
    let clock = Arc::new(ManualClock::new(3_700));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(
        TsoConfig {
            worker_id: "worker-a".to_string(),
            instance_id: "instance-a-stable".to_string(),
            advertise_endpoint: test_endpoint("endpoint-a"),
            ..TsoConfig::default()
        },
        clock,
        metadata,
    );
    let health_status = HealthStatusHandle::serving(&service.health());
    health_status.mark_identity_lease_lost();
    health_status.mark_shutting_down();
    let control_service =
        TsoControlService::with_health_status(service.control_plane(), health_status);

    let response = control_service
        .health(Request::new(()))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response.readiness_state,
        WorkerReadinessState::Degraded as i32
    );
    assert_eq!(
        response.readiness_reason,
        WorkerReadinessReason::IdentityLeaseLost as i32
    );
    assert!(!response.identity_lease_healthy);
}

#[tokio::test]
async fn route_responses_use_configured_cache_ttl() {
    let clock = Arc::new(ManualClock::new(3_800));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(
        TsoConfig {
            route_cache_ttl_ms: 12_345,
            ..TsoConfig::default()
        },
        clock,
        metadata,
    );
    let route_service = TsoRouteService::new(service.control_plane());

    let ensured = route_service
        .ensure_timeline(Request::new(EnsureTimelineRequest {
            timeline_key: "route.ttl.ensure".to_string(),
            desired_resource_tier: chronos::proto::v1::ResourceTier::Shared as i32,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ensured.route.unwrap().cache_ttl_ms, 12_345);

    let fetched = route_service
        .get_timeline_route(Request::new(GetTimelineRouteRequest {
            timeline_key: "route.ttl.ensure".to_string(),
            cached_route_version: Some(0),
            sdk_instance_id: "sdk-route-ttl".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.route.unwrap().cache_ttl_ms, 12_345);
}

#[tokio::test]
async fn watch_route_events_use_configured_cache_ttl() {
    let clock = Arc::new(ManualClock::new(3_900));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(
        TsoConfig {
            route_cache_ttl_ms: 54_321,
            ..TsoConfig::default()
        },
        clock,
        metadata,
    );
    let route = service.ensure_timeline("route.ttl.watch").await.unwrap();
    let route_service = TsoRouteService::new(service.control_plane());

    let request = WatchTimelineRoutesRequest {
        timeline_keys: vec![route.timeline_key.clone()],
        known_route_versions: HashMap::from([(route.timeline_key.clone(), 0)]),
        sdk_instance_id: "sdk-ttl".to_string(),
    };
    let mut stream = route_service
        .watch_timeline_routes(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    let event = tokio::time::timeout(Duration::from_millis(200), stream.next())
        .await
        .expect("expected initial route event")
        .expect("stream should yield an event")
        .expect("event should be ok");

    match event.event.expect("route event should be present") {
        timeline_route_event::Event::Route(route_event) => {
            assert_eq!(route_event.cache_ttl_ms, 54_321);
        }
        other => panic!("unexpected watch ttl event: {:?}", other),
    }
}

#[tokio::test]
async fn watch_followup_route_events_use_configured_cache_ttl() {
    let clock = Arc::new(ManualClock::new(4_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(
        TsoConfig {
            route_cache_ttl_ms: 7_777,
            ..TsoConfig::default()
        },
        clock,
        metadata,
    );
    let route_service = TsoRouteService::new(service.control_plane());
    let initial_route = service.ensure_timeline("route.ttl.followup").await.unwrap();

    let request = WatchTimelineRoutesRequest {
        timeline_keys: vec![initial_route.timeline_key.clone()],
        known_route_versions: HashMap::from([(
            initial_route.timeline_key.clone(),
            initial_route.route_version,
        )]),
        sdk_instance_id: "sdk-followup-ttl".to_string(),
    };
    let mut stream = route_service
        .watch_timeline_routes(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    let target_generator = if initial_route.generator_id == 0 {
        1
    } else {
        0
    };
    let _ = service
        .control_plane()
        .transfer_timeline_for_rpc(
            &initial_route.timeline_key,
            service.advertise_endpoint().to_string(),
            Some(target_generator),
            TransferReason::Rebalance,
        )
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_millis(200), stream.next())
        .await
        .expect("expected follow-up route event")
        .expect("stream should yield an event")
        .expect("event should be ok");

    match event.event.expect("route event should be present") {
        timeline_route_event::Event::Route(route_event) => {
            assert_eq!(route_event.cache_ttl_ms, 7_777);
            assert!(route_event.route_version > initial_route.route_version);
        }
        other => panic!("unexpected follow-up watch event: {:?}", other),
    }
}

#[tokio::test]
async fn watch_lagged_resync_route_events_keep_configured_cache_ttl() {
    let clock = Arc::new(ManualClock::new(4_100));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(
        TsoConfig {
            route_cache_ttl_ms: 9_999,
            ..TsoConfig::default()
        },
        clock,
        metadata,
    );
    let route_service = TsoRouteService::new(service.control_plane());
    let initial_route = service.ensure_timeline("route.ttl.lagged").await.unwrap();

    let request = WatchTimelineRoutesRequest {
        timeline_keys: vec![initial_route.timeline_key.clone()],
        known_route_versions: HashMap::from([(
            initial_route.timeline_key.clone(),
            initial_route.route_version,
        )]),
        sdk_instance_id: "sdk-lagged-ttl".to_string(),
    };
    let mut stream = route_service
        .watch_timeline_routes(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    let mut current_generator = initial_route.generator_id;
    for _ in 0..1400 {
        current_generator = if current_generator == 0 { 1 } else { 0 };
        let _ = service
            .control_plane()
            .transfer_timeline_for_rpc(
                &initial_route.timeline_key,
                service.advertise_endpoint().to_string(),
                Some(current_generator),
                TransferReason::Rebalance,
            )
            .await
            .unwrap();
    }

    let latest_route = service
        .get_timeline_route(&initial_route.timeline_key)
        .await
        .unwrap();

    wait_for_latest_route_version(
        &mut stream,
        latest_route.route_version,
        9_999,
        Duration::from_secs(3),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_watch_timeline_routes_receives_cross_service_updates_from_shared_metadata() {
    let prefix = unique_test_etcd_prefix("rpc-watch-cross-service");
    let clock = Arc::new(ManualClock::new(5_000));
    let service_a = service_with_metadata(
        TsoConfig {
            worker_id: "worker-a".to_string(),
            advertise_endpoint: test_endpoint("endpoint-a"),
            ..TsoConfig::default()
        },
        clock.clone(),
        real_etcd_store(&prefix).await,
    );
    let service_b = service_with_metadata(
        TsoConfig {
            worker_id: "worker-b".to_string(),
            advertise_endpoint: test_endpoint("endpoint-b"),
            ..TsoConfig::default()
        },
        clock,
        real_etcd_store(&prefix).await,
    );
    wait_for_external_watch_ready(&service_a, &service_b, "cross-service").await;

    let route_service = TsoRouteService::new(service_b.control_plane());
    let timeline_key = "watch.cross.service.etcd".to_string();
    let request = WatchTimelineRoutesRequest {
        timeline_keys: vec![timeline_key.clone()],
        known_route_versions: HashMap::new(),
        sdk_instance_id: "sdk-etcd-cross-service".to_string(),
    };
    let mut stream = route_service
        .watch_timeline_routes(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    let route = service_a.ensure_timeline(&timeline_key).await.unwrap();
    let route_event = next_route_event(&mut stream, Duration::from_secs(5)).await;
    assert_eq!(route_event.timeline_key, route.timeline_key);
    assert_eq!(route_event.route_version, route.route_version);
    assert_eq!(
        route_event.owner_worker_endpoint,
        route.owner_worker_endpoint
    );

    service_b.shutdown().await;
    service_a.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_watch_followup_route_events_use_configured_cache_ttl_after_watcher_restart() {
    let prefix = unique_test_etcd_prefix("rpc-watch-restart");
    let clock = Arc::new(ManualClock::new(5_500));
    let owner_service = service_with_metadata(
        TsoConfig {
            worker_id: "worker-owner".to_string(),
            advertise_endpoint: test_endpoint("endpoint-owner"),
            ..TsoConfig::default()
        },
        clock.clone(),
        real_etcd_store(&prefix).await,
    );
    let watcher_before = service_with_metadata(
        TsoConfig {
            worker_id: "worker-watcher".to_string(),
            advertise_endpoint: test_endpoint("endpoint-watcher"),
            route_cache_ttl_ms: 7_777,
            ..TsoConfig::default()
        },
        clock.clone(),
        real_etcd_store(&prefix).await,
    );
    wait_for_external_watch_ready(&owner_service, &watcher_before, "restart-before").await;

    let initial_route = owner_service
        .ensure_timeline("route.ttl.followup.etcd")
        .await
        .unwrap();
    watcher_before.shutdown().await;

    let watcher_after = service_with_metadata(
        TsoConfig {
            worker_id: "worker-watcher".to_string(),
            advertise_endpoint: test_endpoint("endpoint-watcher"),
            route_cache_ttl_ms: 7_777,
            ..TsoConfig::default()
        },
        clock,
        real_etcd_store(&prefix).await,
    );
    wait_for_external_watch_ready(&owner_service, &watcher_after, "restart-after").await;

    let route_service = TsoRouteService::new(watcher_after.control_plane());
    let request = WatchTimelineRoutesRequest {
        timeline_keys: vec![initial_route.timeline_key.clone()],
        known_route_versions: HashMap::from([(
            initial_route.timeline_key.clone(),
            initial_route.route_version,
        )]),
        sdk_instance_id: "sdk-followup-etcd".to_string(),
    };
    let mut stream = route_service
        .watch_timeline_routes(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    let target_generator = if initial_route.generator_id == 0 {
        1
    } else {
        0
    };
    owner_service
        .control_plane()
        .transfer_timeline_for_rpc(
            &initial_route.timeline_key,
            owner_service.advertise_endpoint().to_string(),
            Some(target_generator),
            TransferReason::Rebalance,
        )
        .await
        .unwrap();

    let route_event = next_route_event(&mut stream, Duration::from_secs(5)).await;
    assert_eq!(route_event.cache_ttl_ms, 7_777);
    assert!(route_event.route_version > initial_route.route_version);

    watcher_after.shutdown().await;
    owner_service.shutdown().await;
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_watch_lagged_resync_route_events_keep_configured_cache_ttl() {
    let prefix = unique_test_etcd_prefix("rpc-watch-lagged");
    let clock = Arc::new(ManualClock::new(6_000));
    let owner_service = service_with_metadata(
        TsoConfig {
            worker_id: "worker-owner".to_string(),
            advertise_endpoint: test_endpoint("endpoint-owner"),
            ..TsoConfig::default()
        },
        clock.clone(),
        real_etcd_store(&prefix).await,
    );
    let watcher_service = service_with_metadata(
        TsoConfig {
            worker_id: "worker-watcher".to_string(),
            advertise_endpoint: test_endpoint("endpoint-watcher"),
            route_cache_ttl_ms: 9_999,
            ..TsoConfig::default()
        },
        clock,
        real_etcd_store(&prefix).await,
    );
    wait_for_external_watch_ready(&owner_service, &watcher_service, "lagged").await;

    let route_service = TsoRouteService::new(watcher_service.control_plane());
    let initial_route = owner_service
        .ensure_timeline("route.ttl.lagged.etcd")
        .await
        .unwrap();
    let request = WatchTimelineRoutesRequest {
        timeline_keys: vec![initial_route.timeline_key.clone()],
        known_route_versions: HashMap::from([(
            initial_route.timeline_key.clone(),
            initial_route.route_version,
        )]),
        sdk_instance_id: "sdk-lagged-etcd".to_string(),
    };
    let mut stream = route_service
        .watch_timeline_routes(Request::new(request))
        .await
        .unwrap()
        .into_inner();

    let expected_route_version = initial_route.route_version + 1100;
    let timeline_key = initial_route.timeline_key.clone();
    let owner_service_for_updates = owner_service.clone();
    let update_task = tokio::spawn(async move {
        let mut current_generator = initial_route.generator_id;
        for _ in 0..1100 {
            current_generator = if current_generator == 0 { 1 } else { 0 };
            owner_service_for_updates
                .control_plane()
                .transfer_timeline_for_rpc(
                    &timeline_key,
                    owner_service_for_updates.advertise_endpoint().to_string(),
                    Some(current_generator),
                    TransferReason::Rebalance,
                )
                .await
                .unwrap();
        }
    });

    wait_for_latest_route_version(
        &mut stream,
        expected_route_version,
        9_999,
        Duration::from_secs(60),
    )
    .await;
    update_task.await.unwrap();

    watcher_service.shutdown().await;
    owner_service.shutdown().await;
}
