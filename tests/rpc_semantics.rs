#[path = "common/config.rs"]
mod common_config;
#[path = "common/etcd_endpoints.rs"]
mod common_etcd_endpoints;
#[path = "common/etcd_prefix.rs"]
mod common_etcd_prefix;
#[path = "common/etcd_store.rs"]
mod common_etcd_store;

use std::sync::Arc;

use common_config::required_test_config;
use common_etcd_prefix::unique_test_etcd_prefix;
use common_etcd_store::test_etcd_store;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request};

use chronos::lifecycle::TimelineLifecycleContract;
use chronos::metadata::{
    ControlPlaneStore, EtcdMetadataStore, GeneratorLeaseAuthority, MemoryMetadataStore,
    TimelineAuthority,
};
use chronos::proto::v1::{
    timeline_control_service_server::TimelineControlService,
    timeline_route_service_server::TimelineRouteService,
    timeline_status_service_server::TimelineStatusService,
    timestamp_service_client::TimestampServiceClient,
    timestamp_service_server::{TimestampService, TimestampServiceServer},
    AllocateTimestampsRequest as ProtoAllocateRequest, EnsureTimelineRequest, ErrorCode,
    GetTimelineRouteRequest, GetTimelineStatusRequest, ListTimelineStatusesRequest,
    OperatorActionBlocker, OperatorActionNextStep, TimelineState, TimelineTransferReason,
    TransferTimelineRequest, WorkerReadinessReason, WorkerReadinessState,
};
use chronos::rpc::{
    HealthStatusHandle, TsoControlService, TsoRouteService, TsoTimelineStatusService,
    TsoTimestampService,
};
use chronos::{
    build_commit, build_version, ManualClock, ResourceTier, TimelineLifecycleState, TsoConfig,
    TsoService,
};

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

fn test_endpoint(name: &str) -> String {
    format!("{name}:50051")
}

fn owner_filtered_status_request(owner_worker_endpoint: String) -> ListTimelineStatusesRequest {
    ListTimelineStatusesRequest {
        states: vec![TimelineState::Active as i32],
        owner_worker_endpoint: Some(owner_worker_endpoint),
        page_size: 10,
        page_token: String::new(),
    }
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
    Arc::new(test_etcd_store(prefix).await)
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
    let status_service_via_worker_a = TsoTimelineStatusService::new(service_a.control_plane());

    for timeline_key in ["ops.inventory.timeline-a", "ops.inventory.timeline-b"] {
        route_service_a
            .ensure_timeline(Request::new(EnsureTimelineRequest {
                timeline_key: timeline_key.to_string(),
                desired_resource_tier: chronos::proto::v1::ResourceTier::Shared as i32,
            }))
            .await
            .expect("ensure_timeline should succeed");
    }

    let inventory_before = status_service_via_worker_a
        .list_timeline_statuses(Request::new(owner_filtered_status_request(test_endpoint(
            "endpoint-a",
        ))))
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

    let source_inventory_after = status_service_via_worker_a
        .list_timeline_statuses(Request::new(owner_filtered_status_request(test_endpoint(
            "endpoint-a",
        ))))
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

    let target_inventory_after = status_service_via_worker_a
        .list_timeline_statuses(Request::new(owner_filtered_status_request(test_endpoint(
            "endpoint-b",
        ))))
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

    let point_read = status_service_via_worker_a
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

    let detail = chronos::rpc::decode_error_detail_from_status_details(error.details())
        .expect("error detail should decode");
    assert_eq!(detail.code, ErrorCode::RouteVersionMismatch as i32);
    assert_eq!(detail.current_route_version, route.route_version);
    assert!(detail.message.contains("route version mismatch"));
}

#[tokio::test]
async fn timestamp_stream_preserves_order_and_propagates_structured_errors() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let clock = Arc::new(ManualClock::new(3_050));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(
        TsoConfig {
            advertise_endpoint: addr.to_string(),
            ..TsoConfig::default()
        },
        clock,
        metadata,
    );
    let route = service
        .ensure_timeline("rpc.streaming.timeline")
        .await
        .unwrap();
    let timestamp_service = TsoTimestampService::new(service.data_plane());
    let server_handle = tokio::spawn(
        Server::builder()
            .add_service(TimestampServiceServer::new(timestamp_service))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let mut client = TimestampServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let request = |count, client_request_id: &str, route_version| ProtoAllocateRequest {
        timeline_key: route.timeline_key.clone(),
        count,
        expected_epoch: route.epoch,
        expected_route_version: route_version,
        client_request_id: client_request_id.to_string(),
        request_timeout_ms: 0,
    };

    let mut responses = client
        .allocate_timestamps_stream(tokio_stream::iter([
            request(2, "stream-first", route.route_version),
            request(3, "stream-second", route.route_version),
        ]))
        .await
        .unwrap()
        .into_inner();
    let first = responses.message().await.unwrap().unwrap();
    let second = responses.message().await.unwrap().unwrap();

    assert_eq!(allocated_count(&first), 2);
    assert_eq!(allocated_count(&second), 3);
    assert_eq!(first.timeline_key, route.timeline_key);
    assert_eq!(second.timeline_key, route.timeline_key);
    assert!(second.ranges[0].start_tso > first.ranges.last().unwrap().end_tso);
    assert!(responses.message().await.unwrap().is_none());

    let mut responses = client
        .allocate_timestamps_stream(tokio_stream::iter([
            request(1, "stream-before-error", route.route_version),
            request(1, "stream-stale-route", route.route_version + 1),
            request(1, "stream-after-error", route.route_version),
        ]))
        .await
        .unwrap()
        .into_inner();
    assert!(responses.message().await.unwrap().is_some());
    let error = responses
        .message()
        .await
        .expect_err("a request error should terminate the stream");
    assert_eq!(error.code(), Code::FailedPrecondition);
    let detail = chronos::rpc::decode_error_detail_from_status_details(error.details())
        .expect("stream error detail should decode");
    assert_eq!(detail.code, ErrorCode::RouteVersionMismatch as i32);
    assert_eq!(detail.current_route_version, route.route_version);

    server_handle.abort();
    let _ = server_handle.await;
    service.shutdown().await;
}

fn allocated_count(response: &chronos::proto::v1::AllocateTimestampsResponse) -> u64 {
    response
        .ranges
        .iter()
        .map(|range| range.end_tso - range.start_tso + 1)
        .sum()
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
        let detail = chronos::rpc::decode_error_detail_from_status_details(error.details())
            .expect("error detail should decode");
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
    let detail = chronos::rpc::decode_error_detail_from_status_details(error.details())
        .expect("error detail should decode");
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
    let detail = chronos::rpc::decode_error_detail_from_status_details(error.details())
        .expect("error detail should decode");
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
async fn route_responses_return_routes() {
    let clock = Arc::new(ManualClock::new(3_800));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(TsoConfig::default(), clock, metadata);
    let route_service = TsoRouteService::new(service.control_plane());

    let ensured = route_service
        .ensure_timeline(Request::new(EnsureTimelineRequest {
            timeline_key: "route.ttl.ensure".to_string(),
            desired_resource_tier: chronos::proto::v1::ResourceTier::Shared as i32,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(ensured.route.is_some());

    let fetched = route_service
        .get_timeline_route(Request::new(GetTimelineRouteRequest {
            timeline_key: "route.ttl.ensure".to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(fetched.route.is_some());
}
