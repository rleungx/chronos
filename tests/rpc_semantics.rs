use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tonic::Request;

use chronos_tso::metadata::MemoryMetadataStore;
use chronos_tso::proto::v1::{
    timeline_control_service_server::TimelineControlService,
    timeline_route_event, timeline_route_service_server::TimelineRouteService,
    timestamp_service_server::TimestampService, AllocateTimestampsRequest as ProtoAllocateRequest,
    EnsureTimelineRequest, ErrorCode, ErrorDetail, GetTimelineRouteRequest,
    WatchTimelineRoutesRequest,
};
use chronos_tso::rpc::{TsoControlService, TsoRouteService, TsoTimestampService};
use chronos_tso::{ManualClock, ResourceTier, TransferReason, TsoConfig, TsoService};

fn service_with_metadata(
    config: TsoConfig,
    clock: Arc<ManualClock>,
    metadata: Arc<MemoryMetadataStore>,
) -> Arc<TsoService> {
    TsoService::new(config, clock, metadata).unwrap()
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
async fn watch_timeline_routes_receives_cross_service_updates_from_shared_metadata() {
    let clock = Arc::new(ManualClock::new(1_500));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service_a = service_with_metadata(
        TsoConfig {
            worker_id: "worker-a".to_string(),
            advertise_endpoint: "endpoint-a".to_string(),
            ..TsoConfig::default()
        },
        clock.clone(),
        metadata.clone(),
    );
    let service_b = service_with_metadata(
        TsoConfig {
            worker_id: "worker-b".to_string(),
            advertise_endpoint: "endpoint-b".to_string(),
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
async fn health_rpc_reports_configured_instance_id() {
    let clock = Arc::new(ManualClock::new(3_500));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = service_with_metadata(
        TsoConfig {
            worker_id: "worker-a".to_string(),
            instance_id: "instance-a-stable".to_string(),
            advertise_endpoint: "endpoint-a".to_string(),
            ..TsoConfig::default()
        },
        clock,
        metadata,
    );
    let control_service = TsoControlService::new(service.control_plane());

    let response = control_service.health(Request::new(())).await.unwrap().into_inner();
    assert_eq!(response.instance_id, "instance-a-stable");
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
            desired_resource_tier: chronos_tso::proto::v1::ResourceTier::Shared as i32,
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

    let target_generator = if initial_route.generator_id == 0 { 1 } else { 0 };
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut saw_latest = false;
    while tokio::time::Instant::now() < deadline {
        let maybe_event = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        let Ok(Some(Ok(event))) = maybe_event else {
            continue;
        };

        if let Some(timeline_route_event::Event::Route(route_event)) = event.event {
            assert_eq!(route_event.cache_ttl_ms, 9_999);
            if route_event.route_version == latest_route.route_version {
                saw_latest = true;
                break;
            }
        }
    }

    assert!(saw_latest, "expected lagged resync to deliver latest route version");
}
