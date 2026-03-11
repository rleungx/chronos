use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};
use tonic::Request;

use chronos_tso::metadata::{GeneratorBatchOp, GeneratorRecord, MemoryMetadataStore, MetadataStore, TimelineBatchOp, TimelineRecord};
use chronos_tso::metrics;
use chronos_tso::proto::v1::{
    timestamp_service_server::TimestampService, AllocateTimestampsRequest as ProtoAllocateRequest,
};
use chronos_tso::rpc::TsoTimestampService;
use chronos_tso::timeline_proxy::TimelineScopedAllocator;
use chronos_tso::{ManualClock, TsoConfig, TsoError, TsoService};
use tokio::sync::broadcast;

#[derive(Clone)]
struct SlowMetadataStore {
    inner: Arc<MemoryMetadataStore>,
    read_delay: Duration,
}

#[async_trait]
impl MetadataStore for SlowMetadataStore {
    async fn get_record(&self, timeline_key: &str) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
        sleep(self.read_delay).await;
        self.inner.get_record(timeline_key).await
    }

    async fn create_record(&self, timeline_key: &str, record: &TimelineRecord) -> Result<u64, TsoError> {
        self.inner.create_record(timeline_key, record).await
    }

    async fn cas_record(&self, timeline_key: &str, previous_revision: u64, record: &TimelineRecord) -> Result<u64, TsoError> {
        self.inner.cas_record(timeline_key, previous_revision, record).await
    }

    async fn cas_records_batch(&self, operations: &[TimelineBatchOp]) -> Result<Vec<u64>, TsoError> {
        self.inner.cas_records_batch(operations).await
    }

    async fn get_generator_record(&self, generator_id: u32) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
        sleep(self.read_delay).await;
        self.inner.get_generator_record(generator_id).await
    }

    async fn create_generator_record(&self, generator_id: u32, record: &GeneratorRecord) -> Result<u64, TsoError> {
        self.inner.create_generator_record(generator_id, record).await
    }

    async fn cas_generator_record(&self, generator_id: u32, previous_revision: u64, record: &GeneratorRecord) -> Result<u64, TsoError> {
        self.inner.cas_generator_record(generator_id, previous_revision, record).await
    }

    async fn cas_generator_records_batch(&self, operations: &[GeneratorBatchOp]) -> Result<Vec<u64>, TsoError> {
        self.inner.cas_generator_records_batch(operations).await
    }

    fn subscribe_timeline_routes(&self) -> broadcast::Receiver<chronos_tso::TimelineRoute> {
        self.inner.subscribe_timeline_routes()
    }
}

fn max_tso(response: &chronos_tso::proto::v1::AllocateTimestampsResponse) -> u64 {
    response.ranges.last().unwrap().end_tso
}

fn min_tso(response: &chronos_tso::proto::v1::AllocateTimestampsResponse) -> u64 {
    response.ranges.first().unwrap().start_tso
}

#[tokio::test]
async fn same_timeline_requests_preserve_request_order_at_proxy_ingress() {
    let clock = Arc::new(ManualClock::new(5_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(TsoConfig::default(), clock, metadata).unwrap();
    let route = service.ensure_timeline("proxy.order.timeline").await.unwrap();
    let rpc = Arc::new(TsoTimestampService::new(service.data_plane()));

    let (start_a_tx, start_a_rx) = oneshot::channel();
    let (start_b_tx, start_b_rx) = oneshot::channel();

    let rpc_a = rpc.clone();
    let req_a = ProtoAllocateRequest {
        timeline_key: route.timeline_key.clone(),
        count: 4,
        expected_epoch: route.epoch,
        expected_route_version: route.route_version,
        client_request_id: "req-a".to_string(),
        request_timeout_ms: 0,
    };
    let handle_a = tokio::spawn(async move {
        let _ = start_a_rx.await;
        rpc_a
            .allocate_timestamps(Request::new(req_a))
            .await
            .unwrap()
            .into_inner()
    });

    let rpc_b = rpc.clone();
    let req_b = ProtoAllocateRequest {
        timeline_key: route.timeline_key,
        count: 4,
        expected_epoch: route.epoch,
        expected_route_version: route.route_version,
        client_request_id: "req-b".to_string(),
        request_timeout_ms: 0,
    };
    let handle_b = tokio::spawn(async move {
        let _ = start_b_rx.await;
        rpc_b
            .allocate_timestamps(Request::new(req_b))
            .await
            .unwrap()
            .into_inner()
    });

    start_a_tx.send(()).unwrap();
    sleep(Duration::from_millis(10)).await;
    start_b_tx.send(()).unwrap();

    let response_a = handle_a.await.unwrap();
    let response_b = handle_b.await.unwrap();

    assert!(max_tso(&response_a) < min_tso(&response_b));
}

#[tokio::test]
async fn different_timelines_do_not_break_independent_allocation() {
    let clock = Arc::new(ManualClock::new(6_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(TsoConfig::default(), clock, metadata).unwrap();
    let route_a = service.ensure_timeline("proxy.timeline.a").await.unwrap();
    let route_b = service.ensure_timeline("proxy.timeline.b").await.unwrap();
    let rpc = Arc::new(TsoTimestampService::new(service.data_plane()));

    let rpc_a = rpc.clone();
    let handle_a = tokio::spawn(async move {
        rpc_a
            .allocate_timestamps(Request::new(ProtoAllocateRequest {
                timeline_key: route_a.timeline_key,
                count: 2,
                expected_epoch: route_a.epoch,
                expected_route_version: route_a.route_version,
                client_request_id: "a".to_string(),
                request_timeout_ms: 0,
            }))
            .await
            .unwrap()
            .into_inner()
    });

    let rpc_b = rpc.clone();
    let handle_b = tokio::spawn(async move {
        rpc_b
            .allocate_timestamps(Request::new(ProtoAllocateRequest {
                timeline_key: route_b.timeline_key,
                count: 2,
                expected_epoch: route_b.epoch,
                expected_route_version: route_b.route_version,
                client_request_id: "b".to_string(),
                request_timeout_ms: 0,
            }))
            .await
            .unwrap()
            .into_inner()
    });

    let response_a = handle_a.await.unwrap();
    let response_b = handle_b.await.unwrap();

    assert!(!response_a.ranges.is_empty());
    assert!(!response_b.ranges.is_empty());
}

#[tokio::test]
async fn proxy_reuses_cached_timeline_lanes() {
    let clock = Arc::new(ManualClock::new(7_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(TsoConfig::default(), clock, metadata).unwrap();
    let route = service.ensure_timeline("proxy.cleanup.timeline").await.unwrap();
    let allocator = TimelineScopedAllocator::new(service.data_plane());

    let response = allocator
        .allocate_timestamps(chronos_tso::AllocateTimestampsRequest {
            timeline_key: route.timeline_key,
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "cleanup".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(response.ranges.len(), 1);
    assert_eq!(allocator.serializer_count(), 1);

    let second = allocator
        .allocate_timestamps(chronos_tso::AllocateTimestampsRequest {
            timeline_key: "proxy.cleanup.timeline".to_string(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "cleanup-2".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(second.ranges.len(), 1);
    assert_eq!(allocator.serializer_count(), 1);
}

#[tokio::test]
async fn proxy_prunes_idle_lanes_when_at_capacity() {
    let clock = Arc::new(ManualClock::new(8_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let config = TsoConfig {
        max_timeline_proxy_lanes: 1,
        ..TsoConfig::default()
    };
    let service = TsoService::new(config, clock, metadata).unwrap();
    let route_a = service.ensure_timeline("proxy.prune.a").await.unwrap();
    let route_b = service.ensure_timeline("proxy.prune.b").await.unwrap();
    let allocator = TimelineScopedAllocator::new(service.data_plane());

    allocator
        .allocate_timestamps(chronos_tso::AllocateTimestampsRequest {
            timeline_key: route_a.timeline_key,
            count: 1,
            expected_epoch: route_a.epoch,
            expected_route_version: route_a.route_version,
            client_request_id: "prune-a".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(allocator.serializer_count(), 1);

    allocator
        .allocate_timestamps(chronos_tso::AllocateTimestampsRequest {
            timeline_key: route_b.timeline_key,
            count: 1,
            expected_epoch: route_b.epoch,
            expected_route_version: route_b.route_version,
            client_request_id: "prune-b".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(allocator.serializer_count(), 1);
}

#[tokio::test]
async fn proxy_rejects_new_lanes_when_capacity_is_zero() {
    let clock = Arc::new(ManualClock::new(9_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let config = TsoConfig {
        max_timeline_proxy_lanes: 0,
        ..TsoConfig::default()
    };
    let service = TsoService::new(config, clock, metadata).unwrap();
    let route = service.ensure_timeline("proxy.saturated.timeline").await.unwrap();
    let allocator = TimelineScopedAllocator::new(service.data_plane());

    let before = metrics::TSO_TIMELINE_PROXY_SATURATED_TOTAL.get();
    let result = allocator
        .allocate_timestamps(chronos_tso::AllocateTimestampsRequest {
            timeline_key: route.timeline_key,
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "saturated".to_string(),
        })
        .await;

    match result {
        Err(TsoError::TimelineIngressSaturated {
            timeline_key,
            max_lanes,
        }) => {
            assert_eq!(timeline_key, "proxy.saturated.timeline");
            assert_eq!(max_lanes, 0);
        }
        other => panic!("unexpected saturation result: {:?}", other),
    }
    assert!(metrics::TSO_TIMELINE_PROXY_SATURATED_TOTAL.get() >= before + 1);
}

#[tokio::test]
async fn proxy_timeout_does_not_poison_lane_usage_for_pruning() {
    let clock = Arc::new(ManualClock::new(10_000));
    let base_store = Arc::new(MemoryMetadataStore::new());
    let config = TsoConfig {
        max_timeline_proxy_lanes: 1,
        instance_id: "proxy-timeout-instance".to_string(),
        ..TsoConfig::default()
    };
    let seed_service = TsoService::new(config.clone(), clock.clone(), base_store.clone()).unwrap();
    let route_a = seed_service.ensure_timeline("proxy.timeout.a").await.unwrap();
    let route_b = seed_service.ensure_timeline("proxy.timeout.b").await.unwrap();

    let slow_store = Arc::new(SlowMetadataStore {
        inner: base_store,
        read_delay: Duration::from_millis(50),
    });
    let service = TsoService::new(
        config,
        clock,
        slow_store,
    )
    .unwrap();
    let allocator = TimelineScopedAllocator::new(service.data_plane());

    let before = metrics::TSO_TIMELINE_PROXY_TIMEOUT_TOTAL.get();
    let timeout_result = allocator
        .allocate_timestamps_with_timeout(
            chronos_tso::AllocateTimestampsRequest {
                timeline_key: route_a.timeline_key,
                count: 1,
                expected_epoch: route_a.epoch,
                expected_route_version: route_a.route_version,
                client_request_id: "timeout-a".to_string(),
            },
            1,
        )
        .await;
    assert!(matches!(timeout_result, Err(chronos_tso::timeline_proxy::TimelineProxyError::TimedOut)));
    assert!(metrics::TSO_TIMELINE_PROXY_TIMEOUT_TOTAL.get() >= before + 1);

    let response = allocator
        .allocate_timestamps(chronos_tso::AllocateTimestampsRequest {
            timeline_key: route_b.timeline_key,
            count: 1,
            expected_epoch: route_b.epoch,
            expected_route_version: route_b.route_version,
            client_request_id: "timeout-b".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(response.ranges.len(), 1);
    assert_eq!(allocator.serializer_count(), 1);
    assert_eq!(allocator.max_serializers(), 1);
}
