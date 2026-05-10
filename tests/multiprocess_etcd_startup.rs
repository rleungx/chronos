#[path = "common/etcd_endpoints.rs"]
mod common_etcd_endpoints;
#[path = "common/etcd_prefix.rs"]
mod common_etcd_prefix;

use std::{
    fs::{self, File},
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use chronos::proto::v1::{
    timeline_control_service_client::TimelineControlServiceClient,
    timeline_route_service_client::TimelineRouteServiceClient,
    timestamp_service_client::TimestampServiceClient, AllocateTimestampsRequest,
    EnsureTimelineRequest, ErrorDetail, GetTimelineRouteRequest, ResourceTier, TimelineRoute,
    TimelineTransferReason, TransferTimelineRequest, WorkerReadinessState,
};
use common_etcd_endpoints::{test_etcd_endpoints, test_etcd_endpoints_csv};
use common_etcd_prefix::unique_test_etcd_prefix;
use etcd_client::Client;
use prost::Message;
use tokio::time::{sleep, timeout};
use tonic::{transport::Channel, Code, Request};

struct SpawnedChronosConfig<'a> {
    prefix: &'a str,
    instance_id: &'a str,
    worker_id: &'a str,
    bind_addr: SocketAddr,
    metrics_bind_addr: SocketAddr,
    safety_gap_ms: u64,
}

struct SpawnedChronos {
    child: Child,
    stderr_path: PathBuf,
}

impl Drop for SpawnedChronos {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = fs::remove_file(&self.stderr_path);
    }
}

fn chronos_bin() -> &'static str {
    env!("CARGO_BIN_EXE_chronos")
}

fn parsed_test_etcd_endpoints() -> Vec<String> {
    test_etcd_endpoints()
}

fn free_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("loopback bind should succeed");
    let addr = listener
        .local_addr()
        .expect("loopback listener should expose local addr");
    drop(listener);
    addr
}

fn routable_test_advertise_endpoint(_worker_id: &str, bind_addr: SocketAddr) -> String {
    bind_addr.to_string()
}

fn next_stderr_path(worker_id: &str) -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "chronos-multiprocess-{worker_id}-{}-{}.stderr",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

fn spawn_chronos_process(config: SpawnedChronosConfig<'_>) -> SpawnedChronos {
    let advertise_endpoint = routable_test_advertise_endpoint(config.worker_id, config.bind_addr);
    let stderr_path = next_stderr_path(config.worker_id);
    let stderr_file = File::create(&stderr_path).expect("child stderr file should be created");
    let child = Command::new(chronos_bin())
        .env("CHRONOS_METADATA", "etcd")
        .env("CHRONOS_ETCD_ENDPOINTS", test_etcd_endpoints_csv())
        .env("CHRONOS_ETCD_PREFIX", config.prefix)
        .env("CHRONOS_SECURITY_MODE", "dev-insecure")
        .env("CHRONOS_SAFETY_GAP_MS", config.safety_gap_ms.to_string())
        .env("CHRONOS_WORKER_ID", config.worker_id)
        .env("CHRONOS_INSTANCE_ID", config.instance_id)
        .env("CHRONOS_BIND_ADDR", config.bind_addr.to_string())
        .env("CHRONOS_ADVERTISE_ENDPOINT", advertise_endpoint)
        .env(
            "CHRONOS_METRICS_BIND_ADDR",
            config.metrics_bind_addr.to_string(),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .expect("chronos process should spawn");

    SpawnedChronos { child, stderr_path }
}

fn endpoint_uri(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

async fn control_client(endpoint: SocketAddr) -> TimelineControlServiceClient<Channel> {
    TimelineControlServiceClient::connect(endpoint_uri(endpoint))
        .await
        .expect("control client should connect")
}

async fn route_client(endpoint: SocketAddr) -> TimelineRouteServiceClient<Channel> {
    TimelineRouteServiceClient::connect(endpoint_uri(endpoint))
        .await
        .expect("route client should connect")
}

async fn timestamp_client(endpoint: SocketAddr) -> TimestampServiceClient<Channel> {
    TimestampServiceClient::connect(endpoint_uri(endpoint))
        .await
        .expect("timestamp client should connect")
}

fn read_child_stderr(child: &SpawnedChronos) -> String {
    fs::read_to_string(&child.stderr_path).unwrap_or_default()
}

async fn wait_for_identity_key(prefix: &str, instance_id: &str, child: &mut SpawnedChronos) {
    let key = format!("{prefix}/identity/instances/{instance_id}");
    let endpoints = parsed_test_etcd_endpoints();
    let mut client = Client::connect(endpoints, None)
        .await
        .expect("etcd client should connect");

    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = child
                .child
                .try_wait()
                .expect("child try_wait should succeed")
            {
                let stderr = read_child_stderr(child);
                panic!(
                    "chronos process exited before identity key appeared: status={status} stderr={stderr}"
                );
            }

            let response = client
                .get(key.clone(), None)
                .await
                .expect("identity lease lookup should succeed");
            if !response.kvs().is_empty() {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("identity key should appear before timeout");
}

async fn wait_for_ready(endpoint: SocketAddr, child: &mut SpawnedChronos) {
    timeout(Duration::from_secs(15), async {
        loop {
            if let Some(status) = child
                .child
                .try_wait()
                .expect("child try_wait should succeed")
            {
                let stderr = read_child_stderr(child);
                panic!("chronos process exited before readiness: status={status} stderr={stderr}");
            }

            if let Ok(mut client) =
                TimelineControlServiceClient::connect(endpoint_uri(endpoint)).await
            {
                if let Ok(response) = client.health(()).await {
                    if response.into_inner().readiness_state == WorkerReadinessState::Ready as i32 {
                        return;
                    }
                }
            }

            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("process should become ready before timeout");
}

async fn identity_key_exists(prefix: &str, instance_id: &str) -> bool {
    let key = format!("{prefix}/identity/instances/{instance_id}");
    let endpoints = parsed_test_etcd_endpoints();
    let mut client = Client::connect(endpoints, None)
        .await
        .expect("etcd client should connect");
    let response = client
        .get(key, None)
        .await
        .expect("identity lease lookup should succeed");
    !response.kvs().is_empty()
}

async fn wait_for_exit(child: &mut SpawnedChronos) -> std::process::ExitStatus {
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = child
                .child
                .try_wait()
                .expect("child try_wait should succeed")
            {
                return status;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("child should exit before timeout")
}

async fn terminate_child(child: &mut SpawnedChronos) {
    if child
        .child
        .try_wait()
        .expect("child try_wait should succeed")
        .is_none()
    {
        child.child.kill().expect("child kill should succeed");
    }
    let _ = child.child.wait().expect("child wait should succeed");
    let _ = fs::remove_file(&child.stderr_path);
}

async fn ensure_timeline(endpoint: SocketAddr, timeline_key: &str) -> TimelineRoute {
    route_client(endpoint)
        .await
        .ensure_timeline(Request::new(EnsureTimelineRequest {
            timeline_key: timeline_key.to_string(),
            desired_resource_tier: ResourceTier::Shared as i32,
        }))
        .await
        .expect("ensure_timeline should succeed")
        .into_inner()
        .route
        .expect("ensure_timeline should return route")
}

async fn get_timeline_route(endpoint: SocketAddr, timeline_key: &str) -> TimelineRoute {
    route_client(endpoint)
        .await
        .get_timeline_route(Request::new(GetTimelineRouteRequest {
            timeline_key: timeline_key.to_string(),
        }))
        .await
        .expect("get_timeline_route should succeed")
        .into_inner()
        .route
        .expect("get_timeline_route should return route")
}

async fn allocate_last_tso(
    endpoint: SocketAddr,
    route: &TimelineRoute,
    client_request_id: &str,
    count: u32,
) -> u64 {
    let response = allocate_timestamps_raw(endpoint, route, client_request_id, count)
        .await
        .expect("allocate_timestamps should succeed")
        .into_inner();
    response
        .ranges
        .last()
        .expect("allocate should return at least one range")
        .end_tso
}

async fn allocate_timestamps_raw(
    endpoint: SocketAddr,
    route: &TimelineRoute,
    client_request_id: &str,
    count: u32,
) -> Result<tonic::Response<chronos::proto::v1::AllocateTimestampsResponse>, tonic::Status> {
    timestamp_client(endpoint)
        .await
        .allocate_timestamps(Request::new(AllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: client_request_id.to_string(),
            request_timeout_ms: 0,
        }))
        .await
}

async fn failover_timeline(
    endpoint: SocketAddr,
    timeline_key: &str,
    target_worker_endpoint: &str,
) -> Result<chronos::proto::v1::TransferTimelineResponse, tonic::Status> {
    control_client(endpoint)
        .await
        .transfer_timeline(Request::new(TransferTimelineRequest {
            timeline_key: timeline_key.to_string(),
            target_generator_id: None,
            target_worker_id: Some(target_worker_endpoint.to_string()),
            reason: TimelineTransferReason::Failover as i32,
        }))
        .await
        .map(|response| response.into_inner())
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_spawned_process_rejects_duplicate_instance_identity() {
    let prefix = unique_test_etcd_prefix("multiprocess-dup-instance");
    let instance_id = "spawned-instance-dupe";

    let mut first = spawn_chronos_process(SpawnedChronosConfig {
        prefix: &prefix,
        instance_id,
        worker_id: "worker-a",
        bind_addr: free_loopback_addr(),
        metrics_bind_addr: free_loopback_addr(),
        safety_gap_ms: 1,
    });

    wait_for_identity_key(&prefix, instance_id, &mut first).await;

    let mut second = spawn_chronos_process(SpawnedChronosConfig {
        prefix: &prefix,
        instance_id,
        worker_id: "worker-b",
        bind_addr: free_loopback_addr(),
        metrics_bind_addr: free_loopback_addr(),
        safety_gap_ms: 1,
    });

    let status = wait_for_exit(&mut second).await;
    let stderr = read_child_stderr(&second);
    assert!(
        !status.success(),
        "duplicate instance process should fail, status={status} stderr={stderr}"
    );
    assert!(
        first
            .child
            .try_wait()
            .expect("first child try_wait should succeed")
            .is_none(),
        "first process should still be alive when duplicate startup is rejected"
    );
    assert!(
        stderr.contains("instance identity already in use")
            || stderr.contains("InstanceIdentityInUse"),
        "duplicate instance failure should mention identity conflict, stderr={stderr}"
    );
    assert!(
        identity_key_exists(&prefix, instance_id).await,
        "first process identity key should still exist after duplicate startup rejection"
    );

    terminate_child(&mut first).await;
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_spawned_process_failover_preserves_tso_monotonicity() {
    let prefix = unique_test_etcd_prefix("multiprocess-failover");
    let timeline_key = "spawned.failover.timeline";
    let safety_gap_ms = 200;

    let bind_a = free_loopback_addr();
    let bind_b = free_loopback_addr();
    let endpoint_a = routable_test_advertise_endpoint("worker-a", bind_a);
    let endpoint_b = routable_test_advertise_endpoint("worker-b", bind_b);

    let mut first = spawn_chronos_process(SpawnedChronosConfig {
        prefix: &prefix,
        instance_id: "spawned-instance-a",
        worker_id: "worker-a",
        bind_addr: bind_a,
        metrics_bind_addr: free_loopback_addr(),
        safety_gap_ms,
    });
    wait_for_ready(bind_a, &mut first).await;

    let route_a = ensure_timeline(bind_a, timeline_key).await;
    assert_eq!(route_a.owner_worker_endpoint, endpoint_a);
    let first_last = allocate_last_tso(bind_a, &route_a, "before-real-failover", 2).await;

    let mut second = spawn_chronos_process(SpawnedChronosConfig {
        prefix: &prefix,
        instance_id: "spawned-instance-b",
        worker_id: "worker-b",
        bind_addr: bind_b,
        metrics_bind_addr: free_loopback_addr(),
        safety_gap_ms,
    });
    wait_for_ready(bind_b, &mut second).await;

    terminate_child(&mut first).await;

    let blocked = failover_timeline(bind_b, timeline_key, &endpoint_b).await;
    let blocked = blocked.expect_err("failover should be blocked before lease expiry");
    assert_eq!(blocked.code(), Code::FailedPrecondition);
    let blocked_detail =
        ErrorDetail::decode(blocked.details()).expect("error detail should decode");
    assert_eq!(
        blocked_detail.action_blocker,
        chronos::proto::v1::OperatorActionBlocker::LeaseNotExpired as i32
    );
    let route_still_a = get_timeline_route(bind_b, timeline_key).await;
    assert_eq!(route_still_a.owner_worker_endpoint, endpoint_a);
    let blocked_allocate = timeout(
        Duration::from_secs(2),
        allocate_timestamps_raw(bind_b, &route_still_a, "blocked-before-failover", 1),
    )
    .await
    .expect("blocked allocation should return promptly")
    .expect_err("new owner must not allocate before failover succeeds");
    assert!(
        matches!(
            blocked_allocate.code(),
            Code::FailedPrecondition | Code::Unavailable | Code::Aborted
        ),
        "blocked allocation should fail with ownership/readiness error, got {}",
        blocked_allocate.code()
    );

    timeout(Duration::from_secs(15), async {
        loop {
            match failover_timeline(bind_b, timeline_key, &endpoint_b).await {
                Ok(_) => return,
                Err(status) if status.code() == Code::FailedPrecondition => {
                    let detail = ErrorDetail::decode(status.details())
                        .expect("failover blocker detail should decode");
                    if detail.action_blocker
                        == chronos::proto::v1::OperatorActionBlocker::LeaseNotExpired as i32
                    {
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    panic!("unexpected failover blocker detail: {:?}", detail);
                }
                Err(status) => panic!("unexpected failover status: {status}"),
            }
        }
    })
    .await
    .expect("failover should succeed before timeout");

    let route_b = get_timeline_route(bind_b, timeline_key).await;
    assert_eq!(route_b.owner_worker_endpoint, endpoint_b);
    assert!(route_a.epoch < route_b.epoch);
    assert!(route_a.route_version < route_b.route_version);
    let second_first = allocate_last_tso(bind_b, &route_b, "after-real-failover", 1).await;
    assert!(
        first_last < second_first,
        "post-failover TSO must advance: before={first_last} after={second_first}"
    );

    terminate_child(&mut second).await;
}
