use super::*;

#[tokio::test]
async fn metadata_startup_probe_accepts_memory_store() {
    let metadata = MemoryMetadataStore::new();
    run_metadata_startup_probe(&metadata).await.unwrap();
}

#[tokio::test]
async fn readyz_reports_service_unavailable_until_ready() {
    let response = metrics_handler(
        HyperRequest::builder()
            .uri("/readyz")
            .body(Empty::<Bytes>::new())
            .unwrap(),
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn health_handler_does_not_expose_metrics() {
    let response = health_handler(
        HyperRequest::builder()
            .uri("/metrics")
            .body(Empty::<Bytes>::new())
            .unwrap(),
        Arc::new(AtomicBool::new(true)),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn readyz_only_turns_green_after_all_critical_listeners_are_bound() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(false));
    let mut serving_gate = StartupServingGate::default();

    set_startup_ready(&ready, serving_gate.is_ready());
    let response = metrics_handler(
        HyperRequest::builder()
            .uri("/readyz")
            .body(Empty::<Bytes>::new())
            .unwrap(),
        ready.clone(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    set_startup_ready(
        &ready,
        serving_gate.mark_listener_bound(CriticalStartupListener::Admin),
    );
    let response = metrics_handler(
        HyperRequest::builder()
            .uri("/readyz")
            .body(Empty::<Bytes>::new())
            .unwrap(),
        ready.clone(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    set_startup_ready(
        &ready,
        serving_gate.mark_listener_bound(CriticalStartupListener::Grpc),
    );
    let response = metrics_handler(
        HyperRequest::builder()
            .uri("/readyz")
            .body(Empty::<Bytes>::new())
            .unwrap(),
        ready.clone(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    set_startup_ready(&ready, false);
}

#[test]
fn startup_preflight_failure_records_metric() {
    let _guard = ENV_LOCK.lock().unwrap();
    let before = metrics::TSO_STARTUP_PREFLIGHT_FAIL_TOTAL
        .with_label_values(&["config"])
        .get();

    let config = TsoConfig {
        advertise_endpoint: String::new(),
        ..explicit_required_config()
    };
    let _ = validate_startup_preflight(&memory_startup_config(config));
    record_startup_preflight_failure("config");

    let after = metrics::TSO_STARTUP_PREFLIGHT_FAIL_TOTAL
        .with_label_values(&["config"])
        .get();
    assert!(after > before);
}

#[test]
fn startup_failure_stage_classifies_identity_conflict() {
    let error = TsoError::InstanceIdentityInUse {
        instance_id: "instance-dupe".into(),
    };
    assert_eq!(
        startup_failure_stage(&error as &(dyn std::error::Error + 'static)),
        "identity"
    );
}

#[test]
fn startup_preflight_failure_uses_machine_readable_reason() {
    assert_eq!(
        startup_preflight_failure_reason(),
        WorkerReadinessReason::StartupPreflightFailed
    );
}

#[test]
fn startup_bootstrap_failure_maps_identity_conflict_to_machine_readable_reason() {
    let error = TsoError::InstanceIdentityInUse {
        instance_id: "instance-dupe".into(),
    };
    assert_eq!(
        startup_bootstrap_failure_reason(&error as &(dyn std::error::Error + 'static)),
        WorkerReadinessReason::IdentityLeaseAcquireFailed
    );
}

#[test]
fn startup_bootstrap_failure_defaults_to_metadata_probe_machine_readable_reason() {
    let error = TsoError::Internal("metadata probe failed".into());
    assert_eq!(
        startup_bootstrap_failure_reason(&error as &(dyn std::error::Error + 'static)),
        WorkerReadinessReason::MetadataStartupProbeFailed
    );
}

#[test]
fn startup_bootstrap_failure_records_metric() {
    let before = metrics::TSO_STARTUP_BOOTSTRAP_FAIL_TOTAL
        .with_label_values(&["contract"])
        .get();

    record_startup_bootstrap_failure("contract");

    let after = metrics::TSO_STARTUP_BOOTSTRAP_FAIL_TOTAL
        .with_label_values(&["contract"])
        .get();
    assert!(after > before);
}

#[test]
fn process_signal_shutdown_trigger_normalizes_to_shutting_down_reason() {
    let trigger = ShutdownTrigger::ProcessSignal("signal_sigterm");
    assert_eq!(trigger.label(), "signal_sigterm");
    assert_eq!(
        trigger.readiness_reason(),
        WorkerReadinessReason::ShuttingDown
    );
}

#[test]
fn critical_server_failure_shutdown_trigger_normalizes_to_shutting_down_reason() {
    let trigger = ShutdownTrigger::CriticalServerFailed("grpc");
    assert_eq!(trigger.label(), "critical_server_failed_grpc");
    assert_eq!(
        trigger.readiness_reason(),
        WorkerReadinessReason::ShuttingDown
    );
}

#[test]
fn identity_lease_loss_records_shutdown_metric() {
    let before = metrics::TSO_SHUTDOWN_TOTAL
        .with_label_values(&["identity_lease_lost"])
        .get();
    record_shutdown("identity_lease_lost");
    let after = metrics::TSO_SHUTDOWN_TOTAL
        .with_label_values(&["identity_lease_lost"])
        .get();
    assert!(after > before);
}

#[test]
fn request_shutdown_flips_readiness_and_notifies_watchers() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = test_health_status_handle();
    let (shutdown_tx, shutdown_rx) = shutdown_watch_pair();

    let before = metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
        .with_label_values(&["ready", "degraded", "shutting_down"])
        .get();

    request_shutdown(
        &ready,
        &health_status,
        &shutdown_tx,
        test_shutdown_identity(),
        ShutdownTrigger::ProcessSignal("signal_ctrl_c"),
    );

    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
    assert_eq!(
        health_status.readiness_state(),
        WorkerReadinessState::Degraded
    );
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::ShuttingDown
    );
    assert!(health_status.identity_lease_healthy());
    assert!(
        metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
            .with_label_values(&["ready", "degraded", "shutting_down"])
            .get()
            > before
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn critical_server_failure_requests_shutdown_and_returns_error() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = test_health_status_handle();
    let (shutdown_tx, shutdown_rx) = shutdown_watch_pair();
    let before = metrics::TSO_SHUTDOWN_TOTAL
        .with_label_values(&["critical_server_failed_grpc"])
        .get();

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        supervise_critical_servers(
            CriticalServerContext {
                ready: &ready,
                health_status: &health_status,
                shutdown_tx: &shutdown_tx,
                shutdown_rx: shutdown_rx.clone(),
                worker_id: "worker-a",
                instance_id: "instance-a",
                advertise_endpoint: "endpoint-a:50051",
            },
            async {
                Err(Box::new(std::io::Error::other("grpc failed")) as Box<dyn std::error::Error>)
            },
            wait_for_shutdown_signal(shutdown_rx.clone()).map(|_| Ok(())),
        ),
    )
    .await
    .expect("critical supervision should not hang");

    let error = result.expect_err("critical server failure should return error");
    assert!(
        error.to_string().contains("grpc failed"),
        "unexpected error: {error}"
    );
    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
    assert_eq!(
        health_status.readiness_state(),
        WorkerReadinessState::Degraded
    );
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::ShuttingDown
    );
    assert!(
        metrics::TSO_SHUTDOWN_TOTAL
            .with_label_values(&["critical_server_failed_grpc"])
            .get()
            > before
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn unexpected_critical_server_exit_without_shutdown_is_fatal() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = test_health_status_handle();
    let (shutdown_tx, shutdown_rx) = shutdown_watch_pair();

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        supervise_critical_servers(
            CriticalServerContext {
                ready: &ready,
                health_status: &health_status,
                shutdown_tx: &shutdown_tx,
                shutdown_rx: shutdown_rx.clone(),
                worker_id: "worker-a",
                instance_id: "instance-a",
                advertise_endpoint: "endpoint-a:50051",
            },
            async { Ok(()) },
            wait_for_shutdown_signal(shutdown_rx.clone()).map(|_| Ok(())),
        ),
    )
    .await
    .expect("unexpected exit supervision should not hang");

    let error = result.expect_err("unexpected critical exit should return error");
    assert!(
        error
            .to_string()
            .contains("server exited without shutdown request"),
        "unexpected error: {error}"
    );
    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
}

#[test]
fn request_shutdown_preserves_identity_lease_loss_precedence() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = test_health_status_handle();
    let (shutdown_tx, shutdown_rx) = shutdown_watch_pair();
    let before_lost = metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
        .with_label_values(&["ready", "degraded", "identity_lease_lost"])
        .get();
    let before_shutdown = metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
        .with_label_values(&["degraded", "degraded", "shutting_down"])
        .get();

    request_shutdown(
        &ready,
        &health_status,
        &shutdown_tx,
        test_shutdown_identity(),
        ShutdownTrigger::IdentityLeaseLost,
    );
    request_shutdown(
        &ready,
        &health_status,
        &shutdown_tx,
        test_shutdown_identity(),
        ShutdownTrigger::ProcessSignal("signal_ctrl_c"),
    );

    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
    assert_eq!(
        health_status.readiness_state(),
        WorkerReadinessState::Degraded
    );
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::IdentityLeaseLost
    );
    assert!(!health_status.identity_lease_healthy());
    assert!(
        metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
            .with_label_values(&["ready", "degraded", "identity_lease_lost"])
            .get()
            > before_lost
    );
    assert_eq!(
        metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
            .with_label_values(&["degraded", "degraded", "shutting_down"])
            .get(),
        before_shutdown
    );
}

#[test]
fn ownership_drift_sink_flips_readyz_and_health_reason() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    let startup_complete = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    });
    let sink = StartupWorkerReadinessSink::new(
        ready.clone(),
        startup_complete,
        health_status.clone(),
        "worker-a".into(),
        "instance-a".into(),
        "endpoint-a:50051".into(),
    );

    chronos::WorkerReadinessSink::ownership_drift_started(
        &sink,
        chronos::OwnershipDriftEvidence {
            generator_id: 7,
            contending_instance_id: "instance-b".into(),
            lease_expire_at_ms: 200,
            observed_at_ms: 120,
        },
    );

    assert!(!ready.load(Ordering::Relaxed));
    assert_eq!(
        health_status.readiness_state(),
        WorkerReadinessState::Degraded
    );
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::OwnershipDrift
    );
}

#[test]
fn ownership_drift_clear_waits_for_startup_completion_before_restoring_ready() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(false));
    let startup_complete = Arc::new(AtomicBool::new(false));
    set_startup_ready(&ready, false);
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    });
    let sink = StartupWorkerReadinessSink::new(
        ready.clone(),
        startup_complete.clone(),
        health_status.clone(),
        "worker-a".into(),
        "instance-a".into(),
        "endpoint-a:50051".into(),
    );

    chronos::WorkerReadinessSink::ownership_drift_started(
        &sink,
        chronos::OwnershipDriftEvidence {
            generator_id: 7,
            contending_instance_id: "instance-b".into(),
            lease_expire_at_ms: 200,
            observed_at_ms: 120,
        },
    );
    chronos::WorkerReadinessSink::ownership_drift_cleared(&sink);
    assert!(!ready.load(Ordering::Relaxed));

    chronos::WorkerReadinessSink::ownership_drift_started(
        &sink,
        chronos::OwnershipDriftEvidence {
            generator_id: 7,
            contending_instance_id: "instance-b".into(),
            lease_expire_at_ms: 220,
            observed_at_ms: 140,
        },
    );
    startup_complete.store(true, Ordering::Release);
    chronos::WorkerReadinessSink::ownership_drift_cleared(&sink);

    assert!(ready.load(Ordering::Relaxed));
    assert_eq!(health_status.readiness_state(), WorkerReadinessState::Ready);
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::Serving
    );
}

#[test]
fn ownership_drift_clear_does_not_restore_ready_after_shutdown_or_identity_loss() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    let startup_complete = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    });
    let sink = StartupWorkerReadinessSink::new(
        ready.clone(),
        startup_complete.clone(),
        health_status.clone(),
        "worker-a".into(),
        "instance-a".into(),
        "endpoint-a:50051".into(),
    );

    chronos::WorkerReadinessSink::ownership_drift_started(
        &sink,
        chronos::OwnershipDriftEvidence {
            generator_id: 7,
            contending_instance_id: "instance-b".into(),
            lease_expire_at_ms: 200,
            observed_at_ms: 120,
        },
    );
    health_status.mark_shutting_down();
    chronos::WorkerReadinessSink::ownership_drift_cleared(&sink);
    assert!(!ready.load(Ordering::Relaxed));
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::ShuttingDown
    );

    let ready = Arc::new(AtomicBool::new(true));
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    });
    let sink = StartupWorkerReadinessSink::new(
        ready.clone(),
        startup_complete,
        health_status.clone(),
        "worker-a".into(),
        "instance-a".into(),
        "endpoint-a:50051".into(),
    );
    chronos::WorkerReadinessSink::ownership_drift_started(
        &sink,
        chronos::OwnershipDriftEvidence {
            generator_id: 7,
            contending_instance_id: "instance-b".into(),
            lease_expire_at_ms: 200,
            observed_at_ms: 120,
        },
    );
    health_status.mark_identity_lease_lost();
    chronos::WorkerReadinessSink::ownership_drift_cleared(&sink);
    assert!(!ready.load(Ordering::Relaxed));
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::IdentityLeaseLost
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn identity_lease_loss_flips_readiness_and_triggers_shutdown() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    });
    let (lost_tx, lost_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let service = TsoService::new(
        explicit_required_config(),
        Arc::new(ManualClock::new(1_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let before_lost = metrics::TSO_IDENTITY_LEASE_EVENTS_TOTAL
        .with_label_values(&["lost"])
        .get();
    let before_shutdown = metrics::TSO_SHUTDOWN_TOTAL
        .with_label_values(&["identity_lease_lost"])
        .get();

    let monitor = spawn_identity_lease_loss_monitor(
        lost_rx,
        service.clone(),
        test_shutdown_context(ready.clone(), health_status.clone(), shutdown_tx),
    );

    lost_tx.send(true).unwrap();
    monitor.await.unwrap();

    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
    assert_eq!(
        service
            .ensure_timeline("identity-loss-local-gate")
            .await
            .unwrap_err(),
        TsoError::ServiceShuttingDown
    );
    assert_eq!(
        health_status.readiness_state(),
        WorkerReadinessState::Degraded
    );
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::IdentityLeaseLost
    );
    assert!(!health_status.identity_lease_healthy());
    assert_eq!(metrics::TSO_STARTUP_READY.get(), 0);
    assert!(
        metrics::TSO_IDENTITY_LEASE_EVENTS_TOTAL
            .with_label_values(&["lost"])
            .get()
            > before_lost
    );
    assert!(
        metrics::TSO_SHUTDOWN_TOTAL
            .with_label_values(&["identity_lease_lost"])
            .get()
            > before_shutdown
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn identity_lease_loss_monitor_triggers_shutdown_when_receiver_is_already_lost() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    });
    let (lost_tx, lost_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let service = TsoService::new(
        explicit_required_config(),
        Arc::new(ManualClock::new(1_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    lost_tx.send(true).unwrap();

    let monitor = spawn_identity_lease_loss_monitor(
        lost_rx,
        service.clone(),
        test_shutdown_context(ready.clone(), health_status.clone(), shutdown_tx),
    );

    monitor.await.unwrap();

    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::IdentityLeaseLost
    );
    assert!(!health_status.identity_lease_healthy());
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_identity_lease_loss_flips_readiness_and_triggers_shutdown() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();

    let unique_prefix = unique_test_etcd_prefix("lease-loss");
    let parsed_endpoints = parsed_test_etcd_endpoints();

    let clock = Arc::new(SystemClock);
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        instance_id: "instance-lease-loss".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
        etcd_endpoints: parsed_endpoints.clone(),
        lease_ttl_ms: 2_500,
        generator_maintenance_interval_ms: 200,
        ..explicit_required_config()
    };

    let startup = etcd_startup_config(config.clone(), unique_prefix.clone());
    let (service, identity_lease) =
        tokio::time::timeout(Duration::from_secs(5), build_tso_service(&startup, clock))
            .await
            .expect("etcd startup timed out")
            .expect("etcd startup should succeed");

    let identity_lease = identity_lease.expect("etcd startup should return an identity lease");

    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: config.worker_id.clone(),
        instance_id: config.effective_instance_id().to_string(),
        advertise_endpoint: config.advertise_endpoint.clone(),
    });
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let before_lost = metrics::TSO_IDENTITY_LEASE_EVENTS_TOTAL
        .with_label_values(&["lost"])
        .get();
    let before_shutdown = metrics::TSO_SHUTDOWN_TOTAL
        .with_label_values(&["identity_lease_lost"])
        .get();

    let monitor = spawn_identity_lease_loss_monitor(
        identity_lease.lost_receiver(),
        service.clone(),
        super::runtime::ShutdownContext {
            ready: ready.clone(),
            health_status: health_status.clone(),
            shutdown_tx,
            worker_id: config.worker_id.clone(),
            instance_id: config.effective_instance_id().to_string(),
            advertise_endpoint: config.advertise_endpoint.clone(),
        },
    );

    let mut client = etcd_client::Client::connect(parsed_endpoints, None)
        .await
        .expect("revoke client should connect");
    let lease_key = format!(
        "{}/identity/instances/{}",
        unique_prefix,
        config.effective_instance_id()
    );
    let lease_response = client
        .get(lease_key, None)
        .await
        .expect("identity lease record lookup should succeed");
    let lease_kv = lease_response
        .kvs()
        .first()
        .expect("identity lease record should exist");
    let lease_id = lease_kv.lease();
    assert_ne!(
        lease_id, 0,
        "identity lease record should carry a non-zero lease id"
    );
    let lease_record: serde_json::Value = serde_json::from_slice(lease_kv.value())
        .expect("identity lease record should deserialize as json");
    assert_eq!(
        lease_record
            .get("instance_id")
            .and_then(|value| value.as_str()),
        Some(config.effective_instance_id())
    );
    assert_eq!(
        lease_record
            .get("worker_id")
            .and_then(|value| value.as_str()),
        Some(config.worker_id.as_str())
    );
    assert_eq!(
        lease_record
            .get("advertise_endpoint")
            .and_then(|value| value.as_str()),
        Some(config.advertise_endpoint.as_str())
    );
    client
        .lease_revoke(lease_id)
        .await
        .expect("external revoke should succeed");

    tokio::time::timeout(Duration::from_secs(5), monitor)
        .await
        .expect("identity loss monitor should observe lease revoke")
        .expect("identity loss monitor should exit cleanly");

    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
    assert_eq!(
        health_status.readiness_state(),
        WorkerReadinessState::Degraded
    );
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::IdentityLeaseLost
    );
    assert!(!health_status.identity_lease_healthy());
    assert_eq!(metrics::TSO_STARTUP_READY.get(), 0);
    assert!(
        metrics::TSO_IDENTITY_LEASE_EVENTS_TOTAL
            .with_label_values(&["lost"])
            .get()
            > before_lost
    );
    assert!(
        metrics::TSO_SHUTDOWN_TOTAL
            .with_label_values(&["identity_lease_lost"])
            .get()
            > before_shutdown
    );

    clear_tso_env();
}
