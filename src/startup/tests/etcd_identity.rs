use super::*;

#[test]
fn startup_preflight_validates_etcd_endpoints_from_typed_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let startup = super::config::LoadedStartupConfig::new(
        TsoConfig {
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            advertise_endpoint: "10.0.0.10:50051".into(),
            ..explicit_required_config()
        },
        super::config::StartupMetadata::Etcd(super::config::EtcdStartupConfig::new(
            vec!["127.0.0.1:2379".into()],
            "/chronos",
        )),
        super::config::StartupLoggingConfig::default(),
    );

    validate_startup_preflight(&startup).unwrap();
}

#[test]
fn startup_preflight_enforces_etcd_runtime_contract_from_typed_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let startup = super::config::LoadedStartupConfig::new(
        explicit_required_config(),
        super::config::StartupMetadata::Etcd(super::config::EtcdStartupConfig::new(
            vec!["127.0.0.1:2379".into()],
            "/chronos",
        )),
        super::config::StartupLoggingConfig::default(),
    );

    let error = validate_startup_preflight(&startup).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_WORKER_ID"));
}

#[test]
fn loaded_startup_config_does_not_backfill_config_metadata_kind_from_typed_metadata() {
    let startup = super::config::LoadedStartupConfig::new(
        explicit_required_config(),
        super::config::StartupMetadata::Etcd(super::config::EtcdStartupConfig::new(
            vec!["127.0.0.1:2379".into()],
            "/chronos",
        )),
        super::config::StartupLoggingConfig::default(),
    );

    assert_eq!(startup.config.metadata_kind, "memory");
    assert_eq!(startup.metadata_kind(), "etcd");
}

#[test]
fn startup_ready_transition_records_metric() {
    let before = metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
        .with_label_values(&["starting", "ready", "serving"])
        .get();

    record_worker_readiness_transition(
        "starting",
        chronos::proto::v1::WorkerReadinessState::Ready,
        chronos::proto::v1::WorkerReadinessReason::Serving,
    );

    assert!(
        metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
            .with_label_values(&["starting", "ready", "serving"])
            .get()
            > before
    );
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_startup_rejects_duplicate_instance_identity() {
    clear_tso_env();

    let unique_prefix = unique_test_etcd_prefix("dup");

    let clock = Arc::new(SystemClock);
    let parsed_endpoints = parsed_test_etcd_endpoints();
    let config_a = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        instance_id: "instance-dupe".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
        etcd_endpoints: parsed_endpoints.clone(),
        lease_ttl_ms: 30_000,
        ..explicit_required_config()
    };
    let config_b = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-b".into(),
        instance_id: "instance-dupe".into(),
        advertise_endpoint: "endpoint-b:50051".into(),
        etcd_endpoints: parsed_endpoints,
        lease_ttl_ms: 30_000,
        ..explicit_required_config()
    };

    let startup_a = etcd_startup_config(config_a, unique_prefix.clone());
    let startup_b = etcd_startup_config(config_b, unique_prefix);

    let (service_a, mut lease_a) = tokio::time::timeout(
        Duration::from_secs(5),
        build_tso_service(&startup_a, clock.clone()),
    )
    .await
    .expect("first startup timed out")
    .expect("first startup should succeed");

    let duplicate_result = tokio::time::timeout(
        Duration::from_secs(5),
        build_tso_service(&startup_b, clock.clone()),
    )
    .await
    .expect("second startup timed out");

    let error = match duplicate_result {
        Ok(_) => panic!("duplicate identity startup should fail"),
        Err(error) => error,
    };

    let error = error
        .downcast::<chronos::TsoError>()
        .expect("expected TsoError");
    assert!(matches!(
        *error,
        chronos::TsoError::InstanceIdentityInUse { ref instance_id }
            if instance_id == "instance-dupe"
    ));

    if let Some(identity_lease) = lease_a.as_mut() {
        identity_lease.shutdown().await;
    }
    service_a.shutdown().await;

    let (service_b, mut lease_b) =
        tokio::time::timeout(Duration::from_secs(5), build_tso_service(&startup_b, clock))
            .await
            .expect("restart after graceful release timed out")
            .expect("identity should be reacquired after graceful release");

    if let Some(identity_lease) = lease_b.as_mut() {
        identity_lease.shutdown().await;
    }
    service_b.shutdown().await;

    clear_tso_env();
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_startup_failure_after_identity_lease_revokes_lease() {
    clear_tso_env();

    let unique_prefix = unique_test_etcd_prefix("startup-revoke");
    let parsed_endpoints = parsed_test_etcd_endpoints();
    let instance_id = "instance-startup-revoke";
    let startup = etcd_startup_config(
        TsoConfig {
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            instance_id: instance_id.into(),
            advertise_endpoint: "endpoint-a:50051".into(),
            etcd_endpoints: parsed_endpoints.clone(),
            max_timeline_runtime_entries: 0,
            lease_ttl_ms: 30_000,
            ..explicit_required_config()
        },
        unique_prefix.clone(),
    );

    let error = match tokio::time::timeout(
        Duration::from_secs(5),
        build_tso_service(&startup, Arc::new(SystemClock)),
    )
    .await
    .expect("startup failure should not hang")
    {
        Ok(_) => panic!("startup should fail after lease acquisition"),
        Err(error) => error,
    };

    assert!(
        !error.to_string().is_empty(),
        "startup failure should surface a non-empty error"
    );

    let lease_key = format!("{}/identity/instances/{instance_id}", unique_prefix);
    let mut client = etcd_client::Client::connect(parsed_endpoints, None)
        .await
        .expect("etcd client should connect");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = client
                .get(lease_key.clone(), None)
                .await
                .expect("identity lease record lookup should succeed");
            if response.kvs().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("identity lease should be revoked after startup failure");

    clear_tso_env();
}
