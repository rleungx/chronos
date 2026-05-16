use super::*;

#[test]
fn startup_preflight_allows_derived_instance_id() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = explicit_required_config();
    validate_startup_preflight(&memory_startup_config(config.clone())).unwrap();
    assert_eq!(config.effective_instance_id(), "default-endpoint:50051");
}

#[test]
fn startup_preflight_requires_etcd_endpoints_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_ETCD_ENDPOINTS"));
}

#[test]
fn production_profile_requires_auditable_build_commit() {
    let mut config = explicit_required_config();
    config.production_profile = true;

    let error = validate_build_identity_for_profile(
        &config,
        chronos::BuildIdentity {
            version: chronos::build_version(),
            commit: "unknown",
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("auditable build commit"));
}

#[test]
fn production_profile_accepts_known_build_commit() {
    let mut config = explicit_required_config();
    config.production_profile = true;

    validate_build_identity_for_profile(
        &config,
        chronos::BuildIdentity {
            version: chronos::build_version(),
            commit: "deadbeef",
        },
    )
    .unwrap();
}

#[test]
fn startup_preflight_rejects_production_profile_with_memory_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();

    let startup = memory_startup_config(TsoConfig {
        production_profile: true,
        ..explicit_required_config()
    });

    let error = validate_startup_preflight(&startup).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_METADATA=etcd"));
}

#[test]
fn startup_preflight_accepts_production_profile_with_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();

    let startup = etcd_startup_config(
        TsoConfig {
            production_profile: true,
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            advertise_endpoint: "10.0.0.10:50051".into(),
            etcd_endpoints: vec!["127.0.0.1:2379".into()],
            ..explicit_required_config()
        },
        "/chronos",
    );

    validate_startup_preflight(&startup).unwrap();
}

#[test]
fn non_production_profile_allows_unknown_build_commit() {
    let config = explicit_required_config();

    validate_build_identity_for_profile(
        &config,
        chronos::BuildIdentity {
            version: chronos::build_version(),
            commit: "unknown",
        },
    )
    .unwrap();
}

#[test]
fn startup_preflight_does_not_backfill_etcd_endpoints_from_env() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    unsafe { env::set_var("CHRONOS_ETCD_ENDPOINTS", "127.0.0.1:2379") };
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_ETCD_ENDPOINTS"));
}

#[test]
fn startup_preflight_requires_explicit_worker_id_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        advertise_endpoint: "127.0.0.1:50051".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_WORKER_ID"));
}

#[test]
fn startup_preflight_requires_explicit_advertise_endpoint_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_ADVERTISE_ENDPOINT"));
}

#[test]
fn startup_preflight_requires_nonzero_safety_gap_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "10.0.0.10:50051".into(),
        safety_gap_ms: 0,
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_SAFETY_GAP_MS"));
}

#[test]
fn startup_preflight_rejects_localhost_advertise_endpoint_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "localhost:50051".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("localhost"));
}

#[test]
fn startup_preflight_rejects_loopback_advertise_endpoint_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();

    for advertise_endpoint in ["127.0.0.1:50051", "[::1]:50051"] {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            etcd_endpoints: vec!["127.0.0.1:2379".into()],
            worker_id: "worker-a".into(),
            advertise_endpoint: advertise_endpoint.into(),
            ..explicit_required_config()
        };
        let error =
            validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
        assert!(error.to_string().contains("loopback IP"));
    }
}

#[test]
fn startup_preflight_rejects_wildcard_advertise_endpoint_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "0.0.0.0:50051".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("wildcard"));
}

#[test]
fn startup_preflight_rejects_invalid_etcd_prefix() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "127.0.0.1:50051".into(),
        ..explicit_required_config()
    };
    let error =
        validate_startup_preflight(&etcd_startup_config(config, "relative-prefix")).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_ETCD_PREFIX"));
}

#[test]
fn startup_preflight_accepts_explicit_routable_advertise_endpoint_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "10.0.0.10:50051".into(),
        ..explicit_required_config()
    };
    validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap();
}

#[test]
fn startup_preflight_rejects_localhost_subdomain_advertise_endpoint_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "chronos-soak.localhost:50051".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains(".localhost"));
}

#[test]
fn startup_preflight_accepts_loopback_advertise_endpoint_for_dev_insecure_local_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let bind_addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        safety_gap_ms: 1,
        ..explicit_dev_insecure_local_config(bind_addr)
    };
    validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap();
}

#[test]
fn startup_preflight_rejects_invalid_advertise_endpoint_format_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "endpoint-a".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("host:port"));
}

#[test]
fn startup_preflight_rejects_http_etcd_endpoints_when_tls_is_configured() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let fixture = shared_test_tls_fixture();

    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        advertise_endpoint: "10.0.0.10:50051".into(),
        etcd_endpoints: vec!["http://10.0.0.20:2379".into()],
        etcd_ca_file: Some(fixture.ca_cert_path.clone()),
        etcd_cert_file: Some(fixture.client_cert_path.clone()),
        etcd_key_file: Some(fixture.client_key_path.clone()),
        etcd_timeout_ms: Some(100),
        ..explicit_required_config()
    };

    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error
        .to_string()
        .contains("https:// or bare host:port when etcd TLS is configured"));
}

#[test]
fn startup_preflight_rejects_unset_security_mode_for_local_only_memory_topology() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        advertise_endpoint: "127.0.0.1:50051".into(),
        bind_addr: "127.0.0.1:50051".into(),
        metrics_bind_addr: "127.0.0.1:9898".into(),
        ..TsoConfig::default()
    };
    let error = validate_startup_preflight(&memory_startup_config(config)).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_SECURITY_MODE"));
}

#[test]
fn startup_preflight_rejects_required_remote_exposed_without_grpc_tls_bundle() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        bind_addr: "0.0.0.0:50052".into(),
        advertise_endpoint: "10.0.0.10:50052".into(),
        security_mode: Some(TsoSecurityMode::Required),
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    };
    let error = validate_startup_preflight(&memory_startup_config(config)).unwrap_err();
    assert!(error
        .to_string()
        .contains("gRPC TLS requires a complete bundle"));
}

#[test]
fn startup_preflight_requires_control_and_status_allowlists_when_grpc_mtls_is_configured() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let fixture = shared_test_tls_fixture();
    let config = TsoConfig {
        security_mode: Some(TsoSecurityMode::DevInsecure),
        bind_addr: "127.0.0.1:50052".into(),
        advertise_endpoint: "127.0.0.1:50052".into(),
        grpc_tls_cert_file: Some(fixture.server_cert_path.clone()),
        grpc_tls_key_file: Some(fixture.server_key_path.clone()),
        grpc_client_ca_file: Some(fixture.ca_cert_path.clone()),
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    };

    let error = validate_startup_preflight(&memory_startup_config(config)).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("CHRONOS_GRPC_CONTROL_CERT_ALLOWLIST"));
}

#[test]
fn build_grpc_server_rejects_unreadable_tls_files() {
    let error = build_grpc_server(&TsoConfig {
        grpc_tls_cert_file: Some("/definitely/missing/server.crt".into()),
        grpc_tls_key_file: Some("/definitely/missing/server.key".into()),
        grpc_client_ca_file: Some("/definitely/missing/ca.pem".into()),
        ..explicit_required_config()
    })
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("CHRONOS_GRPC_TLS_CERT_FILE could not read"));
}
