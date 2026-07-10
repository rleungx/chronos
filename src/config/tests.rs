use super::{
    TsoConfig, TsoConfigValidationError, TsoSecurityMode, DEFAULT_GRPC_MAX_CONNECTIONS,
    PRODUCTION_MAX_BATCH_PER_REQUEST, PRODUCTION_MAX_TIMELINE_PROXY_LANES,
    PRODUCTION_MAX_TIMELINE_RUNTIME_ENTRIES,
};
use crate::ResourceTier;
use std::path::PathBuf;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

fn unique_temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "chronos-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn valid_config() -> TsoConfig {
    TsoConfig {
        instance_id: "instance-a".into(),
        advertise_endpoint: "127.0.0.1:50051".into(),
        security_mode: Some(TsoSecurityMode::DevInsecure),
        ..TsoConfig::default()
    }
}

#[test]
fn grpc_tls_paths_require_complete_bundle() {
    let config = TsoConfig {
        grpc_tls_cert_file: Some("server.crt".into()),
        ..valid_config()
    };

    assert!(matches!(
        config.grpc_tls_paths(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("gRPC TLS requires a complete bundle")
    ));
}

#[test]
fn metrics_tls_paths_return_complete_bundle() {
    let config = TsoConfig {
        metrics_tls_cert_file: Some("metrics.crt".into()),
        metrics_tls_key_file: Some("metrics.key".into()),
        metrics_client_ca_file: Some("metrics-ca.pem".into()),
        ..valid_config()
    };

    let paths = config
        .metrics_tls_paths()
        .expect("metrics bundle should parse");
    assert_eq!(
        paths,
        Some(super::ServerTlsPaths {
            cert_file: "metrics.crt",
            key_file: "metrics.key",
            client_ca_file: "metrics-ca.pem",
        })
    );
}

#[test]
fn etcd_tls_paths_require_complete_bundle() {
    let config = TsoConfig {
        etcd_ca_file: Some("ca.pem".into()),
        etcd_cert_file: Some("client.pem".into()),
        ..valid_config()
    };

    assert!(matches!(
        config.etcd_tls_paths(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("etcd TLS requires a complete bundle")
    ));
}

#[test]
fn validate_for_startup_accepts_valid_config() {
    assert!(valid_config().validate_for_startup().is_ok());
}

#[test]
fn validate_for_startup_rejects_blank_instance_id() {
    let mut config = valid_config();
    config.instance_id = "  ".into();
    assert!(config.validate_for_startup().is_ok());
    assert_eq!(config.effective_instance_id(), "127.0.0.1:50051");
}

#[test]
fn validate_for_startup_rejects_invalid_advertise_endpoint_format() {
    let mut config = valid_config();
    config.advertise_endpoint = "endpoint-a".into();
    assert_eq!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::InvalidAdvertiseEndpointFormat)
    );
}

#[test]
fn validate_for_startup_rejects_invalid_health_bind_addr() {
    let mut config = valid_config();
    config.health_bind_addr = Some("not-a-socket".into());
    assert!(matches!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_HEALTH_BIND_ADDR")
    ));
}

#[test]
fn validate_for_startup_rejects_health_bind_addr_port_conflict() {
    let mut config = valid_config();
    config.health_bind_addr = Some(config.metrics_bind_addr.clone());
    assert!(matches!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_HEALTH_BIND_ADDR")
    ));
}

#[test]
fn validate_for_startup_rejects_invalid_ownership_partition() {
    let mut config = valid_config();
    config.generator_ownership_modulo = 2;
    config.generator_ownership_remainder = 2;
    assert_eq!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::GeneratorOwnershipMisconfigured {
            modulo: 2,
            remainder: 2,
        })
    );
}

#[test]
fn validate_for_startup_rejects_blank_ownership_plan_id() {
    let mut config = valid_config();
    config.ownership_plan_id = "  ".into();
    assert!(matches!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("ownership_plan_id")
    ));
}

#[test]
fn validate_for_startup_rejects_zero_runtime_capacity() {
    let mut config = valid_config();
    config.max_timeline_runtime_entries = 0;
    assert_eq!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::ZeroMaxTimelineRuntimeEntries)
    );
}

#[test]
fn validate_for_startup_rejects_zero_concurrent_timeline_load_limit() {
    let mut config = valid_config();
    config.max_concurrent_timeline_loads = 0;
    assert_eq!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::ZeroMaxConcurrentTimelineLoads)
    );
}

#[test]
fn validate_for_startup_rejects_zero_grpc_connection_limit() {
    let mut config = valid_config();
    config.grpc_max_connections = 0;
    assert_eq!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::ZeroGrpcMaxConnections)
    );
}

#[test]
fn validate_for_startup_rejects_maintenance_interval_not_below_lease_ttl() {
    let mut config = valid_config();
    config.lease_ttl_ms = 200;
    config.generator_maintenance_interval_ms = 200;
    assert_eq!(
        config.validate_for_startup(),
        Err(
            TsoConfigValidationError::GeneratorMaintenanceIntervalTooLarge {
                interval_ms: 200,
                lease_ttl_ms: 200,
            }
        )
    );
}

#[test]
fn validate_for_startup_rejects_generator_lease_ttl_not_above_maintenance_interval() {
    let mut config = valid_config();
    config.generator_lease_ttl_ms = 200;
    config.generator_maintenance_interval_ms = 200;
    assert_eq!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::GeneratorLeaseTtlTooSmall {
            interval_ms: 200,
            lease_ttl_ms: 200,
        })
    );
}

#[test]
fn validate_for_startup_rejects_missing_default_tier_capacity() {
    let mut config = valid_config();
    config.default_resource_tier = ResourceTier::Warm;
    config.warm_generators = 0;
    assert_eq!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::MissingDefaultTierCapacity {
            resource_tier: ResourceTier::Warm,
        })
    );
}

#[test]
fn effective_instance_id_prefers_explicit_value() {
    let config = valid_config();
    assert_eq!(config.effective_instance_id(), "instance-a");
}

#[test]
fn resolve_effective_security_mode_requires_explicit_dev_insecure_for_local_only_topology() {
    let config = TsoConfig {
        bind_addr: "127.0.0.1:50051".into(),
        metrics_bind_addr: "127.0.0.1:9898".into(),
        advertise_endpoint: "127.0.0.1:50051".into(),
        ..TsoConfig::default()
    };
    assert!(matches!(
        config.resolve_effective_security_mode(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_SECURITY_MODE")
    ));
}

#[test]
fn resolve_effective_security_mode_accepts_explicit_dev_insecure_for_local_only_topology() {
    let config = TsoConfig {
        bind_addr: "127.0.0.1:50051".into(),
        metrics_bind_addr: "127.0.0.1:9898".into(),
        advertise_endpoint: "127.0.0.1:50051".into(),
        ..TsoConfig::default().with_security_mode(TsoSecurityMode::DevInsecure)
    };
    assert_eq!(
        config.resolve_effective_security_mode(),
        Ok(TsoSecurityMode::DevInsecure)
    );
}

#[test]
fn resolve_effective_security_mode_accepts_localhost_subdomain_for_local_only_topology() {
    let config = TsoConfig {
        bind_addr: "127.0.0.1:50051".into(),
        metrics_bind_addr: "127.0.0.1:9898".into(),
        advertise_endpoint: "chronos-soak.localhost:50051".into(),
        ..TsoConfig::default().with_security_mode(TsoSecurityMode::DevInsecure)
    };
    assert_eq!(
        config.resolve_effective_security_mode(),
        Ok(TsoSecurityMode::DevInsecure)
    );
}

#[test]
fn resolve_effective_security_mode_infers_required_for_nonlocal_topology() {
    let config = TsoConfig {
        advertise_endpoint: "10.0.0.10:50051".into(),
        ..TsoConfig::default()
    };
    assert_eq!(
        config.resolve_effective_security_mode(),
        Ok(TsoSecurityMode::Required)
    );
}

#[test]
fn resolve_effective_security_mode_rejects_dev_insecure_with_production_profile() {
    let config = TsoConfig {
        bind_addr: "127.0.0.1:50051".into(),
        metrics_bind_addr: "127.0.0.1:9898".into(),
        advertise_endpoint: "127.0.0.1:50051".into(),
        production_profile: true,
        ..TsoConfig::default().with_security_mode(TsoSecurityMode::DevInsecure)
    };
    assert!(matches!(
        config.resolve_effective_security_mode(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_PROFILE=production")
    ));
}

#[test]
fn validate_for_startup_rejects_required_remote_exposed_grpc_without_tls_bundle() {
    let config = TsoConfig {
        bind_addr: "0.0.0.0:50052".into(),
        advertise_endpoint: "10.0.0.10:50052".into(),
        security_mode: Some(TsoSecurityMode::Required),
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    };
    assert!(matches!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("gRPC TLS requires a complete bundle")
    ));
}

#[test]
fn validate_for_startup_rejects_unreadable_grpc_tls_files() {
    let config = TsoConfig {
        bind_addr: "0.0.0.0:50052".into(),
        advertise_endpoint: "10.0.0.10:50052".into(),
        security_mode: Some(TsoSecurityMode::Required),
        grpc_tls_cert_file: Some("/definitely/missing/server.crt".into()),
        grpc_tls_key_file: Some("/definitely/missing/server.key".into()),
        grpc_client_ca_file: Some("/definitely/missing/ca.pem".into()),
        grpc_control_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_status_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    };
    assert!(matches!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_GRPC_TLS_CERT_FILE could not read")
    ));
}

#[test]
fn validate_for_startup_rejects_unreadable_metrics_tls_files() {
    let config = TsoConfig {
        metrics_tls_cert_file: Some("/definitely/missing/metrics.crt".into()),
        metrics_tls_key_file: Some("/definitely/missing/metrics.key".into()),
        metrics_client_ca_file: Some("/definitely/missing/metrics-ca.pem".into()),
        ..valid_config()
    };
    assert!(matches!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_METRICS_TLS_CERT_FILE could not read")
    ));
}

#[test]
fn validate_for_startup_rejects_unreadable_etcd_tls_files() {
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["https://10.0.0.20:2379".into()],
        advertise_endpoint: "10.0.0.10:50051".into(),
        security_mode: Some(TsoSecurityMode::Required),
        etcd_ca_file: Some("/definitely/missing/ca.pem".into()),
        etcd_cert_file: Some("/definitely/missing/client.pem".into()),
        etcd_key_file: Some("/definitely/missing/client-key.pem".into()),
        etcd_timeout_ms: Some(100),
        worker_id: "worker-a".into(),
        safety_gap_ms: 500,
        ..valid_config()
    };
    assert!(matches!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_ETCD_CA_FILE could not read")
    ));
}

#[cfg(unix)]
#[test]
fn validate_for_startup_rejects_group_readable_private_key_files() {
    let dir = unique_temp_dir("config-key-perms-reject");
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    let ca_path = dir.join("ca.pem");
    std::fs::write(&cert_path, b"cert").unwrap();
    std::fs::write(&key_path, b"key").unwrap();
    std::fs::write(&ca_path, b"ca").unwrap();
    std::fs::set_permissions(&cert_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o640)).unwrap();
    std::fs::set_permissions(&ca_path, std::fs::Permissions::from_mode(0o644)).unwrap();

    let config = TsoConfig {
        bind_addr: "0.0.0.0:50052".into(),
        advertise_endpoint: "10.0.0.10:50052".into(),
        security_mode: Some(TsoSecurityMode::Required),
        grpc_tls_cert_file: Some(cert_path.to_string_lossy().into_owned()),
        grpc_tls_key_file: Some(key_path.to_string_lossy().into_owned()),
        grpc_client_ca_file: Some(ca_path.to_string_lossy().into_owned()),
        grpc_control_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_route_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_timestamp_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_status_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    };
    assert!(matches!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_GRPC_TLS_KEY_FILE must not be group/other accessible")
    ));
}

#[test]
fn validate_for_startup_rejects_symlink_private_key_files() {
    let dir = unique_temp_dir("config-key-symlink-reject");
    let cert_path = dir.join("server.crt");
    let key_target_path = dir.join("server.key.real");
    let key_link_path = dir.join("server.key");
    let ca_path = dir.join("ca.pem");
    std::fs::write(&cert_path, b"cert").unwrap();
    std::fs::write(&key_target_path, b"key").unwrap();
    std::fs::write(&ca_path, b"ca").unwrap();

    #[cfg(unix)]
    std::os::unix::fs::symlink(&key_target_path, &key_link_path).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_file(&key_target_path, &key_link_path).unwrap();

    let config = TsoConfig {
        bind_addr: "0.0.0.0:50052".into(),
        advertise_endpoint: "10.0.0.10:50052".into(),
        security_mode: Some(TsoSecurityMode::Required),
        grpc_tls_cert_file: Some(cert_path.to_string_lossy().into_owned()),
        grpc_tls_key_file: Some(key_link_path.to_string_lossy().into_owned()),
        grpc_client_ca_file: Some(ca_path.to_string_lossy().into_owned()),
        grpc_control_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_route_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_timestamp_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_status_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    };
    assert!(matches!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_GRPC_TLS_KEY_FILE must not be a symlink")
    ));
}

#[cfg(unix)]
#[test]
fn validate_for_startup_accepts_owner_only_private_key_files() {
    let dir = unique_temp_dir("config-key-owner-only-accept");
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    let ca_path = dir.join("ca.pem");
    std::fs::write(&cert_path, b"cert").unwrap();
    std::fs::write(&key_path, b"key").unwrap();
    std::fs::write(&ca_path, b"ca").unwrap();
    std::fs::set_permissions(&cert_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::set_permissions(&ca_path, std::fs::Permissions::from_mode(0o644)).unwrap();

    let config = TsoConfig {
        bind_addr: "0.0.0.0:50052".into(),
        advertise_endpoint: "10.0.0.10:50052".into(),
        security_mode: Some(TsoSecurityMode::Required),
        grpc_tls_cert_file: Some(cert_path.to_string_lossy().into_owned()),
        grpc_tls_key_file: Some(key_path.to_string_lossy().into_owned()),
        grpc_client_ca_file: Some(ca_path.to_string_lossy().into_owned()),
        grpc_control_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_route_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_timestamp_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_status_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    };
    assert!(config.validate_for_startup().is_ok());
}

#[test]
fn validate_for_startup_rejects_required_remote_exposed_grpc_with_zero_timeout() {
    let (cert_path, key_path, ca_path) = crate::test_tls::readable_test_tls_paths();
    let config = TsoConfig {
        bind_addr: "0.0.0.0:50052".into(),
        advertise_endpoint: "10.0.0.10:50052".into(),
        security_mode: Some(TsoSecurityMode::Required),
        grpc_tls_cert_file: Some(cert_path.into()),
        grpc_tls_key_file: Some(key_path.into()),
        grpc_client_ca_file: Some(ca_path.into()),
        grpc_control_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_status_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_request_timeout_ms: Some(0),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    };
    assert!(matches!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_GRPC_REQUEST_TIMEOUT_MS")
    ));
}

#[test]
fn authoritative_metadata_runtime_contract_rejects_default_worker_id() {
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        advertise_endpoint: "10.0.0.10:50051".into(),
        safety_gap_ms: 500,
        ..valid_config()
    };
    assert!(matches!(
        config.validate_authoritative_metadata_runtime_contract(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_WORKER_ID")
    ));
}

#[test]
fn authoritative_metadata_runtime_contract_rejects_unroutable_advertise_endpoint() {
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        advertise_endpoint: "localhost:50051".into(),
        security_mode: Some(TsoSecurityMode::Required),
        safety_gap_ms: 500,
        ..valid_config()
    };
    assert!(matches!(
        config.validate_authoritative_metadata_runtime_contract(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("localhost")
    ));
}

#[test]
fn authoritative_metadata_runtime_contract_rejects_loopback_advertise_endpoint() {
    for advertise_endpoint in ["127.0.0.1:50051", "[::1]:50051"] {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            advertise_endpoint: advertise_endpoint.into(),
            security_mode: Some(TsoSecurityMode::Required),
            safety_gap_ms: 500,
            ..valid_config()
        };
        assert!(matches!(
            config.validate_authoritative_metadata_runtime_contract(),
            Err(TsoConfigValidationError::Security(message))
                if message.contains("loopback IP")
        ));
    }
}

#[test]
fn authoritative_metadata_runtime_contract_rejects_localhost_subdomain() {
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        advertise_endpoint: "chronos-a.localhost:50051".into(),
        security_mode: Some(TsoSecurityMode::Required),
        safety_gap_ms: 500,
        ..valid_config()
    };
    assert!(matches!(
        config.validate_authoritative_metadata_runtime_contract(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains(".localhost")
    ));
}

#[test]
fn authoritative_metadata_runtime_contract_accepts_dev_insecure_loopback_for_local_tests() {
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        advertise_endpoint: "127.0.0.1:50051".into(),
        safety_gap_ms: 500,
        ..valid_config()
    };
    assert!(config
        .validate_authoritative_metadata_runtime_contract()
        .is_ok());
}

#[test]
fn authoritative_metadata_runtime_contract_rejects_zero_safety_gap() {
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        advertise_endpoint: "10.0.0.10:50051".into(),
        safety_gap_ms: 0,
        ..valid_config()
    };
    assert!(matches!(
        config.validate_authoritative_metadata_runtime_contract(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_SAFETY_GAP_MS")
    ));
}

#[test]
fn authoritative_metadata_runtime_contract_rejects_default_partitioned_ownership_plan() {
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        advertise_endpoint: "10.0.0.10:50051".into(),
        safety_gap_ms: 500,
        generator_ownership_modulo: 2,
        generator_ownership_remainder: 0,
        ownership_plan_id: " Default ".into(),
        ..valid_config()
    };
    assert!(matches!(
        config.validate_authoritative_metadata_runtime_contract(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_OWNERSHIP_PLAN_ID")
    ));
}

#[test]
fn authoritative_metadata_runtime_contract_accepts_explicit_partitioned_ownership_plan() {
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        advertise_endpoint: "10.0.0.10:50051".into(),
        safety_gap_ms: 500,
        generator_ownership_modulo: 2,
        generator_ownership_remainder: 0,
        ownership_plan_id: "prod-2026-05-10".into(),
        ..valid_config()
    };
    assert!(config
        .validate_authoritative_metadata_runtime_contract()
        .is_ok());
}

#[test]
fn authoritative_metadata_store_contract_rejects_invalid_prefix() {
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        ..valid_config()
    };
    assert!(matches!(
        config.validate_authoritative_metadata_store_contract(
            &["127.0.0.1:2379".into()],
            "relative-prefix"
        ),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_ETCD_PREFIX")
    ));
}

#[test]
fn validate_for_startup_rejects_invalid_control_cert_allowlist_entry() {
    let (cert_path, key_path, ca_path) = crate::test_tls::readable_test_tls_paths();
    let config = TsoConfig {
        bind_addr: "0.0.0.0:50052".into(),
        advertise_endpoint: "10.0.0.10:50052".into(),
        security_mode: Some(TsoSecurityMode::Required),
        grpc_tls_cert_file: Some(cert_path.into()),
        grpc_tls_key_file: Some(key_path.into()),
        grpc_client_ca_file: Some(ca_path.into()),
        grpc_control_cert_allowlist: vec!["not-a-sha256".into()],
        grpc_status_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    };

    assert!(matches!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_GRPC_CONTROL_CERT_ALLOWLIST")
    ));
}

#[test]
fn validate_for_startup_requires_status_cert_allowlist_when_grpc_mtls_is_enabled() {
    let (cert_path, key_path, ca_path) = crate::test_tls::readable_test_tls_paths();
    let config = TsoConfig {
        bind_addr: "0.0.0.0:50052".into(),
        advertise_endpoint: "10.0.0.10:50052".into(),
        security_mode: Some(TsoSecurityMode::Required),
        grpc_tls_cert_file: Some(cert_path.into()),
        grpc_tls_key_file: Some(key_path.into()),
        grpc_client_ca_file: Some(ca_path.into()),
        grpc_control_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_route_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_timestamp_cert_allowlist: vec![
            crate::test_tls::placeholder_client_cert_fingerprint().into()
        ],
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    };

    assert!(matches!(
        config.validate_for_startup(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_GRPC_STATUS_CERT_ALLOWLIST")
    ));
}

#[test]
fn default_config_uses_production_safe_capacity_defaults() {
    let config = TsoConfig::default();
    assert_eq!(
        config.max_batch_per_request,
        PRODUCTION_MAX_BATCH_PER_REQUEST
    );
    assert_eq!(
        config.max_timeline_proxy_lanes,
        PRODUCTION_MAX_TIMELINE_PROXY_LANES
    );
    assert_eq!(
        config.max_timeline_runtime_entries,
        PRODUCTION_MAX_TIMELINE_RUNTIME_ENTRIES
    );
    assert_eq!(config.grpc_max_connections, DEFAULT_GRPC_MAX_CONNECTIONS);
}

#[test]
fn production_profile_applies_safer_capacity_defaults() {
    let mut config = TsoConfig::default();
    config.apply_profile("production").unwrap();
    assert_eq!(
        config.max_batch_per_request,
        PRODUCTION_MAX_BATCH_PER_REQUEST
    );
    assert_eq!(
        config.max_timeline_proxy_lanes,
        PRODUCTION_MAX_TIMELINE_PROXY_LANES
    );
    assert_eq!(
        config.max_timeline_runtime_entries,
        PRODUCTION_MAX_TIMELINE_RUNTIME_ENTRIES
    );
    assert_eq!(config.max_clock_skew_ms, 500);
    assert_eq!(config.safety_gap_ms, 500);
}

#[test]
fn production_profile_covers_a_larger_configured_clock_skew_bound() {
    let mut config = TsoConfig {
        max_clock_skew_ms: 750,
        ..TsoConfig::default()
    };

    config.apply_profile("production").unwrap();

    assert_eq!(config.safety_gap_ms, 750);
}

#[test]
fn authoritative_metadata_rejects_safety_gap_below_certified_clock_skew() {
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        advertise_endpoint: "10.0.0.10:50051".into(),
        security_mode: Some(TsoSecurityMode::Required),
        safety_gap_ms: 499,
        max_clock_skew_ms: 500,
        ..valid_config()
    };
    assert!(matches!(
        config.validate_authoritative_metadata_runtime_contract(),
        Err(TsoConfigValidationError::Security(message))
            if message.contains("CHRONOS_MAX_CLOCK_SKEW_MS")
    ));
}
