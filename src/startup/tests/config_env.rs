use super::*;

#[test]
fn load_tso_config_reads_security_mode_and_surface_inputs() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    unsafe {
        env::set_var("CHRONOS_SECURITY_MODE", "dev-insecure");
        env::set_var("CHRONOS_BIND_ADDR", "127.0.0.1:50051");
        env::set_var("CHRONOS_HEALTH_BIND_ADDR", "127.0.0.1:9897");
        env::set_var("CHRONOS_METRICS_BIND_ADDR", "127.0.0.1:9898");
        env::set_var("CHRONOS_METADATA", "etcd");
        env::set_var("CHRONOS_ETCD_ENDPOINTS", "127.0.0.1:2379,127.0.0.1:2380");
        env::set_var("CHRONOS_AUTO_FAILOVER_ENABLED", "true");
        env::set_var("CHRONOS_AUTO_FAILOVER_INTERVAL_MS", "2500");
        env::set_var("CHRONOS_AUTO_FAILOVER_BATCH_SIZE", "7");
        env::set_var("CHRONOS_MAX_TIMELINE_PROXY_LANES", "8192");
        env::set_var("CHRONOS_MAX_TIMELINE_RUNTIME_ENTRIES", "16384");
        env::set_var("CHRONOS_MAX_CONCURRENT_TIMELINE_LOADS", "128");
        env::set_var(
            "CHRONOS_GRPC_CONTROL_CERT_ALLOWLIST",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        );
        env::set_var(
            "CHRONOS_GRPC_ROUTE_CERT_ALLOWLIST",
            "1111111111111111111111111111111111111111111111111111111111111111",
        );
        env::set_var(
            "CHRONOS_GRPC_TIMESTAMP_CERT_ALLOWLIST",
            "2222222222222222222222222222222222222222222222222222222222222222",
        );
        env::set_var(
            "CHRONOS_GRPC_STATUS_CERT_ALLOWLIST",
            "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
        );
    }

    let config = load_tso_config().unwrap();
    assert_eq!(config.security_mode, Some(TsoSecurityMode::DevInsecure));
    assert_eq!(config.bind_addr, "127.0.0.1:50051");
    assert_eq!(config.health_bind_addr.as_deref(), Some("127.0.0.1:9897"));
    assert_eq!(config.metrics_bind_addr, "127.0.0.1:9898");
    assert_eq!(config.metadata_kind, "etcd");
    assert_eq!(
        config.etcd_endpoints,
        vec!["127.0.0.1:2379", "127.0.0.1:2380"]
    );
    assert_eq!(
        config.grpc_control_cert_allowlist,
        vec!["0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"]
    );
    assert_eq!(
        config.grpc_route_cert_allowlist,
        vec!["1111111111111111111111111111111111111111111111111111111111111111"]
    );
    assert_eq!(
        config.grpc_timestamp_cert_allowlist,
        vec!["2222222222222222222222222222222222222222222222222222222222222222"]
    );
    assert_eq!(
        config.grpc_status_cert_allowlist,
        vec!["fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"]
    );
    assert!(config.auto_failover_enabled);
    assert_eq!(config.auto_failover_interval_ms, 2500);
    assert_eq!(config.auto_failover_batch_size, 7);
    assert_eq!(config.max_timeline_proxy_lanes, 8192);
    assert_eq!(config.max_timeline_runtime_entries, 16384);
    assert_eq!(config.max_concurrent_timeline_loads, 128);
}

#[test]
fn load_startup_config_reads_logging_controls() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    unsafe {
        env::set_var("CHRONOS_LOG_FORMAT", "text");
        env::set_var("CHRONOS_LOG_FILTER", "debug,chronos=trace");
    }

    let startup = load_startup_config().unwrap();
    assert_eq!(
        startup.logging.format,
        super::config::StartupLogFormat::Text
    );
    assert_eq!(startup.logging.filter, "debug,chronos=trace");
}

#[test]
fn load_startup_config_rejects_invalid_log_format() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    unsafe { env::set_var("CHRONOS_LOG_FORMAT", "yaml") };

    let error = load_startup_config().unwrap_err();
    assert!(error.to_string().contains("CHRONOS_LOG_FORMAT"));
}

#[test]
fn load_startup_config_rejects_blank_log_filter() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    unsafe { env::set_var("CHRONOS_LOG_FILTER", "   ") };

    let error = load_startup_config().unwrap_err();
    assert!(error
        .to_string()
        .contains("CHRONOS_LOG_FILTER must not be blank"));
}

#[test]
fn load_startup_config_rejects_invalid_log_filter_syntax() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    unsafe { env::set_var("CHRONOS_LOG_FILTER", "chronos=[") };

    let error = load_startup_config().unwrap_err();
    assert!(error.to_string().contains("CHRONOS_LOG_FILTER is invalid"));
}

#[test]
fn load_startup_config_wraps_etcd_bootstrap_tuple_as_typed_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    unsafe {
        env::set_var("CHRONOS_METADATA", "etcd");
        env::set_var("CHRONOS_ETCD_ENDPOINTS", "127.0.0.1:2379,127.0.0.1:2380");
        env::set_var("CHRONOS_ETCD_PREFIX", "/typed-prefix");
    }

    let startup = load_startup_config().unwrap();
    assert_eq!(startup.metadata_kind(), "etcd");
    match startup.metadata {
        super::config::StartupMetadata::Etcd(etcd) => {
            assert_eq!(etcd.endpoints, vec!["127.0.0.1:2379", "127.0.0.1:2380"]);
            assert_eq!(etcd.prefix, "/typed-prefix");
        }
        super::config::StartupMetadata::Memory => {
            panic!("expected etcd startup metadata")
        }
    }
}

#[test]
fn load_startup_config_defaults_etcd_prefix_for_typed_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    unsafe { env::set_var("CHRONOS_METADATA", "etcd") };

    let startup = load_startup_config().unwrap();
    match startup.metadata {
        super::config::StartupMetadata::Etcd(etcd) => {
            assert!(etcd.endpoints.is_empty());
            assert_eq!(etcd.prefix, "/chronos");
        }
        super::config::StartupMetadata::Memory => {
            panic!("expected etcd startup metadata")
        }
    }
}

#[test]
fn startup_shell_raw_env_metadata_reads_remain_centralized_in_config_loader() {
    let preflight = include_str!("../preflight.rs");
    let bootstrap = include_str!("../bootstrap.rs");

    for (label, source) in [("preflight", preflight), ("bootstrap", bootstrap)] {
        assert!(
            !source.contains("env::var("),
            "{label} must not read raw env directly"
        );
        assert!(
            !source.contains("read_env_or_default("),
            "{label} must not reconstruct startup metadata via read_env_or_default"
        );
    }
}

#[test]
fn startup_preflight_returns_validated_plan_with_logging_state() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let fixture = shared_test_tls_fixture();

    let startup = memory_startup_config(TsoConfig {
        metrics_tls_cert_file: Some(fixture.server_cert_path.clone()),
        metrics_tls_key_file: Some(fixture.server_key_path.clone()),
        metrics_client_ca_file: Some(fixture.ca_cert_path.clone()),
        ..explicit_required_config()
    });

    let plan = validate_startup_preflight(&startup).unwrap();
    assert_eq!(plan.effective_security_mode(), TsoSecurityMode::Required);
    assert_eq!(
        plan.metrics_transport(),
        crate::startup::preflight::MetricsTransport::Mtls
    );
}

#[test]
fn startup_preflight_logger_uses_validated_plan_instead_of_rederiving_state() {
    let preflight = include_str!("../preflight.rs");

    assert!(
        preflight.contains("ValidatedStartupPlan"),
        "preflight should expose a validated startup plan"
    );
    assert!(
        !preflight.contains("expect(\"security mode already validated during preflight\")"),
        "logger should not re-validate security mode via expect"
    );
}

#[test]
fn load_tso_config_applies_production_profile_capacity_defaults() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    unsafe { env::set_var("CHRONOS_PROFILE", "production") };

    let config = load_tso_config().unwrap();
    assert_eq!(
        config.max_batch_per_request,
        chronos::PRODUCTION_MAX_BATCH_PER_REQUEST
    );
    assert_eq!(
        config.max_timeline_proxy_lanes,
        chronos::PRODUCTION_MAX_TIMELINE_PROXY_LANES
    );
    assert_eq!(
        config.max_timeline_runtime_entries,
        chronos::PRODUCTION_MAX_TIMELINE_RUNTIME_ENTRIES
    );

    clear_tso_env();
    unsafe { env::set_var("CHRONOS_PROFILE", "production") };
    let config = load_tso_config().unwrap();
    assert_eq!(
        config.max_batch_per_request,
        chronos::PRODUCTION_MAX_BATCH_PER_REQUEST
    );
    assert_eq!(config.generator_ownership_remainder, 0);
    assert_eq!(config.generator_ownership_modulo, 1);
}

#[test]
fn load_tso_config_accepts_production_capacity_env_overrides() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    unsafe {
        env::set_var("CHRONOS_PROFILE", "production");
        env::set_var("CHRONOS_MAX_TIMELINE_PROXY_LANES", "12288");
        env::set_var("CHRONOS_MAX_TIMELINE_RUNTIME_ENTRIES", "24576");
        env::set_var("CHRONOS_MAX_CONCURRENT_TIMELINE_LOADS", "192");
    }

    let config = load_tso_config().unwrap();
    assert_eq!(config.max_timeline_proxy_lanes, 12288);
    assert_eq!(config.max_timeline_runtime_entries, 24576);
    assert_eq!(config.max_concurrent_timeline_loads, 192);
}

#[test]
fn load_tso_config_accepts_generator_ownership_partition_env() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();

    unsafe {
        env::set_var("CHRONOS_OWNERSHIP_PLAN_ID", "scale-2026-05-10");
        env::set_var("CHRONOS_GENERATOR_OWNERSHIP_MODULO", "4");
        env::set_var("CHRONOS_GENERATOR_OWNERSHIP_REMAINDERS", "1,2");
    }

    let config = load_tso_config().unwrap();
    assert_eq!(config.ownership_plan_id, "scale-2026-05-10");
    assert_eq!(config.generator_ownership_modulo, 4);
    assert_eq!(config.generator_ownership_remainder, 1);
    assert_eq!(config.generator_ownership_remainders, vec![1, 2]);
}

#[test]
fn load_tso_config_rejects_removed_internal_tuning_env_vars() {
    let _guard = ENV_LOCK.lock().unwrap();

    for key in ["CHRONOS_ROUTE_CACHE_TTL_MS", "CHRONOS_GENERATOR_OWNERSHIP"] {
        clear_tso_env();
        unsafe { env::set_var(key, "test-value") };

        let error = load_tso_config().unwrap_err();
        let message = error.to_string();
        assert!(message.contains("unsupported startup tuning env var(s)"));
        assert!(message.contains(key));
    }
}

#[test]
fn load_startup_config_accepts_production_capacity_env_vars() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    unsafe {
        env::set_var("CHRONOS_MAX_TIMELINE_PROXY_LANES", "8192");
        env::set_var("CHRONOS_MAX_TIMELINE_RUNTIME_ENTRIES", "16384");
        env::set_var("CHRONOS_MAX_CONCURRENT_TIMELINE_LOADS", "128");
    }

    let startup = load_startup_config().unwrap();
    assert_eq!(startup.config.max_timeline_proxy_lanes, 8192);
    assert_eq!(startup.config.max_timeline_runtime_entries, 16384);
    assert_eq!(startup.config.max_concurrent_timeline_loads, 128);
}
