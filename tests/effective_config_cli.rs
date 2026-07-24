use std::process::Command;

fn chronos_bin() -> &'static str {
    env!("CARGO_BIN_EXE_chronos")
}

#[test]
fn effective_config_distinguishes_configured_and_grant_request_identity_ttl() {
    for (configured_ms, request_seconds, request_ms) in [
        ("500", "1", "1000"),
        ("1500", "2", "2000"),
        ("3000", "3", "3000"),
    ] {
        let output = Command::new(chronos_bin())
            .arg("--print-effective-config")
            .env_clear()
            .env("CHRONOS_SECURITY_MODE", "dev-insecure")
            .env("CHRONOS_METADATA", "etcd")
            .env("CHRONOS_ETCD_ENDPOINTS", "127.0.0.1:2379")
            .env("CHRONOS_ETCD_PREFIX", "/chronos-effective-config-test")
            .env("CHRONOS_WORKER_ID", "worker-a")
            .env("CHRONOS_INSTANCE_ID", "instance-a")
            .env("CHRONOS_BIND_ADDR", "127.0.0.1:50051")
            .env("CHRONOS_ADVERTISE_ENDPOINT", "127.0.0.1:50051")
            .env("CHRONOS_METRICS_BIND_ADDR", "127.0.0.1:9090")
            .env("CHRONOS_LEASE_TTL_MS", configured_ms)
            .env("CHRONOS_GENERATOR_MAINTENANCE_INTERVAL_MS", "100")
            .env("CHRONOS_SAFETY_GAP_MS", "100")
            .env("CHRONOS_MAX_CLOCK_SKEW_MS", "100")
            .output()
            .expect("effective config command should run");

        assert!(
            output.status.success(),
            "effective config failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("output should be UTF-8");
        assert!(stdout.contains(&format!(
            "identity_lease_ttl_configured_ms={configured_ms}\n"
        )));
        assert!(stdout.contains(&format!(
            "identity_lease_grant_request_ttl_seconds={request_seconds}\n"
        )));
        assert!(stdout.contains(&format!(
            "identity_lease_grant_request_ttl_ms={request_ms}\n"
        )));
        assert!(!stdout.contains("identity_lease_granted_ttl"));
    }
}
