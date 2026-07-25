use std::process::Command;

fn chronos_bin() -> &'static str {
    env!("CARGO_BIN_EXE_chronos")
}

fn config_output(command: &str, timing_env: &[(&str, &str)]) -> String {
    let mut process = Command::new(chronos_bin());
    process
        .arg(command)
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
        .env("CHRONOS_SAFETY_GAP_MS", "100")
        .env("CHRONOS_MAX_CLOCK_SKEW_MS", "100");
    for (key, value) in timing_env {
        process.env(key, value);
    }
    let output = process.output().expect("config command should run");

    assert!(
        output.status.success(),
        "config command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("output should be UTF-8")
}

#[test]
fn effective_config_distinguishes_configured_and_grant_request_identity_ttl() {
    for (configured_ms, request_seconds, request_ms) in [
        ("500", "1", "1000"),
        ("1500", "2", "2000"),
        ("3000", "3", "3000"),
    ] {
        let stdout = config_output(
            "--print-effective-config",
            &[
                ("CHRONOS_LEASE_TTL_MS", configured_ms),
                ("CHRONOS_GENERATOR_MAINTENANCE_INTERVAL_MS", "100"),
            ],
        );
        assert!(stdout.contains(&format!(
            "identity_lease_ttl_configured_ms={configured_ms}\n"
        )));
        assert!(stdout.contains(&format!(
            "identity_lease_grant_request_ttl_seconds={request_seconds}\n"
        )));
        assert!(stdout.contains(&format!(
            "identity_lease_grant_request_ttl_ms={request_ms}\n"
        )));
        assert!(stdout.contains("generator_lease_ttl_configured_ms=0\n"));
        assert!(stdout.contains(&format!(
            "generator_lease_ttl_effective_ms={configured_ms}\n"
        )));
        assert!(stdout.contains("generator_maintenance_interval_configured_ms=100\n"));
        assert!(stdout.contains("generator_maintenance_interval_effective_ms=50\n"));
        assert!(stdout.contains("generator_issued_horizon_ms=100\n"));
        assert!(stdout.contains("generator_maintenance_cadence_reason=issued_horizon_cap\n"));
        assert!(!stdout.contains("identity_lease_granted_ttl"));
    }
}

#[test]
fn effective_and_check_config_report_generator_cadence_bounds() {
    for (command, configured, horizon, generator_ttl, effective, reason) in [
        (
            "--print-effective-config",
            "25",
            "100",
            "0",
            "25",
            "configured",
        ),
        (
            "--check-config",
            "100",
            "1000",
            "150",
            "75",
            "lease_ttl_cap",
        ),
        (
            "--print-effective-config",
            "100",
            "150",
            "150",
            "75",
            "issued_horizon_and_lease_ttl_cap",
        ),
    ] {
        let stdout = config_output(
            command,
            &[
                ("CHRONOS_GENERATOR_MAINTENANCE_INTERVAL_MS", configured),
                ("CHRONOS_PRE_BORROW_MS", horizon),
                ("CHRONOS_GENERATOR_LEASE_TTL_MS", generator_ttl),
            ],
        );
        assert!(stdout.contains(&format!(
            "generator_maintenance_interval_configured_ms={configured}\n"
        )));
        assert!(stdout.contains(&format!(
            "generator_maintenance_interval_effective_ms={effective}\n"
        )));
        assert!(stdout.contains(&format!("generator_issued_horizon_ms={horizon}\n")));
        assert!(stdout.contains(&format!("generator_maintenance_cadence_reason={reason}\n")));
    }
}
