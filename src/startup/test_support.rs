use std::env;

use chronos::TsoConfig;

use super::config::LoadedStartupConfig;

pub(crate) static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
pub(crate) static STARTUP_READY_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(crate) fn clear_tso_env() {
    for key in [
        "CHRONOS_METADATA",
        "CHRONOS_ETCD_ENDPOINTS",
        "CHRONOS_ETCD_PREFIX",
        "CHRONOS_PROFILE",
        "CHRONOS_SECURITY_MODE",
        "CHRONOS_BIND_ADDR",
        "CHRONOS_METRICS_BIND_ADDR",
        "CHRONOS_ROUTE_CACHE_TTL_MS",
        "CHRONOS_DEFAULT_RESOURCE_TIER",
        "CHRONOS_WORKER_ID",
        "CHRONOS_INSTANCE_ID",
        "CHRONOS_ADVERTISE_ENDPOINT",
        "CHRONOS_GRPC_TLS_CERT_FILE",
        "CHRONOS_GRPC_TLS_KEY_FILE",
        "CHRONOS_GRPC_CLIENT_CA_FILE",
        "CHRONOS_GRPC_REQUEST_TIMEOUT_MS",
        "CHRONOS_GRPC_MAX_REQUEST_BYTES",
        "CHRONOS_GRPC_MAX_CONCURRENT_REQUESTS",
        "CHRONOS_METRICS_TLS_CERT_FILE",
        "CHRONOS_METRICS_TLS_KEY_FILE",
        "CHRONOS_METRICS_CLIENT_CA_FILE",
        "CHRONOS_ETCD_CA_FILE",
        "CHRONOS_ETCD_CERT_FILE",
        "CHRONOS_ETCD_KEY_FILE",
        "CHRONOS_ETCD_TIMEOUT_MS",
        "CHRONOS_SAFETY_GAP_MS",
        "CHRONOS_GENERATOR_OWNERSHIP",
    ] {
        unsafe { env::remove_var(key) };
    }
}

pub(crate) fn memory_startup_config(config: TsoConfig) -> LoadedStartupConfig {
    LoadedStartupConfig::memory(config)
}

pub(crate) fn etcd_startup_config(
    config: TsoConfig,
    prefix: impl Into<String>,
) -> LoadedStartupConfig {
    LoadedStartupConfig::etcd(config, prefix)
}

pub(crate) fn test_etcd_endpoints() -> String {
    env::var("CHRONOS_TEST_ETCD_ENDPOINTS").unwrap_or_else(|_| "127.0.0.1:2379".into())
}

pub(crate) fn unique_test_etcd_prefix(label: &str) -> String {
    format!(
        "/chronos-test-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
