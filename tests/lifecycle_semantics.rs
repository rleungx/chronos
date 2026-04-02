use std::sync::Arc;

use tokio::time::{timeout, Duration};

use chronos::metadata::MemoryMetadataStore;
use chronos::{ManualClock, TsoConfig, TsoError, TsoSecurityMode, TsoService};

fn required_test_config(config: TsoConfig) -> TsoConfig {
    TsoConfig {
        security_mode: Some(TsoSecurityMode::Required),
        grpc_tls_cert_file: Some("server.crt".into()),
        grpc_tls_key_file: Some("server.key".into()),
        grpc_client_ca_file: Some("ca.pem".into()),
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..config
    }
}

#[tokio::test]
async fn service_shutdown_is_idempotent() {
    let clock = Arc::new(ManualClock::new(10_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service =
        TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();

    service.ensure_timeline("shutdown.timeline").await.unwrap();

    timeout(Duration::from_secs(1), service.shutdown())
        .await
        .expect("first shutdown should complete");
    timeout(Duration::from_secs(1), service.shutdown())
        .await
        .expect("second shutdown should complete");
}

#[tokio::test]
async fn service_new_rejects_invalid_advertise_endpoint_format() {
    let clock = Arc::new(ManualClock::new(10_000));
    let metadata = Arc::new(MemoryMetadataStore::new());
    let config = TsoConfig {
        advertise_endpoint: "endpoint-a".into(),
        ..TsoConfig::default()
    };

    let error = match TsoService::new(config, clock, metadata) {
        Ok(_) => panic!("invalid advertise endpoint should be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, TsoError::Internal(message) if message.contains("host:port")));
}
