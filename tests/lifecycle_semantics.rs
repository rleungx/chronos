#[path = "common/config.rs"]
mod common_config;

use std::sync::Arc;

use tokio::time::{timeout, Duration};

use chronos::metadata::MemoryMetadataStore;
use chronos::{ManualClock, TsoConfig, TsoError, TsoService};
use common_config::required_test_config;

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
