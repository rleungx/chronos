use chronos::metadata::EtcdMetadataStore;
use chronos::{TsoConfig, TsoSecurityMode};

pub async fn test_etcd_store(prefix: impl Into<String>) -> EtcdMetadataStore {
    let endpoints = crate::common_etcd_endpoints::test_etcd_endpoints();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: endpoints,
        security_mode: Some(TsoSecurityMode::DevInsecure),
        worker_id: "integration-etcd-store".into(),
        advertise_endpoint: "127.0.0.1:50051".into(),
        safety_gap_ms: 500,
        max_clock_skew_ms: 500,
        ..TsoConfig::default()
    };

    EtcdMetadataStore::from_config(&config, prefix.into())
        .await
        .expect("etcd store should start")
}
