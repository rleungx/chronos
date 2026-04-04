use chronos::{TsoConfig, TsoSecurityMode};

pub fn required_test_config(config: TsoConfig) -> TsoConfig {
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
