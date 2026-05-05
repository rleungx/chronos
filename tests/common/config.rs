use chronos::{TsoConfig, TsoSecurityMode};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::sync::OnceLock;

pub fn required_test_config(config: TsoConfig) -> TsoConfig {
    let (cert_path, key_path, ca_path) = readable_test_tls_paths();
    TsoConfig {
        security_mode: Some(TsoSecurityMode::Required),
        grpc_tls_cert_file: Some(cert_path.to_string()),
        grpc_tls_key_file: Some(key_path.to_string()),
        grpc_client_ca_file: Some(ca_path.to_string()),
        grpc_control_cert_allowlist: vec![
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
        ],
        grpc_route_cert_allowlist: vec![
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
        ],
        grpc_timestamp_cert_allowlist: vec![
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
        ],
        grpc_status_cert_allowlist: vec![
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
        ],
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..config
    }
}

fn readable_test_tls_paths() -> (&'static str, &'static str, &'static str) {
    static PATHS: OnceLock<(String, String, String)> = OnceLock::new();
    let (cert, key, ca) = PATHS.get_or_init(|| {
        let dir = std::env::temp_dir().join("chronos-tests-common-tls");
        std::fs::create_dir_all(&dir).unwrap();
        let cert = dir.join("server.crt");
        let key = dir.join("server.key");
        let ca = dir.join("ca.pem");
        std::fs::write(&cert, b"tests-common-cert").unwrap();
        std::fs::write(&key, b"tests-common-key").unwrap();
        std::fs::write(&ca, b"tests-common-ca").unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
        (
            cert.to_string_lossy().into_owned(),
            key.to_string_lossy().into_owned(),
            ca.to_string_lossy().into_owned(),
        )
    });
    (cert.as_str(), key.as_str(), ca.as_str())
}
