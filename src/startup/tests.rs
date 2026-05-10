use super::runtime::CriticalServerContext;
use super::serving_gate::{CriticalStartupListener, StartupServingGate};
use super::test_support::{
    clear_tso_env, etcd_startup_config, memory_startup_config, test_etcd_endpoints,
    unique_test_etcd_prefix, ENV_LOCK, STARTUP_READY_LOCK,
};
use super::*;
use chronos::metrics;
use chronos::proto::v1::{
    timeline_control_service_client::TimelineControlServiceClient,
    timeline_control_service_server::TimelineControlServiceServer,
    timeline_route_service_client::TimelineRouteServiceClient,
    timeline_route_service_server::TimelineRouteServiceServer,
    timeline_status_service_client::TimelineStatusServiceClient,
    timeline_status_service_server::TimelineStatusServiceServer,
    timestamp_service_client::TimestampServiceClient,
    timestamp_service_server::TimestampServiceServer,
};
use chronos::rpc::{
    HealthStatusHandle, TsoControlService, TsoRouteService, TsoTimelineStatusService,
    TsoTimestampService,
};
use chronos::{ManualClock, SystemClock, TsoError};
use futures::FutureExt;
use http_body_util::Empty;
use std::env;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::{MutexGuard, Once, OnceLock};
use std::time::Duration;
use tokio::sync::watch;

use chronos::metadata::{EtcdMetadataStore, MemoryMetadataStore};
use chronos::proto::v1::{
    AllocateTimestampsRequest as ProtoAllocateTimestampsRequest,
    EnsureTimelineRequest as ProtoEnsureTimelineRequest, WorkerReadinessReason,
    WorkerReadinessState,
};
use chronos::{TsoConfig, TsoSecurityMode, TsoService};
use hyper::body::Bytes;
use hyper::client::conn::http1;
use hyper::{Request as HyperRequest, StatusCode};
use hyper_util::rt::TokioIo;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use rustls::pki_types::ServerName;
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, ServerConfig};
use sha2::{Digest, Sha256};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

fn parsed_test_etcd_endpoints() -> Vec<String> {
    test_etcd_endpoints()
        .split(',')
        .map(|endpoint| endpoint.trim().to_string())
        .filter(|endpoint| !endpoint.is_empty())
        .collect()
}

fn install_test_crypto_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn startup_ready_guard() -> MutexGuard<'static, ()> {
    install_test_crypto_provider();
    STARTUP_READY_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

fn explicit_required_config() -> TsoConfig {
    let fixture = shared_test_tls_fixture();
    TsoConfig {
        security_mode: Some(TsoSecurityMode::Required),
        safety_gap_ms: 1,
        grpc_tls_cert_file: Some(fixture.server_cert_path.clone()),
        grpc_tls_key_file: Some(fixture.server_key_path.clone()),
        grpc_client_ca_file: Some(fixture.ca_cert_path.clone()),
        grpc_control_cert_allowlist: vec![test_cert_fingerprint(&fixture.client_cert_path)],
        grpc_route_cert_allowlist: vec![test_cert_fingerprint(&fixture.client_cert_path)],
        grpc_timestamp_cert_allowlist: vec![test_cert_fingerprint(&fixture.client_cert_path)],
        grpc_status_cert_allowlist: vec![test_cert_fingerprint(&fixture.client_cert_path)],
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    }
}

fn shared_test_tls_fixture() -> &'static MetricsTlsFixture {
    static FIXTURE: OnceLock<MetricsTlsFixture> = OnceLock::new();
    FIXTURE.get_or_init(build_metrics_tls_fixture)
}

fn explicit_dev_insecure_local_config(bind_addr: SocketAddr) -> TsoConfig {
    TsoConfig {
        security_mode: Some(TsoSecurityMode::DevInsecure),
        bind_addr: bind_addr.to_string(),
        advertise_endpoint: bind_addr.to_string(),
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(128),
        grpc_max_concurrent_requests: Some(4),
        ..TsoConfig::default()
    }
}

fn test_health_status_handle() -> HealthStatusHandle {
    HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    })
}

fn test_shutdown_identity() -> super::runtime::ShutdownIdentity<'static> {
    super::runtime::ShutdownIdentity {
        worker_id: "worker-a",
        instance_id: "instance-a",
        advertise_endpoint: "endpoint-a:50051",
    }
}

fn shutdown_watch_pair() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

fn test_shutdown_context(
    ready: Arc<AtomicBool>,
    health_status: HealthStatusHandle,
    shutdown_tx: watch::Sender<bool>,
) -> super::runtime::ShutdownContext {
    super::runtime::ShutdownContext {
        ready,
        health_status,
        shutdown_tx,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a".into(),
    }
}

struct MetricsTlsFixture {
    _dir: PathBuf,
    ca_cert_path: String,
    server_cert_path: String,
    server_key_path: String,
    client_cert_path: String,
    client_key_path: String,
    unauthorized_client_cert_path: String,
    unauthorized_client_key_path: String,
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let dir = env::temp_dir().join(format!(
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

fn build_metrics_tls_fixture() -> MetricsTlsFixture {
    install_test_crypto_provider();
    let dir = unique_temp_dir("metrics-tls");

    let mut ca_params = CertificateParams::new(vec!["chronos-metrics-ca".into()]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let mut server_params =
        CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    server_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let server_cert = server_params.signed_by(&server_key, &ca).unwrap();

    let client_key = KeyPair::generate().unwrap();
    let mut client_params = CertificateParams::new(vec!["chronos-metrics-client".into()]).unwrap();
    client_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    let client_cert = client_params.signed_by(&client_key, &ca).unwrap();

    let unauthorized_client_key = KeyPair::generate().unwrap();
    let mut unauthorized_client_params =
        CertificateParams::new(vec!["chronos-unauthorized-client".into()]).unwrap();
    unauthorized_client_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    let unauthorized_client_cert = unauthorized_client_params
        .signed_by(&unauthorized_client_key, &ca)
        .unwrap();

    let ca_cert_path = dir.join("ca.pem");
    let server_cert_path = dir.join("server.pem");
    let server_key_path = dir.join("server-key.pem");
    let client_cert_path = dir.join("client.pem");
    let client_key_path = dir.join("client-key.pem");
    let unauthorized_client_cert_path = dir.join("unauthorized-client.pem");
    let unauthorized_client_key_path = dir.join("unauthorized-client-key.pem");

    std::fs::write(&ca_cert_path, ca.pem()).unwrap();
    std::fs::write(&server_cert_path, server_cert.pem()).unwrap();
    std::fs::write(&server_key_path, server_key.serialize_pem()).unwrap();
    std::fs::write(&client_cert_path, client_cert.pem()).unwrap();
    std::fs::write(&client_key_path, client_key.serialize_pem()).unwrap();
    std::fs::write(
        &unauthorized_client_cert_path,
        unauthorized_client_cert.pem(),
    )
    .unwrap();
    std::fs::write(
        &unauthorized_client_key_path,
        unauthorized_client_key.serialize_pem(),
    )
    .unwrap();
    #[cfg(unix)]
    {
        std::fs::set_permissions(&server_key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(&client_key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::set_permissions(
            &unauthorized_client_key_path,
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }

    MetricsTlsFixture {
        _dir: dir,
        ca_cert_path: ca_cert_path.to_string_lossy().into_owned(),
        server_cert_path: server_cert_path.to_string_lossy().into_owned(),
        server_key_path: server_key_path.to_string_lossy().into_owned(),
        client_cert_path: client_cert_path.to_string_lossy().into_owned(),
        client_key_path: client_key_path.to_string_lossy().into_owned(),
        unauthorized_client_cert_path: unauthorized_client_cert_path.to_string_lossy().into_owned(),
        unauthorized_client_key_path: unauthorized_client_key_path.to_string_lossy().into_owned(),
    }
}

fn metrics_tls_config(fixture: &MetricsTlsFixture) -> TsoConfig {
    TsoConfig {
        security_mode: Some(TsoSecurityMode::DevInsecure),
        metrics_tls_cert_file: Some(fixture.server_cert_path.clone()),
        metrics_tls_key_file: Some(fixture.server_key_path.clone()),
        metrics_client_ca_file: Some(fixture.ca_cert_path.clone()),
        ..TsoConfig::default()
    }
}

fn build_metrics_tls_connector(
    fixture: &MetricsTlsFixture,
    include_client_cert: bool,
) -> TlsConnector {
    install_test_crypto_provider();
    let ca_cert = std::fs::read(&fixture.ca_cert_path).unwrap();
    let roots = load_root_cert_store(&ca_cert, "test metrics client ca").expect("load test CA");
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let client_config = if include_client_cert {
        let client_cert = std::fs::read(&fixture.client_cert_path).unwrap();
        let client_key = std::fs::read(&fixture.client_key_path).unwrap();
        builder
            .with_client_auth_cert(
                parse_pem_certificates(&client_cert, "test metrics client cert").unwrap(),
                parse_pem_private_key(&client_key, "test metrics client key").unwrap(),
            )
            .unwrap()
    } else {
        builder.with_no_client_auth()
    };
    TlsConnector::from(Arc::new(client_config))
}

fn test_cert_fingerprint(cert_path: &str) -> String {
    let cert_pem = std::fs::read(cert_path).unwrap();
    let certs = parse_pem_certificates(&cert_pem, "test client cert fingerprint").unwrap();
    let digest = Sha256::digest(certs.first().unwrap().as_ref());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

async fn connect_control_client(
    endpoint: SocketAddr,
    fixture: &MetricsTlsFixture,
    cert_path: &str,
    key_path: &str,
) -> TimelineControlServiceClient<tonic::transport::Channel> {
    let cert_pem = std::fs::read(cert_path).unwrap();
    let key_pem = std::fs::read(key_path).unwrap();
    let ca_pem = std::fs::read(&fixture.ca_cert_path).unwrap();
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .identity(Identity::from_pem(cert_pem, key_pem))
        .domain_name("localhost");
    let endpoint = Endpoint::from_shared(format!("https://{endpoint}"))
        .unwrap()
        .tls_config(tls)
        .unwrap();
    TimelineControlServiceClient::connect(endpoint)
        .await
        .unwrap()
}

async fn connect_route_client(
    endpoint: SocketAddr,
    fixture: &MetricsTlsFixture,
    cert_path: &str,
    key_path: &str,
) -> TimelineRouteServiceClient<tonic::transport::Channel> {
    let cert_pem = std::fs::read(cert_path).unwrap();
    let key_pem = std::fs::read(key_path).unwrap();
    let ca_pem = std::fs::read(&fixture.ca_cert_path).unwrap();
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .identity(Identity::from_pem(cert_pem, key_pem))
        .domain_name("localhost");
    let endpoint = Endpoint::from_shared(format!("https://{endpoint}"))
        .unwrap()
        .tls_config(tls)
        .unwrap();
    TimelineRouteServiceClient::connect(endpoint).await.unwrap()
}

async fn connect_timestamp_client(
    endpoint: SocketAddr,
    fixture: &MetricsTlsFixture,
    cert_path: &str,
    key_path: &str,
) -> TimestampServiceClient<tonic::transport::Channel> {
    let cert_pem = std::fs::read(cert_path).unwrap();
    let key_pem = std::fs::read(key_path).unwrap();
    let ca_pem = std::fs::read(&fixture.ca_cert_path).unwrap();
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .identity(Identity::from_pem(cert_pem, key_pem))
        .domain_name("localhost");
    let endpoint = Endpoint::from_shared(format!("https://{endpoint}"))
        .unwrap()
        .tls_config(tls)
        .unwrap();
    TimestampServiceClient::connect(endpoint).await.unwrap()
}

async fn connect_status_client(
    endpoint: SocketAddr,
    fixture: &MetricsTlsFixture,
    cert_path: &str,
    key_path: &str,
) -> TimelineStatusServiceClient<tonic::transport::Channel> {
    let cert_pem = std::fs::read(cert_path).unwrap();
    let key_pem = std::fs::read(key_path).unwrap();
    let ca_pem = std::fs::read(&fixture.ca_cert_path).unwrap();
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .identity(Identity::from_pem(cert_pem, key_pem))
        .domain_name("localhost");
    let endpoint = Endpoint::from_shared(format!("https://{endpoint}"))
        .unwrap()
        .tls_config(tls)
        .unwrap();
    TimelineStatusServiceClient::connect(endpoint)
        .await
        .unwrap()
}

fn build_test_mtls_acceptor(fixture: &MetricsTlsFixture) -> TlsAcceptor {
    install_test_crypto_provider();
    let ca_cert = std::fs::read(&fixture.ca_cert_path).unwrap();
    let server_cert = std::fs::read(&fixture.server_cert_path).unwrap();
    let server_key = std::fs::read(&fixture.server_key_path).unwrap();
    let mut tls_config = ServerConfig::builder()
        .with_client_cert_verifier(
            WebPkiClientVerifier::builder(
                load_root_cert_store(&ca_cert, "test etcd server ca")
                    .unwrap()
                    .into(),
            )
            .build()
            .unwrap(),
        )
        .with_single_cert(
            parse_pem_certificates(&server_cert, "test etcd server cert").unwrap(),
            parse_pem_private_key(&server_key, "test etcd server key").unwrap(),
        )
        .unwrap();
    tls_config.alpn_protocols = vec![b"h2".to_vec()];
    TlsAcceptor::from(Arc::new(tls_config))
}

#[test]
fn startup_preflight_allows_derived_instance_id() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = explicit_required_config();
    validate_startup_preflight(&memory_startup_config(config.clone())).unwrap();
    assert_eq!(config.effective_instance_id(), "default-endpoint:50051");
}

#[test]
fn startup_preflight_requires_etcd_endpoints_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_ETCD_ENDPOINTS"));
}

#[test]
fn production_profile_requires_auditable_build_commit() {
    let mut config = explicit_required_config();
    config.production_profile = true;

    let error = validate_build_identity_for_profile(
        &config,
        chronos::BuildIdentity {
            version: chronos::build_version(),
            commit: "unknown",
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("auditable build commit"));
}

#[test]
fn production_profile_accepts_known_build_commit() {
    let mut config = explicit_required_config();
    config.production_profile = true;

    validate_build_identity_for_profile(
        &config,
        chronos::BuildIdentity {
            version: chronos::build_version(),
            commit: "deadbeef",
        },
    )
    .unwrap();
}

#[test]
fn startup_preflight_rejects_production_profile_with_memory_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();

    let startup = memory_startup_config(TsoConfig {
        production_profile: true,
        ..explicit_required_config()
    });

    let error = validate_startup_preflight(&startup).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_METADATA=etcd"));
}

#[test]
fn startup_preflight_accepts_production_profile_with_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();

    let startup = etcd_startup_config(
        TsoConfig {
            production_profile: true,
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            advertise_endpoint: "10.0.0.10:50051".into(),
            etcd_endpoints: vec!["127.0.0.1:2379".into()],
            ..explicit_required_config()
        },
        "/chronos",
    );

    validate_startup_preflight(&startup).unwrap();
}

#[test]
fn non_production_profile_allows_unknown_build_commit() {
    let config = explicit_required_config();

    validate_build_identity_for_profile(
        &config,
        chronos::BuildIdentity {
            version: chronos::build_version(),
            commit: "unknown",
        },
    )
    .unwrap();
}

#[test]
fn startup_preflight_does_not_backfill_etcd_endpoints_from_env() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    unsafe { env::set_var("CHRONOS_ETCD_ENDPOINTS", "127.0.0.1:2379") };
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_ETCD_ENDPOINTS"));
}

#[test]
fn startup_preflight_requires_explicit_worker_id_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        advertise_endpoint: "127.0.0.1:50051".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_WORKER_ID"));
}

#[test]
fn startup_preflight_requires_explicit_advertise_endpoint_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_ADVERTISE_ENDPOINT"));
}

#[test]
fn startup_preflight_requires_nonzero_safety_gap_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "10.0.0.10:50051".into(),
        safety_gap_ms: 0,
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_SAFETY_GAP_MS"));
}

#[test]
fn startup_preflight_rejects_localhost_advertise_endpoint_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "localhost:50051".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("localhost"));
}

#[test]
fn startup_preflight_rejects_loopback_advertise_endpoint_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();

    for advertise_endpoint in ["127.0.0.1:50051", "[::1]:50051"] {
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            etcd_endpoints: vec!["127.0.0.1:2379".into()],
            worker_id: "worker-a".into(),
            advertise_endpoint: advertise_endpoint.into(),
            ..explicit_required_config()
        };
        let error =
            validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
        assert!(error.to_string().contains("loopback IP"));
    }
}

#[test]
fn startup_preflight_rejects_wildcard_advertise_endpoint_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "0.0.0.0:50051".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("wildcard"));
}

#[test]
fn startup_preflight_rejects_invalid_etcd_prefix() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "127.0.0.1:50051".into(),
        ..explicit_required_config()
    };
    let error =
        validate_startup_preflight(&etcd_startup_config(config, "relative-prefix")).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_ETCD_PREFIX"));
}

#[test]
fn startup_preflight_accepts_explicit_routable_advertise_endpoint_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "10.0.0.10:50051".into(),
        ..explicit_required_config()
    };
    validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap();
}

#[test]
fn startup_preflight_rejects_localhost_subdomain_advertise_endpoint_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "chronos-soak.localhost:50051".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains(".localhost"));
}

#[test]
fn startup_preflight_accepts_loopback_advertise_endpoint_for_dev_insecure_local_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let bind_addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        safety_gap_ms: 1,
        ..explicit_dev_insecure_local_config(bind_addr)
    };
    validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap();
}

#[test]
fn startup_preflight_rejects_invalid_advertise_endpoint_format_for_etcd_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        etcd_endpoints: vec!["127.0.0.1:2379".into()],
        worker_id: "worker-a".into(),
        advertise_endpoint: "endpoint-a".into(),
        ..explicit_required_config()
    };
    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error.to_string().contains("host:port"));
}

#[test]
fn startup_preflight_rejects_http_etcd_endpoints_when_tls_is_configured() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let fixture = shared_test_tls_fixture();

    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        advertise_endpoint: "10.0.0.10:50051".into(),
        etcd_endpoints: vec!["http://10.0.0.20:2379".into()],
        etcd_ca_file: Some(fixture.ca_cert_path.clone()),
        etcd_cert_file: Some(fixture.client_cert_path.clone()),
        etcd_key_file: Some(fixture.client_key_path.clone()),
        etcd_timeout_ms: Some(100),
        ..explicit_required_config()
    };

    let error = validate_startup_preflight(&etcd_startup_config(config, "/chronos")).unwrap_err();
    assert!(error
        .to_string()
        .contains("https:// or bare host:port when etcd TLS is configured"));
}

#[test]
fn startup_preflight_rejects_unset_security_mode_for_local_only_memory_topology() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        advertise_endpoint: "127.0.0.1:50051".into(),
        bind_addr: "127.0.0.1:50051".into(),
        metrics_bind_addr: "127.0.0.1:9898".into(),
        ..TsoConfig::default()
    };
    let error = validate_startup_preflight(&memory_startup_config(config)).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_SECURITY_MODE"));
}

#[test]
fn startup_preflight_rejects_required_remote_exposed_without_grpc_tls_bundle() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let config = TsoConfig {
        bind_addr: "0.0.0.0:50052".into(),
        advertise_endpoint: "10.0.0.10:50052".into(),
        security_mode: Some(TsoSecurityMode::Required),
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    };
    let error = validate_startup_preflight(&memory_startup_config(config)).unwrap_err();
    assert!(error
        .to_string()
        .contains("gRPC TLS requires a complete bundle"));
}

#[test]
fn startup_preflight_requires_control_and_status_allowlists_when_grpc_mtls_is_configured() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let fixture = shared_test_tls_fixture();
    let config = TsoConfig {
        security_mode: Some(TsoSecurityMode::DevInsecure),
        bind_addr: "127.0.0.1:50052".into(),
        advertise_endpoint: "127.0.0.1:50052".into(),
        grpc_tls_cert_file: Some(fixture.server_cert_path.clone()),
        grpc_tls_key_file: Some(fixture.server_key_path.clone()),
        grpc_client_ca_file: Some(fixture.ca_cert_path.clone()),
        grpc_request_timeout_ms: Some(100),
        grpc_max_request_bytes: Some(1024),
        grpc_max_concurrent_requests: Some(16),
        ..TsoConfig::default()
    };

    let error = validate_startup_preflight(&memory_startup_config(config)).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("CHRONOS_GRPC_CONTROL_CERT_ALLOWLIST"));
}

#[test]
fn build_grpc_server_rejects_unreadable_tls_files() {
    let error = build_grpc_server(&TsoConfig {
        grpc_tls_cert_file: Some("/definitely/missing/server.crt".into()),
        grpc_tls_key_file: Some("/definitely/missing/server.key".into()),
        grpc_client_ca_file: Some("/definitely/missing/ca.pem".into()),
        ..explicit_required_config()
    })
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("CHRONOS_GRPC_TLS_CERT_FILE could not read"));
}

#[tokio::test]
async fn etcd_metadata_from_config_rejects_unreadable_tls_files() {
    let error = match EtcdMetadataStore::from_config(
        &TsoConfig {
            etcd_endpoints: vec!["https://localhost:2379".into()],
            etcd_ca_file: Some("/definitely/missing/ca.pem".into()),
            etcd_cert_file: Some("/definitely/missing/client.pem".into()),
            etcd_key_file: Some("/definitely/missing/client-key.pem".into()),
            etcd_timeout_ms: Some(100),
            ..explicit_required_config()
        },
        "/chronos-test-unreadable-etcd-tls".into(),
    )
    .await
    {
        Ok(_) => panic!("expected unreadable etcd TLS files to fail"),
        Err(error) => error,
    };

    assert!(error
        .to_string()
        .contains("CHRONOS_ETCD_CA_FILE could not read"));
}

#[tokio::test]
async fn build_tso_service_uses_typed_etcd_endpoints_for_bootstrap() {
    let startup = super::config::LoadedStartupConfig::new(
        TsoConfig {
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            advertise_endpoint: "10.0.0.10:50051".into(),
            etcd_endpoints: vec!["http://127.0.0.1:2379".into()],
            etcd_ca_file: Some("/definitely/missing/ca.pem".into()),
            etcd_cert_file: Some("/definitely/missing/client.pem".into()),
            etcd_key_file: Some("/definitely/missing/client-key.pem".into()),
            etcd_timeout_ms: Some(100),
            ..explicit_required_config()
        },
        super::config::StartupMetadata::Etcd(super::config::EtcdStartupConfig::new(
            vec!["https://localhost:2379".into()],
            "/chronos-test-typed-bootstrap",
        )),
        super::config::StartupLoggingConfig::default(),
    );

    let error = match build_tso_service(&startup, Arc::new(SystemClock)).await {
        Ok(_) => panic!("expected unreadable etcd TLS files to fail"),
        Err(error) => error,
    };

    assert!(error
        .to_string()
        .contains("CHRONOS_ETCD_CA_FILE could not read"));
    assert!(!error.to_string().contains(
            "CHRONOS_ETCD_ENDPOINTS entries must use https:// or bare host:port when etcd TLS is configured"
        ));
}

#[test]
fn load_metrics_tls_acceptor_rejects_unreadable_tls_files() {
    let error = match load_metrics_tls_acceptor(&TsoConfig {
        metrics_tls_cert_file: Some("/definitely/missing/server.crt".into()),
        metrics_tls_key_file: Some("/definitely/missing/server.key".into()),
        metrics_client_ca_file: Some("/definitely/missing/ca.pem".into()),
        ..explicit_required_config()
    }) {
        Ok(_) => panic!("expected unreadable metrics TLS files to fail"),
        Err(error) => error,
    };

    assert!(error
        .to_string()
        .contains("CHRONOS_METRICS_TLS_CERT_FILE could not read"));
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn serve_metrics_accepts_https_with_client_certificate() {
    let _guard = startup_ready_guard();
    let fixture = build_metrics_tls_fixture();
    let config = metrics_tls_config(&fixture);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tls_acceptor = load_metrics_tls_acceptor(&config).unwrap();
    let ready = Arc::new(AtomicBool::new(true));
    let ready_for_reset = ready.clone();
    set_startup_ready(&ready, true);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let server_handle = tokio::spawn(async move {
        serve_metrics_listener(listener, tls_acceptor, ready, shutdown_rx)
            .await
            .map_err(|error| error.to_string())
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let connector = build_metrics_tls_connector(&fixture, true);
    let stream = TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("localhost").unwrap().to_owned();
    let tls_stream = connector.connect(server_name, stream).await.unwrap();
    let (mut sender, connection) = http1::handshake(TokioIo::new(tls_stream)).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let response = sender
        .send_request(
            HyperRequest::builder()
                .uri("/readyz")
                .body(Empty::<Bytes>::new())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    shutdown_tx.send(true).unwrap();
    server_handle.await.unwrap().unwrap();
    set_startup_ready(&ready_for_reset, false);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn serve_metrics_rejects_https_without_client_certificate() {
    let _guard = startup_ready_guard();
    let fixture = build_metrics_tls_fixture();
    let config = metrics_tls_config(&fixture);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tls_acceptor = load_metrics_tls_acceptor(&config).unwrap();
    let ready = Arc::new(AtomicBool::new(true));
    let ready_for_reset = ready.clone();
    set_startup_ready(&ready, true);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let server_handle = tokio::spawn(async move {
        serve_metrics_listener(listener, tls_acceptor, ready, shutdown_rx)
            .await
            .map_err(|error| error.to_string())
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let connector = build_metrics_tls_connector(&fixture, false);
    let stream = TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("localhost").unwrap().to_owned();
    let tls_stream = connector.connect(server_name, stream).await.unwrap();
    let (mut sender, connection) = http1::handshake(TokioIo::new(tls_stream)).await.unwrap();
    let connection_handle = tokio::spawn(connection);
    let error = sender
        .send_request(
            HyperRequest::builder()
                .uri("/readyz")
                .body(Empty::<Bytes>::new())
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(!error.to_string().is_empty());
    let _ = connection_handle.await.unwrap();

    shutdown_tx.send(true).unwrap();
    server_handle.await.unwrap().unwrap();
    set_startup_ready(&ready_for_reset, false);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn etcd_metadata_runtime_uses_configured_mtls_and_timeout() {
    let _guard = startup_ready_guard();
    let fixture = build_metrics_tls_fixture();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = build_test_mtls_acceptor(&fixture);
    let handshake_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handshake_count_for_server = handshake_count.clone();
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let server_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    let Ok((socket, _)) = accepted else {
                        break;
                    };
                    let acceptor = acceptor.clone();
                    let handshake_count = handshake_count_for_server.clone();
                    tokio::spawn(async move {
                        if let Ok(_stream) = acceptor.accept(socket).await {
                            handshake_count.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    });
                }
            }
        }
    });

    let config = TsoConfig {
        etcd_endpoints: vec![format!("https://127.0.0.1:{port}")],
        etcd_ca_file: Some(fixture.ca_cert_path.clone()),
        etcd_cert_file: Some(fixture.client_cert_path.clone()),
        etcd_key_file: Some(fixture.client_key_path.clone()),
        etcd_timeout_ms: Some(200),
        ..explicit_required_config()
    };

    let started = std::time::Instant::now();
    let error = match EtcdMetadataStore::from_config(
        &config,
        unique_test_etcd_prefix("secure-timeout-probe"),
    )
    .await
    {
        Ok(_) => panic!("etcd metadata runtime should time out during bootstrap probe"),
        Err(error) => error,
    };
    let elapsed = started.elapsed();
    let error_text = error.to_string();

    assert!(elapsed < Duration::from_secs(3));
    assert!(handshake_count.load(Ordering::Relaxed) > 0);
    assert!(
        error_text.contains("deadline")
            || error_text.contains("Timeout expired")
            || error_text.contains("timeout"),
        "unexpected etcd timeout error: {error_text}",
    );

    shutdown_tx.send(true).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn grpc_runtime_rejects_requests_above_max_request_bytes() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let config = explicit_dev_insecure_local_config(addr);
    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(config.clone(), Arc::new(SystemClock), metadata).unwrap();

    let route_service =
        TimelineRouteServiceServer::new(TsoRouteService::new(service.control_plane()))
            .max_decoding_message_size(config.grpc_max_request_bytes.unwrap());
    let timestamp_service =
        TimestampServiceServer::new(TsoTimestampService::new(service.data_plane()))
            .max_decoding_message_size(config.grpc_max_request_bytes.unwrap());
    let control_service =
        TimelineControlServiceServer::new(TsoControlService::new(service.control_plane()))
            .max_decoding_message_size(config.grpc_max_request_bytes.unwrap());
    let timeline_status_service =
        TimelineStatusServiceServer::new(TsoTimelineStatusService::new(service.control_plane()))
            .max_decoding_message_size(config.grpc_max_request_bytes.unwrap());

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = build_grpc_server(&config)
        .unwrap()
        .add_service(route_service)
        .add_service(timestamp_service)
        .add_service(control_service)
        .add_service(timeline_status_service)
        .serve_with_shutdown(addr, wait_for_shutdown_signal(shutdown_rx));
    let server_handle = tokio::spawn(server);

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = TimestampServiceClient::connect(format!("http://{}", addr))
        .await
        .unwrap();
    let error = client
        .allocate_timestamps(ProtoAllocateTimestampsRequest {
            timeline_key: "grpc.limit.timeline".repeat(16),
            count: 1,
            expected_epoch: 0,
            expected_route_version: 0,
            client_request_id: "grpc-limit-client-request-id".repeat(8),
            request_timeout_ms: 1,
        })
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::OutOfRange);
    assert!(error.message().contains("limit is"));

    shutdown_tx.send(true).unwrap();
    server_handle.await.unwrap().unwrap();
    service.shutdown().await;
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn grpc_runtime_authorizes_all_services_by_peer_certificate() {
    let _guard = startup_ready_guard();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let fixture = shared_test_tls_fixture();
    let mut config = explicit_required_config();
    config.bind_addr = addr.to_string();
    config.advertise_endpoint = addr.to_string();
    config.grpc_control_cert_allowlist = vec![test_cert_fingerprint(&fixture.client_cert_path)];
    config.grpc_route_cert_allowlist = vec![test_cert_fingerprint(&fixture.client_cert_path)];
    config.grpc_timestamp_cert_allowlist = vec![test_cert_fingerprint(&fixture.client_cert_path)];
    config.grpc_status_cert_allowlist = vec![test_cert_fingerprint(&fixture.client_cert_path)];

    let metadata = Arc::new(MemoryMetadataStore::new());
    let service = TsoService::new(config.clone(), Arc::new(SystemClock), metadata).unwrap();
    let route_service = TimelineRouteServiceServer::new(
        TsoRouteService::with_allowlist(service.control_plane(), &config.grpc_route_cert_allowlist)
            .unwrap(),
    )
    .max_decoding_message_size(config.grpc_max_request_bytes.unwrap());
    let timestamp_service = TimestampServiceServer::new(
        TsoTimestampService::with_allowlist(
            service.data_plane(),
            &config.grpc_timestamp_cert_allowlist,
        )
        .unwrap(),
    )
    .max_decoding_message_size(config.grpc_max_request_bytes.unwrap());
    let control_service = TimelineControlServiceServer::new(
        TsoControlService::with_health_status_and_allowlist(
            service.control_plane(),
            test_health_status_handle(),
            &config.grpc_control_cert_allowlist,
        )
        .unwrap(),
    )
    .max_decoding_message_size(config.grpc_max_request_bytes.unwrap());
    let timeline_status_service = TimelineStatusServiceServer::new(
        TsoTimelineStatusService::with_allowlist(
            service.control_plane(),
            &config.grpc_status_cert_allowlist,
        )
        .unwrap(),
    )
    .max_decoding_message_size(config.grpc_max_request_bytes.unwrap());

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = build_grpc_server(&config)
        .unwrap()
        .add_service(route_service)
        .add_service(timestamp_service)
        .add_service(control_service)
        .add_service(timeline_status_service)
        .serve_with_shutdown(addr, wait_for_shutdown_signal(shutdown_rx));
    let server_handle = tokio::spawn(server);

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut authorized_control = connect_control_client(
        addr,
        fixture,
        &fixture.client_cert_path,
        &fixture.client_key_path,
    )
    .await;
    let health = authorized_control.health(()).await.unwrap().into_inner();
    assert_eq!(health.worker_id, "worker-a");

    let mut denied_control = connect_control_client(
        addr,
        fixture,
        &fixture.unauthorized_client_cert_path,
        &fixture.unauthorized_client_key_path,
    )
    .await;
    let error = denied_control.health(()).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);

    let mut authorized_route = connect_route_client(
        addr,
        fixture,
        &fixture.client_cert_path,
        &fixture.client_key_path,
    )
    .await;
    let route = authorized_route
        .ensure_timeline(ProtoEnsureTimelineRequest {
            timeline_key: "grpc.auth.timeline".to_string(),
            desired_resource_tier: chronos::proto::v1::ResourceTier::Shared as i32,
        })
        .await
        .unwrap()
        .into_inner()
        .route
        .expect("authorized route service should create a route");

    let mut denied_route = connect_route_client(
        addr,
        fixture,
        &fixture.unauthorized_client_cert_path,
        &fixture.unauthorized_client_key_path,
    )
    .await;
    let error = denied_route
        .ensure_timeline(ProtoEnsureTimelineRequest {
            timeline_key: "grpc.auth.denied.timeline".to_string(),
            desired_resource_tier: chronos::proto::v1::ResourceTier::Shared as i32,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);

    let mut authorized_timestamp = connect_timestamp_client(
        addr,
        fixture,
        &fixture.client_cert_path,
        &fixture.client_key_path,
    )
    .await;
    let allocation = authorized_timestamp
        .allocate_timestamps(ProtoAllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "grpc-auth-allocate".to_string(),
            request_timeout_ms: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(allocation.timeline_key, route.timeline_key);

    let mut denied_timestamp = connect_timestamp_client(
        addr,
        fixture,
        &fixture.unauthorized_client_cert_path,
        &fixture.unauthorized_client_key_path,
    )
    .await;
    let error = denied_timestamp
        .allocate_timestamps(ProtoAllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "grpc-auth-denied-allocate".to_string(),
            request_timeout_ms: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);

    let mut authorized_status = connect_status_client(
        addr,
        fixture,
        &fixture.client_cert_path,
        &fixture.client_key_path,
    )
    .await;
    let statuses = authorized_status
        .list_timeline_statuses(chronos::proto::v1::ListTimelineStatusesRequest {
            states: Vec::new(),
            owner_worker_endpoint: None,
            page_size: 0,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(statuses.statuses.iter().any(|status| {
        status
            .route
            .as_ref()
            .map(|status_route| status_route.timeline_key == route.timeline_key)
            .unwrap_or(false)
    }));

    let mut denied_status = connect_status_client(
        addr,
        fixture,
        &fixture.unauthorized_client_cert_path,
        &fixture.unauthorized_client_key_path,
    )
    .await;
    let error = denied_status
        .list_timeline_statuses(chronos::proto::v1::ListTimelineStatusesRequest {
            states: Vec::new(),
            owner_worker_endpoint: None,
            page_size: 0,
            page_token: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);

    shutdown_tx.send(true).unwrap();
    server_handle.await.unwrap().unwrap();
    service.shutdown().await;
}

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
    let preflight = include_str!("preflight.rs");
    let bootstrap = include_str!("bootstrap.rs");

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
        super::preflight::MetricsTransport::Mtls
    );
}

#[test]
fn startup_preflight_logger_uses_validated_plan_instead_of_rederiving_state() {
    let preflight = include_str!("preflight.rs");

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
        env::set_var("CHRONOS_GENERATOR_OWNERSHIP_REMAINDER", "2");
    }

    let config = load_tso_config().unwrap();
    assert_eq!(config.ownership_plan_id, "scale-2026-05-10");
    assert_eq!(config.generator_ownership_modulo, 4);
    assert_eq!(config.generator_ownership_remainder, 2);
}

#[test]
fn load_tso_config_rejects_removed_internal_tuning_env_vars() {
    let _guard = ENV_LOCK.lock().unwrap();

    for key in [
        "CHRONOS_ROUTE_CACHE_TTL_MS",
        "CHRONOS_MAX_BATCH_PER_REQUEST",
        "CHRONOS_GENERATOR_OWNERSHIP",
        "CHRONOS_LEASE_TTL_MS",
    ] {
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

#[tokio::test]
async fn metadata_startup_probe_accepts_memory_store() {
    let metadata = MemoryMetadataStore::new();
    run_metadata_startup_probe(&metadata).await.unwrap();
}

#[tokio::test]
async fn readyz_reports_service_unavailable_until_ready() {
    let response = metrics_handler(
        HyperRequest::builder()
            .uri("/readyz")
            .body(Empty::<Bytes>::new())
            .unwrap(),
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn health_handler_does_not_expose_metrics() {
    let response = health_handler(
        HyperRequest::builder()
            .uri("/metrics")
            .body(Empty::<Bytes>::new())
            .unwrap(),
        Arc::new(AtomicBool::new(true)),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn readyz_only_turns_green_after_all_critical_listeners_are_bound() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(false));
    let mut serving_gate = StartupServingGate::default();

    set_startup_ready(&ready, serving_gate.is_ready());
    let response = metrics_handler(
        HyperRequest::builder()
            .uri("/readyz")
            .body(Empty::<Bytes>::new())
            .unwrap(),
        ready.clone(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    set_startup_ready(
        &ready,
        serving_gate.mark_listener_bound(CriticalStartupListener::Admin),
    );
    let response = metrics_handler(
        HyperRequest::builder()
            .uri("/readyz")
            .body(Empty::<Bytes>::new())
            .unwrap(),
        ready.clone(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    set_startup_ready(
        &ready,
        serving_gate.mark_listener_bound(CriticalStartupListener::Grpc),
    );
    let response = metrics_handler(
        HyperRequest::builder()
            .uri("/readyz")
            .body(Empty::<Bytes>::new())
            .unwrap(),
        ready.clone(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    set_startup_ready(&ready, false);
}

#[test]
fn startup_preflight_failure_records_metric() {
    let _guard = ENV_LOCK.lock().unwrap();
    let before = metrics::TSO_STARTUP_PREFLIGHT_FAIL_TOTAL
        .with_label_values(&["config"])
        .get();

    let config = TsoConfig {
        advertise_endpoint: String::new(),
        ..explicit_required_config()
    };
    let _ = validate_startup_preflight(&memory_startup_config(config));
    record_startup_preflight_failure("config");

    let after = metrics::TSO_STARTUP_PREFLIGHT_FAIL_TOTAL
        .with_label_values(&["config"])
        .get();
    assert!(after > before);
}

#[test]
fn startup_failure_stage_classifies_identity_conflict() {
    let error = TsoError::InstanceIdentityInUse {
        instance_id: "instance-dupe".into(),
    };
    assert_eq!(
        startup_failure_stage(&error as &(dyn std::error::Error + 'static)),
        "identity"
    );
}

#[test]
fn startup_preflight_failure_uses_machine_readable_reason() {
    assert_eq!(
        startup_preflight_failure_reason(),
        WorkerReadinessReason::StartupPreflightFailed
    );
}

#[test]
fn startup_bootstrap_failure_maps_identity_conflict_to_machine_readable_reason() {
    let error = TsoError::InstanceIdentityInUse {
        instance_id: "instance-dupe".into(),
    };
    assert_eq!(
        startup_bootstrap_failure_reason(&error as &(dyn std::error::Error + 'static)),
        WorkerReadinessReason::IdentityLeaseAcquireFailed
    );
}

#[test]
fn startup_bootstrap_failure_defaults_to_metadata_probe_machine_readable_reason() {
    let error = TsoError::Internal("metadata probe failed".into());
    assert_eq!(
        startup_bootstrap_failure_reason(&error as &(dyn std::error::Error + 'static)),
        WorkerReadinessReason::MetadataStartupProbeFailed
    );
}

#[test]
fn startup_bootstrap_failure_records_metric() {
    let before = metrics::TSO_STARTUP_BOOTSTRAP_FAIL_TOTAL
        .with_label_values(&["contract"])
        .get();

    record_startup_bootstrap_failure("contract");

    let after = metrics::TSO_STARTUP_BOOTSTRAP_FAIL_TOTAL
        .with_label_values(&["contract"])
        .get();
    assert!(after > before);
}

#[test]
fn process_signal_shutdown_trigger_normalizes_to_shutting_down_reason() {
    let trigger = ShutdownTrigger::ProcessSignal("signal_sigterm");
    assert_eq!(trigger.label(), "signal_sigterm");
    assert_eq!(
        trigger.readiness_reason(),
        WorkerReadinessReason::ShuttingDown
    );
}

#[test]
fn critical_server_failure_shutdown_trigger_normalizes_to_shutting_down_reason() {
    let trigger = ShutdownTrigger::CriticalServerFailed("grpc");
    assert_eq!(trigger.label(), "critical_server_failed_grpc");
    assert_eq!(
        trigger.readiness_reason(),
        WorkerReadinessReason::ShuttingDown
    );
}

#[test]
fn identity_lease_loss_records_shutdown_metric() {
    let before = metrics::TSO_SHUTDOWN_TOTAL
        .with_label_values(&["identity_lease_lost"])
        .get();
    record_shutdown("identity_lease_lost");
    let after = metrics::TSO_SHUTDOWN_TOTAL
        .with_label_values(&["identity_lease_lost"])
        .get();
    assert!(after > before);
}

#[test]
fn request_shutdown_flips_readiness_and_notifies_watchers() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = test_health_status_handle();
    let (shutdown_tx, shutdown_rx) = shutdown_watch_pair();

    let before = metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
        .with_label_values(&["ready", "degraded", "shutting_down"])
        .get();

    request_shutdown(
        &ready,
        &health_status,
        &shutdown_tx,
        test_shutdown_identity(),
        ShutdownTrigger::ProcessSignal("signal_ctrl_c"),
    );

    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
    assert_eq!(
        health_status.readiness_state(),
        WorkerReadinessState::Degraded
    );
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::ShuttingDown
    );
    assert!(health_status.identity_lease_healthy());
    assert!(
        metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
            .with_label_values(&["ready", "degraded", "shutting_down"])
            .get()
            > before
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn critical_server_failure_requests_shutdown_and_returns_error() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = test_health_status_handle();
    let (shutdown_tx, shutdown_rx) = shutdown_watch_pair();
    let before = metrics::TSO_SHUTDOWN_TOTAL
        .with_label_values(&["critical_server_failed_grpc"])
        .get();

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        supervise_critical_servers(
            CriticalServerContext {
                ready: &ready,
                health_status: &health_status,
                shutdown_tx: &shutdown_tx,
                shutdown_rx: shutdown_rx.clone(),
                worker_id: "worker-a",
                instance_id: "instance-a",
                advertise_endpoint: "endpoint-a:50051",
            },
            async {
                Err(Box::new(std::io::Error::other("grpc failed")) as Box<dyn std::error::Error>)
            },
            wait_for_shutdown_signal(shutdown_rx.clone()).map(|_| Ok(())),
        ),
    )
    .await
    .expect("critical supervision should not hang");

    let error = result.expect_err("critical server failure should return error");
    assert!(
        error.to_string().contains("grpc failed"),
        "unexpected error: {error}"
    );
    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
    assert_eq!(
        health_status.readiness_state(),
        WorkerReadinessState::Degraded
    );
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::ShuttingDown
    );
    assert!(
        metrics::TSO_SHUTDOWN_TOTAL
            .with_label_values(&["critical_server_failed_grpc"])
            .get()
            > before
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn unexpected_critical_server_exit_without_shutdown_is_fatal() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = test_health_status_handle();
    let (shutdown_tx, shutdown_rx) = shutdown_watch_pair();

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        supervise_critical_servers(
            CriticalServerContext {
                ready: &ready,
                health_status: &health_status,
                shutdown_tx: &shutdown_tx,
                shutdown_rx: shutdown_rx.clone(),
                worker_id: "worker-a",
                instance_id: "instance-a",
                advertise_endpoint: "endpoint-a:50051",
            },
            async { Ok(()) },
            wait_for_shutdown_signal(shutdown_rx.clone()).map(|_| Ok(())),
        ),
    )
    .await
    .expect("unexpected exit supervision should not hang");

    let error = result.expect_err("unexpected critical exit should return error");
    assert!(
        error
            .to_string()
            .contains("server exited without shutdown request"),
        "unexpected error: {error}"
    );
    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
}

#[test]
fn request_shutdown_preserves_identity_lease_loss_precedence() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = test_health_status_handle();
    let (shutdown_tx, shutdown_rx) = shutdown_watch_pair();
    let before_lost = metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
        .with_label_values(&["ready", "degraded", "identity_lease_lost"])
        .get();
    let before_shutdown = metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
        .with_label_values(&["degraded", "degraded", "shutting_down"])
        .get();

    request_shutdown(
        &ready,
        &health_status,
        &shutdown_tx,
        test_shutdown_identity(),
        ShutdownTrigger::IdentityLeaseLost,
    );
    request_shutdown(
        &ready,
        &health_status,
        &shutdown_tx,
        test_shutdown_identity(),
        ShutdownTrigger::ProcessSignal("signal_ctrl_c"),
    );

    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
    assert_eq!(
        health_status.readiness_state(),
        WorkerReadinessState::Degraded
    );
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::IdentityLeaseLost
    );
    assert!(!health_status.identity_lease_healthy());
    assert!(
        metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
            .with_label_values(&["ready", "degraded", "identity_lease_lost"])
            .get()
            > before_lost
    );
    assert_eq!(
        metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
            .with_label_values(&["degraded", "degraded", "shutting_down"])
            .get(),
        before_shutdown
    );
}

#[test]
fn ownership_drift_sink_flips_readyz_and_health_reason() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    let startup_complete = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    });
    let sink = StartupWorkerReadinessSink::new(
        ready.clone(),
        startup_complete,
        health_status.clone(),
        "worker-a".into(),
        "instance-a".into(),
        "endpoint-a:50051".into(),
    );

    chronos::WorkerReadinessSink::ownership_drift_started(
        &sink,
        chronos::OwnershipDriftEvidence {
            generator_id: 7,
            contending_instance_id: "instance-b".into(),
            lease_expire_at_ms: 200,
            observed_at_ms: 120,
        },
    );

    assert!(!ready.load(Ordering::Relaxed));
    assert_eq!(
        health_status.readiness_state(),
        WorkerReadinessState::Degraded
    );
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::OwnershipDrift
    );
}

#[test]
fn ownership_drift_clear_waits_for_startup_completion_before_restoring_ready() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(false));
    let startup_complete = Arc::new(AtomicBool::new(false));
    set_startup_ready(&ready, false);
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    });
    let sink = StartupWorkerReadinessSink::new(
        ready.clone(),
        startup_complete.clone(),
        health_status.clone(),
        "worker-a".into(),
        "instance-a".into(),
        "endpoint-a:50051".into(),
    );

    chronos::WorkerReadinessSink::ownership_drift_started(
        &sink,
        chronos::OwnershipDriftEvidence {
            generator_id: 7,
            contending_instance_id: "instance-b".into(),
            lease_expire_at_ms: 200,
            observed_at_ms: 120,
        },
    );
    chronos::WorkerReadinessSink::ownership_drift_cleared(&sink);
    assert!(!ready.load(Ordering::Relaxed));

    chronos::WorkerReadinessSink::ownership_drift_started(
        &sink,
        chronos::OwnershipDriftEvidence {
            generator_id: 7,
            contending_instance_id: "instance-b".into(),
            lease_expire_at_ms: 220,
            observed_at_ms: 140,
        },
    );
    startup_complete.store(true, Ordering::Release);
    chronos::WorkerReadinessSink::ownership_drift_cleared(&sink);

    assert!(ready.load(Ordering::Relaxed));
    assert_eq!(health_status.readiness_state(), WorkerReadinessState::Ready);
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::Serving
    );
}

#[test]
fn ownership_drift_clear_does_not_restore_ready_after_shutdown_or_identity_loss() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    let startup_complete = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    });
    let sink = StartupWorkerReadinessSink::new(
        ready.clone(),
        startup_complete.clone(),
        health_status.clone(),
        "worker-a".into(),
        "instance-a".into(),
        "endpoint-a:50051".into(),
    );

    chronos::WorkerReadinessSink::ownership_drift_started(
        &sink,
        chronos::OwnershipDriftEvidence {
            generator_id: 7,
            contending_instance_id: "instance-b".into(),
            lease_expire_at_ms: 200,
            observed_at_ms: 120,
        },
    );
    health_status.mark_shutting_down();
    chronos::WorkerReadinessSink::ownership_drift_cleared(&sink);
    assert!(!ready.load(Ordering::Relaxed));
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::ShuttingDown
    );

    let ready = Arc::new(AtomicBool::new(true));
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    });
    let sink = StartupWorkerReadinessSink::new(
        ready.clone(),
        startup_complete,
        health_status.clone(),
        "worker-a".into(),
        "instance-a".into(),
        "endpoint-a:50051".into(),
    );
    chronos::WorkerReadinessSink::ownership_drift_started(
        &sink,
        chronos::OwnershipDriftEvidence {
            generator_id: 7,
            contending_instance_id: "instance-b".into(),
            lease_expire_at_ms: 200,
            observed_at_ms: 120,
        },
    );
    health_status.mark_identity_lease_lost();
    chronos::WorkerReadinessSink::ownership_drift_cleared(&sink);
    assert!(!ready.load(Ordering::Relaxed));
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::IdentityLeaseLost
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn identity_lease_loss_flips_readiness_and_triggers_shutdown() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    });
    let (lost_tx, lost_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let service = TsoService::new(
        explicit_required_config(),
        Arc::new(ManualClock::new(1_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    let before_lost = metrics::TSO_IDENTITY_LEASE_EVENTS_TOTAL
        .with_label_values(&["lost"])
        .get();
    let before_shutdown = metrics::TSO_SHUTDOWN_TOTAL
        .with_label_values(&["identity_lease_lost"])
        .get();

    let monitor = spawn_identity_lease_loss_monitor(
        lost_rx,
        service.clone(),
        test_shutdown_context(ready.clone(), health_status.clone(), shutdown_tx),
    );

    lost_tx.send(true).unwrap();
    monitor.await.unwrap();

    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
    assert_eq!(
        service
            .ensure_timeline("identity-loss-local-gate")
            .await
            .unwrap_err(),
        TsoError::ServiceShuttingDown
    );
    assert_eq!(
        health_status.readiness_state(),
        WorkerReadinessState::Degraded
    );
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::IdentityLeaseLost
    );
    assert!(!health_status.identity_lease_healthy());
    assert_eq!(metrics::TSO_STARTUP_READY.get(), 0);
    assert!(
        metrics::TSO_IDENTITY_LEASE_EVENTS_TOTAL
            .with_label_values(&["lost"])
            .get()
            > before_lost
    );
    assert!(
        metrics::TSO_SHUTDOWN_TOTAL
            .with_label_values(&["identity_lease_lost"])
            .get()
            > before_shutdown
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn identity_lease_loss_monitor_triggers_shutdown_when_receiver_is_already_lost() {
    let _guard = startup_ready_guard();
    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: "worker-a".into(),
        instance_id: "instance-a".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
    });
    let (lost_tx, lost_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let service = TsoService::new(
        explicit_required_config(),
        Arc::new(ManualClock::new(1_000)),
        Arc::new(MemoryMetadataStore::new()),
    )
    .unwrap();

    lost_tx.send(true).unwrap();

    let monitor = spawn_identity_lease_loss_monitor(
        lost_rx,
        service.clone(),
        test_shutdown_context(ready.clone(), health_status.clone(), shutdown_tx),
    );

    monitor.await.unwrap();

    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::IdentityLeaseLost
    );
    assert!(!health_status.identity_lease_healthy());
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_identity_lease_loss_flips_readiness_and_triggers_shutdown() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();

    let unique_prefix = unique_test_etcd_prefix("lease-loss");
    let parsed_endpoints = parsed_test_etcd_endpoints();

    let clock = Arc::new(SystemClock);
    let config = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        instance_id: "instance-lease-loss".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
        etcd_endpoints: parsed_endpoints.clone(),
        lease_ttl_ms: 2_500,
        generator_maintenance_interval_ms: 200,
        ..explicit_required_config()
    };

    let startup = etcd_startup_config(config.clone(), unique_prefix.clone());
    let (service, identity_lease) =
        tokio::time::timeout(Duration::from_secs(5), build_tso_service(&startup, clock))
            .await
            .expect("etcd startup timed out")
            .expect("etcd startup should succeed");

    let identity_lease = identity_lease.expect("etcd startup should return an identity lease");

    let ready = Arc::new(AtomicBool::new(true));
    set_startup_ready(&ready, true);
    let health_status = HealthStatusHandle::serving(&chronos::HealthInfo {
        generator_count: 0,
        timeline_count: 0,
        worker_id: config.worker_id.clone(),
        instance_id: config.effective_instance_id().to_string(),
        advertise_endpoint: config.advertise_endpoint.clone(),
    });
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let before_lost = metrics::TSO_IDENTITY_LEASE_EVENTS_TOTAL
        .with_label_values(&["lost"])
        .get();
    let before_shutdown = metrics::TSO_SHUTDOWN_TOTAL
        .with_label_values(&["identity_lease_lost"])
        .get();

    let monitor = spawn_identity_lease_loss_monitor(
        identity_lease.lost_receiver(),
        service.clone(),
        super::runtime::ShutdownContext {
            ready: ready.clone(),
            health_status: health_status.clone(),
            shutdown_tx,
            worker_id: config.worker_id.clone(),
            instance_id: config.effective_instance_id().to_string(),
            advertise_endpoint: config.advertise_endpoint.clone(),
        },
    );

    let mut client = etcd_client::Client::connect(parsed_endpoints, None)
        .await
        .expect("revoke client should connect");
    let lease_key = format!(
        "{}/identity/instances/{}",
        unique_prefix,
        config.effective_instance_id()
    );
    let lease_response = client
        .get(lease_key, None)
        .await
        .expect("identity lease record lookup should succeed");
    let lease_kv = lease_response
        .kvs()
        .first()
        .expect("identity lease record should exist");
    let lease_id = lease_kv.lease();
    assert_ne!(
        lease_id, 0,
        "identity lease record should carry a non-zero lease id"
    );
    let lease_record: serde_json::Value = serde_json::from_slice(lease_kv.value())
        .expect("identity lease record should deserialize as json");
    assert_eq!(
        lease_record
            .get("instance_id")
            .and_then(|value| value.as_str()),
        Some(config.effective_instance_id())
    );
    assert_eq!(
        lease_record
            .get("worker_id")
            .and_then(|value| value.as_str()),
        Some(config.worker_id.as_str())
    );
    assert_eq!(
        lease_record
            .get("advertise_endpoint")
            .and_then(|value| value.as_str()),
        Some(config.advertise_endpoint.as_str())
    );
    client
        .lease_revoke(lease_id)
        .await
        .expect("external revoke should succeed");

    tokio::time::timeout(Duration::from_secs(5), monitor)
        .await
        .expect("identity loss monitor should observe lease revoke")
        .expect("identity loss monitor should exit cleanly");

    assert!(!ready.load(Ordering::Relaxed));
    assert!(*shutdown_rx.borrow());
    assert_eq!(
        health_status.readiness_state(),
        WorkerReadinessState::Degraded
    );
    assert_eq!(
        health_status.readiness_reason(),
        WorkerReadinessReason::IdentityLeaseLost
    );
    assert!(!health_status.identity_lease_healthy());
    assert_eq!(metrics::TSO_STARTUP_READY.get(), 0);
    assert!(
        metrics::TSO_IDENTITY_LEASE_EVENTS_TOTAL
            .with_label_values(&["lost"])
            .get()
            > before_lost
    );
    assert!(
        metrics::TSO_SHUTDOWN_TOTAL
            .with_label_values(&["identity_lease_lost"])
            .get()
            > before_shutdown
    );

    clear_tso_env();
}

#[test]
fn startup_preflight_validates_etcd_endpoints_from_typed_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let startup = super::config::LoadedStartupConfig::new(
        TsoConfig {
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            advertise_endpoint: "10.0.0.10:50051".into(),
            ..explicit_required_config()
        },
        super::config::StartupMetadata::Etcd(super::config::EtcdStartupConfig::new(
            vec!["127.0.0.1:2379".into()],
            "/chronos",
        )),
        super::config::StartupLoggingConfig::default(),
    );

    validate_startup_preflight(&startup).unwrap();
}

#[test]
fn startup_preflight_enforces_etcd_runtime_contract_from_typed_metadata() {
    let _guard = ENV_LOCK.lock().unwrap();
    clear_tso_env();
    let startup = super::config::LoadedStartupConfig::new(
        explicit_required_config(),
        super::config::StartupMetadata::Etcd(super::config::EtcdStartupConfig::new(
            vec!["127.0.0.1:2379".into()],
            "/chronos",
        )),
        super::config::StartupLoggingConfig::default(),
    );

    let error = validate_startup_preflight(&startup).unwrap_err();
    assert!(error.to_string().contains("CHRONOS_WORKER_ID"));
}

#[test]
fn loaded_startup_config_does_not_backfill_config_metadata_kind_from_typed_metadata() {
    let startup = super::config::LoadedStartupConfig::new(
        explicit_required_config(),
        super::config::StartupMetadata::Etcd(super::config::EtcdStartupConfig::new(
            vec!["127.0.0.1:2379".into()],
            "/chronos",
        )),
        super::config::StartupLoggingConfig::default(),
    );

    assert_eq!(startup.config.metadata_kind, "memory");
    assert_eq!(startup.metadata_kind(), "etcd");
}

#[test]
fn startup_ready_transition_records_metric() {
    let before = metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
        .with_label_values(&["starting", "ready", "serving"])
        .get();

    record_worker_readiness_transition(
        "starting",
        chronos::proto::v1::WorkerReadinessState::Ready,
        chronos::proto::v1::WorkerReadinessReason::Serving,
    );

    assert!(
        metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
            .with_label_values(&["starting", "ready", "serving"])
            .get()
            > before
    );
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_startup_rejects_duplicate_instance_identity() {
    clear_tso_env();

    let unique_prefix = unique_test_etcd_prefix("dup");

    let clock = Arc::new(SystemClock);
    let parsed_endpoints = parsed_test_etcd_endpoints();
    let config_a = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-a".into(),
        instance_id: "instance-dupe".into(),
        advertise_endpoint: "endpoint-a:50051".into(),
        etcd_endpoints: parsed_endpoints.clone(),
        lease_ttl_ms: 30_000,
        ..explicit_required_config()
    };
    let config_b = TsoConfig {
        metadata_kind: "etcd".into(),
        worker_id: "worker-b".into(),
        instance_id: "instance-dupe".into(),
        advertise_endpoint: "endpoint-b:50051".into(),
        etcd_endpoints: parsed_endpoints,
        lease_ttl_ms: 30_000,
        ..explicit_required_config()
    };

    let startup_a = etcd_startup_config(config_a, unique_prefix.clone());
    let startup_b = etcd_startup_config(config_b, unique_prefix);

    let (service_a, mut lease_a) = tokio::time::timeout(
        Duration::from_secs(5),
        build_tso_service(&startup_a, clock.clone()),
    )
    .await
    .expect("first startup timed out")
    .expect("first startup should succeed");

    let duplicate_result = tokio::time::timeout(
        Duration::from_secs(5),
        build_tso_service(&startup_b, clock.clone()),
    )
    .await
    .expect("second startup timed out");

    let error = match duplicate_result {
        Ok(_) => panic!("duplicate identity startup should fail"),
        Err(error) => error,
    };

    let error = error
        .downcast::<chronos::TsoError>()
        .expect("expected TsoError");
    assert!(matches!(
        *error,
        chronos::TsoError::InstanceIdentityInUse { ref instance_id }
            if instance_id == "instance-dupe"
    ));

    if let Some(identity_lease) = lease_a.as_mut() {
        identity_lease.shutdown().await;
    }
    service_a.shutdown().await;

    let (service_b, mut lease_b) =
        tokio::time::timeout(Duration::from_secs(5), build_tso_service(&startup_b, clock))
            .await
            .expect("restart after graceful release timed out")
            .expect("identity should be reacquired after graceful release");

    if let Some(identity_lease) = lease_b.as_mut() {
        identity_lease.shutdown().await;
    }
    service_b.shutdown().await;

    clear_tso_env();
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_startup_failure_after_identity_lease_revokes_lease() {
    clear_tso_env();

    let unique_prefix = unique_test_etcd_prefix("startup-revoke");
    let parsed_endpoints = parsed_test_etcd_endpoints();
    let instance_id = "instance-startup-revoke";
    let startup = etcd_startup_config(
        TsoConfig {
            metadata_kind: "etcd".into(),
            worker_id: "worker-a".into(),
            instance_id: instance_id.into(),
            advertise_endpoint: "endpoint-a:50051".into(),
            etcd_endpoints: parsed_endpoints.clone(),
            max_timeline_runtime_entries: 0,
            lease_ttl_ms: 30_000,
            ..explicit_required_config()
        },
        unique_prefix.clone(),
    );

    let error = match tokio::time::timeout(
        Duration::from_secs(5),
        build_tso_service(&startup, Arc::new(SystemClock)),
    )
    .await
    .expect("startup failure should not hang")
    {
        Ok(_) => panic!("startup should fail after lease acquisition"),
        Err(error) => error,
    };

    assert!(
        !error.to_string().is_empty(),
        "startup failure should surface a non-empty error"
    );

    let lease_key = format!("{}/identity/instances/{instance_id}", unique_prefix);
    let mut client = etcd_client::Client::connect(parsed_endpoints, None)
        .await
        .expect("etcd client should connect");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = client
                .get(lease_key.clone(), None)
                .await
                .expect("identity lease record lookup should succeed");
            if response.kvs().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("identity lease should be revoked after startup failure");

    clear_tso_env();
}
