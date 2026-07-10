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
use futures::{FutureExt, StreamExt};
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
        safety_gap_ms: 500,
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

mod config_env;
mod etcd_identity;
mod preflight;
mod readiness_shutdown;
mod tls_runtime;
