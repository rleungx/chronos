use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::header::{HeaderValue, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request as HyperRequest, Response as HyperResponse, StatusCode};
use hyper_util::rt::TokioIo;
use prometheus::{Encoder, TextEncoder};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::warn;

use chronos::tls as shared_tls;
use chronos::{metrics, TsoConfig};

use crate::AppResult;

type MetricsResponseBody = Full<Bytes>;

fn plain_text_response(
    status: StatusCode,
    body: impl Into<Bytes>,
) -> HyperResponse<MetricsResponseBody> {
    let mut response = HyperResponse::new(Full::new(body.into()));
    *response.status_mut() = status;
    response
}

fn metrics_response(buffer: Vec<u8>) -> HyperResponse<MetricsResponseBody> {
    let mut response = HyperResponse::new(Full::new(Bytes::from(buffer)));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    response
}

fn listener_addr_label(listen_addr: Option<std::net::SocketAddr>) -> String {
    listen_addr
        .map(|addr| addr.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

pub(crate) async fn metrics_handler<B>(
    req: HyperRequest<B>,
    ready: Arc<AtomicBool>,
) -> Result<HyperResponse<MetricsResponseBody>, hyper::Error> {
    let path = req.uri().path().to_owned();
    match path.as_str() {
        "/healthz" | "/readyz" => health_handler(req, ready).await,
        "/metrics" => {
            let started = Instant::now();
            let encoder = TextEncoder::new();
            let metric_families = prometheus::gather();
            let mut buffer = Vec::new();
            if let Err(error) = encoder.encode(&metric_families, &mut buffer) {
                metrics::TSO_METRICS_RENDER_ERRORS_TOTAL.inc();
                metrics::TSO_METRICS_RENDER_LATENCY.observe(started.elapsed().as_secs_f64());
                warn!(
                    component = "startup",
                    event = "metrics_render_failed",
                    result = "degraded",
                    reason = %error
                );
                return Ok(plain_text_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "metrics unavailable",
                ));
            }
            metrics::TSO_METRICS_RENDER_LATENCY.observe(started.elapsed().as_secs_f64());
            Ok(metrics_response(buffer))
        }
        _ => Ok(plain_text_response(StatusCode::NOT_FOUND, "not found")),
    }
}

pub(crate) async fn health_handler<B>(
    req: HyperRequest<B>,
    ready: Arc<AtomicBool>,
) -> Result<HyperResponse<MetricsResponseBody>, hyper::Error> {
    match req.uri().path() {
        "/healthz" => Ok(plain_text_response(StatusCode::OK, "ok")),
        "/readyz" => {
            let status = if ready.load(Ordering::Relaxed) {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            let body = if status == StatusCode::OK {
                "ready"
            } else {
                "not ready"
            };
            Ok(plain_text_response(status, body))
        }
        _ => Ok(plain_text_response(StatusCode::NOT_FOUND, "not found")),
    }
}

pub(crate) async fn bind_metrics_listener(
    config: &TsoConfig,
) -> Result<(TcpListener, Option<TlsAcceptor>), Box<dyn std::error::Error>> {
    let addr: std::net::SocketAddr = config.metrics_bind_addr.parse()?;
    let listener = TcpListener::bind(addr).await?;
    let tls_acceptor = load_metrics_tls_acceptor(config)?;
    Ok((listener, tls_acceptor))
}

pub(crate) async fn bind_health_listener(
    config: &TsoConfig,
) -> Result<Option<TcpListener>, Box<dyn std::error::Error>> {
    let Some(addr) = config.health_bind_addr.as_deref() else {
        return Ok(None);
    };
    Ok(Some(
        TcpListener::bind(addr.parse::<std::net::SocketAddr>()?).await?,
    ))
}

pub(crate) async fn serve_metrics_listener(
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
    ready: Arc<AtomicBool>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut shutdown = Box::pin(wait_for_shutdown_signal(shutdown_rx));
    let mut connections = JoinSet::new();
    let listen_addr = listener_addr_label(listener.local_addr().ok());

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accept_result = listener.accept() => {
                let (stream, _) = accept_result?;
                let ready = ready.clone();
                let tls_acceptor = tls_acceptor.clone();
                let listen_addr = listen_addr.clone();
                connections.spawn(async move {
                    let service = service_fn(move |req| metrics_handler(req, ready.clone()));
                    if let Some(tls_acceptor) = tls_acceptor {
                        match tls_acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                if let Err(error) = http1::Builder::new()
                                    .serve_connection(TokioIo::new(tls_stream), service)
                                    .await
                                {
                                    warn!(
                                        component = "startup",
                                        event = "listener_connection_error",
                                        result = "degraded",
                                        reason = %error,
                                        transport = "mtls",
                                        listen_addr = %listen_addr
                                    );
                                }
                            }
                            Err(error) => {
                                warn!(
                                    component = "startup",
                                    event = "listener_connection_error",
                                    result = "failure",
                                    reason = %error,
                                    transport = "mtls"
                                );
                            }
                        }
                    } else if let Err(error) = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                    {
                        warn!(
                            component = "startup",
                            event = "listener_connection_error",
                            result = "degraded",
                            reason = %error,
                            transport = "plain"
                        );
                    }
                });
            }
            Some(join_result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = join_result {
                    warn!(
                        component = "startup",
                        event = "listener_task_error",
                        result = "degraded",
                        reason = %error
                    );
                }
            }
        }
    }

    connections.abort_all();
    while let Some(join_result) = connections.join_next().await {
        if let Err(error) = join_result {
            if !error.is_cancelled() {
                warn!(
                    component = "startup",
                    event = "listener_task_error",
                    result = "degraded",
                    reason = %error
                );
            }
        }
    }

    Ok(())
}

pub(crate) async fn serve_health_listener(
    listener: TcpListener,
    ready: Arc<AtomicBool>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut shutdown = Box::pin(wait_for_shutdown_signal(shutdown_rx));
    let mut connections = JoinSet::new();
    let listen_addr = listener_addr_label(listener.local_addr().ok());

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accept_result = listener.accept() => {
                let (stream, _) = accept_result?;
                let ready = ready.clone();
                let listen_addr = listen_addr.clone();
                connections.spawn(async move {
                    let service = service_fn(move |req| health_handler(req, ready.clone()));
                    if let Err(error) = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                    {
                        warn!(
                            component = "startup",
                            event = "listener_connection_error",
                            result = "degraded",
                            reason = %error,
                            transport = "health",
                            listen_addr = %listen_addr
                        );
                    }
                });
            }
            Some(join_result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = join_result {
                    warn!(
                        component = "startup",
                        event = "listener_task_error",
                        result = "degraded",
                        reason = %error
                    );
                }
            }
        }
    }

    connections.abort_all();
    while let Some(join_result) = connections.join_next().await {
        if let Err(error) = join_result {
            if !error.is_cancelled() {
                warn!(
                    component = "startup",
                    event = "listener_task_error",
                    result = "degraded",
                    reason = %error
                );
            }
        }
    }

    Ok(())
}

pub(crate) async fn wait_for_shutdown_signal(mut shutdown_rx: watch::Receiver<bool>) {
    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        if shutdown_rx.changed().await.is_err() {
            break;
        }
    }
}

pub(crate) async fn bind_grpc_listener(config: &TsoConfig) -> AppResult<TcpListener> {
    let addr: std::net::SocketAddr = config.bind_addr.parse()?;
    Ok(TcpListener::bind(addr).await?)
}

pub(crate) fn grpc_listener_stream(
    listener: TcpListener,
) -> impl futures::Stream<Item = Result<TcpStream, std::io::Error>> {
    stream::try_unfold(listener, |listener| async move {
        let (stream, _) = listener.accept().await?;
        Ok(Some((stream, listener)))
    })
}

pub(crate) fn parse_pem_certificates(
    pem: &[u8],
    env_key: &str,
) -> AppResult<Vec<CertificateDer<'static>>> {
    Ok(shared_tls::parse_pem_certificates(pem, env_key)?)
}

pub(crate) fn parse_pem_private_key(
    pem: &[u8],
    env_key: &str,
) -> AppResult<PrivateKeyDer<'static>> {
    Ok(shared_tls::parse_pem_private_key(pem, env_key)?)
}

pub(crate) fn load_root_cert_store(pem: &[u8], env_key: &str) -> AppResult<RootCertStore> {
    Ok(shared_tls::load_root_cert_store(pem, env_key)?)
}

fn load_grpc_server_tls_config(config: &TsoConfig) -> AppResult<Option<ServerTlsConfig>> {
    let Some(paths) = config.grpc_tls_paths()? else {
        return Ok(None);
    };

    let cert = shared_tls::read_required_pem_file(paths.cert_file, "CHRONOS_GRPC_TLS_CERT_FILE")?;
    let key = shared_tls::read_required_pem_file(paths.key_file, "CHRONOS_GRPC_TLS_KEY_FILE")?;
    let client_ca =
        shared_tls::read_required_pem_file(paths.client_ca_file, "CHRONOS_GRPC_CLIENT_CA_FILE")?;

    Ok(Some(
        ServerTlsConfig::new()
            .identity(Identity::from_pem(cert, key))
            .client_ca_root(Certificate::from_pem(client_ca)),
    ))
}

pub(crate) fn load_metrics_tls_acceptor(config: &TsoConfig) -> AppResult<Option<TlsAcceptor>> {
    let Some(paths) = config.metrics_tls_paths()? else {
        return Ok(None);
    };

    let cert =
        shared_tls::read_required_pem_file(paths.cert_file, "CHRONOS_METRICS_TLS_CERT_FILE")?;
    let key = shared_tls::read_required_pem_file(paths.key_file, "CHRONOS_METRICS_TLS_KEY_FILE")?;
    let client_ca =
        shared_tls::read_required_pem_file(paths.client_ca_file, "CHRONOS_METRICS_CLIENT_CA_FILE")?;
    let client_verifier = WebPkiClientVerifier::builder(
        load_root_cert_store(&client_ca, "CHRONOS_METRICS_CLIENT_CA_FILE")?.into(),
    )
    .build()?;
    let tls_config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(
            parse_pem_certificates(&cert, "CHRONOS_METRICS_TLS_CERT_FILE")?,
            parse_pem_private_key(&key, "CHRONOS_METRICS_TLS_KEY_FILE")?,
        )?;

    Ok(Some(TlsAcceptor::from(Arc::new(tls_config))))
}

pub(crate) fn build_grpc_server(config: &TsoConfig) -> AppResult<Server> {
    let mut builder = Server::builder();

    if let Some(timeout_ms) = config.grpc_request_timeout_ms {
        builder = builder.timeout(Duration::from_millis(timeout_ms));
    }

    if let Some(limit) = config.grpc_max_concurrent_requests {
        builder = builder.concurrency_limit_per_connection(limit);
    }

    if let Some(tls_config) = load_grpc_server_tls_config(config)? {
        builder = builder.tls_config(tls_config)?;
    }

    Ok(builder)
}
