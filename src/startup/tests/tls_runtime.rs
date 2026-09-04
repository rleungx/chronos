use super::*;

#[derive(Clone)]
struct GlobalConcurrencyProbeService {
    entered: Arc<std::sync::atomic::AtomicUsize>,
    first_entered: Arc<tokio::sync::Notify>,
    release_first: Arc<tokio::sync::Notify>,
}

#[tonic::async_trait]
impl chronos::proto::v1::timestamp_service_server::TimestampService
    for GlobalConcurrencyProbeService
{
    type AllocateTimestampsStreamStream = std::pin::Pin<
        Box<
            dyn futures::Stream<
                    Item = Result<chronos::proto::v1::AllocateTimestampsResponse, tonic::Status>,
                > + Send
                + 'static,
        >,
    >;

    async fn allocate_timestamps(
        &self,
        _request: tonic::Request<ProtoAllocateTimestampsRequest>,
    ) -> Result<tonic::Response<chronos::proto::v1::AllocateTimestampsResponse>, tonic::Status>
    {
        let ordinal = self.entered.fetch_add(1, Ordering::SeqCst) + 1;
        if ordinal == 1 {
            self.first_entered.notify_one();
            self.release_first.notified().await;
        }
        Ok(tonic::Response::new(
            chronos::proto::v1::AllocateTimestampsResponse::default(),
        ))
    }

    async fn allocate_timestamps_stream(
        &self,
        _request: tonic::Request<tonic::Streaming<ProtoAllocateTimestampsRequest>>,
    ) -> Result<tonic::Response<Self::AllocateTimestampsStreamStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "streaming is not implemented by the concurrency test double",
        ))
    }
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
async fn grpc_concurrency_limit_is_global_across_connections() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let config = TsoConfig {
        grpc_max_concurrent_requests: Some(1),
        grpc_request_timeout_ms: None,
        ..explicit_dev_insecure_local_config(addr)
    };
    let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let first_entered = Arc::new(tokio::sync::Notify::new());
    let release_first = Arc::new(tokio::sync::Notify::new());
    let probe = GlobalConcurrencyProbeService {
        entered: entered.clone(),
        first_entered: first_entered.clone(),
        release_first: release_first.clone(),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = build_grpc_server(&config)
        .unwrap()
        .add_service(TimestampServiceServer::new(probe))
        .serve_with_shutdown(addr, wait_for_shutdown_signal(shutdown_rx));
    let server_handle = tokio::spawn(server);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let endpoint = format!("http://{addr}");
    let mut first_client = TimestampServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut second_client = TimestampServiceClient::connect(endpoint).await.unwrap();
    let first = tokio::spawn(async move {
        first_client
            .allocate_timestamps(ProtoAllocateTimestampsRequest::default())
            .await
    });
    first_entered.notified().await;
    let overload = tokio::time::timeout(
        Duration::from_secs(1),
        second_client.allocate_timestamps(ProtoAllocateTimestampsRequest::default()),
    )
    .await
    .expect("overloaded request should be rejected without queueing")
    .expect_err("overloaded request should fail");
    assert_eq!(overload.code(), tonic::Code::ResourceExhausted);
    assert_eq!(entered.load(Ordering::SeqCst), 1);
    release_first.notify_one();
    first.await.unwrap().unwrap();

    second_client
        .allocate_timestamps(ProtoAllocateTimestampsRequest::default())
        .await
        .unwrap();
    assert_eq!(entered.load(Ordering::SeqCst), 2);

    shutdown_tx.send(true).unwrap();
    server_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn grpc_listener_stream_caps_active_connections() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut incoming = Box::pin(grpc_listener_stream_with_limit(listener, 1));

    let first_client = TcpStream::connect(addr).await.unwrap();
    let first_server = incoming.next().await.unwrap().unwrap();
    let second_client = TcpStream::connect(addr).await.unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(50), incoming.next())
            .await
            .is_err()
    );
    drop(first_server);
    let second_server = tokio::time::timeout(Duration::from_secs(1), incoming.next())
        .await
        .expect("second connection should be accepted after the permit is released")
        .unwrap()
        .unwrap();

    drop((first_client, second_client, second_server));
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

    let mut allocation_stream = authorized_timestamp
        .allocate_timestamps_stream(tokio_stream::iter([ProtoAllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "grpc-auth-stream-allocate".to_string(),
            request_timeout_ms: 0,
        }]))
        .await
        .unwrap()
        .into_inner();
    let streamed_allocation = allocation_stream.message().await.unwrap().unwrap();
    assert_eq!(streamed_allocation.timeline_key, route.timeline_key);
    assert!(allocation_stream.message().await.unwrap().is_none());

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

    let error = denied_timestamp
        .allocate_timestamps_stream(tokio_stream::iter([ProtoAllocateTimestampsRequest {
            timeline_key: route.timeline_key.clone(),
            count: 1,
            expected_epoch: route.epoch,
            expected_route_version: route.route_version,
            client_request_id: "grpc-auth-denied-stream-allocate".to_string(),
            request_timeout_ms: 0,
        }]))
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
