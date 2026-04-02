use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

use tokio::sync::watch;
use tracing::{error, info, warn};

use chronos::metrics;
use chronos::proto::v1::{
    timeline_control_service_server::TimelineControlServiceServer,
    timeline_route_service_server::TimelineRouteServiceServer,
    timeline_status_service_server::TimelineStatusServiceServer,
    timestamp_service_server::TimestampServiceServer, WorkerReadinessReason, WorkerReadinessState,
};
use chronos::rpc::{
    HealthStatusHandle, TsoControlService, TsoRouteService, TsoTimelineStatusService,
    TsoTimestampService,
};
use chronos::{
    OwnershipDriftEvidence, SystemClock, TsoConfig, TsoError, TsoService, WorkerReadinessSink,
};

use crate::AppResult;

use super::bootstrap::build_tso_service;
use super::config::load_startup_config;
use super::preflight::{log_startup_preflight, startup_failure_stage, validate_startup_preflight};
use super::readiness_reason::{
    startup_bootstrap_failure_reason, startup_preflight_failure_reason, ShutdownTrigger,
};
use super::serving_gate::{CriticalStartupListener, StartupServingGate};
use super::transport::{
    bind_grpc_listener, bind_metrics_listener, build_grpc_server, grpc_listener_stream,
    serve_metrics_listener, wait_for_shutdown_signal,
};

fn init_tracing() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_target(false)
            .json()
            .flatten_event(true)
            .try_init();
    });
}

const CRITICAL_SERVER_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct StartupWorkerReadinessSink {
    ready: Arc<AtomicBool>,
    startup_complete: Arc<AtomicBool>,
    health_status: HealthStatusHandle,
    worker_id: String,
    instance_id: String,
    advertise_endpoint: String,
}

impl StartupWorkerReadinessSink {
    pub(crate) fn new(
        ready: Arc<AtomicBool>,
        startup_complete: Arc<AtomicBool>,
        health_status: HealthStatusHandle,
        worker_id: String,
        instance_id: String,
        advertise_endpoint: String,
    ) -> Self {
        Self {
            ready,
            startup_complete,
            health_status,
            worker_id,
            instance_id,
            advertise_endpoint,
        }
    }

    fn maybe_record_transition(
        &self,
        previous_state: WorkerReadinessState,
        previous_reason: WorkerReadinessReason,
    ) -> Option<(WorkerReadinessState, WorkerReadinessReason)> {
        let new_state = self.health_status.readiness_state();
        let new_reason = self.health_status.readiness_reason();
        if previous_state != new_state || previous_reason != new_reason {
            record_worker_readiness_transition(
                readiness_state_label(previous_state),
                new_state,
                new_reason,
            );
            Some((new_state, new_reason))
        } else {
            None
        }
    }
}

impl WorkerReadinessSink for StartupWorkerReadinessSink {
    fn ownership_drift_started(&self, evidence: OwnershipDriftEvidence) {
        let previous_state = self.health_status.readiness_state();
        let previous_reason = self.health_status.readiness_reason();
        self.health_status.mark_ownership_drift();
        if let Some((new_state, new_reason)) =
            self.maybe_record_transition(previous_state, previous_reason)
        {
            set_startup_ready(&self.ready, false);
            warn!(
                component = "startup",
                event = "ownership_drift_detected",
                result = "degraded",
                previous_readiness_state = readiness_state_label(previous_state),
                readiness_state = readiness_state_label(new_state),
                previous_readiness_reason = readiness_reason_label(previous_reason),
                readiness_reason = readiness_reason_label(new_reason),
                generator_id = evidence.generator_id,
                contending_instance_id = evidence.contending_instance_id,
                lease_expire_at_ms = evidence.lease_expire_at_ms,
                observed_at_ms = evidence.observed_at_ms,
                worker_id = self.worker_id,
                instance_id = self.instance_id,
                advertise_endpoint = self.advertise_endpoint,
            );
        }
    }

    fn ownership_drift_cleared(&self) {
        let previous_state = self.health_status.readiness_state();
        let previous_reason = self.health_status.readiness_reason();
        self.health_status.clear_ownership_drift();
        if let Some((new_state, new_reason)) =
            self.maybe_record_transition(previous_state, previous_reason)
        {
            if self.startup_complete.load(Ordering::Acquire)
                && new_state == WorkerReadinessState::Ready
                && new_reason == WorkerReadinessReason::Serving
            {
                set_startup_ready(&self.ready, true);
            }
            info!(
                component = "startup",
                event = "ownership_drift_cleared",
                result = "success",
                previous_readiness_state = readiness_state_label(previous_state),
                readiness_state = readiness_state_label(new_state),
                previous_readiness_reason = readiness_reason_label(previous_reason),
                readiness_reason = readiness_reason_label(new_reason),
                worker_id = self.worker_id,
                instance_id = self.instance_id,
                advertise_endpoint = self.advertise_endpoint,
            );
        }
    }
}

pub(crate) fn set_startup_ready(ready: &Arc<AtomicBool>, is_ready: bool) {
    ready.store(is_ready, Ordering::Relaxed);
    metrics::TSO_STARTUP_READY.set(if is_ready { 1 } else { 0 });
}

pub(crate) fn record_startup_preflight_failure(stage: &'static str) {
    metrics::TSO_STARTUP_PREFLIGHT_FAIL_TOTAL
        .with_label_values(&[stage])
        .inc();
}

pub(crate) fn record_startup_bootstrap_failure(stage: &'static str) {
    metrics::TSO_STARTUP_BOOTSTRAP_FAIL_TOTAL
        .with_label_values(&[stage])
        .inc();
}

pub(crate) fn record_identity_lease_event(event: &'static str) {
    metrics::TSO_IDENTITY_LEASE_EVENTS_TOTAL
        .with_label_values(&[event])
        .inc();
}

pub(crate) fn record_shutdown(reason: &'static str) {
    metrics::TSO_SHUTDOWN_TOTAL
        .with_label_values(&[reason])
        .inc();
}

pub(crate) fn readiness_state_label(state: WorkerReadinessState) -> &'static str {
    match state {
        WorkerReadinessState::Ready => "ready",
        WorkerReadinessState::Degraded => "degraded",
        WorkerReadinessState::Unspecified => "unspecified",
    }
}

pub(crate) fn readiness_reason_label(reason: WorkerReadinessReason) -> &'static str {
    match reason {
        WorkerReadinessReason::Serving => "serving",
        WorkerReadinessReason::StartupPreflightFailed => "startup_preflight_failed",
        WorkerReadinessReason::MetadataStartupProbeFailed => "metadata_startup_probe_failed",
        WorkerReadinessReason::IdentityLeaseAcquireFailed => "identity_lease_acquire_failed",
        WorkerReadinessReason::IdentityLeaseLost => "identity_lease_lost",
        WorkerReadinessReason::ShuttingDown => "shutting_down",
        WorkerReadinessReason::Draining => "draining",
        WorkerReadinessReason::OwnershipDrift => "ownership_drift",
        WorkerReadinessReason::ClusterContractMismatch => "cluster_contract_mismatch",
        WorkerReadinessReason::Unspecified => "unspecified",
    }
}

pub(crate) fn record_worker_readiness_transition(
    from_state: &'static str,
    to_state: WorkerReadinessState,
    reason: WorkerReadinessReason,
) {
    metrics::TSO_WORKER_READINESS_TRANSITIONS_TOTAL
        .with_label_values(&[
            from_state,
            readiness_state_label(to_state),
            readiness_reason_label(reason),
        ])
        .inc();
}

pub(crate) fn request_shutdown(
    ready: &Arc<AtomicBool>,
    health_status: &HealthStatusHandle,
    shutdown_tx: &watch::Sender<bool>,
    worker_id: &str,
    instance_id: &str,
    advertise_endpoint: &str,
    trigger: ShutdownTrigger,
) {
    let previous_state = health_status.readiness_state();
    let previous_reason = health_status.readiness_reason();
    trigger.apply_to(health_status);
    let new_state = health_status.readiness_state();
    let new_reason = health_status.readiness_reason();
    if previous_state != new_state || previous_reason != new_reason {
        record_worker_readiness_transition(
            readiness_state_label(previous_state),
            new_state,
            new_reason,
        );
    }
    warn!(
        component = "shutdown",
        event = "shutdown_requested",
        result = "degraded",
        action_kind = "worker_readiness",
        action_outcome = "started",
        shutdown_trigger = trigger.label(),
        previous_readiness_state = readiness_state_label(previous_state),
        readiness_state = readiness_state_label(new_state),
        previous_readiness_reason = readiness_reason_label(previous_reason),
        readiness_reason = readiness_reason_label(new_reason),
        worker_id,
        instance_id,
        advertise_endpoint
    );
    set_startup_ready(ready, false);
    record_shutdown(trigger.label());
    let _ = shutdown_tx.send(true);
}

fn critical_server_failure_error(
    listener: CriticalStartupListener,
    message: impl Into<String>,
) -> Box<dyn std::error::Error> {
    Box::new(std::io::Error::other(format!(
        "critical {} server failure: {}",
        listener.label(),
        message.into()
    )))
}

async fn await_server_shutdown_with_grace<F>(
    server: F,
    listener: CriticalStartupListener,
    worker_id: &str,
    instance_id: &str,
    advertise_endpoint: &str,
) where
    F: std::future::Future<Output = AppResult<()>>,
{
    match tokio::time::timeout(CRITICAL_SERVER_SHUTDOWN_GRACE, server).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            warn!(
                component = "startup",
                event = "critical_server_shutdown_failed",
                result = "failure",
                transport = listener.label(),
                reason = %error,
                worker_id,
                instance_id,
                advertise_endpoint
            );
        }
        Err(_) => {
            warn!(
                component = "startup",
                event = "critical_server_shutdown_timed_out",
                result = "degraded",
                transport = listener.label(),
                shutdown_grace_ms = CRITICAL_SERVER_SHUTDOWN_GRACE.as_millis(),
                worker_id,
                instance_id,
                advertise_endpoint
            );
        }
    }
}

pub(crate) async fn supervise_critical_servers<GrpcServer, MetricsServer>(
    context: CriticalServerContext<'_>,
    grpc_server: GrpcServer,
    metrics_server: MetricsServer,
) -> AppResult<()>
where
    GrpcServer: std::future::Future<Output = AppResult<()>>,
    MetricsServer: std::future::Future<Output = AppResult<()>>,
{
    tokio::pin!(grpc_server);
    tokio::pin!(metrics_server);
    let shutdown_requested = |shutdown_rx: &watch::Receiver<bool>| *shutdown_rx.borrow();
    let shutdown_rx = context.shutdown_rx;

    tokio::select! {
        grpc_result = &mut grpc_server => {
            match grpc_result {
                Ok(()) if shutdown_requested(&shutdown_rx) => {
                    metrics_server.await?;
                    Ok(())
                }
                Ok(()) => {
                    let error = critical_server_failure_error(
                        CriticalStartupListener::Grpc,
                        "server exited without shutdown request",
                    );
                    request_shutdown(
                        context.ready,
                        context.health_status,
                        context.shutdown_tx,
                        context.worker_id,
                        context.instance_id,
                        context.advertise_endpoint,
                        ShutdownTrigger::CriticalServerFailed(CriticalStartupListener::Grpc.label()),
                    );
                    await_server_shutdown_with_grace(
                        metrics_server,
                        CriticalStartupListener::Admin,
                        context.worker_id,
                        context.instance_id,
                        context.advertise_endpoint,
                    ).await;
                    Err(error)
                }
                Err(error) => {
                    request_shutdown(
                        context.ready,
                        context.health_status,
                        context.shutdown_tx,
                        context.worker_id,
                        context.instance_id,
                        context.advertise_endpoint,
                        ShutdownTrigger::CriticalServerFailed(CriticalStartupListener::Grpc.label()),
                    );
                    await_server_shutdown_with_grace(
                        metrics_server,
                        CriticalStartupListener::Admin,
                        context.worker_id,
                        context.instance_id,
                        context.advertise_endpoint,
                    ).await;
                    Err(error)
                }
            }
        }
        metrics_result = &mut metrics_server => {
            match metrics_result {
                Ok(()) if shutdown_requested(&shutdown_rx) => {
                    grpc_server.await?;
                    Ok(())
                }
                Ok(()) => {
                    let error = critical_server_failure_error(
                        CriticalStartupListener::Admin,
                        "server exited without shutdown request",
                    );
                    request_shutdown(
                        context.ready,
                        context.health_status,
                        context.shutdown_tx,
                        context.worker_id,
                        context.instance_id,
                        context.advertise_endpoint,
                        ShutdownTrigger::CriticalServerFailed(CriticalStartupListener::Admin.label()),
                    );
                    await_server_shutdown_with_grace(
                        grpc_server,
                        CriticalStartupListener::Grpc,
                        context.worker_id,
                        context.instance_id,
                        context.advertise_endpoint,
                    ).await;
                    Err(error)
                }
                Err(error) => {
                    request_shutdown(
                        context.ready,
                        context.health_status,
                        context.shutdown_tx,
                        context.worker_id,
                        context.instance_id,
                        context.advertise_endpoint,
                        ShutdownTrigger::CriticalServerFailed(CriticalStartupListener::Admin.label()),
                    );
                    await_server_shutdown_with_grace(
                        grpc_server,
                        CriticalStartupListener::Grpc,
                        context.worker_id,
                        context.instance_id,
                        context.advertise_endpoint,
                    ).await;
                    Err(error)
                }
            }
        }
    }
}

pub(crate) struct CriticalServerContext<'a> {
    pub(crate) ready: &'a Arc<AtomicBool>,
    pub(crate) health_status: &'a HealthStatusHandle,
    pub(crate) shutdown_tx: &'a watch::Sender<bool>,
    pub(crate) shutdown_rx: watch::Receiver<bool>,
    pub(crate) worker_id: &'a str,
    pub(crate) instance_id: &'a str,
    pub(crate) advertise_endpoint: &'a str,
}

pub(crate) fn spawn_identity_lease_loss_monitor(
    mut lost_rx: watch::Receiver<bool>,
    ready: Arc<AtomicBool>,
    health_status: HealthStatusHandle,
    shutdown_tx: watch::Sender<bool>,
    worker_id: String,
    instance_id: String,
    advertise_endpoint: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while lost_rx.changed().await.is_ok() {
            if *lost_rx.borrow() {
                record_identity_lease_event("lost");
                request_shutdown(
                    &ready,
                    &health_status,
                    &shutdown_tx,
                    &worker_id,
                    &instance_id,
                    &advertise_endpoint,
                    ShutdownTrigger::IdentityLeaseLost,
                );
                error!(
                    component = "identity_lease",
                    event = "keepalive_lost",
                    result = "failure",
                    action_kind = "identity_lease",
                    action_outcome = "failed",
                    readiness_reason = "identity_lease_lost",
                    worker_id,
                    instance_id,
                    advertise_endpoint,
                    action = "shutdown"
                );
                break;
            }
        }
    })
}

#[cfg(unix)]
async fn wait_for_process_signal() -> Result<&'static str, std::io::Error> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok("signal_ctrl_c"),
        _ = terminate.recv() => Ok("signal_sigterm"),
    }
}

#[cfg(not(unix))]
async fn wait_for_process_signal() -> Result<&'static str, std::io::Error> {
    tokio::signal::ctrl_c().await?;
    Ok("signal_ctrl_c")
}

fn spawn_process_signal_monitor(
    ready: Arc<AtomicBool>,
    health_status: HealthStatusHandle,
    shutdown_tx: watch::Sender<bool>,
    worker_id: String,
    instance_id: String,
    advertise_endpoint: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match wait_for_process_signal().await {
            Ok(reason) => {
                request_shutdown(
                    &ready,
                    &health_status,
                    &shutdown_tx,
                    &worker_id,
                    &instance_id,
                    &advertise_endpoint,
                    ShutdownTrigger::ProcessSignal(reason),
                );
                warn!(
                    component = "shutdown",
                    event = "signal_received",
                    result = "degraded",
                    action_kind = "signal",
                    action_outcome = "received",
                    shutdown_trigger = reason,
                    worker_id,
                    instance_id,
                    advertise_endpoint,
                    action = "signal"
                );
            }
            Err(error) => {
                error!(
                    component = "startup",
                    event = "signal_monitor_error",
                    result = "failure",
                    reason = %error
                );
            }
        }
    })
}

fn build_route_service(
    config: &TsoConfig,
    service: &Arc<TsoService>,
) -> TimelineRouteServiceServer<TsoRouteService> {
    let server = TimelineRouteServiceServer::new(TsoRouteService::new(service.control_plane()));
    if let Some(limit) = config.grpc_max_request_bytes {
        server.max_decoding_message_size(limit)
    } else {
        server
    }
}

fn build_timestamp_service(
    config: &TsoConfig,
    service: &Arc<TsoService>,
) -> TimestampServiceServer<TsoTimestampService> {
    let server = TimestampServiceServer::new(TsoTimestampService::new(service.data_plane()));
    if let Some(limit) = config.grpc_max_request_bytes {
        server.max_decoding_message_size(limit)
    } else {
        server
    }
}

fn build_control_service(
    config: &TsoConfig,
    service: &Arc<TsoService>,
    health_status: HealthStatusHandle,
) -> TimelineControlServiceServer<TsoControlService> {
    let server = TimelineControlServiceServer::new(TsoControlService::with_health_status(
        service.control_plane(),
        health_status,
    ));
    if let Some(limit) = config.grpc_max_request_bytes {
        server.max_decoding_message_size(limit)
    } else {
        server
    }
}

fn build_timeline_status_service(
    config: &TsoConfig,
    service: &Arc<TsoService>,
) -> TimelineStatusServiceServer<TsoTimelineStatusService> {
    let server =
        TimelineStatusServiceServer::new(TsoTimelineStatusService::new(service.control_plane()));
    if let Some(limit) = config.grpc_max_request_bytes {
        server.max_decoding_message_size(limit)
    } else {
        server
    }
}

pub(crate) async fn run() -> AppResult<()> {
    init_tracing();
    info!(
        component = "startup",
        event = "startup_begin",
        result = "success",
        reason = "process_start"
    );

    let clock = Arc::new(SystemClock);
    let startup = load_startup_config()?;
    let config = &startup.config;
    let ready = Arc::new(AtomicBool::new(false));
    let startup_complete = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    set_startup_ready(&ready, false);
    let validated_startup = match validate_startup_preflight(&startup) {
        Ok(plan) => plan,
        Err(error) => {
            let readiness_reason = startup_preflight_failure_reason();
            record_startup_preflight_failure("config");
            error!(
                component = "startup",
                event = "preflight_failed",
                result = "failure",
                reason = %error,
                readiness_reason = readiness_reason_label(readiness_reason),
                startup_stage = "config",
                metadata_kind = %startup.metadata_kind(),
                worker_id = %config.worker_id,
                instance_id = %config.effective_instance_id(),
                advertise_endpoint = %config.advertise_endpoint
            );
            return Err(error);
        }
    };
    log_startup_preflight(&validated_startup);

    let (service, identity_lease) =
        match build_tso_service(validated_startup.startup(), clock.clone()).await {
            Ok(built) => built,
            Err(error) => {
                let stage = startup_failure_stage(error.as_ref());
                let readiness_reason = startup_bootstrap_failure_reason(error.as_ref());
                record_startup_bootstrap_failure(stage);
                if let Some(TsoError::InstanceIdentityInUse { instance_id }) =
                    error.as_ref().downcast_ref::<TsoError>()
                {
                    record_identity_lease_event("conflict");
                    error!(
                        component = "identity_lease",
                        event = "acquire_failed",
                        result = "failure",
                        reason = %error,
                        readiness_reason = readiness_reason_label(readiness_reason),
                        instance_id,
                        advertise_endpoint = %config.advertise_endpoint,
                        worker_id = %config.worker_id
                    );
                } else if let Some(TsoError::ClusterContractMismatch {
                    cluster_contract_id,
                    local_contract_id,
                    cluster_writer_build_version,
                    cluster_writer_build_commit,
                }) = error.as_ref().downcast_ref::<TsoError>()
                {
                    error!(
                        component = "startup",
                        event = "cluster_contract_mismatch",
                        result = "failure",
                        reason = %error,
                        readiness_reason = readiness_reason_label(readiness_reason),
                        startup_stage = stage,
                        metadata_kind = %startup.metadata_kind(),
                        worker_id = %config.worker_id,
                        instance_id = %config.effective_instance_id(),
                        advertise_endpoint = %config.advertise_endpoint,
                        cluster_contract_id,
                        local_contract_id,
                        cluster_writer_build_version,
                        cluster_writer_build_commit,
                    );
                } else {
                    error!(
                        component = "startup",
                        event = "metadata_probe_result",
                        result = "failure",
                        reason = %error,
                        readiness_reason = readiness_reason_label(readiness_reason),
                        startup_stage = stage,
                        metadata_kind = %startup.metadata_kind(),
                        worker_id = %config.worker_id,
                        instance_id = %config.effective_instance_id(),
                        advertise_endpoint = %config.advertise_endpoint
                    );
                }
                return Err(error);
            }
        };

    let health_status = HealthStatusHandle::serving(&service.health());
    service.set_worker_readiness_sink(Arc::new(StartupWorkerReadinessSink::new(
        ready.clone(),
        startup_complete.clone(),
        health_status.clone(),
        config.worker_id.clone(),
        config.effective_instance_id().to_string(),
        config.advertise_endpoint.clone(),
    )));
    let mut identity_lease = identity_lease;
    let mut serving_gate = StartupServingGate::default();
    let (metrics_listener, metrics_tls_acceptor) = bind_metrics_listener(config).await?;
    let metrics_addr = metrics_listener.local_addr()?;
    info!(
        component = "startup",
        event = "listener_bound",
        result = "success",
        reason = "bind_complete",
        transport = CriticalStartupListener::Admin.label(),
        listen_addr = %metrics_addr,
        bind_addr = %metrics_addr,
        metadata_kind = %startup.metadata_kind(),
        worker_id = %config.worker_id,
        instance_id = %config.effective_instance_id(),
        advertise_endpoint = %config.advertise_endpoint
    );
    serving_gate.mark_listener_bound(CriticalStartupListener::Admin);

    let grpc_listener = bind_grpc_listener(config).await?;
    let grpc_addr = grpc_listener.local_addr()?;
    info!(
        component = "startup",
        event = "listener_bound",
        result = "success",
        reason = "bind_complete",
        transport = CriticalStartupListener::Grpc.label(),
        listen_addr = %grpc_addr,
        bind_addr = %grpc_addr,
        metadata_kind = %startup.metadata_kind(),
        worker_id = %config.worker_id,
        instance_id = %config.effective_instance_id(),
        advertise_endpoint = %config.advertise_endpoint
    );
    serving_gate.mark_listener_bound(CriticalStartupListener::Grpc);

    let route_service = build_route_service(config, &service);
    let timestamp_service = build_timestamp_service(config, &service);
    let control_service = build_control_service(config, &service, health_status.clone());
    let timeline_status_service = build_timeline_status_service(config, &service);

    let metrics_server = {
        let ready = ready.clone();
        let shutdown_rx = shutdown_rx.clone();
        async move {
            serve_metrics_listener(metrics_listener, metrics_tls_acceptor, ready, shutdown_rx).await
        }
    };

    let grpc_server = {
        let shutdown_rx = shutdown_rx.clone();
        async move {
            build_grpc_server(config)?
                .add_service(route_service)
                .add_service(timestamp_service)
                .add_service(control_service)
                .add_service(timeline_status_service)
                .serve_with_incoming_shutdown(
                    grpc_listener_stream(grpc_listener),
                    wait_for_shutdown_signal(shutdown_rx),
                )
                .await
                .map_err(Into::into)
        }
    };

    startup_complete.store(true, Ordering::Release);

    if serving_gate.is_ready()
        && health_status.readiness_state() == WorkerReadinessState::Ready
        && health_status.readiness_reason() == WorkerReadinessReason::Serving
    {
        set_startup_ready(&ready, true);
        record_worker_readiness_transition(
            "starting",
            health_status.readiness_state(),
            health_status.readiness_reason(),
        );
        info!(
            component = "startup",
            event = "ready_state_changed",
            result = "success",
            action_kind = "worker_readiness",
            action_outcome = "succeeded",
            previous_readiness_state = "starting",
            readiness_state = readiness_state_label(health_status.readiness_state()),
            previous_readiness_reason = "startup_in_progress",
            readiness_reason = readiness_reason_label(health_status.readiness_reason()),
            metadata_kind = %startup.metadata_kind(),
            worker_id = %config.worker_id,
            instance_id = %config.effective_instance_id(),
            advertise_endpoint = %config.advertise_endpoint
        );
    }

    if let Some(identity_lease) = identity_lease.as_ref() {
        spawn_identity_lease_loss_monitor(
            identity_lease.lost_receiver(),
            ready.clone(),
            health_status.clone(),
            shutdown_tx.clone(),
            config.worker_id.clone(),
            config.effective_instance_id().to_string(),
            config.advertise_endpoint.clone(),
        );
    }
    let signal_monitor = spawn_process_signal_monitor(
        ready.clone(),
        health_status.clone(),
        shutdown_tx.clone(),
        config.worker_id.clone(),
        config.effective_instance_id().to_string(),
        config.advertise_endpoint.clone(),
    );

    let servers_result = supervise_critical_servers(
        CriticalServerContext {
            ready: &ready,
            health_status: &health_status,
            shutdown_tx: &shutdown_tx,
            shutdown_rx: shutdown_rx.clone(),
            worker_id: &config.worker_id,
            instance_id: config.effective_instance_id(),
            advertise_endpoint: &config.advertise_endpoint,
        },
        grpc_server,
        metrics_server,
    )
    .await;
    signal_monitor.abort();
    if let Some(identity_lease) = identity_lease.as_mut() {
        identity_lease.shutdown().await;
    }
    service.shutdown().await;
    servers_result?;
    info!(
        component = "shutdown",
        event = "shutdown_completed",
        result = "success",
        action_kind = "shutdown",
        action_outcome = "succeeded",
        readiness_state = readiness_state_label(health_status.readiness_state()),
        readiness_reason = readiness_reason_label(health_status.readiness_reason()),
        metadata_kind = %startup.metadata_kind(),
        worker_id = %config.worker_id,
        instance_id = %config.effective_instance_id(),
        advertise_endpoint = %config.advertise_endpoint
    );

    Ok(())
}
