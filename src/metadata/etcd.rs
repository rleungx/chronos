use async_trait::async_trait;
use etcd_client::{
    Certificate, Client, Compare, CompareOp, ConnectOptions, EventType, GetOptions, Identity,
    TlsOptions, Txn, TxnOp, WatchOptions,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use std::sync::MutexGuard;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

use crate::recovery::record_recovery_event;
use crate::tls::read_required_pem_file;
use crate::{metrics, TimelineRoute, TsoConfig, TsoError};
use futures::future::try_join_all;

use super::{
    identity::{claim_instance_identity, InstanceIdentityLeaseRecord},
    keys,
    types::{
        timeline_route_update_from_routes, RouteUpdateSignal, CURRENT_METADATA_SCHEMA_VERSION,
    },
    GeneratorBatchOp, GeneratorLeaseAuthority, GeneratorRecord, IdentityLeaseAuthority,
    InstanceIdentityLease, RouteUpdateSource, TimelineAuthority, TimelineBatchOp,
    TimelineFilterRecord, TimelineFilterRecordListPage, TimelineRecord, TimelineRecordListPage,
    TimelineRouteRecord,
};

pub struct EtcdMetadataStore {
    client: Client,
    prefix: String,
    route_updates: broadcast::Sender<RouteUpdateSignal>,
    route_watch_shutdown_tx: watch::Sender<bool>,
    route_watch_task: StdMutex<Option<JoinHandle<()>>>,
}

fn load_etcd_client_tls_options(config: &TsoConfig) -> Result<Option<TlsOptions>, TsoError> {
    let Some(paths) = config
        .etcd_tls_paths()
        .map_err(|error| TsoError::Internal(error.to_string()))?
    else {
        return Ok(None);
    };

    let ca = read_required_pem_file(paths.ca_file, "CHRONOS_ETCD_CA_FILE")?;
    let cert = read_required_pem_file(paths.cert_file, "CHRONOS_ETCD_CERT_FILE")?;
    let key = read_required_pem_file(paths.key_file, "CHRONOS_ETCD_KEY_FILE")?;

    Ok(Some(
        TlsOptions::new()
            .ca_certificate(Certificate::from_pem(ca))
            .identity(Identity::from_pem(cert, key)),
    ))
}

fn build_etcd_connect_options(
    config: &TsoConfig,
    endpoints: &[String],
) -> Result<Option<ConnectOptions>, TsoError> {
    let mut endpoint_contract = config.clone();
    endpoint_contract.etcd_endpoints = endpoints.to_vec();
    endpoint_contract
        .validate_etcd_endpoint_contract()
        .map_err(|error| TsoError::Internal(error.to_string()))?;

    let mut options = ConnectOptions::new();
    let mut configured = false;

    if let Some(timeout_ms) = config.etcd_timeout_ms {
        let timeout = Duration::from_millis(timeout_ms);
        options = options.with_timeout(timeout).with_connect_timeout(timeout);
        configured = true;
    }

    if let Some(tls) = load_etcd_client_tls_options(config)? {
        options = options.with_tls(tls);
        configured = true;
    }

    Ok(configured.then_some(options))
}

#[derive(Clone, Copy)]
struct JsonTxnContext {
    op_label: &'static str,
    serialize_context: &'static str,
    txn_context: &'static str,
    invalid_response_context: &'static str,
}

#[derive(Deserialize)]
struct RouteOnlyTimelineRecord {
    #[serde(default = "default_schema_version")]
    _schema_version: u32,
    route: TimelineRoute,
}

#[derive(Deserialize)]
struct TimelineFilterOnlyRecord {
    #[serde(default = "default_schema_version")]
    schema_version: u32,
    route: TimelineRoute,
    #[serde(default = "default_timeline_state")]
    state: crate::TimelineLifecycleState,
}

fn default_schema_version() -> u32 {
    CURRENT_METADATA_SCHEMA_VERSION
}

fn default_timeline_state() -> crate::TimelineLifecycleState {
    crate::TimelineLifecycleState::Active
}

fn parse_prev_route(value: &[u8]) -> Option<TimelineRoute> {
    serde_json::from_slice::<RouteOnlyTimelineRecord>(value)
        .ok()
        .map(|record| record.route)
}

fn parse_timeline_filter_record(value: &[u8]) -> Result<TimelineFilterRecord, serde_json::Error> {
    serde_json::from_slice::<TimelineFilterOnlyRecord>(value).map(|record| TimelineFilterRecord {
        schema_version: record.schema_version,
        route: record.route,
        state: record.state,
    })
}

fn route_update_for_watch_event(
    prev_value: Option<&[u8]>,
    next_route: &TimelineRoute,
) -> Option<TimelineRoute> {
    let previous_route = prev_value.and_then(parse_prev_route);
    timeline_route_update_from_routes(previous_route.as_ref(), next_route)
}

fn prefix_range_end(prefix: &str) -> Vec<u8> {
    let mut end = prefix.as_bytes().to_vec();
    for index in (0..end.len()).rev() {
        if end[index] < u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            return end;
        }
    }
    vec![0]
}

impl EtcdMetadataStore {
    async fn probe_metadata_runtime(&self) -> Result<(), TsoError> {
        self.load_timeline_route("__chronos_startup_probe__")
            .await?;
        self.load_generator(0).await?;
        Ok(())
    }

    fn route_watch_task_lock(&self) -> MutexGuard<'_, Option<JoinHandle<()>>> {
        match self.route_watch_task.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event("metadata", "etcd_route_watch_lock", "mutex_poisoned");
                poisoned.into_inner()
            }
        }
    }

    /// Official config-driven entrypoint for production/library callers.
    ///
    /// This path applies the shared etcd endpoint contract plus config-driven timeout and mTLS
    /// wiring before dialing etcd, then performs a minimal metadata probe before returning.
    pub async fn from_config(config: &TsoConfig, prefix: String) -> Result<Self, TsoError> {
        Self::from_config_endpoints(config, config.etcd_endpoints.clone(), prefix).await
    }

    /// Startup entrypoint when typed startup metadata owns the etcd bootstrap tuple.
    ///
    /// This keeps config-derived timeout and mTLS wiring while making the dial target come from
    /// the typed startup authority rather than a mirrored config field. Like `from_config`, this
    /// verifies basic metadata RPC reachability before returning.
    pub async fn from_config_with_endpoints(
        config: &TsoConfig,
        endpoints: Vec<String>,
        prefix: String,
    ) -> Result<Self, TsoError> {
        Self::from_config_endpoints(config, endpoints, prefix).await
    }

    async fn from_config_endpoints(
        config: &TsoConfig,
        endpoints: Vec<String>,
        prefix: String,
    ) -> Result<Self, TsoError> {
        config
            .validate_authoritative_metadata_runtime_contract()
            .map_err(|error| TsoError::Internal(error.to_string()))?;
        config
            .validate_authoritative_metadata_store_contract(&endpoints, &prefix)
            .map_err(|error| TsoError::Internal(error.to_string()))?;
        let options = build_etcd_connect_options(config, &endpoints)?;
        let store = Self::connect_with_options(endpoints, prefix, options).await?;
        if let Err(error) = store.probe_metadata_runtime().await {
            store.shutdown_route_watch().await;
            return Err(error);
        }
        Ok(store)
    }

    /// Raw etcd entrypoint for tests or explicitly unchecked callers.
    ///
    /// This bypasses config-derived endpoint validation, timeout wiring, and mTLS file loading.
    pub async fn from_raw_endpoints_unchecked(
        endpoints: Vec<String>,
        prefix: String,
    ) -> Result<Self, TsoError> {
        Self::connect_with_options(endpoints, prefix, None).await
    }

    async fn connect_with_options(
        endpoints: Vec<String>,
        prefix: String,
        options: Option<ConnectOptions>,
    ) -> Result<Self, TsoError> {
        let client = Client::connect(endpoints, options)
            .await
            .map_err(|error| TsoError::Internal(format!("Etcd connection failed: {}", error)))?;
        let (route_updates, _) = broadcast::channel(1024);
        let (route_watch_shutdown_tx, _) = watch::channel(false);
        let store = Self {
            client,
            prefix,
            route_updates,
            route_watch_shutdown_tx,
            route_watch_task: StdMutex::new(None),
        };
        store.spawn_route_watch_loop();
        Ok(store)
    }

    fn timeline_key(&self, timeline_key: &str) -> String {
        keys::timeline_key(&self.prefix, timeline_key)
    }

    fn generator_key(&self, generator_id: u32) -> String {
        keys::generator_key(&self.prefix, generator_id)
    }

    fn route_prefix(&self) -> String {
        keys::route_prefix(&self.prefix)
    }

    fn instance_identity_key(&self, instance_id: &str) -> String {
        keys::instance_identity_key(&self.prefix, instance_id)
    }

    async fn acquire_identity_lease_internal(
        &self,
        instance_id: &str,
        worker_id: &str,
        advertise_endpoint: &str,
        ttl: Duration,
    ) -> Result<InstanceIdentityLease, TsoError> {
        info!(
            component = "identity_lease",
            event = "acquire_started",
            result = "success",
            reason = "grant_requested",
            instance_id,
            worker_id,
            advertise_endpoint
        );
        let ttl_seconds = ttl.as_secs().max(1) as i64;
        let key = self.instance_identity_key(instance_id);
        let record = InstanceIdentityLeaseRecord {
            instance_id: instance_id.to_owned(),
            worker_id: worker_id.to_owned(),
            advertise_endpoint: advertise_endpoint.to_owned(),
        };

        let mut client = self.client.clone();
        let revoke_client = self.client.clone();
        let lease_id = client
            .lease_grant(ttl_seconds, None)
            .await
            .map_err(|error| TsoError::Internal(format!("Etcd lease grant failed: {}", error)))?
            .id();

        let value = Self::serialize_record(
            &record,
            "identity_lease_acquire",
            "Identity lease serialization",
        )?;
        claim_instance_identity(&mut client, key.as_bytes(), value, lease_id, instance_id).await?;

        let (mut keeper, mut stream) =
            client.lease_keep_alive(lease_id).await.map_err(|error| {
                TsoError::Internal(format!("Etcd identity lease keepalive failed: {}", error))
            })?;
        let (lost_tx, lost_rx) = watch::channel(false);
        let heartbeat_interval = Duration::from_secs((ttl_seconds.max(3) / 3) as u64);
        let lease_instance_id = instance_id.to_owned();
        let lease_worker_id = worker_id.to_owned();
        let lease_advertise_endpoint = advertise_endpoint.to_owned();
        let keep_alive_task = tokio::spawn(async move {
            loop {
                if keeper.keep_alive().await.is_err() {
                    error!(
                        component = "identity_lease",
                        event = "keepalive_lost",
                        result = "failure",
                        reason = "keepalive_send_failed",
                        lease_id,
                        instance_id = lease_instance_id,
                        worker_id = lease_worker_id,
                        advertise_endpoint = lease_advertise_endpoint
                    );
                    let _ = lost_tx.send(true);
                    break;
                }

                match stream.message().await {
                    Ok(Some(response)) if response.ttl() > 0 => {
                        sleep(heartbeat_interval).await;
                    }
                    _ => {
                        error!(
                            component = "identity_lease",
                            event = "keepalive_lost",
                            result = "failure",
                            reason = "keepalive_stream_closed",
                            lease_id,
                            instance_id = lease_instance_id,
                            worker_id = lease_worker_id,
                            advertise_endpoint = lease_advertise_endpoint
                        );
                        let _ = lost_tx.send(true);
                        break;
                    }
                }
            }
        });

        info!(
            component = "identity_lease",
            event = "acquire_succeeded",
            result = "success",
            reason = "lease_acquired",
            lease_id,
            instance_id,
            worker_id,
            advertise_endpoint
        );

        Ok(InstanceIdentityLease::new(
            lost_rx,
            keep_alive_task,
            revoke_client,
            lease_id,
        ))
    }

    fn spawn_route_watch_loop(&self) {
        let mut watch_client = self.client.clone();
        let route_prefix = self.route_prefix();
        let route_updates = self.route_updates.clone();
        let mut shutdown_rx = self.route_watch_shutdown_tx.subscribe();

        let route_watch_task = tokio::spawn(async move {
            loop {
                let watch_result = tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    result = watch_client.watch(
                        route_prefix.clone(),
                        Some(
                            WatchOptions::new()
                                .with_prefix()
                                .with_prev_key()
                                .with_progress_notify(),
                        ),
                    ) => result,
                };

                let (_watcher, mut watch_stream) = match watch_result {
                    Ok(stream) => {
                        info!(
                            component = "route_watch",
                            event = "watch_started",
                            result = "success",
                            reason = "watch_connected"
                        );
                        stream
                    }
                    Err(_) => {
                        let _ = route_updates.send(RouteUpdateSignal::Reset);
                        warn!(
                            component = "route_watch",
                            event = "watch_restarted",
                            result = "degraded",
                            reason = "watch_connect_failed"
                        );
                        tokio::select! {
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            _ = sleep(Duration::from_millis(500)) => {}
                        }
                        continue;
                    }
                };

                let mut restart_watch = false;
                loop {
                    let watch_message = tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                return;
                            }
                            continue;
                        }
                        message = watch_stream.message() => message,
                    };

                    match watch_message {
                        Ok(Some(response)) => {
                            for event in response.events() {
                                if event.event_type() != EventType::Put {
                                    continue;
                                }
                                let Some(key_value) = event.kv() else {
                                    metrics::TSO_METADATA_ERRORS_TOTAL
                                        .with_label_values(&["route_watch_event_missing_kv"])
                                        .inc();
                                    warn!(
                                        component = "route_watch",
                                        event = "watch_restarted",
                                        result = "degraded",
                                        reason = "watch_event_missing_kv"
                                    );
                                    let _ = route_updates.send(RouteUpdateSignal::Reset);
                                    continue;
                                };
                                let Ok(record) = serde_json::from_slice::<RouteOnlyTimelineRecord>(
                                    key_value.value(),
                                ) else {
                                    metrics::TSO_METADATA_ERRORS_TOTAL
                                        .with_label_values(&["route_watch_event_decode"])
                                        .inc();
                                    warn!(
                                        component = "route_watch",
                                        event = "watch_restarted",
                                        result = "degraded",
                                        reason = "watch_event_decode_failed"
                                    );
                                    let _ = route_updates.send(RouteUpdateSignal::Reset);
                                    continue;
                                };
                                if let Some(route_update) = route_update_for_watch_event(
                                    event.prev_kv().map(|prev_key_value| prev_key_value.value()),
                                    &record.route,
                                ) {
                                    info!(
                                        component = "route_watch",
                                        event = "watch_event_applied",
                                        result = "success",
                                        reason = "route_changed",
                                        timeline_key = %record.route.timeline_key,
                                        generator_id = record.route.generator_id,
                                        epoch = record.route.epoch,
                                        route_version = record.route.route_version,
                                        owner_endpoint = %record.route.owner_worker_endpoint
                                    );
                                    let _ =
                                        route_updates.send(RouteUpdateSignal::Route(route_update));
                                }
                            }
                        }
                        Ok(None) => {
                            let _ = route_updates.send(RouteUpdateSignal::Reset);
                            break;
                        }
                        Err(_) => {
                            let _ = route_updates.send(RouteUpdateSignal::Reset);
                            warn!(
                                component = "route_watch",
                                event = "watch_restarted",
                                result = "degraded",
                                reason = "watch_stream_error"
                            );
                            restart_watch = true;
                            break;
                        }
                    }
                }

                if !restart_watch {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = sleep(Duration::from_millis(100)) => {}
                    }
                } else {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = sleep(Duration::from_millis(500)) => {}
                    }
                }
            }
        });

        *self.route_watch_task_lock() = Some(route_watch_task);
    }

    pub(crate) fn request_route_watch_shutdown(&self) {
        info!(
            component = "shutdown",
            event = "background_stop_started",
            result = "success",
            reason = "route_watch_shutdown_requested"
        );
        let _ = self.route_watch_shutdown_tx.send(true);
    }

    pub(crate) async fn shutdown_route_watch(&self) {
        self.request_route_watch_shutdown();
        let route_watch_task = self.route_watch_task_lock().take();
        if let Some(route_watch_task) = route_watch_task {
            let _ = route_watch_task.await;
        }
        info!(
            component = "shutdown",
            event = "background_stop_completed",
            result = "success",
            reason = "route_watch_shutdown_complete"
        );
    }

    async fn get_json_record<T>(
        &self,
        key: String,
        op_label: &'static str,
        deserialize_context: &'static str,
    ) -> Result<Option<(T, u64)>, TsoError>
    where
        T: DeserializeOwned,
    {
        let mut client = self.client.clone();
        let response = client.get(key, None).await.map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[op_label])
                .inc();
            TsoError::Internal(format!("Etcd {} failed: {}", op_label, error))
        })?;

        if let Some(kv) = response.kvs().first() {
            let record = serde_json::from_slice(kv.value()).map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[op_label])
                    .inc();
                TsoError::Internal(format!("{} failed: {}", deserialize_context, error))
            })?;
            Ok(Some((record, kv.mod_revision() as u64)))
        } else {
            Ok(None)
        }
    }

    async fn get_json_records_with_prefix<T>(
        &self,
        prefix: String,
        op_label: &'static str,
        deserialize_context: &'static str,
    ) -> Result<Vec<T>, TsoError>
    where
        T: DeserializeOwned,
    {
        let mut client = self.client.clone();
        let response = client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[op_label])
                    .inc();
                TsoError::Internal(format!("Etcd {} failed: {}", op_label, error))
            })?;

        let mut records = Vec::with_capacity(response.kvs().len());
        for kv in response.kvs() {
            let record = serde_json::from_slice(kv.value()).map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[op_label])
                    .inc();
                TsoError::Internal(format!("{} failed: {}", deserialize_context, error))
            })?;
            records.push(record);
        }
        Ok(records)
    }

    fn serialize_record<T>(
        record: &T,
        op_label: &'static str,
        serialize_context: &'static str,
    ) -> Result<Vec<u8>, TsoError>
    where
        T: Serialize,
    {
        serde_json::to_vec(record).map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[op_label])
                .inc();
            TsoError::Internal(format!("{} failed: {}", serialize_context, error))
        })
    }

    fn extract_put_revision(
        response: etcd_client::TxnResponse,
        op_label: &'static str,
        invalid_response_context: &'static str,
    ) -> Result<u64, TsoError> {
        let op_responses = response.op_responses();
        let put_response = op_responses
            .first()
            .and_then(|operation| match operation {
                etcd_client::TxnOpResponse::Put(put_response) => Some(put_response),
                _ => None,
            })
            .ok_or_else(|| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[op_label])
                    .inc();
                TsoError::Internal(invalid_response_context.to_string())
            })?;

        let header = put_response.header().ok_or_else(|| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[op_label])
                .inc();
            TsoError::Internal(invalid_response_context.to_string())
        })?;

        Ok(header.revision() as u64)
    }

    async fn create_json_record<T>(
        &self,
        key: String,
        record: &T,
        context: JsonTxnContext,
    ) -> Result<u64, TsoError>
    where
        T: Serialize,
    {
        let mut client = self.client.clone();
        let value = Self::serialize_record(record, context.op_label, context.serialize_context)?;
        let txn = Txn::new()
            .when(vec![Compare::mod_revision(
                key.as_bytes(),
                CompareOp::Equal,
                0,
            )])
            .and_then(vec![TxnOp::put(key.as_bytes(), value, None)]);

        let response = client.txn(txn).await.map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
            TsoError::Internal(format!("{}: {}", context.txn_context, error))
        })?;

        if !response.succeeded() {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
            return Err(TsoError::MetadataAlreadyExists);
        }

        Self::extract_put_revision(response, context.op_label, context.invalid_response_context)
    }

    async fn cas_json_record<T>(
        &self,
        key: String,
        previous_revision: u64,
        record: &T,
        context: JsonTxnContext,
    ) -> Result<u64, TsoError>
    where
        T: Serialize,
    {
        let mut client = self.client.clone();
        let value = Self::serialize_record(record, context.op_label, context.serialize_context)?;
        let txn = Txn::new()
            .when(vec![Compare::mod_revision(
                key.as_bytes(),
                CompareOp::Equal,
                previous_revision as i64,
            )])
            .and_then(vec![TxnOp::put(key.as_bytes(), value, None)]);

        let response = client.txn(txn).await.map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
            TsoError::Internal(format!("{}: {}", context.txn_context, error))
        })?;

        if !response.succeeded() {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
            return Err(TsoError::CasFailed);
        }

        Self::extract_put_revision(response, context.op_label, context.invalid_response_context)
    }

    async fn cas_json_records_batch<T>(
        &self,
        operations: Vec<(String, u64, T)>,
        context: JsonTxnContext,
    ) -> Result<Vec<u64>, TsoError>
    where
        T: Serialize,
    {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        let mut client = self.client.clone();
        let mut compares = Vec::with_capacity(operations.len());
        let mut puts = Vec::with_capacity(operations.len());
        for (key, previous_revision, record) in &operations {
            compares.push(Compare::mod_revision(
                key.as_bytes(),
                CompareOp::Equal,
                *previous_revision as i64,
            ));
            puts.push(TxnOp::put(
                key.as_bytes(),
                Self::serialize_record(record, context.op_label, context.serialize_context)?,
                None,
            ));
        }

        let txn = Txn::new().when(compares).and_then(puts);
        let response = client.txn(txn).await.map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
            TsoError::Internal(format!("{}: {}", context.txn_context, error))
        })?;

        if !response.succeeded() {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
            return Err(TsoError::CasFailed);
        }

        let revision = response
            .header()
            .map(|header| header.revision() as u64)
            .ok_or_else(|| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[context.op_label])
                    .inc();
                TsoError::Internal(context.invalid_response_context.to_string())
            })?;
        Ok(vec![revision; operations.len()])
    }
}

#[async_trait]
impl TimelineAuthority for EtcdMetadataStore {
    async fn load_timeline(
        &self,
        timeline_key: &str,
    ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get"])
            .start_timer();
        let result: Option<(TimelineRecord, u64)> = self
            .get_json_record(
                self.timeline_key(timeline_key),
                "get",
                "Record deserialization",
            )
            .await?;
        result
            .map(|(record, revision)| {
                record.validate_schema_version()?;
                Ok((record, revision))
            })
            .transpose()
    }

    async fn load_timeline_route(
        &self,
        timeline_key: &str,
    ) -> Result<Option<(TimelineRouteRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get_route"])
            .start_timer();
        let result: Option<(TimelineRouteRecord, u64)> = self
            .get_json_record(
                self.timeline_key(timeline_key),
                "get_route",
                "Route record deserialization",
            )
            .await?;
        result
            .map(|(record, revision)| {
                record.validate_schema_version()?;
                Ok((record, revision))
            })
            .transpose()
    }

    async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["list"])
            .start_timer();
        let records: Vec<TimelineRecord> = self
            .get_json_records_with_prefix(
                self.route_prefix(),
                "list",
                "Timeline record list deserialization",
            )
            .await?;
        records
            .into_iter()
            .map(|record| {
                record.validate_schema_version()?;
                Ok(record)
            })
            .collect()
    }

    async fn list_timelines_page(
        &self,
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<TimelineRecordListPage, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["list_page"])
            .start_timer();
        if limit == 0 {
            return Ok(TimelineRecordListPage {
                records: Vec::new(),
                next_start_after_timeline_key: None,
            });
        }

        let mut client = self.client.clone();
        let route_prefix = self.route_prefix();
        let start_key = start_after_timeline_key
            .map(|timeline_key| self.timeline_key(timeline_key))
            .unwrap_or_else(|| route_prefix.clone());
        let fetch_limit = limit
            .saturating_add(1)
            .saturating_add(usize::from(start_after_timeline_key.is_some()))
            .min(i64::MAX as usize) as i64;
        let response = client
            .get(
                start_key.clone(),
                Some(
                    GetOptions::new()
                        .with_range(prefix_range_end(&route_prefix))
                        .with_limit(fetch_limit),
                ),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["list_page"])
                    .inc();
                TsoError::Internal(format!("Etcd list_page failed: {}", error))
            })?;

        let mut records = Vec::with_capacity(response.kvs().len().min(limit + 1));
        for kv in response.kvs() {
            if start_after_timeline_key.is_some() && kv.key() == start_key.as_bytes() {
                continue;
            }

            let record: TimelineRecord = serde_json::from_slice(kv.value()).map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["list_page"])
                    .inc();
                TsoError::Internal(format!(
                    "Timeline record page deserialization failed: {}",
                    error
                ))
            })?;
            record.validate_schema_version()?;
            records.push(record);
        }
        records
            .sort_unstable_by(|left, right| left.route.timeline_key.cmp(&right.route.timeline_key));

        let next_start_after_timeline_key =
            (records.len() > limit).then(|| records[limit - 1].route.timeline_key.clone());
        records.truncate(limit);

        Ok(TimelineRecordListPage {
            records,
            next_start_after_timeline_key,
        })
    }

    async fn list_timeline_filters_page(
        &self,
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<TimelineFilterRecordListPage, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["list_page_filters"])
            .start_timer();
        if limit == 0 {
            return Ok(TimelineFilterRecordListPage {
                records: Vec::new(),
                next_start_after_timeline_key: None,
            });
        }

        let mut client = self.client.clone();
        let route_prefix = self.route_prefix();
        let start_key = start_after_timeline_key
            .map(|timeline_key| self.timeline_key(timeline_key))
            .unwrap_or_else(|| route_prefix.clone());
        let fetch_limit = limit
            .saturating_add(1)
            .saturating_add(usize::from(start_after_timeline_key.is_some()))
            .min(i64::MAX as usize) as i64;
        let response = client
            .get(
                start_key.clone(),
                Some(
                    GetOptions::new()
                        .with_range(prefix_range_end(&route_prefix))
                        .with_limit(fetch_limit),
                ),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["list_page_filters"])
                    .inc();
                TsoError::Internal(format!("Etcd list_page_filters failed: {}", error))
            })?;

        let mut records = Vec::with_capacity(response.kvs().len().min(limit + 1));
        for kv in response.kvs() {
            if start_after_timeline_key.is_some() && kv.key() == start_key.as_bytes() {
                continue;
            }

            let record = parse_timeline_filter_record(kv.value()).map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["list_page_filters"])
                    .inc();
                TsoError::Internal(format!(
                    "Timeline filter record page deserialization failed: {}",
                    error
                ))
            })?;
            record.validate_schema_version()?;
            records.push(record);
        }
        records
            .sort_unstable_by(|left, right| left.route.timeline_key.cmp(&right.route.timeline_key));

        let next_start_after_timeline_key =
            (records.len() > limit).then(|| records[limit - 1].route.timeline_key.clone());
        records.truncate(limit);

        Ok(TimelineFilterRecordListPage {
            records,
            next_start_after_timeline_key,
        })
    }

    async fn create_timeline(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["create"])
            .start_timer();
        record.validate_schema_version()?;
        let stamped = record.stamped_for_persistence();
        self.create_json_record(
            self.timeline_key(timeline_key),
            &stamped,
            JsonTxnContext {
                op_label: "create",
                serialize_context: "Record serialization",
                txn_context: "Etcd txn failed",
                invalid_response_context: "invalid txn response",
            },
        )
        .await
    }

    async fn compare_exchange_timeline(
        &self,
        timeline_key: &str,
        expected_revision: u64,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas"])
            .start_timer();
        record.validate_schema_version()?;
        let stamped = record.stamped_for_persistence();
        self.cas_json_record(
            self.timeline_key(timeline_key),
            expected_revision,
            &stamped,
            JsonTxnContext {
                op_label: "cas",
                serialize_context: "Record serialization",
                txn_context: "Etcd txn failed",
                invalid_response_context: "invalid txn response",
            },
        )
        .await
    }

    async fn compare_exchange_timelines(
        &self,
        operations: &[TimelineBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_batch"])
            .start_timer();
        for operation in operations {
            operation.record.validate_schema_version()?;
        }
        self.cas_json_records_batch(
            operations
                .iter()
                .map(|operation| {
                    (
                        self.timeline_key(operation.timeline_key.as_str()),
                        operation.previous_revision,
                        operation.record.stamped_for_persistence(),
                    )
                })
                .collect(),
            JsonTxnContext {
                op_label: "cas_batch",
                serialize_context: "Batch record serialization",
                txn_context: "Etcd batch txn failed",
                invalid_response_context: "invalid batch txn response",
            },
        )
        .await
    }
}

#[async_trait]
impl GeneratorLeaseAuthority for EtcdMetadataStore {
    async fn load_generator(
        &self,
        generator_id: u32,
    ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get_generator"])
            .start_timer();
        let result: Option<(GeneratorRecord, u64)> = self
            .get_json_record(
                self.generator_key(generator_id),
                "get_generator",
                "Generator record deserialization",
            )
            .await?;
        result
            .map(|(record, revision)| {
                record.validate_schema_version()?;
                Ok((record, revision))
            })
            .transpose()
    }

    async fn load_generators(
        &self,
        generator_ids: &[u32],
    ) -> Result<HashMap<u32, Option<GeneratorRecord>>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get_generator_batch"])
            .start_timer();
        let results = try_join_all(
            generator_ids
                .iter()
                .copied()
                .map(|generator_id| async move {
                    let record = self
                        .load_generator(generator_id)
                        .await?
                        .map(|(record, _)| record);
                    Ok::<(u32, Option<GeneratorRecord>), TsoError>((generator_id, record))
                }),
        )
        .await?;
        Ok(results.into_iter().collect())
    }

    async fn create_generator(
        &self,
        generator_id: u32,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["create_generator"])
            .start_timer();
        record.validate_schema_version()?;
        let stamped = record.stamped_for_persistence();
        self.create_json_record(
            self.generator_key(generator_id),
            &stamped,
            JsonTxnContext {
                op_label: "create_generator",
                serialize_context: "Generator record serialization",
                txn_context: "Etcd generator txn failed",
                invalid_response_context: "invalid generator txn response",
            },
        )
        .await
    }

    async fn compare_exchange_generator(
        &self,
        generator_id: u32,
        expected_revision: u64,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_generator"])
            .start_timer();
        record.validate_schema_version()?;
        let stamped = record.stamped_for_persistence();
        self.cas_json_record(
            self.generator_key(generator_id),
            expected_revision,
            &stamped,
            JsonTxnContext {
                op_label: "cas_generator",
                serialize_context: "Generator record serialization",
                txn_context: "Etcd generator txn failed",
                invalid_response_context: "invalid generator txn response",
            },
        )
        .await
    }

    async fn compare_exchange_generators(
        &self,
        operations: &[GeneratorBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_generator_batch"])
            .start_timer();
        for operation in operations {
            operation.record.validate_schema_version()?;
        }
        self.cas_json_records_batch(
            operations
                .iter()
                .map(|operation| {
                    (
                        self.generator_key(operation.generator_id),
                        operation.previous_revision,
                        operation.record.stamped_for_persistence(),
                    )
                })
                .collect(),
            JsonTxnContext {
                op_label: "cas_generator_batch",
                serialize_context: "Batch generator record serialization",
                txn_context: "Etcd generator batch txn failed",
                invalid_response_context: "invalid generator batch txn response",
            },
        )
        .await
    }
}

impl RouteUpdateSource for EtcdMetadataStore {
    fn subscribe_route_updates(&self) -> broadcast::Receiver<RouteUpdateSignal> {
        self.route_updates.subscribe()
    }
}

#[async_trait]
impl super::ControlPlaneStore for EtcdMetadataStore {
    async fn shutdown(&self) {
        self.shutdown_route_watch().await;
    }
}

#[async_trait]
impl IdentityLeaseAuthority for EtcdMetadataStore {
    async fn acquire_instance_identity_lease(
        &self,
        instance_id: &str,
        worker_id: &str,
        advertise_endpoint: &str,
        ttl: Duration,
    ) -> Result<InstanceIdentityLease, TsoError> {
        self.acquire_identity_lease_internal(instance_id, worker_id, advertise_endpoint, ttl)
            .await
    }
}

impl Drop for EtcdMetadataStore {
    fn drop(&mut self) {
        self.request_route_watch_shutdown();
        let route_watch_task = match self.route_watch_task.get_mut() {
            Ok(route_watch_task) => route_watch_task.take(),
            Err(poisoned) => {
                record_recovery_event("metadata", "etcd_route_watch_drop", "mutex_poisoned");
                poisoned.into_inner().take()
            }
        };
        if let Some(route_watch_task) = route_watch_task {
            route_watch_task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ResourceTier;

    use super::EtcdMetadataStore;
    use super::{
        parse_prev_route, parse_timeline_filter_record, route_update_for_watch_event,
        RouteOnlyTimelineRecord, TimelineRoute, TimelineRouteRecord,
    };
    use crate::metadata::types::CURRENT_METADATA_SCHEMA_VERSION;
    use tokio::time::Duration;

    fn sample_route(generator_id: u32, route_version: u64) -> TimelineRoute {
        TimelineRoute {
            timeline_key: "timeline-a".into(),
            generator_id,
            epoch: 1,
            route_version,
            resource_tier: ResourceTier::Shared,
            owner_worker_endpoint: "worker-a:50051".into(),
        }
    }

    #[test]
    fn parse_prev_route_accepts_route_only_payload() {
        let route = sample_route(7, 1);
        let payload = serde_json::json!({
            "route": route,
        });

        assert_eq!(
            parse_prev_route(payload.to_string().as_bytes()),
            Some(sample_route(7, 1))
        );
    }

    #[test]
    fn route_update_for_watch_event_skips_same_route_when_prev_route_is_known() {
        let route = sample_route(7, 1);
        let next = route.clone();
        let payload = serde_json::json!({ "route": route });

        assert_eq!(
            route_update_for_watch_event(Some(payload.to_string().as_bytes()), &next),
            None
        );
    }

    #[test]
    fn route_update_for_watch_event_preserves_unknown_prev_as_changed() {
        let next = sample_route(7, 1);

        assert_eq!(
            route_update_for_watch_event(Some(br#"{not-json}"#), &next),
            Some(next.clone())
        );
        assert_eq!(route_update_for_watch_event(None, &next), Some(next));
    }

    #[test]
    fn parse_timeline_filter_record_accepts_partial_payload() {
        let payload = serde_json::json!({
            "schema_version": 1,
            "route": sample_route(7, 2),
            "state": "Recovering"
        });

        let record = parse_timeline_filter_record(payload.to_string().as_bytes()).unwrap();
        assert_eq!(record.route, sample_route(7, 2));
        assert_eq!(record.state, crate::TimelineLifecycleState::Recovering);
    }

    #[test]
    fn timeline_route_record_deserializes_from_full_timeline_payload() {
        let payload = serde_json::json!({
            "schema_version": 1,
            "route": sample_route(7, 3),
            "state": "Recovering",
            "recovery_floor_tso": 42,
            "issued_upper_bound": 88,
            "last_graceful_issued": 77,
            "lease_expire_at_ms": 66,
            "updated_at_ms": 10
        });

        let record: TimelineRouteRecord =
            serde_json::from_slice(payload.to_string().as_bytes()).unwrap();
        assert_eq!(record.route, sample_route(7, 3));
        assert!(record.validate_schema_version().is_ok());
    }

    #[test]
    fn route_watch_projection_accepts_newer_schema_version() {
        let next = sample_route(7, 4);
        let payload = serde_json::json!({
            "schema_version": CURRENT_METADATA_SCHEMA_VERSION + 1,
            "route": next,
            "state": "Active",
            "issued_upper_bound": 88,
        });

        let record: RouteOnlyTimelineRecord =
            serde_json::from_slice(payload.to_string().as_bytes()).unwrap();
        assert_eq!(record.route, sample_route(7, 4));
    }

    #[test]
    fn parse_timeline_filter_record_accepts_newer_schema_version() {
        let payload = serde_json::json!({
            "schema_version": CURRENT_METADATA_SCHEMA_VERSION + 1,
            "route": sample_route(7, 5),
            "state": "Recovering",
            "issued_upper_bound": 88,
        });

        let record = parse_timeline_filter_record(payload.to_string().as_bytes()).unwrap();
        assert_eq!(record.schema_version, CURRENT_METADATA_SCHEMA_VERSION + 1);
        assert!(record.validate_schema_version().is_ok());
    }

    #[tokio::test]
    #[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
    async fn etcd_route_watch_shutdown_completes() {
        let endpoints = std::env::var("CHRONOS_TEST_ETCD_ENDPOINTS")
            .unwrap_or_else(|_| "127.0.0.1:2379".into())
            .split(',')
            .map(|endpoint| endpoint.trim().to_string())
            .filter(|endpoint| !endpoint.is_empty())
            .collect();
        let prefix = format!(
            "/chronos-test-watch-shutdown-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let store = EtcdMetadataStore::from_raw_endpoints_unchecked(endpoints, prefix)
            .await
            .expect("etcd store should start");

        tokio::time::timeout(Duration::from_secs(5), store.shutdown_route_watch())
            .await
            .expect("route watch shutdown should complete");
    }
}
