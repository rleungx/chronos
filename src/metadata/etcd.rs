use async_trait::async_trait;
use etcd_client::{
    Certificate, Client, Compare, CompareOp, ConnectOptions, DeleteOptions, EventType, GetOptions,
    Identity, TlsOptions, Txn, TxnOp, WatchOptions,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex as StdMutex;
use std::sync::MutexGuard;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::recovery::record_recovery_event;
use crate::tls::read_required_pem_file;
use crate::{metrics, TimelineLifecycleState, TimelineRoute, TsoConfig, TsoError};

const REQUEST_RECORD_PRUNE_MIN_FETCH_LIMIT: usize = 128;
const REQUEST_RECORD_PRUNE_MAX_FETCH_LIMIT: usize = 4096;
const STATUS_INDEX_REBUILD_BATCH_RECORDS: usize = 32;
const GENERATOR_BATCH_GET_CHUNK_SIZE: usize = 64;
const IDENTITY_KEEPALIVE_RECONNECT_MIN_BACKOFF_MS: u64 = 100;
const IDENTITY_KEEPALIVE_RECONNECT_MAX_BACKOFF_MS: u64 = 500;
const ROUTE_WATCH_MIN_RECONNECT_BACKOFF_MS: u64 = 100;
const ROUTE_WATCH_MAX_RECONNECT_BACKOFF_MS: u64 = 5_000;

use super::{
    identity::{claim_instance_identity, InstanceIdentityLeaseRecord},
    keys,
    types::{
        timeline_route_update_from_routes, RouteUpdateSignal, CURRENT_METADATA_SCHEMA_VERSION,
    },
    GeneratorBatchOp, GeneratorLeaseAuthority, GeneratorRecord, IdentityLeaseAuthority,
    InstanceIdentityLease, OwnershipPlanMember, OwnershipPlanRecord, RequestRecord,
    RequestRecordAuthority, RequestRecordState, RouteUpdateSource, TimelineAuthority,
    TimelineBatchOp, TimelineFilterRecord, TimelineFilterRecordListPage, TimelineRecord,
    TimelineRecordListPage, TimelineRouteRecord,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RequestRecordCleanupIndexEntry {
    timeline_key: String,
    client_request_id: String,
    updated_at_ms: u64,
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

fn record_route_watch_resync(event: &'static str) {
    metrics::TSO_WATCH_RESYNC_TOTAL
        .with_label_values(&[event])
        .inc();
}

fn route_watch_options(start_revision: Option<i64>) -> WatchOptions {
    let mut options = WatchOptions::new()
        .with_prefix()
        .with_prev_key()
        .with_progress_notify();
    if let Some(revision) = start_revision.filter(|revision| *revision > 0) {
        options = options.with_start_revision(revision);
    }
    options
}

fn route_watch_reconnect_backoff(consecutive_failures: u32) -> Duration {
    let shift = consecutive_failures.saturating_sub(1).min(6);
    let base_ms = ROUTE_WATCH_MIN_RECONNECT_BACKOFF_MS
        .saturating_mul(1u64 << shift)
        .min(ROUTE_WATCH_MAX_RECONNECT_BACKOFF_MS);
    let jitter_ms =
        ((std::process::id() as u64).wrapping_add(consecutive_failures as u64 * 97)) % 100;
    Duration::from_millis((base_ms + jitter_ms).min(ROUTE_WATCH_MAX_RECONNECT_BACKOFF_MS))
}

fn identity_keepalive_reconnect_backoff(consecutive_failures: u32) -> Duration {
    let shift = consecutive_failures.saturating_sub(1).min(3);
    let base_ms = IDENTITY_KEEPALIVE_RECONNECT_MIN_BACKOFF_MS
        .saturating_mul(1u64 << shift)
        .min(IDENTITY_KEEPALIVE_RECONNECT_MAX_BACKOFF_MS);
    let jitter_ms =
        ((std::process::id() as u64).wrapping_add(consecutive_failures as u64 * 53)) % 50;
    Duration::from_millis((base_ms + jitter_ms).min(IDENTITY_KEEPALIVE_RECONNECT_MAX_BACKOFF_MS))
}

fn send_route_watch_reset_once(
    route_updates: &broadcast::Sender<RouteUpdateSignal>,
    reset_sent: &mut bool,
) {
    if *reset_sent {
        return;
    }
    let _ = route_updates.send(RouteUpdateSignal::Reset);
    *reset_sent = true;
}

fn verify_instance_identity_lease_record(
    expected_lease_id: i64,
    actual_lease_id: i64,
    value: &[u8],
    instance_id: &str,
    worker_id: &str,
    advertise_endpoint: &str,
) -> Result<(), TsoError> {
    if actual_lease_id != expected_lease_id {
        return Err(TsoError::Internal(format!(
            "Etcd identity lease startup probe observed lease {} but expected {} for instance {}",
            actual_lease_id, expected_lease_id, instance_id
        )));
    }

    let record: InstanceIdentityLeaseRecord = serde_json::from_slice(value).map_err(|error| {
        TsoError::Internal(format!(
            "Etcd identity lease startup probe decode failed: {}",
            error
        ))
    })?;

    if record.instance_id != instance_id
        || record.worker_id != worker_id
        || record.advertise_endpoint != advertise_endpoint
    {
        return Err(TsoError::Internal(format!(
            "Etcd identity lease startup probe observed mismatched identity record for instance {}",
            instance_id
        )));
    }

    Ok(())
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

fn next_etcd_key_after(key: &[u8]) -> Vec<u8> {
    let mut next_key = key.to_vec();
    next_key.push(0);
    next_key
}

fn request_record_prune_fetch_limit(delete_limit: usize) -> i64 {
    delete_limit
        .clamp(
            REQUEST_RECORD_PRUNE_MIN_FETCH_LIMIT,
            REQUEST_RECORD_PRUNE_MAX_FETCH_LIMIT,
        )
        .min(i64::MAX as usize) as i64
}

fn request_record_is_prunable(record: &RequestRecord, older_than_ms: u64) -> bool {
    request_record_is_prunable_candidate(record) && record.updated_at_ms < older_than_ms
}

fn request_record_is_prunable_candidate(record: &RequestRecord) -> bool {
    record.state == RequestRecordState::Completed
}

fn record_metadata_conflict(op_label: &'static str, kind: &'static str) {
    metrics::TSO_METADATA_CONFLICTS_TOTAL
        .with_label_values(&[op_label, kind])
        .inc();
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
        store.rebuild_timeline_status_indexes().await?;
        store.spawn_route_watch_loop();
        Ok(store)
    }

    fn timeline_key(&self, timeline_key: &str) -> String {
        keys::timeline_key(&self.prefix, timeline_key)
    }

    fn generator_key(&self, generator_id: u32) -> String {
        keys::generator_key(&self.prefix, generator_id)
    }

    fn generator_prefix(&self) -> String {
        keys::generator_prefix(&self.prefix)
    }

    fn route_prefix(&self) -> String {
        keys::route_prefix(&self.prefix)
    }

    fn timeline_status_index_prefix(&self) -> String {
        keys::timeline_status_index_prefix(&self.prefix)
    }

    fn timeline_status_index_marker_key(&self) -> String {
        keys::timeline_status_index_marker_key(&self.prefix)
    }

    fn timeline_status_owner_index_key(&self, record: &TimelineRecord) -> String {
        keys::timeline_status_owner_index_key(
            &self.prefix,
            &record.route.owner_worker_endpoint,
            &record.route.timeline_key,
        )
    }

    fn timeline_status_owner_index_prefix(&self, owner_worker_endpoint: &str) -> String {
        keys::timeline_status_owner_index_prefix(&self.prefix, owner_worker_endpoint)
    }

    fn timeline_status_state_index_key(&self, record: &TimelineRecord) -> String {
        keys::timeline_status_state_index_key(
            &self.prefix,
            &record.state.to_string(),
            &record.route.timeline_key,
        )
    }

    fn timeline_status_state_index_prefix(&self, state: TimelineLifecycleState) -> String {
        keys::timeline_status_state_index_prefix(&self.prefix, &state.to_string())
    }

    fn request_key(&self, timeline_key: &str, client_request_id: &str) -> String {
        keys::request_key(&self.prefix, timeline_key, client_request_id)
    }

    fn request_prefix(&self) -> String {
        keys::request_prefix(&self.prefix)
    }

    fn request_cleanup_index_key(
        &self,
        record: &RequestRecord,
        timeline_key: &str,
        client_request_id: &str,
    ) -> String {
        keys::request_cleanup_index_key(
            &self.prefix,
            record.updated_at_ms,
            timeline_key,
            client_request_id,
        )
    }

    fn request_cleanup_index_prefix(&self) -> String {
        keys::request_cleanup_index_prefix(&self.prefix)
    }

    fn request_cleanup_index_cutoff(&self, older_than_ms: u64) -> String {
        keys::request_cleanup_index_cutoff(&self.prefix, older_than_ms)
    }

    fn instance_identity_key(&self, instance_id: &str) -> String {
        keys::instance_identity_key(&self.prefix, instance_id)
    }

    fn instance_identity_prefix(&self) -> String {
        keys::instance_identity_prefix(&self.prefix)
    }

    fn ownership_plan_key(&self) -> String {
        keys::ownership_plan_key(&self.prefix)
    }

    fn ownership_plan_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0)
    }

    async fn active_identity_member_counts(
        &self,
    ) -> Result<HashMap<(String, String), usize>, TsoError> {
        let mut client = self.client.clone();
        let prefix = self.instance_identity_prefix();
        let response = client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["list_identity_leases"])
                    .inc();
                TsoError::Internal(format!("Etcd list_identity_leases failed: {}", error))
            })?;

        let mut counts = HashMap::new();
        for kv in response.kvs() {
            if kv.lease() == 0 {
                continue;
            }
            let record: InstanceIdentityLeaseRecord =
                serde_json::from_slice(kv.value()).map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["list_identity_leases"])
                        .inc();
                    TsoError::Internal(format!(
                        "Identity lease record deserialization failed: {}",
                        error
                    ))
                })?;
            *counts
                .entry((record.worker_id, record.advertise_endpoint))
                .or_default() += 1;
        }
        Ok(counts)
    }

    pub async fn admit_ownership_plan_member(&self, config: &TsoConfig) -> Result<(), TsoError> {
        if config.generator_ownership_modulo <= 1 {
            return Ok(());
        }

        let member = OwnershipPlanMember {
            remainder: config.generator_ownership_remainder,
            worker_id: config.worker_id.clone(),
            advertise_endpoint: config.advertise_endpoint.clone(),
        };
        let member_key = (member.worker_id.clone(), member.advertise_endpoint.clone());
        let expected_plan_id = config.ownership_plan_id.trim();
        let expected_modulo = config.generator_ownership_modulo;
        let key = self.ownership_plan_key();

        for _attempt in 0..8 {
            let now_ms = Self::ownership_plan_timestamp_ms();
            let active_member_counts = self.active_identity_member_counts().await?;
            if active_member_counts.get(&member_key).copied().unwrap_or(0) > 1 {
                return Err(TsoError::Internal(format!(
                    "ownership plan worker_id={} advertise_endpoint={} has multiple active identity leases",
                    member.worker_id, member.advertise_endpoint
                )));
            }
            let active_members = active_member_counts.keys().cloned().collect::<HashSet<_>>();
            match self
                .get_json_record::<OwnershipPlanRecord>(
                    key.clone(),
                    "get_ownership_plan",
                    "Ownership plan record deserialization",
                )
                .await?
            {
                Some((mut record, revision)) => {
                    let pruned = record.prune_inactive_members(&active_members, now_ms)?;
                    if pruned > 0 {
                        info!(
                            component = "metadata",
                            event = "ownership_plan_pruned",
                            result = "success",
                            reason = "inactive_identity",
                            pruned_members = pruned,
                            ownership_plan_id = %expected_plan_id
                        );
                    }
                    let admitted = record.admit_member(
                        expected_plan_id,
                        expected_modulo,
                        member.clone(),
                        now_ms,
                    )?;
                    if pruned == 0 && !admitted {
                        return Ok(());
                    }
                    match self
                        .cas_json_record(
                            key.clone(),
                            revision,
                            &record,
                            JsonTxnContext {
                                op_label: "cas_ownership_plan",
                                serialize_context: "Ownership plan record serialization",
                                txn_context: "Etcd ownership plan CAS txn failed",
                                invalid_response_context: "invalid ownership plan CAS response",
                            },
                        )
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(TsoError::CasFailed) => continue,
                        Err(error) => return Err(error),
                    }
                }
                None => {
                    let record = OwnershipPlanRecord::new(
                        expected_plan_id.to_owned(),
                        expected_modulo,
                        member.clone(),
                        now_ms,
                    );
                    match self
                        .create_json_record(
                            key.clone(),
                            &record,
                            JsonTxnContext {
                                op_label: "create_ownership_plan",
                                serialize_context: "Ownership plan record serialization",
                                txn_context: "Etcd ownership plan create txn failed",
                                invalid_response_context: "invalid ownership plan create response",
                            },
                        )
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(TsoError::MetadataAlreadyExists) => continue,
                        Err(error) => return Err(error),
                    }
                }
            }
        }

        Err(TsoError::CasFailed)
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

        let mut keepalive_client = client.clone();
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
            let mut lease_alive_until =
                Instant::now() + Duration::from_secs(ttl_seconds.max(1) as u64);
            'keepalive: loop {
                let keepalive_result = keeper.keep_alive().await;
                let failure_reason = match keepalive_result {
                    Ok(()) => match stream.message().await {
                        Ok(Some(response)) if response.ttl() > 0 => {
                            lease_alive_until =
                                Instant::now() + Duration::from_secs(response.ttl().max(1) as u64);
                            sleep(heartbeat_interval).await;
                            continue;
                        }
                        Ok(Some(response)) => {
                            format!("keepalive_response_non_positive_ttl: {}", response.ttl())
                        }
                        Ok(None) => "keepalive_stream_closed".to_owned(),
                        Err(error) => format!("keepalive_stream_error: {error}"),
                    },
                    Err(error) => format!("keepalive_send_failed: {error}"),
                };

                let mut consecutive_reconnect_failures = 0u32;
                let mut reconnect_reason = failure_reason;
                loop {
                    let now = Instant::now();
                    if now >= lease_alive_until {
                        error!(
                            component = "identity_lease",
                            event = "keepalive_lost",
                            result = "failure",
                            reason = %reconnect_reason,
                            lease_id,
                            instance_id = lease_instance_id,
                            worker_id = lease_worker_id,
                            advertise_endpoint = lease_advertise_endpoint
                        );
                        let _ = lost_tx.send(true);
                        break 'keepalive;
                    }

                    let backoff = identity_keepalive_reconnect_backoff(
                        consecutive_reconnect_failures.saturating_add(1),
                    );
                    warn!(
                        component = "identity_lease",
                        event = "keepalive_reconnect",
                        result = "degraded",
                        reason = %reconnect_reason,
                        lease_id,
                        remaining_ttl_ms = lease_alive_until.duration_since(now).as_millis(),
                        backoff_ms = backoff.as_millis(),
                        instance_id = lease_instance_id,
                        worker_id = lease_worker_id,
                        advertise_endpoint = lease_advertise_endpoint
                    );
                    sleep(backoff).await;

                    match keepalive_client.lease_keep_alive(lease_id).await {
                        Ok((new_keeper, new_stream)) => {
                            keeper = new_keeper;
                            stream = new_stream;
                            continue 'keepalive;
                        }
                        Err(error) => {
                            consecutive_reconnect_failures =
                                consecutive_reconnect_failures.saturating_add(1);
                            reconnect_reason = format!("keepalive_reconnect_failed: {error}");
                        }
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
            let mut next_watch_revision: Option<i64> = None;
            let mut consecutive_failures = 0u32;
            let mut reset_sent_for_outage = false;
            loop {
                let watch_start_revision = next_watch_revision;
                let watch_result = tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    result = watch_client.watch(
                        route_prefix.clone(),
                        Some(route_watch_options(watch_start_revision)),
                    ) => result,
                };

                let (_watcher, mut watch_stream) = match watch_result {
                    Ok(stream) => {
                        consecutive_failures = 0;
                        reset_sent_for_outage = false;
                        info!(
                            component = "route_watch",
                            event = "watch_started",
                            result = "success",
                            reason = "watch_connected",
                            start_revision = watch_start_revision.unwrap_or(0)
                        );
                        stream
                    }
                    Err(_) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        record_route_watch_resync("watch_connect_failed");
                        send_route_watch_reset_once(&route_updates, &mut reset_sent_for_outage);
                        let backoff = route_watch_reconnect_backoff(consecutive_failures);
                        warn!(
                            component = "route_watch",
                            event = "watch_restarted",
                            result = "degraded",
                            reason = "watch_connect_failed",
                            backoff_ms = backoff.as_millis() as u64,
                            start_revision = watch_start_revision.unwrap_or(0)
                        );
                        tokio::select! {
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            _ = sleep(backoff) => {}
                        }
                        continue;
                    }
                };

                let reconnect_after: Duration;
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
                            if let Some(header) = response.header() {
                                let revision = header.revision();
                                if revision > 0 {
                                    next_watch_revision = revision.checked_add(1);
                                }
                            }
                            if response.compact_revision() > 0 {
                                consecutive_failures = consecutive_failures.saturating_add(1);
                                record_route_watch_resync("watch_compacted");
                                next_watch_revision = None;
                                send_route_watch_reset_once(
                                    &route_updates,
                                    &mut reset_sent_for_outage,
                                );
                                let backoff = route_watch_reconnect_backoff(consecutive_failures);
                                reconnect_after = backoff;
                                warn!(
                                    component = "route_watch",
                                    event = "watch_restarted",
                                    result = "degraded",
                                    reason = "watch_compacted",
                                    compact_revision = response.compact_revision(),
                                    backoff_ms = backoff.as_millis() as u64
                                );
                                break;
                            }
                            if response.canceled() {
                                consecutive_failures = consecutive_failures.saturating_add(1);
                                record_route_watch_resync("watch_canceled");
                                next_watch_revision = None;
                                send_route_watch_reset_once(
                                    &route_updates,
                                    &mut reset_sent_for_outage,
                                );
                                let backoff = route_watch_reconnect_backoff(consecutive_failures);
                                reconnect_after = backoff;
                                warn!(
                                    component = "route_watch",
                                    event = "watch_restarted",
                                    result = "degraded",
                                    reason = "watch_canceled",
                                    cancel_reason = response.cancel_reason(),
                                    backoff_ms = backoff.as_millis() as u64
                                );
                                break;
                            }
                            for event in response.events() {
                                if event.event_type() != EventType::Put {
                                    continue;
                                }
                                let Some(key_value) = event.kv() else {
                                    metrics::TSO_METADATA_ERRORS_TOTAL
                                        .with_label_values(&["route_watch_event_missing_kv"])
                                        .inc();
                                    record_route_watch_resync("watch_event_missing_kv");
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
                                    record_route_watch_resync("watch_event_decode_failed");
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
                                    debug!(
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
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            record_route_watch_resync("watch_stream_closed");
                            send_route_watch_reset_once(&route_updates, &mut reset_sent_for_outage);
                            reconnect_after = route_watch_reconnect_backoff(consecutive_failures);
                            break;
                        }
                        Err(_) => {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            record_route_watch_resync("watch_stream_error");
                            send_route_watch_reset_once(&route_updates, &mut reset_sent_for_outage);
                            let backoff = route_watch_reconnect_backoff(consecutive_failures);
                            reconnect_after = backoff;
                            warn!(
                                component = "route_watch",
                                event = "watch_restarted",
                                result = "degraded",
                                reason = "watch_stream_error",
                                backoff_ms = backoff.as_millis() as u64,
                                next_start_revision = next_watch_revision.unwrap_or(0)
                            );
                            break;
                        }
                    }
                }

                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = sleep(reconnect_after) => {}
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

    pub async fn verify_instance_identity_write_path(
        &self,
        expected_lease_id: i64,
        instance_id: &str,
        worker_id: &str,
        advertise_endpoint: &str,
    ) -> Result<(), TsoError> {
        let key = self.instance_identity_key(instance_id);
        let mut client = self.client.clone();
        let response = client.get(key, None).await.map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&["identity_lease_startup_probe_get"])
                .inc();
            TsoError::Internal(format!(
                "Etcd identity lease startup probe lookup failed: {}",
                error
            ))
        })?;

        let Some(kv) = response.kvs().first() else {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&["identity_lease_startup_probe_missing"])
                .inc();
            return Err(TsoError::Internal(format!(
                "Etcd identity lease startup probe missing record for instance {}",
                instance_id
            )));
        };

        if kv.lease() == 0 {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&["identity_lease_startup_probe_missing_lease"])
                .inc();
            return Err(TsoError::Internal(format!(
                "Etcd identity lease startup probe found record without an attached lease for instance {}",
                instance_id
            )));
        }

        verify_instance_identity_lease_record(
            expected_lease_id,
            kv.lease(),
            kv.value(),
            instance_id,
            worker_id,
            advertise_endpoint,
        )
        .inspect_err(|error| {
            let label = match error.to_string().contains("decode failed") {
                true => "identity_lease_startup_probe_decode",
                false if error.to_string().contains("mismatched identity record") => {
                    "identity_lease_startup_probe_mismatch"
                }
                false if error.to_string().contains("observed lease") => {
                    "identity_lease_startup_probe_wrong_lease"
                }
                false => "identity_lease_startup_probe_mismatch",
            };
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[label])
                .inc();
        })?;

        Ok(())
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
            record_metadata_conflict(context.op_label, "already_exists");
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
            record_metadata_conflict(context.op_label, "cas_failed");
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
            record_metadata_conflict(context.op_label, "cas_failed");
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

    async fn delete_json_record(
        &self,
        key: String,
        previous_revision: u64,
        context: JsonTxnContext,
    ) -> Result<(), TsoError> {
        let mut client = self.client.clone();
        let txn = Txn::new()
            .when(vec![Compare::mod_revision(
                key.as_bytes(),
                CompareOp::Equal,
                previous_revision as i64,
            )])
            .and_then(vec![TxnOp::delete(key.as_bytes(), None)]);

        let response = client.txn(txn).await.map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
            TsoError::Internal(format!("{}: {}", context.txn_context, error))
        })?;

        if !response.succeeded() {
            record_metadata_conflict(context.op_label, "cas_failed");
            return Err(TsoError::CasFailed);
        }

        Ok(())
    }

    fn timeline_status_index_keys(&self, record: &TimelineRecord) -> [String; 2] {
        [
            self.timeline_status_owner_index_key(record),
            self.timeline_status_state_index_key(record),
        ]
    }

    fn timeline_status_index_put_ops(
        &self,
        record: &TimelineRecord,
        op_label: &'static str,
    ) -> Result<Vec<TxnOp>, TsoError> {
        let value =
            Self::serialize_record(record, op_label, "Timeline status index serialization")?;
        Ok(self
            .timeline_status_index_keys(record)
            .into_iter()
            .map(|key| TxnOp::put(key.as_bytes(), value.clone(), None))
            .collect())
    }

    fn timeline_status_index_replace_ops(
        &self,
        previous_record: Option<&TimelineRecord>,
        next_record: &TimelineRecord,
        op_label: &'static str,
    ) -> Result<Vec<TxnOp>, TsoError> {
        let next_keys = self.timeline_status_index_keys(next_record);
        let mut ops = Vec::with_capacity(4);
        if let Some(previous_record) = previous_record {
            for previous_key in self.timeline_status_index_keys(previous_record) {
                if !next_keys.iter().any(|next_key| next_key == &previous_key) {
                    ops.push(TxnOp::delete(previous_key.as_bytes(), None));
                }
            }
        }
        ops.extend(self.timeline_status_index_put_ops(next_record, op_label)?);
        Ok(ops)
    }

    async fn rebuild_timeline_status_indexes(&self) -> Result<(), TsoError> {
        let mut client = self.client.clone();
        let marker_key = self.timeline_status_index_marker_key();
        let marker = client
            .get(marker_key.clone(), None)
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["status_index_rebuild"])
                    .inc();
                TsoError::Internal(format!("Etcd status index marker lookup failed: {}", error))
            })?;
        if !marker.kvs().is_empty() {
            return Ok(());
        }

        let index_prefix = self.timeline_status_index_prefix();
        client
            .delete(
                index_prefix.clone(),
                Some(DeleteOptions::new().with_prefix()),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["status_index_rebuild"])
                    .inc();
                TsoError::Internal(format!("Etcd status index cleanup failed: {}", error))
            })?;

        let route_prefix = self.route_prefix();
        let route_range_end = prefix_range_end(&route_prefix);
        let mut start_key = route_prefix.clone().into_bytes();
        loop {
            let response = client
                .get(
                    start_key.clone(),
                    Some(
                        GetOptions::new()
                            .with_range(route_range_end.clone())
                            .with_limit(STATUS_INDEX_REBUILD_BATCH_RECORDS as i64),
                    ),
                )
                .await
                .map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["status_index_rebuild"])
                        .inc();
                    TsoError::Internal(format!("Etcd status index route scan failed: {}", error))
                })?;
            if response.kvs().is_empty() {
                break;
            }

            let mut ops = Vec::with_capacity(response.kvs().len() * 2);
            for kv in response.kvs() {
                let record: TimelineRecord =
                    serde_json::from_slice(kv.value()).map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["status_index_rebuild"])
                            .inc();
                        TsoError::Internal(format!(
                            "Timeline status index rebuild deserialization failed: {}",
                            error
                        ))
                    })?;
                record.validate_schema_version()?;
                ops.extend(self.timeline_status_index_put_ops(&record, "status_index_rebuild")?);
            }

            if !ops.is_empty() {
                client
                    .txn(Txn::new().and_then(ops))
                    .await
                    .map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["status_index_rebuild"])
                            .inc();
                        TsoError::Internal(format!(
                            "Etcd status index rebuild txn failed: {}",
                            error
                        ))
                    })?;
            }

            let last_key = response
                .kvs()
                .last()
                .map(|kv| kv.key().to_vec())
                .unwrap_or_default();
            start_key = next_etcd_key_after(&last_key);
        }

        client.put(marker_key, "1", None).await.map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&["status_index_rebuild"])
                .inc();
            TsoError::Internal(format!("Etcd status index marker write failed: {}", error))
        })?;
        Ok(())
    }

    async fn create_timeline_with_indexes(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError> {
        let key = self.timeline_key(timeline_key);
        let mut client = self.client.clone();
        let value = Self::serialize_record(record, "create", "Record serialization")?;
        let mut ops = vec![TxnOp::put(key.as_bytes(), value, None)];
        ops.extend(self.timeline_status_index_put_ops(record, "create")?);

        let response = client
            .txn(
                Txn::new()
                    .when(vec![Compare::mod_revision(
                        key.as_bytes(),
                        CompareOp::Equal,
                        0,
                    )])
                    .and_then(ops),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["create"])
                    .inc();
                TsoError::Internal(format!("Etcd txn failed: {}", error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict("create", "already_exists");
            return Err(TsoError::MetadataAlreadyExists);
        }

        Self::extract_put_revision(response, "create", "invalid txn response")
    }

    async fn compare_exchange_timeline_with_indexes(
        &self,
        timeline_key: &str,
        expected_revision: u64,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError> {
        let Some((previous_record, previous_revision)) = self.load_timeline(timeline_key).await?
        else {
            record_metadata_conflict("cas", "not_found");
            return Err(TsoError::TimelineNotFound {
                timeline_key: timeline_key.to_owned(),
            });
        };
        if previous_revision != expected_revision {
            record_metadata_conflict("cas", "revision_mismatch");
            return Err(TsoError::CasFailed);
        }

        let key = self.timeline_key(timeline_key);
        let mut client = self.client.clone();
        let value = Self::serialize_record(record, "cas", "Record serialization")?;
        let mut ops = vec![TxnOp::put(key.as_bytes(), value, None)];
        ops.extend(self.timeline_status_index_replace_ops(
            Some(&previous_record),
            record,
            "cas",
        )?);

        let response = client
            .txn(
                Txn::new()
                    .when(vec![Compare::mod_revision(
                        key.as_bytes(),
                        CompareOp::Equal,
                        expected_revision as i64,
                    )])
                    .and_then(ops),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas"])
                    .inc();
                TsoError::Internal(format!("Etcd txn failed: {}", error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict("cas", "cas_failed");
            return Err(TsoError::CasFailed);
        }

        Self::extract_put_revision(response, "cas", "invalid txn response")
    }

    async fn compare_exchange_timelines_with_indexes(
        &self,
        operations: &[TimelineBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        let mut seen_timeline_keys = HashSet::with_capacity(operations.len());
        let mut previous_records = Vec::with_capacity(operations.len());
        for operation in operations {
            if !seen_timeline_keys.insert(operation.timeline_key.as_str()) {
                record_metadata_conflict("cas_batch", "duplicate_key");
                return Err(TsoError::CasFailed);
            }
            let Some((previous_record, previous_revision)) =
                self.load_timeline(&operation.timeline_key).await?
            else {
                record_metadata_conflict("cas_batch", "not_found");
                return Err(TsoError::TimelineNotFound {
                    timeline_key: operation.timeline_key.clone(),
                });
            };
            if previous_revision != operation.previous_revision {
                record_metadata_conflict("cas_batch", "revision_mismatch");
                return Err(TsoError::CasFailed);
            }
            previous_records.push(previous_record);
        }

        let mut compares = Vec::with_capacity(operations.len());
        let mut ops = Vec::with_capacity(operations.len() * 5);
        for (operation, previous_record) in operations.iter().zip(previous_records.iter()) {
            let key = self.timeline_key(&operation.timeline_key);
            compares.push(Compare::mod_revision(
                key.as_bytes(),
                CompareOp::Equal,
                operation.previous_revision as i64,
            ));
            ops.push(TxnOp::put(
                key.as_bytes(),
                Self::serialize_record(
                    &operation.record,
                    "cas_batch",
                    "Batch record serialization",
                )?,
                None,
            ));
            ops.extend(self.timeline_status_index_replace_ops(
                Some(previous_record),
                &operation.record,
                "cas_batch",
            )?);
        }

        let mut client = self.client.clone();
        let response = client
            .txn(Txn::new().when(compares).and_then(ops))
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_batch"])
                    .inc();
                TsoError::Internal(format!("Etcd batch txn failed: {}", error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict("cas_batch", "cas_failed");
            return Err(TsoError::CasFailed);
        }

        let revision = response
            .header()
            .map(|header| header.revision() as u64)
            .ok_or_else(|| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_batch"])
                    .inc();
                TsoError::Internal("invalid batch txn response".to_string())
            })?;
        Ok(vec![revision; operations.len()])
    }

    fn status_index_start_key(
        index_prefix: &str,
        start_after_timeline_key: Option<&str>,
    ) -> Vec<u8> {
        start_after_timeline_key
            .map(|timeline_key| format!("{index_prefix}{timeline_key}").into_bytes())
            .unwrap_or_else(|| index_prefix.as_bytes().to_vec())
    }

    async fn list_timelines_from_status_index_prefix(
        &self,
        index_prefix: String,
        states: &[TimelineLifecycleState],
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<TimelineRecordListPage, TsoError> {
        if limit == 0 {
            return Ok(TimelineRecordListPage {
                records: Vec::new(),
                next_start_after_timeline_key: None,
            });
        }

        let mut client = self.client.clone();
        let mut start_key = Self::status_index_start_key(&index_prefix, start_after_timeline_key);
        let range_end = prefix_range_end(&index_prefix);
        let mut records = Vec::with_capacity(limit + 1);
        while records.len() <= limit {
            let fetch_limit = limit
                .saturating_add(1)
                .saturating_add(usize::from(start_after_timeline_key.is_some()))
                .min(i64::MAX as usize) as i64;
            let response = client
                .get(
                    start_key.clone(),
                    Some(
                        GetOptions::new()
                            .with_range(range_end.clone())
                            .with_limit(fetch_limit),
                    ),
                )
                .await
                .map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["list_status_index"])
                        .inc();
                    TsoError::Internal(format!("Etcd status index list failed: {}", error))
                })?;
            if response.kvs().is_empty() {
                break;
            }

            for kv in response.kvs() {
                if start_after_timeline_key.is_some() && kv.key() == start_key.as_slice() {
                    continue;
                }
                let record: TimelineRecord =
                    serde_json::from_slice(kv.value()).map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["list_status_index"])
                            .inc();
                        TsoError::Internal(format!(
                            "Timeline status index deserialization failed: {}",
                            error
                        ))
                    })?;
                record.validate_schema_version()?;
                if states.is_empty() || states.contains(&record.state) {
                    records.push(record);
                    if records.len() > limit {
                        break;
                    }
                }
            }

            if records.len() > limit {
                break;
            }
            let Some(last_key) = response.kvs().last().map(|kv| kv.key().to_vec()) else {
                break;
            };
            start_key = next_etcd_key_after(&last_key);
        }

        let next_start_after_timeline_key =
            (records.len() > limit).then(|| records[limit - 1].route.timeline_key.clone());
        records.truncate(limit);

        Ok(TimelineRecordListPage {
            records,
            next_start_after_timeline_key,
        })
    }

    fn request_cleanup_index_entry(
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> RequestRecordCleanupIndexEntry {
        RequestRecordCleanupIndexEntry {
            timeline_key: timeline_key.to_owned(),
            client_request_id: client_request_id.to_owned(),
            updated_at_ms: record.updated_at_ms,
        }
    }

    fn request_cleanup_index_put_op(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> Result<Option<TxnOp>, TsoError> {
        if !request_record_is_prunable_candidate(record) {
            return Ok(None);
        }
        let index_entry =
            Self::request_cleanup_index_entry(timeline_key, client_request_id, record);
        let index_value = Self::serialize_record(
            &index_entry,
            "request_cleanup_index",
            "Request cleanup index serialization",
        )?;
        Ok(Some(TxnOp::put(
            self.request_cleanup_index_key(record, timeline_key, client_request_id)
                .as_bytes(),
            index_value,
            None,
        )))
    }

    fn request_cleanup_index_delete_op(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> Option<TxnOp> {
        request_record_is_prunable_candidate(record).then(|| {
            TxnOp::delete(
                self.request_cleanup_index_key(record, timeline_key, client_request_id)
                    .as_bytes(),
                None,
            )
        })
    }

    async fn create_request_record_with_index(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> Result<u64, TsoError> {
        let key = self.request_key(timeline_key, client_request_id);
        let mut client = self.client.clone();
        let value =
            Self::serialize_record(record, "create_request", "Request record serialization")?;
        let mut puts = vec![TxnOp::put(key.as_bytes(), value, None)];
        if let Some(index_put) =
            self.request_cleanup_index_put_op(timeline_key, client_request_id, record)?
        {
            puts.push(index_put);
        }

        let response = client
            .txn(
                Txn::new()
                    .when(vec![Compare::mod_revision(
                        key.as_bytes(),
                        CompareOp::Equal,
                        0,
                    )])
                    .and_then(puts),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["create_request"])
                    .inc();
                TsoError::Internal(format!("Etcd request create txn failed: {}", error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict("create_request", "already_exists");
            return Err(TsoError::MetadataAlreadyExists);
        }

        Self::extract_put_revision(
            response,
            "create_request",
            "invalid request create txn response",
        )
    }

    async fn compare_exchange_request_record_with_index(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        expected_revision: u64,
        record: &RequestRecord,
    ) -> Result<u64, TsoError> {
        let Some((previous_record, previous_revision)) = self
            .load_request_record(timeline_key, client_request_id)
            .await?
        else {
            record_metadata_conflict("cas_request", "not_found");
            return Err(TsoError::TimelineNotFound {
                timeline_key: format!("request:{timeline_key}:{client_request_id}"),
            });
        };
        if previous_revision != expected_revision {
            record_metadata_conflict("cas_request", "revision_mismatch");
            return Err(TsoError::CasFailed);
        }

        let key = self.request_key(timeline_key, client_request_id);
        let mut client = self.client.clone();
        let value = Self::serialize_record(record, "cas_request", "Request record serialization")?;
        let mut ops = vec![TxnOp::put(key.as_bytes(), value, None)];
        if let Some(index_delete) =
            self.request_cleanup_index_delete_op(timeline_key, client_request_id, &previous_record)
        {
            ops.push(index_delete);
        }
        if let Some(index_put) =
            self.request_cleanup_index_put_op(timeline_key, client_request_id, record)?
        {
            ops.push(index_put);
        }

        let response = client
            .txn(
                Txn::new()
                    .when(vec![Compare::mod_revision(
                        key.as_bytes(),
                        CompareOp::Equal,
                        expected_revision as i64,
                    )])
                    .and_then(ops),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_request"])
                    .inc();
                TsoError::Internal(format!("Etcd request CAS txn failed: {}", error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict("cas_request", "cas_failed");
            return Err(TsoError::CasFailed);
        }

        Self::extract_put_revision(response, "cas_request", "invalid request CAS txn response")
    }

    async fn compare_delete_request_record_with_index(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        expected_revision: u64,
    ) -> Result<(), TsoError> {
        let Some((previous_record, previous_revision)) = self
            .load_request_record(timeline_key, client_request_id)
            .await?
        else {
            record_metadata_conflict("delete_request", "not_found");
            return Err(TsoError::TimelineNotFound {
                timeline_key: format!("request:{timeline_key}:{client_request_id}"),
            });
        };
        if previous_revision != expected_revision {
            record_metadata_conflict("delete_request", "revision_mismatch");
            return Err(TsoError::CasFailed);
        }

        let key = self.request_key(timeline_key, client_request_id);
        let mut ops = vec![TxnOp::delete(key.as_bytes(), None)];
        if let Some(index_delete) =
            self.request_cleanup_index_delete_op(timeline_key, client_request_id, &previous_record)
        {
            ops.push(index_delete);
        }
        self.delete_request_record_txn(key, expected_revision, ops)
            .await
    }

    async fn delete_request_record_txn(
        &self,
        key: String,
        expected_revision: u64,
        ops: Vec<TxnOp>,
    ) -> Result<(), TsoError> {
        let mut client = self.client.clone();
        let response = client
            .txn(
                Txn::new()
                    .when(vec![Compare::mod_revision(
                        key.as_bytes(),
                        CompareOp::Equal,
                        expected_revision as i64,
                    )])
                    .and_then(ops),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["delete_request"])
                    .inc();
                TsoError::Internal(format!("Etcd request delete txn failed: {}", error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict("delete_request", "cas_failed");
            return Err(TsoError::CasFailed);
        }

        Ok(())
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

    async fn list_timelines_by_status_filter_page(
        &self,
        states: &[TimelineLifecycleState],
        owner_worker_endpoint: Option<&str>,
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<TimelineRecordListPage, TsoError> {
        if states.is_empty() && owner_worker_endpoint.is_none() {
            return self
                .list_timelines_page(start_after_timeline_key, limit)
                .await;
        }

        if let Some(owner_worker_endpoint) = owner_worker_endpoint {
            return self
                .list_timelines_from_status_index_prefix(
                    self.timeline_status_owner_index_prefix(owner_worker_endpoint),
                    states,
                    start_after_timeline_key,
                    limit,
                )
                .await;
        }

        if states.len() == 1 {
            return self
                .list_timelines_from_status_index_prefix(
                    self.timeline_status_state_index_prefix(states[0]),
                    &[],
                    start_after_timeline_key,
                    limit,
                )
                .await;
        }

        if limit == 0 {
            return Ok(TimelineRecordListPage {
                records: Vec::new(),
                next_start_after_timeline_key: None,
            });
        }

        let mut merged = BTreeMap::new();
        for state in states {
            let page = self
                .list_timelines_from_status_index_prefix(
                    self.timeline_status_state_index_prefix(*state),
                    &[],
                    start_after_timeline_key,
                    limit,
                )
                .await?;
            for record in page.records {
                merged.insert(record.route.timeline_key.clone(), record);
            }
        }

        let mut records: Vec<_> = merged.into_values().take(limit + 1).collect();
        let next_start_after_timeline_key =
            (records.len() > limit).then(|| records[limit - 1].route.timeline_key.clone());
        records.truncate(limit);

        Ok(TimelineRecordListPage {
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
        self.create_timeline_with_indexes(timeline_key, &stamped)
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
        self.compare_exchange_timeline_with_indexes(timeline_key, expected_revision, &stamped)
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
        let stamped_operations: Vec<_> = operations
            .iter()
            .map(|operation| TimelineBatchOp {
                timeline_key: operation.timeline_key.clone(),
                previous_revision: operation.previous_revision,
                record: operation.record.stamped_for_persistence(),
            })
            .collect();
        self.compare_exchange_timelines_with_indexes(&stamped_operations)
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
        let mut loaded = generator_ids
            .iter()
            .copied()
            .map(|generator_id| (generator_id, None))
            .collect::<HashMap<_, _>>();
        if loaded.is_empty() {
            return Ok(loaded);
        }

        let requested_ids: Vec<_> = loaded.keys().copied().collect();

        for chunk in requested_ids.chunks(GENERATOR_BATCH_GET_CHUNK_SIZE) {
            let mut client = self.client.clone();
            let ops: Vec<_> = chunk
                .iter()
                .map(|generator_id| {
                    TxnOp::get(self.generator_key(*generator_id).into_bytes(), None)
                })
                .collect();
            let response = client
                .txn(Txn::new().and_then(ops))
                .await
                .map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["get_generator_batch"])
                        .inc();
                    TsoError::Internal(format!("Etcd get_generator_batch failed: {}", error))
                })?;

            for (generator_id, op_response) in chunk.iter().zip(response.op_responses()) {
                let etcd_client::TxnOpResponse::Get(get_response) = op_response else {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["get_generator_batch"])
                        .inc();
                    return Err(TsoError::Internal(
                        "invalid generator batch get txn response".to_string(),
                    ));
                };
                let Some(kv) = get_response.kvs().first() else {
                    continue;
                };
                let record: GeneratorRecord =
                    serde_json::from_slice(kv.value()).map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["get_generator_batch"])
                            .inc();
                        TsoError::Internal(format!(
                            "Generator record batch deserialization failed: {}",
                            error
                        ))
                    })?;
                record.validate_schema_version()?;
                loaded.insert(*generator_id, Some(record));
            }
        }

        Ok(loaded)
    }

    async fn scan_generators(&self) -> Result<Vec<GeneratorRecord>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["scan_generators"])
            .start_timer();
        let mut client = self.client.clone();
        let generator_prefix = self.generator_prefix();
        let response = client
            .get(
                generator_prefix.clone(),
                Some(GetOptions::new().with_range(prefix_range_end(&generator_prefix))),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["scan_generators"])
                    .inc();
                TsoError::Internal(format!("Etcd scan_generators failed: {}", error))
            })?;

        let mut records = Vec::with_capacity(response.kvs().len());
        for kv in response.kvs() {
            let record: GeneratorRecord = serde_json::from_slice(kv.value()).map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["scan_generators"])
                    .inc();
                TsoError::Internal(format!(
                    "Generator record scan deserialization failed: {}",
                    error
                ))
            })?;
            record.validate_schema_version()?;
            records.push(record);
        }
        Ok(records)
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

#[async_trait]
impl RequestRecordAuthority for EtcdMetadataStore {
    async fn load_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
    ) -> Result<Option<(RequestRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get_request"])
            .start_timer();
        let result: Option<(RequestRecord, u64)> = self
            .get_json_record(
                self.request_key(timeline_key, client_request_id),
                "get_request",
                "Request record deserialization",
            )
            .await?;
        result
            .map(|(record, revision)| {
                record.validate_schema_version()?;
                Ok((record, revision))
            })
            .transpose()
    }

    async fn create_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["create_request"])
            .start_timer();
        record.validate_schema_version()?;
        let stamped = record.stamped_for_persistence();
        self.create_request_record_with_index(timeline_key, client_request_id, &stamped)
            .await
    }

    async fn compare_exchange_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        expected_revision: u64,
        record: &RequestRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_request"])
            .start_timer();
        record.validate_schema_version()?;
        let stamped = record.stamped_for_persistence();
        self.compare_exchange_request_record_with_index(
            timeline_key,
            client_request_id,
            expected_revision,
            &stamped,
        )
        .await
    }

    async fn compare_delete_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        expected_revision: u64,
    ) -> Result<(), TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["delete_request"])
            .start_timer();
        self.compare_delete_request_record_with_index(
            timeline_key,
            client_request_id,
            expected_revision,
        )
        .await
    }

    async fn prune_completed_request_records(
        &self,
        older_than_ms: u64,
        limit: usize,
    ) -> Result<usize, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["prune_requests"])
            .start_timer();
        if limit == 0 {
            return Ok(0);
        }

        let mut pruned = 0;
        let index_prefix = self.request_cleanup_index_prefix();
        let index_range_end = self.request_cleanup_index_cutoff(older_than_ms);
        let mut index_start_key = index_prefix.into_bytes();
        let index_fetch_limit = request_record_prune_fetch_limit(limit);

        loop {
            let mut client = self.client.clone();
            let response = client
                .get(
                    index_start_key.clone(),
                    Some(
                        GetOptions::new()
                            .with_range(index_range_end.as_bytes())
                            .with_limit(index_fetch_limit),
                    ),
                )
                .await
                .map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["prune_requests"])
                        .inc();
                    TsoError::Internal(format!("Etcd request cleanup index scan failed: {}", error))
                })?;

            let kvs = response.kvs();
            let Some(last_index_key) = kvs.last().map(|kv| kv.key().to_vec()) else {
                break;
            };

            for kv in kvs {
                if pruned >= limit {
                    return Ok(pruned);
                }

                let index_entry: RequestRecordCleanupIndexEntry =
                    serde_json::from_slice(kv.value()).map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["prune_requests"])
                            .inc();
                        TsoError::Internal(format!(
                            "Request cleanup index deserialization failed: {}",
                            error
                        ))
                    })?;
                let index_key = String::from_utf8(kv.key().to_vec()).map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["prune_requests"])
                        .inc();
                    TsoError::Internal(format!(
                        "Request cleanup index key decode failed: {}",
                        error
                    ))
                })?;
                let request_key =
                    self.request_key(&index_entry.timeline_key, &index_entry.client_request_id);
                let Some((record, request_revision)) = self
                    .load_request_record(&index_entry.timeline_key, &index_entry.client_request_id)
                    .await?
                else {
                    match self
                        .delete_json_record(
                            index_key,
                            kv.mod_revision() as u64,
                            JsonTxnContext {
                                op_label: "prune_requests",
                                serialize_context: "Request cleanup index serialization",
                                txn_context: "Etcd request cleanup index delete txn failed",
                                invalid_response_context:
                                    "invalid request cleanup index delete txn response",
                            },
                        )
                        .await
                    {
                        Ok(()) | Err(TsoError::CasFailed) => {}
                        Err(error) => return Err(error),
                    }
                    continue;
                };

                if record.updated_at_ms != index_entry.updated_at_ms
                    || !request_record_is_prunable(&record, older_than_ms)
                {
                    match self
                        .delete_json_record(
                            index_key,
                            kv.mod_revision() as u64,
                            JsonTxnContext {
                                op_label: "prune_requests",
                                serialize_context: "Request cleanup index serialization",
                                txn_context: "Etcd stale request cleanup index delete txn failed",
                                invalid_response_context:
                                    "invalid stale request cleanup index delete txn response",
                            },
                        )
                        .await
                    {
                        Ok(()) | Err(TsoError::CasFailed) => {}
                        Err(error) => return Err(error),
                    }
                    continue;
                }

                let mut client = self.client.clone();
                let response = client
                    .txn(
                        Txn::new()
                            .when(vec![
                                Compare::mod_revision(
                                    request_key.as_bytes(),
                                    CompareOp::Equal,
                                    request_revision as i64,
                                ),
                                Compare::mod_revision(
                                    index_key.as_bytes(),
                                    CompareOp::Equal,
                                    kv.mod_revision(),
                                ),
                            ])
                            .and_then(vec![
                                TxnOp::delete(request_key.as_bytes(), None),
                                TxnOp::delete(index_key.as_bytes(), None),
                            ]),
                    )
                    .await
                    .map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["prune_requests"])
                            .inc();
                        TsoError::Internal(format!("Etcd request prune txn failed: {}", error))
                    })?;
                if response.succeeded() {
                    pruned += 1;
                }
            }

            index_start_key = next_etcd_key_after(&last_index_key);
            if index_start_key.as_slice() >= index_range_end.as_bytes() {
                break;
            }
        }

        if pruned >= limit {
            return Ok(pruned);
        }

        let mut client = self.client.clone();
        let request_prefix = self.request_prefix();
        let range_end = prefix_range_end(&request_prefix);
        let mut start_key = request_prefix.into_bytes();
        let fetch_limit = request_record_prune_fetch_limit(limit - pruned);

        loop {
            let response = client
                .get(
                    start_key.clone(),
                    Some(
                        GetOptions::new()
                            .with_range(range_end.clone())
                            .with_limit(fetch_limit),
                    ),
                )
                .await
                .map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["prune_requests"])
                        .inc();
                    TsoError::Internal(format!("Etcd prune_requests failed: {}", error))
                })?;

            let kvs = response.kvs();
            let Some(last_key) = kvs.last().map(|kv| kv.key().to_vec()) else {
                break;
            };

            let mut candidates = Vec::new();
            for kv in kvs {
                let record: RequestRecord =
                    serde_json::from_slice(kv.value()).map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["prune_requests"])
                            .inc();
                        TsoError::Internal(format!(
                            "Request record prune deserialization failed: {}",
                            error
                        ))
                    })?;
                record.validate_schema_version()?;
                if !request_record_is_prunable(&record, older_than_ms) {
                    continue;
                }
                let key = String::from_utf8(kv.key().to_vec()).map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["prune_requests"])
                        .inc();
                    TsoError::Internal(format!("Request record key decode failed: {}", error))
                })?;
                candidates.push((key, kv.mod_revision() as u64));
            }

            for (key, revision) in candidates {
                match self
                    .delete_json_record(
                        key,
                        revision,
                        JsonTxnContext {
                            op_label: "prune_requests",
                            serialize_context: "Request record serialization",
                            txn_context: "Etcd request prune txn failed",
                            invalid_response_context: "invalid request prune txn response",
                        },
                    )
                    .await
                {
                    Ok(()) => {
                        pruned += 1;
                        if pruned >= limit {
                            return Ok(pruned);
                        }
                    }
                    Err(TsoError::CasFailed) => {}
                    Err(error) => return Err(error),
                }
            }

            start_key = next_etcd_key_after(&last_key);
            if start_key.as_slice() >= range_end.as_slice() {
                break;
            }
        }

        Ok(pruned)
    }
}

impl RouteUpdateSource for EtcdMetadataStore {
    fn subscribe_route_updates(&self) -> broadcast::Receiver<RouteUpdateSignal> {
        self.route_updates.subscribe()
    }
}

#[async_trait]
impl super::ControlPlaneStore for EtcdMetadataStore {
    fn request_records(&self) -> Option<&dyn RequestRecordAuthority> {
        Some(self)
    }

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
    use crate::metrics;
    use crate::ResourceTier;

    use super::EtcdMetadataStore;
    use super::{
        parse_prev_route, parse_timeline_filter_record, route_update_for_watch_event,
        verify_instance_identity_lease_record, RouteOnlyTimelineRecord, TimelineRoute,
        TimelineRouteRecord,
    };
    use crate::metadata::types::CURRENT_METADATA_SCHEMA_VERSION;
    use crate::metadata::{
        AllocationRequestFingerprint, RequestRecord, RequestRecordState, RouteUpdateSignal,
    };
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

    fn sample_request_record(state: RequestRecordState, updated_at_ms: u64) -> RequestRecord {
        RequestRecord {
            schema_version: 1,
            fingerprint: AllocationRequestFingerprint { count: 1 },
            state,
            response: None,
            updated_at_ms,
        }
    }

    #[test]
    fn request_record_prune_predicate_only_matches_old_completed_records() {
        assert!(super::request_record_is_prunable(
            &sample_request_record(RequestRecordState::Completed, 99),
            100
        ));
        assert!(!super::request_record_is_prunable(
            &sample_request_record(RequestRecordState::Completed, 100),
            100
        ));
        assert!(!super::request_record_is_prunable(
            &sample_request_record(RequestRecordState::Pending, 50),
            100
        ));
    }

    #[test]
    fn next_etcd_key_after_advances_without_leaving_prefix_range() {
        let prefix = "/chronos/requests/";
        let key = b"/chronos/requests/timeline/request";
        let next = super::next_etcd_key_after(key);

        assert!(next.as_slice() > key.as_slice());
        assert!(next.as_slice() < super::prefix_range_end(prefix).as_slice());
    }

    #[test]
    fn request_record_prune_fetch_limit_is_bounded() {
        assert_eq!(
            super::request_record_prune_fetch_limit(1),
            super::REQUEST_RECORD_PRUNE_MIN_FETCH_LIMIT as i64
        );
        assert_eq!(
            super::request_record_prune_fetch_limit(usize::MAX),
            super::REQUEST_RECORD_PRUNE_MAX_FETCH_LIMIT as i64
        );
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

    #[test]
    fn record_route_watch_resync_increments_metric() {
        let before = metrics::TSO_WATCH_RESYNC_TOTAL
            .with_label_values(&["watch_stream_error"])
            .get();

        super::record_route_watch_resync("watch_stream_error");

        assert!(
            metrics::TSO_WATCH_RESYNC_TOTAL
                .with_label_values(&["watch_stream_error"])
                .get()
                > before
        );
    }

    #[test]
    fn route_watch_reconnect_backoff_is_bounded() {
        let first = super::route_watch_reconnect_backoff(1);
        let later = super::route_watch_reconnect_backoff(64);

        assert!(first >= Duration::from_millis(super::ROUTE_WATCH_MIN_RECONNECT_BACKOFF_MS));
        assert!(later <= Duration::from_millis(super::ROUTE_WATCH_MAX_RECONNECT_BACKOFF_MS));
    }

    #[tokio::test]
    async fn route_watch_reset_once_suppresses_duplicate_outage_resets() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(4);
        let mut reset_sent = false;

        super::send_route_watch_reset_once(&tx, &mut reset_sent);
        super::send_route_watch_reset_once(&tx, &mut reset_sent);

        assert!(matches!(rx.recv().await.unwrap(), RouteUpdateSignal::Reset));
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn verify_instance_identity_lease_record_accepts_exact_matching_lease_and_payload() {
        let payload = serde_json::json!({
            "instance_id": "instance-a",
            "worker_id": "worker-a",
            "advertise_endpoint": "worker-a:50051"
        });

        verify_instance_identity_lease_record(
            17,
            17,
            payload.to_string().as_bytes(),
            "instance-a",
            "worker-a",
            "worker-a:50051",
        )
        .expect("matching lease record should verify");
    }

    #[test]
    fn verify_instance_identity_lease_record_rejects_wrong_lease_id() {
        let payload = serde_json::json!({
            "instance_id": "instance-a",
            "worker_id": "worker-a",
            "advertise_endpoint": "worker-a:50051"
        });

        let error = verify_instance_identity_lease_record(
            17,
            18,
            payload.to_string().as_bytes(),
            "instance-a",
            "worker-a",
            "worker-a:50051",
        )
        .expect_err("wrong lease id should fail verification");

        assert!(error.to_string().contains("expected 17"));
    }

    #[test]
    fn verify_instance_identity_lease_record_rejects_invalid_payload() {
        let error = verify_instance_identity_lease_record(
            17,
            17,
            br#"{not-json}"#,
            "instance-a",
            "worker-a",
            "worker-a:50051",
        )
        .expect_err("invalid payload should fail verification");

        assert!(error.to_string().contains("decode failed"));
    }

    #[test]
    fn verify_instance_identity_lease_record_rejects_mismatched_identity_fields() {
        let payload = serde_json::json!({
            "instance_id": "instance-a",
            "worker_id": "worker-b",
            "advertise_endpoint": "worker-a:50051"
        });

        let error = verify_instance_identity_lease_record(
            17,
            17,
            payload.to_string().as_bytes(),
            "instance-a",
            "worker-a",
            "worker-a:50051",
        )
        .expect_err("mismatched payload should fail verification");

        assert!(error.to_string().contains("mismatched identity record"));
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
