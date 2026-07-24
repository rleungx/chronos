mod cluster_format;
mod connect;
mod control;
mod generator;
mod identity_lifecycle;
mod json;
mod lease_lock;
mod request_index;
mod request_records;
mod retry;
mod route_watch;
mod status_index;
mod timeline;

use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, DeleteOptions, EventType, GetOptions, PutOptions,
    Txn, TxnOp, WatchOptions,
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
use crate::{metrics, TimelineLifecycleState, TimelineRoute, TsoConfig, TsoError};

const REQUEST_RECORD_PRUNE_MIN_FETCH_LIMIT: usize = 128;
const REQUEST_RECORD_PRUNE_MAX_FETCH_LIMIT: usize = 4096;
const LEGACY_REQUEST_REVISION_FLAG: u64 = 1 << 63;
const STATUS_INDEX_REBUILD_BATCH_RECORDS: usize = 32;
const CURRENT_STATUS_INDEX_VERSION: &str = "2";
const GENERATOR_BATCH_GET_CHUNK_SIZE: usize = 64;
const GENERATOR_SCAN_BATCH_RECORDS: usize = 128;
const STATUS_INDEX_REBUILD_LOCK_TTL_SECS: i64 = 120;
const STATUS_INDEX_REBUILD_WAIT_TIMEOUT_MS: u64 = 120_000;
const TIMELINE_CREATION_LOCK_TTL_SECS: i64 = 30;
const TIMELINE_CREATION_LOCK_RETRY_ATTEMPTS: usize = 200;

use self::connect::build_etcd_connect_options;
use self::retry::{
    etcd_request_retry_budget, identity_keepalive_reconnect_backoff, retry_etcd_request,
    route_watch_reconnect_backoff,
};

use super::{
    identity::InstanceIdentityLeaseRecord,
    identity_lease_grant_request_ttl_seconds, keys,
    types::{
        timeline_route_update_from_routes, RouteUpdateSignal, CURRENT_CLUSTER_FORMAT_VERSION,
        CURRENT_METADATA_SCHEMA_VERSION,
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
    request_retry_budget: Duration,
    route_updates: broadcast::Sender<RouteUpdateSignal>,
    route_watch_shutdown_tx: watch::Sender<bool>,
    route_watch_task: StdMutex<Option<JoinHandle<()>>>,
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
    expected_record: &InstanceIdentityLeaseRecord,
) -> Result<(), TsoError> {
    if actual_lease_id != expected_lease_id {
        return Err(TsoError::Internal(format!(
            "Etcd identity lease startup probe observed lease {} but expected {} for instance {}",
            actual_lease_id, expected_lease_id, expected_record.instance_id
        )));
    }

    let record: InstanceIdentityLeaseRecord = serde_json::from_slice(value).map_err(|error| {
        TsoError::Internal(format!(
            "Etcd identity lease startup probe decode failed: {}",
            error
        ))
    })?;

    if &record != expected_record {
        return Err(TsoError::Internal(format!(
            "Etcd identity lease startup probe observed mismatched identity record for instance {}",
            expected_record.instance_id
        )));
    }

    Ok(())
}

fn identity_record_belongs_to_ownership_plan(
    identity: &InstanceIdentityLeaseRecord,
    plan_id: &str,
    modulo: u32,
) -> bool {
    if identity.ownership_plan_id.is_empty() {
        // Legacy identities do not declare their plan. Treat them as active for the existing
        // plan so a mixed-version rollout cannot silently replace ownership underneath them.
        return true;
    }
    identity.ownership_plan_id == plan_id && identity.ownership_modulo == modulo
}

fn identity_claim_matches_record(
    expected_lease_id: i64,
    actual_lease_id: i64,
    value: &[u8],
    expected_record: &InstanceIdentityLeaseRecord,
) -> Result<bool, TsoError> {
    if actual_lease_id != expected_lease_id {
        return Ok(false);
    }

    let record: InstanceIdentityLeaseRecord = serde_json::from_slice(value).map_err(|error| {
        TsoError::Internal(format!(
            "Etcd identity lease claim verification decode failed: {}",
            error
        ))
    })?;

    Ok(&record == expected_record)
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
        let request_retry_budget = etcd_request_retry_budget(config);
        let store =
            Self::connect_with_options(endpoints, prefix, options, request_retry_budget).await?;
        if let Err(error) = store.initialize_cluster_format_and_indexes().await {
            store.shutdown_route_watch().await;
            return Err(error);
        }
        if let Err(error) = store.probe_metadata_runtime().await {
            store.shutdown_route_watch().await;
            return Err(error);
        }
        Ok(store)
    }

    async fn connect_with_options(
        endpoints: Vec<String>,
        prefix: String,
        options: Option<ConnectOptions>,
        request_retry_budget: Duration,
    ) -> Result<Self, TsoError> {
        let client = Client::connect(endpoints, options)
            .await
            .map_err(|error| TsoError::Internal(format!("Etcd connection failed: {}", error)))?;
        let (route_updates, _) = broadcast::channel(1024);
        let (route_watch_shutdown_tx, _) = watch::channel(false);
        let store = Self {
            client,
            prefix,
            request_retry_budget: request_retry_budget.max(Duration::from_millis(1)),
            route_updates,
            route_watch_shutdown_tx,
            route_watch_task: StdMutex::new(None),
        };
        store.spawn_route_watch_loop();
        Ok(store)
    }

    async fn etcd_get(
        &self,
        op_label: &'static str,
        key: impl Into<Vec<u8>>,
        options: Option<GetOptions>,
    ) -> Result<etcd_client::GetResponse, etcd_client::Error> {
        let key = key.into();
        retry_etcd_request(op_label, self.request_retry_budget, || {
            let mut client = self.client.clone();
            let key = key.clone();
            let options = options.clone();
            async move { client.get(key, options).await }
        })
        .await
    }

    async fn etcd_txn(
        &self,
        op_label: &'static str,
        txn: Txn,
    ) -> Result<etcd_client::TxnResponse, etcd_client::Error> {
        retry_etcd_request(op_label, self.request_retry_budget, || {
            let mut client = self.client.clone();
            let txn = txn.clone();
            async move { client.txn(txn).await }
        })
        .await
    }

    async fn etcd_lease_grant(
        &self,
        ttl: i64,
    ) -> Result<etcd_client::LeaseGrantResponse, etcd_client::Error> {
        self.etcd_lease_grant_for("identity_lease_grant", ttl).await
    }

    async fn etcd_lease_grant_for(
        &self,
        op_label: &'static str,
        ttl: i64,
    ) -> Result<etcd_client::LeaseGrantResponse, etcd_client::Error> {
        retry_etcd_request(op_label, self.request_retry_budget, || {
            let mut client = self.client.clone();
            async move { client.lease_grant(ttl, None).await }
        })
        .await
    }

    async fn etcd_lease_keep_alive(
        &self,
        lease_id: i64,
    ) -> Result<(etcd_client::LeaseKeeper, etcd_client::LeaseKeepAliveStream), etcd_client::Error>
    {
        self.etcd_lease_keep_alive_for("identity_lease_keepalive_open", lease_id)
            .await
    }

    async fn etcd_lease_keep_alive_for(
        &self,
        op_label: &'static str,
        lease_id: i64,
    ) -> Result<(etcd_client::LeaseKeeper, etcd_client::LeaseKeepAliveStream), etcd_client::Error>
    {
        retry_etcd_request(op_label, self.request_retry_budget, || {
            let mut client = self.client.clone();
            async move { client.lease_keep_alive(lease_id).await }
        })
        .await
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

    fn timeline_status_index_rebuild_lock_key(&self) -> String {
        keys::timeline_status_index_rebuild_lock_key(&self.prefix)
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

    fn legacy_request_key(&self, timeline_key: &str, client_request_id: &str) -> String {
        keys::legacy_request_key(&self.prefix, timeline_key, client_request_id)
    }

    fn request_prefix(&self) -> String {
        keys::request_prefix(&self.prefix)
    }

    fn legacy_request_prefix(&self) -> String {
        keys::legacy_request_prefix(&self.prefix)
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

    fn legacy_request_cleanup_index_key(
        &self,
        record: &RequestRecord,
        timeline_key: &str,
        client_request_id: &str,
    ) -> String {
        keys::legacy_request_cleanup_index_key(
            &self.prefix,
            record.updated_at_ms,
            timeline_key,
            client_request_id,
        )
    }

    fn legacy_request_cleanup_index_prefix(&self) -> String {
        keys::legacy_request_cleanup_index_prefix(&self.prefix)
    }

    fn legacy_request_cleanup_index_cutoff(&self, older_than_ms: u64) -> String {
        keys::legacy_request_cleanup_index_cutoff(&self.prefix, older_than_ms)
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

    fn cluster_format_key(&self) -> String {
        keys::cluster_format_key(&self.prefix)
    }

    fn timeline_creation_lock_key(&self) -> String {
        keys::timeline_creation_lock_key(&self.prefix)
    }

    fn ownership_plan_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests;
