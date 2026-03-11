pub mod metadata;
pub mod metrics;
pub mod proto;
pub mod rpc;
pub mod timeline_proxy;
mod runtime;

use std::cmp::{max, Ordering as CmpOrdering};
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{broadcast, Mutex};

use crate::metadata::{
    GeneratorBatchOp, GeneratorRecord, MetadataStore, TimelineRecord,
};
use crate::runtime::{
    Generator, GeneratorLeaseState, GeneratorRuntimeState, TimelineRuntimeState, TimelineState,
};

pub const CUSTOM_EPOCH_UNIX_MS: u64 = 1_767_225_600_000;
pub const PHYSICAL_BITS: u32 = 40;
pub const GENERATOR_ID_BITS: u32 = 13;
pub const SEQUENCE_BITS: u32 = 11;
pub const LOGICAL_BITS: u32 = GENERATOR_ID_BITS + SEQUENCE_BITS;
pub const MAX_GENERATORS: u32 = 1 << GENERATOR_ID_BITS;
pub const SEQUENCE_CAPACITY: u32 = 1 << SEQUENCE_BITS;
pub const MAX_PHYSICAL_MS: u64 = (1u64 << PHYSICAL_BITS) - 1;
pub const GENERATOR_ID_MASK: u64 = (1u64 << GENERATOR_ID_BITS) - 1;
pub const SEQUENCE_MASK: u64 = (1u64 << SEQUENCE_BITS) - 1;

static INSTANCE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceTier {
    Shared,
    Warm,
    Dedicated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimelineLifecycleState {
    Creating,
    Active,
    Draining,
    Locked,
    Recovering,
}

impl fmt::Display for TimelineLifecycleState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimelineLifecycleState::Creating => write!(f, "creating"),
            TimelineLifecycleState::Active => write!(f, "active"),
            TimelineLifecycleState::Draining => write!(f, "draining"),
            TimelineLifecycleState::Locked => write!(f, "locked"),
            TimelineLifecycleState::Recovering => write!(f, "recovering"),
        }
    }
}

impl fmt::Display for ResourceTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResourceTier::Shared => write!(f, "shared"),
            ResourceTier::Warm => write!(f, "warm"),
            ResourceTier::Dedicated => write!(f, "dedicated"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineRoute {
    pub timeline_key: String,
    #[serde(alias = "sequencer_id")]
    pub generator_id: u32,
    pub epoch: u64,
    pub route_version: u64,
    pub resource_tier: ResourceTier,
    pub owner_worker_endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampRange {
    pub start_tso: u64,
    pub end_tso: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocateTimestampsRequest {
    pub timeline_key: String,
    pub count: u32,
    pub expected_epoch: u64,
    pub expected_route_version: u64,
    pub client_request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocateTimestampsResponse {
    pub timeline_key: String,
    pub generator_id: u32,
    pub epoch: u64,
    pub route_version: u64,
    pub ranges: Vec<TimestampRange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedTso {
    pub physical_ms: u64,
    pub logical: u32,
    pub generator_id: u32,
    pub sequence: u32,
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum TsoError {
    #[error("timeline not found: {timeline_key}")]
    TimelineNotFound { timeline_key: String },
    #[error("route version mismatch: expected {expected}, actual {actual}")]
    RouteVersionMismatch { expected: u64, actual: u64 },
    #[error("epoch mismatch: expected {expected}, actual {actual}")]
    EpochMismatch { expected: u64, actual: u64 },
    #[error("count must be positive")]
    InvalidCount,
    #[error("batch too large: requested {requested}, max {max}")]
    BatchTooLarge { requested: u32, max: u32 },
    #[error("future borrow exceeded: requested physical_ms {requested_physical_ms}, allowed {allowed_physical_ms}")]
    FutureBorrowExceeded {
        requested_physical_ms: u64,
        allowed_physical_ms: u64,
    },
    #[error("issued upper bound exceeded: requested_end_tso {requested_end_tso}, issued_upper_bound {issued_upper_bound}")]
    IssuedUpperBoundExceeded {
        requested_end_tso: u64,
        issued_upper_bound: u64,
    },
    #[error("generator pool exhausted")]
    GeneratorPoolExhausted,
    #[error(
        "shared generator jump ahead too large: generator {generator_id} requires {required_jump_ms}ms, threshold {threshold_ms}ms"
    )]
    SharedGeneratorJumpAheadTooLarge {
        generator_id: u32,
        required_jump_ms: u64,
        threshold_ms: u64,
    },
    #[error("generator_id out of range: {generator_id}")]
    GeneratorIdOutOfRange { generator_id: u32 },
    #[error("tso overflow")]
    TsoOverflow,
    #[error("clock went backwards: {delta_ms}ms")]
    ClockBackwards { delta_ms: u64 },
    #[error("lease expired for timeline {timeline_key}")]
    LeaseExpired { timeline_key: String },
    #[error(
        "failover requires expired lease: {timeline_key} lease_expire_at_ms={lease_expire_at_ms}"
    )]
    FailoverRequiresExpiredLease {
        timeline_key: String,
        lease_expire_at_ms: u64,
    },
    #[error(
        "failover missing recovery floor: {timeline_key} previous generator {generator_id} has no persisted floor"
    )]
    FailoverMissingRecoveryFloor {
        timeline_key: String,
        generator_id: u32,
    },
    #[error("lease expired for generator {generator_id}")]
    GeneratorLeaseExpired { generator_id: u32 },
    #[error("timeline not ready: {timeline_key} state={state}")]
    TimelineNotReady {
        timeline_key: String,
        state: TimelineLifecycleState,
    },
    #[error("timeline ingress saturated: timeline_key={timeline_key} max_lanes={max_lanes}")]
    TimelineIngressSaturated {
        timeline_key: String,
        max_lanes: usize,
    },
    #[error("timeline runtime cache saturated: timeline_key={timeline_key} max_entries={max_entries}")]
    TimelineRuntimeCacheSaturated {
        timeline_key: String,
        max_entries: usize,
    },
    #[error("not timeline owner: {owner_worker_endpoint}")]
    NotTimelineOwner { owner_worker_endpoint: String },
    #[error("not generator owner: generator {generator_id} is owned by {owner_worker_endpoint}")]
    NotGeneratorOwner {
        generator_id: u32,
        owner_worker_endpoint: String,
    },
    #[error("generator {generator_id} is not owned by this worker (modulo {modulo} remainder {remainder})")]
    GeneratorNotOwnedByThisWorker {
        generator_id: u32,
        modulo: u32,
        remainder: u32,
    },
    #[error("generator ownership misconfigured: modulo {modulo} remainder {remainder}")]
    GeneratorOwnershipMisconfigured { modulo: u32, remainder: u32 },
    #[error("target_generator_id is required when transferring timeline {timeline_key} to a remote owner")]
    TargetGeneratorIdRequired { timeline_key: String },
    #[error("internal error: {0}")]
    Internal(String),
    #[error("metadata already exists")]
    MetadataAlreadyExists,
    #[error("CAS failed")]
    CasFailed,
}

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_millis() as u64;
        unix_ms.saturating_sub(CUSTOM_EPOCH_UNIX_MS)
    }
}

#[derive(Debug)]
pub struct ManualClock {
    now_ms: std::sync::Mutex<u64>,
}

impl ManualClock {
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms: std::sync::Mutex::new(now_ms),
        }
    }

    pub fn set(&self, now_ms: u64) {
        *self.now_ms.lock().unwrap() = now_ms;
    }

    pub fn advance(&self, delta_ms: u64) {
        let mut guard = self.now_ms.lock().unwrap();
        *guard += delta_ms;
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        *self.now_ms.lock().unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct TsoConfig {
    pub shared_generators: u32,
    pub warm_generators: u32,
    pub max_batch_per_request: u32,
    pub max_future_borrow_ms: u64,
    pub default_resource_tier: ResourceTier,
    pub lease_ttl_ms: u64,
    pub generator_lease_ttl_ms: u64,
    pub generator_maintenance_interval_ms: u64,
    pub generator_ownership_modulo: u32,
    pub generator_ownership_remainder: u32,
    pub worker_id: String,
    pub instance_id: String,
    pub advertise_endpoint: String,
    pub pre_borrow_ms: u64,
    pub max_clock_rewind_ms: u64,
    pub shared_jump_ahead_threshold_ms: u64,
    pub safety_gap_ms: u64,
    pub route_cache_ttl_ms: u32,
    pub max_timeline_proxy_lanes: usize,
    pub max_timeline_runtime_entries: usize,
}

impl Default for TsoConfig {
    fn default() -> Self {
        Self {
            shared_generators: 128,
            warm_generators: 128,
            max_batch_per_request: 16_384,
            max_future_borrow_ms: 10,
            default_resource_tier: ResourceTier::Shared,
            lease_ttl_ms: 3000,
            generator_lease_ttl_ms: 0,
            generator_maintenance_interval_ms: 200,
            generator_ownership_modulo: 1,
            generator_ownership_remainder: 0,
            worker_id: "default-worker".to_owned(),
            instance_id: String::new(),
            advertise_endpoint: "default-endpoint".to_owned(),
            pre_borrow_ms: 1000,
            max_clock_rewind_ms: 30_000,
            shared_jump_ahead_threshold_ms: 5_000,
            safety_gap_ms: 0,
            route_cache_ttl_ms: 60_000,
            max_timeline_proxy_lanes: 65_536,
            max_timeline_runtime_entries: 65_536,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cursor {
    physical_ms: u64,
    sequence: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthInfo {
    pub generator_count: u32,
    pub timeline_count: usize,
    pub worker_id: String,
    pub instance_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferReason {
    Rebalance,
    Hotspot,
    Failover,
    Manual,
}

#[derive(Debug, Clone)]
struct TransferPlan {
    resource_tier: ResourceTier,
    owner_endpoint: String,
    generator_id: Option<u32>,
    reason: TransferReason,
}

pub struct TsoService {
    config: TsoConfig,
    instance_id: String,
    clock: Arc<dyn Clock>,
    metadata: Arc<dyn MetadataStore>,
    generator_runtime: GeneratorRuntimeState,
    timeline_runtime: TimelineRuntimeState,
}

#[derive(Clone)]
pub struct TsoControlPlane {
    inner: Arc<TsoService>,
}

#[derive(Clone)]
pub struct TsoDataPlane {
    inner: Arc<TsoService>,
}

impl TsoControlPlane {
    pub async fn ensure_timeline(&self, timeline_key: &str) -> Result<TimelineRoute, TsoError> {
        self.inner.ensure_timeline(timeline_key).await
    }

    pub async fn ensure_timeline_with_tier(
        &self,
        timeline_key: &str,
        resource_tier: ResourceTier,
    ) -> Result<TimelineRoute, TsoError> {
        self.inner
            .ensure_timeline_with_tier(timeline_key, resource_tier)
            .await
    }

    pub async fn get_timeline_route(&self, timeline_key: &str) -> Result<TimelineRoute, TsoError> {
        self.inner.get_timeline_route(timeline_key).await
    }

    pub async fn renew_timeline_lease(&self, timeline_key: &str) -> Result<(), TsoError> {
        self.inner.renew_timeline_lease(timeline_key).await
    }

    pub async fn transfer_timeline_for_rpc(
        &self,
        timeline_key: &str,
        target_owner_endpoint: String,
        target_generator_id: Option<u32>,
        reason: TransferReason,
    ) -> Result<(TimelineRoute, u32, TimelineLifecycleState), TsoError> {
        self.inner
            .transfer_timeline_for_rpc(
                timeline_key,
                target_owner_endpoint,
                target_generator_id,
                reason,
            )
            .await
    }

    pub fn subscribe_route_changes(&self) -> broadcast::Receiver<TimelineRoute> {
        self.inner.subscribe_route_changes()
    }

    pub fn advertise_endpoint(&self) -> &str {
        self.inner.advertise_endpoint()
    }

    pub fn health(&self) -> HealthInfo {
        self.inner.health()
    }

    pub fn route_cache_ttl_ms(&self) -> u32 {
        self.inner.config.route_cache_ttl_ms
    }
}

impl TsoDataPlane {
    pub async fn allocate_timestamps(
        &self,
        request: AllocateTimestampsRequest,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        self.inner.allocate_timestamps(request).await
    }

    pub fn health(&self) -> HealthInfo {
        self.inner.health()
    }

    pub fn max_timeline_proxy_lanes(&self) -> usize {
        self.inner.config.max_timeline_proxy_lanes
    }
}

impl TsoService {
    pub fn new<C, M>(
        mut config: TsoConfig,
        clock: Arc<C>,
        metadata: Arc<M>,
    ) -> Result<Arc<Self>, TsoError>
    where
        C: Clock + 'static,
        M: MetadataStore + 'static,
    {
        if config.generator_lease_ttl_ms == 0 {
            config.generator_lease_ttl_ms = config.lease_ttl_ms;
        }

        if config.shared_generators + config.warm_generators > MAX_GENERATORS {
            return Err(TsoError::GeneratorIdOutOfRange {
                generator_id: config.shared_generators + config.warm_generators,
            });
        }
        if config.generator_ownership_modulo == 0
            || config.generator_ownership_remainder >= config.generator_ownership_modulo
        {
            return Err(TsoError::GeneratorOwnershipMisconfigured {
                modulo: config.generator_ownership_modulo,
                remainder: config.generator_ownership_remainder,
            });
        }
        let instance_id = if config.instance_id.trim().is_empty() {
            format!(
                "{}#{}",
                config.worker_id,
                INSTANCE_ID_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
            )
        } else {
            config.instance_id.clone()
        };
        let max_timeline_runtime_entries = config.max_timeline_runtime_entries;
        let service = Arc::new(Self {
            config,
            instance_id,
            clock,
            metadata: metadata as Arc<dyn MetadataStore>,
            generator_runtime: GeneratorRuntimeState::new(MAX_GENERATORS),
            timeline_runtime: TimelineRuntimeState::new(max_timeline_runtime_entries),
        });

        let s_clone = service.clone();
        tokio::spawn(async move {
            s_clone.background_generator_maintenance_loop().await;
        });
        let mut metadata_route_updates = service.metadata.subscribe_timeline_routes();
        let route_notifier = service.timeline_runtime.notifier();
        let route_update_service = service.clone();
        tokio::spawn(async move {
            loop {
                match metadata_route_updates.recv().await {
                    Ok(route) => {
                        route_update_service
                            .invalidate_timeline_cache_for_route_update(&route)
                            .await;
                        let _ = route_notifier.send(route);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        route_update_service.timeline_runtime.clear();
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        Ok(service)
    }

    async fn background_generator_maintenance_loop(&self) {
        let mut interval = tokio::time::interval(Duration::from_millis(
            self.config.generator_maintenance_interval_ms,
        ));
        loop {
            interval.tick().await;
            let now_ms = self.clock.now_ms();
            let keys = self.generator_runtime.lease_keys();
            self.refresh_generator_leases_batch(keys, now_ms).await;
        }
    }

    fn owns_generator_id(&self, generator_id: u32) -> bool {
        generator_id % self.config.generator_ownership_modulo
            == self.config.generator_ownership_remainder
    }

    fn is_local_endpoint(&self, owner_endpoint: &str) -> bool {
        owner_endpoint == self.config.advertise_endpoint
    }

    fn local_instance_id(&self) -> &str {
        &self.instance_id
    }

    fn is_local_generator_owner(&self, record: &GeneratorRecord) -> bool {
        self.is_local_endpoint(&record.owner_worker_endpoint)
            && record.owner_instance_id == self.local_instance_id()
    }

    fn timeline_is_ready(state: TimelineLifecycleState) -> bool {
        matches!(state, TimelineLifecycleState::Active | TimelineLifecycleState::Draining)
    }

    fn is_generator_lease_valid(&self, generator_id: u32, now_ms: u64) -> bool {
        self.generator_runtime
            .lease_valid_for_owner(generator_id, self.local_instance_id(), now_ms)
    }

    fn valid_generator_lease_upper_bound(&self, generator_id: u32, now_ms: u64) -> Option<u64> {
        self.generator_runtime.valid_lease_upper_bound_for_owner(
            generator_id,
            self.local_instance_id(),
            now_ms,
        )
    }

    fn clear_timeline_cache(&self, timeline_key: &str) {
        self.timeline_runtime.remove_timeline(timeline_key);
    }

    async fn invalidate_timeline_cache_for_route_update(&self, updated_route: &TimelineRoute) {
        let Some(cached_timeline_state_handle) =
            self.timeline_runtime.timeline_handle(&updated_route.timeline_key)
        else {
            return;
        };

        let should_clear = {
            let cached_timeline_state = cached_timeline_state_handle.lock().await;
            updated_route != &cached_timeline_state.route
                && updated_route.route_version >= cached_timeline_state.route.route_version
        };

        if should_clear {
            self.clear_timeline_cache(&updated_route.timeline_key);
        }
    }

    fn recovered_timeline_floor_tso(timeline_record: &TimelineRecord) -> Option<u64> {
        match (
            timeline_record.last_graceful_issued,
            timeline_record.recovery_floor_tso,
        ) {
            (Some(a), Some(b)) => Some(max(a, b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    fn build_timeline_state(
        timeline_record: &TimelineRecord,
        revision: u64,
        recovered_last_issued_tso: Option<u64>,
    ) -> TimelineState {
        TimelineState {
            route: timeline_record.route.clone(),
            state: timeline_record.state,
            last_issued_tso: recovered_last_issued_tso,
            last_graceful_issued: timeline_record.last_graceful_issued,
            revision,
        }
    }

    async fn activate_local_timeline_record(
        &self,
        timeline_key: &str,
        mut record: TimelineRecord,
        mut revision: u64,
    ) -> Result<(TimelineRecord, u64), TsoError> {
        loop {
            if !self.is_local_endpoint(&record.route.owner_worker_endpoint) {
                return Ok((record, revision));
            }
            self.ensure_generator_lease(record.route.generator_id).await?;
            if record.state == TimelineLifecycleState::Active {
                return Ok((record, revision));
            }

            let mut activated_record = record.clone();
            activated_record.state = TimelineLifecycleState::Active;
            activated_record.updated_at_ms = self.clock.now_ms();
            match self
                .metadata
                .cas_record(timeline_key, revision, &activated_record)
                .await
            {
                Ok(new_revision) => return Ok((activated_record, new_revision)),
                Err(TsoError::CasFailed) => {
                    let (latest, latest_revision) = self
                        .metadata
                        .get_record(timeline_key)
                        .await?
                        .ok_or_else(|| TsoError::TimelineNotFound {
                            timeline_key: timeline_key.to_owned(),
                        })?;
                    record = latest;
                    revision = latest_revision;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn restore_dedicated_claim(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
    ) -> Result<(), TsoError> {
        if record.route.resource_tier != ResourceTier::Dedicated {
            return Ok(());
        }
        if !self.is_local_endpoint(&record.route.owner_worker_endpoint) {
            return Ok(());
        }

        self.claim_specific_dedicated(record.route.generator_id, timeline_key)
            .map(|_| ())
    }

    fn compute_generator_upper_bound(&self, generator_id: u32, base_ms: u64) -> Option<u64> {
        encode_tso(
            base_ms + self.config.pre_borrow_ms,
            generator_id,
            SEQUENCE_CAPACITY - 1,
        )
        .ok()
    }

    fn generator_recovery_floor_tso(generator_record: &GeneratorRecord) -> Option<u64> {
        match (
            generator_record.last_issued_tso,
            generator_record.issued_upper_bound,
        ) {
            (Some(a), Some(b)) => Some(max(a, b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    async fn ensure_generator_lease(&self, generator_id: u32) -> Result<(), TsoError> {
        self.ensure_generator_id_range(generator_id)?;
        if !self.owns_generator_id(generator_id) {
            return Err(TsoError::GeneratorNotOwnedByThisWorker {
                generator_id,
                modulo: self.config.generator_ownership_modulo,
                remainder: self.config.generator_ownership_remainder,
            });
        }

        let now_ms = self.clock.now_ms();
        if self.is_generator_lease_valid(generator_id, now_ms) {
            return Ok(());
        }

        loop {
            match self.metadata.get_generator_record(generator_id).await? {
                Some((record, revision)) => {
                    let exp = record.lease_expire_at_ms.unwrap_or(0);
                    let generator_floor_tso = Self::generator_recovery_floor_tso(&record);
                    if self.is_local_generator_owner(&record) && exp > now_ms {
                        if let Some(generator_floor_tso) = generator_floor_tso {
                            self.lookup_generator(generator_id)?
                                .init_after_floor(generator_floor_tso)?;
                        }
                        self.generator_runtime.upsert_lease(
                            generator_id,
                            GeneratorLeaseState {
                                revision,
                                owner_instance_id: record.owner_instance_id.clone(),
                                generator_lease_token: record.generator_lease_token,
                                lease_expire_at_ms: exp,
                                last_persisted_tso: generator_floor_tso,
                                issued_upper_bound: record.issued_upper_bound,
                            },
                        );
                        return Ok(());
                    }

                    if self.is_local_generator_owner(&record) {
                        return Err(TsoError::GeneratorLeaseExpired { generator_id });
                    }

                    if exp.saturating_add(self.config.safety_gap_ms) <= now_ms {
                        let mut record = record;
                        if let Some(generator_floor_tso) = generator_floor_tso {
                            let floor_cursor = next_cursor_after(generator_floor_tso, generator_id)?;
                            let upper_bound_base_ms = max(now_ms, floor_cursor.physical_ms);
                            record.last_issued_tso = Some(generator_floor_tso);
                            record.issued_upper_bound =
                                self.compute_generator_upper_bound(generator_id, upper_bound_base_ms);
                        } else {
                            record.issued_upper_bound =
                                self.compute_generator_upper_bound(generator_id, now_ms);
                        }
                        record.owner_worker_endpoint = self.config.advertise_endpoint.clone();
                        record.owner_instance_id = self.local_instance_id().to_owned();
                        record.generator_lease_token =
                            record.generator_lease_token.saturating_add(1).max(1);
                        record.lease_expire_at_ms =
                            Some(now_ms + self.config.generator_lease_ttl_ms);
                        record.updated_at_ms = now_ms;
                        match self
                            .metadata
                            .cas_generator_record(generator_id, revision, &record)
                            .await
                        {
                            Ok(new_rev) => {
                                if let Some(generator_floor_tso) =
                                    Self::generator_recovery_floor_tso(&record)
                                {
                                    self.lookup_generator(generator_id)?
                                        .init_after_floor(generator_floor_tso)?;
                                }
                                self.generator_runtime.upsert_lease(
                                    generator_id,
                                    GeneratorLeaseState {
                                        revision: new_rev,
                                        owner_instance_id: record.owner_instance_id.clone(),
                                        generator_lease_token: record.generator_lease_token,
                                        lease_expire_at_ms: record.lease_expire_at_ms.unwrap_or(0),
                                        last_persisted_tso: record.last_issued_tso,
                                        issued_upper_bound: record.issued_upper_bound,
                                    },
                                );
                                return Ok(());
                            }
                            Err(TsoError::CasFailed) => continue,
                            Err(e) => return Err(e),
                        }
                    }

                    return Err(TsoError::NotGeneratorOwner {
                        generator_id,
                        owner_worker_endpoint: record.owner_worker_endpoint,
                    });
                }
                None => {
                    let record = GeneratorRecord {
                        generator_id,
                        owner_worker_endpoint: self.config.advertise_endpoint.clone(),
                        owner_instance_id: self.local_instance_id().to_owned(),
                        generator_lease_token: 1,
                        lease_expire_at_ms: Some(now_ms + self.config.generator_lease_ttl_ms),
                        last_issued_tso: None,
                        issued_upper_bound: self.compute_generator_upper_bound(generator_id, now_ms),
                        updated_at_ms: now_ms,
                    };
                    match self
                        .metadata
                        .create_generator_record(generator_id, &record)
                        .await
                    {
                        Ok(revision) => {
                            self.generator_runtime.upsert_lease(
                                generator_id,
                                GeneratorLeaseState {
                                    revision,
                                    owner_instance_id: record.owner_instance_id.clone(),
                                    generator_lease_token: record.generator_lease_token,
                                    lease_expire_at_ms: record.lease_expire_at_ms.unwrap_or(0),
                                    last_persisted_tso: None,
                                    issued_upper_bound: record.issued_upper_bound,
                                },
                            );
                            return Ok(());
                        }
                        Err(TsoError::MetadataAlreadyExists) => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
        }
    }

    async fn ensure_generator_lease_for_allocation(
        &self,
        timeline_key: &str,
        generator_id: u32,
        now_ms: u64,
    ) -> Result<Option<u64>, TsoError> {
        if let Some(issued_upper_bound) =
            self.valid_generator_lease_upper_bound(generator_id, now_ms)
        {
            return Ok(Some(issued_upper_bound));
        }

        let record = self
            .metadata
            .get_generator_record(generator_id)
            .await?
            .map(|(record, _)| record)
            .ok_or_else(|| TsoError::LeaseExpired {
                timeline_key: timeline_key.to_owned(),
            })?;
        let exp = record.lease_expire_at_ms.unwrap_or(0);
        if !self.is_local_generator_owner(&record) || exp <= now_ms {
            metrics::TSO_LEASE_EXPIRED_TOTAL.inc();
            return Err(TsoError::LeaseExpired {
                timeline_key: timeline_key.to_owned(),
            });
        }

        self.ensure_generator_lease(generator_id).await?;
        Ok(self.valid_generator_lease_upper_bound(generator_id, now_ms))
    }

    async fn refresh_generator_lease(
        &self,
        generator_id: u32,
        now_ms: u64,
    ) -> Result<(), TsoError> {
        self.refresh_generator_lease_inner(generator_id, now_ms, false)
            .await
    }

    async fn refresh_generator_lease_inner(
        &self,
        generator_id: u32,
        now_ms: u64,
        force: bool,
    ) -> Result<(), TsoError> {
        let lease_state = self.generator_runtime.lease_state(generator_id);
        let Some(lease_state) = lease_state else {
            return Ok(());
        };

        if lease_state.lease_expire_at_ms <= now_ms {
            return Err(TsoError::GeneratorLeaseExpired { generator_id });
        }

        let ttl = self.config.generator_lease_ttl_ms;
        let should_renew = lease_state.lease_expire_at_ms <= now_ms.saturating_add(ttl / 2);
        let local_last = self
            .lookup_generator(generator_id)?
            .current_last_issued_tso()?;
        let candidate_last = match (lease_state.last_persisted_tso, local_last) {
            (Some(a), Some(b)) => Some(max(a, b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let current_upper_bound = match (lease_state.issued_upper_bound, candidate_last) {
            (Some(a), Some(b)) => Some(max(a, b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let should_update_last = match (lease_state.last_persisted_tso, candidate_last) {
            (Some(prev), Some(next)) => next > prev,
            (None, Some(_)) => true,
            _ => false,
        };
        let should_extend_upper_bound = current_upper_bound
            .map(|upper_bound| {
                decode_tso(upper_bound).physical_ms
                    <= now_ms.saturating_add(self.config.pre_borrow_ms / 2)
            })
            .unwrap_or(true);

        if !force && !should_renew && !should_update_last && !should_extend_upper_bound {
            return Ok(());
        }

        let base_ms = current_upper_bound
            .map(|upper_bound| decode_tso(upper_bound).physical_ms)
            .map(|upper_bound_ms| max(now_ms, upper_bound_ms))
            .unwrap_or(now_ms);
        let next_upper_bound = self.compute_generator_upper_bound(generator_id, base_ms);

        let record = GeneratorRecord {
            generator_id,
            owner_worker_endpoint: self.config.advertise_endpoint.clone(),
            owner_instance_id: lease_state.owner_instance_id.clone(),
            generator_lease_token: lease_state.generator_lease_token,
            lease_expire_at_ms: Some(now_ms + ttl),
            last_issued_tso: candidate_last,
            issued_upper_bound: match (lease_state.issued_upper_bound, next_upper_bound) {
                (Some(old), Some(new)) => Some(max(old, new)),
                (None, value) => value,
                (value, None) => value,
            },
            updated_at_ms: now_ms,
        };
        match self
            .metadata
            .cas_generator_record(generator_id, lease_state.revision, &record)
            .await
        {
            Ok(new_rev) => {
                self.generator_runtime.upsert_lease(
                    generator_id,
                    GeneratorLeaseState {
                        revision: new_rev,
                        owner_instance_id: record.owner_instance_id.clone(),
                        generator_lease_token: record.generator_lease_token,
                        lease_expire_at_ms: record.lease_expire_at_ms.unwrap_or(0),
                        last_persisted_tso: candidate_last,
                        issued_upper_bound: record.issued_upper_bound,
                    },
                );
                Ok(())
            }
            Err(TsoError::CasFailed) => {
                self.generator_runtime.remove_lease(generator_id);
                self.ensure_generator_lease(generator_id).await
            }
            Err(e) => Err(e),
        }
    }

    async fn refresh_generator_leases_batch(&self, generator_ids: Vec<u32>, now_ms: u64) {
        if generator_ids.is_empty() {
            return;
        }

        let mut operations = Vec::new();
        let mut next_states = Vec::new();
        for generator_id in generator_ids {
            let lease_state = self.generator_runtime.lease_state(generator_id);
            let Some(lease_state) = lease_state else {
                continue;
            };
            if lease_state.lease_expire_at_ms <= now_ms {
                self.generator_runtime.remove_lease(generator_id);
                continue;
            }

            let ttl = self.config.generator_lease_ttl_ms;
            let should_renew = lease_state.lease_expire_at_ms <= now_ms.saturating_add(ttl / 2);
            let local_last = match self
                .lookup_generator(generator_id)
                .and_then(|generator| generator.current_last_issued_tso())
            {
                Ok(value) => value,
                Err(_) => continue,
            };
            let candidate_last = match (lease_state.last_persisted_tso, local_last) {
                (Some(a), Some(b)) => Some(max(a, b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            let current_upper_bound = match (lease_state.issued_upper_bound, candidate_last) {
                (Some(a), Some(b)) => Some(max(a, b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            let should_update_last = match (lease_state.last_persisted_tso, candidate_last) {
                (Some(prev), Some(next)) => next > prev,
                (None, Some(_)) => true,
                _ => false,
            };
            let should_extend_upper_bound = current_upper_bound
                .map(|upper_bound| {
                    decode_tso(upper_bound).physical_ms
                        <= now_ms.saturating_add(self.config.pre_borrow_ms / 2)
                })
                .unwrap_or(true);
            if !should_renew && !should_update_last && !should_extend_upper_bound {
                continue;
            }

            let base_ms = current_upper_bound
                .map(|upper_bound| decode_tso(upper_bound).physical_ms)
                .map(|upper_bound_ms| max(now_ms, upper_bound_ms))
                .unwrap_or(now_ms);
            let next_upper_bound = self.compute_generator_upper_bound(generator_id, base_ms);

            let record = GeneratorRecord {
                generator_id,
                owner_worker_endpoint: self.config.advertise_endpoint.clone(),
                owner_instance_id: lease_state.owner_instance_id.clone(),
                generator_lease_token: lease_state.generator_lease_token,
                lease_expire_at_ms: Some(now_ms + ttl),
                last_issued_tso: candidate_last,
                issued_upper_bound: match (lease_state.issued_upper_bound, next_upper_bound) {
                    (Some(old), Some(new)) => Some(max(old, new)),
                    (None, value) => value,
                    (value, None) => value,
                },
                updated_at_ms: now_ms,
            };
            operations.push(GeneratorBatchOp {
                generator_id,
                previous_revision: lease_state.revision,
                record: record.clone(),
            });
            next_states.push((
                generator_id,
                GeneratorLeaseState {
                    revision: 0,
                    owner_instance_id: record.owner_instance_id.clone(),
                    generator_lease_token: record.generator_lease_token,
                    lease_expire_at_ms: record.lease_expire_at_ms.unwrap_or(0),
                    last_persisted_tso: candidate_last,
                    issued_upper_bound: record.issued_upper_bound,
                },
            ));
        }

        if operations.is_empty() {
            return;
        }

        match self.metadata.cas_generator_records_batch(&operations).await {
            Ok(revisions) => {
                for ((generator_id, mut state), revision) in
                    next_states.into_iter().zip(revisions.into_iter())
                {
                    state.revision = revision;
                    self.generator_runtime.upsert_lease(generator_id, state);
                }
            }
            Err(_) => {
                for operation in operations {
                    let _ = self.refresh_generator_lease(operation.generator_id, now_ms).await;
                }
            }
        }
    }

    async fn upsert_timeline_cache_from_record(
        &self,
        timeline_key: &str,
        timeline_record: &TimelineRecord,
        revision: u64,
    ) -> Result<(), TsoError> {
        self.restore_dedicated_claim(timeline_key, timeline_record)?;

        let cached_timeline_state_handle =
            self.timeline_runtime
                .timeline_handle_or_insert_with(timeline_key, || {
                    Arc::new(Mutex::new(Self::build_timeline_state(
                        timeline_record,
                        revision,
                        Self::recovered_timeline_floor_tso(timeline_record),
                    )))
                })?;

        let mut cached_timeline_state = cached_timeline_state_handle.lock().await;
        cached_timeline_state.route = timeline_record.route.clone();
        cached_timeline_state.state = timeline_record.state;
        cached_timeline_state.last_issued_tso = cached_timeline_state
            .last_issued_tso
            .max(Self::recovered_timeline_floor_tso(timeline_record));
        cached_timeline_state.last_graceful_issued = timeline_record.last_graceful_issued;
        cached_timeline_state.revision = revision;
        Ok(())
    }

    pub fn health(&self) -> HealthInfo {
        HealthInfo {
            generator_count: self.generator_runtime.generator_count(),
            timeline_count: self.timeline_runtime.timeline_count(),
            worker_id: self.config.worker_id.clone(),
            instance_id: self.instance_id.clone(),
        }
    }

    pub fn control_plane(self: &Arc<Self>) -> TsoControlPlane {
        TsoControlPlane {
            inner: self.clone(),
        }
    }

    pub fn data_plane(self: &Arc<Self>) -> TsoDataPlane {
        TsoDataPlane {
            inner: self.clone(),
        }
    }

    pub fn advertise_endpoint(&self) -> &str {
        &self.config.advertise_endpoint
    }

    pub async fn ensure_timeline(&self, timeline_key: &str) -> Result<TimelineRoute, TsoError> {
        self.ensure_timeline_with_tier(timeline_key, self.config.default_resource_tier)
            .await
    }

    pub async fn ensure_timeline_with_tier(
        &self,
        timeline_key: &str,
        resource_tier: ResourceTier,
    ) -> Result<TimelineRoute, TsoError> {
        loop {
            let cached_timeline = self.timeline_runtime.timeline_handle(timeline_key);
            if let Some(timeline_handle) = cached_timeline {
                let timeline = timeline_handle.lock().await;
                if self.is_local_endpoint(&timeline.route.owner_worker_endpoint)
                    && Self::timeline_is_ready(timeline.state)
                {
                    return Ok(timeline.route.clone());
                }
            }

            match self.metadata.get_record(timeline_key).await? {
                Some((record, revision)) => {
                    if self.is_local_endpoint(&record.route.owner_worker_endpoint) {
                        match self
                            .activate_local_timeline_record(timeline_key, record.clone(), revision)
                            .await
                        {
                            Ok((record, revision)) => {
                                self.upsert_timeline_cache_from_record(
                                    timeline_key,
                                    &record,
                                    revision,
                                )
                                .await?;
                                return Ok(record.route);
                            }
                            Err(TsoError::NotGeneratorOwner { .. }) => {
                                self.restore_dedicated_claim(timeline_key, &record)?;
                                return Ok(record.route);
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    return Ok(record.route);
                }
                None => {
                    let generator_id = self.pick_generator_id(timeline_key, resource_tier)?;
                    self.ensure_generator_lease(generator_id).await?;
                    let route = TimelineRoute {
                        timeline_key: timeline_key.to_owned(),
                        generator_id,
                        epoch: 1,
                        route_version: 1,
                        resource_tier,
                        owner_worker_endpoint: self.config.advertise_endpoint.clone(),
                    };

                    let record = TimelineRecord {
                        route: route.clone(),
                        state: TimelineLifecycleState::Active,
                        recovery_floor_tso: None,
                        issued_upper_bound: None,
                        last_graceful_issued: None,
                        lease_expire_at_ms: None,
                        updated_at_ms: self.clock.now_ms(),
                    };

                    match self.metadata.create_record(timeline_key, &record).await {
                        Ok(revision) => {
                            let timeline = Arc::new(Mutex::new(Self::build_timeline_state(
                                &record, revision, None,
                            )));
                            self.timeline_runtime
                                .insert_timeline(timeline_key.to_owned(), timeline)?;
                            return Ok(route);
                        }
                        Err(TsoError::MetadataAlreadyExists) => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
        }
    }

    pub async fn get_timeline_route(&self, timeline_key: &str) -> Result<TimelineRoute, TsoError> {
        // P0: Always fetch authoritative source for Route queries to avoid stale caches in other nodes
        let (record, _) = self
            .metadata
            .get_record(timeline_key)
            .await?
            .ok_or_else(|| TsoError::TimelineNotFound {
                timeline_key: timeline_key.to_owned(),
            })?;

        Ok(record.route)
    }

    pub async fn get_timeline_record(
        &self,
        timeline_key: &str,
    ) -> Result<TimelineRecord, TsoError> {
        let (record, _) = self
            .metadata
            .get_record(timeline_key)
            .await?
            .ok_or_else(|| TsoError::TimelineNotFound {
                timeline_key: timeline_key.to_owned(),
            })?;
        Ok(record)
    }

    pub async fn get_generator_record(
        &self,
        generator_id: u32,
    ) -> Result<GeneratorRecord, TsoError> {
        let (record, _) = self
            .metadata
            .get_generator_record(generator_id)
            .await?
            .ok_or_else(|| TsoError::TimelineNotFound {
                timeline_key: format!("generator:{}", generator_id),
            })?;
        Ok(record)
    }

    pub async fn renew_timeline_lease(&self, timeline_key: &str) -> Result<(), TsoError> {
        let (record, _) = self
            .metadata
            .get_record(timeline_key)
            .await?
            .ok_or_else(|| TsoError::TimelineNotFound {
                timeline_key: timeline_key.to_owned(),
            })?;
        if !self.is_local_endpoint(&record.route.owner_worker_endpoint) {
            return Err(TsoError::NotTimelineOwner {
                owner_worker_endpoint: record.route.owner_worker_endpoint,
            });
        }

        self.ensure_generator_lease(record.route.generator_id).await?;
        self.refresh_generator_lease_inner(record.route.generator_id, self.clock.now_ms(), true)
            .await
    }

    pub async fn transfer_timeline_for_rpc(
        &self,
        timeline_key: &str,
        target_owner_endpoint: String,
        target_generator_id: Option<u32>,
        reason: TransferReason,
    ) -> Result<(TimelineRoute, u32, TimelineLifecycleState), TsoError> {
        let (record, revision) =
            self.metadata
                .get_record(timeline_key)
                .await?
                .ok_or_else(|| TsoError::TimelineNotFound {
                    timeline_key: timeline_key.to_owned(),
                })?;

        if target_owner_endpoint != self.config.advertise_endpoint && target_generator_id.is_none()
        {
            return Err(TsoError::TargetGeneratorIdRequired {
                timeline_key: timeline_key.to_owned(),
            });
        }

        let resource_tier = match target_generator_id {
            Some(generator_id) => self.resource_tier_for_generator_id(generator_id)?,
            None => record.route.resource_tier,
        };
        let transfer = TransferPlan {
            resource_tier,
            owner_endpoint: target_owner_endpoint,
            generator_id: target_generator_id,
            reason,
        };

        self.apply_transfer_plan(timeline_key, record, revision, transfer)
            .await
    }

    async fn apply_transfer_plan(
        &self,
        timeline_key: &str,
        mut record: TimelineRecord,
        revision: u64,
        transfer: TransferPlan,
    ) -> Result<(TimelineRoute, u32, TimelineLifecycleState), TsoError> {
        let previous_route = record.route.clone();
        let old_generator_id = previous_route.generator_id;

        let now = self.clock.now_ms();
        let previous_generator_record = self.metadata.get_generator_record(old_generator_id).await?;
        let previous_generator_floor_tso = previous_generator_record
            .as_ref()
            .and_then(|(generator_record, _)| Self::generator_recovery_floor_tso(generator_record));
        if transfer.reason == TransferReason::Failover {
            if let Some((generator_record, _)) = previous_generator_record.as_ref() {
                let exp = generator_record.lease_expire_at_ms.unwrap_or(0);
                if exp.saturating_add(self.config.safety_gap_ms) > now {
                    return Err(TsoError::FailoverRequiresExpiredLease {
                        timeline_key: timeline_key.to_owned(),
                        lease_expire_at_ms: exp,
                    });
                }
            }
            if previous_generator_floor_tso.is_none() {
                return Err(TsoError::FailoverMissingRecoveryFloor {
                    timeline_key: timeline_key.to_owned(),
                    generator_id: old_generator_id,
                });
            }
        }

        if transfer.reason != TransferReason::Failover
            && self.is_local_endpoint(&previous_route.owner_worker_endpoint)
        {
            if let Some(timeline_handle) = self.timeline_runtime.timeline_handle(timeline_key) {
                let timeline = timeline_handle.lock().await;
                if let Some(last_issued) = timeline.last_issued_tso {
                    record.last_graceful_issued = Some(last_issued);
                }
            }
        }

        let safe_floor = match transfer.reason {
            TransferReason::Failover => previous_generator_floor_tso,
            _ => match (record.last_graceful_issued, previous_generator_floor_tso) {
                (Some(a), Some(b)) => Some(max(a, b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            },
        };

        let mut target_resource_tier = transfer.resource_tier;
        let mut claimed_dedicated = None;
        let mut new_generator_id = match (target_resource_tier, transfer.generator_id) {
            (_, Some(generator_id)) => {
                self.ensure_generator_id_range(generator_id)?;
                if target_resource_tier == ResourceTier::Dedicated {
                    let claimed = self.claim_specific_dedicated(generator_id, timeline_key)?;
                    claimed_dedicated = Some((generator_id, claimed));
                }
                generator_id
            }
            (ResourceTier::Dedicated, None) => {
                let (generator_id, claimed) = self.claim_dedicated(timeline_key)?;
                claimed_dedicated = Some((generator_id, claimed));
                generator_id
            }
            (_, None) => self.pick_generator_id(timeline_key, target_resource_tier)?,
        };

        if target_resource_tier != ResourceTier::Dedicated {
        if let Some(recovery_floor_tso) = safe_floor {
            let required_jump_ms = self
                    .required_jump_ms_for_generator(new_generator_id, recovery_floor_tso)
                    .await?;
                if required_jump_ms > self.config.shared_jump_ahead_threshold_ms {
                    if transfer.generator_id.is_some() {
                        return Err(TsoError::SharedGeneratorJumpAheadTooLarge {
                            generator_id: new_generator_id,
                            required_jump_ms,
                            threshold_ms: self.config.shared_jump_ahead_threshold_ms,
                        });
                    }
                    let (isolated_generator_id, claimed) = self.claim_dedicated(timeline_key)?;
                    claimed_dedicated = Some((isolated_generator_id, claimed));
                    new_generator_id = isolated_generator_id;
                    target_resource_tier = ResourceTier::Dedicated;
                }
            }
        }

        if self.is_local_endpoint(&transfer.owner_endpoint)
            && !self.owns_generator_id(new_generator_id)
        {
            if let Some((generator_id, claimed)) = claimed_dedicated {
                if claimed
                    && (previous_route.resource_tier != ResourceTier::Dedicated
                        || previous_route.generator_id != generator_id)
                {
                    self.release_dedicated(generator_id, timeline_key);
                }
            }
            return Err(TsoError::GeneratorNotOwnedByThisWorker {
                generator_id: new_generator_id,
                modulo: self.config.generator_ownership_modulo,
                remainder: self.config.generator_ownership_remainder,
            });
        }

        record.route.generator_id = new_generator_id;
        record.route.resource_tier = target_resource_tier;
        record.route.epoch += 1;
        record.route.route_version += 1;
        record.route.owner_worker_endpoint = transfer.owner_endpoint.clone();
        record.state = TimelineLifecycleState::Recovering;
        record.recovery_floor_tso = safe_floor;
        record.issued_upper_bound = None;
        record.lease_expire_at_ms = None;
        record.updated_at_ms = now;

        let new_rev = match self
            .metadata
            .cas_record(timeline_key, revision, &record)
            .await
        {
            Ok(rev) => rev,
            Err(e) => {
                if let Some((generator_id, claimed)) = claimed_dedicated {
                    if claimed
                        && (previous_route.resource_tier != ResourceTier::Dedicated
                            || previous_route.generator_id != generator_id)
                    {
                        self.release_dedicated(generator_id, timeline_key);
                    }
                }
                return Err(e);
            }
        };

        if previous_route.resource_tier == ResourceTier::Dedicated
            && (target_resource_tier != ResourceTier::Dedicated
                || previous_route.generator_id != new_generator_id)
        {
            self.release_dedicated(previous_route.generator_id, timeline_key);
        }

        let (record, final_revision) = if self.is_local_endpoint(&transfer.owner_endpoint) {
            self.ensure_generator_lease(new_generator_id).await?;
            self.activate_local_timeline_record(timeline_key, record, new_rev)
                .await?
        } else {
            (record, new_rev)
        };

        let route = record.route.clone();
        if self.is_local_endpoint(&transfer.owner_endpoint) {
            let timeline = Arc::new(Mutex::new(Self::build_timeline_state(
                &record,
                final_revision,
                safe_floor,
            )));
            self.timeline_runtime
                .insert_timeline(timeline_key.to_owned(), timeline)?;
        } else {
            self.clear_timeline_cache(timeline_key);
            if target_resource_tier == ResourceTier::Dedicated {
                self.release_dedicated(new_generator_id, timeline_key);
            }
        }

        Ok((route, old_generator_id, record.state))
    }

    pub async fn transfer_timeline(
        &self,
        timeline_key: &str,
        target_resource_tier: ResourceTier,
        target_owner_endpoint: String,
        target_generator_id: Option<u32>,
    ) -> Result<TimelineRoute, TsoError> {
        if target_owner_endpoint != self.config.advertise_endpoint && target_generator_id.is_none()
        {
            return Err(TsoError::TargetGeneratorIdRequired {
                timeline_key: timeline_key.to_owned(),
            });
        }
        let (record, revision) =
            self.metadata
                .get_record(timeline_key)
                .await?
                .ok_or_else(|| TsoError::TimelineNotFound {
                    timeline_key: timeline_key.to_owned(),
                })?;
        let transfer = TransferPlan {
            resource_tier: target_resource_tier,
            owner_endpoint: target_owner_endpoint,
            generator_id: target_generator_id,
            reason: TransferReason::Manual,
        };
        let (route, _, _) = self
            .apply_transfer_plan(timeline_key, record, revision, transfer)
            .await?;
        Ok(route)
    }

    pub async fn allocate_timestamps(
        &self,
        request: AllocateTimestampsRequest,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        let _timer = metrics::TSO_ALLOCATE_LATENCY.start_timer();

        if request.count == 0 {
            return Err(TsoError::InvalidCount);
        }
        if request.count > self.config.max_batch_per_request {
            return Err(TsoError::BatchTooLarge {
                requested: request.count,
                max: self.config.max_batch_per_request,
            });
        }

        loop {
            let now_ms = self.clock.now_ms();
        let cached_timeline_state = self.timeline_runtime.timeline_handle(&request.timeline_key);

            if let Some(timeline_state_handle) = cached_timeline_state {
                let mut timeline_state = timeline_state_handle.lock().await;
                let route_matches_request = timeline_state.route.route_version
                    == request.expected_route_version
                    && timeline_state.route.epoch == request.expected_epoch;
                let owner_matches =
                    self.is_local_endpoint(&timeline_state.route.owner_worker_endpoint);
                let timeline_ready = Self::timeline_is_ready(timeline_state.state);

                if owner_matches && route_matches_request && timeline_ready {
                    let generator_id = timeline_state.route.generator_id;
                    match self
                        .ensure_generator_lease_for_allocation(
                            &request.timeline_key,
                            generator_id,
                            now_ms,
                        )
                        .await
                    {
                        Ok(issued_upper_bound) => {
                            let generator = self.lookup_generator(generator_id)?;
                            match generator.allocate_after(
                                request.count,
                                timeline_state.last_issued_tso,
                                now_ms,
                                self.config.max_future_borrow_ms,
                                self.config.max_clock_rewind_ms,
                                issued_upper_bound,
                            ) {
                                Ok(ranges) => {
                                    timeline_state.last_issued_tso = ranges.last().map(|r| r.end_tso);
                                    metrics::TSO_ALLOCATE_TOTAL.inc();
                                    return Ok(AllocateTimestampsResponse {
                                        timeline_key: timeline_state.route.timeline_key.clone(),
                                        generator_id,
                                        epoch: timeline_state.route.epoch,
                                        route_version: timeline_state.route.route_version,
                                        ranges,
                                    });
                                }
                                Err(TsoError::IssuedUpperBoundExceeded { .. }) => {
                                    drop(timeline_state);
                                    self.refresh_generator_lease_inner(generator_id, now_ms, true)
                                        .await?;
                                    continue;
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        Err(_) => {
                            drop(timeline_state);
                            self.clear_timeline_cache(&request.timeline_key);
                        }
                    }
                } else if owner_matches && route_matches_request && !timeline_ready {
                    let state = timeline_state.state;
                    drop(timeline_state);
                    self.clear_timeline_cache(&request.timeline_key);
                    return Err(TsoError::TimelineNotReady {
                        timeline_key: request.timeline_key.clone(),
                        state,
                    });
                } else if !owner_matches {
                    drop(timeline_state);
                    self.clear_timeline_cache(&request.timeline_key);
                }
            }

            let (timeline_record, revision) = self
                .metadata
                .get_record(&request.timeline_key)
                .await?
                .ok_or_else(|| TsoError::TimelineNotFound {
                    timeline_key: request.timeline_key.clone(),
                })?;

            if timeline_record.route.route_version != request.expected_route_version {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::RouteVersionMismatch {
                    expected: request.expected_route_version,
                    actual: timeline_record.route.route_version,
                });
            }
            if timeline_record.route.epoch != request.expected_epoch {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::EpochMismatch {
                    expected: request.expected_epoch,
                    actual: timeline_record.route.epoch,
                });
            }

            if !self.is_local_endpoint(&timeline_record.route.owner_worker_endpoint) {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::NotTimelineOwner {
                    owner_worker_endpoint: timeline_record.route.owner_worker_endpoint,
                });
            }

            let (timeline_record, revision) = match self
                .activate_local_timeline_record(&request.timeline_key, timeline_record, revision)
                .await
            {
                Ok(value) => value,
                Err(TsoError::GeneratorLeaseExpired { .. }) => {
                    return Err(TsoError::LeaseExpired {
                        timeline_key: request.timeline_key.clone(),
                    })
                }
                Err(error) => return Err(error),
            };

            if !Self::timeline_is_ready(timeline_record.state) {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::TimelineNotReady {
                    timeline_key: request.timeline_key.clone(),
                    state: timeline_record.state,
                });
            }

            self.ensure_generator_lease_for_allocation(
                &request.timeline_key,
                timeline_record.route.generator_id,
                now_ms,
            )
            .await?;
            self.upsert_timeline_cache_from_record(
                &request.timeline_key,
                &timeline_record,
                revision,
            )
            .await?;
        }
    }

    pub fn subscribe_route_changes(&self) -> broadcast::Receiver<TimelineRoute> {
        self.timeline_runtime.subscribe_route_changes()
    }

    fn lookup_generator(&self, generator_id: u32) -> Result<Arc<Generator>, TsoError> {
        self.ensure_generator_id_range(generator_id)?;
        Ok(self.generator_runtime.generator(generator_id))
    }

    fn ensure_generator_id_range(&self, generator_id: u32) -> Result<(), TsoError> {
        if generator_id >= MAX_GENERATORS {
            return Err(TsoError::GeneratorIdOutOfRange { generator_id });
        }
        Ok(())
    }

    fn resource_tier_for_generator_id(&self, generator_id: u32) -> Result<ResourceTier, TsoError> {
        self.ensure_generator_id_range(generator_id)?;
        let warm_start = self.config.shared_generators;
        let dedicated_start = warm_start + self.config.warm_generators;
        if generator_id < warm_start {
            Ok(ResourceTier::Shared)
        } else if generator_id < dedicated_start {
            Ok(ResourceTier::Warm)
        } else {
            Ok(ResourceTier::Dedicated)
        }
    }

    async fn current_generator_floor(&self, generator_id: u32) -> Result<Option<u64>, TsoError> {
        let local_last = self
            .lookup_generator(generator_id)?
            .current_last_issued_tso()?;
        let persisted_floor = self
            .metadata
            .get_generator_record(generator_id)
            .await?
            .and_then(|(record, _)| Self::generator_recovery_floor_tso(&record));
        Ok(match (local_last, persisted_floor) {
            (Some(a), Some(b)) => Some(max(a, b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        })
    }

    async fn required_jump_ms_for_generator(
        &self,
        generator_id: u32,
        safe_floor: u64,
    ) -> Result<u64, TsoError> {
        let Some(current_floor) = self.current_generator_floor(generator_id).await? else {
            return Ok(0);
        };
        let current_ms = decode_tso(current_floor).physical_ms;
        let target_ms = next_cursor_after(safe_floor, generator_id)?.physical_ms;
        Ok(target_ms.saturating_sub(current_ms))
    }

    fn pick_generator_id(
        &self,
        timeline_key: &str,
        resource_tier: ResourceTier,
    ) -> Result<u32, TsoError> {
        match resource_tier {
            ResourceTier::Shared => {
                self.pick_owned_generator_by_hash(timeline_key, 0, self.config.shared_generators)
            }
            ResourceTier::Warm => self.pick_owned_generator_by_hash(
                timeline_key,
                self.config.shared_generators,
                self.config.warm_generators,
            ),
            ResourceTier::Dedicated => self.claim_dedicated(timeline_key).map(|(id, _)| id),
        }
    }

    fn pick_owned_generator_by_hash(
        &self,
        timeline_key: &str,
        start: u32,
        width: u32,
    ) -> Result<u32, TsoError> {
        if width == 0 {
            return Err(TsoError::GeneratorPoolExhausted);
        }
        let end = start.saturating_add(width);
        let mut owned = Vec::new();
        for generator_id in start..end {
            if self.owns_generator_id(generator_id) {
                owned.push(generator_id);
            }
        }
        if owned.is_empty() {
            return Err(TsoError::GeneratorPoolExhausted);
        }
        let mut hasher = DefaultHasher::new();
        timeline_key.hash(&mut hasher);
        let offset = (hasher.finish() as usize) % owned.len();
        Ok(owned[offset])
    }

    fn claim_dedicated(&self, timeline_key: &str) -> Result<(u32, bool), TsoError> {
        let dedicated_start = self.config.shared_generators + self.config.warm_generators;
        if let Some(generator_id) = self
            .generator_runtime
            .claimed_generator_for_timeline(timeline_key)
        {
            return Ok((generator_id, false));
        }
        for generator_id in dedicated_start..MAX_GENERATORS {
            if !self.owns_generator_id(generator_id) {
                continue;
            }
            match self
                .generator_runtime
                .claim_dedicated(generator_id, timeline_key)
            {
                Ok(claimed) => return Ok((generator_id, claimed)),
                Err(TsoError::GeneratorPoolExhausted) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(TsoError::GeneratorPoolExhausted)
    }

    fn claim_specific_dedicated(
        &self,
        generator_id: u32,
        timeline_key: &str,
    ) -> Result<bool, TsoError> {
        let dedicated_start = self.config.shared_generators + self.config.warm_generators;
        if generator_id < dedicated_start {
            return Err(TsoError::GeneratorIdOutOfRange { generator_id });
        }
        self.generator_runtime
            .claim_dedicated(generator_id, timeline_key)
    }

    fn release_dedicated(&self, generator_id: u32, timeline_key: &str) {
        self.generator_runtime
            .release_dedicated(generator_id, timeline_key);
    }
}

pub fn encode_tso(physical_ms: u64, generator_id: u32, sequence: u32) -> Result<u64, TsoError> {
    if physical_ms > MAX_PHYSICAL_MS {
        return Err(TsoError::TsoOverflow);
    }
    if generator_id >= MAX_GENERATORS {
        return Err(TsoError::GeneratorIdOutOfRange { generator_id });
    }
    if sequence >= SEQUENCE_CAPACITY {
        return Err(TsoError::TsoOverflow);
    }
    Ok((physical_ms << LOGICAL_BITS) | ((generator_id as u64) << SEQUENCE_BITS) | sequence as u64)
}

pub fn decode_tso(tso: u64) -> DecodedTso {
    let physical_ms = (tso >> LOGICAL_BITS) & MAX_PHYSICAL_MS;
    let generator_id = ((tso >> SEQUENCE_BITS) & GENERATOR_ID_MASK) as u32;
    let sequence = (tso & SEQUENCE_MASK) as u32;
    let logical = (tso & ((1u64 << LOGICAL_BITS) - 1)) as u32;
    DecodedTso {
        physical_ms,
        logical,
        generator_id,
        sequence,
    }
}

fn next_cursor_after(tso_floor: u64, target_generator_id: u32) -> Result<Cursor, TsoError> {
    let floor = decode_tso(tso_floor);
    match target_generator_id.cmp(&floor.generator_id) {
        CmpOrdering::Greater => Ok(Cursor {
            physical_ms: floor.physical_ms,
            sequence: 0,
        }),
        CmpOrdering::Equal => {
            if floor.sequence + 1 < SEQUENCE_CAPACITY {
                Ok(Cursor {
                    physical_ms: floor.physical_ms,
                    sequence: floor.sequence + 1,
                })
            } else {
                let physical_ms = floor
                    .physical_ms
                    .checked_add(1)
                    .ok_or(TsoError::TsoOverflow)?;
                Ok(Cursor {
                    physical_ms,
                    sequence: 0,
                })
            }
        }
        CmpOrdering::Less => {
            let physical_ms = floor
                .physical_ms
                .checked_add(1)
                .ok_or(TsoError::TsoOverflow)?;
            Ok(Cursor {
                physical_ms,
                sequence: 0,
            })
        }
    }
}
