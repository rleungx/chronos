use thiserror::Error;

use crate::TimelineLifecycleState;

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
    #[error(
        "recovery catch-up budget exceeded: generator {generator_id} requires {required_jump_ms}ms, budget {budget_ms}ms"
    )]
    RecoveryCatchupBudgetExceeded {
        generator_id: u32,
        required_jump_ms: u64,
        budget_ms: u64,
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
    #[error("failover requires known lease expiry: {timeline_key} previous generator lease expiry is missing")]
    FailoverLeaseExpiryUnknown { timeline_key: String },
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
    #[error(
        "timeline runtime cache saturated: timeline_key={timeline_key} max_entries={max_entries}"
    )]
    TimelineRuntimeCacheSaturated {
        timeline_key: String,
        max_entries: usize,
    },
    #[error("generator allocation contention exceeded retry budget: generator {generator_id}")]
    AllocationContention { generator_id: u32 },
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
    #[error("instance identity already in use: {instance_id}")]
    InstanceIdentityInUse { instance_id: String },
    #[error("target_generator_id is required when transferring timeline {timeline_key} to a remote owner")]
    TargetGeneratorIdRequired { timeline_key: String },
    #[error("invalid resource tier: {value}")]
    InvalidResourceTier { value: i32 },
    #[error("target_worker_id must not be blank; pass a target owner endpoint or omit it for local transfer")]
    InvalidTargetOwnerEndpoint,
    #[error("invalid transfer reason: {value}")]
    InvalidTransferReason { value: i32 },
    #[error(
        "unsafe remote transfer target: generator {generator_id} cannot be proven safe for remote owner {target_owner_endpoint}"
    )]
    UnsafeRemoteTransferTarget {
        generator_id: u32,
        target_owner_endpoint: String,
    },
    #[error("internal error: {0}")]
    Internal(String),
    #[error("metadata already exists")]
    MetadataAlreadyExists,
    #[error("CAS failed")]
    CasFailed,
    #[error("service is shutting down")]
    ServiceShuttingDown,
    #[error("request cancelled")]
    RequestCancelled,
}
