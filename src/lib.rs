pub(crate) mod authz;
mod build_info;
pub mod client;
mod clock;
mod config;
mod cursor;
mod error;
mod lease_policy;
#[doc(hidden)]
pub mod lifecycle;
pub mod metadata;
pub mod metrics;
mod plane;
mod planning;
#[doc(hidden)]
pub mod process_runtime;
pub mod proto;
mod recovery;
pub mod rpc;
mod runtime;
mod service;
mod status;
#[cfg(test)]
mod test_tls;
pub mod timeline_proxy;
mod timeline_state;
#[doc(hidden)]
pub mod tls;
mod tso_codec;
mod types;

pub use build_info::{build_commit, build_identity, build_version, BuildIdentity};
pub use client::{Client, ClientConfig, ClientError, ClientTransportConfig};
pub use clock::{Clock, ManualClock, SystemClock};
pub use config::{
    parse_advertise_endpoint_host, TsoConfig, TsoSecurityMode, DEFAULT_ADVERTISE_ENDPOINT,
    DEFAULT_AUTO_FAILOVER_BATCH_SIZE, DEFAULT_AUTO_FAILOVER_INTERVAL_MS, DEFAULT_BIND_ADDR,
    DEFAULT_GRPC_MAX_CONNECTIONS, DEFAULT_MAX_BATCH_PER_REQUEST, DEFAULT_MAX_CLOCK_SKEW_MS,
    DEFAULT_MAX_TIMELINE_PROXY_LANES, DEFAULT_MAX_TIMELINE_RECORDS,
    DEFAULT_MAX_TIMELINE_RUNTIME_ENTRIES, DEFAULT_METADATA_KIND, DEFAULT_METRICS_BIND_ADDR,
    DEFAULT_REQUEST_RECORD_CLEANUP_BATCH_SIZE, DEFAULT_REQUEST_RECORD_CLEANUP_INTERVAL_MS,
    DEFAULT_REQUEST_RECORD_PENDING_TIMEOUT_MS, DEFAULT_REQUEST_RECORD_RETENTION_MS,
    DEFAULT_WORKER_ID, PRODUCTION_MAX_BATCH_PER_REQUEST, PRODUCTION_MAX_TIMELINE_PROXY_LANES,
    PRODUCTION_MAX_TIMELINE_RUNTIME_ENTRIES,
};
pub(crate) use cursor::next_cursor_after;
pub use error::TsoError;
pub(crate) use lease_policy::lease_expired_with_safety_gap;
pub use plane::{TsoControlPlane, TsoDataPlane};
pub use service::{OwnershipDriftEvidence, TsoService, WorkerReadinessSink};
pub use tso_codec::{
    checked_physical_ms_from_unix_ms, decode_tso, encode_tso, TsoCapacityEnvelope,
    TsoUnixMsBoundary, CUSTOM_EPOCH_UNIX_MS, GENERATOR_ID_BITS, GENERATOR_ID_MASK, LOGICAL_BITS,
    MAX_GENERATORS, MAX_PHYSICAL_MS, MAX_UNIX_MS, PHYSICAL_BITS, SEQUENCE_BITS, SEQUENCE_CAPACITY,
    SEQUENCE_MASK, TSO_CAPACITY_ENVELOPE,
};
pub use types::{
    AllocateTimestampsRequest, AllocateTimestampsResponse, DecodedTso, HealthInfo, ResourceTier,
    TimelineLifecycleState, TimelineRoute, TimestampRange, TransferReason,
};
