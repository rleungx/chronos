use std::fmt;

use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthInfo {
    pub generator_count: u32,
    pub timeline_count: usize,
    pub worker_id: String,
    pub instance_id: String,
    pub advertise_endpoint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferReason {
    Rebalance,
    Hotspot,
    Failover,
    Manual,
}
