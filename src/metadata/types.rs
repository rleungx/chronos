use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use tokio::sync::broadcast;

use crate::{
    AllocateTimestampsResponse, TimelineLifecycleState, TimelineRoute, TimestampRange, TsoError,
};

pub const CURRENT_METADATA_SCHEMA_VERSION: u32 = 1;
/// Cluster-wide writer format. A change requires a quiesced, all-at-once worker upgrade.
pub const CURRENT_CLUSTER_FORMAT_VERSION: u32 = 2;

fn default_metadata_schema_version() -> u32 {
    CURRENT_METADATA_SCHEMA_VERSION
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteUpdateSignal {
    Route(TimelineRoute),
    Reset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineRecord {
    #[serde(default = "default_metadata_schema_version")]
    pub schema_version: u32,
    pub route: TimelineRoute,
    #[serde(default = "default_timeline_state")]
    pub state: TimelineLifecycleState,
    #[serde(default)]
    pub recovery_floor_tso: Option<u64>,
    pub issued_upper_bound: Option<u64>,
    pub last_graceful_issued: Option<u64>,
    pub lease_expire_at_ms: Option<u64>,
    #[serde(default)]
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineFilterRecord {
    #[serde(default = "default_metadata_schema_version")]
    pub schema_version: u32,
    pub route: TimelineRoute,
    #[serde(default = "default_timeline_state")]
    pub state: TimelineLifecycleState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineRouteRecord {
    #[serde(default = "default_metadata_schema_version")]
    pub schema_version: u32,
    pub route: TimelineRoute,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratorRecord {
    #[serde(default = "default_metadata_schema_version")]
    pub schema_version: u32,
    pub generator_id: u32,
    pub owner_worker_endpoint: String,
    #[serde(default)]
    pub owner_instance_id: String,
    #[serde(default)]
    pub generator_lease_token: u64,
    pub lease_expire_at_ms: Option<u64>,
    pub last_issued_tso: Option<u64>,
    pub issued_upper_bound: Option<u64>,
    #[serde(default)]
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipPlanMember {
    pub remainder: u32,
    pub worker_id: String,
    pub advertise_endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipPlanRecord {
    #[serde(default = "default_metadata_schema_version")]
    pub schema_version: u32,
    pub plan_id: String,
    pub modulo: u32,
    pub members: Vec<OwnershipPlanMember>,
    #[serde(default)]
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestRecordState {
    Pending,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllocationRequestFingerprint {
    #[serde(default)]
    pub timeline_key: String,
    pub count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllocationResponseRecord {
    #[serde(default)]
    pub timeline_key: String,
    pub generator_id: u32,
    pub epoch: u64,
    pub route_version: u64,
    pub ranges: Vec<TimestampRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestRecord {
    #[serde(default = "default_metadata_schema_version")]
    pub schema_version: u32,
    pub fingerprint: AllocationRequestFingerprint,
    pub state: RequestRecordState,
    #[serde(default)]
    pub response: Option<AllocationResponseRecord>,
    #[serde(default)]
    pub updated_at_ms: u64,
}

fn default_timeline_state() -> TimelineLifecycleState {
    TimelineLifecycleState::Active
}

impl TimelineRecord {
    pub fn validate_schema_version(&self) -> Result<(), TsoError> {
        if self.schema_version == CURRENT_METADATA_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(TsoError::Internal(format!(
                "unsupported timeline metadata schema_version {}",
                self.schema_version
            )))
        }
    }

    pub fn stamped_for_persistence(&self) -> Self {
        let mut record = self.clone();
        record.schema_version = CURRENT_METADATA_SCHEMA_VERSION;
        record
    }
}

impl OwnershipPlanRecord {
    pub fn new(
        plan_id: String,
        modulo: u32,
        member: OwnershipPlanMember,
        updated_at_ms: u64,
    ) -> Self {
        Self {
            schema_version: CURRENT_METADATA_SCHEMA_VERSION,
            plan_id,
            modulo,
            members: vec![member],
            updated_at_ms,
        }
    }

    pub fn validate_schema_version(&self) -> Result<(), TsoError> {
        if self.schema_version == CURRENT_METADATA_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(TsoError::Internal(format!(
                "unsupported ownership plan metadata schema_version {}",
                self.schema_version
            )))
        }
    }

    pub fn admit_member(
        &mut self,
        expected_plan_id: &str,
        expected_modulo: u32,
        member: OwnershipPlanMember,
        updated_at_ms: u64,
    ) -> Result<bool, TsoError> {
        self.validate_schema_version()?;
        self.validate_integrity()?;
        if self.plan_id != expected_plan_id {
            return Err(TsoError::Internal(format!(
                "ownership plan mismatch: metadata plan_id={} local plan_id={}",
                self.plan_id, expected_plan_id
            )));
        }
        if self.modulo != expected_modulo {
            return Err(TsoError::Internal(format!(
                "ownership plan modulo mismatch: metadata modulo={} local modulo={}",
                self.modulo, expected_modulo
            )));
        }
        if member.remainder >= self.modulo {
            return Err(TsoError::Internal(format!(
                "ownership plan member remainder {} is outside modulo {}",
                member.remainder, self.modulo
            )));
        }

        for existing in &self.members {
            if existing.remainder == member.remainder {
                if existing.worker_id == member.worker_id
                    && existing.advertise_endpoint == member.advertise_endpoint
                {
                    return Ok(false);
                }
                return Err(TsoError::Internal(format!(
                    "ownership plan remainder {} is already assigned to worker_id={} advertise_endpoint={}",
                    existing.remainder, existing.worker_id, existing.advertise_endpoint
                )));
            }
        }

        self.members.push(member);
        self.members.sort_by_key(|member| member.remainder);
        self.updated_at_ms = updated_at_ms;
        Ok(true)
    }

    pub fn replace_if_empty(
        &mut self,
        expected_plan_id: &str,
        expected_modulo: u32,
        updated_at_ms: u64,
    ) -> Result<bool, TsoError> {
        self.validate_schema_version()?;
        self.validate_integrity()?;
        if !self.members.is_empty()
            || (self.plan_id == expected_plan_id && self.modulo == expected_modulo)
        {
            return Ok(false);
        }
        self.plan_id = expected_plan_id.to_owned();
        self.modulo = expected_modulo;
        self.updated_at_ms = updated_at_ms;
        Ok(true)
    }

    pub fn prune_inactive_members(
        &mut self,
        active_members: &HashSet<(String, String)>,
        updated_at_ms: u64,
    ) -> Result<usize, TsoError> {
        self.validate_schema_version()?;
        self.validate_integrity()?;

        let original_len = self.members.len();
        self.members.retain(|member| {
            active_members.contains(&(member.worker_id.clone(), member.advertise_endpoint.clone()))
        });
        let pruned = original_len.saturating_sub(self.members.len());
        if pruned > 0 {
            self.updated_at_ms = updated_at_ms;
        }
        Ok(pruned)
    }

    fn validate_integrity(&self) -> Result<(), TsoError> {
        if self.plan_id.trim().is_empty() {
            return Err(TsoError::Internal(
                "ownership plan metadata plan_id must not be blank".into(),
            ));
        }
        if self.modulo == 0 {
            return Err(TsoError::Internal(
                "ownership plan metadata modulo must be greater than 0".into(),
            ));
        }

        let mut remainders = HashSet::new();
        for member in &self.members {
            if member.remainder >= self.modulo {
                return Err(TsoError::Internal(format!(
                    "ownership plan member remainder {} is outside modulo {}",
                    member.remainder, self.modulo
                )));
            }
            if !remainders.insert(member.remainder) {
                return Err(TsoError::Internal(format!(
                    "ownership plan contains duplicate remainder {}",
                    member.remainder
                )));
            }
        }
        Ok(())
    }
}

impl TimelineFilterRecord {
    pub fn validate_schema_version(&self) -> Result<(), TsoError> {
        if self.schema_version >= CURRENT_METADATA_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(TsoError::Internal(format!(
                "unsupported timeline metadata schema_version {}",
                self.schema_version
            )))
        }
    }
}

impl TimelineRouteRecord {
    pub fn validate_schema_version(&self) -> Result<(), TsoError> {
        if self.schema_version >= CURRENT_METADATA_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(TsoError::Internal(format!(
                "unsupported timeline metadata schema_version {}",
                self.schema_version
            )))
        }
    }
}

impl GeneratorRecord {
    pub fn validate_schema_version(&self) -> Result<(), TsoError> {
        if self.schema_version == CURRENT_METADATA_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(TsoError::Internal(format!(
                "unsupported generator metadata schema_version {}",
                self.schema_version
            )))
        }
    }

    pub fn stamped_for_persistence(&self) -> Self {
        let mut record = self.clone();
        record.schema_version = CURRENT_METADATA_SCHEMA_VERSION;
        record
    }
}

impl RequestRecord {
    pub fn validate_schema_version(&self) -> Result<(), TsoError> {
        if self.schema_version == CURRENT_METADATA_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(TsoError::Internal(format!(
                "unsupported request metadata schema_version {}",
                self.schema_version
            )))
        }
    }

    pub fn stamped_for_persistence(&self) -> Self {
        let mut record = self.clone();
        record.schema_version = CURRENT_METADATA_SCHEMA_VERSION;
        record
    }

    pub fn completed_response(
        &self,
        timeline_key: &str,
    ) -> Result<Option<AllocateTimestampsResponse>, TsoError> {
        match (&self.state, &self.response) {
            (RequestRecordState::Pending, _) => Ok(None),
            (RequestRecordState::Completed, Some(response)) => {
                if !response.timeline_key.is_empty() && response.timeline_key != timeline_key {
                    return Err(TsoError::Internal(format!(
                        "request response timeline mismatch: stored={} requested={}",
                        response.timeline_key, timeline_key
                    )));
                }
                Ok(Some(AllocateTimestampsResponse {
                    timeline_key: timeline_key.to_string(),
                    generator_id: response.generator_id,
                    epoch: response.epoch,
                    route_version: response.route_version,
                    ranges: response.ranges.clone(),
                }))
            }
            (RequestRecordState::Completed, None) => Err(TsoError::Internal(format!(
                "completed request record missing response for timeline {}",
                timeline_key
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimelineBatchOp {
    pub timeline_key: String,
    pub previous_revision: u64,
    pub record: TimelineRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineRecordListPage {
    pub records: Vec<TimelineRecord>,
    pub next_start_after_timeline_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineFilterRecordListPage {
    pub records: Vec<TimelineFilterRecord>,
    pub next_start_after_timeline_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GeneratorBatchOp {
    pub generator_id: u32,
    pub previous_revision: u64,
    pub record: GeneratorRecord,
}

pub(super) fn timeline_route_changed(
    previous_record: Option<&TimelineRecord>,
    next_record: &TimelineRecord,
) -> bool {
    timeline_route_update_from_routes(
        previous_record.map(|record| &record.route),
        &next_record.route,
    )
    .is_some()
}

pub(super) fn timeline_route_update_from_routes(
    previous_route: Option<&TimelineRoute>,
    next_route: &TimelineRoute,
) -> Option<TimelineRoute> {
    previous_route
        .map(|route| route != next_route)
        .unwrap_or(true)
        .then(|| next_route.clone())
}

pub(super) fn timeline_route_update(
    previous_record: Option<&TimelineRecord>,
    next_record: &TimelineRecord,
) -> Option<TimelineRoute> {
    timeline_route_changed(previous_record, next_record).then(|| next_record.route.clone())
}

pub(super) fn collect_timeline_route_update(
    previous_record: Option<&TimelineRecord>,
    next_record: &TimelineRecord,
    route_updates: &mut Vec<TimelineRoute>,
) {
    if let Some(route_update) = timeline_route_update(previous_record, next_record) {
        route_updates.push(route_update);
    }
}

fn owner_endpoint_matches(left: &str, right: &str) -> bool {
    let left = left.trim();
    let right = right.trim();
    if left == right || left.eq_ignore_ascii_case(right) {
        return true;
    }

    match (left.parse::<SocketAddr>(), right.parse::<SocketAddr>()) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn timeline_matches_status_filter(
    record: &TimelineRecord,
    states: &[TimelineLifecycleState],
    owner_worker_endpoint: Option<&str>,
) -> bool {
    (states.is_empty() || states.contains(&record.state))
        && owner_worker_endpoint
            .map(|endpoint| owner_endpoint_matches(&record.route.owner_worker_endpoint, endpoint))
            .unwrap_or(true)
}

#[async_trait]
pub trait TimelineAuthority: Send + Sync {
    async fn load_timeline(
        &self,
        timeline_key: &str,
    ) -> Result<Option<(TimelineRecord, u64)>, TsoError>;
    async fn load_timeline_route(
        &self,
        timeline_key: &str,
    ) -> Result<Option<(TimelineRouteRecord, u64)>, TsoError> {
        self.load_timeline(timeline_key).await.map(|loaded| {
            loaded.map(|(record, revision)| {
                (
                    TimelineRouteRecord {
                        schema_version: record.schema_version,
                        route: record.route,
                    },
                    revision,
                )
            })
        })
    }
    async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError>;
    async fn list_timelines_page(
        &self,
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<TimelineRecordListPage, TsoError> {
        if limit == 0 {
            return Ok(TimelineRecordListPage {
                records: Vec::new(),
                next_start_after_timeline_key: None,
            });
        }

        let mut records = self.list_timelines().await?;
        records
            .sort_unstable_by(|left, right| left.route.timeline_key.cmp(&right.route.timeline_key));
        let start_index = start_after_timeline_key
            .map(|start_after| {
                records.partition_point(|record| record.route.timeline_key.as_str() <= start_after)
            })
            .unwrap_or(0);
        let mut page_records: Vec<_> = records
            .into_iter()
            .skip(start_index)
            .take(limit + 1)
            .collect();
        let next_start_after_timeline_key = (page_records.len() > limit)
            .then(|| page_records[limit - 1].route.timeline_key.clone());
        page_records.truncate(limit);

        Ok(TimelineRecordListPage {
            records: page_records,
            next_start_after_timeline_key,
        })
    }
    async fn list_timeline_filters_page(
        &self,
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<TimelineFilterRecordListPage, TsoError> {
        let page = self
            .list_timelines_page(start_after_timeline_key, limit)
            .await?;
        Ok(TimelineFilterRecordListPage {
            records: page
                .records
                .into_iter()
                .map(|record| TimelineFilterRecord {
                    schema_version: record.schema_version,
                    route: record.route,
                    state: record.state,
                })
                .collect(),
            next_start_after_timeline_key: page.next_start_after_timeline_key,
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
        if limit == 0 {
            return Ok(TimelineRecordListPage {
                records: Vec::new(),
                next_start_after_timeline_key: None,
            });
        }

        let mut scan_cursor = start_after_timeline_key.map(str::to_owned);
        let mut matched = Vec::with_capacity(limit + 1);
        while matched.len() <= limit {
            let page = self
                .list_timelines_page(scan_cursor.as_deref(), limit)
                .await?;
            if page.records.is_empty() {
                break;
            }
            scan_cursor = page.next_start_after_timeline_key.clone();
            for record in page.records {
                if timeline_matches_status_filter(&record, states, owner_worker_endpoint) {
                    matched.push(record);
                    if matched.len() > limit {
                        break;
                    }
                }
            }
            if scan_cursor.is_none() {
                break;
            }
        }

        let next_start_after_timeline_key =
            (matched.len() > limit).then(|| matched[limit - 1].route.timeline_key.clone());
        matched.truncate(limit);

        Ok(TimelineRecordListPage {
            records: matched,
            next_start_after_timeline_key,
        })
    }
    async fn create_timeline(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError>;
    async fn create_timeline_with_limit(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
        max_timelines: usize,
    ) -> Result<u64, TsoError> {
        let page = self.list_timelines_page(None, max_timelines).await?;
        if page.records.len() >= max_timelines {
            return Err(TsoError::TimelineLimitReached { max: max_timelines });
        }
        self.create_timeline(timeline_key, record).await
    }
    async fn compare_exchange_timeline(
        &self,
        timeline_key: &str,
        expected_revision: u64,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError>;
    async fn compare_exchange_timelines(
        &self,
        operations: &[TimelineBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let mut revisions = Vec::with_capacity(operations.len());
        for operation in operations {
            revisions.push(
                self.compare_exchange_timeline(
                    &operation.timeline_key,
                    operation.previous_revision,
                    &operation.record,
                )
                .await?,
            );
        }
        Ok(revisions)
    }
}

#[async_trait]
pub trait GeneratorLeaseAuthority: Send + Sync {
    async fn load_generator(
        &self,
        generator_id: u32,
    ) -> Result<Option<(GeneratorRecord, u64)>, TsoError>;
    async fn load_generators(
        &self,
        generator_ids: &[u32],
    ) -> Result<HashMap<u32, Option<GeneratorRecord>>, TsoError> {
        let mut loaded = HashMap::with_capacity(generator_ids.len());
        for &generator_id in generator_ids {
            loaded.insert(
                generator_id,
                self.load_generator(generator_id)
                    .await?
                    .map(|(record, _)| record),
            );
        }
        Ok(loaded)
    }
    async fn scan_generators(&self) -> Result<Vec<GeneratorRecord>, TsoError> {
        let generator_ids = (0..crate::MAX_GENERATORS).collect::<Vec<_>>();
        Ok(self
            .load_generators(&generator_ids)
            .await?
            .into_values()
            .flatten()
            .collect())
    }
    async fn create_generator(
        &self,
        generator_id: u32,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError>;
    async fn compare_exchange_generator(
        &self,
        generator_id: u32,
        expected_revision: u64,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError>;
    async fn compare_exchange_generators(
        &self,
        operations: &[GeneratorBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let mut revisions = Vec::with_capacity(operations.len());
        for operation in operations {
            revisions.push(
                self.compare_exchange_generator(
                    operation.generator_id,
                    operation.previous_revision,
                    &operation.record,
                )
                .await?,
            );
        }
        Ok(revisions)
    }
}

#[async_trait]
pub trait RequestRecordAuthority: Send + Sync {
    async fn load_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
    ) -> Result<Option<(RequestRecord, u64)>, TsoError>;
    async fn create_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> Result<u64, TsoError>;
    async fn compare_exchange_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        expected_revision: u64,
        record: &RequestRecord,
    ) -> Result<u64, TsoError>;
    async fn compare_delete_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        expected_revision: u64,
    ) -> Result<(), TsoError>;
    async fn prune_completed_request_records(
        &self,
        older_than_ms: u64,
        limit: usize,
    ) -> Result<usize, TsoError> {
        let _ = older_than_ms;
        let _ = limit;
        Ok(0)
    }
}

/// Subscribes to timeline route-change signals from the metadata backend.
///
/// These updates are eventually consistent convergence signals, not a synchronous write
/// acknowledgement. Consumers must not assume that a successful metadata write immediately emits a
/// route update on this channel.
///
/// Backend boundary:
/// - `EtcdMetadataStore` emits route updates from its watch path after etcd watch events are
///   applied. Its write path does not directly broadcast.
/// - `MemoryMetadataStore` emits route updates directly from its write paths after applying the
///   in-process mutation.
pub trait RouteUpdateSource: Send + Sync {
    fn subscribe_route_updates(&self) -> broadcast::Receiver<RouteUpdateSignal>;
}

#[async_trait]
pub trait ControlPlaneStore:
    TimelineAuthority + GeneratorLeaseAuthority + RouteUpdateSource + Send + Sync
{
    fn request_records(&self) -> Option<&dyn RequestRecordAuthority> {
        None
    }

    async fn shutdown(&self) {}
}

#[cfg(test)]
mod tests {
    use crate::ResourceTier;

    use super::*;

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

    fn sample_record(route: TimelineRoute) -> TimelineRecord {
        TimelineRecord {
            schema_version: 1,
            route,
            state: TimelineLifecycleState::Active,
            recovery_floor_tso: None,
            issued_upper_bound: Some(10),
            last_graceful_issued: Some(9),
            lease_expire_at_ms: Some(100),
            updated_at_ms: 1,
        }
    }

    fn plan_member(remainder: u32, worker_id: &str, endpoint: &str) -> OwnershipPlanMember {
        OwnershipPlanMember {
            remainder,
            worker_id: worker_id.into(),
            advertise_endpoint: endpoint.into(),
        }
    }

    #[test]
    fn ownership_plan_accepts_unique_members_and_rejects_mixed_modulo() {
        let mut plan = OwnershipPlanRecord::new(
            "plan-a".into(),
            2,
            plan_member(0, "worker-a", "worker-a:50051"),
            1,
        );

        assert_eq!(
            plan.admit_member("plan-a", 2, plan_member(1, "worker-b", "worker-b:50051"), 2),
            Ok(true)
        );
        assert_eq!(
            plan.admit_member("plan-a", 2, plan_member(1, "worker-b", "worker-b:50051"), 3),
            Ok(false)
        );
        assert!(matches!(
            plan.admit_member("plan-a", 4, plan_member(2, "worker-c", "worker-c:50051"), 4),
            Err(TsoError::Internal(_))
        ));
    }

    #[test]
    fn ownership_plan_rejects_duplicate_remainder_and_allows_multi_shard_worker() {
        let mut plan = OwnershipPlanRecord::new(
            "plan-a".into(),
            3,
            plan_member(0, "worker-a", "worker-a:50051"),
            1,
        );

        assert!(matches!(
            plan.admit_member("plan-a", 3, plan_member(0, "worker-b", "worker-b:50051"), 2),
            Err(TsoError::Internal(_))
        ));
        assert!(plan
            .admit_member("plan-a", 3, plan_member(1, "worker-a", "worker-a:50051"), 2)
            .expect("same worker should be allowed to own multiple remainders"));
        assert_eq!(plan.members.len(), 2);
    }

    #[test]
    fn ownership_plan_prunes_inactive_members_before_replacement() {
        let mut plan = OwnershipPlanRecord::new(
            "plan-a".into(),
            2,
            plan_member(0, "worker-a", "worker-a:50051"),
            1,
        );
        let active_members =
            HashSet::from([("worker-b".to_string(), "worker-b:50051".to_string())]);

        let pruned = plan
            .prune_inactive_members(&active_members, 2)
            .expect("inactive ownership member should prune");
        assert_eq!(pruned, 1);
        assert!(plan.members.is_empty());
        assert_eq!(plan.updated_at_ms, 2);

        assert!(plan
            .admit_member("plan-a", 2, plan_member(0, "worker-b", "worker-b:50051"), 3)
            .expect("replacement member should be admitted after stale member pruning"));
    }

    #[test]
    fn ownership_plan_can_change_identity_only_after_all_old_members_are_inactive() {
        let mut plan = OwnershipPlanRecord::new(
            "plan-a".into(),
            2,
            plan_member(0, "worker-a", "worker-a:50051"),
            1,
        );
        assert!(!plan.replace_if_empty("plan-b", 4, 2).unwrap());

        plan.prune_inactive_members(&HashSet::new(), 3).unwrap();
        assert!(plan.replace_if_empty("plan-b", 4, 4).unwrap());
        assert_eq!(plan.plan_id, "plan-b");
        assert_eq!(plan.modulo, 4);
        assert!(plan.members.is_empty());
    }

    #[test]
    fn timeline_route_update_from_routes_returns_none_for_same_route() {
        let route = sample_route(7, 1);

        assert_eq!(
            timeline_route_update_from_routes(Some(&route), &route),
            None
        );
    }

    #[test]
    fn timeline_route_update_from_routes_returns_next_route_for_changes() {
        let previous = sample_route(7, 1);
        let next = sample_route(8, 2);

        assert_eq!(
            timeline_route_update_from_routes(Some(&previous), &next),
            Some(next)
        );
    }

    #[test]
    fn timeline_route_update_from_routes_treats_unknown_previous_as_changed() {
        let next = sample_route(7, 1);

        assert_eq!(timeline_route_update_from_routes(None, &next), Some(next));
    }

    #[test]
    fn timeline_route_changed_still_tracks_record_routes() {
        let previous = sample_record(sample_route(7, 1));
        let next = sample_record(sample_route(8, 2));

        assert!(timeline_route_changed(Some(&previous), &next));
        assert!(!timeline_route_changed(Some(&next), &next));
    }

    #[test]
    fn timeline_record_rejects_unknown_schema_version() {
        let mut record = sample_record(sample_route(7, 1));
        record.schema_version = CURRENT_METADATA_SCHEMA_VERSION + 1;

        assert!(matches!(
            record.validate_schema_version(),
            Err(TsoError::Internal(_))
        ));
    }

    #[test]
    fn timeline_filter_record_rejects_unknown_schema_version() {
        let mut record = TimelineFilterRecord {
            schema_version: CURRENT_METADATA_SCHEMA_VERSION.saturating_sub(1),
            route: sample_route(7, 1),
            state: TimelineLifecycleState::Active,
        };

        assert!(matches!(
            record.validate_schema_version(),
            Err(TsoError::Internal(_))
        ));

        record.schema_version = CURRENT_METADATA_SCHEMA_VERSION;
        assert!(record.validate_schema_version().is_ok());

        record.schema_version = CURRENT_METADATA_SCHEMA_VERSION + 1;
        assert!(record.validate_schema_version().is_ok());
    }

    #[test]
    fn timeline_route_record_accepts_newer_schema_version() {
        let record = TimelineRouteRecord {
            schema_version: CURRENT_METADATA_SCHEMA_VERSION + 1,
            route: sample_route(7, 1),
        };

        assert!(record.validate_schema_version().is_ok());
    }

    #[test]
    fn timeline_route_record_rejects_older_schema_version() {
        let record = TimelineRouteRecord {
            schema_version: CURRENT_METADATA_SCHEMA_VERSION.saturating_sub(1),
            route: sample_route(7, 1),
        };

        assert!(matches!(
            record.validate_schema_version(),
            Err(TsoError::Internal(_))
        ));
    }
}
