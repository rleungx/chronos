use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::{TimelineLifecycleState, TimelineRoute, TsoError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteUpdateSignal {
    Route(TimelineRoute),
    Reset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineRecord {
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
pub struct GeneratorRecord {
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

fn default_timeline_state() -> TimelineLifecycleState {
    TimelineLifecycleState::Active
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

#[async_trait]
pub trait TimelineAuthority: Send + Sync {
    async fn load_timeline(
        &self,
        timeline_key: &str,
    ) -> Result<Option<(TimelineRecord, u64)>, TsoError>;
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
    async fn create_timeline(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError>;
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
            route,
            state: TimelineLifecycleState::Active,
            recovery_floor_tso: None,
            issued_upper_bound: Some(10),
            last_graceful_issued: Some(9),
            lease_expire_at_ms: Some(100),
            updated_at_ms: 1,
        }
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
}
