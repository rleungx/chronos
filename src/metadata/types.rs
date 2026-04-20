use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::broadcast;

use crate::{TimelineLifecycleState, TimelineRoute, TsoError};

pub const CURRENT_METADATA_SCHEMA_VERSION: u32 = 1;

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
