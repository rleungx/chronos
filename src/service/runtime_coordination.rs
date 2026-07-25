mod activation;
mod cache;
mod loading;

pub(in crate::service) use loading::TimelineLoadCoordinator;

use std::collections::HashMap;

use crate::lifecycle::{TimelineLifecycleContract, TimelineServingReadiness};
use crate::metadata::{GeneratorRecord, TimelineRecord};
use crate::status::{
    build_timeline_status_snapshot, TimelineStatusListPage, TimelineStatusSnapshot,
};
use crate::{TimelineLifecycleState, TimelineRoute, TsoError};

use super::TsoService;

impl TsoService {
    pub(super) fn timeline_is_ready(state: TimelineLifecycleState) -> bool {
        matches!(
            TimelineLifecycleContract::classify(state).serving_readiness(),
            TimelineServingReadiness::Serving
        )
    }

    pub(super) fn is_generator_lease_valid(&self, generator_id: u32, now_ms: u64) -> bool {
        self.generator_runtime
            .lease_valid_for_owner(generator_id, self.local_instance_id(), now_ms)
    }

    pub(super) fn valid_generator_lease_upper_bound(
        &self,
        generator_id: u32,
        now_ms: u64,
    ) -> Option<u64> {
        self.generator_runtime.valid_lease_upper_bound_for_owner(
            generator_id,
            self.local_instance_id(),
            now_ms,
        )
    }

    fn cached_local_generator_record(
        &self,
        generator_id: u32,
        now_ms: u64,
    ) -> Option<GeneratorRecord> {
        let lease = self.generator_runtime.lease_state(generator_id)?;
        if lease.owner_instance_id != self.local_instance_id() || lease.lease_expire_at_ms <= now_ms
        {
            return None;
        }

        Some(GeneratorRecord {
            schema_version: crate::metadata::CURRENT_METADATA_SCHEMA_VERSION,
            generator_id,
            owner_worker_endpoint: self.config.advertise_endpoint.clone(),
            owner_instance_id: lease.owner_instance_id,
            generator_lease_token: lease.generator_lease_token,
            lease_expire_at_ms: Some(lease.lease_expire_at_ms),
            last_issued_tso: lease.last_persisted_tso,
            issued_upper_bound: lease.issued_upper_bound,
            updated_at_ms: now_ms,
        })
    }

    pub(super) fn compute_generator_upper_bound(
        &self,
        generator_id: u32,
        base_ms: u64,
    ) -> Option<u64> {
        crate::encode_tso(
            base_ms + self.config.pre_borrow_ms,
            generator_id,
            crate::SEQUENCE_CAPACITY - 1,
        )
        .ok()
    }

    pub async fn get_timeline_route(&self, timeline_key: &str) -> Result<TimelineRoute, TsoError> {
        super::validation::validate_timeline_key(timeline_key)?;
        let (record, _) = self
            .load_timeline_route_with_singleflight(timeline_key)
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
        super::validation::validate_timeline_key(timeline_key)?;
        let (record, _) = self
            .load_timeline_with_singleflight(timeline_key)
            .await?
            .ok_or_else(|| TsoError::TimelineNotFound {
                timeline_key: timeline_key.to_owned(),
            })?;
        Ok(record)
    }

    pub(crate) async fn get_timeline_status(
        &self,
        timeline_key: &str,
    ) -> Result<TimelineStatusSnapshot, TsoError> {
        super::validation::validate_timeline_key(timeline_key)?;
        let (timeline, _) = self
            .load_timeline_with_singleflight(timeline_key)
            .await?
            .ok_or_else(|| TsoError::TimelineNotFound {
                timeline_key: timeline_key.to_owned(),
            })?;
        let generator = self
            .metadata
            .load_generator(timeline.route.generator_id)
            .await?
            .map(|(record, _)| record);

        Ok(build_timeline_status_snapshot(
            &self.config,
            self.clock.now_ms(),
            &timeline,
            generator.as_ref(),
        ))
    }

    pub(crate) async fn list_timeline_statuses(
        &self,
        states: &[TimelineLifecycleState],
        owner_worker_endpoint: Option<&str>,
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<TimelineStatusListPage, TsoError> {
        if let Some(start_after_timeline_key) = start_after_timeline_key {
            super::validation::validate_timeline_key(start_after_timeline_key)?;
        }
        if limit == 0 {
            return Ok(TimelineStatusListPage {
                statuses: Vec::new(),
                next_start_after: None,
            });
        }

        let mut generator_cache: HashMap<u32, Option<GeneratorRecord>> = HashMap::new();
        let mut statuses = Vec::with_capacity(limit);
        let now_ms = self.clock.now_ms();

        let page = self
            .metadata
            .list_timelines_by_status_filter_page(
                states,
                owner_worker_endpoint,
                start_after_timeline_key,
                limit,
            )
            .await?;

        let missing_generator_ids: Vec<_> = page
            .records
            .iter()
            .map(|timeline| timeline.route.generator_id)
            .filter(|generator_id| !generator_cache.contains_key(generator_id))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();

        let mut metadata_generator_ids = Vec::with_capacity(missing_generator_ids.len());
        for generator_id in missing_generator_ids {
            match self.cached_local_generator_record(generator_id, now_ms) {
                Some(record) => {
                    generator_cache.insert(generator_id, Some(record));
                }
                None => metadata_generator_ids.push(generator_id),
            }
        }

        if !metadata_generator_ids.is_empty() {
            generator_cache.extend(
                self.metadata
                    .load_generators(&metadata_generator_ids)
                    .await?,
            );
        }

        for timeline in page.records {
            let generator_id = timeline.route.generator_id;
            let generator = match generator_cache.get(&generator_id) {
                Some(generator) => (*generator).as_ref(),
                _ => None,
            };

            statuses.push(build_timeline_status_snapshot(
                &self.config,
                now_ms,
                &timeline,
                generator,
            ));
        }

        Ok(TimelineStatusListPage {
            statuses,
            next_start_after: page.next_start_after_timeline_key,
        })
    }

    pub async fn load_generator_record(
        &self,
        generator_id: u32,
    ) -> Result<GeneratorRecord, TsoError> {
        let (record, _) = self
            .metadata
            .load_generator(generator_id)
            .await?
            .ok_or_else(|| TsoError::TimelineNotFound {
                timeline_key: format!("generator:{}", generator_id),
            })?;
        Ok(record)
    }

    pub async fn renew_timeline_lease(&self, timeline_key: &str) -> Result<(), TsoError> {
        super::validation::validate_timeline_key(timeline_key)?;
        let (record, _) = self
            .load_timeline_route_with_singleflight(timeline_key)
            .await?
            .ok_or_else(|| TsoError::TimelineNotFound {
                timeline_key: timeline_key.to_owned(),
            })?;
        if !self.is_local_endpoint(&record.route.owner_worker_endpoint) {
            return Err(TsoError::NotTimelineOwner {
                owner_worker_endpoint: record.route.owner_worker_endpoint,
            });
        }

        self.ensure_generator_lease(record.route.generator_id)
            .await?;
        self.refresh_generator_lease_inner(
            record.route.generator_id,
            super::lease::GeneratorLeaseRefreshReason::ExplicitRenewal,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::lifecycle::{TimelineLifecycleContract, TimelineServingReadiness};
    use crate::metadata::{MemoryMetadataStore, TimelineAuthority};
    use crate::{
        AllocateTimestampsRequest, ManualClock, TimelineLifecycleState, TsoConfig, TsoError,
        TsoService,
    };

    fn required_test_config(config: TsoConfig) -> TsoConfig {
        crate::test_tls::required_grpc_tls_test_config(config, 100)
    }

    #[test]
    fn timeline_readiness_follows_lifecycle_contract() {
        let states = [
            TimelineLifecycleState::Active,
            TimelineLifecycleState::Creating,
            TimelineLifecycleState::Recovering,
            TimelineLifecycleState::Draining,
            TimelineLifecycleState::Locked,
        ];

        for state in states {
            let contract = TimelineLifecycleContract::classify(state);
            assert_eq!(
                TsoService::timeline_is_ready(state),
                matches!(
                    contract.serving_readiness(),
                    TimelineServingReadiness::Serving
                )
            );
        }
    }

    #[tokio::test]
    async fn get_timeline_route_reads_route_without_full_record_use() {
        let clock = Arc::new(ManualClock::new(19_500));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service =
            TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();

        let route = service
            .ensure_timeline("runtime.route-only.read")
            .await
            .unwrap();
        let loaded = service
            .get_timeline_route(&route.timeline_key)
            .await
            .unwrap();

        assert_eq!(loaded, route);
    }

    #[tokio::test]
    async fn local_draining_record_is_not_auto_activated() {
        let clock = Arc::new(ManualClock::new(20_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let route = service
            .ensure_timeline("runtime.draining.activation")
            .await
            .unwrap();
        let (mut record, revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        record.state = TimelineLifecycleState::Draining;
        let draining_revision = metadata
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();
        service.clear_timeline_cache(&route.timeline_key);

        let (after, after_revision) = service
            .activate_local_timeline_record(&route.timeline_key, record, draining_revision)
            .await
            .unwrap();

        assert_eq!(after.state, TimelineLifecycleState::Draining);
        assert_eq!(after_revision, draining_revision);
    }

    #[tokio::test]
    async fn local_creating_record_is_not_auto_activated() {
        let clock = Arc::new(ManualClock::new(20_500));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let route = service
            .ensure_timeline("runtime.creating.activation")
            .await
            .unwrap();
        let (mut record, revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        record.state = TimelineLifecycleState::Creating;
        let creating_revision = metadata
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();
        service.clear_timeline_cache(&route.timeline_key);

        let (after, after_revision) = service
            .activate_local_timeline_record(&route.timeline_key, record, creating_revision)
            .await
            .unwrap();

        assert_eq!(after.state, TimelineLifecycleState::Creating);
        assert_eq!(after_revision, creating_revision);
    }

    #[tokio::test]
    async fn local_locked_record_is_not_auto_activated() {
        let clock = Arc::new(ManualClock::new(20_750));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let route = service
            .ensure_timeline("runtime.locked.activation")
            .await
            .unwrap();
        let (mut record, revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        record.state = TimelineLifecycleState::Locked;
        let locked_revision = metadata
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();
        service.clear_timeline_cache(&route.timeline_key);

        let (after, after_revision) = service
            .activate_local_timeline_record(&route.timeline_key, record, locked_revision)
            .await
            .unwrap();

        assert_eq!(after.state, TimelineLifecycleState::Locked);
        assert_eq!(after_revision, locked_revision);
    }

    #[tokio::test]
    async fn local_draining_timeline_returns_timeline_not_ready() {
        let clock = Arc::new(ManualClock::new(21_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let route = service
            .ensure_timeline("runtime.draining.allocate")
            .await
            .unwrap();
        let (mut record, revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        record.state = TimelineLifecycleState::Draining;
        metadata
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();
        service.clear_timeline_cache(&route.timeline_key);

        let error = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "draining-not-ready".to_string(),
            })
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TsoError::TimelineNotReady {
                state: TimelineLifecycleState::Draining,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn local_creating_timeline_returns_timeline_not_ready() {
        let clock = Arc::new(ManualClock::new(21_500));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let route = service
            .ensure_timeline("runtime.creating.allocate")
            .await
            .unwrap();
        let (mut record, revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        record.state = TimelineLifecycleState::Creating;
        metadata
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();
        service.clear_timeline_cache(&route.timeline_key);

        let error = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "creating-not-ready".to_string(),
            })
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TsoError::TimelineNotReady {
                state: TimelineLifecycleState::Creating,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn local_locked_timeline_returns_timeline_not_ready() {
        let clock = Arc::new(ManualClock::new(21_750));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let route = service
            .ensure_timeline("runtime.locked.allocate")
            .await
            .unwrap();
        let (mut record, revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        record.state = TimelineLifecycleState::Locked;
        metadata
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();
        service.clear_timeline_cache(&route.timeline_key);

        let error = service
            .allocate_timestamps(AllocateTimestampsRequest {
                timeline_key: route.timeline_key.clone(),
                count: 1,
                expected_epoch: route.epoch,
                expected_route_version: route.route_version,
                client_request_id: "locked-not-ready".to_string(),
            })
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TsoError::TimelineNotReady {
                state: TimelineLifecycleState::Locked,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn recovering_timeline_still_activates_locally() {
        let clock = Arc::new(ManualClock::new(22_000));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let route = service
            .ensure_timeline("runtime.recovering.activation")
            .await
            .unwrap();
        let (mut record, revision) = metadata
            .load_timeline(&route.timeline_key)
            .await
            .unwrap()
            .unwrap();
        record.state = TimelineLifecycleState::Recovering;
        let recovering_revision = metadata
            .compare_exchange_timeline(&route.timeline_key, revision, &record)
            .await
            .unwrap();
        service.clear_timeline_cache(&route.timeline_key);

        let (after, after_revision) = service
            .activate_local_timeline_record(&route.timeline_key, record, recovering_revision)
            .await
            .unwrap();

        assert_eq!(after.state, TimelineLifecycleState::Active);
        assert!(after_revision > recovering_revision);
    }
}
