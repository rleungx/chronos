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
    fn timeline_matches_status_filters(
        timeline: &TimelineRecord,
        states: &[TimelineLifecycleState],
        owner_worker_endpoint: Option<&str>,
    ) -> bool {
        (states.is_empty() || states.contains(&timeline.state))
            && owner_worker_endpoint
                .map(|endpoint| timeline.route.owner_worker_endpoint == endpoint)
                .unwrap_or(true)
    }

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
        let (record, _) = self
            .load_timeline_with_singleflight(timeline_key)
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
        if limit == 0 {
            return Ok(TimelineStatusListPage {
                statuses: Vec::new(),
                next_start_after: None,
            });
        }

        let mut generator_cache: HashMap<u32, Option<GeneratorRecord>> = HashMap::new();
        let mut statuses = Vec::with_capacity(limit);
        let now_ms = self.clock.now_ms();

        let mut scan_cursor = start_after_timeline_key.map(str::to_owned);
        let scan_limit = limit;
        while statuses.len() < limit {
            let page = self
                .metadata
                .list_timelines_page(scan_cursor.as_deref(), scan_limit)
                .await?;
            if page.records.is_empty() {
                break;
            }
            scan_cursor = page.next_start_after_timeline_key.clone();

            for timeline in page.records {
                if !Self::timeline_matches_status_filters(&timeline, states, owner_worker_endpoint)
                {
                    continue;
                }
                let generator_id = timeline.route.generator_id;
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    generator_cache.entry(generator_id)
                {
                    let loaded = self
                        .metadata
                        .load_generator(generator_id)
                        .await?
                        .map(|(record, _)| record);
                    entry.insert(loaded);
                }
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
                if statuses.len() == limit {
                    break;
                }
            }

            if scan_cursor.is_none() {
                break;
            }
        }

        let next_start_after = if statuses.len() < limit {
            None
        } else if states.is_empty() && owner_worker_endpoint.is_none() {
            scan_cursor.as_ref().and_then(|_| {
                statuses
                    .last()
                    .map(|status| status.route.timeline_key.clone())
            })
        } else if let Some(last_returned_key) = statuses
            .last()
            .map(|status| status.route.timeline_key.clone())
        {
            let mut lookahead_cursor = Some(last_returned_key.clone());
            let mut has_more_matching = false;
            loop {
                let page = self
                    .metadata
                    .list_timelines_page(lookahead_cursor.as_deref(), scan_limit)
                    .await?;
                if page.records.is_empty() {
                    break;
                }
                if page.records.iter().any(|timeline| {
                    Self::timeline_matches_status_filters(timeline, states, owner_worker_endpoint)
                }) {
                    has_more_matching = true;
                    break;
                }
                let next_cursor = page.next_start_after_timeline_key;
                let Some(next_cursor) = next_cursor else {
                    break;
                };
                lookahead_cursor = Some(next_cursor);
            }
            has_more_matching.then_some(last_returned_key)
        } else {
            None
        };

        Ok(TimelineStatusListPage {
            statuses,
            next_start_after,
        })
    }

    pub(crate) async fn list_timeline_routes(
        &self,
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<TimelineRoute>, Option<String>), TsoError> {
        if limit == 0 {
            return Ok((Vec::new(), None));
        }

        let page = self
            .metadata
            .list_timelines_page(start_after_timeline_key, limit)
            .await?;

        let next_start_after = page.next_start_after_timeline_key.as_ref().and_then(|_| {
            page.records
                .last()
                .map(|record| record.route.timeline_key.clone())
        });
        let routes = page
            .records
            .into_iter()
            .map(|record| record.route)
            .collect();

        Ok((routes, next_start_after))
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
        let (record, _) = self
            .load_timeline_with_singleflight(timeline_key)
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
        self.refresh_generator_lease_inner(record.route.generator_id, self.clock.now_ms(), true)
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
        TsoSecurityMode, TsoService,
    };

    fn required_test_config(config: TsoConfig) -> TsoConfig {
        TsoConfig {
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some("server.crt".into()),
            grpc_tls_key_file: Some("server.key".into()),
            grpc_client_ca_file: Some("ca.pem".into()),
            grpc_request_timeout_ms: Some(100),
            grpc_max_request_bytes: Some(1024),
            grpc_max_concurrent_requests: Some(16),
            ..config
        }
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
