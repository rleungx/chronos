use std::sync::Arc;

use tokio::sync::Mutex;

use crate::metadata::TimelineRecord;
use crate::timeline_state::build_timeline_state;
use crate::{ResourceTier, TimelineLifecycleState, TimelineRoute, TsoError};

use super::TsoService;

impl TsoService {
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
            self.reject_new_work_if_shutting_down()?;
            let cached_timeline = self.timeline_runtime.timeline_handle(timeline_key);
            if let Some(timeline_handle) = cached_timeline {
                let timeline = timeline_handle.lock().await;
                if self.is_local_endpoint(&timeline.route.owner_worker_endpoint)
                    && Self::timeline_is_ready(timeline.state)
                {
                    return Ok(timeline.route.clone());
                }
            }

            match self
                .load_timeline_with_singleflight_and_cancellation(timeline_key, None)
                .await?
            {
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
                        schema_version: 1,
                        route: route.clone(),
                        state: TimelineLifecycleState::Active,
                        recovery_floor_tso: None,
                        issued_upper_bound: None,
                        last_graceful_issued: None,
                        lease_expire_at_ms: None,
                        updated_at_ms: self.clock.now_ms(),
                    };

                    match self.metadata.create_timeline(timeline_key, &record).await {
                        Ok(revision) => {
                            let timeline =
                                Arc::new(Mutex::new(build_timeline_state(&record, revision, None)));
                            let _ =
                                self.best_effort_insert_timeline_cache(timeline_key, timeline)?;
                            return Ok(route);
                        }
                        Err(TsoError::MetadataAlreadyExists) => continue,
                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }
}
