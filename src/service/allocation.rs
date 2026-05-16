mod ensure;
mod idempotency;
mod quota;
mod serve;

use self::idempotency::IdempotentAllocationPreparation;
use self::serve::{AllocationPath, CachedServeGuardOptions};
use crate::plane::RequestCancellation;
use crate::{metrics, AllocateTimestampsRequest, AllocateTimestampsResponse, TsoError};
use std::time::Instant;

use super::TsoService;

impl TsoService {
    pub async fn allocate_timestamps(
        &self,
        request: AllocateTimestampsRequest,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        self.allocate_timestamps_with_cancellation(request, None)
            .await
    }

    pub(crate) async fn allocate_timestamps_with_cancellation(
        &self,
        request: AllocateTimestampsRequest,
        cancellation: Option<RequestCancellation>,
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

        let idempotency = self.prepare_idempotent_allocation(&request).await?;
        if let IdempotentAllocationPreparation::Replay(response) = idempotency {
            return Ok(response);
        }

        let response = self
            .allocate_timestamps_after_validation(request.clone(), cancellation)
            .await;
        self.finish_idempotent_allocation(&request, idempotency, response)
            .await
    }

    async fn allocate_timestamps_after_validation(
        &self,
        request: AllocateTimestampsRequest,
        cancellation: Option<RequestCancellation>,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        loop {
            self.reject_new_work_if_shutting_down()?;
            Self::check_request_cancellation(cancellation.as_ref())?;
            if let Some(response) = self
                .try_allocate_from_cached_timeline(&request, cancellation.clone())
                .await?
            {
                return Ok(response);
            }

            let metadata_load_started = Instant::now();
            let (timeline_record, revision) = self
                .load_timeline_with_singleflight_and_cancellation(
                    &request.timeline_key,
                    cancellation.clone(),
                )
                .await?
                .ok_or_else(|| TsoError::TimelineNotFound {
                    timeline_key: request.timeline_key.clone(),
                })?;
            Self::record_allocation_stage_latency(
                AllocationPath::Metadata,
                "metadata_load",
                metadata_load_started,
            );

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
            self.validate_batch_for_route(request.count, &timeline_record.route)?;

            let activate_local_started = Instant::now();
            let (timeline_record, activated_revision) = match self
                .activate_local_timeline_record_with_cancellation(
                    &request.timeline_key,
                    timeline_record,
                    revision,
                    cancellation.clone(),
                )
                .await
            {
                Ok(value) => value,
                Err(TsoError::GeneratorLeaseExpired { .. }) => {
                    return Err(TsoError::LeaseExpired {
                        timeline_key: request.timeline_key.clone(),
                    });
                }
                Err(error) => return Err(error),
            };
            Self::record_allocation_stage_latency(
                AllocationPath::Metadata,
                "activate_local",
                activate_local_started,
            );

            if !Self::timeline_is_ready(timeline_record.state) {
                self.clear_timeline_cache(&request.timeline_key);
                return Err(TsoError::TimelineNotReady {
                    timeline_key: request.timeline_key.clone(),
                    state: timeline_record.state,
                });
            }

            let lease_ensure_started = Instant::now();
            let issued_upper_bound = self
                .ensure_generator_lease_for_allocation_with_cancellation(
                    &request.timeline_key,
                    timeline_record.route.generator_id,
                    cancellation.clone(),
                )
                .await?;
            Self::record_allocation_stage_latency(
                AllocationPath::Metadata,
                "lease_ensure",
                lease_ensure_started,
            );
            let timeline_state_handle = self
                .timeline_state_handle_from_record(
                    &request.timeline_key,
                    &timeline_record,
                    activated_revision,
                )
                .await?;
            let generator_id = timeline_record.route.generator_id;
            if let Some(response) = self
                .try_serve_timeline_state_handle_with_guard(
                    &request,
                    timeline_state_handle,
                    generator_id,
                    self.clock.now_ms(),
                    issued_upper_bound,
                    CachedServeGuardOptions {
                        cancellation: cancellation.clone(),
                        revalidate_authority: true,
                        path: AllocationPath::Metadata,
                    },
                )
                .await?
            {
                return Ok(response);
            }
        }
    }
}

#[cfg(test)]
mod tests;
