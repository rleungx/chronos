use std::sync::Arc;

use tokio::sync::Mutex;

use crate::plane::RequestCancellation;
use crate::runtime::TimelineState;
use crate::{
    metrics, AllocateTimestampsRequest, AllocateTimestampsResponse, TimelineLifecycleState,
    TimelineRoute, TsoError,
};

use super::TsoService;

impl TsoService {
    pub(super) async fn try_allocate_from_cached_timeline(
        &self,
        request: &AllocateTimestampsRequest,
        cancellation: Option<RequestCancellation>,
    ) -> Result<Option<AllocateTimestampsResponse>, TsoError> {
        let Some(timeline_state_handle) =
            self.timeline_runtime.timeline_handle(&request.timeline_key)
        else {
            return Ok(None);
        };

        let (cached_route, cached_state) = {
            let timeline_state = timeline_state_handle.lock().await;
            (timeline_state.route.clone(), timeline_state.state)
        };
        let route_matches_request = Self::request_matches_timeline_route(request, &cached_route);
        let owner_matches = self.is_local_endpoint(&cached_route.owner_worker_endpoint);
        let timeline_ready = Self::timeline_is_ready(cached_state);

        if owner_matches && route_matches_request {
            self.validate_batch_for_route(request.count, &cached_route)?;
        }

        if owner_matches && route_matches_request {
            let authority_matches = self
                .cached_timeline_matches_authority(
                    &request.timeline_key,
                    &cached_route,
                    cached_state,
                    cancellation.clone(),
                )
                .await?;
            if !authority_matches {
                self.clear_timeline_cache(&request.timeline_key);
                return Ok(None);
            }
        }

        if owner_matches && route_matches_request && timeline_ready {
            let generator_id = cached_route.generator_id;
            match self
                .ensure_generator_lease_for_allocation_with_cancellation(
                    &request.timeline_key,
                    generator_id,
                    cancellation.clone(),
                )
                .await
            {
                Ok(issued_upper_bound) => {
                    return self
                        .try_serve_timeline_state_handle_with_guard(
                            request,
                            timeline_state_handle,
                            generator_id,
                            self.clock.now_ms(),
                            issued_upper_bound,
                            cancellation,
                        )
                        .await;
                }
                Err(_) => {
                    self.clear_timeline_cache(&request.timeline_key);
                    return Ok(None);
                }
            }
        }

        if owner_matches && route_matches_request && !timeline_ready {
            self.clear_timeline_cache(&request.timeline_key);
            return Err(TsoError::TimelineNotReady {
                timeline_key: request.timeline_key.clone(),
                state: cached_state,
            });
        }
        if !owner_matches {
            self.clear_timeline_cache(&request.timeline_key);
        }

        Ok(None)
    }

    async fn cached_timeline_matches_authority(
        &self,
        timeline_key: &str,
        cached_route: &TimelineRoute,
        cached_state: TimelineLifecycleState,
        cancellation: Option<RequestCancellation>,
    ) -> Result<bool, TsoError> {
        let Some((record, _)) = self
            .load_timeline_with_singleflight_and_cancellation(timeline_key, cancellation)
            .await?
        else {
            return Ok(false);
        };

        let Some((generator_record, _)) = self
            .metadata
            .load_generator(cached_route.generator_id)
            .await?
        else {
            return Ok(false);
        };

        Ok(record.route == *cached_route
            && record.state == cached_state
            && self.is_local_generator_owner(&generator_record))
    }

    pub(super) async fn serve_allocation_from_timeline_state_handle(
        &self,
        request: &AllocateTimestampsRequest,
        timeline_state_handle: Arc<Mutex<TimelineState>>,
        generator_id: u32,
        now_ms: u64,
        issued_upper_bound: Option<u64>,
        cancellation: Option<RequestCancellation>,
    ) -> Result<Option<AllocateTimestampsResponse>, TsoError> {
        let mut timeline_state = timeline_state_handle.lock().await;
        let generator = self.lookup_generator(generator_id)?;
        match generator.allocate_after(
            request.count,
            timeline_state.last_issued_tso,
            now_ms,
            self.effective_future_borrow_ms_for_timeline_state(&timeline_state, now_ms),
            self.config.max_clock_rewind_ms,
            issued_upper_bound,
        ) {
            Ok(ranges) => {
                timeline_state.last_issued_tso = ranges.last().map(|r| r.end_tso);
                metrics::TSO_ALLOCATE_TOTAL.inc();
                Ok(Some(AllocateTimestampsResponse {
                    timeline_key: timeline_state.route.timeline_key.clone(),
                    generator_id,
                    epoch: timeline_state.route.epoch,
                    route_version: timeline_state.route.route_version,
                    ranges,
                }))
            }
            Err(TsoError::IssuedUpperBoundExceeded { .. }) => {
                drop(timeline_state);
                let refresh_now_ms = self.clock.now_ms();
                self.refresh_generator_lease_inner_with_cancellation(
                    generator_id,
                    refresh_now_ms,
                    true,
                    cancellation,
                )
                .await?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn try_serve_timeline_state_handle_with_guard(
        &self,
        request: &AllocateTimestampsRequest,
        timeline_state_handle: Arc<Mutex<TimelineState>>,
        generator_id: u32,
        now_ms: u64,
        issued_upper_bound: Option<u64>,
        cancellation: Option<RequestCancellation>,
    ) -> Result<Option<AllocateTimestampsResponse>, TsoError> {
        Self::check_request_cancellation(cancellation.as_ref())?;
        let timeline_state = timeline_state_handle.lock().await;
        let route_matches_request =
            Self::request_matches_timeline_route(request, &timeline_state.route);
        let owner_matches = self.is_local_endpoint(&timeline_state.route.owner_worker_endpoint);
        let timeline_ready = Self::timeline_is_ready(timeline_state.state);
        let current_state = timeline_state.state;
        let generator_still_matches = timeline_state.route.generator_id == generator_id;
        drop(timeline_state);

        if owner_matches && route_matches_request && !timeline_ready {
            self.clear_timeline_cache(&request.timeline_key);
            return Err(TsoError::TimelineNotReady {
                timeline_key: request.timeline_key.clone(),
                state: current_state,
            });
        }
        if !owner_matches {
            self.clear_timeline_cache(&request.timeline_key);
            return Ok(None);
        }
        if !(owner_matches && route_matches_request && timeline_ready && generator_still_matches) {
            return Ok(None);
        }

        let authority_matches = self
            .cached_timeline_matches_authority(
                &request.timeline_key,
                &{
                    let timeline_state = timeline_state_handle.lock().await;
                    timeline_state.route.clone()
                },
                current_state,
                cancellation.clone(),
            )
            .await?;
        if !authority_matches {
            self.clear_timeline_cache(&request.timeline_key);
            return Ok(None);
        }

        self.serve_allocation_from_timeline_state_handle(
            request,
            timeline_state_handle,
            generator_id,
            now_ms,
            issued_upper_bound,
            cancellation,
        )
        .await
    }

    fn request_matches_timeline_route(
        request: &AllocateTimestampsRequest,
        route: &TimelineRoute,
    ) -> bool {
        route.route_version == request.expected_route_version
            && route.epoch == request.expected_epoch
    }
}
