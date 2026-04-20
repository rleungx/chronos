use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;
use tokio::task::yield_now;

use crate::plane::RequestCancellation;
use crate::runtime::{AllocateAfterResult, TimelineState};
use crate::{
    metrics, AllocateTimestampsRequest, AllocateTimestampsResponse, TimelineLifecycleState,
    TimelineRoute, TsoError,
};

use super::TsoService;

struct CachedTimelineChecks {
    owner_matches: bool,
    route_matches_request: bool,
    timeline_ready: bool,
    state: TimelineLifecycleState,
}

#[derive(Clone, Copy)]
pub(super) enum AllocationPath {
    Cached,
    Metadata,
}

impl AllocationPath {
    fn as_label(self) -> &'static str {
        match self {
            Self::Cached => "cached",
            Self::Metadata => "metadata",
        }
    }
}

pub(super) struct CachedServeGuardOptions {
    pub(super) cancellation: Option<RequestCancellation>,
    pub(super) revalidate_authority: bool,
    pub(super) path: AllocationPath,
}

pub(super) struct AllocationServeOptions {
    path: AllocationPath,
    cancellation: Option<RequestCancellation>,
}

const MAX_ALLOCATION_CONTENTION_RETRIES: u32 = 8;

enum ServeOutcome {
    Served(AllocateTimestampsResponse),
    Contended,
    RetryAfterLeaseRefresh,
    Error(TsoError),
}

impl CachedTimelineChecks {
    fn new(
        service: &TsoService,
        request: &AllocateTimestampsRequest,
        route: &TimelineRoute,
        state: TimelineLifecycleState,
    ) -> Self {
        Self {
            owner_matches: service.is_local_endpoint(&route.owner_worker_endpoint),
            route_matches_request: TsoService::request_matches_timeline_route(request, route),
            timeline_ready: TsoService::timeline_is_ready(state),
            state,
        }
    }

    fn matches_local_route(&self) -> bool {
        self.owner_matches && self.route_matches_request
    }

    fn can_serve(&self, generator_still_matches: bool) -> bool {
        self.matches_local_route() && self.timeline_ready && generator_still_matches
    }
}

impl TsoService {
    fn record_allocation_outcome(path: AllocationPath, outcome: &'static str) {
        metrics::TSO_ALLOCATE_OUTCOMES_TOTAL
            .with_label_values(&[path.as_label(), outcome])
            .inc();
    }

    fn record_allocation_ranges(ranges: &[crate::TimestampRange]) {
        metrics::TSO_ALLOCATE_RANGES_TOTAL.inc_by(ranges.len() as u64);
        if ranges.len() > 1 {
            metrics::TSO_ALLOCATE_CROSS_MS_TOTAL.inc();
        }
    }

    pub(super) fn record_allocation_stage_latency(
        path: AllocationPath,
        stage: &'static str,
        started_at: Instant,
    ) {
        metrics::TSO_ALLOCATE_STAGE_LATENCY
            .with_label_values(&[path.as_label(), stage])
            .observe(started_at.elapsed().as_secs_f64());
    }

    pub(super) async fn try_allocate_from_cached_timeline(
        &self,
        request: &AllocateTimestampsRequest,
        cancellation: Option<RequestCancellation>,
    ) -> Result<Option<AllocateTimestampsResponse>, TsoError> {
        let cached_check_started = Instant::now();
        let Some(timeline_state_handle) =
            self.timeline_runtime.timeline_handle(&request.timeline_key)
        else {
            Self::record_allocation_stage_latency(
                AllocationPath::Cached,
                "cached_check",
                cached_check_started,
            );
            return Ok(None);
        };

        let (cached_route, cached_state) = {
            let timeline_state = timeline_state_handle.lock().await;
            (timeline_state.route.clone(), timeline_state.state)
        };
        let checks = CachedTimelineChecks::new(self, request, &cached_route, cached_state);
        let cached_lease_upper_bound = (checks.matches_local_route() && checks.timeline_ready)
            .then(|| {
                self.valid_generator_lease_upper_bound(
                    cached_route.generator_id,
                    self.clock.now_ms(),
                )
            })
            .flatten();

        if checks.matches_local_route() {
            self.validate_batch_for_route(request.count, &cached_route)?;
        }

        if checks.matches_local_route() && cached_lease_upper_bound.is_none() {
            let authority_matches = self
                .cached_timeline_still_matches_metadata(
                    &request.timeline_key,
                    &cached_route,
                    cached_state,
                    cancellation.clone(),
                )
                .await?;
            if !authority_matches {
                Self::record_allocation_outcome(AllocationPath::Cached, "metadata_retry");
                self.clear_timeline_cache(&request.timeline_key);
                return Ok(None);
            }
        }

        if checks.matches_local_route() && checks.timeline_ready {
            let generator_id = cached_route.generator_id;
            if let Some(issued_upper_bound) = cached_lease_upper_bound {
                return self
                    .try_serve_timeline_state_handle_with_guard(
                        request,
                        timeline_state_handle,
                        generator_id,
                        self.clock.now_ms(),
                        Some(issued_upper_bound),
                        CachedServeGuardOptions {
                            cancellation,
                            revalidate_authority: false,
                            path: AllocationPath::Cached,
                        },
                    )
                    .await;
            }
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
                            CachedServeGuardOptions {
                                cancellation,
                                revalidate_authority: true,
                                path: AllocationPath::Metadata,
                            },
                        )
                        .await;
                }
                Err(_) => {
                    Self::record_allocation_outcome(AllocationPath::Cached, "lease_retry");
                    self.clear_timeline_cache(&request.timeline_key);
                    return Ok(None);
                }
            }
        }

        if checks.matches_local_route() && !checks.timeline_ready {
            Self::record_allocation_outcome(AllocationPath::Cached, "not_ready");
            self.clear_timeline_cache(&request.timeline_key);
            return Err(TsoError::TimelineNotReady {
                timeline_key: request.timeline_key.clone(),
                state: checks.state,
            });
        }
        if !checks.owner_matches {
            Self::record_allocation_outcome(AllocationPath::Cached, "metadata_retry");
            self.clear_timeline_cache(&request.timeline_key);
        }

        Self::record_allocation_stage_latency(
            AllocationPath::Cached,
            "cached_check",
            cached_check_started,
        );

        Ok(None)
    }

    async fn cached_timeline_still_matches_metadata(
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
        options: AllocationServeOptions,
    ) -> Result<Option<AllocateTimestampsResponse>, TsoError> {
        let generator = self.lookup_generator(generator_id)?;
        let mut contention_retries = 0u32;
        let serve_started = Instant::now();
        let charged_quota = {
            let mut timeline_state = timeline_state_handle.lock().await;
            self.charge_timeline_quota(&mut timeline_state, request.count, now_ms)?
        };

        loop {
            Self::check_request_cancellation(options.cancellation.as_ref())?;
            let (resource_tier, timeline_state) = {
                let timeline_state = timeline_state_handle.lock().await;
                (timeline_state.route.resource_tier, timeline_state)
            };
            drop(timeline_state);

            let admission_wait_started = Instant::now();
            let _admission_permit = match self
                .acquire_generator_admission(
                    generator_id,
                    resource_tier,
                    &request.timeline_key,
                    options.cancellation.clone(),
                )
                .await
            {
                Ok(permit) => permit,
                Err(error) => {
                    let mut timeline_state = timeline_state_handle.lock().await;
                    self.refund_timeline_quota(&mut timeline_state, charged_quota);
                    return Err(error);
                }
            };
            Self::record_allocation_stage_latency(
                options.path,
                "admission_wait",
                admission_wait_started,
            );

            let mut timeline_state = timeline_state_handle.lock().await;
            let outcome = match generator.allocate_after(
                request.count,
                timeline_state.last_issued_tso,
                now_ms,
                self.effective_future_borrow_ms_for_timeline_state(&timeline_state, now_ms),
                self.config.max_clock_rewind_ms,
                issued_upper_bound,
            ) {
                Ok(AllocateAfterResult::Allocated(ranges)) => {
                    timeline_state.last_issued_tso = ranges.last().map(|r| r.end_tso);
                    let timeline_key = timeline_state.route.timeline_key.clone();
                    let epoch = timeline_state.route.epoch;
                    let route_version = timeline_state.route.route_version;
                    metrics::TSO_ALLOCATE_TOTAL.inc();
                    Self::record_allocation_outcome(options.path, "served");
                    Self::record_allocation_ranges(&ranges);
                    Self::record_allocation_stage_latency(options.path, "serve", serve_started);
                    drop(timeline_state);
                    ServeOutcome::Served(AllocateTimestampsResponse {
                        timeline_key,
                        generator_id,
                        epoch,
                        route_version,
                        ranges,
                    })
                }
                Ok(AllocateAfterResult::Contended) => {
                    drop(timeline_state);
                    ServeOutcome::Contended
                }
                Err(TsoError::IssuedUpperBoundExceeded { .. }) => {
                    drop(timeline_state);
                    ServeOutcome::RetryAfterLeaseRefresh
                }
                Err(error) => {
                    drop(timeline_state);
                    ServeOutcome::Error(error)
                }
            };

            self.release_generator_admission_turn(
                generator_id,
                resource_tier,
                &request.timeline_key,
            )
            .await;

            match outcome {
                ServeOutcome::Served(response) => {
                    return Ok(Some(response));
                }
                ServeOutcome::Contended => {
                    contention_retries = contention_retries.saturating_add(1);
                    if contention_retries >= MAX_ALLOCATION_CONTENTION_RETRIES {
                        Self::record_allocation_outcome(options.path, "contention_exhausted");
                        Self::record_allocation_stage_latency(options.path, "serve", serve_started);
                        let mut timeline_state = timeline_state_handle.lock().await;
                        self.refund_timeline_quota(&mut timeline_state, charged_quota);
                        return Err(TsoError::AllocationContention { generator_id });
                    }
                    yield_now().await;
                }
                ServeOutcome::RetryAfterLeaseRefresh => {
                    Self::record_allocation_outcome(options.path, "lease_refresh_retry");
                    let mut timeline_state = timeline_state_handle.lock().await;
                    self.refund_timeline_quota(&mut timeline_state, charged_quota);
                    drop(timeline_state);
                    self.refresh_generator_lease_if_unchanged_with_cancellation(
                        generator_id,
                        issued_upper_bound,
                        options.cancellation.clone(),
                    )
                    .await?;
                    Self::record_allocation_stage_latency(options.path, "serve", serve_started);
                    return Ok(None);
                }
                ServeOutcome::Error(error) => {
                    Self::record_allocation_stage_latency(options.path, "serve", serve_started);
                    let mut timeline_state = timeline_state_handle.lock().await;
                    self.refund_timeline_quota(&mut timeline_state, charged_quota);
                    return Err(error);
                }
            }
        }
    }

    pub(super) async fn try_serve_timeline_state_handle_with_guard(
        &self,
        request: &AllocateTimestampsRequest,
        timeline_state_handle: Arc<Mutex<TimelineState>>,
        generator_id: u32,
        now_ms: u64,
        issued_upper_bound: Option<u64>,
        options: CachedServeGuardOptions,
    ) -> Result<Option<AllocateTimestampsResponse>, TsoError> {
        Self::check_request_cancellation(options.cancellation.as_ref())?;
        let (current_route, checks, generator_still_matches) = {
            let timeline_state = timeline_state_handle.lock().await;
            let route = timeline_state.route.clone();
            let checks = CachedTimelineChecks::new(self, request, &route, timeline_state.state);
            let generator_still_matches = route.generator_id == generator_id;
            (route, checks, generator_still_matches)
        };

        if checks.matches_local_route() && !checks.timeline_ready {
            self.clear_timeline_cache(&request.timeline_key);
            return Err(TsoError::TimelineNotReady {
                timeline_key: request.timeline_key.clone(),
                state: checks.state,
            });
        }
        if !checks.owner_matches {
            self.clear_timeline_cache(&request.timeline_key);
            return Ok(None);
        }
        if !generator_still_matches {
            self.clear_timeline_cache(&request.timeline_key);
            return Ok(None);
        }
        if !checks.can_serve(true) {
            return Ok(None);
        }

        if options.revalidate_authority {
            let authority_matches = self
                .cached_timeline_still_matches_metadata(
                    &request.timeline_key,
                    &current_route,
                    checks.state,
                    options.cancellation.clone(),
                )
                .await?;
            if !authority_matches {
                Self::record_allocation_outcome(options.path, "metadata_retry");
                self.clear_timeline_cache(&request.timeline_key);
                return Ok(None);
            }
        }

        let (post_route, post_state, post_generator_matches) = {
            let timeline_state = timeline_state_handle.lock().await;
            (
                timeline_state.route.clone(),
                timeline_state.state,
                timeline_state.route.generator_id == generator_id,
            )
        };
        if post_state != checks.state || post_route != current_route || !post_generator_matches {
            Self::record_allocation_outcome(options.path, "metadata_retry");
            self.clear_timeline_cache(&request.timeline_key);
            return Ok(None);
        }

        self.serve_allocation_from_timeline_state_handle(
            request,
            timeline_state_handle,
            generator_id,
            now_ms,
            issued_upper_bound,
            AllocationServeOptions {
                path: options.path,
                cancellation: options.cancellation,
            },
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
