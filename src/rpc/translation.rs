use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use tonic::Status;
use tracing::{error, warn};

use crate::timeline_proxy::TimelineProxyError;
use crate::TsoError;

use super::public_mapping;

const LEASE_WARNING_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Default)]
struct WarningRateLimitState {
    last_emitted_at: Option<Instant>,
    suppressed: u64,
}

impl WarningRateLimitState {
    fn take_suppressed_at(&mut self, now: Instant) -> Option<u64> {
        if self
            .last_emitted_at
            .is_some_and(|last| now.duration_since(last) < LEASE_WARNING_INTERVAL)
        {
            self.suppressed = self.suppressed.saturating_add(1);
            return None;
        }

        self.last_emitted_at = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

static TIMELINE_LEASE_WARNING_LIMITER: LazyLock<Mutex<WarningRateLimitState>> =
    LazyLock::new(|| Mutex::new(WarningRateLimitState::default()));
static GENERATOR_LEASE_WARNING_LIMITER: LazyLock<Mutex<WarningRateLimitState>> =
    LazyLock::new(|| Mutex::new(WarningRateLimitState::default()));

fn take_suppressed_warning_count(limiter: &Mutex<WarningRateLimitState>) -> Option<u64> {
    let mut state = match limiter.lock() {
        Ok(state) => state,
        Err(poisoned) => poisoned.into_inner(),
    };
    state.take_suppressed_at(Instant::now())
}

pub(super) fn map_tso_error(err: TsoError) -> Status {
    trace_runtime_protection(&err);
    public_mapping::map_tso_error(err)
}

pub(super) fn map_timeline_proxy_error(err: TimelineProxyError) -> Status {
    match err {
        TimelineProxyError::Tso(err) => map_tso_error(err),
        TimelineProxyError::TimedOut => Status::deadline_exceeded("AllocateTimestamps timed out"),
    }
}

fn trace_runtime_protection(err: &TsoError) {
    match err {
        TsoError::LeaseExpired { timeline_key } => {
            if let Some(suppressed_since_last) =
                take_suppressed_warning_count(&TIMELINE_LEASE_WARNING_LIMITER)
            {
                warn!(
                    component = "runtime_protection",
                    event = "lease_expired",
                    result = "failure",
                    reason = "timeline_lease_expired",
                    timeline_key,
                    suppressed_since_last
                );
            }
        }
        TsoError::GeneratorLeaseExpired { generator_id } => {
            if let Some(suppressed_since_last) =
                take_suppressed_warning_count(&GENERATOR_LEASE_WARNING_LIMITER)
            {
                warn!(
                    component = "runtime_protection",
                    event = "lease_expired",
                    result = "failure",
                    reason = "generator_lease_expired",
                    generator_id,
                    suppressed_since_last
                );
            }
        }
        TsoError::TimelineIngressSaturated {
            timeline_key,
            max_lanes,
        } => warn!(
            component = "runtime_protection",
            event = "timeline_proxy_saturated",
            result = "degraded",
            reason = "max_lanes_reached",
            timeline_key,
            max_lanes
        ),
        TsoError::TimelineRuntimeCacheSaturated {
            timeline_key,
            max_entries,
        } => warn!(
            component = "runtime_protection",
            event = "timeline_runtime_cache_saturated",
            result = "degraded",
            reason = "max_entries_reached",
            timeline_key,
            max_entries
        ),
        TsoError::ClockBackwards { delta_ms } => error!(
            component = "runtime_protection",
            event = "clock_backwards",
            result = "failure",
            reason = "clock_backwards",
            delta_ms
        ),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;
    use crate::proto::v1::ErrorCode;
    use crate::rpc::decode_error_detail_from_status_details;

    #[test]
    fn timeline_proxy_timeout_maps_to_deadline_exceeded_status() {
        let status = map_timeline_proxy_error(TimelineProxyError::TimedOut);

        assert_eq!(status.code(), Code::DeadlineExceeded);
        assert_eq!(status.message(), "AllocateTimestamps timed out");
    }

    #[test]
    fn timeline_proxy_tso_variant_preserves_public_error_detail_mapping() {
        let status =
            map_timeline_proxy_error(TimelineProxyError::Tso(TsoError::TimelineNotFound {
                timeline_key: "timeline-a".into(),
            }));

        let detail = decode_error_detail_from_status_details(status.details())
            .expect("error detail should decode");

        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(detail.code, ErrorCode::TimelineNotFound as i32);
    }

    #[test]
    fn lease_warning_limiter_reports_suppressed_count_per_window() {
        let mut limiter = WarningRateLimitState::default();
        let started_at = Instant::now();

        assert_eq!(limiter.take_suppressed_at(started_at), Some(0));
        assert_eq!(
            limiter.take_suppressed_at(started_at + Duration::from_secs(1)),
            None
        );
        assert_eq!(
            limiter.take_suppressed_at(started_at + LEASE_WARNING_INTERVAL),
            Some(1)
        );
    }
}
