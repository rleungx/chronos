use tonic::Status;
use tracing::{error, warn};

use crate::timeline_proxy::TimelineProxyError;
use crate::TsoError;

use super::public_mapping;

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
        TsoError::LeaseExpired { timeline_key } => warn!(
            component = "runtime_protection",
            event = "lease_expired",
            result = "failure",
            reason = "timeline_lease_expired",
            timeline_key
        ),
        TsoError::GeneratorLeaseExpired { generator_id } => warn!(
            component = "runtime_protection",
            event = "lease_expired",
            result = "failure",
            reason = "generator_lease_expired",
            generator_id
        ),
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
}
