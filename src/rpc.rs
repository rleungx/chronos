mod error_details;
mod health_handle;
mod public_mapping;
mod route_timestamp_service_facade;
mod service_facade;
mod status_mapping;
mod timeline_status_query;
mod transfer_adapter;
mod translation;

use crate::recovery::duration_since_unix_epoch_or_zero;
use std::time::SystemTime;

#[doc(hidden)]
pub use error_details::decode_error_detail_from_status_details;
pub(crate) use error_details::encode_error_detail_status;
pub use health_handle::HealthStatusHandle;
pub use route_timestamp_service_facade::{TsoRouteService, TsoTimestampService};
pub use service_facade::{TsoControlService, TsoTimelineStatusService};

fn current_timestamp() -> prost_types::Timestamp {
    current_timestamp_from(SystemTime::now())
}

fn current_timestamp_from(now: SystemTime) -> prost_types::Timestamp {
    let since_epoch = duration_since_unix_epoch_or_zero(now, "rpc", "current_timestamp");
    prost_types::Timestamp {
        seconds: since_epoch.as_secs() as i64,
        nanos: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics;
    use tokio::time::Duration;

    #[test]
    fn current_timestamp_before_unix_epoch_records_recovery_metric() {
        let before = metrics::TSO_RECOVERY_EVENTS_TOTAL
            .with_label_values(&["rpc", "current_timestamp", "system_time_before_unix_epoch"])
            .get();

        let timestamp = current_timestamp_from(SystemTime::UNIX_EPOCH - Duration::from_secs(1));

        assert_eq!(timestamp.seconds, 0);
        assert_eq!(timestamp.nanos, 0);
        assert!(
            metrics::TSO_RECOVERY_EVENTS_TOTAL
                .with_label_values(&["rpc", "current_timestamp", "system_time_before_unix_epoch"])
                .get()
                > before
        );
    }
}
