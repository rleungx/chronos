use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::warn;

use crate::metrics;

pub(crate) fn record_recovery_event(
    component: &'static str,
    operation: &'static str,
    reason: &'static str,
) {
    metrics::TSO_RECOVERY_EVENTS_TOTAL
        .with_label_values(&[component, operation, reason])
        .inc();
    warn!(
        component,
        event = "recovery_applied",
        result = "degraded",
        operation,
        reason,
        action = "continue"
    );
}

pub(crate) fn duration_since_unix_epoch_or_zero(
    now: SystemTime,
    component: &'static str,
    operation: &'static str,
) -> Duration {
    now.duration_since(UNIX_EPOCH).unwrap_or_else(|_| {
        record_recovery_event(component, operation, "system_time_before_unix_epoch");
        Duration::default()
    })
}
