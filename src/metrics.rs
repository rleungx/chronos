use lazy_static::lazy_static;
use prometheus::{
    register_histogram, register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge, register_int_gauge_vec, Histogram, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec,
};

fn expect_metric<T>(result: Result<T, prometheus::Error>, metric_name: &'static str) -> T {
    result.unwrap_or_else(|error| panic!("failed to register metric {metric_name}: {error}"))
}

lazy_static! {
    pub static ref TSO_ALLOCATE_TOTAL: IntCounter = expect_metric(
        register_int_counter!(
            "tso_allocate_total",
            "Total number of TSO allocation requests"
        ),
        "tso_allocate_total"
    );
    pub static ref TSO_ALLOCATE_COUNT_TOTAL: IntCounter = expect_metric(
        register_int_counter!(
            "tso_allocate_count_total",
            "Total number of timestamps allocated"
        ),
        "tso_allocate_count_total"
    );
    pub static ref TSO_ALLOCATE_LATENCY: Histogram = expect_metric(
        register_histogram!(
            "tso_allocate_latency_seconds",
            "Latency of TSO allocation in seconds"
        ),
        "tso_allocate_latency_seconds"
    );
    pub static ref TSO_SEQUENCE_UTILIZATION: IntGauge = expect_metric(
        register_int_gauge!(
            "tso_sequence_utilization",
            "Sequence number utilization within the current millisecond"
        ),
        "tso_sequence_utilization"
    );
    pub static ref TSO_LEASE_EXPIRED_TOTAL: IntCounter = expect_metric(
        register_int_counter!(
            "tso_lease_expired_total",
            "Total number of lease expiration events"
        ),
        "tso_lease_expired_total"
    );
    pub static ref TSO_CLOCK_BACKWARDS_TOTAL: IntCounter = expect_metric(
        register_int_counter!(
            "tso_clock_backwards_total",
            "Total number of clock backwards events"
        ),
        "tso_clock_backwards_total"
    );
    pub static ref TSO_METADATA_LATENCY: HistogramVec = expect_metric(
        register_histogram_vec!(
            "tso_metadata_latency_seconds",
            "Metadata store request latency in seconds",
            &["op"]
        ),
        "tso_metadata_latency_seconds"
    );
    pub static ref TSO_METADATA_ERRORS_TOTAL: IntCounterVec = expect_metric(
        register_int_counter_vec!(
            "tso_metadata_errors_total",
            "Metadata store request errors",
            &["op"]
        ),
        "tso_metadata_errors_total"
    );
    pub static ref TSO_MAINTENANCE_TRACKED_TIMELINES: IntGauge = expect_metric(
        register_int_gauge!(
            "tso_maintenance_tracked_timelines",
            "Number of timelines tracked by the maintenance scheduler"
        ),
        "tso_maintenance_tracked_timelines"
    );
    pub static ref TSO_MAINTENANCE_EXTEND_TOTAL: IntCounter = expect_metric(
        register_int_counter!(
            "tso_maintenance_extend_total",
            "Total number of authority window extension attempts triggered by maintenance"
        ),
        "tso_maintenance_extend_total"
    );
    pub static ref TSO_TIMELINE_PROXY_LANES: IntGauge = expect_metric(
        register_int_gauge!(
            "tso_timeline_proxy_lanes",
            "Current number of timeline proxy lanes"
        ),
        "tso_timeline_proxy_lanes"
    );
    pub static ref TSO_TIMELINE_PROXY_WAIT: Histogram = expect_metric(
        register_histogram!(
            "tso_timeline_proxy_wait_seconds",
            "Time spent waiting on timeline proxy serialization"
        ),
        "tso_timeline_proxy_wait_seconds"
    );
    pub static ref TSO_TIMELINE_PROXY_SATURATED_TOTAL: IntCounter = expect_metric(
        register_int_counter!(
            "tso_timeline_proxy_saturated_total",
            "Total number of timeline proxy saturation events"
        ),
        "tso_timeline_proxy_saturated_total"
    );
    pub static ref TSO_TIMELINE_PROXY_TIMEOUT_TOTAL: IntCounter = expect_metric(
        register_int_counter!(
            "tso_timeline_proxy_timeout_total",
            "Total number of timeline proxy timeout events"
        ),
        "tso_timeline_proxy_timeout_total"
    );
    pub static ref TSO_WATCH_KEEPALIVE_DROPPED_TOTAL: IntCounter = expect_metric(
        register_int_counter!(
            "tso_watch_keepalive_dropped_total",
            "Total number of watch keepalive events dropped due to backpressure"
        ),
        "tso_watch_keepalive_dropped_total"
    );
    pub static ref TSO_WATCH_ROUTE_SEND_TIMEOUT_TOTAL: IntCounter = expect_metric(
        register_int_counter!(
            "tso_watch_route_send_timeout_total",
            "Total number of watch route/event sends that timed out due to slow consumers"
        ),
        "tso_watch_route_send_timeout_total"
    );
    pub static ref TSO_WATCH_RESYNC_TOTAL: IntCounterVec = expect_metric(
        register_int_counter_vec!(
            "tso_watch_resync_total",
            "Route watch resync lifecycle events",
            &["event"]
        ),
        "tso_watch_resync_total"
    );
    pub static ref TSO_TIMELINE_RUNTIME_CACHE_ENTRIES: IntGauge = expect_metric(
        register_int_gauge!(
            "tso_timeline_runtime_cache_entries",
            "Current number of cached timeline runtime entries"
        ),
        "tso_timeline_runtime_cache_entries"
    );
    pub static ref TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL: IntCounterVec = expect_metric(
        register_int_counter_vec!(
            "tso_timeline_runtime_cache_events_total",
            "Timeline runtime cache events",
            &["event"]
        ),
        "tso_timeline_runtime_cache_events_total"
    );
    pub static ref TSO_STARTUP_PREFLIGHT_FAIL_TOTAL: IntCounterVec = expect_metric(
        register_int_counter_vec!(
            "tso_startup_preflight_fail_total",
            "Total number of startup preflight failures",
            &["stage"]
        ),
        "tso_startup_preflight_fail_total"
    );
    pub static ref TSO_STARTUP_BOOTSTRAP_FAIL_TOTAL: IntCounterVec = expect_metric(
        register_int_counter_vec!(
            "tso_startup_bootstrap_fail_total",
            "Total number of startup bootstrap failures",
            &["stage"]
        ),
        "tso_startup_bootstrap_fail_total"
    );
    pub static ref TSO_STARTUP_READY: IntGauge = expect_metric(
        register_int_gauge!(
            "tso_startup_ready",
            "Whether the service has completed startup and is ready"
        ),
        "tso_startup_ready"
    );
    pub static ref TSO_IDENTITY_LEASE_EVENTS_TOTAL: IntCounterVec = expect_metric(
        register_int_counter_vec!(
            "tso_identity_lease_events_total",
            "Identity lease lifecycle events",
            &["event"]
        ),
        "tso_identity_lease_events_total"
    );
    pub static ref TSO_SHUTDOWN_TOTAL: IntCounterVec = expect_metric(
        register_int_counter_vec!(
            "tso_shutdown_total",
            "Controlled shutdown events",
            &["reason"]
        ),
        "tso_shutdown_total"
    );
    pub static ref TSO_WORKER_READINESS_TRANSITIONS_TOTAL: IntCounterVec = expect_metric(
        register_int_counter_vec!(
            "tso_worker_readiness_transitions_total",
            "Worker readiness transitions by state and reason",
            &["from_state", "to_state", "reason"]
        ),
        "tso_worker_readiness_transitions_total"
    );
    pub static ref TSO_TRANSFER_OUTCOMES_TOTAL: IntCounterVec = expect_metric(
        register_int_counter_vec!(
            "tso_transfer_outcomes_total",
            "Transfer and failover action outcomes",
            &["action", "reason", "outcome"]
        ),
        "tso_transfer_outcomes_total"
    );
    pub static ref TSO_RECOVERY_EVENTS_TOTAL: IntCounterVec = expect_metric(
        register_int_counter_vec!(
            "tso_recovery_events_total",
            "Recover-and-continue events by component, operation, and reason",
            &["component", "operation", "reason"]
        ),
        "tso_recovery_events_total"
    );
    pub static ref TSO_BUILD_INFO: IntGaugeVec = expect_metric(
        register_int_gauge_vec!(
            "tso_build_info",
            "Chronos build identity labeled by version and commit",
            &["version", "commit"]
        ),
        "tso_build_info"
    );
    pub static ref TSO_MIXED_VERSION_CONTRACT_INFO: IntGaugeVec = expect_metric(
        register_int_gauge_vec!(
            "tso_mixed_version_contract_info",
            "Chronos mixed-version contract identity",
            &["contract_id"]
        ),
        "tso_mixed_version_contract_info"
    );
    pub static ref TSO_VERSION_COMPATIBILITY_MISMATCH_TOTAL: IntCounterVec = expect_metric(
        register_int_counter_vec!(
            "tso_version_compatibility_mismatch_total",
            "Mixed-version compatibility mismatches observed across Chronos surfaces",
            &["kind"]
        ),
        "tso_version_compatibility_mismatch_total"
    );
}

pub fn init_build_info_metric() {
    TSO_BUILD_INFO
        .with_label_values(&[
            crate::build_info::build_version(),
            crate::build_info::build_commit(),
        ])
        .set(1);
    TSO_MIXED_VERSION_CONTRACT_INFO
        .with_label_values(&[crate::build_info::mixed_version_contract_id()])
        .set(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_info_metric_is_initialized_with_current_build_identity() {
        init_build_info_metric();

        assert_eq!(
            TSO_BUILD_INFO
                .with_label_values(&[
                    crate::build_info::build_version(),
                    crate::build_info::build_commit(),
                ])
                .get(),
            1
        );
        assert_eq!(
            TSO_MIXED_VERSION_CONTRACT_INFO
                .with_label_values(&[crate::build_info::mixed_version_contract_id()])
                .get(),
            1
        );
    }
}
