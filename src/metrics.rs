use prometheus::{
    core::Collector, register, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec, Opts,
};
use std::sync::LazyLock;

fn create_metric<T>(result: Result<T, prometheus::Error>, metric_name: &str) -> T {
    result.unwrap_or_else(|error| panic!("failed to create metric {metric_name}: {error}"))
}

fn register_metric<T>(metric: T, metric_name: &str) -> T
where
    T: Collector + Clone + 'static,
{
    if let Err(error) = register(Box::new(metric.clone())) {
        tracing::error!(
            component = "metrics",
            event = "metric_register_failed",
            result = "degraded",
            metric_name,
            error = %error
        );
    }
    metric
}

fn register_int_counter_metric(name: &'static str, help: &'static str) -> IntCounter {
    register_metric(create_metric(IntCounter::new(name, help), name), name)
}

fn register_int_gauge_metric(name: &'static str, help: &'static str) -> IntGauge {
    register_metric(create_metric(IntGauge::new(name, help), name), name)
}

fn register_histogram_metric(name: &'static str, help: &'static str) -> Histogram {
    register_metric(
        create_metric(Histogram::with_opts(HistogramOpts::new(name, help)), name),
        name,
    )
}

fn register_int_counter_vec_metric(
    name: &'static str,
    help: &'static str,
    labels: &[&str],
) -> IntCounterVec {
    register_metric(
        create_metric(IntCounterVec::new(Opts::new(name, help), labels), name),
        name,
    )
}

fn register_int_gauge_vec_metric(
    name: &'static str,
    help: &'static str,
    labels: &[&str],
) -> IntGaugeVec {
    register_metric(
        create_metric(IntGaugeVec::new(Opts::new(name, help), labels), name),
        name,
    )
}

fn register_histogram_vec_metric(
    name: &'static str,
    help: &'static str,
    labels: &[&str],
) -> HistogramVec {
    register_metric(
        create_metric(
            HistogramVec::new(HistogramOpts::new(name, help), labels),
            name,
        ),
        name,
    )
}

pub static TSO_ALLOCATE_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter_metric(
        "tso_allocate_total",
        "Total number of TSO allocation requests",
    )
});
pub static TSO_ALLOCATE_COUNT_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter_metric(
        "tso_allocate_count_total",
        "Total number of timestamps allocated",
    )
});
pub static TSO_ALLOCATE_LATENCY: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram_metric(
        "tso_allocate_latency_seconds",
        "Latency of TSO allocation in seconds",
    )
});
pub static TSO_SEQUENCE_UTILIZATION: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_metric(
        "tso_sequence_utilization",
        "Sequence number utilization within the current millisecond",
    )
});
pub static TSO_LEASE_EXPIRED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter_metric(
        "tso_lease_expired_total",
        "Total number of lease expiration events",
    )
});
pub static TSO_CLOCK_BACKWARDS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter_metric(
        "tso_clock_backwards_total",
        "Total number of clock backwards events",
    )
});
pub static TSO_METADATA_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec_metric(
        "tso_metadata_latency_seconds",
        "Metadata store request latency in seconds",
        &["op"],
    )
});
pub static TSO_METADATA_ERRORS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_metric(
        "tso_metadata_errors_total",
        "Metadata store request errors",
        &["op"],
    )
});
pub static TSO_MAINTENANCE_TRACKED_TIMELINES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_metric(
        "tso_maintenance_tracked_timelines",
        "Number of timelines tracked by the maintenance scheduler",
    )
});
pub static TSO_MAINTENANCE_EXTEND_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter_metric(
        "tso_maintenance_extend_total",
        "Total number of authority window extension attempts triggered by maintenance",
    )
});
pub static TSO_TIMELINE_PROXY_LANES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_metric(
        "tso_timeline_proxy_lanes",
        "Current number of timeline proxy lanes",
    )
});
pub static TSO_TIMELINE_PROXY_WAIT: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram_metric(
        "tso_timeline_proxy_wait_seconds",
        "Time spent waiting on timeline proxy serialization",
    )
});
pub static TSO_TIMELINE_PROXY_SATURATED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter_metric(
        "tso_timeline_proxy_saturated_total",
        "Total number of timeline proxy saturation events",
    )
});
pub static TSO_TIMELINE_PROXY_TIMEOUT_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter_metric(
        "tso_timeline_proxy_timeout_total",
        "Total number of timeline proxy timeout events",
    )
});
pub static TSO_WATCH_KEEPALIVE_DROPPED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter_metric(
        "tso_watch_keepalive_dropped_total",
        "Total number of watch keepalive events dropped due to backpressure",
    )
});
pub static TSO_WATCH_ROUTE_SEND_TIMEOUT_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter_metric(
        "tso_watch_route_send_timeout_total",
        "Total number of watch route/event sends that timed out due to slow consumers",
    )
});
pub static TSO_WATCH_RESYNC_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_metric(
        "tso_watch_resync_total",
        "Route watch resync lifecycle events",
        &["event"],
    )
});
pub static TSO_TIMELINE_RUNTIME_CACHE_ENTRIES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_metric(
        "tso_timeline_runtime_cache_entries",
        "Current number of cached timeline runtime entries",
    )
});
pub static TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_metric(
        "tso_timeline_runtime_cache_events_total",
        "Timeline runtime cache events",
        &["event"],
    )
});
pub static TSO_STARTUP_PREFLIGHT_FAIL_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_metric(
        "tso_startup_preflight_fail_total",
        "Total number of startup preflight failures",
        &["stage"],
    )
});
pub static TSO_STARTUP_BOOTSTRAP_FAIL_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_metric(
        "tso_startup_bootstrap_fail_total",
        "Total number of startup bootstrap failures",
        &["stage"],
    )
});
pub static TSO_STARTUP_READY: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_metric(
        "tso_startup_ready",
        "Whether the service has completed startup and is ready",
    )
});
pub static TSO_IDENTITY_LEASE_EVENTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_metric(
        "tso_identity_lease_events_total",
        "Identity lease lifecycle events",
        &["event"],
    )
});
pub static TSO_SHUTDOWN_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_metric(
        "tso_shutdown_total",
        "Controlled shutdown events",
        &["reason"],
    )
});
pub static TSO_WORKER_READINESS_TRANSITIONS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_metric(
        "tso_worker_readiness_transitions_total",
        "Worker readiness transitions by state and reason",
        &["from_state", "to_state", "reason"],
    )
});
pub static TSO_TRANSFER_OUTCOMES_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_metric(
        "tso_transfer_outcomes_total",
        "Transfer and failover action outcomes",
        &["action", "reason", "outcome"],
    )
});
pub static TSO_RECOVERY_EVENTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_metric(
        "tso_recovery_events_total",
        "Recover-and-continue events by component, operation, and reason",
        &["component", "operation", "reason"],
    )
});
pub static TSO_BUILD_INFO: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec_metric(
        "tso_build_info",
        "Chronos build identity labeled by version and commit",
        &["version", "commit"],
    )
});
pub static TSO_MIXED_VERSION_CONTRACT_INFO: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec_metric(
        "tso_mixed_version_contract_info",
        "Chronos mixed-version contract identity",
        &["contract_id"],
    )
});
pub static TSO_VERSION_COMPATIBILITY_MISMATCH_TOTAL: LazyLock<IntCounterVec> =
    LazyLock::new(|| {
        register_int_counter_vec_metric(
            "tso_version_compatibility_mismatch_total",
            "Mixed-version compatibility mismatches observed across Chronos surfaces",
            &["kind"],
        )
    });

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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    static DUPLICATE_METRIC_COUNTER: AtomicUsize = AtomicUsize::new(0);

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

    #[test]
    fn duplicate_metric_registration_does_not_panic() {
        let suffix = DUPLICATE_METRIC_COUNTER.fetch_add(1, Ordering::Relaxed);
        let metric_name = format!("chronos_duplicate_metric_registration_{suffix}");

        let first = register_metric(
            create_metric(
                IntCounter::new(metric_name.clone(), "duplicate test metric"),
                &metric_name,
            ),
            &metric_name,
        );
        let second = register_metric(
            create_metric(
                IntCounter::new(metric_name.clone(), "duplicate test metric"),
                &metric_name,
            ),
            &metric_name,
        );

        first.inc();
        second.inc();

        let families = prometheus::gather();
        assert!(families
            .iter()
            .any(|family| family.get_name() == metric_name));
    }
}
