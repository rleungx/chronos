use lazy_static::lazy_static;
use prometheus::{
    register_histogram, register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge,
};

lazy_static! {
    pub static ref TSO_ALLOCATE_TOTAL: IntCounter = register_int_counter!(
        "tso_allocate_total",
        "Total number of TSO allocation requests"
    )
    .unwrap();
    pub static ref TSO_ALLOCATE_COUNT_TOTAL: IntCounter = register_int_counter!(
        "tso_allocate_count_total",
        "Total number of timestamps allocated"
    )
    .unwrap();
    pub static ref TSO_ALLOCATE_LATENCY: Histogram = register_histogram!(
        "tso_allocate_latency_seconds",
        "Latency of TSO allocation in seconds"
    )
    .unwrap();
    pub static ref TSO_SEQUENCE_UTILIZATION: IntGauge = register_int_gauge!(
        "tso_sequence_utilization",
        "Sequence number utilization within the current millisecond"
    )
    .unwrap();
    pub static ref TSO_LEASE_EXPIRED_TOTAL: IntCounter = register_int_counter!(
        "tso_lease_expired_total",
        "Total number of lease expiration events"
    )
    .unwrap();
    pub static ref TSO_CLOCK_BACKWARDS_TOTAL: IntCounter = register_int_counter!(
        "tso_clock_backwards_total",
        "Total number of clock backwards events"
    )
    .unwrap();
    pub static ref TSO_METADATA_LATENCY: HistogramVec = register_histogram_vec!(
        "tso_metadata_latency_seconds",
        "Metadata store request latency in seconds",
        &["op"]
    )
    .unwrap();
    pub static ref TSO_METADATA_ERRORS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "tso_metadata_errors_total",
        "Metadata store request errors",
        &["op"]
    )
    .unwrap();
    pub static ref TSO_MAINTENANCE_TRACKED_TIMELINES: IntGauge = register_int_gauge!(
        "tso_maintenance_tracked_timelines",
        "Number of timelines tracked by the maintenance scheduler"
    )
    .unwrap();
    pub static ref TSO_MAINTENANCE_EXTEND_TOTAL: IntCounter = register_int_counter!(
        "tso_maintenance_extend_total",
        "Total number of authority window extension attempts triggered by maintenance"
    )
    .unwrap();
    pub static ref TSO_TIMELINE_PROXY_LANES: IntGauge = register_int_gauge!(
        "tso_timeline_proxy_lanes",
        "Current number of timeline proxy lanes"
    )
    .unwrap();
    pub static ref TSO_TIMELINE_PROXY_WAIT: Histogram = register_histogram!(
        "tso_timeline_proxy_wait_seconds",
        "Time spent waiting on timeline proxy serialization"
    )
    .unwrap();
    pub static ref TSO_TIMELINE_PROXY_SATURATED_TOTAL: IntCounter = register_int_counter!(
        "tso_timeline_proxy_saturated_total",
        "Total number of timeline proxy saturation events"
    )
    .unwrap();
    pub static ref TSO_TIMELINE_PROXY_TIMEOUT_TOTAL: IntCounter = register_int_counter!(
        "tso_timeline_proxy_timeout_total",
        "Total number of timeline proxy timeout events"
    )
    .unwrap();
    pub static ref TSO_WATCH_KEEPALIVE_DROPPED_TOTAL: IntCounter = register_int_counter!(
        "tso_watch_keepalive_dropped_total",
        "Total number of watch keepalive events dropped due to backpressure"
    )
    .unwrap();
    pub static ref TSO_WATCH_ROUTE_SEND_TIMEOUT_TOTAL: IntCounter = register_int_counter!(
        "tso_watch_route_send_timeout_total",
        "Total number of watch route/event sends that timed out due to slow consumers"
    )
    .unwrap();
    pub static ref TSO_TIMELINE_RUNTIME_CACHE_ENTRIES: IntGauge = register_int_gauge!(
        "tso_timeline_runtime_cache_entries",
        "Current number of cached timeline runtime entries"
    )
    .unwrap();
    pub static ref TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "tso_timeline_runtime_cache_events_total",
            "Timeline runtime cache events",
            &["event"]
        )
        .unwrap();
}
