use std::pin::Pin;

use tokio::time::{Duration, Instant, Sleep};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tracing::{error, info, warn};

use crate::proto::v1::{
    AllocateTimestampsRequest, AllocateTimestampsResponse, TimelineRouteEvent,
    WatchTimelineRoutesRequest,
};
use crate::timeline_proxy::TimelineScopedAllocator;
use crate::{metrics, TsoControlPlane};

use super::{current_timestamp, route_mapping, translation, watch_completeness};

const WATCH_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const WATCH_ROUTE_SEND_TIMEOUT: Duration = Duration::from_millis(250);
const WATCH_RESYNC_MIN_INTERVAL: Duration = Duration::from_millis(200);
static WATCH_SESSION_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub(super) type WatchTimelineRoutesStream = ReceiverStream<Result<TimelineRouteEvent, Status>>;

struct WatchResyncGate {
    next_allowed_at: Instant,
    pending: bool,
}

enum WatchResyncLagEffect {
    Scheduled(Instant),
    Coalesced(Instant),
}

impl WatchResyncGate {
    fn new() -> Self {
        Self {
            next_allowed_at: Instant::now(),
            pending: false,
        }
    }

    fn lagged(&mut self, now: Instant) -> WatchResyncLagEffect {
        let scheduled_at = self.next_allowed_at.max(now);
        let already_pending = self.pending;
        self.pending = true;
        if already_pending {
            WatchResyncLagEffect::Coalesced(scheduled_at)
        } else {
            WatchResyncLagEffect::Scheduled(scheduled_at)
        }
    }

    fn complete(&mut self, now: Instant) {
        self.pending = false;
        self.next_allowed_at = now + WATCH_RESYNC_MIN_INTERVAL;
    }

    fn is_pending(&self) -> bool {
        self.pending
    }
}

pub(super) async fn watch_timeline_routes_stream(
    control_plane: &TsoControlPlane,
    route_cache_ttl_ms: u32,
    request: WatchTimelineRoutesRequest,
) -> Result<WatchTimelineRoutesStream, Status> {
    let mut completeness = watch_completeness::WatchCompletenessTracker::new(request);
    let requested_timeline_key_count = completeness.requested_timeline_key_count();
    let mut rx = control_plane.subscribe_route_changes();
    let mut reset_rx = control_plane.subscribe_route_resets();
    let initial_routes = completeness.load_snapshot_routes(control_plane).await?;
    let control_plane = control_plane.clone();
    let (event_tx, event_rx) = tokio::sync::mpsc::channel(128);
    let watch_session = WATCH_SESSION_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    tokio::spawn(async move {
        let mut keepalive_interval = tokio::time::interval(WATCH_KEEPALIVE_INTERVAL);
        let far_future = Duration::from_secs(24 * 60 * 60);
        let mut resync_gate = WatchResyncGate::new();
        let mut resync_sleep: Pin<Box<Sleep>> = Box::pin(tokio::time::sleep(far_future));
        keepalive_interval.tick().await;
        info!(
            component = "route_watch",
            event = "watch_started",
            result = "success",
            reason = "rpc_stream_started",
            watch_session,
            timeline_key_count = requested_timeline_key_count
        );
        if !send_route_events(&event_tx, initial_routes, route_cache_ttl_ms).await {
            return;
        }
        loop {
            tokio::select! {
                _ = &mut resync_sleep, if resync_gate.is_pending() => {
                    metrics::TSO_WATCH_RESYNC_TOTAL
                        .with_label_values(&["started"])
                        .inc();
                    let routes = match completeness.load_snapshot_routes(&control_plane).await {
                        Ok(routes) => routes,
                        Err(status) => {
                            error!(
                                component = "route_watch",
                                event = "resync_failed",
                                result = "failure",
                                reason = %status,
                                watch_session
                            );
                            let _ = send_watch_event_with_timeout(&event_tx, Err(status)).await;
                            break;
                        }
                    };
                    if !send_route_events(&event_tx, routes, route_cache_ttl_ms).await {
                        break;
                    }
                    info!(
                        component = "route_watch",
                        event = "resync_completed",
                        result = "success",
                        reason = "watch_lagged",
                        watch_session
                    );
                    metrics::TSO_WATCH_RESYNC_TOTAL
                        .with_label_values(&["completed"])
                        .inc();
                    resync_gate.complete(Instant::now());
                    resync_sleep.as_mut().reset(Instant::now() + far_future);
                }
                res = rx.recv() => {
                    match res {
                        Ok(route) => {
                            if !completeness.accept_live_route(&route) {
                                continue;
                            }
                            if !send_watch_event_with_timeout(
                                &event_tx,
                                Ok(route_mapping::build_route_event(route, route_cache_ttl_ms)),
                            )
                            .await
                            {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            match resync_gate.lagged(Instant::now()) {
                                WatchResyncLagEffect::Scheduled(when) => {
                                    warn!(
                                        component = "route_watch",
                                        event = "resync_scheduled",
                                        result = "degraded",
                                        reason = "watch_lagged",
                                        watch_session
                                    );
                                    metrics::TSO_WATCH_RESYNC_TOTAL
                                        .with_label_values(&["scheduled"])
                                        .inc();
                                    resync_sleep.as_mut().reset(when);
                                }
                                WatchResyncLagEffect::Coalesced(when) => {
                                    metrics::TSO_WATCH_RESYNC_TOTAL
                                        .with_label_values(&["coalesced"])
                                        .inc();
                                    resync_sleep.as_mut().reset(when);
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                res = reset_rx.recv() => {
                    match res {
                        Ok(()) => {
                            match resync_gate.lagged(Instant::now()) {
                                WatchResyncLagEffect::Scheduled(when) => {
                                    warn!(
                                        component = "route_watch",
                                        event = "resync_scheduled",
                                        result = "degraded",
                                        reason = "upstream_watch_reset",
                                        watch_session
                                    );
                                    metrics::TSO_WATCH_RESYNC_TOTAL
                                        .with_label_values(&["scheduled"])
                                        .inc();
                                    resync_sleep.as_mut().reset(when);
                                }
                                WatchResyncLagEffect::Coalesced(when) => {
                                    metrics::TSO_WATCH_RESYNC_TOTAL
                                        .with_label_values(&["coalesced"])
                                        .inc();
                                    resync_sleep.as_mut().reset(when);
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            match resync_gate.lagged(Instant::now()) {
                                WatchResyncLagEffect::Scheduled(when) => {
                                    warn!(
                                        component = "route_watch",
                                        event = "resync_scheduled",
                                        result = "degraded",
                                        reason = "reset_signal_lagged",
                                        watch_session
                                    );
                                    metrics::TSO_WATCH_RESYNC_TOTAL
                                        .with_label_values(&["scheduled"])
                                        .inc();
                                    resync_sleep.as_mut().reset(when);
                                }
                                WatchResyncLagEffect::Coalesced(when) => {
                                    metrics::TSO_WATCH_RESYNC_TOTAL
                                        .with_label_values(&["coalesced"])
                                        .inc();
                                    resync_sleep.as_mut().reset(when);
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = keepalive_interval.tick() => {
                    if !try_send_keepalive_event(&event_tx) {
                        break;
                    }
                }
            }
        }
    });

    Ok(ReceiverStream::new(event_rx))
}

pub(super) async fn allocate_timestamps_response(
    allocator: &TimelineScopedAllocator,
    request: AllocateTimestampsRequest,
) -> Result<AllocateTimestampsResponse, Status> {
    let (request, timeout_ms) = normalize_allocate_timestamps_request(request);
    let response = allocate_with_optional_timeout(allocator, request, timeout_ms).await?;
    Ok(build_allocate_timestamps_response(response))
}

fn try_send_keepalive_event(
    event_tx: &tokio::sync::mpsc::Sender<Result<TimelineRouteEvent, Status>>,
) -> bool {
    match event_tx.try_send(Ok(
        route_mapping::build_keepalive_event(current_timestamp()),
    )) {
        Ok(()) => true,
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            metrics::TSO_WATCH_KEEPALIVE_DROPPED_TOTAL.inc();
            warn!(
                component = "route_watch",
                event = "keepalive_dropped",
                result = "degraded",
                reason = "consumer_backpressure"
            );
            true
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
    }
}

async fn send_watch_event_with_timeout(
    event_tx: &tokio::sync::mpsc::Sender<Result<TimelineRouteEvent, Status>>,
    event: Result<TimelineRouteEvent, Status>,
) -> bool {
    match tokio::time::timeout(WATCH_ROUTE_SEND_TIMEOUT, event_tx.send(event)).await {
        Ok(Ok(())) => true,
        Ok(Err(_)) => false,
        Err(_) => {
            metrics::TSO_WATCH_ROUTE_SEND_TIMEOUT_TOTAL.inc();
            warn!(
                component = "route_watch",
                event = "send_timeout",
                result = "degraded",
                reason = "consumer_backpressure"
            );
            let _ = event_tx.try_send(Err(Status::resource_exhausted(
                "watch consumer backpressure",
            )));
            false
        }
    }
}

async fn allocate_with_optional_timeout(
    allocator: &TimelineScopedAllocator,
    request: crate::AllocateTimestampsRequest,
    timeout_ms: u32,
) -> Result<crate::AllocateTimestampsResponse, Status> {
    allocator
        .allocate_timestamps_with_timeout(request, timeout_ms)
        .await
        .map_err(translation::map_timeline_proxy_error)
}

fn normalize_allocate_timestamps_request(
    request: AllocateTimestampsRequest,
) -> (crate::AllocateTimestampsRequest, u32) {
    let timeout_ms = request.request_timeout_ms;
    (
        crate::AllocateTimestampsRequest {
            timeline_key: request.timeline_key,
            count: request.count,
            expected_epoch: request.expected_epoch,
            expected_route_version: request.expected_route_version,
            client_request_id: request.client_request_id,
        },
        timeout_ms,
    )
}

fn build_allocate_timestamps_response(
    response: crate::AllocateTimestampsResponse,
) -> AllocateTimestampsResponse {
    AllocateTimestampsResponse {
        timeline_key: response.timeline_key,
        generator_id: response.generator_id,
        epoch: response.epoch,
        route_version: response.route_version,
        ranges: route_mapping::build_timestamp_ranges(response.ranges),
    }
}

async fn send_route_events(
    event_tx: &tokio::sync::mpsc::Sender<Result<TimelineRouteEvent, Status>>,
    routes: Vec<crate::TimelineRoute>,
    route_cache_ttl_ms: u32,
) -> bool {
    for route in routes {
        if !send_watch_event_with_timeout(
            event_tx,
            Ok(route_mapping::build_route_event(route, route_cache_ttl_ms)),
        )
        .await
        {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use tokio::time::Duration;
    use tonic::Code;

    use super::*;
    use crate::TimestampRange;

    #[tokio::test]
    async fn route_event_send_times_out_when_watch_queue_is_full() {
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(1);
        event_tx
            .try_send(Ok(
                route_mapping::build_keepalive_event(current_timestamp()),
            ))
            .unwrap();

        let before = metrics::TSO_WATCH_ROUTE_SEND_TIMEOUT_TOTAL.get();
        let delivered = send_watch_event_with_timeout(
            &event_tx,
            Ok(route_mapping::build_keepalive_event(current_timestamp())),
        )
        .await;

        assert!(!delivered);
        assert!(metrics::TSO_WATCH_ROUTE_SEND_TIMEOUT_TOTAL.get() > before);
    }

    #[tokio::test]
    async fn route_event_timeout_only_best_effort_delivers_resource_exhausted() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
        event_tx
            .try_send(Ok(
                route_mapping::build_keepalive_event(current_timestamp()),
            ))
            .unwrap();

        let delivered = send_watch_event_with_timeout(
            &event_tx,
            Ok(route_mapping::build_keepalive_event(current_timestamp())),
        )
        .await;

        assert!(!delivered);

        let first = event_rx.recv().await.expect("expected queued event");
        match first {
            Ok(event) => {
                assert!(matches!(
                    event.event,
                    Some(crate::proto::v1::timeline_route_event::Event::Keepalive(_))
                ));
            }
            Err(status) => panic!("unexpected first event status: {status}"),
        }

        let maybe_second = tokio::time::timeout(Duration::from_millis(50), event_rx.recv()).await;
        if let Ok(Some(Err(status))) = maybe_second {
            assert_eq!(status.code(), Code::ResourceExhausted);
            assert!(status.message().contains("backpressure"));
        }
    }

    #[test]
    fn keepalive_is_dropped_when_watch_queue_is_full() {
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(1);
        event_tx
            .try_send(Ok(
                route_mapping::build_keepalive_event(current_timestamp()),
            ))
            .unwrap();

        let before = metrics::TSO_WATCH_KEEPALIVE_DROPPED_TOTAL.get();
        let keep_running = try_send_keepalive_event(&event_tx);

        assert!(keep_running);
        assert!(metrics::TSO_WATCH_KEEPALIVE_DROPPED_TOTAL.get() > before);
    }

    #[test]
    fn keepalive_success_does_not_increment_drop_metric() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);

        let keep_running = try_send_keepalive_event(&event_tx);

        assert!(keep_running);
        assert!(event_rx.try_recv().is_ok());
    }

    #[test]
    fn keepalive_returns_false_when_watch_queue_is_closed() {
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(1);
        drop(event_rx);

        assert!(!try_send_keepalive_event(&event_tx));
    }

    #[test]
    fn watch_resync_gate_coalesces_lagged_signals_until_completion() {
        let mut gate = WatchResyncGate::new();
        let now = Instant::now();

        let first = gate.lagged(now);
        let second = gate.lagged(now + Duration::from_millis(50));

        assert!(gate.is_pending());
        assert!(matches!(first, WatchResyncLagEffect::Scheduled(when) if when == now));
        assert!(matches!(
            second,
            WatchResyncLagEffect::Coalesced(when) if when == now + Duration::from_millis(50)
        ));

        gate.complete(now + Duration::from_millis(50));
        assert!(!gate.is_pending());

        let scheduled = gate.lagged(now + Duration::from_millis(100));
        assert!(matches!(
            scheduled,
            WatchResyncLagEffect::Scheduled(when) if when == now + Duration::from_millis(250)
        ));
    }

    #[test]
    fn watch_resync_metrics_distinguish_scheduled_from_coalesced() {
        let scheduled_before = metrics::TSO_WATCH_RESYNC_TOTAL
            .with_label_values(&["scheduled"])
            .get();
        let coalesced_before = metrics::TSO_WATCH_RESYNC_TOTAL
            .with_label_values(&["coalesced"])
            .get();

        let mut gate = WatchResyncGate::new();
        let now = Instant::now();
        let first = gate.lagged(now);
        let second = gate.lagged(now + Duration::from_millis(10));

        if matches!(first, WatchResyncLagEffect::Scheduled(_)) {
            metrics::TSO_WATCH_RESYNC_TOTAL
                .with_label_values(&["scheduled"])
                .inc();
        }
        if matches!(second, WatchResyncLagEffect::Coalesced(_)) {
            metrics::TSO_WATCH_RESYNC_TOTAL
                .with_label_values(&["coalesced"])
                .inc();
        }

        assert!(
            metrics::TSO_WATCH_RESYNC_TOTAL
                .with_label_values(&["scheduled"])
                .get()
                > scheduled_before
        );
        assert!(
            metrics::TSO_WATCH_RESYNC_TOTAL
                .with_label_values(&["coalesced"])
                .get()
                > coalesced_before
        );
    }

    #[test]
    fn normalize_allocate_request_preserves_rpc_fields_and_timeout() {
        let (request, timeout_ms) =
            normalize_allocate_timestamps_request(AllocateTimestampsRequest {
                timeline_key: "timeline-a".into(),
                count: 8,
                expected_epoch: 4,
                expected_route_version: 12,
                client_request_id: "req-1".into(),
                request_timeout_ms: 150,
            });

        assert_eq!(request.timeline_key, "timeline-a");
        assert_eq!(request.count, 8);
        assert_eq!(request.expected_epoch, 4);
        assert_eq!(request.expected_route_version, 12);
        assert_eq!(request.client_request_id, "req-1");
        assert_eq!(timeout_ms, 150);
    }

    #[test]
    fn allocate_response_keeps_epoch_route_version_and_ranges() {
        let response = build_allocate_timestamps_response(crate::AllocateTimestampsResponse {
            timeline_key: "timeline-a".into(),
            generator_id: 7,
            epoch: 4,
            route_version: 12,
            ranges: vec![
                TimestampRange {
                    start_tso: 100,
                    end_tso: 109,
                },
                TimestampRange {
                    start_tso: 110,
                    end_tso: 119,
                },
            ],
        });

        assert_eq!(response.timeline_key, "timeline-a");
        assert_eq!(response.generator_id, 7);
        assert_eq!(response.epoch, 4);
        assert_eq!(response.route_version, 12);
        assert_eq!(response.ranges.len(), 2);
        assert_eq!(response.ranges[0].start_tso, 100);
        assert_eq!(response.ranges[0].end_tso, 109);
        assert_eq!(response.ranges[1].start_tso, 110);
        assert_eq!(response.ranges[1].end_tso, 119);
    }
}
