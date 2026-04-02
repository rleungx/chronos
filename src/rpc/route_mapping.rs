use prost_types::Timestamp;

use crate::proto::v1::{timeline_route_event, TimelineRouteEvent, WatchKeepalive};
use crate::{TimelineRoute, TimestampRange};

use super::status_mapping;

pub(super) fn build_route_event(
    route: TimelineRoute,
    route_cache_ttl_ms: u32,
) -> TimelineRouteEvent {
    TimelineRouteEvent {
        event: Some(timeline_route_event::Event::Route(
            status_mapping::proto_timeline_route(route, route_cache_ttl_ms),
        )),
    }
}

pub(super) fn build_keepalive_event(server_time: Timestamp) -> TimelineRouteEvent {
    TimelineRouteEvent {
        event: Some(timeline_route_event::Event::Keepalive(WatchKeepalive {
            server_time: Some(server_time),
        })),
    }
}

pub(super) fn build_timestamp_ranges(
    ranges: Vec<TimestampRange>,
) -> Vec<crate::proto::v1::TimestampRange> {
    ranges
        .into_iter()
        .map(|range| crate::proto::v1::TimestampRange {
            start_tso: range.start_tso,
            end_tso: range.end_tso,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::v1::{
        ResourceTier as ProtoResourceTier, TimelineRoute as ProtoTimelineRoute,
    };
    use crate::ResourceTier;

    #[test]
    fn route_event_keeps_route_fields_and_cache_ttl() {
        let event = build_route_event(
            TimelineRoute {
                timeline_key: "timeline-a".into(),
                generator_id: 7,
                owner_worker_endpoint: "worker-a:50051".into(),
                epoch: 4,
                route_version: 9,
                resource_tier: ResourceTier::Warm,
            },
            54_321,
        );

        let Some(timeline_route_event::Event::Route(route)) = event.event else {
            panic!("expected route event");
        };

        assert_eq!(
            route,
            ProtoTimelineRoute {
                timeline_key: "timeline-a".into(),
                generator_id: 7,
                owner_worker_endpoint: "worker-a:50051".into(),
                epoch: 4,
                route_version: 9,
                resource_tier: ProtoResourceTier::Warm as i32,
                cache_ttl_ms: 54_321,
            }
        );
    }

    #[test]
    fn keepalive_event_keeps_supplied_server_time() {
        let event = build_keepalive_event(Timestamp {
            seconds: 12,
            nanos: 34,
        });

        let Some(timeline_route_event::Event::Keepalive(keepalive)) = event.event else {
            panic!("expected keepalive event");
        };

        let server_time = keepalive.server_time.expect("server_time");
        assert_eq!(server_time.seconds, 12);
        assert_eq!(server_time.nanos, 34);
    }

    #[test]
    fn timestamp_ranges_keep_start_and_end_bounds() {
        let ranges = build_timestamp_ranges(vec![
            TimestampRange {
                start_tso: 10,
                end_tso: 19,
            },
            TimestampRange {
                start_tso: 20,
                end_tso: 29,
            },
        ]);

        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].start_tso, 10);
        assert_eq!(ranges[0].end_tso, 19);
        assert_eq!(ranges[1].start_tso, 20);
        assert_eq!(ranges[1].end_tso, 29);
    }
}
