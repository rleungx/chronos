use std::collections::{HashMap, HashSet};

use tonic::Status;

use crate::proto::v1::WatchTimelineRoutesRequest;
use crate::{TimelineRoute, TsoControlPlane, TsoError};

use super::translation;

const WATCH_ALL_SNAPSHOT_PAGE_SIZE: usize = 1_000;

enum WatchRouteScope {
    All,
    Filtered(HashSet<String>),
}

impl WatchRouteScope {
    fn from_timeline_keys(timeline_keys: Vec<String>) -> Self {
        let filter_keys: HashSet<String> = timeline_keys.into_iter().collect();
        if filter_keys.is_empty() {
            Self::All
        } else {
            Self::Filtered(filter_keys)
        }
    }

    fn contains(&self, timeline_key: &str) -> bool {
        match self {
            Self::All => true,
            Self::Filtered(filter_keys) => filter_keys.contains(timeline_key),
        }
    }

    fn requested_timeline_key_count(&self) -> usize {
        match self {
            Self::All => 0,
            Self::Filtered(filter_keys) => filter_keys.len(),
        }
    }
}

pub(super) struct WatchCompletenessTracker {
    scope: WatchRouteScope,
    delivered_versions: HashMap<String, u64>,
}

impl WatchCompletenessTracker {
    pub(super) fn new(request: WatchTimelineRoutesRequest) -> Self {
        let scope = WatchRouteScope::from_timeline_keys(request.timeline_keys);
        let delivered_versions = match &scope {
            WatchRouteScope::All => request.known_route_versions,
            WatchRouteScope::Filtered(filter_keys) => request
                .known_route_versions
                .into_iter()
                .filter(|(timeline_key, _)| filter_keys.contains(timeline_key))
                .collect(),
        };

        Self {
            scope,
            delivered_versions,
        }
    }

    pub(super) fn requested_timeline_key_count(&self) -> usize {
        self.scope.requested_timeline_key_count()
    }

    pub(super) fn accept_live_route(&mut self, route: &TimelineRoute) -> bool {
        if !self.scope.contains(route.timeline_key.as_str()) {
            return false;
        }

        let known_version = self
            .delivered_versions
            .get(route.timeline_key.as_str())
            .copied()
            .unwrap_or(0);
        if route.route_version <= known_version {
            return false;
        }

        self.delivered_versions
            .insert(route.timeline_key.clone(), route.route_version);
        true
    }

    pub(super) async fn load_snapshot_routes(
        &mut self,
        control_plane: &TsoControlPlane,
    ) -> Result<Vec<TimelineRoute>, Status> {
        match &self.scope {
            WatchRouteScope::All => {
                load_watch_all_routes(control_plane, &mut self.delivered_versions).await
            }
            WatchRouteScope::Filtered(filter_keys) => {
                load_keyed_routes(control_plane, filter_keys, &mut self.delivered_versions).await
            }
        }
    }
}

async fn load_keyed_routes(
    control_plane: &TsoControlPlane,
    filter_keys: &HashSet<String>,
    delivered_versions: &mut HashMap<String, u64>,
) -> Result<Vec<TimelineRoute>, Status> {
    let mut initial_routes = Vec::new();
    for timeline_key in filter_keys {
        match control_plane.get_timeline_route(timeline_key).await {
            Ok(route) => maybe_push_newer_route(route, delivered_versions, &mut initial_routes),
            Err(TsoError::TimelineNotFound { .. }) => {}
            Err(error) => return Err(translation::map_tso_error(error)),
        }
    }

    Ok(initial_routes)
}

async fn load_watch_all_routes(
    control_plane: &TsoControlPlane,
    delivered_versions: &mut HashMap<String, u64>,
) -> Result<Vec<TimelineRoute>, Status> {
    let mut routes = Vec::new();
    let mut start_after_timeline_key = None;

    loop {
        let page = control_plane
            .list_timeline_statuses(
                &[],
                None,
                start_after_timeline_key.as_deref(),
                WATCH_ALL_SNAPSHOT_PAGE_SIZE,
            )
            .await
            .map_err(translation::map_tso_error)?;

        for status in page.statuses {
            maybe_push_newer_route(status.route, delivered_versions, &mut routes);
        }

        let Some(next_start_after) = page.next_start_after else {
            break;
        };
        start_after_timeline_key = Some(next_start_after);
    }

    Ok(routes)
}

fn maybe_push_newer_route(
    route: TimelineRoute,
    delivered_versions: &mut HashMap<String, u64>,
    routes: &mut Vec<TimelineRoute>,
) {
    let known_version = delivered_versions
        .get(route.timeline_key.as_str())
        .copied()
        .unwrap_or(0);
    if route.route_version <= known_version {
        return;
    }

    delivered_versions.insert(route.timeline_key.clone(), route.route_version);
    routes.push(route);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::metadata::MemoryMetadataStore;
    use crate::{ManualClock, TsoConfig, TsoSecurityMode, TsoService};

    fn test_service() -> Arc<TsoService> {
        TsoService::new(
            TsoConfig {
                security_mode: Some(TsoSecurityMode::Required),
                grpc_tls_cert_file: Some("server.crt".into()),
                grpc_tls_key_file: Some("server.key".into()),
                grpc_client_ca_file: Some("ca.pem".into()),
                grpc_request_timeout_ms: Some(100),
                grpc_max_request_bytes: Some(1_024),
                grpc_max_concurrent_requests: Some(16),
                ..TsoConfig::default()
            },
            Arc::new(ManualClock::new(1_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn watch_all_snapshot_reads_authoritative_inventory() {
        let service = test_service();
        let first = service
            .ensure_timeline("watch.all.snapshot.alpha")
            .await
            .unwrap();
        let second = service
            .ensure_timeline("watch.all.snapshot.beta")
            .await
            .unwrap();

        let mut tracker = WatchCompletenessTracker::new(WatchTimelineRoutesRequest {
            timeline_keys: Vec::new(),
            known_route_versions: HashMap::new(),
            sdk_instance_id: "sdk-watch-all".into(),
        });

        let mut timeline_keys: Vec<_> = tracker
            .load_snapshot_routes(&service.control_plane())
            .await
            .unwrap()
            .into_iter()
            .map(|route| route.timeline_key)
            .collect();
        timeline_keys.sort();

        let mut expected = vec![first.timeline_key, second.timeline_key];
        expected.sort();
        assert_eq!(timeline_keys, expected);
    }

    #[tokio::test]
    async fn watch_all_resync_discovers_timelines_outside_known_versions() {
        let service = test_service();
        let known = service
            .ensure_timeline("watch.all.resync.known")
            .await
            .unwrap();

        let mut tracker = WatchCompletenessTracker::new(WatchTimelineRoutesRequest {
            timeline_keys: Vec::new(),
            known_route_versions: HashMap::from([(
                known.timeline_key.clone(),
                known.route_version,
            )]),
            sdk_instance_id: "sdk-watch-all-resync".into(),
        });

        let initial_routes = tracker
            .load_snapshot_routes(&service.control_plane())
            .await
            .unwrap();
        assert!(
            initial_routes.is_empty(),
            "known-version watch-all snapshot should not replay unchanged routes"
        );

        let late = service
            .ensure_timeline("watch.all.resync.late")
            .await
            .unwrap();
        let routes = tracker
            .load_snapshot_routes(&service.control_plane())
            .await
            .unwrap();

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].timeline_key, late.timeline_key);
        assert_eq!(routes[0].route_version, late.route_version);
    }

    #[test]
    fn filtered_watch_discards_request_external_known_versions() {
        let tracker = WatchCompletenessTracker::new(WatchTimelineRoutesRequest {
            timeline_keys: vec!["timeline-a".into()],
            known_route_versions: HashMap::from([
                ("timeline-a".into(), 3),
                ("timeline-b".into(), 9),
            ]),
            sdk_instance_id: "sdk-filtered".into(),
        });

        assert_eq!(tracker.delivered_versions.len(), 1);
        assert_eq!(tracker.delivered_versions.get("timeline-a"), Some(&3));
        assert!(!tracker.delivered_versions.contains_key("timeline-b"));
    }

    #[test]
    fn live_route_acceptance_respects_scope_and_monotonic_versions() {
        let mut tracker = WatchCompletenessTracker::new(WatchTimelineRoutesRequest {
            timeline_keys: vec!["timeline-a".into()],
            known_route_versions: HashMap::from([("timeline-a".into(), 3)]),
            sdk_instance_id: "sdk-filtered".into(),
        });

        assert!(!tracker.accept_live_route(&TimelineRoute {
            timeline_key: "timeline-b".into(),
            generator_id: 1,
            owner_worker_endpoint: "worker-b:50051".into(),
            epoch: 1,
            route_version: 1,
            resource_tier: crate::ResourceTier::Shared,
        }));
        assert!(!tracker.accept_live_route(&TimelineRoute {
            timeline_key: "timeline-a".into(),
            generator_id: 1,
            owner_worker_endpoint: "worker-a:50051".into(),
            epoch: 1,
            route_version: 3,
            resource_tier: crate::ResourceTier::Shared,
        }));
        assert!(tracker.accept_live_route(&TimelineRoute {
            timeline_key: "timeline-a".into(),
            generator_id: 1,
            owner_worker_endpoint: "worker-a:50051".into(),
            epoch: 1,
            route_version: 4,
            resource_tier: crate::ResourceTier::Shared,
        }));
    }
}
