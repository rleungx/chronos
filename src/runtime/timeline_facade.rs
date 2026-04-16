use std::sync::Arc;

use tokio::sync::{broadcast, Mutex};

use crate::{TimelineLifecycleState, TimelineRoute, TsoError};

use super::cache_eviction;

#[derive(Debug, Clone)]
pub(crate) struct TimelineState {
    pub(crate) route: TimelineRoute,
    pub(crate) state: TimelineLifecycleState,
    pub(crate) last_issued_tso: Option<u64>,
    pub(crate) recovery_floor_tso: Option<u64>,
    pub(crate) last_graceful_issued: Option<u64>,
    pub(crate) revision: u64,
}

pub(crate) struct TimelineRuntimeState {
    cache: cache_eviction::TimelineCacheStore,
    route_notifier: broadcast::Sender<TimelineRoute>,
    route_reset_notifier: broadcast::Sender<()>,
}

impl TimelineRuntimeState {
    pub(crate) fn new(max_entries: usize) -> Self {
        let (route_notifier, _) = broadcast::channel(1024);
        let (route_reset_notifier, _) = broadcast::channel(1024);
        Self {
            cache: cache_eviction::TimelineCacheStore::new(max_entries),
            route_notifier,
            route_reset_notifier,
        }
    }

    pub(crate) fn timeline_count(&self) -> usize {
        self.cache.timeline_count()
    }

    pub(crate) fn timeline_handle(&self, timeline_key: &str) -> Option<Arc<Mutex<TimelineState>>> {
        self.cache.timeline_handle(timeline_key)
    }

    pub(crate) fn timeline_handle_or_insert_with<F>(
        &self,
        timeline_key: &str,
        build: F,
    ) -> Result<Arc<Mutex<TimelineState>>, TsoError>
    where
        F: FnOnce() -> Arc<Mutex<TimelineState>>,
    {
        self.cache
            .timeline_handle_or_insert_with(timeline_key, build)
    }

    pub(crate) fn insert_timeline(
        &self,
        timeline_key: String,
        timeline: Arc<Mutex<TimelineState>>,
    ) -> Result<(), TsoError> {
        self.cache.insert_timeline(timeline_key, timeline)
    }

    pub(crate) fn remove_timeline(&self, timeline_key: &str) {
        self.cache.remove_timeline(timeline_key);
    }

    pub(crate) fn clear(&self) {
        self.cache.clear();
    }

    pub(crate) fn subscribe_route_changes(&self) -> broadcast::Receiver<TimelineRoute> {
        self.route_notifier.subscribe()
    }

    pub(crate) fn notifier(&self) -> broadcast::Sender<TimelineRoute> {
        self.route_notifier.clone()
    }

    pub(crate) fn reset_notifier(&self) -> broadcast::Sender<()> {
        self.route_reset_notifier.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceTier;

    fn sample_route(timeline_key: &str, route_version: u64) -> TimelineRoute {
        TimelineRoute {
            timeline_key: timeline_key.to_owned(),
            generator_id: 3,
            epoch: 2,
            route_version,
            resource_tier: ResourceTier::Shared,
            owner_worker_endpoint: "worker-a".to_owned(),
        }
    }

    fn sample_timeline(timeline_key: &str, route_version: u64) -> Arc<Mutex<TimelineState>> {
        Arc::new(Mutex::new(TimelineState {
            route: sample_route(timeline_key, route_version),
            state: TimelineLifecycleState::Active,
            last_issued_tso: Some(41),
            recovery_floor_tso: Some(41),
            last_graceful_issued: Some(40),
            revision: 9,
        }))
    }

    #[test]
    fn timeline_runtime_facade_delegates_cache_surface() {
        let runtime = TimelineRuntimeState::new(2);
        let timeline = sample_timeline("runtime.facade.timeline", 1);

        runtime
            .insert_timeline("runtime.facade.timeline".to_owned(), timeline.clone())
            .expect("insert should succeed");

        assert_eq!(runtime.timeline_count(), 1);
        assert!(runtime.timeline_handle("runtime.facade.timeline").is_some());

        runtime.remove_timeline("runtime.facade.timeline");
        assert_eq!(runtime.timeline_count(), 0);

        let rebuilt = runtime
            .timeline_handle_or_insert_with("runtime.facade.timeline", || timeline.clone())
            .expect("reinsert through facade should succeed");
        assert!(Arc::ptr_eq(&rebuilt, &timeline));

        runtime.clear();
        assert_eq!(runtime.timeline_count(), 0);
    }

    #[tokio::test]
    async fn route_notifier_surface_round_trips_updates() {
        let runtime = TimelineRuntimeState::new(1);
        let mut updates = runtime.subscribe_route_changes();
        let route = sample_route("runtime.facade.route", 7);

        runtime
            .notifier()
            .send(route.clone())
            .expect("route send should succeed");

        let observed = updates.recv().await.expect("route update should arrive");
        assert_eq!(observed, route);
    }
}
