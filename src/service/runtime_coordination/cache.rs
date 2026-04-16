use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::{broadcast, watch, Mutex};
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::metadata::{RouteUpdateSignal, TimelineRecord};
use crate::metrics;
use crate::planning::recovered_timeline_floor_tso;
use crate::service::endpoints_match;
use crate::timeline_state::build_timeline_state;
use crate::{ResourceTier, TimelineRoute, TsoError};

use super::super::TsoService;

const ROUTE_WATCH_LAG_CLEAR_COOLDOWN: Duration = Duration::from_millis(200);

fn should_clear_timeline_cache_for_route_update(
    cached_route: &TimelineRoute,
    updated_route: &TimelineRoute,
) -> bool {
    let same_route_target = cached_route.timeline_key == updated_route.timeline_key
        && cached_route.generator_id == updated_route.generator_id
        && cached_route.epoch == updated_route.epoch
        && cached_route.resource_tier == updated_route.resource_tier
        && endpoints_match(
            &cached_route.owner_worker_endpoint,
            &updated_route.owner_worker_endpoint,
        );

    !same_route_target && updated_route.route_version >= cached_route.route_version
}

impl TsoService {
    pub(in crate::service) fn best_effort_insert_timeline_cache(
        &self,
        timeline_key: &str,
        timeline_state: Arc<Mutex<crate::runtime::TimelineState>>,
    ) -> Result<bool, TsoError> {
        match self
            .timeline_runtime
            .insert_timeline(timeline_key.to_owned(), timeline_state)
        {
            Ok(()) => Ok(true),
            Err(TsoError::TimelineRuntimeCacheSaturated { .. }) => {
                metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                    .with_label_values(&["degraded_uncached"])
                    .inc();
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    pub(in crate::service) async fn timeline_state_handle_from_record(
        &self,
        timeline_key: &str,
        timeline_record: &TimelineRecord,
        revision: u64,
    ) -> Result<Arc<Mutex<crate::runtime::TimelineState>>, TsoError> {
        self.restore_dedicated_claim(timeline_key, timeline_record)?;

        let timeline_state =
            match self
                .timeline_runtime
                .timeline_handle_or_insert_with(timeline_key, || {
                    Arc::new(Mutex::new(build_timeline_state(
                        timeline_record,
                        revision,
                        recovered_timeline_floor_tso(timeline_record),
                    )))
                }) {
                Ok(handle) => handle,
                Err(TsoError::TimelineRuntimeCacheSaturated { .. }) => {
                    metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                        .with_label_values(&["degraded_uncached"])
                        .inc();
                    Arc::new(Mutex::new(build_timeline_state(
                        timeline_record,
                        revision,
                        recovered_timeline_floor_tso(timeline_record),
                    )))
                }
                Err(error) => return Err(error),
            };

        let mut cached_timeline_state = timeline_state.lock().await;
        cached_timeline_state.route = timeline_record.route.clone();
        cached_timeline_state.state = timeline_record.state;
        cached_timeline_state.last_issued_tso = cached_timeline_state
            .last_issued_tso
            .max(recovered_timeline_floor_tso(timeline_record));
        cached_timeline_state.recovery_floor_tso = cached_timeline_state
            .recovery_floor_tso
            .max(recovered_timeline_floor_tso(timeline_record));
        cached_timeline_state.last_graceful_issued = timeline_record.last_graceful_issued;
        cached_timeline_state.revision = revision;
        drop(cached_timeline_state);

        Ok(timeline_state)
    }

    pub(in crate::service) async fn invalidate_timeline_cache_for_route_update(
        &self,
        updated_route: &TimelineRoute,
    ) {
        let Some(cached_timeline_state_handle) = self
            .timeline_runtime
            .timeline_handle(&updated_route.timeline_key)
        else {
            return;
        };

        let should_clear = {
            let cached_timeline_state = cached_timeline_state_handle.lock().await;
            should_clear_timeline_cache_for_route_update(
                &cached_timeline_state.route,
                updated_route,
            )
        };

        if should_clear {
            let cached_route = {
                let cached_timeline_state = cached_timeline_state_handle.lock().await;
                cached_timeline_state.route.clone()
            };
            if cached_route.resource_tier == ResourceTier::Dedicated {
                self.release_dedicated(cached_route.generator_id, &cached_route.timeline_key);
            }
            self.clear_timeline_cache(&updated_route.timeline_key);
        }
    }

    pub(in crate::service) async fn metadata_route_update_loop(
        service: Weak<Self>,
        metadata_route_updates: &mut broadcast::Receiver<RouteUpdateSignal>,
        route_notifier: broadcast::Sender<TimelineRoute>,
        route_reset_notifier: broadcast::Sender<()>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let mut last_lag_clear_at: Option<Instant> = None;
        loop {
            let route_update = tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                    continue;
                }
                route_update = metadata_route_updates.recv() => route_update,
            };

            match route_update {
                Ok(RouteUpdateSignal::Route(route)) => {
                    let Some(service) = service.upgrade() else {
                        break;
                    };
                    debug!(
                        component = "route_watch",
                        event = "local_cache_invalidated",
                        result = "success",
                        reason = "metadata_update_broadcast",
                        timeline_key = %route.timeline_key,
                        route_version = route.route_version
                    );
                    service
                        .invalidate_timeline_cache_for_route_update(&route)
                        .await;
                    let _ = route_notifier.send(route);
                }
                Ok(RouteUpdateSignal::Reset) => {
                    let Some(service) = service.upgrade() else {
                        break;
                    };
                    warn!(
                        component = "route_watch",
                        event = "watch_restarted",
                        result = "degraded",
                        reason = "metadata_watch_reset"
                    );
                    service.timeline_runtime.clear();
                    let _ = route_reset_notifier.send(());
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let Some(service) = service.upgrade() else {
                        break;
                    };
                    let now = Instant::now();
                    if last_lag_clear_at.is_some_and(|last| {
                        now.duration_since(last) < ROUTE_WATCH_LAG_CLEAR_COOLDOWN
                    }) {
                        continue;
                    }
                    last_lag_clear_at = Some(now);
                    warn!(
                        component = "route_watch",
                        event = "watch_restarted",
                        result = "degraded",
                        reason = "broadcast_lagged"
                    );
                    service.timeline_runtime.clear();
                    service.clear_dedicated_claims();
                    let _ = route_reset_notifier.send(());
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    pub(in crate::service) fn restore_dedicated_claim(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
    ) -> Result<(), TsoError> {
        if record.route.resource_tier != ResourceTier::Dedicated {
            return Ok(());
        }
        if !self.is_local_endpoint(&record.route.owner_worker_endpoint) {
            return Ok(());
        }

        self.claim_specific_dedicated(record.route.generator_id, timeline_key)
            .map(|_| ())
    }

    pub(in crate::service) async fn upsert_timeline_cache_from_record(
        &self,
        timeline_key: &str,
        timeline_record: &TimelineRecord,
        revision: u64,
    ) -> Result<(), TsoError> {
        let _ = self
            .timeline_state_handle_from_record(timeline_key, timeline_record, revision)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::{
        sync::broadcast,
        time::{timeout, Duration as TokioDuration},
    };

    use crate::metadata::{
        ControlPlaneStore, GeneratorBatchOp, GeneratorLeaseAuthority, GeneratorRecord,
        MemoryMetadataStore, RouteUpdateSignal, RouteUpdateSource, TimelineAuthority,
        TimelineBatchOp, TimelineRecord,
    };
    use crate::{ManualClock, TsoConfig, TsoError, TsoSecurityMode, TsoService};
    use crate::{ResourceTier, TimelineRoute};

    use super::should_clear_timeline_cache_for_route_update;

    fn required_test_config(config: TsoConfig) -> TsoConfig {
        TsoConfig {
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some("server.crt".into()),
            grpc_tls_key_file: Some("server.key".into()),
            grpc_client_ca_file: Some("ca.pem".into()),
            grpc_request_timeout_ms: Some(100),
            grpc_max_request_bytes: Some(1024),
            grpc_max_concurrent_requests: Some(16),
            ..config
        }
    }

    fn with_worker(mut config: TsoConfig, worker_id: &str) -> TsoConfig {
        config.worker_id = worker_id.into();
        config.advertise_endpoint = format!("{worker_id}:50051");
        required_test_config(config)
    }

    fn route(generator_id: u32, route_version: u64) -> TimelineRoute {
        TimelineRoute {
            timeline_key: "timeline-a".into(),
            generator_id,
            epoch: 1,
            route_version,
            resource_tier: ResourceTier::Shared,
            owner_worker_endpoint: "worker-a:50051".into(),
        }
    }

    #[derive(Clone)]
    struct ResettableRouteStore {
        inner: Arc<MemoryMetadataStore>,
        signals: broadcast::Sender<RouteUpdateSignal>,
    }

    impl ResettableRouteStore {
        fn new(inner: Arc<MemoryMetadataStore>) -> Self {
            let (signals, _) = broadcast::channel(16);
            Self { inner, signals }
        }

        fn send_reset(&self) {
            let _ = self.signals.send(RouteUpdateSignal::Reset);
        }
    }

    #[async_trait]
    impl TimelineAuthority for ResettableRouteStore {
        async fn load_timeline(
            &self,
            timeline_key: &str,
        ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
            self.inner.load_timeline(timeline_key).await
        }

        async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
            self.inner.list_timelines().await
        }

        async fn create_timeline(
            &self,
            timeline_key: &str,
            record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            self.inner.create_timeline(timeline_key, record).await
        }

        async fn compare_exchange_timeline(
            &self,
            timeline_key: &str,
            expected_revision: u64,
            record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            self.inner
                .compare_exchange_timeline(timeline_key, expected_revision, record)
                .await
        }

        async fn compare_exchange_timelines(
            &self,
            operations: &[TimelineBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            self.inner.compare_exchange_timelines(operations).await
        }
    }

    #[async_trait]
    impl GeneratorLeaseAuthority for ResettableRouteStore {
        async fn load_generator(
            &self,
            generator_id: u32,
        ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
            self.inner.load_generator(generator_id).await
        }

        async fn create_generator(
            &self,
            generator_id: u32,
            record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            self.inner.create_generator(generator_id, record).await
        }

        async fn compare_exchange_generator(
            &self,
            generator_id: u32,
            expected_revision: u64,
            record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            self.inner
                .compare_exchange_generator(generator_id, expected_revision, record)
                .await
        }

        async fn compare_exchange_generators(
            &self,
            operations: &[GeneratorBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            self.inner.compare_exchange_generators(operations).await
        }
    }

    impl RouteUpdateSource for ResettableRouteStore {
        fn subscribe_route_updates(&self) -> broadcast::Receiver<RouteUpdateSignal> {
            self.signals.subscribe()
        }
    }

    #[async_trait]
    impl ControlPlaneStore for ResettableRouteStore {}

    #[test]
    fn cache_update_ignores_same_route_even_when_version_advances() {
        let cached = route(7, 1);
        let mut updated = cached.clone();
        updated.route_version = 2;

        assert!(!should_clear_timeline_cache_for_route_update(
            &cached, &updated
        ));
    }

    #[test]
    fn cache_update_ignores_older_conflicting_routes() {
        let cached = route(7, 3);
        let updated = route(8, 2);

        assert!(!should_clear_timeline_cache_for_route_update(
            &cached, &updated
        ));
    }

    #[test]
    fn cache_update_clears_on_equal_or_newer_conflicting_routes() {
        let cached = route(7, 3);

        assert!(should_clear_timeline_cache_for_route_update(
            &cached,
            &route(8, 3)
        ));
        assert!(should_clear_timeline_cache_for_route_update(
            &cached,
            &route(8, 4)
        ));
    }

    #[test]
    fn cache_update_treats_equivalent_owner_endpoint_forms_as_same_route() {
        let cached = route(7, 3);
        let mut updated = cached.clone();
        updated.route_version = 4;
        updated.owner_worker_endpoint = " WORKER-A:50051 ".into();

        assert!(!should_clear_timeline_cache_for_route_update(
            &cached, &updated
        ));
    }

    #[tokio::test]
    async fn route_update_notifies_after_cache_invalidation_for_stale_routes() {
        let base = TsoConfig {
            shared_generators: 4,
            warm_generators: 0,
            max_batch_per_request: 16,
            max_future_borrow_ms: 10_000,
            default_resource_tier: ResourceTier::Shared,
            ..TsoConfig::default()
        };
        let clock = Arc::new(ManualClock::new(20_000));
        let metadata = Arc::new(MemoryMetadataStore::new());

        let service_a = TsoService::new(
            with_worker(base.clone(), "worker-a"),
            clock.clone(),
            metadata.clone(),
        )
        .unwrap();
        let service_b = TsoService::new(with_worker(base, "worker-b"), clock, metadata).unwrap();

        let route_a = service_a
            .ensure_timeline("background.route.invalidate.timeline")
            .await
            .unwrap();
        assert!(service_a
            .timeline_runtime
            .timeline_handle(&route_a.timeline_key)
            .is_some());
        let mut route_updates = service_a.subscribe_route_changes();

        let transferred = service_b
            .transfer_timeline(
                &route_a.timeline_key,
                route_a.resource_tier,
                service_b.advertise_endpoint().to_string(),
                Some(1),
            )
            .await
            .unwrap();

        let observed = loop {
            let route_update = timeout(TokioDuration::from_millis(500), route_updates.recv())
                .await
                .expect("expected route update")
                .expect("route notifier should stay open");
            if route_update.timeline_key == transferred.timeline_key
                && route_update.route_version >= transferred.route_version
            {
                break route_update;
            }
        };

        assert_eq!(observed.route_version, transferred.route_version);
        assert!(
            service_a
                .timeline_runtime
                .timeline_handle(&route_a.timeline_key)
                .is_none(),
            "cache entry should already be invalidated when route notification is observed"
        );
    }

    #[tokio::test]
    async fn upstream_route_reset_clears_runtime_cache_and_notifies_watch_layer() {
        let clock = Arc::new(ManualClock::new(25_000));
        let inner = Arc::new(MemoryMetadataStore::new());
        let metadata = Arc::new(ResettableRouteStore::new(inner));
        let service = TsoService::new(
            with_worker(TsoConfig::default(), "worker-a"),
            clock,
            metadata.clone(),
        )
        .unwrap();

        let route = service
            .ensure_timeline("route-reset.timeline")
            .await
            .unwrap();
        assert!(service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .is_some());

        let mut reset_rx = service.timeline_runtime.reset_notifier().subscribe();
        metadata.send_reset();

        timeout(TokioDuration::from_millis(500), reset_rx.recv())
            .await
            .expect("route reset should be forwarded")
            .expect("route reset receiver should stay open");

        assert!(service
            .timeline_runtime
            .timeline_handle(&route.timeline_key)
            .is_none());
    }
}
