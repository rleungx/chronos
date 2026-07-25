use std::collections::BTreeSet;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::MutexGuard;
use std::sync::{Mutex as StdMutex, Weak};
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::recovery::record_recovery_event;
use crate::{TimelineLifecycleState, TransferReason, TsoError};

const REQUEST_RECORD_CLEANUP_INTERVAL_MS: u64 = 60_000;
const REQUEST_RECORD_CLEANUP_BATCH_SIZE: usize = 512;

pub(super) struct BackgroundCoordinator {
    shutdown_tx: watch::Sender<bool>,
    tasks: StdMutex<Vec<JoinHandle<()>>>,
    shutdown_started: AtomicBool,
}

impl BackgroundCoordinator {
    fn tasks_lock(&self) -> MutexGuard<'_, Vec<JoinHandle<()>>> {
        match self.tasks.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event("service", "background_tasks_lock", "mutex_poisoned");
                poisoned.into_inner()
            }
        }
    }

    pub(super) fn new() -> Self {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        Self {
            shutdown_tx,
            tasks: StdMutex::new(Vec::new()),
            shutdown_started: AtomicBool::new(false),
        }
    }

    fn register_task(&self, handle: JoinHandle<()>) {
        self.tasks_lock().push(handle);
    }

    pub(super) fn shutdown_listener(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    pub(super) fn spawn_tracked<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.register_task(tokio::spawn(async move {
            if AssertUnwindSafe(task).catch_unwind().await.is_err() {
                record_recovery_event("service", "background_task", "panic");
            }
        }));
    }

    pub(super) fn begin_shutdown(&self) -> bool {
        if self.shutdown_started.swap(true, AtomicOrdering::AcqRel) {
            return false;
        }
        let _ = self.shutdown_tx.send(true);
        true
    }

    pub(super) async fn drain_tasks(&self) {
        let tasks = {
            let mut guard = self.tasks_lock();
            std::mem::take(&mut *guard)
        };
        for task in tasks {
            let _ = task.await;
        }
    }

    pub(super) fn abort_all(&mut self) {
        if !self.shutdown_started.swap(true, AtomicOrdering::AcqRel) {
            let _ = self.shutdown_tx.send(true);
        }
        let tasks = match self.tasks.get_mut() {
            Ok(tasks) => tasks,
            Err(poisoned) => {
                record_recovery_event("service", "background_tasks_abort", "mutex_poisoned");
                poisoned.into_inner()
            }
        };
        for task in tasks.drain(..) {
            task.abort();
        }
    }
}

use super::TsoService;

impl TsoService {
    pub(super) async fn background_generator_maintenance_loop(
        service: Weak<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Some(interval_ms) = service
            .upgrade()
            .map(|service| service.config.generator_maintenance_interval_ms)
        else {
            return;
        };
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => {
                    let Some(service) = service.upgrade() else {
                        break;
                    };
                    let now_ms = service.clock.now_ms();
                    if let Some(generator_id) = service.next_generator_with_ownership_drift(now_ms) {
                        if service.ensure_generator_lease(generator_id).await.is_err() {
                            record_recovery_event(
                                "service",
                                "background_generator_maintenance",
                                "drift_reacquire_failed",
                            );
                        }
                    }
                    let keys = service.generator_runtime.lease_keys();
                    service.record_generator_lease_headroom(&keys, now_ms);
                    service.refresh_generator_leases_batch(keys).await;
                }
            }
        }
    }

    pub(super) async fn background_request_record_cleanup_loop(
        service: Weak<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Some(interval_ms) = service
            .upgrade()
            .map(|_| REQUEST_RECORD_CLEANUP_INTERVAL_MS)
        else {
            return;
        };
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => {
                    let Some(service) = service.upgrade() else {
                        break;
                    };
                    let Some(request_records) = service.metadata.request_records() else {
                        continue;
                    };
                    let cutoff_ms = service
                        .clock
                        .now_ms()
                        .saturating_sub(service.config.request_record_retention_ms);
                    match request_records
                        .prune_completed_request_records(
                            cutoff_ms,
                            REQUEST_RECORD_CLEANUP_BATCH_SIZE,
                        )
                        .await
                    {
                        Ok(pruned) => {
                            if pruned > 0 {
                                crate::metrics::TSO_REQUEST_RECORD_CLEANUP_TOTAL
                                    .with_label_values(&["pruned"])
                                    .inc_by(pruned as u64);
                            }
                        }
                        Err(_) => {
                            crate::metrics::TSO_REQUEST_RECORD_CLEANUP_TOTAL
                                .with_label_values(&["error"])
                                .inc();
                            record_recovery_event(
                                "service",
                                "background_request_record_cleanup",
                                "metadata_error",
                            );
                        }
                    }
                }
            }
        }
    }

    pub(super) async fn background_auto_failover_loop(
        service: Weak<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Some(interval_ms) = service
            .upgrade()
            .map(|service| service.config.auto_failover_interval_ms)
        else {
            return;
        };
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => {
                    let Some(service) = service.upgrade() else {
                        break;
                    };
                    if let Err(error) = service.auto_failover_pass().await {
                        crate::metrics::TSO_AUTO_FAILOVER_TOTAL
                            .with_label_values(&["scan_error"])
                            .inc();
                        record_recovery_event(
                            "service",
                            "auto_failover",
                            auto_failover_error_reason(&error),
                        );
                    }
                }
            }
        }
    }

    pub(super) async fn auto_failover_pass(&self) -> Result<usize, TsoError> {
        let owner_endpoints = self.auto_failover_remote_owner_endpoints().await?;
        if owner_endpoints.is_empty() {
            crate::metrics::TSO_AUTO_FAILOVER_TOTAL
                .with_label_values(&["idle"])
                .inc();
            return Ok(0);
        }
        self.auto_failover_owner_targeted_pass(&owner_endpoints)
            .await
    }

    async fn auto_failover_remote_owner_endpoints(&self) -> Result<Vec<String>, TsoError> {
        let now_ms = self.clock.now_ms();
        if let Some(owner_endpoints) = self.load_cached_auto_failover_owner_endpoints(now_ms) {
            return Ok(owner_endpoints);
        }

        let mut endpoints = BTreeSet::new();
        for record in self.metadata.scan_generators().await? {
            let endpoint = record.owner_worker_endpoint.trim();
            if endpoint.is_empty() || self.is_local_endpoint(endpoint) {
                continue;
            }
            endpoints.insert(endpoint.to_owned());
        }
        let owner_endpoints = endpoints.into_iter().collect::<Vec<_>>();
        self.store_cached_auto_failover_owner_endpoints(owner_endpoints.clone(), now_ms);
        Ok(owner_endpoints)
    }

    async fn auto_failover_owner_targeted_pass(
        &self,
        owner_endpoints: &[String],
    ) -> Result<usize, TsoError> {
        let batch_size = self.config.auto_failover_batch_size;
        let scan_limit = batch_size.saturating_mul(8).max(batch_size);
        let mut scanned = 0usize;
        let mut attempted = 0usize;
        let mut succeeded = 0usize;
        let mut owner_index = self.load_auto_failover_owner_index(owner_endpoints);
        let mut idle_owner_pages = 0usize;

        while attempted < batch_size
            && scanned < scan_limit
            && idle_owner_pages < owner_endpoints.len()
        {
            let owner_endpoint = owner_endpoints[owner_index].clone();
            let cursor = self.load_auto_failover_owner_cursor(&owner_endpoint);
            let fetch_limit = (scan_limit - scanned).min(batch_size).max(1);
            let page = self
                .metadata
                .list_timelines_by_status_filter_page(
                    &[TimelineLifecycleState::Active],
                    Some(&owner_endpoint),
                    cursor.as_deref(),
                    fetch_limit,
                )
                .await?;

            if page.records.is_empty() && page.next_start_after_timeline_key.is_none() {
                self.store_auto_failover_owner_cursor(&owner_endpoint, None);
                owner_index = (owner_index + 1) % owner_endpoints.len();
                idle_owner_pages += 1;
                continue;
            }

            idle_owner_pages = 0;
            let page_exhausted = page.next_start_after_timeline_key.is_none();
            let page_record_count = page.records.len();
            let mut processed_records = 0usize;
            let mut next_cursor = page.next_start_after_timeline_key.clone();
            let mut retry_before_cursor_advance = false;
            for timeline in page.records {
                scanned += 1;
                next_cursor = Some(timeline.route.timeline_key.clone());
                processed_records += 1;

                attempted += 1;
                match self
                    .transfer_timeline_for_rpc(
                        &timeline.route.timeline_key,
                        self.config.advertise_endpoint.clone(),
                        None,
                        TransferReason::Failover,
                    )
                    .await
                {
                    Ok(_) => {
                        succeeded += 1;
                        crate::metrics::TSO_AUTO_FAILOVER_TOTAL
                            .with_label_values(&["succeeded"])
                            .inc();
                    }
                    Err(error) => {
                        crate::metrics::TSO_AUTO_FAILOVER_TOTAL
                            .with_label_values(&[auto_failover_error_outcome(&error)])
                            .inc();
                        if !auto_failover_error_is_expected(&error) {
                            record_recovery_event(
                                "service",
                                "auto_failover",
                                auto_failover_error_reason(&error),
                            );
                        }
                        if auto_failover_retry_before_cursor_advance(&error) {
                            retry_before_cursor_advance = true;
                        }
                    }
                }

                if attempted >= batch_size || retry_before_cursor_advance {
                    break;
                }
            }

            if retry_before_cursor_advance {
                crate::metrics::TSO_AUTO_FAILOVER_TOTAL
                    .with_label_values(&["deferred"])
                    .inc();
            } else if page_exhausted && processed_records == page_record_count {
                next_cursor = None;
            }
            self.store_auto_failover_owner_cursor(&owner_endpoint, next_cursor);
            owner_index = (owner_index + 1) % owner_endpoints.len();
            self.store_auto_failover_owner_index(owner_index, owner_endpoints);

            if attempted >= batch_size || scanned >= scan_limit {
                break;
            }
        }

        if attempted == 0 {
            crate::metrics::TSO_AUTO_FAILOVER_TOTAL
                .with_label_values(&["idle"])
                .inc();
        }

        Ok(succeeded)
    }

    fn auto_failover_scan_lock(&self) -> MutexGuard<'_, super::AutoFailoverScanState> {
        match self.auto_failover_scan.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event("service", "auto_failover_scan", "mutex_poisoned");
                poisoned.into_inner()
            }
        }
    }

    fn load_auto_failover_owner_index(&self, owner_endpoints: &[String]) -> usize {
        if owner_endpoints.is_empty() {
            return 0;
        }
        self.auto_failover_scan_lock().owner_cursor_index % owner_endpoints.len()
    }

    fn store_auto_failover_owner_index(&self, owner_index: usize, owner_endpoints: &[String]) {
        let mut scan = self.auto_failover_scan_lock();
        scan.owner_cursor_index = if owner_endpoints.is_empty() {
            0
        } else {
            owner_index % owner_endpoints.len()
        };
        scan.owner_cursors.retain(|owner_endpoint, _| {
            owner_endpoints
                .iter()
                .any(|active_owner| active_owner == owner_endpoint)
        });
    }

    fn load_cached_auto_failover_owner_endpoints(&self, now_ms: u64) -> Option<Vec<String>> {
        let scan = self.auto_failover_scan_lock();
        if scan.owner_endpoints_refresh_after_ms == 0
            || now_ms >= scan.owner_endpoints_refresh_after_ms
        {
            return None;
        }
        Some(scan.owner_endpoints.clone())
    }

    fn store_cached_auto_failover_owner_endpoints(
        &self,
        owner_endpoints: Vec<String>,
        now_ms: u64,
    ) {
        let refresh_after_ms =
            now_ms.saturating_add(self.auto_failover_owner_endpoint_refresh_interval_ms());
        let mut scan = self.auto_failover_scan_lock();
        scan.owner_endpoints = owner_endpoints;
        scan.owner_endpoints_refresh_after_ms = refresh_after_ms;
    }

    fn auto_failover_owner_endpoint_refresh_interval_ms(&self) -> u64 {
        self.config
            .auto_failover_interval_ms
            .saturating_mul(10)
            .clamp(1_000, 30_000)
    }

    fn load_auto_failover_owner_cursor(&self, owner_endpoint: &str) -> Option<String> {
        self.auto_failover_scan_lock()
            .owner_cursors
            .get(owner_endpoint)
            .cloned()
            .flatten()
    }

    fn store_auto_failover_owner_cursor(&self, owner_endpoint: &str, cursor: Option<String>) {
        self.auto_failover_scan_lock()
            .owner_cursors
            .insert(owner_endpoint.to_owned(), cursor);
    }
}

fn auto_failover_error_is_expected(error: &TsoError) -> bool {
    matches!(
        error,
        TsoError::FailoverRequiresExpiredLease { .. }
            | TsoError::FailoverLeaseExpiryUnknown { .. }
            | TsoError::FailoverMissingRecoveryFloor { .. }
            | TsoError::RecoveryCatchupBudgetExceeded { .. }
            | TsoError::GeneratorPoolExhausted
            | TsoError::GeneratorNotOwnedByThisWorker { .. }
            | TsoError::UnsafeRemoteTransferTarget { .. }
            | TsoError::TimelineNotFound { .. }
            | TsoError::CasFailed
    )
}

fn auto_failover_retry_before_cursor_advance(error: &TsoError) -> bool {
    matches!(error, TsoError::FailoverRequiresExpiredLease { .. })
}

fn auto_failover_error_outcome(error: &TsoError) -> &'static str {
    match error {
        TsoError::CasFailed | TsoError::TimelineNotFound { .. } => "contended",
        error if auto_failover_error_is_expected(error) => "blocked",
        _ => "error",
    }
}

fn auto_failover_error_reason(error: &TsoError) -> &'static str {
    match error {
        TsoError::FailoverRequiresExpiredLease { .. } => "lease_not_expired",
        TsoError::FailoverLeaseExpiryUnknown { .. } => "lease_expiry_unknown",
        TsoError::FailoverMissingRecoveryFloor { .. } => "recovery_floor_missing",
        TsoError::RecoveryCatchupBudgetExceeded { .. } => "catchup_budget_exceeded",
        TsoError::GeneratorPoolExhausted => "generator_pool_exhausted",
        TsoError::GeneratorNotOwnedByThisWorker { .. } => "generator_not_owned",
        TsoError::UnsafeRemoteTransferTarget { .. } => "unsafe_remote_target",
        TsoError::TimelineNotFound { .. } => "timeline_not_found",
        TsoError::CasFailed => "cas_failed",
        _ => "unexpected_error",
    }
}

impl TsoService {
    fn record_generator_lease_headroom(&self, generator_ids: &[u32], now_ms: u64) {
        let mut active_local_leases = 0i64;
        let mut min_headroom_ms: Option<u64> = None;

        for generator_id in generator_ids {
            let Some(lease_state) = self.generator_runtime.lease_state(*generator_id) else {
                continue;
            };
            if lease_state.owner_instance_id != self.local_instance_id()
                || lease_state.lease_expire_at_ms <= now_ms
            {
                continue;
            }

            active_local_leases += 1;
            let headroom_ms = lease_state.lease_expire_at_ms - now_ms;
            min_headroom_ms = Some(match min_headroom_ms {
                Some(current) => current.min(headroom_ms),
                None => headroom_ms,
            });
        }

        crate::metrics::TSO_GENERATOR_ACTIVE_LEASES.set(active_local_leases);
        crate::metrics::TSO_GENERATOR_MIN_LEASE_HEADROOM_MS
            .set(min_headroom_ms.unwrap_or(0).min(i64::MAX as u64) as i64);
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::Arc;
    use tokio::sync::Notify;

    use crate::metadata::{
        GeneratorLeaseAuthority, GeneratorRecord, MemoryMetadataStore, TimelineAuthority,
        TimelineRecord,
    };
    use crate::{
        encode_tso, metrics, ManualClock, ResourceTier, TimelineLifecycleState, TimelineRoute,
        TsoConfig, TsoSecurityMode, TsoService,
    };

    use super::{AtomicBool, AtomicOrdering, BackgroundCoordinator, Duration};

    #[tokio::test]
    async fn background_coordinator_begin_shutdown_notifies_once() {
        let coordinator = BackgroundCoordinator::new();
        let mut shutdown_rx = coordinator.shutdown_listener();

        assert!(coordinator.begin_shutdown());
        shutdown_rx.changed().await.expect("shutdown should notify");
        assert!(*shutdown_rx.borrow());
        assert!(!coordinator.begin_shutdown());
    }

    #[tokio::test]
    async fn background_coordinator_drain_tasks_waits_for_registered_tasks() {
        let coordinator = BackgroundCoordinator::new();
        let completed = Arc::new(Notify::new());
        let completed_signal = completed.clone();

        coordinator.spawn_tracked(async move {
            completed_signal.notify_one();
        });

        coordinator.drain_tasks().await;
        completed.notified().await;
    }

    #[tokio::test]
    async fn background_coordinator_abort_all_aborts_registered_tasks() {
        let mut coordinator = BackgroundCoordinator::new();
        let mut shutdown_rx = coordinator.shutdown_listener();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_flag = completed.clone();

        coordinator.spawn_tracked(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            completed_flag.store(true, AtomicOrdering::Release);
        });

        coordinator.abort_all();
        shutdown_rx
            .changed()
            .await
            .expect("abort should notify shutdown");
        tokio::task::yield_now().await;
        assert!(!completed.load(AtomicOrdering::Acquire));
    }

    #[test]
    fn background_coordinator_recovers_from_poisoned_task_lock() {
        let mut coordinator = BackgroundCoordinator::new();
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = coordinator.tasks.lock().unwrap();
            panic!("poison task lock");
        }));

        let _guard = coordinator.tasks_lock();
        drop(_guard);
        coordinator.abort_all();
    }

    #[tokio::test]
    async fn spawn_tracked_records_recovery_event_when_task_panics() {
        let coordinator = BackgroundCoordinator::new();
        let before = metrics::TSO_RECOVERY_EVENTS_TOTAL
            .with_label_values(&["service", "background_task", "panic"])
            .get();

        coordinator.spawn_tracked(async move {
            panic!("background task panic should be recorded");
        });
        coordinator.drain_tasks().await;

        assert!(
            metrics::TSO_RECOVERY_EVENTS_TOTAL
                .with_label_values(&["service", "background_task", "panic"])
                .get()
                > before
        );
    }

    fn test_config(endpoint: &str, instance_id: &str) -> TsoConfig {
        TsoConfig {
            worker_id: instance_id.to_string(),
            instance_id: instance_id.to_string(),
            advertise_endpoint: endpoint.to_string(),
            bind_addr: endpoint.to_string(),
            safety_gap_ms: 1,
            lease_ttl_ms: 100,
            generator_maintenance_interval_ms: 10,
            ..TsoConfig::default().with_security_mode(TsoSecurityMode::DevInsecure)
        }
    }

    #[tokio::test]
    async fn auto_failover_pass_moves_expired_remote_timeline_to_local_owner() {
        let metadata = Arc::new(MemoryMetadataStore::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let previous_floor = encode_tso(900, 0, 0).expect("floor should encode");
        metadata
            .create_generator(
                0,
                &GeneratorRecord {
                    schema_version: 1,
                    generator_id: 0,
                    owner_worker_endpoint: "127.0.0.1:50051".into(),
                    owner_instance_id: "instance-a".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(900),
                    last_issued_tso: Some(previous_floor),
                    issued_upper_bound: Some(previous_floor),
                    updated_at_ms: 900,
                },
            )
            .await
            .expect("generator should seed");
        metadata
            .create_timeline(
                "auto.failover.timeline",
                &TimelineRecord {
                    schema_version: 1,
                    route: TimelineRoute {
                        timeline_key: "auto.failover.timeline".into(),
                        generator_id: 0,
                        owner_worker_endpoint: "127.0.0.1:50051".into(),
                        epoch: 1,
                        route_version: 1,
                        resource_tier: ResourceTier::Shared,
                    },
                    state: TimelineLifecycleState::Active,
                    recovery_floor_tso: None,
                    issued_upper_bound: Some(previous_floor),
                    last_graceful_issued: None,
                    lease_expire_at_ms: Some(900),
                    updated_at_ms: 900,
                },
            )
            .await
            .expect("timeline should seed");

        let service = TsoService::new(
            test_config("127.0.0.1:50052", "instance-b"),
            clock,
            metadata.clone(),
        )
        .expect("service should start");

        let moved = service
            .auto_failover_pass()
            .await
            .expect("auto failover pass should complete");

        assert_eq!(moved, 1);
        let (timeline, _) = metadata
            .load_timeline("auto.failover.timeline")
            .await
            .expect("timeline should load")
            .expect("timeline should exist");
        assert_eq!(timeline.route.owner_worker_endpoint, "127.0.0.1:50052");
        assert!(timeline.route.route_version > 1);

        service.shutdown().await;
    }

    #[tokio::test]
    async fn auto_failover_pass_defers_unexpired_timeline_without_blocking_later_records() {
        let metadata = Arc::new(MemoryMetadataStore::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let previous_floor = encode_tso(900, 0, 0).expect("floor should encode");
        metadata
            .create_generator(
                0,
                &GeneratorRecord {
                    schema_version: 1,
                    generator_id: 0,
                    owner_worker_endpoint: "127.0.0.1:50051".into(),
                    owner_instance_id: "instance-a".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(1_100),
                    last_issued_tso: Some(previous_floor),
                    issued_upper_bound: Some(previous_floor),
                    updated_at_ms: 900,
                },
            )
            .await
            .expect("generator should seed");

        for timeline_key in ["a.remote.timeline", "b.remote.timeline"] {
            metadata
                .create_timeline(
                    timeline_key,
                    &TimelineRecord {
                        schema_version: 1,
                        route: TimelineRoute {
                            timeline_key: timeline_key.into(),
                            generator_id: 0,
                            owner_worker_endpoint: "127.0.0.1:50051".into(),
                            epoch: 1,
                            route_version: 1,
                            resource_tier: ResourceTier::Shared,
                        },
                        state: TimelineLifecycleState::Active,
                        recovery_floor_tso: None,
                        issued_upper_bound: Some(previous_floor),
                        last_graceful_issued: None,
                        lease_expire_at_ms: Some(1_100),
                        updated_at_ms: 900,
                    },
                )
                .await
                .expect("timeline should seed");
        }

        let mut config = test_config("127.0.0.1:50052", "instance-b");
        config.auto_failover_batch_size = 1;
        let service =
            TsoService::new(config, clock.clone(), metadata.clone()).expect("service should start");

        let moved = service
            .auto_failover_pass()
            .await
            .expect("auto failover pass should complete");
        assert_eq!(moved, 0);

        clock.set(1_101);
        let moved = service
            .auto_failover_pass()
            .await
            .expect("auto failover retry should complete");
        assert_eq!(moved, 1);

        let (first, _) = metadata
            .load_timeline("a.remote.timeline")
            .await
            .expect("timeline should load")
            .expect("timeline should exist");
        let (second, _) = metadata
            .load_timeline("b.remote.timeline")
            .await
            .expect("timeline should load")
            .expect("timeline should exist");
        assert_eq!(first.route.owner_worker_endpoint, "127.0.0.1:50051");
        assert_eq!(second.route.owner_worker_endpoint, "127.0.0.1:50052");

        let moved = service
            .auto_failover_pass()
            .await
            .expect("auto failover wraparound retry should complete");
        assert_eq!(moved, 1);

        let (first, _) = metadata
            .load_timeline("a.remote.timeline")
            .await
            .expect("timeline should load")
            .expect("timeline should exist");
        assert_eq!(first.route.owner_worker_endpoint, "127.0.0.1:50052");

        service.shutdown().await;
    }

    #[tokio::test]
    async fn auto_failover_pass_targets_remote_owner_without_scanning_local_prefix() {
        let metadata = Arc::new(MemoryMetadataStore::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let previous_floor = encode_tso(900, 0, 0).expect("floor should encode");
        metadata
            .create_generator(
                0,
                &GeneratorRecord {
                    schema_version: 1,
                    generator_id: 0,
                    owner_worker_endpoint: "127.0.0.1:50051".into(),
                    owner_instance_id: "instance-a".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(900),
                    last_issued_tso: Some(previous_floor),
                    issued_upper_bound: Some(previous_floor),
                    updated_at_ms: 900,
                },
            )
            .await
            .expect("generator should seed");

        for index in 0..9 {
            let timeline_key = format!("a.local.{index:03}");
            metadata
                .create_timeline(
                    &timeline_key,
                    &TimelineRecord {
                        schema_version: 1,
                        route: TimelineRoute {
                            timeline_key: timeline_key.clone(),
                            generator_id: 0,
                            owner_worker_endpoint: "127.0.0.1:50052".into(),
                            epoch: 1,
                            route_version: 1,
                            resource_tier: ResourceTier::Shared,
                        },
                        state: TimelineLifecycleState::Active,
                        recovery_floor_tso: None,
                        issued_upper_bound: Some(previous_floor),
                        last_graceful_issued: None,
                        lease_expire_at_ms: Some(900),
                        updated_at_ms: 900,
                    },
                )
                .await
                .expect("local timeline should seed");
        }

        metadata
            .create_timeline(
                "z.remote.timeline",
                &TimelineRecord {
                    schema_version: 1,
                    route: TimelineRoute {
                        timeline_key: "z.remote.timeline".into(),
                        generator_id: 0,
                        owner_worker_endpoint: "127.0.0.1:50051".into(),
                        epoch: 1,
                        route_version: 1,
                        resource_tier: ResourceTier::Shared,
                    },
                    state: TimelineLifecycleState::Active,
                    recovery_floor_tso: None,
                    issued_upper_bound: Some(previous_floor),
                    last_graceful_issued: None,
                    lease_expire_at_ms: Some(900),
                    updated_at_ms: 900,
                },
            )
            .await
            .expect("remote timeline should seed");

        let mut config = test_config("127.0.0.1:50052", "instance-b");
        config.auto_failover_batch_size = 1;
        let service =
            TsoService::new(config, clock, metadata.clone()).expect("service should start");

        let moved = service
            .auto_failover_pass()
            .await
            .expect("auto failover pass should complete");
        assert_eq!(moved, 1);

        let (timeline, _) = metadata
            .load_timeline("z.remote.timeline")
            .await
            .expect("timeline should load")
            .expect("timeline should exist");
        assert_eq!(timeline.route.owner_worker_endpoint, "127.0.0.1:50052");

        service.shutdown().await;
    }
}
