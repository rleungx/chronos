use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::MutexGuard as StdMutexGuard;
use std::time::Duration;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::sync::Mutex;

use crate::plane::RequestCancellation;
use crate::recovery::record_recovery_event;
use crate::{
    metrics, AllocateTimestampsRequest, AllocateTimestampsResponse, TsoDataPlane, TsoError,
};

#[derive(Clone)]
pub struct TimelineScopedAllocator {
    data_plane: TsoDataPlane,
    timeline_serializers: Arc<DashMap<String, Arc<TimelineSerializer>>>,
    idle_serializers: Arc<DashMap<Arc<str>, ()>>,
    idle_transition_gate: Arc<StdMutex<()>>,
    serializer_slots: Arc<AtomicUsize>,
    max_serializers: usize,
}

const TIMEOUT_CLEANUP_GRACE_MS: u64 = 10;

struct TimelineSerializer {
    serialize: Mutex<()>,
    active_references: AtomicUsize,
    timeline_key: Arc<str>,
    lifecycle: StdMutex<()>,
}

struct TimelineSerializerLease {
    serializer: Arc<TimelineSerializer>,
    idle_serializers: Arc<DashMap<Arc<str>, ()>>,
    idle_transition_gate: Arc<StdMutex<()>>,
}

impl Drop for TimelineSerializerLease {
    fn drop(&mut self) {
        loop {
            let current = self.serializer.active_references.load(Ordering::Acquire);
            if current > 1 {
                if self
                    .serializer
                    .active_references
                    .compare_exchange(current, current - 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return;
                }
                continue;
            }

            let _transition =
                TimelineScopedAllocator::lock_idle_transition_gate(&self.idle_transition_gate);
            if self
                .serializer
                .active_references
                .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.idle_serializers
                    .insert(self.serializer.timeline_key.clone(), ());
                return;
            }
        }
    }
}

impl TimelineScopedAllocator {
    pub fn new(data_plane: TsoDataPlane) -> Self {
        let max_serializers = data_plane.max_timeline_proxy_lanes();
        Self {
            data_plane,
            timeline_serializers: Arc::new(DashMap::new()),
            idle_serializers: Arc::new(DashMap::new()),
            idle_transition_gate: Arc::new(StdMutex::new(())),
            serializer_slots: Arc::new(AtomicUsize::new(0)),
            max_serializers,
        }
    }

    pub async fn allocate_timestamps(
        &self,
        request: AllocateTimestampsRequest,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        self.allocate_timestamps_cancellable(request, None).await
    }

    async fn allocate_timestamps_cancellable(
        &self,
        request: AllocateTimestampsRequest,
        cancellation: Option<RequestCancellation>,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        let serializer_lease = self.serializer_for(request.timeline_key.as_str())?;
        let timer = metrics::TSO_TIMELINE_PROXY_WAIT.start_timer();
        let _guard = serializer_lease.serializer.serialize.lock().await;
        timer.observe_duration();
        self.data_plane
            .allocate_timestamps_with_cancellation(request, cancellation)
            .await
    }

    pub async fn allocate_timestamps_with_timeout(
        &self,
        request: AllocateTimestampsRequest,
        timeout_ms: u32,
    ) -> Result<AllocateTimestampsResponse, TimelineProxyError> {
        if timeout_ms == 0 {
            return self
                .allocate_timestamps(request)
                .await
                .map_err(TimelineProxyError::Tso);
        }

        let cancellation = RequestCancellation::new();
        let allocator = self.clone();
        let mut task = tokio::spawn({
            let cancellation = cancellation.clone();
            async move {
                allocator
                    .allocate_timestamps_cancellable(request, Some(cancellation))
                    .await
            }
        });

        tokio::select! {
            result = &mut task => {
                match result {
                    Ok(Ok(response)) => Ok(response),
                    Ok(Err(TsoError::RequestCancelled)) => {
                        metrics::TSO_TIMELINE_PROXY_TIMEOUT_TOTAL.inc();
                        Err(TimelineProxyError::TimedOut)
                    }
                    Ok(Err(error)) => Err(TimelineProxyError::Tso(error)),
                    Err(join_error) => Err(TimelineProxyError::Tso(TsoError::Internal(format!("timeline proxy task join failed: {join_error}")))),
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(timeout_ms as u64)) => {
                cancellation.cancel();
                let cleanup_grace = Duration::from_millis(TIMEOUT_CLEANUP_GRACE_MS.min(timeout_ms as u64));
                if tokio::time::timeout(cleanup_grace, &mut task).await.is_err() {
                    task.abort();
                    let _ = task.await;
                }
                metrics::TSO_TIMELINE_PROXY_TIMEOUT_TOTAL.inc();
                Err(TimelineProxyError::TimedOut)
            }
        }
    }

    fn lease_existing_serializer(&self, timeline_key: &str) -> Option<TimelineSerializerLease> {
        let existing = self.timeline_serializers.get(timeline_key)?;
        let serializer = existing.clone();
        {
            let _lifecycle = Self::lock_serializer_lifecycle(&serializer);
            let previous_references = serializer.active_references.fetch_add(1, Ordering::AcqRel);
            if previous_references == 0 {
                self.idle_serializers
                    .remove(serializer.timeline_key.as_ref());
            }
        }
        Some(TimelineSerializerLease {
            serializer,
            idle_serializers: self.idle_serializers.clone(),
            idle_transition_gate: self.idle_transition_gate.clone(),
        })
    }

    fn serializer_for(&self, timeline_key: &str) -> Result<TimelineSerializerLease, TsoError> {
        if let Some(existing) = self.lease_existing_serializer(timeline_key) {
            return Ok(existing);
        }

        loop {
            let current = self.serializer_slots.load(Ordering::Acquire);
            if current >= self.max_serializers {
                if let Some(existing) = self.lease_existing_serializer(timeline_key) {
                    return Ok(existing);
                }
                self.prune_one_idle_serializer();
                if let Some(existing) = self.lease_existing_serializer(timeline_key) {
                    return Ok(existing);
                }
                if self.serializer_slots.load(Ordering::Acquire) >= self.max_serializers {
                    metrics::TSO_TIMELINE_PROXY_SATURATED_TOTAL.inc();
                    return Err(TsoError::TimelineIngressSaturated {
                        timeline_key: timeline_key.to_owned(),
                        max_lanes: self.max_serializers,
                    });
                }
                continue;
            }

            if self
                .serializer_slots
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }

            match self.timeline_serializers.entry(timeline_key.to_owned()) {
                Entry::Occupied(entry) => {
                    let serializer = entry.get().clone();
                    {
                        let _lifecycle = Self::lock_serializer_lifecycle(&serializer);
                        self.serializer_slots.fetch_sub(1, Ordering::AcqRel);
                        let previous_references =
                            serializer.active_references.fetch_add(1, Ordering::AcqRel);
                        if previous_references == 0 {
                            self.idle_serializers
                                .remove(serializer.timeline_key.as_ref());
                        }
                    }
                    return Ok(TimelineSerializerLease {
                        serializer,
                        idle_serializers: self.idle_serializers.clone(),
                        idle_transition_gate: self.idle_transition_gate.clone(),
                    });
                }
                Entry::Vacant(entry) => {
                    let timeline_key = Arc::<str>::from(timeline_key);
                    let serializer = Arc::new(TimelineSerializer {
                        serialize: Mutex::new(()),
                        active_references: AtomicUsize::new(1),
                        timeline_key,
                        lifecycle: StdMutex::new(()),
                    });
                    entry.insert(serializer.clone());
                    metrics::TSO_TIMELINE_PROXY_LANES.set(self.timeline_serializers.len() as i64);
                    return Ok(TimelineSerializerLease {
                        serializer,
                        idle_serializers: self.idle_serializers.clone(),
                        idle_transition_gate: self.idle_transition_gate.clone(),
                    });
                }
            }
        }
    }

    fn prune_one_idle_serializer(&self) -> bool {
        if self.try_prune_idle_candidates(4) {
            return true;
        }

        {
            let _transition = Self::lock_idle_transition_gate(&self.idle_transition_gate);
        }

        self.try_prune_idle_candidates(4)
    }

    fn try_prune_idle_candidates(&self, attempts: usize) -> bool {
        for _ in 0..attempts {
            let idle_key = self
                .idle_serializers
                .iter()
                .next()
                .map(|entry| entry.key().clone());
            let Some(idle_key) = idle_key else {
                break;
            };
            self.idle_serializers.remove(idle_key.as_ref());
            if let Entry::Occupied(entry) = self.timeline_serializers.entry(idle_key.to_string()) {
                let serializer = entry.get().clone();
                let _lifecycle = Self::lock_serializer_lifecycle(&serializer);
                if serializer.active_references.load(Ordering::Acquire) == 0 {
                    entry.remove();
                    self.serializer_slots.fetch_sub(1, Ordering::AcqRel);
                    metrics::TSO_TIMELINE_PROXY_LANES.set(self.timeline_serializers.len() as i64);
                    return true;
                }
            }
        }
        false
    }

    fn lock_serializer_lifecycle(serializer: &TimelineSerializer) -> StdMutexGuard<'_, ()> {
        match serializer.lifecycle.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event(
                    "timeline_proxy",
                    "serializer_lifecycle_lock",
                    "mutex_poisoned",
                );
                poisoned.into_inner()
            }
        }
    }

    fn lock_idle_transition_gate(gate: &StdMutex<()>) -> StdMutexGuard<'_, ()> {
        match gate.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event("timeline_proxy", "idle_transition_gate", "mutex_poisoned");
                poisoned.into_inner()
            }
        }
    }

    pub fn serializer_count(&self) -> usize {
        self.timeline_serializers.len()
    }

    pub fn max_serializers(&self) -> usize {
        self.max_serializers
    }
}

#[derive(Debug)]
pub enum TimelineProxyError {
    Tso(TsoError),
    TimedOut,
}

#[cfg(test)]
mod tests {
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::Arc;

    use crate::metadata::MemoryMetadataStore;
    use crate::metrics;
    use crate::{ManualClock, TsoConfig, TsoSecurityMode, TsoService};

    use super::*;

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

    fn test_allocator(max_timeline_proxy_lanes: usize) -> TimelineScopedAllocator {
        let service = TsoService::new(
            required_test_config(TsoConfig {
                max_timeline_proxy_lanes,
                ..TsoConfig::default()
            }),
            Arc::new(ManualClock::new(12_000)),
            Arc::new(MemoryMetadataStore::new()),
        )
        .unwrap();
        TimelineScopedAllocator::new(service.data_plane())
    }

    #[tokio::test]
    async fn lease_drop_marks_idle_only_on_last_reference() {
        let allocator = test_allocator(2);
        let first = allocator.serializer_for("proxy.unit.idle").unwrap();
        let second = allocator.serializer_for("proxy.unit.idle").unwrap();

        drop(first);
        assert!(allocator.idle_serializers.is_empty());

        drop(second);
        assert!(allocator.idle_serializers.contains_key("proxy.unit.idle"));
    }

    #[tokio::test]
    async fn reacquiring_idle_serializer_clears_idle_index() {
        let allocator = test_allocator(1);
        let first = allocator.serializer_for("proxy.unit.reuse").unwrap();
        drop(first);
        assert!(allocator.idle_serializers.contains_key("proxy.unit.reuse"));

        let second = allocator.serializer_for("proxy.unit.reuse").unwrap();
        assert!(!allocator.idle_serializers.contains_key("proxy.unit.reuse"));

        drop(second);
        assert!(allocator.idle_serializers.contains_key("proxy.unit.reuse"));
    }

    #[tokio::test]
    async fn stale_idle_index_entry_does_not_prune_active_lane() {
        let allocator = test_allocator(1);
        let lease = allocator.serializer_for("proxy.unit.stale").unwrap();

        allocator
            .idle_serializers
            .insert(Arc::<str>::from("proxy.unit.stale"), ());

        assert!(!allocator.prune_one_idle_serializer());
        assert_eq!(allocator.serializer_count(), 1);
        assert_eq!(allocator.serializer_slots.load(Ordering::Acquire), 1);

        drop(lease);
        assert!(allocator.idle_serializers.contains_key("proxy.unit.stale"));
    }

    #[tokio::test]
    async fn lease_revival_blocks_prune_until_lane_is_active() {
        let allocator = test_allocator(1);
        let initial = allocator.serializer_for("proxy.unit.race").unwrap();
        drop(initial);

        let serializer = allocator
            .timeline_serializers
            .get("proxy.unit.race")
            .expect("serializer should remain cached")
            .clone();
        let lease_allocator = allocator.clone();
        let prune_allocator = allocator.clone();
        let (lease_started_tx, lease_started_rx) = std::sync::mpsc::channel();
        let (lease_task, prune_task) = {
            let _lifecycle = serializer
                .lifecycle
                .lock()
                .expect("timeline serializer lifecycle lock poisoned");

            let lease_task = tokio::task::spawn_blocking(move || {
                lease_started_tx
                    .send(())
                    .expect("lease start signal should send");
                lease_allocator.serializer_for("proxy.unit.race")
            });
            lease_started_rx
                .recv()
                .expect("lease task should reach blocked state");

            let prune_task =
                tokio::task::spawn_blocking(move || prune_allocator.prune_one_idle_serializer());

            (lease_task, prune_task)
        };

        let revived = lease_task
            .await
            .expect("lease task should join")
            .expect("reviving existing serializer should succeed");
        let pruned = prune_task.await.expect("prune task should join");

        assert!(!pruned);
        assert_eq!(allocator.serializer_count(), 1);
        assert_eq!(allocator.serializer_slots.load(Ordering::Acquire), 1);

        drop(revived);
        assert!(allocator.idle_serializers.contains_key("proxy.unit.race"));
    }

    #[tokio::test]
    async fn poisoned_lifecycle_lock_recovers_and_records_metric() {
        let allocator = test_allocator(1);
        let initial = allocator.serializer_for("proxy.unit.poison").unwrap();
        drop(initial);

        let serializer = allocator
            .timeline_serializers
            .get("proxy.unit.poison")
            .expect("serializer should remain cached")
            .clone();
        let before = metrics::TSO_RECOVERY_EVENTS_TOTAL
            .with_label_values(&[
                "timeline_proxy",
                "serializer_lifecycle_lock",
                "mutex_poisoned",
            ])
            .get();

        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = serializer.lifecycle.lock().unwrap();
            panic!("poison timeline serializer lifecycle lock");
        }));

        let revived = allocator
            .serializer_for("proxy.unit.poison")
            .expect("poisoned lifecycle lock should recover");
        assert!(
            metrics::TSO_RECOVERY_EVENTS_TOTAL
                .with_label_values(&[
                    "timeline_proxy",
                    "serializer_lifecycle_lock",
                    "mutex_poisoned",
                ])
                .get()
                > before
        );

        drop(revived);
        assert!(allocator.idle_serializers.contains_key("proxy.unit.poison"));
    }

    #[tokio::test]
    async fn prune_does_not_evict_when_idle_index_missing() {
        let allocator = test_allocator(1);
        let lease = allocator.serializer_for("proxy.unit.no-index").unwrap();
        drop(lease);

        allocator.idle_serializers.remove("proxy.unit.no-index");

        assert!(!allocator.prune_one_idle_serializer());
        assert_eq!(allocator.serializer_count(), 1);
        assert_eq!(allocator.serializer_slots.load(Ordering::Acquire), 1);
    }
}
