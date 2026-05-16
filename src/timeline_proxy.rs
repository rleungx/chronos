use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::MutexGuard as StdMutexGuard;
use std::time::{Duration, Instant};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::sync::{Mutex, MutexGuard};

use crate::plane::RequestCancellation;
use crate::recovery::record_recovery_event;
use crate::{
    metrics, AllocateTimestampsRequest, AllocateTimestampsResponse, TsoDataPlane, TsoError,
};

#[derive(Clone)]
pub struct TimelineScopedAllocator {
    data_plane: TsoDataPlane,
    timeline_serializers: Arc<DashMap<String, Arc<TimelineSerializer>>>,
    idle_serializers: Arc<StdMutex<VecDeque<String>>>,
    serializer_slots: Arc<AtomicUsize>,
    max_serializers: usize,
}

const TIMEOUT_CLEANUP_GRACE_MS: u64 = 10;

struct TimelineSerializer {
    serialize: Mutex<()>,
    active_references: AtomicUsize,
    lifecycle: StdMutex<()>,
}

struct TimelineSerializerLease {
    timeline_key: String,
    serializer: Arc<TimelineSerializer>,
    idle_serializers: Arc<StdMutex<VecDeque<String>>>,
}

impl Drop for TimelineSerializerLease {
    fn drop(&mut self) {
        let previous = self
            .serializer
            .active_references
            .fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
        if previous == 1 {
            lock_idle_serializers(&self.idle_serializers).push_back(self.timeline_key.clone());
        }
    }
}

fn lock_idle_serializers(
    idle_serializers: &StdMutex<VecDeque<String>>,
) -> StdMutexGuard<'_, VecDeque<String>> {
    match idle_serializers.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            record_recovery_event("timeline_proxy", "idle_serializer_queue", "mutex_poisoned");
            poisoned.into_inner()
        }
    }
}

impl TimelineScopedAllocator {
    pub fn new(data_plane: TsoDataPlane) -> Self {
        let max_serializers = data_plane.max_timeline_proxy_lanes();
        Self {
            data_plane,
            timeline_serializers: Arc::new(DashMap::new()),
            idle_serializers: Arc::new(StdMutex::new(VecDeque::new())),
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
        let serializer = serializer_lease.serializer.clone();
        let allocation_result = match serializer.serialize.try_lock() {
            Ok(_guard) => {
                self.data_plane
                    .allocate_timestamps_with_cancellation(request, cancellation)
                    .await
            }
            Err(_) => {
                let wait_started = Instant::now();
                let _guard = Self::lock_serializer_with_cancellation(
                    &serializer_lease.serializer,
                    cancellation.as_ref(),
                )
                .await?;
                metrics::TSO_TIMELINE_PROXY_WAIT.observe(wait_started.elapsed().as_secs_f64());
                self.data_plane
                    .allocate_timestamps_with_cancellation(request, cancellation)
                    .await
            }
        };
        drop(serializer_lease);
        allocation_result
    }

    async fn lock_serializer_with_cancellation<'a>(
        serializer: &'a TimelineSerializer,
        cancellation: Option<&RequestCancellation>,
    ) -> Result<MutexGuard<'a, ()>, TsoError> {
        let Some(cancellation) = cancellation else {
            return Ok(serializer.serialize.lock().await);
        };

        if cancellation.is_cancelled() {
            return Err(TsoError::RequestCancelled);
        }

        tokio::select! {
            guard = serializer.serialize.lock() => Ok(guard),
            _ = cancellation.cancelled() => Err(TsoError::RequestCancelled),
        }
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
        let allocation = self.allocate_timestamps_cancellable(request, Some(cancellation.clone()));
        tokio::pin!(allocation);

        tokio::select! {
            result = &mut allocation => {
                match result {
                    Ok(response) => Ok(response),
                    Err(TsoError::RequestCancelled) => {
                        metrics::TSO_TIMELINE_PROXY_TIMEOUT_TOTAL.inc();
                        Err(TimelineProxyError::TimedOut)
                    }
                    Err(error) => Err(TimelineProxyError::Tso(error)),
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(timeout_ms as u64)) => {
                cancellation.cancel();
                let cleanup_grace = Duration::from_millis(TIMEOUT_CLEANUP_GRACE_MS.min(timeout_ms as u64));
                let _ = tokio::time::timeout(cleanup_grace, &mut allocation).await;
                metrics::TSO_TIMELINE_PROXY_TIMEOUT_TOTAL.inc();
                Err(TimelineProxyError::TimedOut)
            }
        }
    }

    fn lease_existing_serializer(&self, timeline_key: &str) -> Option<TimelineSerializerLease> {
        let serializer = {
            let existing = self.timeline_serializers.get(timeline_key)?;
            existing.clone()
        };
        {
            let _lifecycle = Self::lock_serializer_lifecycle(&serializer);
            if !self.serializer_still_current(timeline_key, &serializer) {
                return None;
            }
            serializer.active_references.fetch_add(1, Ordering::AcqRel);
        }
        Some(TimelineSerializerLease {
            timeline_key: timeline_key.to_owned(),
            serializer,
            idle_serializers: self.idle_serializers.clone(),
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
                    drop(entry);
                    self.serializer_slots.fetch_sub(1, Ordering::AcqRel);
                    if let Some(existing) = self.lease_existing_serializer(timeline_key) {
                        return Ok(existing);
                    }
                }
                Entry::Vacant(entry) => {
                    let serializer = Arc::new(TimelineSerializer {
                        serialize: Mutex::new(()),
                        active_references: AtomicUsize::new(1),
                        lifecycle: StdMutex::new(()),
                    });
                    entry.insert(serializer.clone());
                    metrics::TSO_TIMELINE_PROXY_LANES.set(self.timeline_serializers.len() as i64);
                    return Ok(TimelineSerializerLease {
                        timeline_key: timeline_key.to_owned(),
                        serializer,
                        idle_serializers: self.idle_serializers.clone(),
                    });
                }
            }
        }
    }

    fn prune_one_idle_serializer(&self) -> bool {
        self.try_prune_idle_candidates(self.timeline_serializers.len().max(1))
    }

    fn try_prune_idle_candidates(&self, attempts: usize) -> bool {
        for _ in 0..attempts {
            let Some(timeline_key) = lock_idle_serializers(&self.idle_serializers).pop_front()
            else {
                return false;
            };
            let Some(serializer) = self
                .timeline_serializers
                .get(&timeline_key)
                .map(|entry| entry.value().clone())
            else {
                continue;
            };
            if serializer.active_references.load(Ordering::Acquire) != 0 {
                continue;
            }

            let _lifecycle = Self::lock_serializer_lifecycle(&serializer);
            if serializer.active_references.load(Ordering::Acquire) != 0 {
                continue;
            }

            if let Entry::Occupied(entry) = self.timeline_serializers.entry(timeline_key) {
                if Arc::ptr_eq(entry.get(), &serializer) {
                    entry.remove();
                    self.serializer_slots.fetch_sub(1, Ordering::AcqRel);
                    metrics::TSO_TIMELINE_PROXY_LANES.set(self.timeline_serializers.len() as i64);
                    return true;
                }
            }
        }
        false
    }

    fn serializer_still_current(
        &self,
        timeline_key: &str,
        serializer: &Arc<TimelineSerializer>,
    ) -> bool {
        self.timeline_serializers
            .get(timeline_key)
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current.value(), serializer))
    }

    #[cfg(test)]
    fn active_references_for_test(&self, timeline_key: &str) -> Option<usize> {
        self.timeline_serializers
            .get(timeline_key)
            .map(|serializer| serializer.active_references.load(Ordering::Acquire))
    }

    #[cfg(test)]
    fn contains_serializer_for_test(&self, timeline_key: &str) -> bool {
        self.timeline_serializers.contains_key(timeline_key)
    }

    #[cfg(test)]
    fn serializer_for_test(&self, timeline_key: &str) -> Option<Arc<TimelineSerializer>> {
        self.timeline_serializers
            .get(timeline_key)
            .map(|serializer| serializer.clone())
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
    use crate::{ManualClock, TsoConfig, TsoService};

    use super::*;

    fn required_test_config(config: TsoConfig) -> TsoConfig {
        crate::test_tls::required_grpc_tls_test_config(config, 100)
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
    async fn lease_drop_releases_active_reference_without_idle_index_writes() {
        let allocator = test_allocator(2);
        let first = allocator
            .serializer_for("proxy.unit.active-reference")
            .unwrap();
        assert_eq!(
            allocator.active_references_for_test("proxy.unit.active-reference"),
            Some(1)
        );

        let second = allocator
            .serializer_for("proxy.unit.active-reference")
            .unwrap();
        assert_eq!(
            allocator.active_references_for_test("proxy.unit.active-reference"),
            Some(2)
        );
        drop(first);
        assert_eq!(
            allocator.active_references_for_test("proxy.unit.active-reference"),
            Some(1)
        );

        drop(second);
        assert_eq!(
            allocator.active_references_for_test("proxy.unit.active-reference"),
            Some(0)
        );
        assert!(allocator.contains_serializer_for_test("proxy.unit.active-reference"));
    }

    #[tokio::test]
    async fn reacquiring_idle_serializer_reuses_cached_lane() {
        let allocator = test_allocator(1);
        let first = allocator.serializer_for("proxy.unit.reuse").unwrap();
        let initial_serializer = allocator
            .serializer_for_test("proxy.unit.reuse")
            .expect("serializer should be cached");
        drop(first);
        assert_eq!(
            allocator.active_references_for_test("proxy.unit.reuse"),
            Some(0)
        );

        let second = allocator.serializer_for("proxy.unit.reuse").unwrap();
        let reused_serializer = allocator
            .serializer_for_test("proxy.unit.reuse")
            .expect("serializer should remain cached");
        assert!(Arc::ptr_eq(&initial_serializer, &reused_serializer));
        assert_eq!(
            allocator.active_references_for_test("proxy.unit.reuse"),
            Some(1)
        );

        drop(second);
        assert_eq!(
            allocator.active_references_for_test("proxy.unit.reuse"),
            Some(0)
        );
    }

    #[tokio::test]
    async fn prune_does_not_evict_active_lane() {
        let allocator = test_allocator(1);
        let lease = allocator.serializer_for("proxy.unit.active").unwrap();

        assert!(!allocator.prune_one_idle_serializer());
        assert_eq!(allocator.serializer_count(), 1);
        assert_eq!(allocator.serializer_slots.load(Ordering::Acquire), 1);

        drop(lease);
        assert_eq!(
            allocator.active_references_for_test("proxy.unit.active"),
            Some(0)
        );
    }

    #[tokio::test]
    async fn prune_evicts_idle_lane() {
        let allocator = test_allocator(1);
        let lease = allocator.serializer_for("proxy.unit.prune").unwrap();
        drop(lease);

        assert!(allocator.prune_one_idle_serializer());
        assert_eq!(allocator.serializer_count(), 0);
        assert_eq!(allocator.serializer_slots.load(Ordering::Acquire), 0);
        assert!(!allocator.contains_serializer_for_test("proxy.unit.prune"));
    }

    #[tokio::test]
    async fn lease_existing_rejects_serializer_removed_before_reference_increment() {
        let allocator = test_allocator(1);
        let initial = allocator
            .serializer_for("proxy.unit.stale-reference")
            .unwrap();
        drop(initial);

        let serializer = allocator
            .serializer_for_test("proxy.unit.stale-reference")
            .expect("serializer should remain cached");
        allocator
            .timeline_serializers
            .remove("proxy.unit.stale-reference");
        allocator.serializer_slots.fetch_sub(1, Ordering::AcqRel);

        assert!(allocator
            .lease_existing_serializer("proxy.unit.stale-reference")
            .is_none());
        assert_eq!(serializer.active_references.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn cancelled_serializer_waiter_returns_without_lane_release() {
        let allocator = test_allocator(1);
        let timeline_key = "proxy.unit.cancelled-wait";
        let lease = allocator.serializer_for(timeline_key).unwrap();
        let busy_guard = lease.serializer.serialize.lock().await;
        let cancellation = RequestCancellation::new();

        let waiter = {
            let allocator = allocator.clone();
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                allocator
                    .allocate_timestamps_cancellable(
                        AllocateTimestampsRequest {
                            timeline_key: timeline_key.to_owned(),
                            count: 1,
                            expected_epoch: 1,
                            expected_route_version: 1,
                            client_request_id: "cancelled-serializer-waiter".to_string(),
                        },
                        Some(cancellation),
                    )
                    .await
            })
        };

        for _ in 0..20 {
            if allocator.active_references_for_test(timeline_key) == Some(2) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(allocator.active_references_for_test(timeline_key), Some(2));

        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .expect("cancelled waiter should not remain parked on serializer lock")
            .expect("waiter task should complete");
        assert!(matches!(result, Err(TsoError::RequestCancelled)));
        assert_eq!(allocator.active_references_for_test(timeline_key), Some(1));

        drop(busy_guard);
        drop(lease);
        assert_eq!(allocator.active_references_for_test(timeline_key), Some(0));
    }

    #[tokio::test]
    async fn poisoned_lifecycle_lock_recovers_and_records_metric() {
        let allocator = test_allocator(1);
        let initial = allocator.serializer_for("proxy.unit.poison").unwrap();
        drop(initial);

        let serializer = allocator
            .serializer_for_test("proxy.unit.poison")
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

        assert_eq!(
            allocator.active_references_for_test("proxy.unit.poison"),
            Some(1)
        );
        drop(revived);
        assert_eq!(
            allocator.active_references_for_test("proxy.unit.poison"),
            Some(0)
        );
    }
}
