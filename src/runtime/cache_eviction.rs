use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::sync::{Mutex as StdMutex, MutexGuard as StdMutexGuard};

use dashmap::DashMap;
use tokio::sync::Mutex;

use crate::recovery::record_recovery_event;
use crate::{metrics, TsoError};

use super::TimelineState;

const TIMELINE_RUNTIME_SHARD_AMOUNT: usize = 64;
const EVICTION_CANDIDATE_BATCH_LIMIT: usize = 128;

pub(super) struct TimelineCacheStore {
    timelines: DashMap<String, TimelineCacheEntry>,
    max_entries: usize,
    access_counter: AtomicU64,
    eviction_generation_counter: AtomicU64,
    entry_count: AtomicUsize,
    capacity_lock: StdMutex<()>,
    eviction_generations: StdMutex<HashMap<String, u64>>,
    eviction_candidates: StdMutex<BinaryHeap<Reverse<(u64, u64, String)>>>,
}

struct TimelineCacheEntry {
    timeline: Arc<Mutex<TimelineState>>,
    last_access_tick: AtomicU64,
}

impl TimelineCacheStore {
    pub(super) fn new(max_entries: usize) -> Self {
        Self {
            timelines: DashMap::with_shard_amount(TIMELINE_RUNTIME_SHARD_AMOUNT),
            max_entries,
            access_counter: AtomicU64::new(1),
            eviction_generation_counter: AtomicU64::new(1),
            entry_count: AtomicUsize::new(0),
            capacity_lock: StdMutex::new(()),
            eviction_generations: StdMutex::new(HashMap::new()),
            eviction_candidates: StdMutex::new(BinaryHeap::new()),
        }
    }

    pub(super) fn timeline_count(&self) -> usize {
        self.entry_count.load(AtomicOrdering::Acquire)
    }

    pub(super) fn timeline_handle(&self, timeline_key: &str) -> Option<Arc<Mutex<TimelineState>>> {
        match self.timelines.get(timeline_key) {
            Some(entry) => {
                let access_tick = self.next_access_tick();
                metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                    .with_label_values(&["hit"])
                    .inc();
                entry
                    .last_access_tick
                    .store(access_tick, AtomicOrdering::Release);
                Some(entry.timeline.clone())
            }
            None => {
                metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                    .with_label_values(&["miss"])
                    .inc();
                None
            }
        }
    }

    pub(super) fn timeline_handle_or_insert_with<F>(
        &self,
        timeline_key: &str,
        build: F,
    ) -> Result<Arc<Mutex<TimelineState>>, TsoError>
    where
        F: FnOnce() -> Arc<Mutex<TimelineState>>,
    {
        if let Some(existing) = self.timelines.get(timeline_key) {
            let access_tick = self.next_access_tick();
            metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                .with_label_values(&["hit"])
                .inc();
            existing
                .last_access_tick
                .store(access_tick, AtomicOrdering::Release);
            return Ok(existing.timeline.clone());
        }

        metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
            .with_label_values(&["miss"])
            .inc();

        let _capacity = self.capacity_lock();
        if let Some(existing) = self.timelines.get(timeline_key) {
            let access_tick = self.next_access_tick();
            existing
                .last_access_tick
                .store(access_tick, AtomicOrdering::Release);
            return Ok(existing.timeline.clone());
        }

        if self.timeline_count() >= self.max_entries && !self.evict_one_idle_entry() {
            metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                .with_label_values(&["saturated"])
                .inc();
            return Err(TsoError::TimelineRuntimeCacheSaturated {
                timeline_key: timeline_key.to_owned(),
                max_entries: self.max_entries,
            });
        }
        let timeline = build();
        let access_tick = self.next_access_tick();

        match self.timelines.entry(timeline_key.to_owned()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                entry
                    .get()
                    .last_access_tick
                    .store(access_tick, AtomicOrdering::Release);
                Ok(entry.get().timeline.clone())
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(TimelineCacheEntry {
                    timeline: timeline.clone(),
                    last_access_tick: AtomicU64::new(access_tick),
                });
                self.entry_count.fetch_add(1, AtomicOrdering::AcqRel);
                self.set_entry_count_metric();
                self.record_eviction_candidate(timeline_key, access_tick);
                Ok(timeline)
            }
        }
    }

    pub(super) fn insert_timeline(
        &self,
        timeline_key: String,
        timeline: Arc<Mutex<TimelineState>>,
    ) -> Result<(), TsoError> {
        if let Some(mut existing) = self.timelines.get_mut(&timeline_key) {
            let access_tick = self.next_access_tick();
            metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                .with_label_values(&["hit"])
                .inc();
            existing.timeline = timeline;
            existing
                .last_access_tick
                .store(access_tick, AtomicOrdering::Release);
            return Ok(());
        }

        let _capacity = self.capacity_lock();
        if let Some(mut existing) = self.timelines.get_mut(&timeline_key) {
            let access_tick = self.next_access_tick();
            existing.timeline = timeline;
            existing
                .last_access_tick
                .store(access_tick, AtomicOrdering::Release);
            return Ok(());
        }

        if self.timeline_count() >= self.max_entries && !self.evict_one_idle_entry() {
            metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                .with_label_values(&["saturated"])
                .inc();
            return Err(TsoError::TimelineRuntimeCacheSaturated {
                timeline_key,
                max_entries: self.max_entries,
            });
        }

        match self.timelines.entry(timeline_key) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let access_tick = self.next_access_tick();
                entry.get_mut().timeline = timeline;
                entry
                    .get()
                    .last_access_tick
                    .store(access_tick, AtomicOrdering::Release);
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                    .with_label_values(&["miss"])
                    .inc();
                let access_tick = self.next_access_tick();
                let timeline_key = entry.key().clone();
                entry.insert(TimelineCacheEntry {
                    timeline,
                    last_access_tick: AtomicU64::new(access_tick),
                });
                self.entry_count.fetch_add(1, AtomicOrdering::AcqRel);
                self.set_entry_count_metric();
                self.record_eviction_candidate(&timeline_key, access_tick);
            }
        }
        Ok(())
    }

    pub(super) fn remove_timeline(&self, timeline_key: &str) {
        let _capacity = self.capacity_lock();
        if self.timelines.remove(timeline_key).is_some() {
            self.remove_eviction_generation(timeline_key);
            self.maybe_compact_eviction_candidates();
            self.entry_count.fetch_sub(1, AtomicOrdering::AcqRel);
            self.set_entry_count_metric();
        }
    }

    pub(super) fn clear(&self) {
        let _capacity = self.capacity_lock();
        self.timelines.clear();
        self.eviction_generations_lock().clear();
        self.eviction_candidates_lock().clear();
        self.entry_count.store(0, AtomicOrdering::Release);
        self.set_entry_count_metric();
    }

    fn next_access_tick(&self) -> u64 {
        self.access_counter.fetch_add(1, AtomicOrdering::Relaxed)
    }

    fn set_entry_count_metric(&self) {
        metrics::TSO_TIMELINE_RUNTIME_CACHE_ENTRIES.set(self.timeline_count() as i64);
    }

    fn record_eviction_candidate(&self, timeline_key: &str, access_tick: u64) {
        let generation = self.ensure_eviction_generation(timeline_key);
        self.eviction_candidates_lock().push(Reverse((
            access_tick,
            generation,
            timeline_key.to_owned(),
        )));
    }

    fn ensure_eviction_generation(&self, timeline_key: &str) -> u64 {
        let mut generations = self.eviction_generations_lock();
        *generations
            .entry(timeline_key.to_owned())
            .or_insert_with(|| {
                self.eviction_generation_counter
                    .fetch_add(1, AtomicOrdering::Relaxed)
            })
    }

    fn remove_eviction_generation(&self, timeline_key: &str) {
        self.eviction_generations_lock().remove(timeline_key);
    }

    fn maybe_compact_eviction_candidates(&self) {
        let heap_len = self.eviction_candidates_lock().len();
        let live_entries = self.timeline_count();
        if live_entries == 0 {
            if heap_len == 0 {
                return;
            }
        } else {
            let threshold = live_entries.saturating_mul(4).saturating_add(8).max(16);
            if heap_len <= threshold {
                return;
            }
        }

        let generations = self.eviction_generations_lock();
        let mut heap = self.eviction_candidates_lock();
        let retained: BinaryHeap<_> = heap
            .drain()
            .filter(|Reverse((_, generation, candidate_key))| {
                generations
                    .get(candidate_key)
                    .is_some_and(|current_generation| *current_generation == *generation)
            })
            .collect();
        *heap = retained;
    }

    fn evict_one_idle_entry(&self) -> bool {
        let mut deferred = Vec::new();
        if self.try_evict_idle_entry_batch(EVICTION_CANDIDATE_BATCH_LIMIT, &mut deferred) {
            return true;
        }

        self.maybe_compact_eviction_candidates();
        if self.try_evict_idle_entry_batch(EVICTION_CANDIDATE_BATCH_LIMIT, &mut deferred) {
            self.eviction_candidates_lock().extend(deferred.drain(..));
            return true;
        }

        self.eviction_candidates_lock().extend(deferred.drain(..));
        let mut retried_deferred = Vec::new();
        let evicted =
            self.try_evict_idle_entry_batch(EVICTION_CANDIDATE_BATCH_LIMIT, &mut retried_deferred);
        self.eviction_candidates_lock()
            .extend(retried_deferred.drain(..));
        evicted
    }

    fn try_evict_idle_entry_batch(
        &self,
        attempts: usize,
        deferred: &mut Vec<Reverse<(u64, u64, String)>>,
    ) -> bool {
        let initial_len = self.eviction_candidates_lock().len().min(attempts);
        for _ in 0..initial_len {
            let Some(Reverse((tick, generation, key))) = self.eviction_candidates_lock().pop()
            else {
                break;
            };
            let current_generation = self.eviction_generations_lock().get(&key).copied();
            if current_generation != Some(generation) {
                continue;
            }
            let Some(entry) = self.timelines.get(&key) else {
                continue;
            };
            let current_tick = entry.last_access_tick.load(AtomicOrdering::Acquire);
            let is_idle = Arc::strong_count(&entry.timeline) == 1;
            drop(entry);

            if current_tick != tick || !is_idle {
                deferred.push(Reverse((current_tick, generation, key)));
                continue;
            }

            let removed = self.timelines.remove(&key).is_some();
            self.remove_eviction_generation(&key);
            if removed {
                self.eviction_candidates_lock().extend(deferred.drain(..));
                self.entry_count.fetch_sub(1, AtomicOrdering::AcqRel);
                metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                    .with_label_values(&["evicted_idle"])
                    .inc();
                self.set_entry_count_metric();
                return true;
            }
        }
        false
    }

    fn capacity_lock(&self) -> StdMutexGuard<'_, ()> {
        match self.capacity_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event("runtime", "timeline_capacity_lock", "mutex_poisoned");
                poisoned.into_inner()
            }
        }
    }

    fn eviction_generations_lock(&self) -> StdMutexGuard<'_, HashMap<String, u64>> {
        match self.eviction_generations.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event("runtime", "timeline_eviction_generations", "mutex_poisoned");
                poisoned.into_inner()
            }
        }
    }

    fn eviction_candidates_lock(
        &self,
    ) -> StdMutexGuard<'_, BinaryHeap<Reverse<(u64, u64, String)>>> {
        match self.eviction_candidates.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event("runtime", "timeline_eviction_candidates", "mutex_poisoned");
                poisoned.into_inner()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{metrics, ResourceTier, TimelineLifecycleState, TimelineRoute};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc as StdArc, Barrier};

    fn test_timeline(key: &str) -> Arc<Mutex<TimelineState>> {
        Arc::new(Mutex::new(TimelineState {
            route: TimelineRoute {
                timeline_key: key.to_owned(),
                generator_id: 1,
                epoch: 1,
                route_version: 1,
                resource_tier: ResourceTier::Shared,
                owner_worker_endpoint: "worker-a".to_string(),
            },
            state: TimelineLifecycleState::Active,
            last_issued_tso: None,
            last_graceful_issued: None,
            revision: 1,
        }))
    }

    #[test]
    fn runtime_cache_rejects_when_full_and_only_busy_entry_exists() {
        let runtime = TimelineCacheStore::new(1);
        let first = test_timeline("runtime-saturated.a");
        runtime
            .insert_timeline("runtime-saturated.a".to_string(), first.clone())
            .unwrap();

        let before = metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
            .with_label_values(&["saturated"])
            .get();
        let result = runtime.insert_timeline(
            "runtime-saturated.b".to_string(),
            test_timeline("runtime-saturated.b"),
        );
        match result {
            Err(TsoError::TimelineRuntimeCacheSaturated {
                timeline_key,
                max_entries,
            }) => {
                assert_eq!(timeline_key, "runtime-saturated.b");
                assert_eq!(max_entries, 1);
            }
            other => panic!("unexpected runtime cache result: {:?}", other),
        }

        assert_eq!(runtime.timeline_count(), 1);
        assert!(
            metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                .with_label_values(&["saturated"])
                .get()
                > before
        );
        drop(first);
    }

    #[test]
    fn runtime_cache_records_evicted_idle_metric() {
        let runtime = TimelineCacheStore::new(1);
        runtime
            .insert_timeline(
                "runtime-evict.a".to_string(),
                test_timeline("runtime-evict.a"),
            )
            .unwrap();

        let before = metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
            .with_label_values(&["evicted_idle"])
            .get();
        runtime
            .insert_timeline(
                "runtime-evict.b".to_string(),
                test_timeline("runtime-evict.b"),
            )
            .unwrap();

        assert_eq!(runtime.timeline_count(), 1);
        assert!(
            metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                .with_label_values(&["evicted_idle"])
                .get()
                > before
        );
    }

    #[test]
    fn runtime_cache_retries_eviction_after_requeueing_updated_access_tick() {
        let runtime = TimelineCacheStore::new(1);
        runtime
            .insert_timeline(
                "runtime-requeue-updated.a".to_string(),
                test_timeline("runtime-requeue-updated.a"),
            )
            .unwrap();

        let handle = runtime
            .timeline_handle("runtime-requeue-updated.a")
            .expect("existing timeline should be present");
        drop(handle);

        runtime
            .insert_timeline(
                "runtime-requeue-updated.b".to_string(),
                test_timeline("runtime-requeue-updated.b"),
            )
            .unwrap();

        assert!(runtime
            .timeline_handle("runtime-requeue-updated.a")
            .is_none());
        assert!(runtime
            .timeline_handle("runtime-requeue-updated.b")
            .is_some());
        assert_eq!(runtime.timeline_count(), 1);
    }

    #[test]
    fn runtime_cache_does_not_build_when_saturated() {
        let runtime = TimelineCacheStore::new(1);
        let first = test_timeline("runtime-no-build.a");
        runtime
            .insert_timeline("runtime-no-build.a".to_string(), first.clone())
            .unwrap();

        let built = AtomicBool::new(false);
        let result = runtime.timeline_handle_or_insert_with("runtime-no-build.b", || {
            built.store(true, Ordering::Release);
            test_timeline("runtime-no-build.b")
        });

        assert!(matches!(
            result,
            Err(TsoError::TimelineRuntimeCacheSaturated {
                timeline_key,
                max_entries: 1,
            }) if timeline_key == "runtime-no-build.b"
        ));
        assert!(!built.load(Ordering::Acquire));
        drop(first);
    }

    #[test]
    fn runtime_cache_evicts_oldest_idle_candidate_without_full_scan() {
        let runtime = TimelineCacheStore::new(2);
        runtime
            .insert_timeline(
                "runtime-oldest.a".to_string(),
                test_timeline("runtime-oldest.a"),
            )
            .unwrap();
        runtime
            .insert_timeline(
                "runtime-oldest.b".to_string(),
                test_timeline("runtime-oldest.b"),
            )
            .unwrap();

        let handle_b = runtime.timeline_handle("runtime-oldest.b").unwrap();
        drop(handle_b);

        runtime
            .insert_timeline(
                "runtime-oldest.c".to_string(),
                test_timeline("runtime-oldest.c"),
            )
            .unwrap();

        assert!(runtime.timeline_handle("runtime-oldest.a").is_none());
        assert!(runtime.timeline_handle("runtime-oldest.b").is_some());
        assert!(runtime.timeline_handle("runtime-oldest.c").is_some());
        assert_eq!(runtime.timeline_count(), 2);
    }

    #[test]
    fn runtime_cache_capacity_is_strict_under_racing_inserts() {
        let runtime = StdArc::new(TimelineCacheStore::new(1));
        let barrier = StdArc::new(Barrier::new(8));
        let timelines: Vec<_> = (0..8)
            .map(|index| {
                let key = format!("runtime-race-{index}");
                (key.clone(), test_timeline(&key))
            })
            .collect();

        let handles: Vec<_> = timelines
            .iter()
            .map(|(key, timeline)| {
                let runtime = runtime.clone();
                let barrier = barrier.clone();
                let key = key.clone();
                let timeline = timeline.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    runtime.insert_timeline(key, timeline).is_ok()
                })
            })
            .collect();

        let successes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|success| *success)
            .count();

        assert_eq!(successes, 1);
        assert_eq!(runtime.timeline_count(), 1);
    }

    #[test]
    fn runtime_cache_requeues_busy_candidate_until_it_becomes_idle() {
        let runtime = TimelineCacheStore::new(1);
        let first = test_timeline("runtime-requeue.a");
        runtime
            .insert_timeline("runtime-requeue.a".to_string(), first.clone())
            .unwrap();

        let result = runtime.insert_timeline(
            "runtime-requeue.b".to_string(),
            test_timeline("runtime-requeue.b"),
        );
        assert!(matches!(
            result,
            Err(TsoError::TimelineRuntimeCacheSaturated {
                timeline_key,
                max_entries: 1,
            }) if timeline_key == "runtime-requeue.b"
        ));

        drop(first);

        runtime
            .insert_timeline(
                "runtime-requeue.b".to_string(),
                test_timeline("runtime-requeue.b"),
            )
            .unwrap();

        assert!(runtime.timeline_handle("runtime-requeue.a").is_none());
        assert!(runtime.timeline_handle("runtime-requeue.b").is_some());
    }

    #[test]
    fn runtime_cache_hits_do_not_unboundedly_grow_eviction_candidates() {
        let runtime = TimelineCacheStore::new(1);
        runtime
            .insert_timeline(
                "runtime-heap.a".to_string(),
                test_timeline("runtime-heap.a"),
            )
            .unwrap();

        for _ in 0..32 {
            let handle = runtime.timeline_handle("runtime-heap.a").unwrap();
            drop(handle);
        }

        assert_eq!(runtime.eviction_candidates_lock().len(), 1);
    }

    #[test]
    fn runtime_cache_remove_and_reinsert_does_not_accumulate_stale_candidates() {
        let runtime = TimelineCacheStore::new(1);

        for _ in 0..24 {
            runtime
                .insert_timeline(
                    "runtime-churn.a".to_string(),
                    test_timeline("runtime-churn.a"),
                )
                .unwrap();
            runtime.remove_timeline("runtime-churn.a");
        }

        assert!(runtime.eviction_candidates_lock().len() <= 16);
        runtime.maybe_compact_eviction_candidates();
        assert_eq!(runtime.eviction_candidates_lock().len(), 0);
    }

    #[test]
    fn runtime_cache_evicts_idle_candidate_beyond_busy_batch_window() {
        let runtime = TimelineCacheStore::new(33);
        let mut busy = Vec::new();

        for index in 0..32 {
            let key = format!("runtime-busy-{index}");
            let timeline = test_timeline(&key);
            runtime
                .insert_timeline(key.clone(), timeline.clone())
                .unwrap();
            busy.push(timeline);
        }

        runtime
            .insert_timeline(
                "runtime-idle-tail".to_string(),
                test_timeline("runtime-idle-tail"),
            )
            .unwrap();

        runtime
            .insert_timeline(
                "runtime-new-tail".to_string(),
                test_timeline("runtime-new-tail"),
            )
            .unwrap();

        assert!(runtime.timeline_handle("runtime-idle-tail").is_none());
        assert!(runtime.timeline_handle("runtime-new-tail").is_some());
        assert_eq!(runtime.timeline_count(), 33);

        drop(busy);
    }

    #[test]
    fn runtime_cache_evicts_idle_candidate_beyond_sixty_four_busy_candidates() {
        let runtime = TimelineCacheStore::new(97);
        let mut busy = Vec::new();

        for index in 0..96 {
            let key = format!("runtime-busy-wide-{index}");
            let timeline = test_timeline(&key);
            runtime
                .insert_timeline(key.clone(), timeline.clone())
                .unwrap();
            busy.push(timeline);
        }

        runtime
            .insert_timeline(
                "runtime-idle-wide-tail".to_string(),
                test_timeline("runtime-idle-wide-tail"),
            )
            .unwrap();

        runtime
            .insert_timeline(
                "runtime-new-wide-tail".to_string(),
                test_timeline("runtime-new-wide-tail"),
            )
            .unwrap();

        assert!(runtime.timeline_handle("runtime-idle-wide-tail").is_none());
        assert!(runtime.timeline_handle("runtime-new-wide-tail").is_some());
        assert_eq!(runtime.timeline_count(), 97);

        drop(busy);
    }

    #[test]
    fn runtime_cache_reinsert_skips_stale_generation_candidates() {
        let runtime = TimelineCacheStore::new(1);
        runtime
            .insert_timeline(
                "runtime-generation.a".to_string(),
                test_timeline("runtime-generation.a"),
            )
            .unwrap();
        runtime.remove_timeline("runtime-generation.a");
        runtime
            .insert_timeline(
                "runtime-generation.a".to_string(),
                test_timeline("runtime-generation.a"),
            )
            .unwrap();

        let result = runtime.insert_timeline(
            "runtime-generation.b".to_string(),
            test_timeline("runtime-generation.b"),
        );

        assert!(result.is_ok());
        assert!(runtime.timeline_handle("runtime-generation.a").is_none());
        assert!(runtime.timeline_handle("runtime-generation.b").is_some());
    }
}
