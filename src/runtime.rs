use std::cmp::{max, min};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{broadcast, Mutex};

use crate::{
    encode_tso, metrics, next_cursor_after, Cursor, TimelineLifecycleState, TimelineRoute,
    TimestampRange, TsoError, SEQUENCE_CAPACITY,
};

#[derive(Debug)]
pub(crate) struct Generator {
    id: u32,
    next_index: AtomicU64,
}

impl Generator {
    pub(crate) fn new(id: u32) -> Self {
        Self {
            id,
            next_index: AtomicU64::new(0),
        }
    }

    pub(crate) fn init_after_floor(&self, floor: u64) -> Result<(), TsoError> {
        let cap = SEQUENCE_CAPACITY as u64;
        let cursor = next_cursor_after(floor, self.id)?;
        let index = cursor
            .physical_ms
            .checked_mul(cap)
            .and_then(|value| value.checked_add(cursor.sequence as u64))
            .ok_or(TsoError::TsoOverflow)?;
        self.next_index.fetch_max(index, AtomicOrdering::AcqRel);
        Ok(())
    }

    pub(crate) fn current_last_issued_tso(&self) -> Result<Option<u64>, TsoError> {
        let cap = SEQUENCE_CAPACITY as u64;
        let next = self.next_index.load(AtomicOrdering::Acquire);
        if next == 0 {
            return Ok(None);
        }
        let last_index = next - 1;
        let physical_ms = last_index / cap;
        let sequence = (last_index % cap) as u32;
        Ok(Some(encode_tso(physical_ms, self.id, sequence)?))
    }

    pub(crate) fn allocate_after(
        &self,
        count: u32,
        floor: Option<u64>,
        now_ms: u64,
        max_future_borrow_ms: u64,
        max_clock_rewind_ms: u64,
        issued_upper_bound: Option<u64>,
    ) -> Result<Vec<TimestampRange>, TsoError> {
        let cap = SEQUENCE_CAPACITY as u64;
        let floor_cursor = match floor {
            Some(value) => next_cursor_after(value, self.id)?,
            None => Cursor {
                physical_ms: 0,
                sequence: 0,
            },
        };
        let floor_index = floor_cursor
            .physical_ms
            .checked_mul(cap)
            .and_then(|value| value.checked_add(floor_cursor.sequence as u64))
            .unwrap_or(u64::MAX);

        loop {
            let old_index = self.next_index.load(AtomicOrdering::Acquire);
            let current_physical_ms = old_index / cap;
            let mut effective_now_ms = now_ms;
            if now_ms < current_physical_ms {
                let delta = current_physical_ms - now_ms;
                if delta > max_clock_rewind_ms {
                    metrics::TSO_CLOCK_BACKWARDS_TOTAL.inc();
                    return Err(TsoError::ClockBackwards { delta_ms: delta });
                }
                effective_now_ms = current_physical_ms;
            }

            let allowed_physical_ms = max(
                current_physical_ms,
                now_ms.saturating_add(max_future_borrow_ms),
            );

            let base_index = max(old_index, effective_now_ms.saturating_mul(cap));
            let start_index = max(base_index, floor_index);
            let start_physical_ms = start_index / cap;
            if start_physical_ms > allowed_physical_ms {
                return Err(TsoError::FutureBorrowExceeded {
                    requested_physical_ms: start_physical_ms,
                    allowed_physical_ms,
                });
            }

            let last_index = start_index
                .checked_add(count as u64)
                .and_then(|value| value.checked_sub(1))
                .ok_or(TsoError::TsoOverflow)?;
            let last_physical_ms = last_index / cap;
            let last_sequence = (last_index % cap) as u32;
            if last_physical_ms > allowed_physical_ms {
                return Err(TsoError::FutureBorrowExceeded {
                    requested_physical_ms: last_physical_ms,
                    allowed_physical_ms,
                });
            }
            if let Some(issued_upper_bound) = issued_upper_bound {
                let requested_end_tso = encode_tso(last_physical_ms, self.id, last_sequence)?;
                if requested_end_tso > issued_upper_bound {
                    return Err(TsoError::IssuedUpperBoundExceeded {
                        requested_end_tso,
                        issued_upper_bound,
                    });
                }
            }

            let new_index = last_index.checked_add(1).ok_or(TsoError::TsoOverflow)?;
            match self.next_index.compare_exchange(
                old_index,
                new_index,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => {
                    let mut remaining = count as u64;
                    let mut cursor_index = start_index;
                    let mut ranges = Vec::new();
                    while remaining > 0 {
                        let physical_ms = cursor_index / cap;
                        let sequence = (cursor_index % cap) as u32;
                        let available = cap - (cursor_index % cap);
                        let take = min(remaining, available);
                        let start_tso = encode_tso(physical_ms, self.id, sequence)?;
                        let end_sequence = sequence + (take as u32) - 1;
                        let end_tso = encode_tso(physical_ms, self.id, end_sequence)?;
                        ranges.push(TimestampRange { start_tso, end_tso });
                        remaining -= take;
                        cursor_index = cursor_index
                            .checked_add(take)
                            .ok_or(TsoError::TsoOverflow)?;
                    }

                    metrics::TSO_ALLOCATE_COUNT_TOTAL.inc_by(count as u64);
                    metrics::TSO_SEQUENCE_UTILIZATION.set((new_index % cap) as i64);
                    return Ok(ranges);
                }
                Err(_) => continue,
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TimelineState {
    pub(crate) route: TimelineRoute,
    pub(crate) state: TimelineLifecycleState,
    pub(crate) last_issued_tso: Option<u64>,
    pub(crate) last_graceful_issued: Option<u64>,
    pub(crate) revision: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct GeneratorLeaseState {
    pub(crate) revision: u64,
    pub(crate) owner_instance_id: String,
    pub(crate) generator_lease_token: u64,
    pub(crate) lease_expire_at_ms: u64,
    pub(crate) last_persisted_tso: Option<u64>,
    pub(crate) issued_upper_bound: Option<u64>,
}

pub(crate) struct GeneratorRuntimeState {
    generators: Vec<Arc<Generator>>,
    leases: DashMap<u32, GeneratorLeaseState>,
    dedicated_claims: DashMap<u32, String>,
}

impl GeneratorRuntimeState {
    pub(crate) fn new(max_generators: u32) -> Self {
        let mut generators = Vec::with_capacity(max_generators as usize);
        for id in 0..max_generators {
            generators.push(Arc::new(Generator::new(id)));
        }
        Self {
            generators,
            leases: DashMap::new(),
            dedicated_claims: DashMap::new(),
        }
    }

    pub(crate) fn generator_count(&self) -> u32 {
        self.generators.len() as u32
    }

    pub(crate) fn generator(&self, generator_id: u32) -> Arc<Generator> {
        self.generators[generator_id as usize].clone()
    }

    pub(crate) fn lease_keys(&self) -> Vec<u32> {
        self.leases.iter().map(|entry| *entry.key()).collect()
    }

    pub(crate) fn lease_state(&self, generator_id: u32) -> Option<GeneratorLeaseState> {
        self.leases.get(&generator_id).map(|entry| entry.clone())
    }

    pub(crate) fn lease_valid_for_owner(
        &self,
        generator_id: u32,
        owner_instance_id: &str,
        now_ms: u64,
    ) -> bool {
        self.leases
            .get(&generator_id)
            .map(|lease| Self::lease_matches_owner_and_time(&lease, owner_instance_id, now_ms))
            .unwrap_or(false)
    }

    pub(crate) fn valid_lease_upper_bound_for_owner(
        &self,
        generator_id: u32,
        owner_instance_id: &str,
        now_ms: u64,
    ) -> Option<u64> {
        self.leases.get(&generator_id).and_then(|lease| {
            Self::lease_matches_owner_and_time(&lease, owner_instance_id, now_ms)
                .then_some(lease.issued_upper_bound)
                .flatten()
        })
    }

    fn lease_matches_owner_and_time(
        lease: &GeneratorLeaseState,
        owner_instance_id: &str,
        now_ms: u64,
    ) -> bool {
        lease.lease_expire_at_ms > now_ms && lease.owner_instance_id.as_str() == owner_instance_id
    }

    pub(crate) fn upsert_lease(&self, generator_id: u32, state: GeneratorLeaseState) {
        self.leases.insert(generator_id, state);
    }

    pub(crate) fn remove_lease(&self, generator_id: u32) {
        self.leases.remove(&generator_id);
    }

    pub(crate) fn claimed_generator_for_timeline(&self, timeline_key: &str) -> Option<u32> {
        self.dedicated_claims
            .iter()
            .find_map(|entry| (entry.value().as_str() == timeline_key).then_some(*entry.key()))
    }

    pub(crate) fn claim_dedicated(
        &self,
        generator_id: u32,
        timeline_key: &str,
    ) -> Result<bool, TsoError> {
        match self.dedicated_claims.entry(generator_id) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                if entry.get().as_str() != timeline_key {
                    return Err(TsoError::GeneratorPoolExhausted);
                }
                Ok(false)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(timeline_key.to_owned());
                Ok(true)
            }
        }
    }

    pub(crate) fn release_dedicated(&self, generator_id: u32, timeline_key: &str) {
        if let Some(entry) = self.dedicated_claims.get(&generator_id) {
            if entry.as_str() == timeline_key {
                drop(entry);
                self.dedicated_claims.remove(&generator_id);
            }
        }
    }
}

pub(crate) struct TimelineRuntimeState {
    timelines: DashMap<String, TimelineCacheEntry>,
    route_notifier: broadcast::Sender<TimelineRoute>,
    max_entries: usize,
    access_counter: AtomicU64,
}

struct TimelineCacheEntry {
    timeline: Arc<Mutex<TimelineState>>,
    last_access_tick: AtomicU64,
}

impl TimelineRuntimeState {
    pub(crate) fn new(max_entries: usize) -> Self {
        let (route_notifier, _) = broadcast::channel(1024);
        Self {
            timelines: DashMap::new(),
            route_notifier,
            max_entries,
            access_counter: AtomicU64::new(1),
        }
    }

    pub(crate) fn timeline_count(&self) -> usize {
        self.timelines.len()
    }

    pub(crate) fn timeline_handle(&self, timeline_key: &str) -> Option<Arc<Mutex<TimelineState>>> {
        match self.timelines.get(timeline_key) {
            Some(entry) => {
                metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                    .with_label_values(&["hit"])
                    .inc();
                entry
                    .last_access_tick
                    .store(self.next_access_tick(), AtomicOrdering::Release);
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

    pub(crate) fn timeline_handle_or_insert_with<F>(
        &self,
        timeline_key: &str,
        build: F,
    ) -> Result<Arc<Mutex<TimelineState>>, TsoError>
    where
        F: FnOnce() -> Arc<Mutex<TimelineState>>,
    {
        if let Some(existing) = self.timelines.get(timeline_key) {
            metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                .with_label_values(&["hit"])
                .inc();
            existing
                .last_access_tick
                .store(self.next_access_tick(), AtomicOrdering::Release);
            return Ok(existing.timeline.clone());
        }

        metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
            .with_label_values(&["miss"])
            .inc();

        let timeline = build();
        if self.timelines.len() >= self.max_entries && !self.evict_one_idle_entry() {
            metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                .with_label_values(&["saturated"])
                .inc();
            return Err(TsoError::TimelineRuntimeCacheSaturated {
                timeline_key: timeline_key.to_owned(),
                max_entries: self.max_entries,
            });
        }

        match self.timelines.entry(timeline_key.to_owned()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                entry
                    .get()
                    .last_access_tick
                    .store(self.next_access_tick(), AtomicOrdering::Release);
                Ok(entry.get().timeline.clone())
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(TimelineCacheEntry {
                    timeline: timeline.clone(),
                    last_access_tick: AtomicU64::new(self.next_access_tick()),
                });
                metrics::TSO_TIMELINE_RUNTIME_CACHE_ENTRIES.set(self.timelines.len() as i64);
                Ok(timeline)
            }
        }
    }

    pub(crate) fn insert_timeline(
        &self,
        timeline_key: String,
        timeline: Arc<Mutex<TimelineState>>,
    ) -> Result<(), TsoError> {
        if let Some(mut existing) = self.timelines.get_mut(&timeline_key) {
            metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                .with_label_values(&["hit"])
                .inc();
            existing.timeline = timeline;
            existing
                .last_access_tick
                .store(self.next_access_tick(), AtomicOrdering::Release);
            return Ok(());
        }

        if self.timelines.len() >= self.max_entries && !self.evict_one_idle_entry() {
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
                entry.get_mut().timeline = timeline;
                entry
                    .get()
                    .last_access_tick
                    .store(self.next_access_tick(), AtomicOrdering::Release);
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                    .with_label_values(&["miss"])
                    .inc();
                entry.insert(TimelineCacheEntry {
                    timeline,
                    last_access_tick: AtomicU64::new(self.next_access_tick()),
                });
                metrics::TSO_TIMELINE_RUNTIME_CACHE_ENTRIES.set(self.timelines.len() as i64);
            }
        }
        Ok(())
    }

    pub(crate) fn remove_timeline(&self, timeline_key: &str) {
        self.timelines.remove(timeline_key);
        metrics::TSO_TIMELINE_RUNTIME_CACHE_ENTRIES.set(self.timelines.len() as i64);
    }

    pub(crate) fn clear(&self) {
        self.timelines.clear();
        metrics::TSO_TIMELINE_RUNTIME_CACHE_ENTRIES.set(0);
    }

    pub(crate) fn subscribe_route_changes(&self) -> broadcast::Receiver<TimelineRoute> {
        self.route_notifier.subscribe()
    }

    pub(crate) fn notifier(&self) -> broadcast::Sender<TimelineRoute> {
        self.route_notifier.clone()
    }

    fn next_access_tick(&self) -> u64 {
        self.access_counter.fetch_add(1, AtomicOrdering::Relaxed)
    }

    fn evict_one_idle_entry(&self) -> bool {
        let candidate = self
            .timelines
            .iter()
            .filter_map(|entry| {
                (Arc::strong_count(&entry.timeline) == 1).then(|| {
                    (
                        entry.key().clone(),
                        entry.last_access_tick.load(AtomicOrdering::Acquire),
                    )
                })
            })
            .min_by_key(|(_, tick)| *tick)
            .map(|(key, _)| key);

        if let Some(key) = candidate {
            let removed = self.timelines.remove(&key).is_some();
            if removed {
                metrics::TSO_TIMELINE_RUNTIME_CACHE_EVENTS_TOTAL
                    .with_label_values(&["evicted_idle"])
                    .inc();
                metrics::TSO_TIMELINE_RUNTIME_CACHE_ENTRIES.set(self.timelines.len() as i64);
            }
            return removed;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{metrics, ResourceTier, TimelineLifecycleState, TimelineRoute};

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
        let runtime = TimelineRuntimeState::new(1);
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
                >= before + 1
        );
        drop(first);
    }

    #[test]
    fn runtime_cache_records_evicted_idle_metric() {
        let runtime = TimelineRuntimeState::new(1);
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
                >= before + 1
        );
    }
}
