use std::cmp::{max, min};
use std::hint::spin_loop;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use dashmap::DashMap;

use crate::{encode_tso, metrics, next_cursor_after, TimestampRange, TsoError, SEQUENCE_CAPACITY};

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
        let count_u64 = u64::from(count);
        let floor_index = match floor {
            Some(value) => {
                let floor_cursor = next_cursor_after(value, self.id)?;
                floor_cursor
                    .physical_ms
                    .checked_mul(cap)
                    .and_then(|value| value.checked_add(floor_cursor.sequence as u64))
                    .unwrap_or(u64::MAX)
            }
            None => 0,
        };

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
                .checked_add(count_u64)
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
                    let mut remaining = count_u64;
                    let mut cursor_index = start_index;
                    let range_count = (last_physical_ms - start_physical_ms + 1) as usize;
                    let mut ranges = Vec::with_capacity(range_count);
                    while remaining > 0 {
                        let physical_ms = cursor_index / cap;
                        let sequence_index = cursor_index % cap;
                        let sequence = sequence_index as u32;
                        let available = cap - sequence_index;
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
                Err(_) => spin_loop(),
            }
        }
    }
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

    pub(crate) fn clear_leases(&self) {
        self.leases.clear();
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

    pub(crate) fn clear_dedicated_claims(&self) {
        self.dedicated_claims.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_after_splits_ranges_across_physical_boundaries() {
        let generator = Generator::new(7);

        let ranges = generator
            .allocate_after(SEQUENCE_CAPACITY + 1, None, 5, 2, 0, None)
            .expect("allocation should succeed");

        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].start_tso, encode_tso(5, 7, 0).unwrap());
        assert_eq!(
            ranges[0].end_tso,
            encode_tso(5, 7, SEQUENCE_CAPACITY - 1).unwrap()
        );
        assert_eq!(ranges[1].start_tso, encode_tso(6, 7, 0).unwrap());
        assert_eq!(ranges[1].end_tso, encode_tso(6, 7, 0).unwrap());
    }

    #[test]
    fn allocate_after_respects_floor_cursor() {
        let generator = Generator::new(3);
        let floor = encode_tso(8, 3, 9).unwrap();

        let ranges = generator
            .allocate_after(2, Some(floor), 1, 10, 0, None)
            .expect("allocation should honor floor");

        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start_tso, encode_tso(8, 3, 10).unwrap());
        assert_eq!(ranges[0].end_tso, encode_tso(8, 3, 11).unwrap());
    }

    #[test]
    fn allocate_after_rejects_clock_rewind_beyond_limit() {
        let generator = Generator::new(5);
        generator
            .next_index
            .store(SEQUENCE_CAPACITY as u64 * 10, AtomicOrdering::Release);

        let error = generator
            .allocate_after(1, None, 5, 0, 2, None)
            .expect_err("rewind beyond limit should fail");

        assert_eq!(error, TsoError::ClockBackwards { delta_ms: 5 });
    }

    #[test]
    fn allocate_after_rejects_future_borrow_beyond_limit() {
        let generator = Generator::new(4);

        let error = generator
            .allocate_after(1, Some(encode_tso(20, 4, 0).unwrap()), 1, 0, 0, None)
            .expect_err("future borrow beyond limit should fail");

        assert_eq!(
            error,
            TsoError::FutureBorrowExceeded {
                requested_physical_ms: 20,
                allowed_physical_ms: 1,
            }
        );
    }

    #[test]
    fn allocate_after_rejects_requests_beyond_issued_upper_bound() {
        let generator = Generator::new(6);
        let upper_bound = encode_tso(3, 6, 1).unwrap();

        let error = generator
            .allocate_after(3, None, 3, 0, 0, Some(upper_bound))
            .expect_err("upper bound should cap allocation");

        assert_eq!(
            error,
            TsoError::IssuedUpperBoundExceeded {
                requested_end_tso: encode_tso(3, 6, 2).unwrap(),
                issued_upper_bound: upper_bound,
            }
        );
    }

    #[test]
    fn lease_validation_requires_matching_owner_and_unexpired_time() {
        let runtime = GeneratorRuntimeState::new(2);
        runtime.upsert_lease(
            1,
            GeneratorLeaseState {
                revision: 7,
                owner_instance_id: "worker-a".to_owned(),
                generator_lease_token: 11,
                lease_expire_at_ms: 500,
                last_persisted_tso: Some(99),
                issued_upper_bound: Some(123),
            },
        );

        assert!(runtime.lease_valid_for_owner(1, "worker-a", 499));
        assert_eq!(
            runtime.valid_lease_upper_bound_for_owner(1, "worker-a", 499),
            Some(123)
        );
        assert!(!runtime.lease_valid_for_owner(1, "worker-b", 499));
        assert_eq!(
            runtime.valid_lease_upper_bound_for_owner(1, "worker-b", 499),
            None
        );
        assert!(!runtime.lease_valid_for_owner(1, "worker-a", 500));
        assert_eq!(
            runtime.valid_lease_upper_bound_for_owner(1, "worker-a", 500),
            None
        );
    }

    #[test]
    fn dedicated_claims_stay_single_home_until_matching_release() {
        let runtime = GeneratorRuntimeState::new(8);

        assert_eq!(runtime.claimed_generator_for_timeline("timeline-a"), None);
        assert!(runtime.claim_dedicated(6, "timeline-a").unwrap());
        assert_eq!(
            runtime.claimed_generator_for_timeline("timeline-a"),
            Some(6)
        );
        assert!(!runtime.claim_dedicated(6, "timeline-a").unwrap());
        assert_eq!(
            runtime.claim_dedicated(6, "timeline-b"),
            Err(TsoError::GeneratorPoolExhausted)
        );

        runtime.release_dedicated(6, "timeline-b");
        assert_eq!(
            runtime.claimed_generator_for_timeline("timeline-a"),
            Some(6)
        );

        runtime.release_dedicated(6, "timeline-a");
        assert_eq!(runtime.claimed_generator_for_timeline("timeline-a"), None);
    }
}
