mod allocation;
mod background;
mod contention;
mod facade;
mod lease;
mod runtime_coordination;
mod transfer;
mod worker_readiness;

use std::cmp::max;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::metadata::{ControlPlaneStore, GeneratorRecord};
use crate::plane::RequestCancellation;
use crate::planning::{generator_recovery_floor_tso, pick_owned_generator_by_hash};
use crate::recovery::record_recovery_event;
use crate::runtime::Generator;
use crate::{decode_tso, next_cursor_after, Clock, TsoConfig, TsoError, MAX_GENERATORS};

use crate::runtime::{GeneratorRuntimeState, TimelineRuntimeState};
use background::BackgroundCoordinator;
use contention::MetadataContentionCoordinator;
use lease::GeneratorLeaseCoordinator;
use runtime_coordination::TimelineLoadCoordinator;
use worker_readiness::OwnershipDriftTracker;
pub use worker_readiness::{OwnershipDriftEvidence, WorkerReadinessSink};

pub struct TsoService {
    pub(super) config: TsoConfig,
    pub(super) instance_id: String,
    pub(super) clock: Arc<dyn Clock>,
    pub(super) metadata: Arc<dyn ControlPlaneStore>,
    pub(super) generator_runtime: GeneratorRuntimeState,
    pub(super) timeline_runtime: TimelineRuntimeState,
    pub(super) generator_admission_gates: Vec<Arc<Semaphore>>,
    pub(super) generator_fairness_trackers: Vec<Arc<StdMutex<GeneratorFairnessState>>>,
    pub(super) generator_fairness_notifiers: Vec<Arc<Notify>>,
    timeline_load_coordinator: TimelineLoadCoordinator,
    timeline_load_limiter: Arc<Semaphore>,
    generator_lease_coordinator: GeneratorLeaseCoordinator,
    metadata_contention: MetadataContentionCoordinator,
    auto_failover_scan: StdMutex<AutoFailoverScanState>,
    background: BackgroundCoordinator,
    ownership_drift: OwnershipDriftTracker,
    shutdown_gate: AtomicBool,
}

#[derive(Default)]
pub(super) struct GeneratorFairnessState {
    active_timeline_key: Option<String>,
    wait_queue: VecDeque<GeneratorWaitQueueEntry>,
    next_waiter_id: u64,
}

pub(super) struct GeneratorWaitQueueEntry {
    timeline_key: String,
    waiter_id: u64,
}

#[derive(Default)]
pub(super) struct AutoFailoverScanState {
    owner_cursor_index: usize,
    owner_cursors: HashMap<String, Option<String>>,
    owner_endpoints: Vec<String>,
    owner_endpoints_refresh_after_ms: u64,
}

pub(super) struct GeneratorAdmissionPermit {
    _permit: OwnedSemaphorePermit,
    turn: Option<GeneratorAdmissionTurnGuard>,
}

struct GeneratorAdmissionTurnGuard {
    fairness: Arc<StdMutex<GeneratorFairnessState>>,
    notifier: Arc<Notify>,
    timeline_key: String,
    released: bool,
}

struct GeneratorAdmissionWaiterGuard {
    fairness: Arc<StdMutex<GeneratorFairnessState>>,
    notifier: Arc<Notify>,
    waiter_id: u64,
    queued: bool,
}

impl GeneratorAdmissionTurnGuard {
    fn new(
        fairness: Arc<StdMutex<GeneratorFairnessState>>,
        notifier: Arc<Notify>,
        timeline_key: String,
    ) -> Self {
        Self {
            fairness,
            notifier,
            timeline_key,
            released: false,
        }
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let mut fairness = lock_generator_fairness(&self.fairness);
        if fairness.active_timeline_key.as_deref() == Some(&self.timeline_key) {
            fairness.active_timeline_key = None;
            drop(fairness);
            self.notifier.notify_waiters();
        }
    }

    fn release(mut self) {
        self.release_inner();
    }
}

impl GeneratorAdmissionWaiterGuard {
    fn new(
        fairness: Arc<StdMutex<GeneratorFairnessState>>,
        notifier: Arc<Notify>,
        waiter_id: u64,
    ) -> Self {
        Self {
            fairness,
            notifier,
            waiter_id,
            queued: true,
        }
    }

    fn disarm(mut self) {
        self.queued = false;
    }
}

impl Drop for GeneratorAdmissionWaiterGuard {
    fn drop(&mut self) {
        if !self.queued {
            return;
        }

        let mut fairness = lock_generator_fairness(&self.fairness);
        let was_front = fairness
            .wait_queue
            .front()
            .is_some_and(|queued| queued.waiter_id == self.waiter_id);
        let original_len = fairness.wait_queue.len();
        fairness
            .wait_queue
            .retain(|queued| queued.waiter_id != self.waiter_id);
        let removed = fairness.wait_queue.len() != original_len;
        if removed {
            drop(fairness);
            if was_front {
                self.notifier.notify_waiters();
            }
        }
    }
}

impl Drop for GeneratorAdmissionTurnGuard {
    fn drop(&mut self) {
        self.release_inner();
    }
}

impl GeneratorAdmissionPermit {
    pub(super) fn release(mut self) {
        if let Some(turn) = self.turn.take() {
            turn.release();
        }
    }
}

fn lock_generator_fairness(
    fairness: &StdMutex<GeneratorFairnessState>,
) -> StdMutexGuard<'_, GeneratorFairnessState> {
    match fairness.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            record_recovery_event("service", "generator_fairness_lock", "mutex_poisoned");
            poisoned.into_inner()
        }
    }
}

fn queue_generator_waiter_if_needed(
    fairness_state: &mut GeneratorFairnessState,
    fairness: &Arc<StdMutex<GeneratorFairnessState>>,
    notifier: &Arc<Notify>,
    timeline_key: &str,
    waiter_guard: &mut Option<GeneratorAdmissionWaiterGuard>,
) {
    if waiter_guard.is_some() {
        return;
    }

    let waiter_id = fairness_state.next_waiter_id;
    fairness_state.next_waiter_id = fairness_state.next_waiter_id.wrapping_add(1);
    fairness_state
        .wait_queue
        .push_back(GeneratorWaitQueueEntry {
            timeline_key: timeline_key.to_owned(),
            waiter_id,
        });
    *waiter_guard = Some(GeneratorAdmissionWaiterGuard::new(
        fairness.clone(),
        notifier.clone(),
        waiter_id,
    ));
}

impl TsoService {
    pub(super) async fn backoff_after_metadata_contention(
        &self,
        retries: u32,
        deadline: Instant,
    ) -> Result<(), TsoError> {
        self.metadata_contention
            .backoff_after_contention(retries, deadline)
            .await
    }

    pub(super) fn owns_generator_id(&self, generator_id: u32) -> bool {
        generator_id % self.config.generator_ownership_modulo
            == self.config.generator_ownership_remainder
    }

    pub(super) fn is_local_endpoint(&self, owner_endpoint: &str) -> bool {
        endpoints_match(owner_endpoint, &self.config.advertise_endpoint)
    }

    pub(super) fn local_instance_id(&self) -> &str {
        &self.instance_id
    }

    pub(super) fn is_local_generator_owner(&self, record: &GeneratorRecord) -> bool {
        self.is_local_endpoint(&record.owner_worker_endpoint)
            && record.owner_instance_id == self.local_instance_id()
    }

    pub(super) fn clear_timeline_cache(&self, timeline_key: &str) {
        self.timeline_runtime.remove_timeline(timeline_key);
    }

    pub(super) async fn acquire_generator_admission(
        &self,
        generator_id: u32,
        resource_tier: crate::ResourceTier,
        timeline_key: &str,
        cancellation: Option<RequestCancellation>,
    ) -> Result<Option<GeneratorAdmissionPermit>, TsoError> {
        if !matches!(
            resource_tier,
            crate::ResourceTier::Shared | crate::ResourceTier::Warm
        ) {
            return Ok(None);
        }

        let fairness = self.generator_fairness_trackers[generator_id as usize].clone();
        let notifier = self.generator_fairness_notifiers[generator_id as usize].clone();
        let timeline_key_owned = timeline_key.to_owned();
        let mut waiter_guard: Option<GeneratorAdmissionWaiterGuard> = None;

        let turn_guard = loop {
            Self::check_request_cancellation(cancellation.as_ref())?;
            let notified = notifier.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let grant_turn = {
                let mut fairness_state = lock_generator_fairness(&fairness);
                if fairness_state.active_timeline_key.is_none() {
                    match fairness_state.wait_queue.front() {
                        Some(front)
                            if waiter_guard
                                .as_ref()
                                .is_some_and(|guard| guard.waiter_id == front.waiter_id) =>
                        {
                            let queued = fairness_state
                                .wait_queue
                                .pop_front()
                                .expect("front queue entry must exist");
                            fairness_state.active_timeline_key = Some(queued.timeline_key);
                            if let Some(waiter_guard) = waiter_guard.take() {
                                waiter_guard.disarm();
                            }
                            true
                        }
                        Some(_) => {
                            queue_generator_waiter_if_needed(
                                &mut fairness_state,
                                &fairness,
                                &notifier,
                                &timeline_key_owned,
                                &mut waiter_guard,
                            );
                            false
                        }
                        None => {
                            if let Some(waiter_guard) = waiter_guard.take() {
                                waiter_guard.disarm();
                            }
                            fairness_state.active_timeline_key = Some(timeline_key_owned.clone());
                            true
                        }
                    }
                } else {
                    queue_generator_waiter_if_needed(
                        &mut fairness_state,
                        &fairness,
                        &notifier,
                        &timeline_key_owned,
                        &mut waiter_guard,
                    );
                    false
                }
            };

            if grant_turn {
                break GeneratorAdmissionTurnGuard::new(
                    fairness.clone(),
                    notifier.clone(),
                    timeline_key_owned.clone(),
                );
            }

            if let Some(cancellation) = cancellation.as_ref() {
                tokio::select! {
                    _ = &mut notified => continue,
                    _ = cancellation.cancelled() => return Err(TsoError::RequestCancelled),
                }
            } else {
                notified.await;
            }
        };

        let permit = self.generator_admission_gates[generator_id as usize]
            .clone()
            .acquire_owned();
        let permit_result = if let Some(cancellation) = cancellation {
            tokio::select! {
                permit = permit => permit
                    .map_err(|_| TsoError::ServiceShuttingDown),
                _ = cancellation.cancelled() => Err(TsoError::RequestCancelled),
            }
        } else {
            permit.await.map_err(|_| TsoError::ServiceShuttingDown)
        };

        match permit_result {
            Ok(permit) => Ok(Some(GeneratorAdmissionPermit {
                _permit: permit,
                turn: Some(turn_guard),
            })),
            Err(error) => {
                turn_guard.release();
                Err(error)
            }
        }
    }

    pub(super) fn check_request_cancellation(
        cancellation: Option<&RequestCancellation>,
    ) -> Result<(), TsoError> {
        if cancellation.is_some_and(RequestCancellation::is_cancelled) {
            Err(TsoError::RequestCancelled)
        } else {
            Ok(())
        }
    }

    pub(super) fn reject_new_work_if_shutting_down(&self) -> Result<(), TsoError> {
        if self.shutdown_gate.load(AtomicOrdering::Acquire) {
            Err(TsoError::ServiceShuttingDown)
        } else {
            Ok(())
        }
    }

    #[doc(hidden)]
    pub fn request_local_shutdown_gate(&self) {
        self.shutdown_gate.store(true, AtomicOrdering::Release);
    }

    pub(super) async fn acquire_timeline_load_permit(
        &self,
        cancellation: Option<RequestCancellation>,
    ) -> Result<OwnedSemaphorePermit, TsoError> {
        let limiter = self.timeline_load_limiter.clone();
        let deadline = tokio::time::Instant::now() + self.metadata_contention_retry_budget();
        if let Some(cancellation) = cancellation {
            tokio::select! {
                permit = limiter.acquire_owned() => permit
                    .map_err(|_| TsoError::Internal("timeline load limiter closed".into())),
                _ = cancellation.cancelled() => Err(TsoError::RequestCancelled),
                _ = tokio::time::sleep_until(deadline) => Err(TsoError::RequestCancelled),
            }
        } else {
            tokio::time::timeout_at(deadline, limiter.acquire_owned())
                .await
                .map_err(|_| TsoError::RequestCancelled)?
                .map_err(|_| TsoError::Internal("timeline load limiter closed".into()))
        }
    }

    pub(super) fn observe_contended_local_generator_ownership_drift(
        &self,
        generator_id: u32,
        contending_instance_id: &str,
        lease_expire_at_ms: u64,
        observed_at_ms: u64,
    ) {
        self.ownership_drift.observe_contended_local_generator(
            generator_id,
            contending_instance_id,
            lease_expire_at_ms,
            observed_at_ms,
        );
    }

    pub(super) fn clear_generator_ownership_drift(&self, generator_id: u32) {
        self.ownership_drift.clear_generator(generator_id);
    }

    pub(super) fn next_generator_with_ownership_drift(&self, now_ms: u64) -> Option<u32> {
        self.ownership_drift
            .next_generator_to_retry(now_ms, self.config.generator_maintenance_interval_ms)
    }

    pub(super) fn lookup_generator(&self, generator_id: u32) -> Result<Arc<Generator>, TsoError> {
        self.ensure_generator_id_range(generator_id)?;
        Ok(self.generator_runtime.generator(generator_id))
    }

    pub(super) fn ensure_generator_id_range(&self, generator_id: u32) -> Result<(), TsoError> {
        if generator_id >= MAX_GENERATORS {
            return Err(TsoError::GeneratorIdOutOfRange { generator_id });
        }
        Ok(())
    }

    pub(super) async fn current_generator_floor(
        &self,
        generator_id: u32,
    ) -> Result<Option<u64>, TsoError> {
        let local_last = self
            .lookup_generator(generator_id)?
            .current_last_issued_tso()?;
        let persisted_floor = self
            .metadata
            .load_generator(generator_id)
            .await?
            .and_then(|(record, _)| generator_recovery_floor_tso(&record));
        Ok(match (local_last, persisted_floor) {
            (Some(a), Some(b)) => Some(max(a, b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        })
    }

    pub(super) async fn required_jump_ms_for_generator(
        &self,
        generator_id: u32,
        safe_floor: u64,
    ) -> Result<u64, TsoError> {
        let Some(current_floor) = self.current_generator_floor(generator_id).await? else {
            return Ok(0);
        };
        let current_ms = decode_tso(current_floor).physical_ms;
        let target_ms = next_cursor_after(safe_floor, generator_id)?.physical_ms;
        Ok(target_ms.saturating_sub(current_ms))
    }

    pub(super) fn pick_generator_id(
        &self,
        timeline_key: &str,
        resource_tier: crate::ResourceTier,
    ) -> Result<u32, TsoError> {
        match resource_tier {
            crate::ResourceTier::Shared => pick_owned_generator_by_hash(
                timeline_key,
                0,
                self.config.shared_generators,
                |generator_id| self.owns_generator_id(generator_id),
            ),
            crate::ResourceTier::Warm => pick_owned_generator_by_hash(
                timeline_key,
                self.config.shared_generators,
                self.config.warm_generators,
                |generator_id| self.owns_generator_id(generator_id),
            ),
            crate::ResourceTier::Dedicated => self.claim_dedicated(timeline_key).map(|(id, _)| id),
        }
    }

    pub(super) fn claim_dedicated(&self, timeline_key: &str) -> Result<(u32, bool), TsoError> {
        let dedicated_start = self.config.shared_generators + self.config.warm_generators;
        if let Some(generator_id) = self
            .generator_runtime
            .claimed_generator_for_timeline(timeline_key)
        {
            return Ok((generator_id, false));
        }
        for generator_id in dedicated_start..MAX_GENERATORS {
            if !self.owns_generator_id(generator_id) {
                continue;
            }
            match self
                .generator_runtime
                .claim_dedicated(generator_id, timeline_key)
            {
                Ok(claimed) => return Ok((generator_id, claimed)),
                Err(TsoError::GeneratorPoolExhausted) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(TsoError::GeneratorPoolExhausted)
    }

    pub(super) fn claim_specific_dedicated(
        &self,
        generator_id: u32,
        timeline_key: &str,
    ) -> Result<bool, TsoError> {
        let dedicated_start = self.config.shared_generators + self.config.warm_generators;
        if generator_id < dedicated_start {
            return Err(TsoError::GeneratorIdOutOfRange { generator_id });
        }
        self.generator_runtime
            .claim_dedicated(generator_id, timeline_key)
    }

    pub(super) fn release_dedicated(&self, generator_id: u32, timeline_key: &str) {
        self.generator_runtime
            .release_dedicated(generator_id, timeline_key);
    }

    pub(super) fn clear_dedicated_claims(&self) {
        self.generator_runtime.clear_dedicated_claims();
    }
}

pub(crate) fn endpoints_match(left: &str, right: &str) -> bool {
    let left = left.trim();
    let right = right.trim();
    if left == right || left.eq_ignore_ascii_case(right) {
        return true;
    }

    match (left.parse::<SocketAddr>(), right.parse::<SocketAddr>()) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::endpoints_match;

    #[test]
    fn endpoints_match_accepts_trimmed_and_case_insensitive_host_forms() {
        assert!(endpoints_match(" worker-a:50051 ", "WORKER-A:50051"));
    }

    #[test]
    fn endpoints_match_accepts_equivalent_socket_addrs() {
        assert!(endpoints_match("127.0.0.1:50051", "127.0.0.1:50051"));
    }

    #[test]
    fn endpoints_match_rejects_different_endpoints() {
        assert!(!endpoints_match("worker-a:50051", "worker-b:50051"));
    }
}
