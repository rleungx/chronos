mod allocation;
mod background;
mod contention;
mod facade;
mod lease;
mod runtime_coordination;
mod transfer;
mod worker_readiness;

use std::cmp::max;
use std::sync::Arc;

use tokio::time::Instant;

use crate::metadata::{ControlPlaneStore, GeneratorRecord};
use crate::plane::RequestCancellation;
use crate::planning::{generator_recovery_floor_tso, pick_owned_generator_by_hash};
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
    timeline_load_coordinator: TimelineLoadCoordinator,
    generator_lease_coordinator: GeneratorLeaseCoordinator,
    metadata_contention: MetadataContentionCoordinator,
    background: BackgroundCoordinator,
    ownership_drift: OwnershipDriftTracker,
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
        owner_endpoint == self.config.advertise_endpoint
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

    pub(super) fn check_request_cancellation(
        cancellation: Option<&RequestCancellation>,
    ) -> Result<(), TsoError> {
        if cancellation.is_some_and(RequestCancellation::is_cancelled) {
            Err(TsoError::RequestCancelled)
        } else {
            Ok(())
        }
    }

    pub(super) fn observe_contended_local_generator_ownership(
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
}
