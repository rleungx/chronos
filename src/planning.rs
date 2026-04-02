use std::cmp::max;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::metadata::{GeneratorRecord, TimelineRecord};
use crate::{ResourceTier, TransferReason, TsoError, MAX_GENERATORS};

#[derive(Debug, Clone)]
pub(crate) struct TransferPlan {
    pub(crate) resource_tier: ResourceTier,
    pub(crate) owner_endpoint: String,
    pub(crate) generator_id: Option<u32>,
    pub(crate) reason: TransferReason,
}

pub(crate) fn resource_tier_for_generator_id(
    generator_id: u32,
    shared_generators: u32,
    warm_generators: u32,
) -> Result<ResourceTier, TsoError> {
    if generator_id >= MAX_GENERATORS {
        return Err(TsoError::GeneratorIdOutOfRange { generator_id });
    }

    let dedicated_start = shared_generators + warm_generators;
    if generator_id < shared_generators {
        Ok(ResourceTier::Shared)
    } else if generator_id < dedicated_start {
        Ok(ResourceTier::Warm)
    } else {
        Ok(ResourceTier::Dedicated)
    }
}

pub(crate) fn pick_owned_generator_by_hash<F>(
    timeline_key: &str,
    start: u32,
    width: u32,
    owns_generator_id: F,
) -> Result<u32, TsoError>
where
    F: Fn(u32) -> bool,
{
    if width == 0 {
        return Err(TsoError::GeneratorPoolExhausted);
    }

    let end = start.saturating_add(width);
    let mut owned = Vec::new();
    for generator_id in start..end {
        if owns_generator_id(generator_id) {
            owned.push(generator_id);
        }
    }

    if owned.is_empty() {
        return Err(TsoError::GeneratorPoolExhausted);
    }

    let mut hasher = DefaultHasher::new();
    timeline_key.hash(&mut hasher);
    let offset = (hasher.finish() as usize) % owned.len();
    Ok(owned[offset])
}

pub(crate) fn should_release_claimed_dedicated_generator(
    claimed: bool,
    previous_resource_tier: ResourceTier,
    previous_generator_id: u32,
    claimed_generator_id: u32,
) -> bool {
    claimed
        && (previous_resource_tier != ResourceTier::Dedicated
            || previous_generator_id != claimed_generator_id)
}

pub(crate) fn should_release_previous_dedicated_generator_after_route_change(
    previous_resource_tier: ResourceTier,
    previous_generator_id: u32,
    target_resource_tier: ResourceTier,
    new_generator_id: u32,
) -> bool {
    previous_resource_tier == ResourceTier::Dedicated
        && (target_resource_tier != ResourceTier::Dedicated
            || previous_generator_id != new_generator_id)
}

pub(crate) fn safe_floor_for_transfer_reason(
    transfer_reason: TransferReason,
    last_graceful_issued: Option<u64>,
    previous_generator_floor_tso: Option<u64>,
) -> Option<u64> {
    match transfer_reason {
        TransferReason::Failover => previous_generator_floor_tso,
        _ => match (last_graceful_issued, previous_generator_floor_tso) {
            (Some(a), Some(b)) => Some(max(a, b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        },
    }
}

pub(crate) fn generator_recovery_floor_tso(generator_record: &GeneratorRecord) -> Option<u64> {
    match (
        generator_record.last_issued_tso,
        generator_record.issued_upper_bound,
    ) {
        (Some(a), Some(b)) => Some(max(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

pub(crate) fn recovered_timeline_floor_tso(timeline_record: &TimelineRecord) -> Option<u64> {
    match (
        timeline_record.last_graceful_issued,
        timeline_record.recovery_floor_tso,
    ) {
        (Some(a), Some(b)) => Some(max(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        generator_recovery_floor_tso, recovered_timeline_floor_tso, safe_floor_for_transfer_reason,
    };
    use crate::metadata::{GeneratorRecord, TimelineRecord};
    use crate::TransferReason;
    use crate::{ResourceTier, TimelineLifecycleState, TimelineRoute};

    #[test]
    fn safe_floor_prefers_previous_floor_for_failover() {
        assert_eq!(
            safe_floor_for_transfer_reason(TransferReason::Failover, Some(10), Some(20)),
            Some(20)
        );
    }

    #[test]
    fn safe_floor_uses_max_for_non_failover_transfers() {
        assert_eq!(
            safe_floor_for_transfer_reason(TransferReason::Manual, Some(10), Some(20)),
            Some(20)
        );
    }

    #[test]
    fn safe_floor_preserves_single_available_floor() {
        assert_eq!(
            safe_floor_for_transfer_reason(TransferReason::Rebalance, None, Some(20)),
            Some(20)
        );
        assert_eq!(
            safe_floor_for_transfer_reason(TransferReason::Hotspot, Some(10), None),
            Some(10)
        );
    }

    #[test]
    fn generator_recovery_floor_prefers_upper_bound_max() {
        let record = GeneratorRecord {
            generator_id: 7,
            owner_worker_endpoint: "worker".into(),
            owner_instance_id: "instance".into(),
            generator_lease_token: 1,
            lease_expire_at_ms: Some(10),
            last_issued_tso: Some(100),
            issued_upper_bound: Some(200),
            updated_at_ms: 20,
        };
        assert_eq!(generator_recovery_floor_tso(&record), Some(200));
    }

    #[test]
    fn recovered_timeline_floor_prefers_max_of_graceful_and_recovery() {
        let record = TimelineRecord {
            route: TimelineRoute {
                timeline_key: "t".into(),
                generator_id: 7,
                epoch: 1,
                route_version: 1,
                resource_tier: ResourceTier::Shared,
                owner_worker_endpoint: "worker".into(),
            },
            state: TimelineLifecycleState::Active,
            recovery_floor_tso: Some(200),
            issued_upper_bound: None,
            last_graceful_issued: Some(100),
            lease_expire_at_ms: None,
            updated_at_ms: 20,
        };
        assert_eq!(recovered_timeline_floor_tso(&record), Some(200));
    }
}
