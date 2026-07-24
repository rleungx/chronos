mod etcd;
mod identity;
mod keys;
mod memory;
mod types;

use async_trait::async_trait;
use tokio::time::Duration;

pub use etcd::EtcdMetadataStore;
pub use identity::InstanceIdentityLease;
pub use memory::MemoryMetadataStore;
pub use types::{
    AllocationRequestFingerprint, AllocationResponseRecord, ControlPlaneStore, GeneratorBatchOp,
    GeneratorLeaseAuthority, GeneratorRecord, OwnershipPlanMember, OwnershipPlanRecord,
    RequestRecord, RequestRecordAuthority, RequestRecordState, RouteUpdateSignal,
    RouteUpdateSource, TimelineAuthority, TimelineBatchOp, TimelineFilterRecord,
    TimelineFilterRecordListPage, TimelineRecord, TimelineRecordListPage, TimelineRouteRecord,
    CURRENT_CLUSTER_FORMAT_VERSION, CURRENT_METADATA_SCHEMA_VERSION,
};

/// Returns the whole-second minimum sent in an etcd identity lease grant request.
///
/// etcd treats the requested TTL as an advisory minimum and chooses the authoritative granted
/// TTL. Round fractional seconds up so the request never asks for less than the configured
/// duration. A zero duration retains the historical one-second minimum for direct callers.
pub fn identity_lease_grant_request_ttl_seconds(ttl: Duration) -> Result<i64, crate::TsoError> {
    let whole_seconds = ttl.as_secs();
    let rounded_seconds = whole_seconds
        .checked_add(u64::from(ttl.subsec_nanos() != 0))
        .ok_or_else(|| {
            crate::TsoError::Internal(
                "identity lease grant request TTL seconds overflowed u64".into(),
            )
        })?
        .max(1);

    i64::try_from(rounded_seconds).map_err(|_| {
        crate::TsoError::Internal(format!(
            "identity lease grant request TTL {}s exceeds etcd i64 seconds",
            rounded_seconds
        ))
    })
}

#[async_trait]
/// Issues instance identity leases for metadata backends that support external lease ownership.
///
/// This is currently an etcd-backed capability. `EtcdMetadataStore` implements the trait by
/// acquiring a real etcd lease, while `MemoryMetadataStore` does not implement it.
pub trait IdentityLeaseAuthority: Send + Sync {
    async fn acquire_instance_identity_lease(
        &self,
        instance_id: &str,
        worker_id: &str,
        advertise_endpoint: &str,
        ownership_plan_id: &str,
        ownership_modulo: u32,
        ttl: Duration,
    ) -> Result<InstanceIdentityLease, crate::TsoError>;
}

#[cfg(test)]
mod tests {
    use super::identity_lease_grant_request_ttl_seconds;
    use tokio::time::Duration;

    #[test]
    fn identity_lease_grant_request_rounds_fractional_seconds_up() {
        for (configured_ms, expected_seconds) in [(500, 1), (1_500, 2), (3_000, 3)] {
            let ttl = Duration::from_millis(configured_ms);
            assert_eq!(
                identity_lease_grant_request_ttl_seconds(ttl).unwrap(),
                expected_seconds
            );
        }
    }

    #[test]
    fn identity_lease_grant_request_preserves_direct_zero_duration_compatibility() {
        assert_eq!(
            identity_lease_grant_request_ttl_seconds(Duration::ZERO).unwrap(),
            1
        );
    }

    #[test]
    fn identity_lease_grant_request_rejects_seconds_above_etcd_i64_limit() {
        let ttl = Duration::from_secs(i64::MAX as u64 + 1);
        let error = identity_lease_grant_request_ttl_seconds(ttl).unwrap_err();

        assert!(error.to_string().contains("exceeds etcd i64 seconds"));
    }
}
