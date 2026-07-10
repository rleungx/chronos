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
