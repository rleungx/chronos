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
    ControlPlaneStore, GeneratorBatchOp, GeneratorLeaseAuthority, GeneratorRecord,
    RouteUpdateSignal, RouteUpdateSource, TimelineAuthority, TimelineBatchOp,
    TimelineFilterRecord, TimelineFilterRecordListPage, TimelineRecord, TimelineRecordListPage,
    TimelineRouteRecord,
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
        ttl: Duration,
    ) -> Result<InstanceIdentityLease, crate::TsoError>;
}
