use async_trait::async_trait;

use super::*;

impl RouteUpdateSource for EtcdMetadataStore {
    fn subscribe_route_updates(&self) -> broadcast::Receiver<RouteUpdateSignal> {
        self.route_updates.subscribe()
    }
}

#[async_trait]
impl crate::metadata::ControlPlaneStore for EtcdMetadataStore {
    fn request_records(&self) -> Option<&dyn RequestRecordAuthority> {
        Some(self)
    }

    async fn shutdown(&self) {
        self.shutdown_route_watch().await;
    }
}

#[async_trait]
impl IdentityLeaseAuthority for EtcdMetadataStore {
    async fn acquire_instance_identity_lease(
        &self,
        instance_id: &str,
        worker_id: &str,
        advertise_endpoint: &str,
        ttl: Duration,
    ) -> Result<InstanceIdentityLease, TsoError> {
        self.acquire_identity_lease_internal(instance_id, worker_id, advertise_endpoint, ttl)
            .await
    }
}

impl Drop for EtcdMetadataStore {
    fn drop(&mut self) {
        self.request_route_watch_shutdown();
        let route_watch_task = match self.route_watch_task.get_mut() {
            Ok(route_watch_task) => route_watch_task.take(),
            Err(poisoned) => {
                record_recovery_event("metadata", "etcd_route_watch_drop", "mutex_poisoned");
                poisoned.into_inner().take()
            }
        };
        if let Some(route_watch_task) = route_watch_task {
            route_watch_task.abort();
        }
    }
}
