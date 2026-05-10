use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{broadcast, Notify};

use crate::{
    AllocateTimestampsRequest, AllocateTimestampsResponse, HealthInfo, ResourceTier,
    TimelineLifecycleState, TimelineRoute, TransferReason, TsoError, TsoService,
};

#[derive(Clone, Default)]
pub(crate) struct RequestCancellation {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl RequestCancellation {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) async fn cancelled(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{timeout, Duration};

    use super::RequestCancellation;

    #[tokio::test]
    async fn request_cancellation_waiter_observes_pre_cancelled_token() {
        let cancellation = RequestCancellation::new();
        cancellation.cancel();

        timeout(Duration::from_millis(100), cancellation.cancelled())
            .await
            .expect("pre-cancelled token should return immediately");
    }

    #[tokio::test]
    async fn request_cancellation_waiter_observes_later_cancel() {
        let cancellation = RequestCancellation::new();
        let waiter_cancellation = cancellation.clone();
        let waiter = tokio::spawn(async move {
            waiter_cancellation.cancelled().await;
        });

        tokio::task::yield_now().await;
        cancellation.cancel();

        timeout(Duration::from_millis(100), waiter)
            .await
            .expect("cancelled waiter should finish")
            .expect("waiter task should succeed");
    }
}

#[derive(Clone)]
pub struct TsoControlPlane {
    inner: Arc<TsoService>,
}

impl TsoControlPlane {
    pub(crate) fn new(inner: Arc<TsoService>) -> Self {
        Self { inner }
    }

    pub async fn ensure_timeline(&self, timeline_key: &str) -> Result<TimelineRoute, TsoError> {
        self.inner.ensure_timeline(timeline_key).await
    }

    pub async fn ensure_timeline_with_tier(
        &self,
        timeline_key: &str,
        resource_tier: ResourceTier,
    ) -> Result<TimelineRoute, TsoError> {
        self.inner
            .ensure_timeline_with_tier(timeline_key, resource_tier)
            .await
    }

    pub async fn get_timeline_route(&self, timeline_key: &str) -> Result<TimelineRoute, TsoError> {
        self.inner.get_timeline_route(timeline_key).await
    }

    pub(crate) async fn get_timeline_status(
        &self,
        timeline_key: &str,
    ) -> Result<crate::status::TimelineStatusSnapshot, TsoError> {
        self.inner.get_timeline_status(timeline_key).await
    }

    pub(crate) async fn list_timeline_statuses(
        &self,
        states: &[TimelineLifecycleState],
        owner_worker_endpoint: Option<&str>,
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<crate::status::TimelineStatusListPage, TsoError> {
        self.inner
            .list_timeline_statuses(
                states,
                owner_worker_endpoint,
                start_after_timeline_key,
                limit,
            )
            .await
    }

    pub async fn renew_timeline_lease(&self, timeline_key: &str) -> Result<(), TsoError> {
        self.inner.renew_timeline_lease(timeline_key).await
    }

    pub async fn transfer_timeline_for_rpc(
        &self,
        timeline_key: &str,
        target_owner_endpoint: String,
        target_generator_id: Option<u32>,
        reason: TransferReason,
    ) -> Result<(TimelineRoute, u32, TimelineLifecycleState), TsoError> {
        self.inner
            .transfer_timeline_for_rpc(
                timeline_key,
                target_owner_endpoint,
                target_generator_id,
                reason,
            )
            .await
    }

    pub fn subscribe_route_changes(&self) -> broadcast::Receiver<TimelineRoute> {
        self.inner.subscribe_route_changes()
    }

    pub fn advertise_endpoint(&self) -> &str {
        self.inner.advertise_endpoint()
    }

    pub fn health(&self) -> HealthInfo {
        self.inner.health()
    }
}

#[derive(Clone)]
pub struct TsoDataPlane {
    inner: Arc<TsoService>,
}

impl TsoDataPlane {
    pub(crate) fn new(inner: Arc<TsoService>) -> Self {
        Self { inner }
    }

    pub async fn allocate_timestamps(
        &self,
        request: AllocateTimestampsRequest,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        self.allocate_timestamps_with_cancellation(request, None)
            .await
    }

    pub(crate) async fn allocate_timestamps_with_cancellation(
        &self,
        request: AllocateTimestampsRequest,
        cancellation: Option<RequestCancellation>,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        self.inner
            .allocate_timestamps_with_cancellation(request, cancellation)
            .await
    }

    pub fn health(&self) -> HealthInfo {
        self.inner.health()
    }

    pub fn max_timeline_proxy_lanes(&self) -> usize {
        self.inner.max_timeline_proxy_lanes()
    }
}
