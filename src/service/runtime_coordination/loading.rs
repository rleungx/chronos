use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::watch;

use crate::metadata::{TimelineRecord, TimelineRouteRecord};
use crate::TsoError;

use super::super::TsoService;

#[derive(Clone, Default)]
pub(in crate::service) struct TimelineLoadCoordinator {
    flights: Arc<DashMap<String, Arc<watch::Sender<bool>>>>,
}

pub(in crate::service) struct TimelineLoadFlightGuard {
    key: String,
    flights: Arc<DashMap<String, Arc<watch::Sender<bool>>>>,
    completion: Arc<watch::Sender<bool>>,
}

impl Drop for TimelineLoadFlightGuard {
    fn drop(&mut self) {
        let _ = self.completion.send(true);
        self.flights.remove(&self.key);
    }
}

impl TimelineLoadCoordinator {
    async fn acquire(&self, timeline_key: &str) -> TimelineLoadFlightGuard {
        let key = timeline_key.to_owned();
        loop {
            let entry = self.flights.entry(key.clone());
            match entry {
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    let (completion, _rx) = watch::channel(false);
                    let completion = Arc::new(completion);
                    entry.insert(completion.clone());
                    return TimelineLoadFlightGuard {
                        key,
                        flights: self.flights.clone(),
                        completion,
                    };
                }
                dashmap::mapref::entry::Entry::Occupied(entry) => {
                    let mut completion_rx = entry.get().subscribe();
                    drop(entry);
                    if *completion_rx.borrow() {
                        continue;
                    }
                    let _ = completion_rx.changed().await;
                }
            }
        }
    }
}

impl TsoService {
    pub(in crate::service) async fn acquire_timeline_load_singleflight(
        &self,
        timeline_key: &str,
    ) -> TimelineLoadFlightGuard {
        self.timeline_load_coordinator.acquire(timeline_key).await
    }

    pub(in crate::service) async fn load_timeline_with_singleflight(
        &self,
        timeline_key: &str,
    ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
        self.load_timeline_with_singleflight_and_cancellation(timeline_key, None)
            .await
    }

    pub(in crate::service) async fn load_timeline_with_singleflight_and_cancellation(
        &self,
        timeline_key: &str,
        cancellation: Option<crate::plane::RequestCancellation>,
    ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
        // Singleflight only serializes one cold metadata load per timeline key. It does not retry
        // failed loads internally; callers observe the underlying error and decide whether to retry.
        let _flight = self.acquire_timeline_load_singleflight(timeline_key).await;
        let _permit = self.acquire_timeline_load_permit(cancellation).await?;
        self.metadata.load_timeline(timeline_key).await
    }

    pub(in crate::service) async fn load_timeline_route_with_singleflight(
        &self,
        timeline_key: &str,
    ) -> Result<Option<(TimelineRouteRecord, u64)>, TsoError> {
        self.load_timeline_route_with_singleflight_and_cancellation(timeline_key, None)
            .await
    }

    pub(in crate::service) async fn load_timeline_route_with_singleflight_and_cancellation(
        &self,
        timeline_key: &str,
        cancellation: Option<crate::plane::RequestCancellation>,
    ) -> Result<Option<(TimelineRouteRecord, u64)>, TsoError> {
        let _flight = self.acquire_timeline_load_singleflight(timeline_key).await;
        let _permit = self.acquire_timeline_load_permit(cancellation).await?;
        self.metadata.load_timeline_route(timeline_key).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use tokio::time::{sleep, timeout, Duration};

    use super::TimelineLoadCoordinator;

    #[tokio::test]
    async fn timeline_load_singleflight_waits_for_inflight_owner() {
        let coordinator = TimelineLoadCoordinator::default();
        let guard = coordinator.acquire("timeline.load.singleflight").await;
        let acquired = Arc::new(AtomicBool::new(false));

        let waiter_coordinator = coordinator.clone();
        let waiter_acquired = acquired.clone();
        let waiter = tokio::spawn(async move {
            let _guard = waiter_coordinator
                .acquire("timeline.load.singleflight")
                .await;
            waiter_acquired.store(true, Ordering::Release);
        });

        sleep(Duration::from_millis(25)).await;
        assert!(!acquired.load(Ordering::Acquire));

        drop(guard);
        timeout(Duration::from_millis(100), waiter)
            .await
            .expect("waiter should acquire once the inflight owner releases")
            .expect("waiter task should succeed");
        assert!(acquired.load(Ordering::Acquire));
    }
}
