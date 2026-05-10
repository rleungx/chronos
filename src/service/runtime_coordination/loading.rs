use std::future::Future;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};

use crate::metadata::{TimelineRecord, TimelineRouteRecord};
use crate::plane::RequestCancellation;
use crate::TsoError;

use super::super::TsoService;

#[derive(Clone, Default)]
pub(in crate::service) struct TimelineLoadCoordinator {
    flights: Arc<DashMap<String, Arc<TimelineLoadFlight>>>,
}

struct TimelineLoadFlight {
    result: Mutex<Option<TimelineLoadResult>>,
    notify: Notify,
}

struct TimelineLoadOwnerGuard<'a> {
    coordinator: &'a TimelineLoadCoordinator,
    key: String,
    flight: Arc<TimelineLoadFlight>,
    armed: bool,
}

impl<'a> TimelineLoadOwnerGuard<'a> {
    fn new(
        coordinator: &'a TimelineLoadCoordinator,
        key: String,
        flight: Arc<TimelineLoadFlight>,
    ) -> Self {
        Self {
            coordinator,
            key,
            flight,
            armed: true,
        }
    }

    fn finish(mut self) {
        self.coordinator
            .remove_flight_if_current(&self.key, &self.flight);
        self.flight.notify.notify_waiters();
        self.armed = false;
    }
}

impl Drop for TimelineLoadOwnerGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.coordinator
                .remove_flight_if_current(&self.key, &self.flight);
            self.flight.notify.notify_waiters();
        }
    }
}

#[derive(Clone)]
enum TimelineLoadValue {
    Full(Option<(TimelineRecord, u64)>),
    Route(Option<(TimelineRouteRecord, u64)>),
}

type TimelineLoadResult = Result<TimelineLoadValue, TsoError>;

impl TimelineLoadCoordinator {
    async fn load_once<F, Fut>(
        &self,
        key: String,
        cancellation: Option<RequestCancellation>,
        load: F,
    ) -> TimelineLoadResult
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = TimelineLoadResult>,
    {
        let mut load = Some(load);
        loop {
            if cancellation
                .as_ref()
                .is_some_and(RequestCancellation::is_cancelled)
            {
                return Err(TsoError::RequestCancelled);
            }
            let entry = self.flights.entry(key.clone());
            match entry {
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    let flight = Arc::new(TimelineLoadFlight {
                        result: Mutex::new(None),
                        notify: Notify::new(),
                    });
                    entry.insert(flight.clone());
                    let owner_guard =
                        TimelineLoadOwnerGuard::new(self, key.clone(), flight.clone());
                    let result =
                        load.take().expect("timeline load owner must retain loader")().await;
                    if !matches!(result, Err(TsoError::RequestCancelled)) {
                        *flight.result.lock().await = Some(result.clone());
                    }
                    owner_guard.finish();
                    return result;
                }
                dashmap::mapref::entry::Entry::Occupied(entry) => {
                    let flight = entry.get().clone();
                    drop(entry);
                    if let Some(result) = self
                        .wait_for_flight_result(&key, &flight, cancellation.clone())
                        .await?
                    {
                        return result;
                    }
                }
            }
        }
    }

    async fn wait_for_flight_result(
        &self,
        key: &str,
        flight: &Arc<TimelineLoadFlight>,
        cancellation: Option<RequestCancellation>,
    ) -> Result<Option<TimelineLoadResult>, TsoError> {
        loop {
            if let Some(result) = flight.result.lock().await.clone() {
                return Ok(Some(result));
            }
            if !self.flight_is_current(key, flight) {
                return Ok(None);
            }

            let notified = flight.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Some(result) = flight.result.lock().await.clone() {
                return Ok(Some(result));
            }
            if !self.flight_is_current(key, flight) {
                return Ok(None);
            }

            if let Some(cancellation) = cancellation.as_ref() {
                tokio::select! {
                    _ = &mut notified => continue,
                    _ = cancellation.cancelled() => return Err(TsoError::RequestCancelled),
                }
            } else {
                notified.await;
            }
        }
    }

    fn flight_is_current(&self, key: &str, flight: &Arc<TimelineLoadFlight>) -> bool {
        self.flights
            .get(key)
            .is_some_and(|entry| Arc::ptr_eq(entry.value(), flight))
    }

    fn remove_flight_if_current(&self, key: &str, flight: &Arc<TimelineLoadFlight>) {
        if let dashmap::mapref::entry::Entry::Occupied(entry) = self.flights.entry(key.to_owned()) {
            if Arc::ptr_eq(entry.get(), flight) {
                entry.remove();
            }
        }
    }
}

fn full_load_key(timeline_key: &str) -> String {
    format!("full:{timeline_key}")
}

fn route_load_key(timeline_key: &str) -> String {
    format!("route:{timeline_key}")
}

fn expect_full_load(value: TimelineLoadValue) -> Option<(TimelineRecord, u64)> {
    match value {
        TimelineLoadValue::Full(record) => record,
        TimelineLoadValue::Route(_) => unreachable!("full load key returned route result"),
    }
}

fn expect_route_load(value: TimelineLoadValue) -> Option<(TimelineRouteRecord, u64)> {
    match value {
        TimelineLoadValue::Route(record) => record,
        TimelineLoadValue::Full(_) => unreachable!("route load key returned full result"),
    }
}

impl TsoService {
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
        let value = self
            .timeline_load_coordinator
            .load_once(
                full_load_key(timeline_key),
                cancellation.clone(),
                || async {
                    let _permit = self.acquire_timeline_load_permit(cancellation).await?;
                    self.metadata
                        .load_timeline(timeline_key)
                        .await
                        .map(TimelineLoadValue::Full)
                },
            )
            .await?;
        Ok(expect_full_load(value))
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
        let value = self
            .timeline_load_coordinator
            .load_once(
                route_load_key(timeline_key),
                cancellation.clone(),
                || async {
                    let _permit = self.acquire_timeline_load_permit(cancellation).await?;
                    self.metadata
                        .load_timeline_route(timeline_key)
                        .await
                        .map(TimelineLoadValue::Route)
                },
            )
            .await?;
        Ok(expect_route_load(value))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tokio::sync::Notify;
    use tokio::time::{sleep, timeout, Duration};

    use crate::plane::RequestCancellation;
    use crate::TsoError;

    use super::{TimelineLoadCoordinator, TimelineLoadValue};

    #[tokio::test]
    async fn timeline_load_singleflight_shares_inflight_result() {
        let coordinator = TimelineLoadCoordinator::default();
        let load_entered = Arc::new(Notify::new());
        let release_load = Arc::new(Notify::new());
        let load_count = Arc::new(AtomicUsize::new(0));

        let owner = tokio::spawn({
            let coordinator = coordinator.clone();
            let load_entered = load_entered.clone();
            let release_load = release_load.clone();
            let load_count = load_count.clone();
            async move {
                coordinator
                    .load_once("timeline.load.shared".to_string(), None, || async move {
                        load_count.fetch_add(1, Ordering::AcqRel);
                        load_entered.notify_one();
                        release_load.notified().await;
                        Ok(TimelineLoadValue::Full(None))
                    })
                    .await
            }
        });
        load_entered.notified().await;

        let waiter = tokio::spawn({
            let coordinator = coordinator.clone();
            let load_count = load_count.clone();
            async move {
                coordinator
                    .load_once("timeline.load.shared".to_string(), None, || async move {
                        load_count.fetch_add(1, Ordering::AcqRel);
                        Ok(TimelineLoadValue::Full(None))
                    })
                    .await
            }
        });

        sleep(Duration::from_millis(25)).await;
        assert_eq!(load_count.load(Ordering::Acquire), 1);
        release_load.notify_waiters();
        owner.await.unwrap().unwrap();
        timeout(Duration::from_millis(100), waiter)
            .await
            .expect("waiter should reuse the owner result")
            .expect("waiter task should succeed")
            .expect("waiter should observe a successful load");
        assert_eq!(load_count.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn timeline_load_singleflight_waiter_respects_cancellation() {
        let coordinator = TimelineLoadCoordinator::default();
        let load_entered = Arc::new(Notify::new());
        let release_load = Arc::new(Notify::new());
        let owner = tokio::spawn({
            let coordinator = coordinator.clone();
            let load_entered = load_entered.clone();
            let release_load = release_load.clone();
            async move {
                coordinator
                    .load_once("timeline.load.cancelled".to_string(), None, || async move {
                        load_entered.notify_one();
                        release_load.notified().await;
                        Ok(TimelineLoadValue::Full(None))
                    })
                    .await
            }
        });
        load_entered.notified().await;
        let cancellation = RequestCancellation::new();

        let waiter_coordinator = coordinator.clone();
        let waiter_cancellation = cancellation.clone();
        let waiter = tokio::spawn(async move {
            waiter_coordinator
                .load_once(
                    "timeline.load.cancelled".to_string(),
                    Some(waiter_cancellation),
                    || async { Ok(TimelineLoadValue::Full(None)) },
                )
                .await
        });

        sleep(Duration::from_millis(25)).await;
        cancellation.cancel();

        let result = timeout(Duration::from_millis(100), waiter)
            .await
            .expect("cancelled waiter should finish")
            .expect("waiter task should succeed");
        assert!(matches!(result, Err(TsoError::RequestCancelled)));

        release_load.notify_waiters();
        owner.await.unwrap().unwrap();
        coordinator
            .load_once("timeline.load.cancelled".to_string(), None, || async {
                Ok(TimelineLoadValue::Full(None))
            })
            .await
            .expect("released flight should allow a new owner");
    }

    #[tokio::test]
    async fn timeline_load_singleflight_owner_drop_releases_waiters() {
        let coordinator = TimelineLoadCoordinator::default();
        let load_entered = Arc::new(Notify::new());
        let retry_load_count = Arc::new(AtomicUsize::new(0));

        let owner = tokio::spawn({
            let coordinator = coordinator.clone();
            let load_entered = load_entered.clone();
            async move {
                coordinator
                    .load_once(
                        "timeline.load.owner.dropped".to_string(),
                        None,
                        || async move {
                            load_entered.notify_one();
                            std::future::pending().await
                        },
                    )
                    .await
            }
        });
        load_entered.notified().await;

        let waiter = tokio::spawn({
            let coordinator = coordinator.clone();
            let retry_load_count = retry_load_count.clone();
            async move {
                coordinator
                    .load_once(
                        "timeline.load.owner.dropped".to_string(),
                        None,
                        || async move {
                            retry_load_count.fetch_add(1, Ordering::AcqRel);
                            Ok(TimelineLoadValue::Full(None))
                        },
                    )
                    .await
            }
        });

        sleep(Duration::from_millis(25)).await;
        assert_eq!(retry_load_count.load(Ordering::Acquire), 0);
        owner.abort();
        match owner.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("owner task should be aborted"),
        }

        timeout(Duration::from_millis(100), waiter)
            .await
            .expect("waiter should retry after owner future is dropped")
            .expect("waiter task should succeed")
            .expect("waiter should observe a successful retry");
        assert_eq!(retry_load_count.load(Ordering::Acquire), 1);
    }
}
