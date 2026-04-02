use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::watch;

use super::super::TsoService;

#[derive(Clone, Default)]
pub(in crate::service) struct GeneratorLeaseCoordinator {
    flights: Arc<DashMap<u32, Arc<watch::Sender<bool>>>>,
}

pub(in crate::service) struct GeneratorLeaseFlightGuard {
    generator_id: u32,
    flights: Arc<DashMap<u32, Arc<watch::Sender<bool>>>>,
    completion: Arc<watch::Sender<bool>>,
}

impl Drop for GeneratorLeaseFlightGuard {
    fn drop(&mut self) {
        let _ = self.completion.send(true);
        self.flights.remove(&self.generator_id);
    }
}

impl GeneratorLeaseCoordinator {
    async fn acquire(&self, generator_id: u32) -> GeneratorLeaseFlightGuard {
        loop {
            let entry = self.flights.entry(generator_id);
            match entry {
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    let (completion, _rx) = watch::channel(false);
                    let completion = Arc::new(completion);
                    entry.insert(completion.clone());
                    return GeneratorLeaseFlightGuard {
                        generator_id,
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
    pub(in crate::service) async fn acquire_generator_lease_singleflight(
        &self,
        generator_id: u32,
    ) -> GeneratorLeaseFlightGuard {
        self.generator_lease_coordinator.acquire(generator_id).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use tokio::time::{sleep, timeout, Duration};

    use super::GeneratorLeaseCoordinator;

    #[tokio::test]
    async fn generator_lease_singleflight_waits_for_inflight_owner() {
        let coordinator = GeneratorLeaseCoordinator::default();
        let guard = coordinator.acquire(42).await;
        let acquired = Arc::new(AtomicBool::new(false));

        let waiter_coordinator = coordinator.clone();
        let waiter_acquired = acquired.clone();
        let waiter = tokio::spawn(async move {
            let _guard = waiter_coordinator.acquire(42).await;
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
