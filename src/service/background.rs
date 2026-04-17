use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::MutexGuard;
use std::sync::{Mutex as StdMutex, Weak};
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::recovery::record_recovery_event;

pub(super) struct BackgroundCoordinator {
    shutdown_tx: watch::Sender<bool>,
    tasks: StdMutex<Vec<JoinHandle<()>>>,
    shutdown_started: AtomicBool,
}

impl BackgroundCoordinator {
    fn tasks_lock(&self) -> MutexGuard<'_, Vec<JoinHandle<()>>> {
        match self.tasks.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event("service", "background_tasks_lock", "mutex_poisoned");
                poisoned.into_inner()
            }
        }
    }

    pub(super) fn new() -> Self {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        Self {
            shutdown_tx,
            tasks: StdMutex::new(Vec::new()),
            shutdown_started: AtomicBool::new(false),
        }
    }

    fn register_task(&self, handle: JoinHandle<()>) {
        self.tasks_lock().push(handle);
    }

    pub(super) fn shutdown_listener(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    pub(super) fn spawn_tracked<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.register_task(tokio::spawn(async move {
            if AssertUnwindSafe(task).catch_unwind().await.is_err() {
                record_recovery_event("service", "background_task", "panic");
            }
        }));
    }

    pub(super) fn begin_shutdown(&self) -> bool {
        if self.shutdown_started.swap(true, AtomicOrdering::AcqRel) {
            return false;
        }
        let _ = self.shutdown_tx.send(true);
        true
    }

    pub(super) async fn drain_tasks(&self) {
        let tasks = {
            let mut guard = self.tasks_lock();
            std::mem::take(&mut *guard)
        };
        for task in tasks {
            let _ = task.await;
        }
    }

    pub(super) fn abort_all(&mut self) {
        if !self.shutdown_started.swap(true, AtomicOrdering::AcqRel) {
            let _ = self.shutdown_tx.send(true);
        }
        let tasks = match self.tasks.get_mut() {
            Ok(tasks) => tasks,
            Err(poisoned) => {
                record_recovery_event("service", "background_tasks_abort", "mutex_poisoned");
                poisoned.into_inner()
            }
        };
        for task in tasks.drain(..) {
            task.abort();
        }
    }
}

use super::TsoService;

impl TsoService {
    pub(super) async fn background_generator_maintenance_loop(
        service: Weak<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        let Some(interval_ms) = service
            .upgrade()
            .map(|service| service.config.generator_maintenance_interval_ms)
        else {
            return;
        };
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => {
                    let Some(service) = service.upgrade() else {
                        break;
                    };
                    let now_ms = service.clock.now_ms();
                    if let Some(generator_id) = service.next_generator_with_ownership_drift(now_ms) {
                        if service.ensure_generator_lease(generator_id).await.is_err() {
                            record_recovery_event(
                                "service",
                                "background_generator_maintenance",
                                "drift_reacquire_failed",
                            );
                        }
                    }
                    let keys = service.generator_runtime.lease_keys();
                    service.refresh_generator_leases_batch(keys).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::Arc;
    use tokio::sync::Notify;

    use crate::metrics;

    use super::{AtomicBool, AtomicOrdering, BackgroundCoordinator, Duration};

    #[tokio::test]
    async fn background_coordinator_begin_shutdown_notifies_once() {
        let coordinator = BackgroundCoordinator::new();
        let mut shutdown_rx = coordinator.shutdown_listener();

        assert!(coordinator.begin_shutdown());
        shutdown_rx.changed().await.expect("shutdown should notify");
        assert!(*shutdown_rx.borrow());
        assert!(!coordinator.begin_shutdown());
    }

    #[tokio::test]
    async fn background_coordinator_drain_tasks_waits_for_registered_tasks() {
        let coordinator = BackgroundCoordinator::new();
        let completed = Arc::new(Notify::new());
        let completed_signal = completed.clone();

        coordinator.spawn_tracked(async move {
            completed_signal.notify_one();
        });

        coordinator.drain_tasks().await;
        completed.notified().await;
    }

    #[tokio::test]
    async fn background_coordinator_abort_all_aborts_registered_tasks() {
        let mut coordinator = BackgroundCoordinator::new();
        let mut shutdown_rx = coordinator.shutdown_listener();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_flag = completed.clone();

        coordinator.spawn_tracked(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            completed_flag.store(true, AtomicOrdering::Release);
        });

        coordinator.abort_all();
        shutdown_rx
            .changed()
            .await
            .expect("abort should notify shutdown");
        tokio::task::yield_now().await;
        assert!(!completed.load(AtomicOrdering::Acquire));
    }

    #[test]
    fn background_coordinator_recovers_from_poisoned_task_lock() {
        let mut coordinator = BackgroundCoordinator::new();
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = coordinator.tasks.lock().unwrap();
            panic!("poison task lock");
        }));

        let _guard = coordinator.tasks_lock();
        drop(_guard);
        coordinator.abort_all();
    }

    #[tokio::test]
    async fn spawn_tracked_records_recovery_event_when_task_panics() {
        let coordinator = BackgroundCoordinator::new();
        let before = metrics::TSO_RECOVERY_EVENTS_TOTAL
            .with_label_values(&["service", "background_task", "panic"])
            .get();

        coordinator.spawn_tracked(async move {
            panic!("background task panic should be recorded");
        });
        coordinator.drain_tasks().await;

        assert!(
            metrics::TSO_RECOVERY_EVENTS_TOTAL
                .with_label_values(&["service", "background_task", "panic"])
                .get()
                > before
        );
    }
}
