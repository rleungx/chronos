use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use crate::recovery::record_recovery_event;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipDriftEvidence {
    pub generator_id: u32,
    pub contending_instance_id: String,
    pub lease_expire_at_ms: u64,
    pub observed_at_ms: u64,
}

pub trait WorkerReadinessSink: Send + Sync {
    fn ownership_drift_started(&self, evidence: OwnershipDriftEvidence);
    fn ownership_drift_cleared(&self);
}

#[derive(Debug, Clone)]
struct OwnershipDriftEntry {
    contending_instance_id: String,
    last_lease_expire_at_ms: u64,
    active: bool,
    next_retry_after_ms: u64,
}

#[derive(Default)]
pub(super) struct OwnershipDriftTracker {
    entries: Mutex<HashMap<u32, OwnershipDriftEntry>>,
    sink: OnceLock<Arc<dyn WorkerReadinessSink>>,
}

enum SinkAction {
    Started(OwnershipDriftEvidence),
    Cleared,
}

impl OwnershipDriftTracker {
    pub(super) fn set_sink(&self, sink: Arc<dyn WorkerReadinessSink>) {
        let _ = self.sink.set(sink);
    }

    pub(super) fn observe_contended_local_generator(
        &self,
        generator_id: u32,
        contending_instance_id: &str,
        lease_expire_at_ms: u64,
        observed_at_ms: u64,
    ) {
        let sink_action = {
            let mut entries = self.entries_lock();
            let mut sink_action = None;
            let entry = entries
                .entry(generator_id)
                .or_insert_with(|| OwnershipDriftEntry {
                    contending_instance_id: contending_instance_id.to_owned(),
                    last_lease_expire_at_ms: lease_expire_at_ms,
                    active: false,
                    next_retry_after_ms: observed_at_ms,
                });

            let renewed = entry.contending_instance_id == contending_instance_id
                && lease_expire_at_ms > entry.last_lease_expire_at_ms;
            entry.contending_instance_id = contending_instance_id.to_owned();
            entry.last_lease_expire_at_ms = lease_expire_at_ms;

            if renewed && !entry.active {
                entry.active = true;
                sink_action = Some(SinkAction::Started(OwnershipDriftEvidence {
                    generator_id,
                    contending_instance_id: contending_instance_id.to_owned(),
                    lease_expire_at_ms,
                    observed_at_ms,
                }));
            }

            sink_action
        };

        self.dispatch_sink_action(sink_action);
    }

    pub(super) fn clear_generator(&self, generator_id: u32) {
        let sink_action = {
            let mut entries = self.entries_lock();
            let removed = entries.remove(&generator_id);
            let should_clear = removed.as_ref().is_some_and(|entry| entry.active)
                && entries.values().all(|entry| !entry.active);
            should_clear.then_some(SinkAction::Cleared)
        };

        self.dispatch_sink_action(sink_action);
    }

    pub(super) fn next_generator_to_retry(
        &self,
        now_ms: u64,
        retry_interval_ms: u64,
    ) -> Option<u32> {
        let mut entries = self.entries_lock();
        let mut selected = None;
        for (generator_id, entry) in entries.iter_mut() {
            if entry.active && entry.next_retry_after_ms <= now_ms {
                entry.next_retry_after_ms = now_ms.saturating_add(retry_interval_ms.max(1));
                selected = Some(*generator_id);
                break;
            }
        }
        selected
    }

    #[cfg(test)]
    pub(super) fn is_active(&self, generator_id: u32) -> bool {
        self.entries_lock()
            .get(&generator_id)
            .is_some_and(|entry| entry.active)
    }

    fn entries_lock(&self) -> MutexGuard<'_, HashMap<u32, OwnershipDriftEntry>> {
        match self.entries.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event("service", "ownership_drift_entries", "mutex_poisoned");
                poisoned.into_inner()
            }
        }
    }

    fn dispatch_sink_action(&self, sink_action: Option<SinkAction>) {
        let Some(action) = sink_action else {
            return;
        };
        let Some(sink) = self.sink.get() else {
            return;
        };

        match action {
            SinkAction::Started(evidence) => sink.ownership_drift_started(evidence),
            SinkAction::Cleared => sink.ownership_drift_cleared(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::Mutex as StdMutex;
    use std::thread;

    use super::*;

    #[derive(Default)]
    struct TestSink {
        started: StdMutex<Vec<OwnershipDriftEvidence>>,
        cleared: StdMutex<u32>,
        events: StdMutex<Vec<&'static str>>,
    }

    impl WorkerReadinessSink for TestSink {
        fn ownership_drift_started(&self, evidence: OwnershipDriftEvidence) {
            self.started.lock().unwrap().push(evidence);
            self.events.lock().unwrap().push("started");
        }

        fn ownership_drift_cleared(&self) {
            *self.cleared.lock().unwrap() += 1;
            self.events.lock().unwrap().push("cleared");
        }
    }

    #[test]
    fn ownership_drift_only_activates_after_renewal() {
        let tracker = OwnershipDriftTracker::default();
        let sink = Arc::new(TestSink::default());
        tracker.set_sink(sink.clone());

        tracker.observe_contended_local_generator(7, "instance-b", 110, 100);
        assert!(!tracker.is_active(7));
        assert!(sink.started.lock().unwrap().is_empty());

        tracker.observe_contended_local_generator(7, "instance-b", 120, 105);
        assert!(tracker.is_active(7));
        assert_eq!(sink.started.lock().unwrap().len(), 1);
    }

    #[test]
    fn clearing_last_active_drift_notifies_sink_once() {
        let tracker = OwnershipDriftTracker::default();
        let sink = Arc::new(TestSink::default());
        tracker.set_sink(sink.clone());

        tracker.observe_contended_local_generator(7, "instance-b", 110, 100);
        tracker.observe_contended_local_generator(7, "instance-b", 120, 105);
        tracker.clear_generator(7);

        assert_eq!(*sink.cleared.lock().unwrap(), 1);
        assert!(!tracker.is_active(7));
    }

    #[test]
    fn clearing_never_reports_cleared_while_another_generator_is_active() {
        let tracker = Arc::new(OwnershipDriftTracker::default());
        let sink = Arc::new(TestSink::default());
        tracker.set_sink(sink.clone());
        tracker.observe_contended_local_generator(7, "instance-b", 110, 100);
        tracker.observe_contended_local_generator(7, "instance-b", 120, 105);

        let start = Arc::new(Barrier::new(2));
        let clear_tracker = tracker.clone();
        let clear_start = start.clone();
        let clearer = thread::spawn(move || {
            clear_start.wait();
            clear_tracker.clear_generator(7);
        });

        let observe_tracker = tracker.clone();
        let observe_start = start.clone();
        let observer = thread::spawn(move || {
            observe_start.wait();
            observe_tracker.observe_contended_local_generator(8, "instance-c", 130, 110);
            observe_tracker.observe_contended_local_generator(8, "instance-c", 140, 115);
        });

        clearer.join().unwrap();
        observer.join().unwrap();

        assert!(tracker.is_active(8));
        let events = sink.events.lock().unwrap();
        assert_eq!(events.last().copied(), Some("started"));
    }

    struct ReentrantSink {
        tracker: Arc<OwnershipDriftTracker>,
    }

    impl WorkerReadinessSink for ReentrantSink {
        fn ownership_drift_started(&self, evidence: OwnershipDriftEvidence) {
            assert_eq!(
                self.tracker
                    .next_generator_to_retry(evidence.observed_at_ms, 10),
                Some(evidence.generator_id)
            );
        }

        fn ownership_drift_cleared(&self) {}
    }

    #[test]
    fn sink_callbacks_run_after_releasing_entries_lock() {
        let tracker = Arc::new(OwnershipDriftTracker::default());
        tracker.set_sink(Arc::new(ReentrantSink {
            tracker: tracker.clone(),
        }));

        tracker.observe_contended_local_generator(7, "instance-b", 110, 100);
        tracker.observe_contended_local_generator(7, "instance-b", 120, 105);
        assert!(tracker.is_active(7));
    }

    #[test]
    fn entries_lock_recovers_after_poison() {
        let tracker = OwnershipDriftTracker::default();
        let poisoned = std::panic::catch_unwind(|| {
            let _guard = tracker.entries.lock().unwrap();
            panic!("poison ownership drift entries");
        });
        assert!(poisoned.is_err());

        tracker.observe_contended_local_generator(7, "instance-b", 110, 100);
        tracker.observe_contended_local_generator(7, "instance-b", 120, 105);
        assert!(tracker.is_active(7));
    }
}
