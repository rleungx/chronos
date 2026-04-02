use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::proto::v1::WorkerReadinessReason as ProtoWorkerReadinessReason;
use crate::proto::v1::WorkerReadinessState as ProtoWorkerReadinessState;
use crate::recovery::record_recovery_event;
use crate::status::{WorkerStatusIdentity, WorkerStatusSnapshot};

use super::status_mapping;

#[derive(Clone)]
pub struct HealthStatusHandle {
    inner: Arc<RwLock<WorkerStatusSnapshot>>,
}

impl HealthStatusHandle {
    fn read_snapshot(&self) -> RwLockReadGuard<'_, WorkerStatusSnapshot> {
        match self.inner.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event("rpc", "health_status_read_lock", "rwlock_poisoned");
                poisoned.into_inner()
            }
        }
    }

    fn write_snapshot(&self) -> RwLockWriteGuard<'_, WorkerStatusSnapshot> {
        match self.inner.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event("rpc", "health_status_write_lock", "rwlock_poisoned");
                poisoned.into_inner()
            }
        }
    }

    pub fn serving(health_info: &crate::HealthInfo) -> Self {
        let snapshot =
            WorkerStatusSnapshot::serving(WorkerStatusIdentity::from_health_info(health_info));
        Self {
            inner: Arc::new(RwLock::new(snapshot)),
        }
    }

    pub fn mark_shutting_down(&self) {
        self.write_snapshot().mark_shutting_down();
    }

    pub fn mark_identity_lease_lost(&self) {
        self.write_snapshot().mark_identity_lease_lost();
    }

    pub fn mark_ownership_drift(&self) {
        self.write_snapshot().mark_ownership_drift();
    }

    pub fn clear_ownership_drift(&self) {
        self.write_snapshot().clear_ownership_drift();
    }

    pub fn readiness_state(&self) -> ProtoWorkerReadinessState {
        status_mapping::proto_worker_readiness_state(self.read_snapshot().readiness_state)
    }

    pub fn readiness_reason(&self) -> ProtoWorkerReadinessReason {
        status_mapping::proto_worker_readiness_reason(self.read_snapshot().readiness_reason)
    }

    pub fn identity_lease_healthy(&self) -> bool {
        self.read_snapshot().identity_lease_healthy
    }

    pub(super) fn snapshot(&self) -> WorkerStatusSnapshot {
        self.read_snapshot().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics;

    #[test]
    fn health_status_handle_recovers_from_poisoned_lock_and_records_metric() {
        let handle = HealthStatusHandle::serving(&crate::HealthInfo {
            generator_count: 1,
            timeline_count: 1,
            worker_id: "worker-a".into(),
            instance_id: "instance-a".into(),
            advertise_endpoint: "worker-a:50051".into(),
        });
        let read_before = metrics::TSO_RECOVERY_EVENTS_TOTAL
            .with_label_values(&["rpc", "health_status_read_lock", "rwlock_poisoned"])
            .get();
        let write_before = metrics::TSO_RECOVERY_EVENTS_TOTAL
            .with_label_values(&["rpc", "health_status_write_lock", "rwlock_poisoned"])
            .get();

        let inner = handle.inner.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = inner.write().unwrap();
            panic!("poison health status lock");
        }));

        handle.mark_shutting_down();

        assert_eq!(
            handle.readiness_state(),
            ProtoWorkerReadinessState::Degraded
        );
        assert_eq!(
            handle.readiness_reason(),
            ProtoWorkerReadinessReason::ShuttingDown
        );
        assert!(
            metrics::TSO_RECOVERY_EVENTS_TOTAL
                .with_label_values(&["rpc", "health_status_write_lock", "rwlock_poisoned"])
                .get()
                > write_before
        );
        assert!(
            metrics::TSO_RECOVERY_EVENTS_TOTAL
                .with_label_values(&["rpc", "health_status_read_lock", "rwlock_poisoned"])
                .get()
                > read_before
        );
    }

    #[test]
    fn health_status_handle_emits_ownership_drift_reason() {
        let handle = HealthStatusHandle::serving(&crate::HealthInfo {
            generator_count: 1,
            timeline_count: 1,
            worker_id: "worker-a".into(),
            instance_id: "instance-a".into(),
            advertise_endpoint: "worker-a:50051".into(),
        });

        handle.mark_ownership_drift();

        assert_eq!(
            handle.readiness_state(),
            ProtoWorkerReadinessState::Degraded
        );
        assert_eq!(
            handle.readiness_reason(),
            ProtoWorkerReadinessReason::OwnershipDrift
        );

        handle.clear_ownership_drift();

        assert_eq!(handle.readiness_state(), ProtoWorkerReadinessState::Ready);
        assert_eq!(
            handle.readiness_reason(),
            ProtoWorkerReadinessReason::Serving
        );
    }
}
