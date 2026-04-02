use std::error::Error;

use chronos::proto::v1::WorkerReadinessReason;
use chronos::rpc::HealthStatusHandle;
use chronos::TsoError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShutdownTrigger {
    IdentityLeaseLost,
    ProcessSignal(&'static str),
    CriticalServerFailed(&'static str),
}

impl ShutdownTrigger {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::IdentityLeaseLost => "identity_lease_lost",
            Self::ProcessSignal(signal) => signal,
            Self::CriticalServerFailed("grpc") => "critical_server_failed_grpc",
            Self::CriticalServerFailed("admin") => "critical_server_failed_admin",
            Self::CriticalServerFailed(_) => "critical_server_failed",
        }
    }

    pub(crate) fn readiness_reason(self) -> WorkerReadinessReason {
        match self {
            Self::IdentityLeaseLost => WorkerReadinessReason::IdentityLeaseLost,
            Self::ProcessSignal(_) | Self::CriticalServerFailed(_) => {
                WorkerReadinessReason::ShuttingDown
            }
        }
    }

    pub(crate) fn apply_to(self, health_status: &HealthStatusHandle) {
        match self.readiness_reason() {
            WorkerReadinessReason::IdentityLeaseLost => health_status.mark_identity_lease_lost(),
            WorkerReadinessReason::ShuttingDown => health_status.mark_shutting_down(),
            _ => unreachable!("shutdown trigger only maps to degraded running-server reasons"),
        }
    }
}

pub(crate) fn startup_preflight_failure_reason() -> WorkerReadinessReason {
    WorkerReadinessReason::StartupPreflightFailed
}

pub(crate) fn startup_bootstrap_failure_reason(
    error: &(dyn Error + 'static),
) -> WorkerReadinessReason {
    match error.downcast_ref::<TsoError>() {
        Some(TsoError::InstanceIdentityInUse { .. }) => {
            WorkerReadinessReason::IdentityLeaseAcquireFailed
        }
        Some(TsoError::ClusterContractMismatch { .. }) => {
            WorkerReadinessReason::ClusterContractMismatch
        }
        _ => WorkerReadinessReason::MetadataStartupProbeFailed,
    }
}
