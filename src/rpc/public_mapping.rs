use prost::Message;
use tonic::{Code, Status};

use crate::lifecycle::{TimelineLifecycleContract, TimelinePublicUnavailability};
use crate::proto::v1::{ErrorCode, ErrorDetail, OperatorActionBlocker, OperatorActionNextStep};
use crate::{TimelineLifecycleState, TsoError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimelineNotReadyPublicMapping {
    grpc_code: Code,
    detail_code: ErrorCode,
}

pub(super) fn status_with_error_detail(code: Code, err: TsoError) -> Status {
    let detail = build_error_detail(&err);
    let encoded_detail = detail.encode_to_vec();
    Status::with_details(
        code,
        err.to_string(),
        prost::bytes::Bytes::from(encoded_detail),
    )
}

pub(super) fn invalid_argument_status_with_detail(message: impl Into<String>) -> Status {
    let message = message.into();
    let detail = ErrorDetail {
        code: ErrorCode::InvalidArgument as i32,
        message: message.clone(),
        current_epoch: 0,
        current_route_version: 0,
        redirect_endpoint: String::new(),
        action_blocker: OperatorActionBlocker::Unspecified as i32,
        next_step: OperatorActionNextStep::Unspecified as i32,
    };
    Status::with_details(
        Code::InvalidArgument,
        message,
        prost::bytes::Bytes::from(detail.encode_to_vec()),
    )
}

pub(super) fn map_tso_error(err: TsoError) -> Status {
    match err {
        TsoError::TimelineNotFound { .. } => status_with_error_detail(Code::NotFound, err),
        TsoError::RouteVersionMismatch { .. } => {
            status_with_error_detail(Code::FailedPrecondition, err)
        }
        TsoError::EpochMismatch { .. } => status_with_error_detail(Code::FailedPrecondition, err),
        TsoError::LeaseExpired { .. } => status_with_error_detail(Code::Unavailable, err),
        TsoError::TimelineNotReady { state, .. } => {
            status_with_error_detail(timeline_not_ready_public_mapping(state).grpc_code, err)
        }
        TsoError::TimelineIngressSaturated { .. } => {
            status_with_error_detail(Code::Unavailable, err)
        }
        TsoError::TimelineRuntimeCacheSaturated { .. } => {
            status_with_error_detail(Code::Unavailable, err)
        }
        TsoError::FailoverRequiresExpiredLease { .. } => {
            status_with_error_detail(Code::FailedPrecondition, err)
        }
        TsoError::FailoverMissingRecoveryFloor { .. } => {
            status_with_error_detail(Code::FailedPrecondition, err)
        }
        TsoError::NotTimelineOwner { .. } => {
            status_with_error_detail(Code::FailedPrecondition, err)
        }
        TsoError::GeneratorLeaseExpired { .. } => status_with_error_detail(Code::Unavailable, err),
        TsoError::NotGeneratorOwner { .. }
        | TsoError::GeneratorNotOwnedByThisWorker { .. }
        | TsoError::GeneratorOwnershipMisconfigured { .. } => {
            status_with_error_detail(Code::FailedPrecondition, err)
        }
        TsoError::ClockBackwards { .. } => status_with_error_detail(Code::Internal, err),
        TsoError::BatchTooLarge { .. } | TsoError::InvalidCount => {
            status_with_error_detail(Code::InvalidArgument, err)
        }
        TsoError::TargetGeneratorIdRequired { .. } => {
            status_with_error_detail(Code::InvalidArgument, err)
        }
        TsoError::SharedGeneratorJumpAheadTooLarge { .. } => {
            status_with_error_detail(Code::FailedPrecondition, err)
        }
        TsoError::FutureBorrowExceeded { .. } => {
            status_with_error_detail(Code::ResourceExhausted, err)
        }
        TsoError::IssuedUpperBoundExceeded { .. } => {
            status_with_error_detail(Code::ResourceExhausted, err)
        }
        TsoError::GeneratorPoolExhausted => status_with_error_detail(Code::ResourceExhausted, err),
        TsoError::GeneratorIdOutOfRange { .. } => status_with_error_detail(Code::OutOfRange, err),
        TsoError::TsoOverflow => status_with_error_detail(Code::OutOfRange, err),
        TsoError::MetadataAlreadyExists => status_with_error_detail(Code::AlreadyExists, err),
        TsoError::CasFailed => status_with_error_detail(Code::Aborted, err),
        TsoError::RequestCancelled => status_with_error_detail(Code::DeadlineExceeded, err),
        TsoError::InstanceIdentityInUse { .. } => {
            status_with_error_detail(Code::AlreadyExists, err)
        }
        TsoError::Internal(_) => status_with_error_detail(Code::Internal, err),
    }
}

fn error_detail_code(err: &TsoError) -> ErrorCode {
    match err {
        TsoError::TimelineNotFound { .. } => ErrorCode::TimelineNotFound,
        TsoError::RouteVersionMismatch { .. } => ErrorCode::RouteVersionMismatch,
        TsoError::EpochMismatch { .. } => ErrorCode::EpochMismatch,
        TsoError::LeaseExpired { .. } | TsoError::GeneratorLeaseExpired { .. } => {
            ErrorCode::LeaseExpired
        }
        TsoError::NotTimelineOwner { .. }
        | TsoError::NotGeneratorOwner { .. }
        | TsoError::GeneratorNotOwnedByThisWorker { .. } => ErrorCode::NotTimelineOwner,
        TsoError::BatchTooLarge { .. }
        | TsoError::InvalidCount
        | TsoError::TargetGeneratorIdRequired { .. }
        | TsoError::SharedGeneratorJumpAheadTooLarge { .. }
        | TsoError::GeneratorOwnershipMisconfigured { .. }
        | TsoError::GeneratorIdOutOfRange { .. }
        | TsoError::TsoOverflow => ErrorCode::InvalidArgument,
        TsoError::FutureBorrowExceeded { .. }
        | TsoError::IssuedUpperBoundExceeded { .. }
        | TsoError::GeneratorPoolExhausted => ErrorCode::RateLimited,
        TsoError::FailoverRequiresExpiredLease { .. }
        | TsoError::CasFailed
        | TsoError::InstanceIdentityInUse { .. }
        | TsoError::FailoverMissingRecoveryFloor { .. }
        | TsoError::TimelineIngressSaturated { .. }
        | TsoError::TimelineRuntimeCacheSaturated { .. }
        | TsoError::RequestCancelled => ErrorCode::TemporarilyUnavailable,
        TsoError::TimelineNotReady { state, .. } => {
            timeline_not_ready_public_mapping(*state).detail_code
        }
        TsoError::ClockBackwards { .. }
        | TsoError::MetadataAlreadyExists
        | TsoError::Internal(_) => ErrorCode::Internal,
    }
}

fn timeline_not_ready_public_mapping(
    state: TimelineLifecycleState,
) -> TimelineNotReadyPublicMapping {
    match TimelineLifecycleContract::classify(state).direct_public_unavailability() {
        Some(TimelinePublicUnavailability::TemporarilyUnavailable) => {
            TimelineNotReadyPublicMapping {
                grpc_code: Code::Unavailable,
                detail_code: ErrorCode::TemporarilyUnavailable,
            }
        }
        None => {
            debug_assert!(
                false,
                "TimelineNotReady reached public mapping for non-direct-public state {state}"
            );
            TimelineNotReadyPublicMapping {
                grpc_code: Code::Unavailable,
                detail_code: ErrorCode::TemporarilyUnavailable,
            }
        }
    }
}

fn build_error_detail(err: &TsoError) -> ErrorDetail {
    let mut detail = ErrorDetail {
        code: error_detail_code(err) as i32,
        message: err.to_string(),
        current_epoch: 0,
        current_route_version: 0,
        redirect_endpoint: String::new(),
        action_blocker: OperatorActionBlocker::Unspecified as i32,
        next_step: OperatorActionNextStep::Unspecified as i32,
    };

    match err {
        TsoError::RouteVersionMismatch { actual, .. } => {
            detail.current_route_version = *actual;
        }
        TsoError::EpochMismatch { actual, .. } => {
            detail.current_epoch = *actual;
        }
        TsoError::NotTimelineOwner {
            owner_worker_endpoint,
        }
        | TsoError::NotGeneratorOwner {
            owner_worker_endpoint,
            ..
        } => {
            detail.redirect_endpoint = owner_worker_endpoint.clone();
        }
        TsoError::FailoverRequiresExpiredLease { .. } => {
            detail.action_blocker = OperatorActionBlocker::LeaseNotExpired as i32;
            detail.next_step = OperatorActionNextStep::WaitForLeaseExpiry as i32;
        }
        TsoError::FailoverMissingRecoveryFloor { .. } => {
            detail.action_blocker = OperatorActionBlocker::RecoveryFloorMissing as i32;
            detail.next_step = OperatorActionNextStep::PersistRecoveryFloor as i32;
        }
        _ => {}
    }

    detail
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::v1::{OperatorActionBlocker, OperatorActionNextStep};

    #[test]
    fn status_with_error_detail_encodes_expected_payload() {
        let status = status_with_error_detail(
            Code::FailedPrecondition,
            TsoError::NotTimelineOwner {
                owner_worker_endpoint: "worker-b:50051".into(),
            },
        );

        let detail = ErrorDetail::decode(status.details()).expect("error detail should decode");

        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(detail.code, ErrorCode::NotTimelineOwner as i32);
        assert_eq!(detail.redirect_endpoint, "worker-b:50051");
    }

    #[test]
    fn invalid_argument_status_with_detail_uses_structured_payload() {
        let status = invalid_argument_status_with_detail("page_token is malformed");
        let detail = ErrorDetail::decode(status.details()).expect("error detail should decode");

        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(detail.code, ErrorCode::InvalidArgument as i32);
        assert_eq!(detail.message, "page_token is malformed");
    }

    #[test]
    fn timeline_not_ready_public_mapping_follows_lifecycle_contract() {
        for state in [
            TimelineLifecycleState::Creating,
            TimelineLifecycleState::Draining,
            TimelineLifecycleState::Locked,
        ] {
            let mapping = timeline_not_ready_public_mapping(state);
            assert_eq!(mapping.grpc_code, Code::Unavailable);
            assert_eq!(mapping.detail_code, ErrorCode::TemporarilyUnavailable);
        }
    }

    #[test]
    fn failover_error_detail_maps_operator_blocker_and_next_step() {
        let cases = [
            (
                TsoError::FailoverRequiresExpiredLease {
                    timeline_key: "timeline-a".into(),
                    lease_expire_at_ms: 42,
                },
                OperatorActionBlocker::LeaseNotExpired,
                OperatorActionNextStep::WaitForLeaseExpiry,
            ),
            (
                TsoError::FailoverMissingRecoveryFloor {
                    timeline_key: "timeline-b".into(),
                    generator_id: 7,
                },
                OperatorActionBlocker::RecoveryFloorMissing,
                OperatorActionNextStep::PersistRecoveryFloor,
            ),
        ];

        for (error, blocker, next_step) in cases {
            let status = map_tso_error(error);
            let detail = ErrorDetail::decode(status.details()).expect("error detail should decode");

            assert_eq!(status.code(), Code::FailedPrecondition);
            assert_eq!(detail.code, ErrorCode::TemporarilyUnavailable as i32);
            assert_eq!(detail.action_blocker, blocker as i32);
            assert_eq!(detail.next_step, next_step as i32);
        }
    }

    #[test]
    fn public_error_mapping_table_stays_stable_for_key_variants() {
        let cases = [
            (
                TsoError::TimelineNotFound {
                    timeline_key: "timeline-a".into(),
                },
                Code::NotFound,
                ErrorCode::TimelineNotFound,
            ),
            (
                TsoError::LeaseExpired {
                    timeline_key: "timeline-a".into(),
                },
                Code::Unavailable,
                ErrorCode::LeaseExpired,
            ),
            (
                TsoError::TimelineIngressSaturated {
                    timeline_key: "timeline-a".into(),
                    max_lanes: 4,
                },
                Code::Unavailable,
                ErrorCode::TemporarilyUnavailable,
            ),
            (
                TsoError::TimelineRuntimeCacheSaturated {
                    timeline_key: "timeline-a".into(),
                    max_entries: 1,
                },
                Code::Unavailable,
                ErrorCode::TemporarilyUnavailable,
            ),
            (
                TsoError::NotGeneratorOwner {
                    generator_id: 7,
                    owner_worker_endpoint: "worker-b:50051".into(),
                },
                Code::FailedPrecondition,
                ErrorCode::NotTimelineOwner,
            ),
            (
                TsoError::FutureBorrowExceeded {
                    requested_physical_ms: 42,
                    allowed_physical_ms: 41,
                },
                Code::ResourceExhausted,
                ErrorCode::RateLimited,
            ),
            (
                TsoError::MetadataAlreadyExists,
                Code::AlreadyExists,
                ErrorCode::Internal,
            ),
            (
                TsoError::RequestCancelled,
                Code::DeadlineExceeded,
                ErrorCode::TemporarilyUnavailable,
            ),
            (
                TsoError::InstanceIdentityInUse {
                    instance_id: "instance-a".into(),
                },
                Code::AlreadyExists,
                ErrorCode::TemporarilyUnavailable,
            ),
        ];

        for (error, expected_grpc_code, expected_detail_code) in cases {
            let status = map_tso_error(error);
            let detail = ErrorDetail::decode(status.details()).expect("error detail should decode");

            assert_eq!(status.code(), expected_grpc_code);
            assert_eq!(detail.code, expected_detail_code as i32);
        }
    }
}
