use crate::metadata::{
    AllocationRequestFingerprint, AllocationResponseRecord, RequestRecord, RequestRecordState,
};
use crate::recovery::record_recovery_event;
use crate::{metrics, AllocateTimestampsRequest, AllocateTimestampsResponse, TsoError};

use super::TsoService;

pub(super) struct PendingIdempotentAllocation {
    revision: u64,
    fingerprint: AllocationRequestFingerprint,
}

pub(super) enum IdempotentAllocationPreparation {
    Disabled,
    Replay(AllocateTimestampsResponse),
    Pending(PendingIdempotentAllocation),
}

impl TsoService {
    fn idempotency_enabled(request: &AllocateTimestampsRequest) -> bool {
        !request.client_request_id.trim().is_empty()
    }

    fn allocation_request_fingerprint(
        request: &AllocateTimestampsRequest,
    ) -> AllocationRequestFingerprint {
        AllocationRequestFingerprint {
            count: request.count,
        }
    }

    fn pending_request_record(
        fingerprint: AllocationRequestFingerprint,
        updated_at_ms: u64,
    ) -> RequestRecord {
        RequestRecord {
            schema_version: 1,
            fingerprint,
            state: RequestRecordState::Pending,
            response: None,
            updated_at_ms,
        }
    }

    fn completed_request_record(
        fingerprint: AllocationRequestFingerprint,
        response: &AllocateTimestampsResponse,
        updated_at_ms: u64,
    ) -> RequestRecord {
        RequestRecord {
            schema_version: 1,
            fingerprint,
            state: RequestRecordState::Completed,
            response: Some(AllocationResponseRecord {
                generator_id: response.generator_id,
                epoch: response.epoch,
                route_version: response.route_version,
                ranges: response.ranges.clone(),
            }),
            updated_at_ms,
        }
    }

    fn record_idempotency_outcome(outcome: &'static str) {
        metrics::TSO_REQUEST_IDEMPOTENCY_TOTAL
            .with_label_values(&[outcome])
            .inc();
    }

    fn pending_record_is_stale(&self, record: &RequestRecord, now_ms: u64) -> bool {
        record.state == RequestRecordState::Pending
            && now_ms.saturating_sub(record.updated_at_ms)
                >= self.config.request_record_pending_timeout_ms
    }

    fn validate_idempotent_record_fingerprint(
        request: &AllocateTimestampsRequest,
        record: &RequestRecord,
    ) -> Result<(), TsoError> {
        if record.fingerprint == Self::allocation_request_fingerprint(request) {
            return Ok(());
        }

        Self::record_idempotency_outcome("conflict");
        Err(TsoError::ClientRequestConflict {
            timeline_key: request.timeline_key.clone(),
            client_request_id: request.client_request_id.clone(),
        })
    }

    pub(super) async fn prepare_idempotent_allocation(
        &self,
        request: &AllocateTimestampsRequest,
    ) -> Result<IdempotentAllocationPreparation, TsoError> {
        if !Self::idempotency_enabled(request) {
            Self::record_idempotency_outcome("disabled");
            return Ok(IdempotentAllocationPreparation::Disabled);
        }
        let Some(request_records) = self.metadata.request_records() else {
            Self::record_idempotency_outcome("unsupported");
            return Ok(IdempotentAllocationPreparation::Disabled);
        };

        let fingerprint = Self::allocation_request_fingerprint(request);
        loop {
            let now_ms = self.clock.now_ms();
            let pending_record = Self::pending_request_record(fingerprint.clone(), now_ms);
            match request_records
                .create_request_record(
                    &request.timeline_key,
                    &request.client_request_id,
                    &pending_record,
                )
                .await
            {
                Ok(revision) => {
                    Self::record_idempotency_outcome("pending_created");
                    return Ok(IdempotentAllocationPreparation::Pending(
                        PendingIdempotentAllocation {
                            revision,
                            fingerprint,
                        },
                    ));
                }
                Err(TsoError::MetadataAlreadyExists) => {}
                Err(error) => {
                    Self::record_idempotency_outcome("prepare_error");
                    return Err(error);
                }
            }

            let Some((record, revision)) = request_records
                .load_request_record(&request.timeline_key, &request.client_request_id)
                .await?
            else {
                continue;
            };
            Self::validate_idempotent_record_fingerprint(request, &record)?;

            if let Some(response) = record.completed_response(&request.timeline_key)? {
                Self::record_idempotency_outcome("replay");
                return Ok(IdempotentAllocationPreparation::Replay(response));
            }

            if self.pending_record_is_stale(&record, now_ms) {
                match request_records
                    .compare_delete_request_record(
                        &request.timeline_key,
                        &request.client_request_id,
                        revision,
                    )
                    .await
                {
                    Ok(()) => {
                        Self::record_idempotency_outcome("stale_pending_deleted");
                        continue;
                    }
                    Err(TsoError::CasFailed) | Err(TsoError::TimelineNotFound { .. }) => {
                        continue;
                    }
                    Err(error) => {
                        Self::record_idempotency_outcome("stale_pending_delete_error");
                        return Err(error);
                    }
                }
            }

            Self::record_idempotency_outcome("pending_in_progress");
            return Err(TsoError::ClientRequestInProgress {
                timeline_key: request.timeline_key.clone(),
                client_request_id: request.client_request_id.clone(),
            });
        }
    }

    pub(super) async fn finish_idempotent_allocation(
        &self,
        request: &AllocateTimestampsRequest,
        preparation: IdempotentAllocationPreparation,
        allocation_result: Result<AllocateTimestampsResponse, TsoError>,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        let IdempotentAllocationPreparation::Pending(pending) = preparation else {
            return allocation_result;
        };
        let Some(request_records) = self.metadata.request_records() else {
            return allocation_result;
        };

        let response = match allocation_result {
            Ok(response) => response,
            Err(error) => {
                if request_records
                    .compare_delete_request_record(
                        &request.timeline_key,
                        &request.client_request_id,
                        pending.revision,
                    )
                    .await
                    .is_err()
                {
                    record_recovery_event(
                        "service",
                        "request_record_pending_cleanup",
                        "metadata_error",
                    );
                    Self::record_idempotency_outcome("pending_cleanup_error");
                } else {
                    Self::record_idempotency_outcome("pending_cleaned");
                }
                return Err(error);
            }
        };

        let completed_record = Self::completed_request_record(
            pending.fingerprint.clone(),
            &response,
            self.clock.now_ms(),
        );

        match request_records
            .compare_exchange_request_record(
                &request.timeline_key,
                &request.client_request_id,
                pending.revision,
                &completed_record,
            )
            .await
        {
            Ok(_) => {
                Self::record_idempotency_outcome("completed");
                Ok(response)
            }
            Err(TsoError::CasFailed) | Err(TsoError::TimelineNotFound { .. }) => {
                self.recover_idempotent_completion_after_cas_failure(
                    request,
                    completed_record,
                    response,
                )
                .await
            }
            Err(error) => {
                Self::record_idempotency_outcome("complete_error");
                record_recovery_event("service", "request_record_complete", "metadata_error");
                Err(error)
            }
        }
    }

    async fn recover_idempotent_completion_after_cas_failure(
        &self,
        request: &AllocateTimestampsRequest,
        completed_record: RequestRecord,
        response: AllocateTimestampsResponse,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        let Some(request_records) = self.metadata.request_records() else {
            return Ok(response);
        };

        match request_records
            .load_request_record(&request.timeline_key, &request.client_request_id)
            .await?
        {
            Some((record, revision)) => {
                Self::validate_idempotent_record_fingerprint(request, &record)?;
                if let Some(existing_response) = record.completed_response(&request.timeline_key)? {
                    Self::record_idempotency_outcome("complete_race_replay");
                    return Ok(existing_response);
                }
                match request_records
                    .compare_exchange_request_record(
                        &request.timeline_key,
                        &request.client_request_id,
                        revision,
                        &completed_record,
                    )
                    .await
                {
                    Ok(_) => {
                        Self::record_idempotency_outcome("complete_race_repaired");
                        Ok(response)
                    }
                    Err(TsoError::CasFailed) => {
                        Self::record_idempotency_outcome("complete_race_lost");
                        Err(TsoError::ClientRequestInProgress {
                            timeline_key: request.timeline_key.clone(),
                            client_request_id: request.client_request_id.clone(),
                        })
                    }
                    Err(error) => {
                        Self::record_idempotency_outcome("complete_repair_error");
                        Err(error)
                    }
                }
            }
            None => match request_records
                .create_request_record(
                    &request.timeline_key,
                    &request.client_request_id,
                    &completed_record,
                )
                .await
            {
                Ok(_) => {
                    Self::record_idempotency_outcome("complete_missing_recreated");
                    Ok(response)
                }
                Err(TsoError::MetadataAlreadyExists) => Err(TsoError::ClientRequestInProgress {
                    timeline_key: request.timeline_key.clone(),
                    client_request_id: request.client_request_id.clone(),
                }),
                Err(error) => {
                    Self::record_idempotency_outcome("complete_recreate_error");
                    Err(error)
                }
            },
        }
    }
}
