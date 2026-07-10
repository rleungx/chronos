use super::*;

impl EtcdMetadataStore {
    pub(super) fn request_cleanup_index_entry(
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> RequestRecordCleanupIndexEntry {
        RequestRecordCleanupIndexEntry {
            timeline_key: timeline_key.to_owned(),
            client_request_id: client_request_id.to_owned(),
            updated_at_ms: record.updated_at_ms,
        }
    }

    pub(super) fn request_cleanup_index_put_op(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> Result<Option<TxnOp>, TsoError> {
        if !request_record_is_prunable_candidate(record) {
            return Ok(None);
        }
        let index_entry =
            Self::request_cleanup_index_entry(timeline_key, client_request_id, record);
        let index_value = Self::serialize_record(
            &index_entry,
            "request_cleanup_index",
            "Request cleanup index serialization",
        )?;
        Ok(Some(TxnOp::put(
            self.request_cleanup_index_key(record, timeline_key, client_request_id)
                .as_bytes(),
            index_value,
            None,
        )))
    }

    pub(super) fn request_cleanup_index_delete_op(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> Option<TxnOp> {
        request_record_is_prunable_candidate(record).then(|| {
            TxnOp::delete(
                self.request_cleanup_index_key(record, timeline_key, client_request_id)
                    .as_bytes(),
                None,
            )
        })
    }

    fn legacy_request_cleanup_index_delete_op(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> Option<TxnOp> {
        request_record_is_prunable_candidate(record).then(|| {
            TxnOp::delete(
                self.legacy_request_cleanup_index_key(record, timeline_key, client_request_id)
                    .as_bytes(),
                None,
            )
        })
    }

    pub(super) async fn create_request_record_with_index(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> Result<u64, TsoError> {
        let key = self.request_key(timeline_key, client_request_id);
        let value =
            Self::serialize_record(record, "create_request", "Request record serialization")?;
        let mut puts = vec![TxnOp::put(key.as_bytes(), value, None)];
        if let Some(index_put) =
            self.request_cleanup_index_put_op(timeline_key, client_request_id, record)?
        {
            puts.push(index_put);
        }

        let mut comparisons = vec![Compare::mod_revision(key.as_bytes(), CompareOp::Equal, 0)];
        if keys::legacy_request_key_is_unambiguous(timeline_key, client_request_id) {
            comparisons.push(Compare::mod_revision(
                self.legacy_request_key(timeline_key, client_request_id)
                    .as_bytes(),
                CompareOp::Equal,
                0,
            ));
        }

        let response = self
            .etcd_txn(
                "create_request",
                Txn::new().when(comparisons).and_then(puts),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["create_request"])
                    .inc();
                TsoError::Internal(format!("Etcd request create txn failed: {}", error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict("create_request", "already_exists");
            return Err(TsoError::MetadataAlreadyExists);
        }

        Self::extract_put_revision(
            response,
            "create_request",
            "invalid request create txn response",
        )
    }

    pub(super) async fn compare_exchange_request_record_with_index(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        expected_revision: u64,
        record: &RequestRecord,
    ) -> Result<u64, TsoError> {
        let legacy = expected_revision & LEGACY_REQUEST_REVISION_FLAG != 0;
        let expected_revision = expected_revision & !LEGACY_REQUEST_REVISION_FLAG;
        let previous_key = if legacy {
            self.legacy_request_key(timeline_key, client_request_id)
        } else {
            self.request_key(timeline_key, client_request_id)
        };
        let Some((previous_record, previous_revision)) = self
            .get_json_record::<RequestRecord>(
                previous_key.clone(),
                "get_request_for_cas",
                "Request record deserialization",
            )
            .await?
        else {
            record_metadata_conflict("cas_request", "not_found");
            return Err(TsoError::TimelineNotFound {
                timeline_key: format!("request:{timeline_key}:{client_request_id}"),
            });
        };
        if previous_revision != expected_revision {
            record_metadata_conflict("cas_request", "revision_mismatch");
            return Err(TsoError::CasFailed);
        }

        previous_record.validate_schema_version()?;
        let key = self.request_key(timeline_key, client_request_id);
        let value = Self::serialize_record(record, "cas_request", "Request record serialization")?;
        let mut ops = vec![TxnOp::put(key.as_bytes(), value, None)];
        let index_delete = if legacy {
            self.legacy_request_cleanup_index_delete_op(
                timeline_key,
                client_request_id,
                &previous_record,
            )
        } else {
            self.request_cleanup_index_delete_op(timeline_key, client_request_id, &previous_record)
        };
        if let Some(index_delete) = index_delete {
            ops.push(index_delete);
        }
        if legacy {
            ops.push(TxnOp::delete(previous_key.as_bytes(), None));
        }
        if let Some(index_put) =
            self.request_cleanup_index_put_op(timeline_key, client_request_id, record)?
        {
            ops.push(index_put);
        }

        let mut comparisons = vec![Compare::mod_revision(
            previous_key.as_bytes(),
            CompareOp::Equal,
            expected_revision as i64,
        )];
        if legacy {
            comparisons.push(Compare::mod_revision(key.as_bytes(), CompareOp::Equal, 0));
        }
        let response = self
            .etcd_txn("cas_request", Txn::new().when(comparisons).and_then(ops))
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_request"])
                    .inc();
                TsoError::Internal(format!("Etcd request CAS txn failed: {}", error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict("cas_request", "cas_failed");
            return Err(TsoError::CasFailed);
        }

        Self::extract_put_revision(response, "cas_request", "invalid request CAS txn response")
    }

    pub(super) async fn compare_delete_request_record_with_index(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        expected_revision: u64,
    ) -> Result<(), TsoError> {
        let legacy = expected_revision & LEGACY_REQUEST_REVISION_FLAG != 0;
        let expected_revision = expected_revision & !LEGACY_REQUEST_REVISION_FLAG;
        let key = if legacy {
            self.legacy_request_key(timeline_key, client_request_id)
        } else {
            self.request_key(timeline_key, client_request_id)
        };
        let Some((previous_record, previous_revision)) = self
            .get_json_record::<RequestRecord>(
                key.clone(),
                "get_request_for_delete",
                "Request record deserialization",
            )
            .await?
        else {
            record_metadata_conflict("delete_request", "not_found");
            return Err(TsoError::TimelineNotFound {
                timeline_key: format!("request:{timeline_key}:{client_request_id}"),
            });
        };
        if previous_revision != expected_revision {
            record_metadata_conflict("delete_request", "revision_mismatch");
            return Err(TsoError::CasFailed);
        }

        previous_record.validate_schema_version()?;
        let mut ops = vec![TxnOp::delete(key.as_bytes(), None)];
        let index_delete = if legacy {
            self.legacy_request_cleanup_index_delete_op(
                timeline_key,
                client_request_id,
                &previous_record,
            )
        } else {
            self.request_cleanup_index_delete_op(timeline_key, client_request_id, &previous_record)
        };
        if let Some(index_delete) = index_delete {
            ops.push(index_delete);
        }
        self.delete_request_record_txn(key, expected_revision, ops)
            .await
    }

    pub(super) async fn delete_request_record_txn(
        &self,
        key: String,
        expected_revision: u64,
        ops: Vec<TxnOp>,
    ) -> Result<(), TsoError> {
        let response = self
            .etcd_txn(
                "delete_request",
                Txn::new()
                    .when(vec![Compare::mod_revision(
                        key.as_bytes(),
                        CompareOp::Equal,
                        expected_revision as i64,
                    )])
                    .and_then(ops),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["delete_request"])
                    .inc();
                TsoError::Internal(format!("Etcd request delete txn failed: {}", error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict("delete_request", "cas_failed");
            return Err(TsoError::CasFailed);
        }

        Ok(())
    }
}
