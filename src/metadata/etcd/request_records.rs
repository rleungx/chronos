use async_trait::async_trait;

use super::*;

#[async_trait]
impl RequestRecordAuthority for EtcdMetadataStore {
    async fn load_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
    ) -> Result<Option<(RequestRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get_request"])
            .start_timer();
        let result: Option<(RequestRecord, u64)> = self
            .get_json_record(
                self.request_key(timeline_key, client_request_id),
                "get_request",
                "Request record deserialization",
            )
            .await?;
        result
            .map(|(record, revision)| {
                record.validate_schema_version()?;
                Ok((record, revision))
            })
            .transpose()
    }

    async fn create_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["create_request"])
            .start_timer();
        record.validate_schema_version()?;
        let stamped = record.stamped_for_persistence();
        self.create_request_record_with_index(timeline_key, client_request_id, &stamped)
            .await
    }

    async fn compare_exchange_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        expected_revision: u64,
        record: &RequestRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_request"])
            .start_timer();
        record.validate_schema_version()?;
        let stamped = record.stamped_for_persistence();
        self.compare_exchange_request_record_with_index(
            timeline_key,
            client_request_id,
            expected_revision,
            &stamped,
        )
        .await
    }

    async fn compare_delete_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        expected_revision: u64,
    ) -> Result<(), TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["delete_request"])
            .start_timer();
        self.compare_delete_request_record_with_index(
            timeline_key,
            client_request_id,
            expected_revision,
        )
        .await
    }

    async fn prune_completed_request_records(
        &self,
        older_than_ms: u64,
        limit: usize,
    ) -> Result<usize, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["prune_requests"])
            .start_timer();
        if limit == 0 {
            return Ok(0);
        }

        let mut pruned = 0;
        let index_prefix = self.request_cleanup_index_prefix();
        let index_range_end = self.request_cleanup_index_cutoff(older_than_ms);
        let mut index_start_key = index_prefix.into_bytes();
        let index_fetch_limit = request_record_prune_fetch_limit(limit);

        loop {
            let response = self
                .etcd_get(
                    "prune_requests",
                    index_start_key.clone(),
                    Some(
                        GetOptions::new()
                            .with_range(index_range_end.as_bytes())
                            .with_limit(index_fetch_limit),
                    ),
                )
                .await
                .map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["prune_requests"])
                        .inc();
                    TsoError::Internal(format!("Etcd request cleanup index scan failed: {}", error))
                })?;

            let kvs = response.kvs();
            let Some(last_index_key) = kvs.last().map(|kv| kv.key().to_vec()) else {
                break;
            };

            for kv in kvs {
                if pruned >= limit {
                    return Ok(pruned);
                }

                let index_entry: RequestRecordCleanupIndexEntry =
                    serde_json::from_slice(kv.value()).map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["prune_requests"])
                            .inc();
                        TsoError::Internal(format!(
                            "Request cleanup index deserialization failed: {}",
                            error
                        ))
                    })?;
                let index_key = String::from_utf8(kv.key().to_vec()).map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["prune_requests"])
                        .inc();
                    TsoError::Internal(format!(
                        "Request cleanup index key decode failed: {}",
                        error
                    ))
                })?;
                let request_key =
                    self.request_key(&index_entry.timeline_key, &index_entry.client_request_id);
                let Some((record, request_revision)) = self
                    .load_request_record(&index_entry.timeline_key, &index_entry.client_request_id)
                    .await?
                else {
                    match self
                        .delete_json_record(
                            index_key,
                            kv.mod_revision() as u64,
                            JsonTxnContext {
                                op_label: "prune_requests",
                                serialize_context: "Request cleanup index serialization",
                                txn_context: "Etcd request cleanup index delete txn failed",
                                invalid_response_context:
                                    "invalid request cleanup index delete txn response",
                            },
                        )
                        .await
                    {
                        Ok(()) | Err(TsoError::CasFailed) => {}
                        Err(error) => return Err(error),
                    }
                    continue;
                };

                if record.updated_at_ms != index_entry.updated_at_ms
                    || !request_record_is_prunable(&record, older_than_ms)
                {
                    match self
                        .delete_json_record(
                            index_key,
                            kv.mod_revision() as u64,
                            JsonTxnContext {
                                op_label: "prune_requests",
                                serialize_context: "Request cleanup index serialization",
                                txn_context: "Etcd stale request cleanup index delete txn failed",
                                invalid_response_context:
                                    "invalid stale request cleanup index delete txn response",
                            },
                        )
                        .await
                    {
                        Ok(()) | Err(TsoError::CasFailed) => {}
                        Err(error) => return Err(error),
                    }
                    continue;
                }

                let response = self
                    .etcd_txn(
                        "prune_requests",
                        Txn::new()
                            .when(vec![
                                Compare::mod_revision(
                                    request_key.as_bytes(),
                                    CompareOp::Equal,
                                    request_revision as i64,
                                ),
                                Compare::mod_revision(
                                    index_key.as_bytes(),
                                    CompareOp::Equal,
                                    kv.mod_revision(),
                                ),
                            ])
                            .and_then(vec![
                                TxnOp::delete(request_key.as_bytes(), None),
                                TxnOp::delete(index_key.as_bytes(), None),
                            ]),
                    )
                    .await
                    .map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["prune_requests"])
                            .inc();
                        TsoError::Internal(format!("Etcd request prune txn failed: {}", error))
                    })?;
                if response.succeeded() {
                    pruned += 1;
                }
            }

            index_start_key = next_etcd_key_after(&last_index_key);
            if index_start_key.as_slice() >= index_range_end.as_bytes() {
                break;
            }
        }

        if pruned >= limit {
            return Ok(pruned);
        }

        let request_prefix = self.request_prefix();
        let range_end = prefix_range_end(&request_prefix);
        let mut start_key = request_prefix.into_bytes();
        let fetch_limit = request_record_prune_fetch_limit(limit - pruned);

        loop {
            let response = self
                .etcd_get(
                    "prune_requests",
                    start_key.clone(),
                    Some(
                        GetOptions::new()
                            .with_range(range_end.clone())
                            .with_limit(fetch_limit),
                    ),
                )
                .await
                .map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["prune_requests"])
                        .inc();
                    TsoError::Internal(format!("Etcd prune_requests failed: {}", error))
                })?;

            let kvs = response.kvs();
            let Some(last_key) = kvs.last().map(|kv| kv.key().to_vec()) else {
                break;
            };

            let mut candidates = Vec::new();
            for kv in kvs {
                let record: RequestRecord =
                    serde_json::from_slice(kv.value()).map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["prune_requests"])
                            .inc();
                        TsoError::Internal(format!(
                            "Request record prune deserialization failed: {}",
                            error
                        ))
                    })?;
                record.validate_schema_version()?;
                if !request_record_is_prunable(&record, older_than_ms) {
                    continue;
                }
                let key = String::from_utf8(kv.key().to_vec()).map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["prune_requests"])
                        .inc();
                    TsoError::Internal(format!("Request record key decode failed: {}", error))
                })?;
                candidates.push((key, kv.mod_revision() as u64));
            }

            for (key, revision) in candidates {
                match self
                    .delete_json_record(
                        key,
                        revision,
                        JsonTxnContext {
                            op_label: "prune_requests",
                            serialize_context: "Request record serialization",
                            txn_context: "Etcd request prune txn failed",
                            invalid_response_context: "invalid request prune txn response",
                        },
                    )
                    .await
                {
                    Ok(()) => {
                        pruned += 1;
                        if pruned >= limit {
                            return Ok(pruned);
                        }
                    }
                    Err(TsoError::CasFailed) => {}
                    Err(error) => return Err(error),
                }
            }

            start_key = next_etcd_key_after(&last_key);
            if start_key.as_slice() >= range_end.as_slice() {
                break;
            }
        }

        Ok(pruned)
    }
}
