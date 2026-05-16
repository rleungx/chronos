use async_trait::async_trait;

use super::*;

#[async_trait]
impl GeneratorLeaseAuthority for EtcdMetadataStore {
    async fn load_generator(
        &self,
        generator_id: u32,
    ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get_generator"])
            .start_timer();
        let result: Option<(GeneratorRecord, u64)> = self
            .get_json_record(
                self.generator_key(generator_id),
                "get_generator",
                "Generator record deserialization",
            )
            .await?;
        result
            .map(|(record, revision)| {
                record.validate_schema_version()?;
                Ok((record, revision))
            })
            .transpose()
    }

    async fn load_generators(
        &self,
        generator_ids: &[u32],
    ) -> Result<HashMap<u32, Option<GeneratorRecord>>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get_generator_batch"])
            .start_timer();
        let mut loaded = generator_ids
            .iter()
            .copied()
            .map(|generator_id| (generator_id, None))
            .collect::<HashMap<_, _>>();
        if loaded.is_empty() {
            return Ok(loaded);
        }

        let requested_ids: Vec<_> = loaded.keys().copied().collect();

        for chunk in requested_ids.chunks(GENERATOR_BATCH_GET_CHUNK_SIZE) {
            let ops: Vec<_> = chunk
                .iter()
                .map(|generator_id| {
                    TxnOp::get(self.generator_key(*generator_id).into_bytes(), None)
                })
                .collect();
            let response = self
                .etcd_txn("get_generator_batch", Txn::new().and_then(ops))
                .await
                .map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["get_generator_batch"])
                        .inc();
                    TsoError::Internal(format!("Etcd get_generator_batch failed: {}", error))
                })?;

            for (generator_id, op_response) in chunk.iter().zip(response.op_responses()) {
                let etcd_client::TxnOpResponse::Get(get_response) = op_response else {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["get_generator_batch"])
                        .inc();
                    return Err(TsoError::Internal(
                        "invalid generator batch get txn response".to_string(),
                    ));
                };
                let Some(kv) = get_response.kvs().first() else {
                    continue;
                };
                let record: GeneratorRecord =
                    serde_json::from_slice(kv.value()).map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["get_generator_batch"])
                            .inc();
                        TsoError::Internal(format!(
                            "Generator record batch deserialization failed: {}",
                            error
                        ))
                    })?;
                record.validate_schema_version()?;
                loaded.insert(*generator_id, Some(record));
            }
        }

        Ok(loaded)
    }

    async fn scan_generators(&self) -> Result<Vec<GeneratorRecord>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["scan_generators"])
            .start_timer();
        let generator_prefix = self.generator_prefix();
        let generator_range_end = prefix_range_end(&generator_prefix);
        let mut start_key = generator_prefix.clone().into_bytes();
        let mut records = Vec::new();
        loop {
            let response = self
                .etcd_get(
                    "scan_generators",
                    start_key.clone(),
                    Some(
                        GetOptions::new()
                            .with_range(generator_range_end.clone())
                            .with_limit(GENERATOR_SCAN_BATCH_RECORDS as i64),
                    ),
                )
                .await
                .map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["scan_generators"])
                        .inc();
                    TsoError::Internal(format!("Etcd scan_generators failed: {}", error))
                })?;

            if response.kvs().is_empty() {
                break;
            }
            for kv in response.kvs() {
                let record: GeneratorRecord =
                    serde_json::from_slice(kv.value()).map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["scan_generators"])
                            .inc();
                        TsoError::Internal(format!(
                            "Generator record scan deserialization failed: {}",
                            error
                        ))
                    })?;
                record.validate_schema_version()?;
                records.push(record);
            }
            let last_key = response
                .kvs()
                .last()
                .map(|kv| kv.key().to_vec())
                .unwrap_or_default();
            start_key = next_etcd_key_after(&last_key);
        }
        Ok(records)
    }

    async fn create_generator(
        &self,
        generator_id: u32,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["create_generator"])
            .start_timer();
        record.validate_schema_version()?;
        let stamped = record.stamped_for_persistence();
        self.create_json_record(
            self.generator_key(generator_id),
            &stamped,
            JsonTxnContext {
                op_label: "create_generator",
                serialize_context: "Generator record serialization",
                txn_context: "Etcd generator txn failed",
                invalid_response_context: "invalid generator txn response",
            },
        )
        .await
    }

    async fn compare_exchange_generator(
        &self,
        generator_id: u32,
        expected_revision: u64,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_generator"])
            .start_timer();
        record.validate_schema_version()?;
        let stamped = record.stamped_for_persistence();
        self.cas_json_record(
            self.generator_key(generator_id),
            expected_revision,
            &stamped,
            JsonTxnContext {
                op_label: "cas_generator",
                serialize_context: "Generator record serialization",
                txn_context: "Etcd generator txn failed",
                invalid_response_context: "invalid generator txn response",
            },
        )
        .await
    }

    async fn compare_exchange_generators(
        &self,
        operations: &[GeneratorBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_generator_batch"])
            .start_timer();
        for operation in operations {
            operation.record.validate_schema_version()?;
        }
        self.cas_json_records_batch(
            operations
                .iter()
                .map(|operation| {
                    (
                        self.generator_key(operation.generator_id),
                        operation.previous_revision,
                        operation.record.stamped_for_persistence(),
                    )
                })
                .collect(),
            JsonTxnContext {
                op_label: "cas_generator_batch",
                serialize_context: "Batch generator record serialization",
                txn_context: "Etcd generator batch txn failed",
                invalid_response_context: "invalid generator batch txn response",
            },
        )
        .await
    }
}
