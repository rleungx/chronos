use super::*;

impl EtcdMetadataStore {
    pub(super) async fn get_json_record<T>(
        &self,
        key: String,
        op_label: &'static str,
        deserialize_context: &'static str,
    ) -> Result<Option<(T, u64)>, TsoError>
    where
        T: DeserializeOwned,
    {
        let response = self.etcd_get(op_label, key, None).await.map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[op_label])
                .inc();
            TsoError::Internal(format!("Etcd {} failed: {}", op_label, error))
        })?;

        if let Some(kv) = response.kvs().first() {
            let record = serde_json::from_slice(kv.value()).map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[op_label])
                    .inc();
                TsoError::Internal(format!("{} failed: {}", deserialize_context, error))
            })?;
            Ok(Some((record, kv.mod_revision() as u64)))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn get_json_records_with_prefix<T>(
        &self,
        prefix: String,
        op_label: &'static str,
        deserialize_context: &'static str,
    ) -> Result<Vec<T>, TsoError>
    where
        T: DeserializeOwned,
    {
        let response = self
            .etcd_get(op_label, prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[op_label])
                    .inc();
                TsoError::Internal(format!("Etcd {} failed: {}", op_label, error))
            })?;

        let mut records = Vec::with_capacity(response.kvs().len());
        for kv in response.kvs() {
            let record = serde_json::from_slice(kv.value()).map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[op_label])
                    .inc();
                TsoError::Internal(format!("{} failed: {}", deserialize_context, error))
            })?;
            records.push(record);
        }
        Ok(records)
    }

    pub(super) fn serialize_record<T>(
        record: &T,
        op_label: &'static str,
        serialize_context: &'static str,
    ) -> Result<Vec<u8>, TsoError>
    where
        T: Serialize,
    {
        serde_json::to_vec(record).map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[op_label])
                .inc();
            TsoError::Internal(format!("{} failed: {}", serialize_context, error))
        })
    }

    pub(super) fn extract_put_revision(
        response: etcd_client::TxnResponse,
        op_label: &'static str,
        invalid_response_context: &'static str,
    ) -> Result<u64, TsoError> {
        let op_responses = response.op_responses();
        let put_response = op_responses
            .first()
            .and_then(|operation| match operation {
                etcd_client::TxnOpResponse::Put(put_response) => Some(put_response),
                _ => None,
            })
            .ok_or_else(|| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[op_label])
                    .inc();
                TsoError::Internal(invalid_response_context.to_string())
            })?;

        let header = put_response.header().ok_or_else(|| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[op_label])
                .inc();
            TsoError::Internal(invalid_response_context.to_string())
        })?;

        Ok(header.revision() as u64)
    }

    pub(super) async fn create_json_record<T>(
        &self,
        key: String,
        record: &T,
        context: JsonTxnContext,
    ) -> Result<u64, TsoError>
    where
        T: Serialize,
    {
        let value = Self::serialize_record(record, context.op_label, context.serialize_context)?;
        let txn = Txn::new()
            .when(vec![Compare::mod_revision(
                key.as_bytes(),
                CompareOp::Equal,
                0,
            )])
            .and_then(vec![TxnOp::put(key.as_bytes(), value, None)]);

        let response = self
            .etcd_txn(context.op_label, txn)
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[context.op_label])
                    .inc();
                TsoError::Internal(format!("{}: {}", context.txn_context, error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict(context.op_label, "already_exists");
            return Err(TsoError::MetadataAlreadyExists);
        }

        Self::extract_put_revision(response, context.op_label, context.invalid_response_context)
    }

    pub(super) async fn cas_json_record<T>(
        &self,
        key: String,
        previous_revision: u64,
        record: &T,
        context: JsonTxnContext,
    ) -> Result<u64, TsoError>
    where
        T: Serialize,
    {
        let value = Self::serialize_record(record, context.op_label, context.serialize_context)?;
        let txn = Txn::new()
            .when(vec![Compare::mod_revision(
                key.as_bytes(),
                CompareOp::Equal,
                previous_revision as i64,
            )])
            .and_then(vec![TxnOp::put(key.as_bytes(), value, None)]);

        let response = self
            .etcd_txn(context.op_label, txn)
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[context.op_label])
                    .inc();
                TsoError::Internal(format!("{}: {}", context.txn_context, error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict(context.op_label, "cas_failed");
            return Err(TsoError::CasFailed);
        }

        Self::extract_put_revision(response, context.op_label, context.invalid_response_context)
    }

    pub(super) async fn cas_json_records_batch<T>(
        &self,
        operations: Vec<(String, u64, T)>,
        context: JsonTxnContext,
    ) -> Result<Vec<u64>, TsoError>
    where
        T: Serialize,
    {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        let mut compares = Vec::with_capacity(operations.len());
        let mut puts = Vec::with_capacity(operations.len());
        for (key, previous_revision, record) in &operations {
            compares.push(Compare::mod_revision(
                key.as_bytes(),
                CompareOp::Equal,
                *previous_revision as i64,
            ));
            puts.push(TxnOp::put(
                key.as_bytes(),
                Self::serialize_record(record, context.op_label, context.serialize_context)?,
                None,
            ));
        }

        let txn = Txn::new().when(compares).and_then(puts);
        let response = self
            .etcd_txn(context.op_label, txn)
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[context.op_label])
                    .inc();
                TsoError::Internal(format!("{}: {}", context.txn_context, error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict(context.op_label, "cas_failed");
            return Err(TsoError::CasFailed);
        }

        let revision = response
            .header()
            .map(|header| header.revision() as u64)
            .ok_or_else(|| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[context.op_label])
                    .inc();
                TsoError::Internal(context.invalid_response_context.to_string())
            })?;
        Ok(vec![revision; operations.len()])
    }

    pub(super) async fn delete_json_record(
        &self,
        key: String,
        previous_revision: u64,
        context: JsonTxnContext,
    ) -> Result<(), TsoError> {
        let txn = Txn::new()
            .when(vec![Compare::mod_revision(
                key.as_bytes(),
                CompareOp::Equal,
                previous_revision as i64,
            )])
            .and_then(vec![TxnOp::delete(key.as_bytes(), None)]);

        let response = self
            .etcd_txn(context.op_label, txn)
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[context.op_label])
                    .inc();
                TsoError::Internal(format!("{}: {}", context.txn_context, error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict(context.op_label, "cas_failed");
            return Err(TsoError::CasFailed);
        }

        Ok(())
    }
}
