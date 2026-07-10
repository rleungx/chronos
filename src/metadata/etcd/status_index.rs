use super::lease_lock::EtcdLeaseLock;
use super::*;

enum StatusIndexRebuildWait {
    Ready,
    LockReleased,
}

impl EtcdMetadataStore {
    pub(super) fn timeline_status_index_keys(&self, record: &TimelineRecord) -> [String; 2] {
        [
            self.timeline_status_owner_index_key(record),
            self.timeline_status_state_index_key(record),
        ]
    }

    pub(super) fn timeline_status_index_put_ops(
        &self,
        record: &TimelineRecord,
        op_label: &'static str,
    ) -> Result<Vec<TxnOp>, TsoError> {
        let value =
            Self::serialize_record(record, op_label, "Timeline status index serialization")?;
        Ok(self
            .timeline_status_index_keys(record)
            .into_iter()
            .map(|key| TxnOp::put(key.as_bytes(), value.clone(), None))
            .collect())
    }

    pub(super) fn timeline_status_index_replace_ops(
        &self,
        previous_record: Option<&TimelineRecord>,
        next_record: &TimelineRecord,
        op_label: &'static str,
    ) -> Result<Vec<TxnOp>, TsoError> {
        let next_keys = self.timeline_status_index_keys(next_record);
        let mut ops = Vec::with_capacity(4);
        if let Some(previous_record) = previous_record {
            for previous_key in self.timeline_status_index_keys(previous_record) {
                if !next_keys.iter().any(|next_key| next_key == &previous_key) {
                    ops.push(TxnOp::delete(previous_key.as_bytes(), None));
                }
            }
        }
        ops.extend(self.timeline_status_index_put_ops(next_record, op_label)?);
        Ok(ops)
    }

    pub(super) async fn rebuild_timeline_status_indexes(&self) -> Result<(), TsoError> {
        let marker_key = self.timeline_status_index_marker_key();
        let lock_key = self.timeline_status_index_rebuild_lock_key();
        let lock = loop {
            if self.status_index_marker_is_current(&marker_key).await? {
                return Ok(());
            }
            if let Some(lock) = self
                .try_acquire_lease_lock(
                    lock_key.clone(),
                    STATUS_INDEX_REBUILD_LOCK_TTL_SECS,
                    Vec::new(),
                    "status_index_rebuild_lock",
                )
                .await?
            {
                // The marker may have become current after the pre-lock read. Recheck while
                // holding the lock so a waiter never performs a redundant destructive rebuild.
                if self.status_index_marker_is_current(&marker_key).await? {
                    self.release_lease_lock(lock, "status_index_rebuild_lock")
                        .await;
                    return Ok(());
                }
                break lock;
            }
            match self
                .wait_for_status_index_rebuild(&marker_key, &lock_key)
                .await?
            {
                StatusIndexRebuildWait::Ready => return Ok(()),
                StatusIndexRebuildWait::LockReleased => continue,
            }
        };

        let result = self
            .rebuild_timeline_status_indexes_with_lock(&marker_key, &lock)
            .await;
        self.release_lease_lock(lock, "status_index_rebuild_lock")
            .await;
        result
    }

    async fn rebuild_timeline_status_indexes_with_lock(
        &self,
        marker_key: &str,
        lock: &EtcdLeaseLock,
    ) -> Result<(), TsoError> {
        let index_prefix = self.timeline_status_index_prefix();
        let cleanup = self
            .etcd_txn(
                "status_index_rebuild",
                Txn::new()
                    .when(vec![lock.fence_compare()])
                    .and_then(vec![TxnOp::delete(
                        index_prefix.as_bytes(),
                        Some(DeleteOptions::new().with_prefix()),
                    )]),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["status_index_rebuild"])
                    .inc();
                TsoError::Internal(format!("Etcd status index cleanup failed: {error}"))
            })?;
        if !cleanup.succeeded() {
            return Err(TsoError::Internal(
                "Etcd status index rebuild lock was lost before cleanup".into(),
            ));
        }

        let route_prefix = self.route_prefix();
        let route_range_end = prefix_range_end(&route_prefix);
        let mut start_key = route_prefix.clone().into_bytes();
        loop {
            let response = self
                .etcd_get(
                    "status_index_rebuild",
                    start_key.clone(),
                    Some(
                        GetOptions::new()
                            .with_range(route_range_end.clone())
                            .with_limit(STATUS_INDEX_REBUILD_BATCH_RECORDS as i64),
                    ),
                )
                .await
                .map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["status_index_rebuild"])
                        .inc();
                    TsoError::Internal(format!("Etcd status index route scan failed: {}", error))
                })?;
            if response.kvs().is_empty() {
                break;
            }

            for kv in response.kvs() {
                let route_key = kv.key().to_vec();
                let route_revision = kv.mod_revision();
                let record: TimelineRecord =
                    serde_json::from_slice(kv.value()).map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["status_index_rebuild"])
                            .inc();
                        TsoError::Internal(format!(
                            "Timeline status index rebuild deserialization failed: {}",
                            error
                        ))
                    })?;
                record.validate_schema_version()?;
                let ops = self.timeline_status_index_put_ops(&record, "status_index_rebuild")?;
                let response = self
                    .etcd_txn(
                        "status_index_rebuild",
                        Txn::new()
                            .when(vec![
                                lock.fence_compare(),
                                Compare::mod_revision(route_key, CompareOp::Equal, route_revision),
                            ])
                            .and_then(ops),
                    )
                    .await
                    .map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["status_index_rebuild"])
                            .inc();
                        TsoError::Internal(format!(
                            "Etcd status index rebuild txn failed: {}",
                            error
                        ))
                    })?;
                if !response.succeeded() && !self.lease_lock_is_owned(lock).await? {
                    return Err(TsoError::Internal(
                        "Etcd status index rebuild lock was lost while indexing routes".into(),
                    ));
                }
            }

            let last_key = response
                .kvs()
                .last()
                .map(|kv| kv.key().to_vec())
                .unwrap_or_default();
            start_key = next_etcd_key_after(&last_key);
        }

        let response = self
            .etcd_txn(
                "status_index_rebuild",
                Txn::new().when(vec![lock.fence_compare()]).and_then(vec![
                    TxnOp::put(marker_key.as_bytes(), CURRENT_STATUS_INDEX_VERSION, None),
                    TxnOp::delete(lock.key().as_bytes(), None),
                ]),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["status_index_rebuild"])
                    .inc();
                TsoError::Internal(format!("Etcd status index marker write failed: {error}"))
            })?;
        if !response.succeeded() {
            return Err(TsoError::Internal(
                "Etcd status index rebuild lock was lost before publishing the marker".into(),
            ));
        }
        Ok(())
    }

    async fn status_index_marker_is_current(&self, marker_key: &str) -> Result<bool, TsoError> {
        let marker = self
            .etcd_get("status_index_rebuild", marker_key.as_bytes().to_vec(), None)
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["status_index_rebuild"])
                    .inc();
                TsoError::Internal(format!("Etcd status index marker lookup failed: {}", error))
            })?;
        Ok(marker
            .kvs()
            .first()
            .is_some_and(|kv| kv.value() == CURRENT_STATUS_INDEX_VERSION.as_bytes()))
    }

    async fn wait_for_status_index_rebuild(
        &self,
        marker_key: &str,
        lock_key: &str,
    ) -> Result<StatusIndexRebuildWait, TsoError> {
        let deadline = Instant::now() + Duration::from_millis(STATUS_INDEX_REBUILD_WAIT_TIMEOUT_MS);
        loop {
            if self.status_index_marker_is_current(marker_key).await? {
                return Ok(StatusIndexRebuildWait::Ready);
            }
            let lock = self
                .etcd_get(
                    "status_index_rebuild_wait",
                    lock_key.as_bytes().to_vec(),
                    None,
                )
                .await
                .map_err(|error| {
                    TsoError::Internal(format!(
                        "Etcd status index rebuild lock lookup failed: {error}"
                    ))
                })?;
            if lock.kvs().is_empty() {
                return Ok(StatusIndexRebuildWait::LockReleased);
            }
            if Instant::now() >= deadline {
                return Err(TsoError::Internal(
                    "timed out waiting for concurrent status index rebuild".into(),
                ));
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    pub(super) async fn create_timeline_with_indexes(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError> {
        self.create_timeline_with_indexes_internal(timeline_key, record, None)
            .await
    }

    pub(super) async fn create_timeline_with_indexes_fenced(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
        lock: &EtcdLeaseLock,
    ) -> Result<u64, TsoError> {
        self.create_timeline_with_indexes_internal(timeline_key, record, Some(lock))
            .await
    }

    async fn create_timeline_with_indexes_internal(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
        lock: Option<&EtcdLeaseLock>,
    ) -> Result<u64, TsoError> {
        let key = self.timeline_key(timeline_key);
        let value = Self::serialize_record(record, "create", "Record serialization")?;
        let mut ops = vec![TxnOp::put(key.as_bytes(), value, None)];
        ops.extend(self.timeline_status_index_put_ops(record, "create")?);
        let mut comparisons = vec![Compare::mod_revision(key.as_bytes(), CompareOp::Equal, 0)];
        if let Some(lock) = lock {
            comparisons.push(lock.fence_compare());
        }

        let response = self
            .etcd_txn("create", Txn::new().when(comparisons).and_then(ops))
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["create"])
                    .inc();
                TsoError::Internal(format!("Etcd txn failed: {}", error))
            })?;

        if !response.succeeded() {
            record_metadata_conflict("create", "already_exists");
            return Err(if lock.is_some() {
                TsoError::CasFailed
            } else {
                TsoError::MetadataAlreadyExists
            });
        }

        Self::extract_put_revision(response, "create", "invalid txn response")
    }

    pub(super) async fn compare_exchange_timeline_with_indexes(
        &self,
        timeline_key: &str,
        expected_revision: u64,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError> {
        let Some((previous_record, previous_revision)) = self.load_timeline(timeline_key).await?
        else {
            record_metadata_conflict("cas", "not_found");
            return Err(TsoError::TimelineNotFound {
                timeline_key: timeline_key.to_owned(),
            });
        };
        if previous_revision != expected_revision {
            record_metadata_conflict("cas", "revision_mismatch");
            return Err(TsoError::CasFailed);
        }

        let key = self.timeline_key(timeline_key);
        let value = Self::serialize_record(record, "cas", "Record serialization")?;
        let mut ops = vec![TxnOp::put(key.as_bytes(), value, None)];
        ops.extend(self.timeline_status_index_replace_ops(
            Some(&previous_record),
            record,
            "cas",
        )?);

        let response = self
            .etcd_txn(
                "cas",
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
                    .with_label_values(&["cas"])
                    .inc();
                TsoError::Internal(format!("Etcd txn failed: {}", error))
            })?;

        if !response.succeeded() {
            if let Some((existing, revision)) = self.load_timeline(timeline_key).await? {
                if existing == *record {
                    return Ok(revision);
                }
            }
            record_metadata_conflict("cas", "cas_failed");
            return Err(TsoError::CasFailed);
        }

        Self::extract_put_revision(response, "cas", "invalid txn response")
    }

    pub(super) async fn compare_exchange_timelines_with_indexes(
        &self,
        operations: &[TimelineBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        let mut seen_timeline_keys = HashSet::with_capacity(operations.len());
        let mut previous_records = Vec::with_capacity(operations.len());
        for operation in operations {
            if !seen_timeline_keys.insert(operation.timeline_key.as_str()) {
                record_metadata_conflict("cas_batch", "duplicate_key");
                return Err(TsoError::CasFailed);
            }
            let Some((previous_record, previous_revision)) =
                self.load_timeline(&operation.timeline_key).await?
            else {
                record_metadata_conflict("cas_batch", "not_found");
                return Err(TsoError::TimelineNotFound {
                    timeline_key: operation.timeline_key.clone(),
                });
            };
            if previous_revision != operation.previous_revision {
                record_metadata_conflict("cas_batch", "revision_mismatch");
                return Err(TsoError::CasFailed);
            }
            previous_records.push(previous_record);
        }

        let mut compares = Vec::with_capacity(operations.len());
        let mut ops = Vec::with_capacity(operations.len() * 5);
        for (operation, previous_record) in operations.iter().zip(previous_records.iter()) {
            let key = self.timeline_key(&operation.timeline_key);
            compares.push(Compare::mod_revision(
                key.as_bytes(),
                CompareOp::Equal,
                operation.previous_revision as i64,
            ));
            ops.push(TxnOp::put(
                key.as_bytes(),
                Self::serialize_record(
                    &operation.record,
                    "cas_batch",
                    "Batch record serialization",
                )?,
                None,
            ));
            ops.extend(self.timeline_status_index_replace_ops(
                Some(previous_record),
                &operation.record,
                "cas_batch",
            )?);
        }

        let response = self
            .etcd_txn("cas_batch", Txn::new().when(compares).and_then(ops))
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_batch"])
                    .inc();
                TsoError::Internal(format!("Etcd batch txn failed: {}", error))
            })?;

        if !response.succeeded() {
            let mut recovered_revisions = Vec::with_capacity(operations.len());
            let mut recovered = true;
            for operation in operations {
                match self.load_timeline(&operation.timeline_key).await? {
                    Some((existing, revision)) if existing == operation.record => {
                        recovered_revisions.push(revision);
                    }
                    _ => {
                        recovered = false;
                        break;
                    }
                }
            }
            if recovered {
                return Ok(recovered_revisions);
            }
            record_metadata_conflict("cas_batch", "cas_failed");
            return Err(TsoError::CasFailed);
        }

        let revision = response
            .header()
            .map(|header| header.revision() as u64)
            .ok_or_else(|| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_batch"])
                    .inc();
                TsoError::Internal("invalid batch txn response".to_string())
            })?;
        Ok(vec![revision; operations.len()])
    }

    pub(super) fn status_index_start_key(
        index_prefix: &str,
        start_after_timeline_key: Option<&str>,
    ) -> Vec<u8> {
        start_after_timeline_key
            .map(|timeline_key| format!("{index_prefix}{timeline_key}").into_bytes())
            .unwrap_or_else(|| index_prefix.as_bytes().to_vec())
    }

    pub(super) async fn list_timelines_from_status_index_prefix(
        &self,
        index_prefix: String,
        states: &[TimelineLifecycleState],
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<TimelineRecordListPage, TsoError> {
        if limit == 0 {
            return Ok(TimelineRecordListPage {
                records: Vec::new(),
                next_start_after_timeline_key: None,
            });
        }

        let mut start_key = Self::status_index_start_key(&index_prefix, start_after_timeline_key);
        let range_end = prefix_range_end(&index_prefix);
        let mut records = Vec::with_capacity(limit + 1);
        while records.len() <= limit {
            let fetch_limit = limit
                .saturating_add(1)
                .saturating_add(usize::from(start_after_timeline_key.is_some()))
                .min(i64::MAX as usize) as i64;
            let response = self
                .etcd_get(
                    "list_status_index",
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
                        .with_label_values(&["list_status_index"])
                        .inc();
                    TsoError::Internal(format!("Etcd status index list failed: {}", error))
                })?;
            if response.kvs().is_empty() {
                break;
            }

            for kv in response.kvs() {
                if start_after_timeline_key.is_some() && kv.key() == start_key.as_slice() {
                    continue;
                }
                let record: TimelineRecord =
                    serde_json::from_slice(kv.value()).map_err(|error| {
                        metrics::TSO_METADATA_ERRORS_TOTAL
                            .with_label_values(&["list_status_index"])
                            .inc();
                        TsoError::Internal(format!(
                            "Timeline status index deserialization failed: {}",
                            error
                        ))
                    })?;
                record.validate_schema_version()?;
                if states.is_empty() || states.contains(&record.state) {
                    records.push(record);
                    if records.len() > limit {
                        break;
                    }
                }
            }

            if records.len() > limit {
                break;
            }
            let Some(last_key) = response.kvs().last().map(|kv| kv.key().to_vec()) else {
                break;
            };
            start_key = next_etcd_key_after(&last_key);
        }

        let next_start_after_timeline_key =
            (records.len() > limit).then(|| records[limit - 1].route.timeline_key.clone());
        records.truncate(limit);

        Ok(TimelineRecordListPage {
            records,
            next_start_after_timeline_key,
        })
    }
}
