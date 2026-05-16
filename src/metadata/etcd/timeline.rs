use async_trait::async_trait;

use super::*;

#[async_trait]
impl TimelineAuthority for EtcdMetadataStore {
    async fn load_timeline(
        &self,
        timeline_key: &str,
    ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get"])
            .start_timer();
        let result: Option<(TimelineRecord, u64)> = self
            .get_json_record(
                self.timeline_key(timeline_key),
                "get",
                "Record deserialization",
            )
            .await?;
        result
            .map(|(record, revision)| {
                record.validate_schema_version()?;
                Ok((record, revision))
            })
            .transpose()
    }

    async fn load_timeline_route(
        &self,
        timeline_key: &str,
    ) -> Result<Option<(TimelineRouteRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get_route"])
            .start_timer();
        let result: Option<(TimelineRouteRecord, u64)> = self
            .get_json_record(
                self.timeline_key(timeline_key),
                "get_route",
                "Route record deserialization",
            )
            .await?;
        result
            .map(|(record, revision)| {
                record.validate_schema_version()?;
                Ok((record, revision))
            })
            .transpose()
    }

    async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["list"])
            .start_timer();
        let records: Vec<TimelineRecord> = self
            .get_json_records_with_prefix(
                self.route_prefix(),
                "list",
                "Timeline record list deserialization",
            )
            .await?;
        records
            .into_iter()
            .map(|record| {
                record.validate_schema_version()?;
                Ok(record)
            })
            .collect()
    }

    async fn list_timelines_page(
        &self,
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<TimelineRecordListPage, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["list_page"])
            .start_timer();
        if limit == 0 {
            return Ok(TimelineRecordListPage {
                records: Vec::new(),
                next_start_after_timeline_key: None,
            });
        }

        let route_prefix = self.route_prefix();
        let start_key = start_after_timeline_key
            .map(|timeline_key| self.timeline_key(timeline_key))
            .unwrap_or_else(|| route_prefix.clone());
        let fetch_limit = limit
            .saturating_add(1)
            .saturating_add(usize::from(start_after_timeline_key.is_some()))
            .min(i64::MAX as usize) as i64;
        let response = self
            .etcd_get(
                "list_page",
                start_key.clone(),
                Some(
                    GetOptions::new()
                        .with_range(prefix_range_end(&route_prefix))
                        .with_limit(fetch_limit),
                ),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["list_page"])
                    .inc();
                TsoError::Internal(format!("Etcd list_page failed: {}", error))
            })?;

        let mut records = Vec::with_capacity(response.kvs().len().min(limit + 1));
        for kv in response.kvs() {
            if start_after_timeline_key.is_some() && kv.key() == start_key.as_bytes() {
                continue;
            }

            let record: TimelineRecord = serde_json::from_slice(kv.value()).map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["list_page"])
                    .inc();
                TsoError::Internal(format!(
                    "Timeline record page deserialization failed: {}",
                    error
                ))
            })?;
            record.validate_schema_version()?;
            records.push(record);
        }
        records
            .sort_unstable_by(|left, right| left.route.timeline_key.cmp(&right.route.timeline_key));

        let next_start_after_timeline_key =
            (records.len() > limit).then(|| records[limit - 1].route.timeline_key.clone());
        records.truncate(limit);

        Ok(TimelineRecordListPage {
            records,
            next_start_after_timeline_key,
        })
    }

    async fn list_timeline_filters_page(
        &self,
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<TimelineFilterRecordListPage, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["list_page_filters"])
            .start_timer();
        if limit == 0 {
            return Ok(TimelineFilterRecordListPage {
                records: Vec::new(),
                next_start_after_timeline_key: None,
            });
        }

        let route_prefix = self.route_prefix();
        let start_key = start_after_timeline_key
            .map(|timeline_key| self.timeline_key(timeline_key))
            .unwrap_or_else(|| route_prefix.clone());
        let fetch_limit = limit
            .saturating_add(1)
            .saturating_add(usize::from(start_after_timeline_key.is_some()))
            .min(i64::MAX as usize) as i64;
        let response = self
            .etcd_get(
                "list_page_filters",
                start_key.clone(),
                Some(
                    GetOptions::new()
                        .with_range(prefix_range_end(&route_prefix))
                        .with_limit(fetch_limit),
                ),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["list_page_filters"])
                    .inc();
                TsoError::Internal(format!("Etcd list_page_filters failed: {}", error))
            })?;

        let mut records = Vec::with_capacity(response.kvs().len().min(limit + 1));
        for kv in response.kvs() {
            if start_after_timeline_key.is_some() && kv.key() == start_key.as_bytes() {
                continue;
            }

            let record = parse_timeline_filter_record(kv.value()).map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["list_page_filters"])
                    .inc();
                TsoError::Internal(format!(
                    "Timeline filter record page deserialization failed: {}",
                    error
                ))
            })?;
            record.validate_schema_version()?;
            records.push(record);
        }
        records
            .sort_unstable_by(|left, right| left.route.timeline_key.cmp(&right.route.timeline_key));

        let next_start_after_timeline_key =
            (records.len() > limit).then(|| records[limit - 1].route.timeline_key.clone());
        records.truncate(limit);

        Ok(TimelineFilterRecordListPage {
            records,
            next_start_after_timeline_key,
        })
    }

    async fn list_timelines_by_status_filter_page(
        &self,
        states: &[TimelineLifecycleState],
        owner_worker_endpoint: Option<&str>,
        start_after_timeline_key: Option<&str>,
        limit: usize,
    ) -> Result<TimelineRecordListPage, TsoError> {
        if states.is_empty() && owner_worker_endpoint.is_none() {
            return self
                .list_timelines_page(start_after_timeline_key, limit)
                .await;
        }

        if let Some(owner_worker_endpoint) = owner_worker_endpoint {
            return self
                .list_timelines_from_status_index_prefix(
                    self.timeline_status_owner_index_prefix(owner_worker_endpoint),
                    states,
                    start_after_timeline_key,
                    limit,
                )
                .await;
        }

        if states.len() == 1 {
            return self
                .list_timelines_from_status_index_prefix(
                    self.timeline_status_state_index_prefix(states[0]),
                    &[],
                    start_after_timeline_key,
                    limit,
                )
                .await;
        }

        if limit == 0 {
            return Ok(TimelineRecordListPage {
                records: Vec::new(),
                next_start_after_timeline_key: None,
            });
        }

        let mut merged = BTreeMap::new();
        for state in states {
            let page = self
                .list_timelines_from_status_index_prefix(
                    self.timeline_status_state_index_prefix(*state),
                    &[],
                    start_after_timeline_key,
                    limit,
                )
                .await?;
            for record in page.records {
                merged.insert(record.route.timeline_key.clone(), record);
            }
        }

        let mut records: Vec<_> = merged.into_values().take(limit + 1).collect();
        let next_start_after_timeline_key =
            (records.len() > limit).then(|| records[limit - 1].route.timeline_key.clone());
        records.truncate(limit);

        Ok(TimelineRecordListPage {
            records,
            next_start_after_timeline_key,
        })
    }

    async fn create_timeline(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["create"])
            .start_timer();
        record.validate_schema_version()?;
        let stamped = record.stamped_for_persistence();
        self.create_timeline_with_indexes(timeline_key, &stamped)
            .await
    }

    async fn compare_exchange_timeline(
        &self,
        timeline_key: &str,
        expected_revision: u64,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas"])
            .start_timer();
        record.validate_schema_version()?;
        let stamped = record.stamped_for_persistence();
        self.compare_exchange_timeline_with_indexes(timeline_key, expected_revision, &stamped)
            .await
    }

    async fn compare_exchange_timelines(
        &self,
        operations: &[TimelineBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_batch"])
            .start_timer();
        for operation in operations {
            operation.record.validate_schema_version()?;
        }
        let stamped_operations: Vec<_> = operations
            .iter()
            .map(|operation| TimelineBatchOp {
                timeline_key: operation.timeline_key.clone(),
                previous_revision: operation.previous_revision,
                record: operation.record.stamped_for_persistence(),
            })
            .collect();
        self.compare_exchange_timelines_with_indexes(&stamped_operations)
            .await
    }
}
