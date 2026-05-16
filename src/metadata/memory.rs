use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

#[cfg(test)]
use std::sync::MutexGuard as StdMutexGuard;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::{sync::broadcast, sync::Mutex};

use crate::{metrics, recovery::record_recovery_event, TimelineRoute, TsoError};

use super::{
    keys,
    types::{collect_timeline_route_update, RouteUpdateSignal},
    ControlPlaneStore, GeneratorBatchOp, GeneratorLeaseAuthority, GeneratorRecord, RequestRecord,
    RequestRecordAuthority, RouteUpdateSource, TimelineAuthority, TimelineBatchOp,
    TimelineFilterRecord, TimelineFilterRecordListPage, TimelineRecord, TimelineRecordListPage,
    TimelineRouteRecord,
};

pub struct MemoryMetadataStore {
    records: DashMap<String, (TimelineRecord, u64)>,
    generators: DashMap<u32, (GeneratorRecord, u64)>,
    request_records: DashMap<String, (RequestRecord, u64)>,
    request_cleanup_index: DashMap<String, String>,
    sorted_timeline_keys: RwLock<Option<Arc<[String]>>>,
    route_updates: broadcast::Sender<RouteUpdateSignal>,
    timeline_cas_lock: Mutex<()>,
    generator_cas_lock: Mutex<()>,
    request_cas_lock: Mutex<()>,
    #[cfg(test)]
    timeline_batch_cas_acquired: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    #[cfg(test)]
    generator_batch_cas_acquired: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl MemoryMetadataStore {
    pub fn new() -> Self {
        let (route_updates, _) = broadcast::channel(1024);
        Self {
            records: DashMap::new(),
            generators: DashMap::new(),
            request_records: DashMap::new(),
            request_cleanup_index: DashMap::new(),
            sorted_timeline_keys: RwLock::new(None),
            route_updates,
            timeline_cas_lock: Mutex::new(()),
            generator_cas_lock: Mutex::new(()),
            request_cas_lock: Mutex::new(()),
            #[cfg(test)]
            timeline_batch_cas_acquired: std::sync::Mutex::new(None),
            #[cfg(test)]
            generator_batch_cas_acquired: std::sync::Mutex::new(None),
        }
    }

    fn create_entry<K, V>(
        store: &DashMap<K, (V, u64)>,
        key: K,
        record: &V,
        op_label: &'static str,
    ) -> Result<u64, TsoError>
    where
        K: Eq + Hash,
        V: Clone,
    {
        match store.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[op_label])
                    .inc();
                Err(TsoError::MetadataAlreadyExists)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let revision = 1;
                entry.insert((record.clone(), revision));
                Ok(revision)
            }
        }
    }

    fn cas_entry<K, V>(
        store: &DashMap<K, (V, u64)>,
        key: &K,
        previous_revision: u64,
        record: &V,
        op_label: &'static str,
        not_found_error: TsoError,
    ) -> Result<u64, TsoError>
    where
        K: Eq + Hash,
        V: Clone,
    {
        if let Some(mut entry) = store.get_mut(key) {
            let (_, current_revision) = entry.value();
            if *current_revision != previous_revision {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&[op_label])
                    .inc();
                return Err(TsoError::CasFailed);
            }

            let new_revision = *current_revision + 1;
            *entry.value_mut() = (record.clone(), new_revision);
            Ok(new_revision)
        } else {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[op_label])
                .inc();
            Err(not_found_error)
        }
    }

    fn publish_route_update(&self, route: &TimelineRoute) {
        let _ = self
            .route_updates
            .send(RouteUpdateSignal::Route(route.clone()));
    }

    fn request_key(timeline_key: &str, client_request_id: &str) -> String {
        keys::request_key("", timeline_key, client_request_id)
    }

    fn request_cleanup_index_key(
        record: &RequestRecord,
        timeline_key: &str,
        client_request_id: &str,
    ) -> String {
        keys::request_cleanup_index_key("", record.updated_at_ms, timeline_key, client_request_id)
    }

    fn insert_request_cleanup_index(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) {
        if record.state != super::RequestRecordState::Completed {
            return;
        }
        self.request_cleanup_index.insert(
            Self::request_cleanup_index_key(record, timeline_key, client_request_id),
            Self::request_key(timeline_key, client_request_id),
        );
    }

    fn remove_request_cleanup_index(
        &self,
        timeline_key: &str,
        client_request_id: &str,
        record: &RequestRecord,
    ) {
        if record.state != super::RequestRecordState::Completed {
            return;
        }
        self.request_cleanup_index
            .remove(&Self::request_cleanup_index_key(
                record,
                timeline_key,
                client_request_id,
            ));
    }

    fn sorted_timeline_keys_read(&self) -> RwLockReadGuard<'_, Option<Arc<[String]>>> {
        match self.sorted_timeline_keys.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event(
                    "metadata",
                    "memory_sorted_timeline_keys_read",
                    "rwlock_poisoned",
                );
                poisoned.into_inner()
            }
        }
    }

    fn sorted_timeline_keys_write(&self) -> RwLockWriteGuard<'_, Option<Arc<[String]>>> {
        match self.sorted_timeline_keys.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event(
                    "metadata",
                    "memory_sorted_timeline_keys_write",
                    "rwlock_poisoned",
                );
                poisoned.into_inner()
            }
        }
    }

    #[cfg(test)]
    fn timeline_batch_cas_hook_lock(
        &self,
    ) -> StdMutexGuard<'_, Option<tokio::sync::oneshot::Sender<()>>> {
        match self.timeline_batch_cas_acquired.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event(
                    "metadata",
                    "memory_timeline_batch_cas_hook",
                    "mutex_poisoned",
                );
                poisoned.into_inner()
            }
        }
    }

    #[cfg(test)]
    fn generator_batch_cas_hook_lock(
        &self,
    ) -> StdMutexGuard<'_, Option<tokio::sync::oneshot::Sender<()>>> {
        match self.generator_batch_cas_acquired.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                record_recovery_event(
                    "metadata",
                    "memory_generator_batch_cas_hook",
                    "mutex_poisoned",
                );
                poisoned.into_inner()
            }
        }
    }

    fn invalidate_sorted_timeline_keys(&self) {
        *self.sorted_timeline_keys_write() = None;
    }

    fn sorted_timeline_keys(&self) -> Arc<[String]> {
        if let Some(keys) = self.sorted_timeline_keys_read().as_ref().cloned() {
            return keys;
        }

        let mut timeline_keys: Vec<_> = self
            .records
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        timeline_keys.sort_unstable();
        let timeline_keys: Arc<[String]> = timeline_keys.into();

        let mut cached = self.sorted_timeline_keys_write();
        cached.get_or_insert_with(|| timeline_keys.clone()).clone()
    }

    #[cfg(test)]
    fn arm_timeline_batch_cas_hook(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.timeline_batch_cas_hook_lock() = Some(tx);
        rx
    }

    #[cfg(test)]
    fn notify_timeline_batch_cas_acquired(&self) {
        if let Some(tx) = self.timeline_batch_cas_hook_lock().take() {
            let _ = tx.send(());
        }
    }

    #[cfg(test)]
    fn arm_generator_batch_cas_hook(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.generator_batch_cas_hook_lock() = Some(tx);
        rx
    }

    #[cfg(test)]
    fn notify_generator_batch_cas_acquired(&self) {
        if let Some(tx) = self.generator_batch_cas_hook_lock().take() {
            let _ = tx.send(());
        }
    }
}

impl Default for MemoryMetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TimelineAuthority for MemoryMetadataStore {
    async fn load_timeline(
        &self,
        timeline_key: &str,
    ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get"])
            .start_timer();
        self.records
            .get(timeline_key)
            .map(|entry| {
                let (record, revision) = entry.value().clone();
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
        self.records
            .get(timeline_key)
            .map(|entry| {
                let (record, revision) = entry.value().clone();
                let route_record = TimelineRouteRecord {
                    schema_version: record.schema_version,
                    route: record.route,
                };
                route_record.validate_schema_version()?;
                Ok((route_record, revision))
            })
            .transpose()
    }

    async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["list"])
            .start_timer();
        self.records
            .iter()
            .map(|entry| {
                let record = entry.value().0.clone();
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

        let timeline_keys = self.sorted_timeline_keys();
        let start_index = start_after_timeline_key
            .map(|start_after| {
                timeline_keys.partition_point(|timeline_key| timeline_key.as_str() <= start_after)
            })
            .unwrap_or(0);
        let mut page_records = Vec::with_capacity(limit + 1);
        for timeline_key in timeline_keys.iter().skip(start_index).take(limit + 1) {
            if let Some(record) = self.records.get(timeline_key.as_str()) {
                let record = record.value().0.clone();
                record.validate_schema_version()?;
                page_records.push(record);
            }
        }
        let next_start_after_timeline_key = (page_records.len() > limit)
            .then(|| page_records[limit - 1].route.timeline_key.clone());
        page_records.truncate(limit);

        Ok(TimelineRecordListPage {
            records: page_records,
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

        let timeline_keys = self.sorted_timeline_keys();
        let start_index = start_after_timeline_key
            .map(|start_after| {
                timeline_keys.partition_point(|timeline_key| timeline_key.as_str() <= start_after)
            })
            .unwrap_or(0);
        let mut page_records = Vec::with_capacity(limit + 1);
        for timeline_key in timeline_keys.iter().skip(start_index).take(limit + 1) {
            if let Some(record) = self.records.get(timeline_key.as_str()) {
                let record = record.value().0.clone();
                let filter_record = TimelineFilterRecord {
                    schema_version: record.schema_version,
                    route: record.route,
                    state: record.state,
                };
                filter_record.validate_schema_version()?;
                page_records.push(filter_record);
            }
        }
        let next_start_after_timeline_key = (page_records.len() > limit)
            .then(|| page_records[limit - 1].route.timeline_key.clone());
        page_records.truncate(limit);

        Ok(TimelineFilterRecordListPage {
            records: page_records,
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
        let revision =
            Self::create_entry(&self.records, timeline_key.to_string(), &stamped, "create")?;
        self.invalidate_sorted_timeline_keys();
        let mut route_updates = Vec::with_capacity(1);
        collect_timeline_route_update(None, &stamped, &mut route_updates);
        for route_update in route_updates {
            self.publish_route_update(&route_update);
        }
        Ok(revision)
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
        let _cas_guard = self.timeline_cas_lock.lock().await;
        let revision = if let Some(mut entry) = self.records.get_mut(timeline_key) {
            let (current_record, current_revision) = entry.value();
            if *current_revision != expected_revision {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas"])
                    .inc();
                return Err(TsoError::CasFailed);
            }

            let mut route_updates = Vec::with_capacity(1);
            collect_timeline_route_update(Some(current_record), &stamped, &mut route_updates);
            let timeline_key_changed = stamped.route.timeline_key != timeline_key;
            let new_revision = *current_revision + 1;
            *entry.value_mut() = (stamped.clone(), new_revision);
            drop(entry);

            if timeline_key_changed {
                self.invalidate_sorted_timeline_keys();
            }

            for route_update in route_updates {
                self.publish_route_update(&route_update);
            }

            new_revision
        } else {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&["cas"])
                .inc();
            return Err(TsoError::TimelineNotFound {
                timeline_key: timeline_key.to_string(),
            });
        };
        Ok(revision)
    }

    async fn compare_exchange_timelines(
        &self,
        operations: &[TimelineBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_batch"])
            .start_timer();
        let _cas_guard = self.timeline_cas_lock.lock().await;
        #[cfg(test)]
        self.notify_timeline_batch_cas_acquired();
        let mut seen_timeline_keys = HashSet::with_capacity(operations.len());
        for operation in operations {
            operation.record.validate_schema_version()?;
            if !seen_timeline_keys.insert(operation.timeline_key.as_str()) {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_batch"])
                    .inc();
                return Err(TsoError::CasFailed);
            }
            let Some(entry) = self.records.get(operation.timeline_key.as_str()) else {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_batch"])
                    .inc();
                return Err(TsoError::TimelineNotFound {
                    timeline_key: operation.timeline_key.clone(),
                });
            };
            if entry.value().1 != operation.previous_revision {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_batch"])
                    .inc();
                return Err(TsoError::CasFailed);
            }
        }

        let mut revisions = Vec::with_capacity(operations.len());
        let mut route_updates = Vec::new();
        let mut timeline_key_changed = false;
        for operation in operations {
            let Some(mut entry) = self.records.get_mut(operation.timeline_key.as_str()) else {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_batch"])
                    .inc();
                return Err(TsoError::TimelineNotFound {
                    timeline_key: operation.timeline_key.clone(),
                });
            };
            let (current_record, current_revision) = entry.value();
            collect_timeline_route_update(
                Some(current_record),
                &operation.record,
                &mut route_updates,
            );
            timeline_key_changed |= operation.record.route.timeline_key != operation.timeline_key;
            let new_revision = *current_revision + 1;
            *entry.value_mut() = (operation.record.clone(), new_revision);
            revisions.push(new_revision);
        }
        if timeline_key_changed {
            self.invalidate_sorted_timeline_keys();
        }
        for route in route_updates {
            self.publish_route_update(&route);
        }
        Ok(revisions)
    }
}

#[async_trait]
impl GeneratorLeaseAuthority for MemoryMetadataStore {
    async fn load_generator(
        &self,
        generator_id: u32,
    ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get_generator"])
            .start_timer();
        self.generators
            .get(&generator_id)
            .map(|entry| {
                let (record, revision) = entry.value().clone();
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
        let mut loaded = HashMap::with_capacity(generator_ids.len());
        for &generator_id in generator_ids {
            let record = self
                .generators
                .get(&generator_id)
                .map(|entry| {
                    let (record, _) = entry.value().clone();
                    record.validate_schema_version()?;
                    Ok(record)
                })
                .transpose()?;
            loaded.insert(generator_id, record);
        }
        Ok(loaded)
    }

    async fn scan_generators(&self) -> Result<Vec<GeneratorRecord>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["scan_generators"])
            .start_timer();
        let mut records = Vec::with_capacity(self.generators.len());
        for entry in self.generators.iter() {
            let (record, _) = entry.value().clone();
            record.validate_schema_version()?;
            records.push(record);
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
        Self::create_entry(&self.generators, generator_id, &stamped, "create_generator")
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
        let _cas_guard = self.generator_cas_lock.lock().await;
        Self::cas_entry(
            &self.generators,
            &generator_id,
            expected_revision,
            &stamped,
            "cas_generator",
            TsoError::TimelineNotFound {
                timeline_key: format!("generator:{}", generator_id),
            },
        )
    }

    async fn compare_exchange_generators(
        &self,
        operations: &[GeneratorBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_generator_batch"])
            .start_timer();
        let _cas_guard = self.generator_cas_lock.lock().await;
        #[cfg(test)]
        self.notify_generator_batch_cas_acquired();
        let mut seen_generator_ids = HashSet::with_capacity(operations.len());
        for operation in operations {
            if !seen_generator_ids.insert(operation.generator_id) {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_generator_batch"])
                    .inc();
                return Err(TsoError::CasFailed);
            }
            let Some(entry) = self.generators.get(&operation.generator_id) else {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_generator_batch"])
                    .inc();
                return Err(TsoError::TimelineNotFound {
                    timeline_key: format!("generator:{}", operation.generator_id),
                });
            };
            if entry.value().1 != operation.previous_revision {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_generator_batch"])
                    .inc();
                return Err(TsoError::CasFailed);
            }
        }

        let mut revisions = Vec::with_capacity(operations.len());
        for operation in operations {
            let Some(mut entry) = self.generators.get_mut(&operation.generator_id) else {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_generator_batch"])
                    .inc();
                return Err(TsoError::TimelineNotFound {
                    timeline_key: format!("generator:{}", operation.generator_id),
                });
            };
            let (_, current_revision) = entry.value();
            let new_revision = *current_revision + 1;
            *entry.value_mut() = (operation.record.clone(), new_revision);
            revisions.push(new_revision);
        }
        Ok(revisions)
    }
}

impl RouteUpdateSource for MemoryMetadataStore {
    fn subscribe_route_updates(&self) -> broadcast::Receiver<RouteUpdateSignal> {
        self.route_updates.subscribe()
    }
}

#[async_trait]
impl RequestRecordAuthority for MemoryMetadataStore {
    async fn load_request_record(
        &self,
        timeline_key: &str,
        client_request_id: &str,
    ) -> Result<Option<(RequestRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get_request"])
            .start_timer();
        self.request_records
            .get(&Self::request_key(timeline_key, client_request_id))
            .map(|entry| {
                let (record, revision) = entry.value().clone();
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
        let _cas_guard = self.request_cas_lock.lock().await;
        let revision = Self::create_entry(
            &self.request_records,
            Self::request_key(timeline_key, client_request_id),
            &stamped,
            "create_request",
        )?;
        self.insert_request_cleanup_index(timeline_key, client_request_id, &stamped);
        Ok(revision)
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
        let _cas_guard = self.request_cas_lock.lock().await;
        let key = Self::request_key(timeline_key, client_request_id);
        let old_record;
        let new_revision;
        {
            let Some(mut entry) = self.request_records.get_mut(&key) else {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_request"])
                    .inc();
                return Err(TsoError::TimelineNotFound {
                    timeline_key: format!("request:{timeline_key}:{client_request_id}"),
                });
            };

            let (current_record, current_revision) = entry.value();
            if *current_revision != expected_revision {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas_request"])
                    .inc();
                return Err(TsoError::CasFailed);
            }

            old_record = current_record.clone();
            new_revision = *current_revision + 1;
            *entry.value_mut() = (stamped.clone(), new_revision);
        }
        self.remove_request_cleanup_index(timeline_key, client_request_id, &old_record);
        self.insert_request_cleanup_index(timeline_key, client_request_id, &stamped);
        Ok(new_revision)
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
        let key = Self::request_key(timeline_key, client_request_id);
        let _cas_guard = self.request_cas_lock.lock().await;
        let Some(entry) = self.request_records.get(&key) else {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&["delete_request"])
                .inc();
            return Err(TsoError::TimelineNotFound {
                timeline_key: format!("request:{timeline_key}:{client_request_id}"),
            });
        };
        if entry.value().1 != expected_revision {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&["delete_request"])
                .inc();
            return Err(TsoError::CasFailed);
        }
        let record = entry.value().0.clone();
        drop(entry);
        self.request_records.remove(&key);
        self.remove_request_cleanup_index(timeline_key, client_request_id, &record);
        Ok(())
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

        let _cas_guard = self.request_cas_lock.lock().await;
        let mut pruned = 0;
        let index_prefix = keys::request_cleanup_index_prefix("");
        let index_cutoff = keys::request_cleanup_index_cutoff("", older_than_ms);
        let mut index_candidates: Vec<(String, String)> = self
            .request_cleanup_index
            .iter()
            .filter_map(|entry| {
                let index_key = entry.key();
                (index_key.starts_with(&index_prefix) && index_key < &index_cutoff)
                    .then(|| (index_key.clone(), entry.value().clone()))
            })
            .collect();
        index_candidates.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        for (index_key, request_key) in index_candidates {
            if pruned >= limit {
                return Ok(pruned);
            }
            let Some(entry) = self.request_records.get(&request_key) else {
                self.request_cleanup_index.remove(&index_key);
                continue;
            };
            let record = entry.value().0.clone();
            if record.state != super::RequestRecordState::Completed
                || record.updated_at_ms >= older_than_ms
            {
                drop(entry);
                self.request_cleanup_index.remove(&index_key);
                continue;
            }
            drop(entry);
            self.request_records.remove(&request_key);
            self.request_cleanup_index.remove(&index_key);
            pruned += 1;
        }

        if pruned >= limit {
            return Ok(pruned);
        }

        let mut legacy_candidates = Vec::with_capacity(limit - pruned);
        for entry in self.request_records.iter() {
            let (record, _revision) = entry.value();
            if record.state == super::RequestRecordState::Completed
                && record.updated_at_ms < older_than_ms
            {
                legacy_candidates.push(entry.key().clone());
                if legacy_candidates.len() >= limit - pruned {
                    break;
                }
            }
        }

        for key in legacy_candidates {
            if self.request_records.remove(&key).is_some() {
                let stale_index_keys: Vec<String> = self
                    .request_cleanup_index
                    .iter()
                    .filter_map(|entry| (entry.value() == &key).then(|| entry.key().clone()))
                    .collect();
                for index_key in stale_index_keys {
                    self.request_cleanup_index.remove(&index_key);
                }
                pruned += 1;
            };
            if pruned >= limit {
                break;
            }
        }
        Ok(pruned)
    }
}

#[async_trait]
impl ControlPlaneStore for MemoryMetadataStore {
    fn request_records(&self) -> Option<&dyn RequestRecordAuthority> {
        Some(self)
    }
}

#[cfg(test)]
mod tests;
