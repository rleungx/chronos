use std::{collections::HashSet, hash::Hash};

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::{sync::broadcast, sync::Mutex};

use crate::{metrics, TimelineRoute, TsoError};

use super::{
    types::{collect_timeline_route_update, RouteUpdateSignal},
    ControlPlaneStore, GeneratorBatchOp, GeneratorLeaseAuthority, GeneratorRecord,
    RouteUpdateSource, TimelineAuthority, TimelineBatchOp, TimelineRecord, TimelineRecordListPage,
};

pub struct MemoryMetadataStore {
    // P2: Use DashMap to reduce lock contention in large concurrent tests
    records: DashMap<String, (TimelineRecord, u64)>,
    generators: DashMap<u32, (GeneratorRecord, u64)>,
    route_updates: broadcast::Sender<RouteUpdateSignal>,
    timeline_cas_lock: Mutex<()>,
    generator_cas_lock: Mutex<()>,
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
            route_updates,
            timeline_cas_lock: Mutex::new(()),
            generator_cas_lock: Mutex::new(()),
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

    #[cfg(test)]
    fn arm_timeline_batch_cas_hook(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.timeline_batch_cas_acquired.lock().unwrap() = Some(tx);
        rx
    }

    #[cfg(test)]
    fn notify_timeline_batch_cas_acquired(&self) {
        if let Some(tx) = self.timeline_batch_cas_acquired.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }

    #[cfg(test)]
    fn arm_generator_batch_cas_hook(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.generator_batch_cas_acquired.lock().unwrap() = Some(tx);
        rx
    }

    #[cfg(test)]
    fn notify_generator_batch_cas_acquired(&self) {
        if let Some(tx) = self.generator_batch_cas_acquired.lock().unwrap().take() {
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
        Ok(self
            .records
            .get(timeline_key)
            .map(|entry| entry.value().clone()))
    }

    async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["list"])
            .start_timer();
        Ok(self
            .records
            .iter()
            .map(|entry| entry.value().0.clone())
            .collect())
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

        let mut records: Vec<_> = self
            .records
            .iter()
            .map(|entry| entry.value().0.clone())
            .collect();
        records
            .sort_unstable_by(|left, right| left.route.timeline_key.cmp(&right.route.timeline_key));
        let start_index = start_after_timeline_key
            .map(|start_after| {
                records.partition_point(|record| record.route.timeline_key.as_str() <= start_after)
            })
            .unwrap_or(0);
        let mut page_records: Vec<_> = records
            .into_iter()
            .skip(start_index)
            .take(limit + 1)
            .collect();
        let next_start_after_timeline_key = (page_records.len() > limit)
            .then(|| page_records[limit - 1].route.timeline_key.clone());
        page_records.truncate(limit);

        Ok(TimelineRecordListPage {
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
        let revision =
            Self::create_entry(&self.records, timeline_key.to_string(), record, "create")?;
        let mut route_updates = Vec::with_capacity(1);
        collect_timeline_route_update(None, record, &mut route_updates);
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
            collect_timeline_route_update(Some(current_record), record, &mut route_updates);
            let new_revision = *current_revision + 1;
            *entry.value_mut() = (record.clone(), new_revision);
            drop(entry);

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
            let new_revision = *current_revision + 1;
            *entry.value_mut() = (operation.record.clone(), new_revision);
            revisions.push(new_revision);
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
        Ok(self
            .generators
            .get(&generator_id)
            .map(|entry| entry.value().clone()))
    }

    async fn create_generator(
        &self,
        generator_id: u32,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["create_generator"])
            .start_timer();
        Self::create_entry(&self.generators, generator_id, record, "create_generator")
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
        let _cas_guard = self.generator_cas_lock.lock().await;
        Self::cas_entry(
            &self.generators,
            &generator_id,
            expected_revision,
            record,
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
impl ControlPlaneStore for MemoryMetadataStore {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::time::{timeout, Duration};

    use crate::{ResourceTier, TimelineLifecycleState, TimelineRoute};

    use super::*;

    fn sample_record(timeline_key: &str, generator_id: u32) -> TimelineRecord {
        TimelineRecord {
            route: TimelineRoute {
                timeline_key: timeline_key.to_string(),
                generator_id,
                owner_worker_endpoint: "worker-a:50051".into(),
                epoch: 1,
                route_version: 1,
                resource_tier: ResourceTier::Shared,
            },
            state: TimelineLifecycleState::Active,
            recovery_floor_tso: None,
            issued_upper_bound: Some(10),
            last_graceful_issued: Some(9),
            lease_expire_at_ms: Some(100),
            updated_at_ms: 1,
        }
    }

    #[tokio::test]
    async fn list_timelines_page_orders_records_and_returns_next_cursor() {
        let store = MemoryMetadataStore::new();
        for (timeline_key, generator_id) in
            [("timeline-c", 3), ("timeline-a", 1), ("timeline-b", 2)]
        {
            store
                .create_timeline(timeline_key, &sample_record(timeline_key, generator_id))
                .await
                .unwrap();
        }

        let page = store.list_timelines_page(None, 2).await.unwrap();
        let timeline_keys: Vec<_> = page
            .records
            .iter()
            .map(|record| record.route.timeline_key.as_str())
            .collect();

        assert_eq!(timeline_keys, vec!["timeline-a", "timeline-b"]);
        assert_eq!(
            page.next_start_after_timeline_key.as_deref(),
            Some("timeline-b")
        );
    }

    #[tokio::test]
    async fn list_timelines_page_treats_start_after_as_exclusive() {
        let store = MemoryMetadataStore::new();
        for (timeline_key, generator_id) in
            [("timeline-a", 1), ("timeline-b", 2), ("timeline-c", 3)]
        {
            store
                .create_timeline(timeline_key, &sample_record(timeline_key, generator_id))
                .await
                .unwrap();
        }

        let page = store
            .list_timelines_page(Some("timeline-a"), 2)
            .await
            .unwrap();
        let timeline_keys: Vec<_> = page
            .records
            .iter()
            .map(|record| record.route.timeline_key.as_str())
            .collect();

        assert_eq!(timeline_keys, vec!["timeline-b", "timeline-c"]);
        assert_eq!(page.next_start_after_timeline_key, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_timeline_cas_serializes_against_single_key_cas_under_contention() {
        let store = Arc::new(MemoryMetadataStore::new());
        let rev_a = store
            .create_timeline("timeline-a", &sample_record("timeline-a", 1))
            .await
            .unwrap();
        let rev_b = store
            .create_timeline("timeline-b", &sample_record("timeline-b", 2))
            .await
            .unwrap();

        let mut next_a = sample_record("timeline-a", 1);
        next_a.lease_expire_at_ms = Some(200);
        let mut next_b = sample_record("timeline-b", 2);
        next_b.lease_expire_at_ms = Some(300);
        let mut conflicting_b = sample_record("timeline-b", 2);
        conflicting_b.lease_expire_at_ms = Some(400);

        let read_guard = store
            .records
            .get("timeline-a")
            .expect("timeline-a should exist");
        let acquired = store.arm_timeline_batch_cas_hook();

        let batch_store = store.clone();
        let batch_task = tokio::spawn(async move {
            batch_store
                .compare_exchange_timelines(&[
                    TimelineBatchOp {
                        timeline_key: "timeline-a".to_string(),
                        previous_revision: rev_a,
                        record: next_a,
                    },
                    TimelineBatchOp {
                        timeline_key: "timeline-b".to_string(),
                        previous_revision: rev_b,
                        record: next_b,
                    },
                ])
                .await
        });

        timeout(Duration::from_secs(1), acquired)
            .await
            .expect("batch CAS should acquire the timeline CAS lock")
            .expect("timeline CAS hook should fire");

        let single_store = store.clone();
        let single_task = tokio::spawn(async move {
            single_store
                .compare_exchange_timeline("timeline-b", rev_b, &conflicting_b)
                .await
        });

        drop(read_guard);

        let revisions = timeout(Duration::from_secs(1), batch_task)
            .await
            .expect("batch CAS should complete")
            .unwrap()
            .unwrap();
        assert_eq!(revisions, vec![rev_a + 1, rev_b + 1]);

        let single_result = timeout(Duration::from_secs(1), single_task)
            .await
            .expect("single-key CAS should complete")
            .unwrap();
        assert!(matches!(single_result, Err(TsoError::CasFailed)));

        let loaded_a = store.load_timeline("timeline-a").await.unwrap().unwrap().0;
        let loaded_b = store.load_timeline("timeline-b").await.unwrap().unwrap().0;
        assert_eq!(loaded_a.lease_expire_at_ms, Some(200));
        assert_eq!(loaded_b.lease_expire_at_ms, Some(300));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_generator_cas_serializes_against_single_key_cas_under_contention() {
        let store = Arc::new(MemoryMetadataStore::new());
        let rev_a = store
            .create_generator(
                1,
                &GeneratorRecord {
                    generator_id: 1,
                    owner_worker_endpoint: "worker-a:50051".into(),
                    owner_instance_id: "worker-a#1".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(100),
                    last_issued_tso: Some(10),
                    issued_upper_bound: Some(20),
                    updated_at_ms: 1,
                },
            )
            .await
            .unwrap();
        let rev_b = store
            .create_generator(
                2,
                &GeneratorRecord {
                    generator_id: 2,
                    owner_worker_endpoint: "worker-b:50051".into(),
                    owner_instance_id: "worker-b#1".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(100),
                    last_issued_tso: Some(20),
                    issued_upper_bound: Some(30),
                    updated_at_ms: 1,
                },
            )
            .await
            .unwrap();

        let next_a = GeneratorRecord {
            generator_id: 1,
            owner_worker_endpoint: "worker-a:50051".into(),
            owner_instance_id: "worker-a#2".into(),
            generator_lease_token: 2,
            lease_expire_at_ms: Some(200),
            last_issued_tso: Some(11),
            issued_upper_bound: Some(21),
            updated_at_ms: 2,
        };
        let next_b = GeneratorRecord {
            generator_id: 2,
            owner_worker_endpoint: "worker-b:50051".into(),
            owner_instance_id: "worker-b#2".into(),
            generator_lease_token: 2,
            lease_expire_at_ms: Some(300),
            last_issued_tso: Some(21),
            issued_upper_bound: Some(31),
            updated_at_ms: 2,
        };
        let conflicting_b = GeneratorRecord {
            generator_id: 2,
            owner_worker_endpoint: "worker-b:50051".into(),
            owner_instance_id: "worker-b#3".into(),
            generator_lease_token: 3,
            lease_expire_at_ms: Some(400),
            last_issued_tso: Some(22),
            issued_upper_bound: Some(32),
            updated_at_ms: 3,
        };

        let read_guard = store.generators.get(&1).expect("generator 1 should exist");
        let acquired = store.arm_generator_batch_cas_hook();

        let batch_store = store.clone();
        let batch_task = tokio::spawn(async move {
            batch_store
                .compare_exchange_generators(&[
                    GeneratorBatchOp {
                        generator_id: 1,
                        previous_revision: rev_a,
                        record: next_a,
                    },
                    GeneratorBatchOp {
                        generator_id: 2,
                        previous_revision: rev_b,
                        record: next_b,
                    },
                ])
                .await
        });

        timeout(Duration::from_secs(1), acquired)
            .await
            .expect("batch generator CAS should acquire the generator CAS lock")
            .expect("generator CAS hook should fire");

        let single_store = store.clone();
        let single_task = tokio::spawn(async move {
            single_store
                .compare_exchange_generator(2, rev_b, &conflicting_b)
                .await
        });

        drop(read_guard);

        let revisions = timeout(Duration::from_secs(1), batch_task)
            .await
            .expect("batch generator CAS should complete")
            .unwrap()
            .unwrap();
        assert_eq!(revisions, vec![rev_a + 1, rev_b + 1]);

        let single_result = timeout(Duration::from_secs(1), single_task)
            .await
            .expect("single generator CAS should complete")
            .unwrap();
        assert!(matches!(single_result, Err(TsoError::CasFailed)));

        let loaded_a = store.load_generator(1).await.unwrap().unwrap().0;
        let loaded_b = store.load_generator(2).await.unwrap().unwrap().0;
        assert_eq!(loaded_a.generator_lease_token, 2);
        assert_eq!(loaded_b.generator_lease_token, 2);
    }
}
