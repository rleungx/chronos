use async_trait::async_trait;
use dashmap::DashMap;
use etcd_client::{Client, EventType, WatchOptions};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};

use crate::{metrics, TimelineLifecycleState, TimelineRoute, TsoError};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineRecord {
    pub route: TimelineRoute,
    #[serde(default = "default_timeline_state")]
    pub state: TimelineLifecycleState,
    #[serde(default)]
    pub recovery_floor_tso: Option<u64>,
    pub issued_upper_bound: Option<u64>,
    pub last_graceful_issued: Option<u64>,
    pub lease_expire_at_ms: Option<u64>,
    #[serde(default)]
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratorRecord {
    pub generator_id: u32,
    pub owner_worker_endpoint: String,
    #[serde(default)]
    pub owner_instance_id: String,
    #[serde(default)]
    pub generator_lease_token: u64,
    pub lease_expire_at_ms: Option<u64>,
    pub last_issued_tso: Option<u64>,
    pub issued_upper_bound: Option<u64>,
    #[serde(default)]
    pub updated_at_ms: u64,
}

fn default_timeline_state() -> TimelineLifecycleState {
    TimelineLifecycleState::Active
}

#[derive(Debug, Clone)]
pub struct TimelineBatchOp {
    pub timeline_key: String,
    pub previous_revision: u64,
    pub record: TimelineRecord,
}

#[derive(Debug, Clone)]
pub struct GeneratorBatchOp {
    pub generator_id: u32,
    pub previous_revision: u64,
    pub record: GeneratorRecord,
}

fn timeline_route_changed(
    previous_record: Option<&TimelineRecord>,
    next_record: &TimelineRecord,
) -> bool {
    previous_record
        .map(|record| record.route != next_record.route)
        .unwrap_or(true)
}

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn get_record(
        &self,
        timeline_key: &str,
    ) -> Result<Option<(TimelineRecord, u64)>, TsoError>;
    async fn create_record(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError>;
    async fn cas_record(
        &self,
        timeline_key: &str,
        previous_revision: u64,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError>;
    async fn cas_records_batch(
        &self,
        operations: &[TimelineBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let mut revisions = Vec::with_capacity(operations.len());
        for operation in operations {
            revisions.push(
                self.cas_record(
                    &operation.timeline_key,
                    operation.previous_revision,
                    &operation.record,
                )
                .await?,
            );
        }
        Ok(revisions)
    }

    async fn get_generator_record(
        &self,
        generator_id: u32,
    ) -> Result<Option<(GeneratorRecord, u64)>, TsoError>;
    async fn create_generator_record(
        &self,
        generator_id: u32,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError>;
    async fn cas_generator_record(
        &self,
        generator_id: u32,
        previous_revision: u64,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError>;
    async fn cas_generator_records_batch(
        &self,
        operations: &[GeneratorBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let mut revisions = Vec::with_capacity(operations.len());
        for operation in operations {
            revisions.push(
                self.cas_generator_record(
                    operation.generator_id,
                    operation.previous_revision,
                    &operation.record,
                )
                .await?,
            );
        }
        Ok(revisions)
    }

    fn subscribe_timeline_routes(&self) -> broadcast::Receiver<TimelineRoute>;
}

pub struct MemoryMetadataStore {
    // P2: Use DashMap to reduce lock contention in large concurrent tests
    records: DashMap<String, (TimelineRecord, u64)>,
    generators: DashMap<u32, (GeneratorRecord, u64)>,
    route_updates: broadcast::Sender<TimelineRoute>,
}

impl MemoryMetadataStore {
    pub fn new() -> Self {
        let (route_updates, _) = broadcast::channel(1024);
        Self {
            records: DashMap::new(),
            generators: DashMap::new(),
            route_updates,
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
        let _ = self.route_updates.send(route.clone());
    }
}

impl Default for MemoryMetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetadataStore for MemoryMetadataStore {
    async fn get_record(
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

    async fn create_record(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["create"])
            .start_timer();
        let revision =
            Self::create_entry(&self.records, timeline_key.to_string(), record, "create")?;
        self.publish_route_update(&record.route);
        Ok(revision)
    }

    async fn cas_record(
        &self,
        timeline_key: &str,
        previous_revision: u64,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas"])
            .start_timer();
        let revision = if let Some(mut entry) = self.records.get_mut(timeline_key) {
            let (current_record, current_revision) = entry.value();
            if *current_revision != previous_revision {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["cas"])
                    .inc();
                return Err(TsoError::CasFailed);
            }

            let should_publish = timeline_route_changed(Some(current_record), record);
            let new_revision = *current_revision + 1;
            *entry.value_mut() = (record.clone(), new_revision);
            drop(entry);

            if should_publish {
                self.publish_route_update(&record.route);
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

    async fn cas_records_batch(
        &self,
        operations: &[TimelineBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_batch"])
            .start_timer();
        for operation in operations {
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
            let mut entry = self
                .records
                .get_mut(operation.timeline_key.as_str())
                .expect("record validated before batch update");
            let (current_record, current_revision) = entry.value();
            let should_publish = timeline_route_changed(Some(current_record), &operation.record);
            let new_revision = *current_revision + 1;
            *entry.value_mut() = (operation.record.clone(), new_revision);
            revisions.push(new_revision);
            if should_publish {
                route_updates.push(operation.record.route.clone());
            }
        }
        for route in route_updates {
            self.publish_route_update(&route);
        }
        Ok(revisions)
    }

    async fn get_generator_record(
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

    async fn create_generator_record(
        &self,
        generator_id: u32,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["create_generator"])
            .start_timer();
        Self::create_entry(&self.generators, generator_id, record, "create_generator")
    }

    async fn cas_generator_record(
        &self,
        generator_id: u32,
        previous_revision: u64,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_generator"])
            .start_timer();
        Self::cas_entry(
            &self.generators,
            &generator_id,
            previous_revision,
            record,
            "cas_generator",
            TsoError::TimelineNotFound {
                timeline_key: format!("generator:{}", generator_id),
            },
        )
    }

    async fn cas_generator_records_batch(
        &self,
        operations: &[GeneratorBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_generator_batch"])
            .start_timer();
        for operation in operations {
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
            let mut entry = self
                .generators
                .get_mut(&operation.generator_id)
                .expect("generator validated before batch update");
            let (_, current_revision) = entry.value();
            let new_revision = *current_revision + 1;
            *entry.value_mut() = (operation.record.clone(), new_revision);
            revisions.push(new_revision);
        }
        Ok(revisions)
    }

    fn subscribe_timeline_routes(&self) -> broadcast::Receiver<TimelineRoute> {
        self.route_updates.subscribe()
    }
}

pub struct EtcdMetadataStore {
    client: Client,
    prefix: String,
    route_updates: broadcast::Sender<TimelineRoute>,
}

#[derive(Clone, Copy)]
struct JsonTxnContext {
    op_label: &'static str,
    serialize_context: &'static str,
    txn_context: &'static str,
    invalid_response_context: &'static str,
}

impl EtcdMetadataStore {
    pub async fn new(endpoints: Vec<String>, prefix: String) -> Result<Self, TsoError> {
        let client = Client::connect(endpoints, None)
            .await
            .map_err(|error| TsoError::Internal(format!("Etcd connection failed: {}", error)))?;
        let (route_updates, _) = broadcast::channel(1024);
        let store = Self {
            client,
            prefix,
            route_updates,
        };
        store.spawn_route_watch_loop();
        Ok(store)
    }

    fn timeline_key(&self, timeline_key: &str) -> String {
        format!("{}/routes/{}", self.prefix, timeline_key)
    }

    fn generator_key(&self, generator_id: u32) -> String {
        format!("{}/generators/{}", self.prefix, generator_id)
    }

    fn route_prefix(&self) -> String {
        format!("{}/routes/", self.prefix)
    }

    fn spawn_route_watch_loop(&self) {
        let mut watch_client = self.client.clone();
        let route_prefix = self.route_prefix();
        let route_updates = self.route_updates.clone();

        tokio::spawn(async move {
            loop {
                let watch_result = watch_client
                    .watch(
                        route_prefix.clone(),
                        Some(
                            WatchOptions::new()
                                .with_prefix()
                                .with_prev_key()
                                .with_progress_notify(),
                        ),
                    )
                    .await;

                let (_watcher, mut watch_stream) = match watch_result {
                    Ok(stream) => stream,
                    Err(_) => {
                        sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                };

                let mut restart_watch = false;
                loop {
                    match watch_stream.message().await {
                        Ok(Some(response)) => {
                            for event in response.events() {
                                if event.event_type() != EventType::Put {
                                    continue;
                                }
                                let Some(key_value) = event.kv() else {
                                    continue;
                                };
                                let Ok(record) =
                                    serde_json::from_slice::<TimelineRecord>(key_value.value())
                                else {
                                    continue;
                                };
                                let previous_record = event.prev_kv().and_then(|prev_key_value| {
                                    serde_json::from_slice::<TimelineRecord>(prev_key_value.value())
                                        .ok()
                                });
                                if timeline_route_changed(previous_record.as_ref(), &record) {
                                    let _ = route_updates.send(record.route);
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(_) => {
                            restart_watch = true;
                            break;
                        }
                    }
                }

                if !restart_watch {
                    sleep(Duration::from_millis(100)).await;
                } else {
                    sleep(Duration::from_millis(500)).await;
                }
            }
        });
    }

    async fn get_json_record<T>(
        &self,
        key: String,
        op_label: &'static str,
        deserialize_context: &'static str,
    ) -> Result<Option<(T, u64)>, TsoError>
    where
        T: DeserializeOwned,
    {
        let mut client = self.client.clone();
        let response = client.get(key, None).await.map_err(|error| {
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

    fn serialize_record<T>(
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

    fn extract_put_revision(
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

        Ok(put_response.header().unwrap().revision() as u64)
    }

    async fn create_json_record<T>(
        &self,
        key: String,
        record: &T,
        context: JsonTxnContext,
    ) -> Result<u64, TsoError>
    where
        T: Serialize,
    {
        let mut client = self.client.clone();
        let value = Self::serialize_record(record, context.op_label, context.serialize_context)?;
        let txn = etcd_client::Txn::new()
            .when(vec![etcd_client::Compare::mod_revision(
                key.as_bytes(),
                etcd_client::CompareOp::Equal,
                0,
            )])
            .and_then(vec![etcd_client::TxnOp::put(key.as_bytes(), value, None)]);

        let response = client.txn(txn).await.map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
            TsoError::Internal(format!("{}: {}", context.txn_context, error))
        })?;

        if !response.succeeded() {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
            return Err(TsoError::MetadataAlreadyExists);
        }

        Self::extract_put_revision(response, context.op_label, context.invalid_response_context)
    }

    async fn cas_json_record<T>(
        &self,
        key: String,
        previous_revision: u64,
        record: &T,
        context: JsonTxnContext,
    ) -> Result<u64, TsoError>
    where
        T: Serialize,
    {
        let mut client = self.client.clone();
        let value = Self::serialize_record(record, context.op_label, context.serialize_context)?;
        let txn = etcd_client::Txn::new()
            .when(vec![etcd_client::Compare::mod_revision(
                key.as_bytes(),
                etcd_client::CompareOp::Equal,
                previous_revision as i64,
            )])
            .and_then(vec![etcd_client::TxnOp::put(key.as_bytes(), value, None)]);

        let response = client.txn(txn).await.map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
            TsoError::Internal(format!("{}: {}", context.txn_context, error))
        })?;

        if !response.succeeded() {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
            return Err(TsoError::CasFailed);
        }

        Self::extract_put_revision(response, context.op_label, context.invalid_response_context)
    }

    async fn cas_json_records_batch<T>(
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

        let mut client = self.client.clone();
        let mut compares = Vec::with_capacity(operations.len());
        let mut puts = Vec::with_capacity(operations.len());
        for (key, previous_revision, record) in &operations {
            compares.push(etcd_client::Compare::mod_revision(
                key.as_bytes(),
                etcd_client::CompareOp::Equal,
                *previous_revision as i64,
            ));
            puts.push(etcd_client::TxnOp::put(
                key.as_bytes(),
                Self::serialize_record(record, context.op_label, context.serialize_context)?,
                None,
            ));
        }

        let txn = etcd_client::Txn::new().when(compares).and_then(puts);
        let response = client.txn(txn).await.map_err(|error| {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
            TsoError::Internal(format!("{}: {}", context.txn_context, error))
        })?;

        if !response.succeeded() {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[context.op_label])
                .inc();
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
}

#[async_trait]
impl MetadataStore for EtcdMetadataStore {
    async fn get_record(
        &self,
        timeline_key: &str,
    ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get"])
            .start_timer();
        self.get_json_record(
            self.timeline_key(timeline_key),
            "get",
            "Record deserialization",
        )
        .await
    }

    async fn create_record(
        &self,
        timeline_key: &str,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["create"])
            .start_timer();
        self.create_json_record(
            self.timeline_key(timeline_key),
            record,
            JsonTxnContext {
                op_label: "create",
                serialize_context: "Record serialization",
                txn_context: "Etcd txn failed",
                invalid_response_context: "invalid txn response",
            },
        )
        .await
    }

    async fn cas_record(
        &self,
        timeline_key: &str,
        previous_revision: u64,
        record: &TimelineRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas"])
            .start_timer();
        self.cas_json_record(
            self.timeline_key(timeline_key),
            previous_revision,
            record,
            JsonTxnContext {
                op_label: "cas",
                serialize_context: "Record serialization",
                txn_context: "Etcd txn failed",
                invalid_response_context: "invalid txn response",
            },
        )
        .await
    }

    async fn cas_records_batch(
        &self,
        operations: &[TimelineBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_batch"])
            .start_timer();
        self.cas_json_records_batch(
            operations
                .iter()
                .map(|operation| {
                    (
                        self.timeline_key(operation.timeline_key.as_str()),
                        operation.previous_revision,
                        operation.record.clone(),
                    )
                })
                .collect(),
            JsonTxnContext {
                op_label: "cas_batch",
                serialize_context: "Batch record serialization",
                txn_context: "Etcd batch txn failed",
                invalid_response_context: "invalid batch txn response",
            },
        )
        .await
    }

    async fn get_generator_record(
        &self,
        generator_id: u32,
    ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["get_generator"])
            .start_timer();
        self.get_json_record(
            self.generator_key(generator_id),
            "get_generator",
            "Generator record deserialization",
        )
        .await
    }

    async fn create_generator_record(
        &self,
        generator_id: u32,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["create_generator"])
            .start_timer();
        self.create_json_record(
            self.generator_key(generator_id),
            record,
            JsonTxnContext {
                op_label: "create_generator",
                serialize_context: "Generator record serialization",
                txn_context: "Etcd generator txn failed",
                invalid_response_context: "invalid generator txn response",
            },
        )
        .await
    }

    async fn cas_generator_record(
        &self,
        generator_id: u32,
        previous_revision: u64,
        record: &GeneratorRecord,
    ) -> Result<u64, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_generator"])
            .start_timer();
        self.cas_json_record(
            self.generator_key(generator_id),
            previous_revision,
            record,
            JsonTxnContext {
                op_label: "cas_generator",
                serialize_context: "Generator record serialization",
                txn_context: "Etcd generator txn failed",
                invalid_response_context: "invalid generator txn response",
            },
        )
        .await
    }

    async fn cas_generator_records_batch(
        &self,
        operations: &[GeneratorBatchOp],
    ) -> Result<Vec<u64>, TsoError> {
        let _timer = metrics::TSO_METADATA_LATENCY
            .with_label_values(&["cas_generator_batch"])
            .start_timer();
        self.cas_json_records_batch(
            operations
                .iter()
                .map(|operation| {
                    (
                        self.generator_key(operation.generator_id),
                        operation.previous_revision,
                        operation.record.clone(),
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

    fn subscribe_timeline_routes(&self) -> broadcast::Receiver<TimelineRoute> {
        self.route_updates.subscribe()
    }
}
