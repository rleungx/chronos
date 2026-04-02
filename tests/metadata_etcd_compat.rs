use chronos::{
    build_commit, build_version,
    metadata::{
        EtcdMetadataStore, GeneratorBatchOp, GeneratorLeaseAuthority, GeneratorRecord,
        MemoryMetadataStore, TimelineAuthority, TimelineBatchOp, TimelineRecord,
    },
    mixed_version_contract_id, ResourceTier, TimelineLifecycleState, TimelineRoute, TsoError,
};
use etcd_client::Client;
use std::sync::Arc;
use tokio::sync::Barrier;

#[derive(Debug, serde::Deserialize)]
struct ClusterContractRecord {
    contract_id: String,
    writer_build_version: String,
    writer_build_commit: String,
}

fn timeline_record(
    timeline_key: &str,
    generator_id: u32,
    lease_expire_at_ms: u64,
) -> TimelineRecord {
    TimelineRecord {
        route: TimelineRoute {
            timeline_key: timeline_key.to_string(),
            generator_id,
            epoch: 1,
            route_version: 1,
            resource_tier: ResourceTier::Shared,
            owner_worker_endpoint: "worker-1".to_string(),
        },
        state: TimelineLifecycleState::Active,
        recovery_floor_tso: None,
        issued_upper_bound: None,
        last_graceful_issued: None,
        lease_expire_at_ms: Some(lease_expire_at_ms),
        updated_at_ms: 0,
    }
}

fn generator_record(
    generator_id: u32,
    owner_instance_id: &str,
    lease_expire_at_ms: u64,
    last_issued_tso: u64,
    issued_upper_bound: u64,
    generator_lease_token: u64,
) -> GeneratorRecord {
    GeneratorRecord {
        generator_id,
        owner_worker_endpoint: "worker-1".to_string(),
        owner_instance_id: owner_instance_id.to_string(),
        generator_lease_token,
        lease_expire_at_ms: Some(lease_expire_at_ms),
        last_issued_tso: Some(last_issued_tso),
        issued_upper_bound: Some(issued_upper_bound),
        updated_at_ms: 0,
    }
}

fn test_etcd_endpoints() -> Vec<String> {
    std::env::var("CHRONOS_TEST_ETCD_ENDPOINTS")
        .unwrap_or_else(|_| "127.0.0.1:2379".into())
        .split(',')
        .map(|endpoint| endpoint.trim().to_string())
        .filter(|endpoint| !endpoint.is_empty())
        .collect()
}

fn unique_test_etcd_prefix(label: &str) -> String {
    format!(
        "/chronos-test-{}-{}-{}",
        label,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

async fn real_etcd_store(label: &str) -> Arc<EtcdMetadataStore> {
    Arc::new(
        EtcdMetadataStore::from_raw_endpoints_unchecked(
            test_etcd_endpoints(),
            unique_test_etcd_prefix(label),
        )
        .await
        .expect("etcd store should start"),
    )
}

async fn read_cluster_contract(prefix: &str) -> ClusterContractRecord {
    let mut client = Client::connect(test_etcd_endpoints(), None)
        .await
        .expect("etcd client should connect");
    let response = client
        .get(format!("{prefix}/meta/cluster_contract"), None)
        .await
        .expect("cluster contract read should succeed");
    let kv = response
        .kvs()
        .first()
        .expect("cluster contract key should exist");
    serde_json::from_slice(kv.value()).expect("cluster contract json should decode")
}

async fn assert_metadata_cas_semantics<S>(store: Arc<S>, timeline_key: &str)
where
    S: TimelineAuthority + Send + Sync,
{
    let record = timeline_record(timeline_key, 1, 1000);

    let rev1 = store.create_timeline(timeline_key, &record).await.unwrap();
    assert!(rev1 > 0);

    let mut record2 = record.clone();
    record2.lease_expire_at_ms = Some(2000);
    let rev2 = store
        .compare_exchange_timeline(timeline_key, rev1, &record2)
        .await
        .unwrap();
    assert!(rev2 > rev1);

    let res = store
        .compare_exchange_timeline(timeline_key, rev1, &record2)
        .await;
    assert!(matches!(res, Err(TsoError::CasFailed)));

    let res = store.create_timeline(timeline_key, &record).await;
    assert!(matches!(res, Err(TsoError::MetadataAlreadyExists)));

    let (loaded, rev_final) = store.load_timeline(timeline_key).await.unwrap().unwrap();
    assert_eq!(rev_final, rev2);
    assert_eq!(loaded.lease_expire_at_ms, Some(2000));
}

async fn assert_create_is_atomic_under_contention<S>(store: Arc<S>, timeline_key: &str)
where
    S: TimelineAuthority + Send + Sync + 'static,
{
    let barrier = Arc::new(Barrier::new(9));
    let record = timeline_record(timeline_key, 1, 1000);
    let timeline_key = timeline_key.to_string();

    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        let barrier = barrier.clone();
        let record = record.clone();
        let timeline_key = timeline_key.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            store.create_timeline(&timeline_key, &record).await
        }));
    }
    barrier.wait().await;

    let mut success_count = 0usize;
    let mut already_exists_count = 0usize;
    for handle in handles {
        match handle.await.unwrap() {
            Ok(revision) if revision > 0 => success_count += 1,
            Err(TsoError::MetadataAlreadyExists) => already_exists_count += 1,
            other => panic!("unexpected create result: {:?}", other),
        }
    }

    assert_eq!(success_count, 1);
    assert_eq!(already_exists_count, 7);
}

async fn assert_metadata_batch_cas_is_atomic<S>(
    store: Arc<S>,
    timeline_key_a: &str,
    timeline_key_b: &str,
) where
    S: TimelineAuthority + GeneratorLeaseAuthority + Send + Sync,
{
    let record_a = timeline_record(timeline_key_a, 1, 100);
    let record_b = timeline_record(timeline_key_b, 2, 100);

    let rev_a = store
        .create_timeline(timeline_key_a, &record_a)
        .await
        .unwrap();
    let rev_b = store
        .create_timeline(timeline_key_b, &record_b)
        .await
        .unwrap();

    let mut next_a = record_a.clone();
    let mut next_b = record_b.clone();
    next_a.lease_expire_at_ms = Some(200);
    next_b.lease_expire_at_ms = Some(300);

    let revisions = store
        .compare_exchange_timelines(&[
            TimelineBatchOp {
                timeline_key: timeline_key_a.to_string(),
                previous_revision: rev_a,
                record: next_a.clone(),
            },
            TimelineBatchOp {
                timeline_key: timeline_key_b.to_string(),
                previous_revision: rev_b,
                record: next_b.clone(),
            },
        ])
        .await
        .unwrap();
    assert_eq!(revisions.len(), 2);

    let loaded_a = store
        .load_timeline(timeline_key_a)
        .await
        .unwrap()
        .unwrap()
        .0;
    let loaded_b = store
        .load_timeline(timeline_key_b)
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(loaded_a.lease_expire_at_ms, Some(200));
    assert_eq!(loaded_b.lease_expire_at_ms, Some(300));

    let failed = store
        .compare_exchange_timelines(&[
            TimelineBatchOp {
                timeline_key: timeline_key_a.to_string(),
                previous_revision: rev_a,
                record: next_a,
            },
            TimelineBatchOp {
                timeline_key: timeline_key_b.to_string(),
                previous_revision: revisions[1],
                record: next_b,
            },
        ])
        .await;
    assert!(matches!(failed, Err(TsoError::CasFailed)));

    let unchanged_a = store
        .load_timeline(timeline_key_a)
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(unchanged_a.lease_expire_at_ms, Some(200));

    let generator_rev_a = store
        .create_generator(1, &generator_record(1, "worker-1#1", 100, 10, 19, 1))
        .await
        .unwrap();
    let generator_rev_b = store
        .create_generator(2, &generator_record(2, "worker-1#2", 100, 20, 29, 1))
        .await
        .unwrap();

    let generator_revisions = store
        .compare_exchange_generators(&[
            GeneratorBatchOp {
                generator_id: 1,
                previous_revision: generator_rev_a,
                record: generator_record(1, "worker-1#1", 200, 11, 21, 2),
            },
            GeneratorBatchOp {
                generator_id: 2,
                previous_revision: generator_rev_b,
                record: generator_record(2, "worker-1#2", 300, 21, 31, 2),
            },
        ])
        .await
        .unwrap();
    assert_eq!(generator_revisions.len(), 2);

    let loaded_generator_a = store.load_generator(1).await.unwrap().unwrap().0;
    let loaded_generator_b = store.load_generator(2).await.unwrap().unwrap().0;
    assert_eq!(loaded_generator_a.generator_lease_token, 2);
    assert_eq!(loaded_generator_a.lease_expire_at_ms, Some(200));
    assert_eq!(loaded_generator_b.generator_lease_token, 2);
    assert_eq!(loaded_generator_b.lease_expire_at_ms, Some(300));
}

#[tokio::test]
async fn test_metadata_cas_semantics() {
    assert_metadata_cas_semantics(Arc::new(MemoryMetadataStore::new()), "test.cas.timeline").await;
}

#[tokio::test]
async fn test_memory_metadata_create_is_atomic_under_contention() {
    assert_create_is_atomic_under_contention(
        Arc::new(MemoryMetadataStore::new()),
        "test.concurrent.create",
    )
    .await;
}

#[tokio::test]
async fn test_memory_metadata_batch_cas_is_atomic() {
    assert_metadata_batch_cas_is_atomic(
        Arc::new(MemoryMetadataStore::new()),
        "batch.timeline.a",
        "batch.timeline.b",
    )
    .await;
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_metadata_cas_semantics_match_memory_path() {
    assert_metadata_cas_semantics(real_etcd_store("metadata-cas").await, "test.cas.timeline").await;
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_metadata_create_is_atomic_under_contention() {
    assert_create_is_atomic_under_contention(
        real_etcd_store("metadata-create-contention").await,
        "test.concurrent.create",
    )
    .await;
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_metadata_batch_cas_is_atomic() {
    assert_metadata_batch_cas_is_atomic(
        real_etcd_store("metadata-batch-cas").await,
        "batch.timeline.a",
        "batch.timeline.b",
    )
    .await;
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_cluster_contract_key_is_created_on_connect() {
    let prefix = unique_test_etcd_prefix("cluster-contract-create");
    let _store =
        EtcdMetadataStore::from_raw_endpoints_unchecked(test_etcd_endpoints(), prefix.clone())
            .await
            .expect("etcd store should start");

    assert_ne!(build_commit(), "unknown");
    let record = read_cluster_contract(&prefix).await;
    assert_eq!(record.contract_id, mixed_version_contract_id());
    assert_eq!(record.writer_build_version, build_version());
    assert_eq!(record.writer_build_commit, build_commit());
}

#[tokio::test]
#[ignore = "requires a reachable etcd; set CHRONOS_TEST_ETCD_ENDPOINTS or run one on 127.0.0.1:2379"]
async fn etcd_cluster_contract_mismatch_rejects_connect() {
    let prefix = unique_test_etcd_prefix("cluster-contract-mismatch");
    let mut client = Client::connect(test_etcd_endpoints(), None)
        .await
        .expect("etcd client should connect");
    client
        .put(
            format!("{prefix}/meta/cluster_contract"),
            serde_json::to_vec(&serde_json::json!({
                "contract_id": "future-contract-v9",
                "writer_build_version": "9.9.9",
                "writer_build_commit": "deadbeef"
            }))
            .expect("contract json should serialize"),
            None,
        )
        .await
        .expect("prewriting cluster contract should succeed");

    let error = match EtcdMetadataStore::from_raw_endpoints_unchecked(test_etcd_endpoints(), prefix)
        .await
    {
        Ok(_) => panic!("mismatched contract should fail startup"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        TsoError::ClusterContractMismatch {
            cluster_contract_id,
            local_contract_id,
            cluster_writer_build_version,
            cluster_writer_build_commit,
        } if cluster_contract_id == "future-contract-v9"
            && local_contract_id == mixed_version_contract_id()
            && cluster_writer_build_version == "9.9.9"
            && cluster_writer_build_commit == "deadbeef"
    ));
}
