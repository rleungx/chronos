use chronos_tso::{
    metadata::{
        GeneratorBatchOp, MemoryMetadataStore, MetadataStore, TimelineBatchOp, TimelineRecord,
    },
    ResourceTier, TimelineLifecycleState, TimelineRoute, TsoError,
};
use std::sync::Arc;
use tokio::sync::Barrier;

#[tokio::test]
async fn test_metadata_cas_semantics() {
    let store = Arc::new(MemoryMetadataStore::new());
    let timeline_key = "test.cas.timeline";

    let route = TimelineRoute {
        timeline_key: timeline_key.to_string(),
        generator_id: 1,
        epoch: 1,
        route_version: 1,
        resource_tier: ResourceTier::Shared,
        owner_worker_endpoint: "worker-1".to_string(),
    };

    let record = TimelineRecord {
        route: route.clone(),
        state: TimelineLifecycleState::Active,
        recovery_floor_tso: None,
        issued_upper_bound: None,
        last_graceful_issued: None,
        lease_expire_at_ms: Some(1000),
        updated_at_ms: 0,
    };

    // 1. Create initial record
    let rev1 = store.create_record(timeline_key, &record).await.unwrap();
    assert_eq!(rev1, 1);

    // 2. CAS with correct revision
    let mut record2 = record.clone();
    record2.lease_expire_at_ms = Some(2000);
    let rev2 = store
        .cas_record(timeline_key, rev1, &record2)
        .await
        .unwrap();
    assert!(rev2 > rev1);

    // 3. CAS with STALE revision should fail
    let res = store.cas_record(timeline_key, rev1, &record2).await;
    assert!(matches!(res, Err(TsoError::CasFailed)));

    // 4. Double create should fail
    let res = store.create_record(timeline_key, &record).await;
    assert!(matches!(res, Err(TsoError::MetadataAlreadyExists)));

    // 5. Get and verify
    let (loaded, rev_final) = store.get_record(timeline_key).await.unwrap().unwrap();
    assert_eq!(rev_final, rev2);
    assert_eq!(loaded.lease_expire_at_ms, Some(2000));
}

#[tokio::test]
async fn test_memory_metadata_create_is_atomic_under_contention() {
    let store = Arc::new(MemoryMetadataStore::new());
    let barrier = Arc::new(Barrier::new(9));
    let timeline_key = "test.concurrent.create";
    let record = TimelineRecord {
        route: TimelineRoute {
            timeline_key: timeline_key.to_string(),
            generator_id: 1,
            epoch: 1,
            route_version: 1,
            resource_tier: ResourceTier::Shared,
            owner_worker_endpoint: "worker-1".to_string(),
        },
        state: TimelineLifecycleState::Active,
        recovery_floor_tso: None,
        issued_upper_bound: None,
        last_graceful_issued: None,
        lease_expire_at_ms: Some(1000),
        updated_at_ms: 0,
    };

    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        let barrier = barrier.clone();
        let record = record.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            store.create_record(timeline_key, &record).await
        }));
    }
    barrier.wait().await;

    let mut success_count = 0usize;
    let mut already_exists_count = 0usize;
    for handle in handles {
        match handle.await.unwrap() {
            Ok(1) => success_count += 1,
            Err(TsoError::MetadataAlreadyExists) => already_exists_count += 1,
            other => panic!("unexpected create result: {:?}", other),
        }
    }

    assert_eq!(success_count, 1);
    assert_eq!(already_exists_count, 7);
}

#[tokio::test]
async fn test_memory_metadata_batch_cas_is_atomic() {
    let store = Arc::new(MemoryMetadataStore::new());

    let route_a = TimelineRoute {
        timeline_key: "batch.timeline.a".to_string(),
        generator_id: 1,
        epoch: 1,
        route_version: 1,
        resource_tier: ResourceTier::Shared,
        owner_worker_endpoint: "worker-1".to_string(),
    };
    let route_b = TimelineRoute {
        timeline_key: "batch.timeline.b".to_string(),
        generator_id: 2,
        epoch: 1,
        route_version: 1,
        resource_tier: ResourceTier::Shared,
        owner_worker_endpoint: "worker-1".to_string(),
    };

    let record_a = TimelineRecord {
        route: route_a.clone(),
        state: TimelineLifecycleState::Active,
        recovery_floor_tso: None,
        issued_upper_bound: None,
        last_graceful_issued: None,
        lease_expire_at_ms: Some(100),
        updated_at_ms: 0,
    };
    let record_b = TimelineRecord {
        route: route_b.clone(),
        state: TimelineLifecycleState::Active,
        recovery_floor_tso: None,
        issued_upper_bound: None,
        last_graceful_issued: None,
        lease_expire_at_ms: Some(100),
        updated_at_ms: 0,
    };

    let rev_a = store
        .create_record(&route_a.timeline_key, &record_a)
        .await
        .unwrap();
    let rev_b = store
        .create_record(&route_b.timeline_key, &record_b)
        .await
        .unwrap();

    let mut next_a = record_a.clone();
    let mut next_b = record_b.clone();
    next_a.lease_expire_at_ms = Some(200);
    next_b.lease_expire_at_ms = Some(300);

    let revisions = store
        .cas_records_batch(&[
            TimelineBatchOp {
                timeline_key: route_a.timeline_key.clone(),
                previous_revision: rev_a,
                record: next_a.clone(),
            },
            TimelineBatchOp {
                timeline_key: route_b.timeline_key.clone(),
                previous_revision: rev_b,
                record: next_b.clone(),
            },
        ])
        .await
        .unwrap();
    assert_eq!(revisions.len(), 2);

    let loaded_a = store
        .get_record(&route_a.timeline_key)
        .await
        .unwrap()
        .unwrap()
        .0;
    let loaded_b = store
        .get_record(&route_b.timeline_key)
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(loaded_a.lease_expire_at_ms, Some(200));
    assert_eq!(loaded_b.lease_expire_at_ms, Some(300));

    let failed = store
        .cas_records_batch(&[
            TimelineBatchOp {
                timeline_key: route_a.timeline_key.clone(),
                previous_revision: rev_a,
                record: next_a.clone(),
            },
            TimelineBatchOp {
                timeline_key: route_b.timeline_key.clone(),
                previous_revision: revisions[1],
                record: next_b.clone(),
            },
        ])
        .await;
    assert!(matches!(failed, Err(TsoError::CasFailed)));

    let unchanged_a = store
        .get_record(&route_a.timeline_key)
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(unchanged_a.lease_expire_at_ms, Some(200));

    store
        .create_generator_record(
            1,
            &chronos_tso::metadata::GeneratorRecord {
                generator_id: 1,
                owner_worker_endpoint: "worker-1".to_string(),
                owner_instance_id: "worker-1#1".to_string(),
                generator_lease_token: 1,
                lease_expire_at_ms: Some(100),
                last_issued_tso: Some(10),
                issued_upper_bound: Some(19),
                updated_at_ms: 0,
            },
        )
        .await
        .unwrap();
    store
        .create_generator_record(
            2,
            &chronos_tso::metadata::GeneratorRecord {
                generator_id: 2,
                owner_worker_endpoint: "worker-1".to_string(),
                owner_instance_id: "worker-1#2".to_string(),
                generator_lease_token: 1,
                lease_expire_at_ms: Some(100),
                last_issued_tso: Some(20),
                issued_upper_bound: Some(29),
                updated_at_ms: 0,
            },
        )
        .await
        .unwrap();

    let generator_revisions = store
        .cas_generator_records_batch(&[
            GeneratorBatchOp {
                generator_id: 1,
                previous_revision: 1,
                record: chronos_tso::metadata::GeneratorRecord {
                    generator_id: 1,
                    owner_worker_endpoint: "worker-1".to_string(),
                    owner_instance_id: "worker-1#1".to_string(),
                    generator_lease_token: 2,
                    lease_expire_at_ms: Some(200),
                    last_issued_tso: Some(11),
                    issued_upper_bound: Some(21),
                    updated_at_ms: 0,
                },
            },
            GeneratorBatchOp {
                generator_id: 2,
                previous_revision: 1,
                record: chronos_tso::metadata::GeneratorRecord {
                    generator_id: 2,
                    owner_worker_endpoint: "worker-1".to_string(),
                    owner_instance_id: "worker-1#2".to_string(),
                    generator_lease_token: 2,
                    lease_expire_at_ms: Some(300),
                    last_issued_tso: Some(21),
                    issued_upper_bound: Some(31),
                    updated_at_ms: 0,
                },
            },
        ])
        .await
        .unwrap();
    assert_eq!(generator_revisions.len(), 2);
}
