use std::sync::Arc;
use std::{panic, panic::AssertUnwindSafe};

use tokio::time::{timeout, Duration};

use crate::{metrics, ResourceTier, TimelineLifecycleState, TimelineRoute};

use super::*;
use crate::metadata::types::CURRENT_METADATA_SCHEMA_VERSION;
use crate::metadata::{AllocationRequestFingerprint, AllocationResponseRecord, RequestRecordState};

fn sample_record(timeline_key: &str, generator_id: u32) -> TimelineRecord {
    TimelineRecord {
        schema_version: 1,
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

fn sample_request_record(state: RequestRecordState, updated_at_ms: u64) -> RequestRecord {
    RequestRecord {
        schema_version: 1,
        fingerprint: AllocationRequestFingerprint { count: 1 },
        state,
        response: None,
        updated_at_ms,
    }
}

#[tokio::test]
async fn prune_completed_request_records_removes_only_old_completed_records() {
    let store = MemoryMetadataStore::new();
    let old_completed = RequestRecord {
        response: Some(AllocationResponseRecord {
            generator_id: 1,
            epoch: 1,
            route_version: 1,
            ranges: vec![crate::TimestampRange {
                start_tso: 10,
                end_tso: 10,
            }],
        }),
        ..sample_request_record(RequestRecordState::Completed, 100)
    };
    let fresh_completed = RequestRecord {
        response: Some(AllocationResponseRecord {
            generator_id: 1,
            epoch: 1,
            route_version: 1,
            ranges: vec![crate::TimestampRange {
                start_tso: 11,
                end_tso: 11,
            }],
        }),
        ..sample_request_record(RequestRecordState::Completed, 300)
    };

    store
        .create_request_record("timeline", "old", &old_completed)
        .await
        .unwrap();
    store
        .create_request_record("timeline", "fresh", &fresh_completed)
        .await
        .unwrap();
    store
        .create_request_record(
            "timeline",
            "pending",
            &sample_request_record(RequestRecordState::Pending, 50),
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .prune_completed_request_records(200, 10)
            .await
            .unwrap(),
        1
    );
    assert!(store
        .load_request_record("timeline", "old")
        .await
        .unwrap()
        .is_none());
    assert!(store
        .load_request_record("timeline", "fresh")
        .await
        .unwrap()
        .is_some());
    assert!(store
        .load_request_record("timeline", "pending")
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn request_cleanup_index_tracks_completed_record_updates() {
    let store = MemoryMetadataStore::new();
    let pending = sample_request_record(RequestRecordState::Pending, 100);
    let completed = RequestRecord {
        response: Some(AllocationResponseRecord {
            generator_id: 1,
            epoch: 1,
            route_version: 1,
            ranges: vec![crate::TimestampRange {
                start_tso: 10,
                end_tso: 10,
            }],
        }),
        ..sample_request_record(RequestRecordState::Completed, 200)
    };
    let fresh_completed = RequestRecord {
        updated_at_ms: 300,
        ..completed.clone()
    };

    let pending_revision = store
        .create_request_record("timeline", "indexed", &pending)
        .await
        .unwrap();
    assert!(store.request_cleanup_index.is_empty());

    let completed_revision = store
        .compare_exchange_request_record("timeline", "indexed", pending_revision, &completed)
        .await
        .unwrap();
    assert_eq!(store.request_cleanup_index.len(), 1);

    store
        .compare_exchange_request_record(
            "timeline",
            "indexed",
            completed_revision,
            &fresh_completed,
        )
        .await
        .unwrap();
    assert_eq!(store.request_cleanup_index.len(), 1);
    assert_eq!(
        store
            .prune_completed_request_records(250, 10)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .prune_completed_request_records(400, 10)
            .await
            .unwrap(),
        1
    );
    assert!(store
        .load_request_record("timeline", "indexed")
        .await
        .unwrap()
        .is_none());
    assert!(store.request_cleanup_index.is_empty());
}

#[tokio::test]
async fn prune_completed_request_records_handles_legacy_records_without_cleanup_index() {
    let store = MemoryMetadataStore::new();
    let legacy_completed = RequestRecord {
        response: Some(AllocationResponseRecord {
            generator_id: 1,
            epoch: 1,
            route_version: 1,
            ranges: vec![crate::TimestampRange {
                start_tso: 20,
                end_tso: 20,
            }],
        }),
        ..sample_request_record(RequestRecordState::Completed, 100)
    };

    store.request_records.insert(
        MemoryMetadataStore::request_key("timeline", "legacy"),
        (legacy_completed, 1),
    );
    assert!(store.request_cleanup_index.is_empty());

    assert_eq!(
        store
            .prune_completed_request_records(200, 10)
            .await
            .unwrap(),
        1
    );
    assert!(store
        .load_request_record("timeline", "legacy")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn load_timeline_route_accepts_newer_schema_projection() {
    let store = MemoryMetadataStore::new();
    let mut record = sample_record("timeline-route-future", 1);
    record.schema_version = CURRENT_METADATA_SCHEMA_VERSION + 1;
    store
        .records
        .insert(record.route.timeline_key.clone(), (record, 1));

    let (route_record, revision) = store
        .load_timeline_route("timeline-route-future")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        route_record.schema_version,
        CURRENT_METADATA_SCHEMA_VERSION + 1
    );
    assert_eq!(route_record.route.timeline_key, "timeline-route-future");
    assert_eq!(revision, 1);
}

#[tokio::test]
async fn list_timeline_filters_page_accepts_newer_schema_projection() {
    let store = MemoryMetadataStore::new();
    let mut record = sample_record("timeline-filter-future", 1);
    record.schema_version = CURRENT_METADATA_SCHEMA_VERSION + 1;
    store
        .records
        .insert(record.route.timeline_key.clone(), (record, 1));

    let page = store.list_timeline_filters_page(None, 10).await.unwrap();

    assert_eq!(page.records.len(), 1);
    assert_eq!(
        page.records[0].schema_version,
        CURRENT_METADATA_SCHEMA_VERSION + 1
    );
    assert_eq!(page.records[0].route.timeline_key, "timeline-filter-future");
}

#[tokio::test]
async fn load_timeline_still_rejects_newer_schema_full_record() {
    let store = MemoryMetadataStore::new();
    let mut record = sample_record("timeline-full-future", 1);
    record.schema_version = CURRENT_METADATA_SCHEMA_VERSION + 1;
    store
        .records
        .insert(record.route.timeline_key.clone(), (record, 1));

    assert!(matches!(
        store.load_timeline("timeline-full-future").await,
        Err(TsoError::Internal(message)) if message.contains("unsupported timeline metadata schema_version")
    ));
}

#[tokio::test]
async fn list_timelines_page_orders_records_and_returns_next_cursor() {
    let store = MemoryMetadataStore::new();
    for (timeline_key, generator_id) in [("timeline-c", 3), ("timeline-a", 1), ("timeline-b", 2)] {
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
    for (timeline_key, generator_id) in [("timeline-a", 1), ("timeline-b", 2), ("timeline-c", 3)] {
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

#[test]
fn sorted_timeline_key_cache_recovers_after_rwlock_poison() {
    let store = MemoryMetadataStore::new();
    let before = metrics::TSO_RECOVERY_EVENTS_TOTAL
        .with_label_values(&[
            "metadata",
            "memory_sorted_timeline_keys_write",
            "rwlock_poisoned",
        ])
        .get();

    let _ = panic::catch_unwind(AssertUnwindSafe(|| {
        let _guard = store.sorted_timeline_keys.write().unwrap();
        panic!("poison sorted timeline keys");
    }));

    store.invalidate_sorted_timeline_keys();
    let keys = store.sorted_timeline_keys();

    assert!(keys.is_empty());
    assert!(
        metrics::TSO_RECOVERY_EVENTS_TOTAL
            .with_label_values(&[
                "metadata",
                "memory_sorted_timeline_keys_write",
                "rwlock_poisoned"
            ])
            .get()
            > before
    );
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
                schema_version: 1,
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
                schema_version: 1,
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
        schema_version: 1,
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
        schema_version: 1,
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
        schema_version: 1,
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
