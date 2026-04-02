use chronos::{
    checked_physical_ms_from_unix_ms, decode_tso, encode_tso, AllocateTimestampsRequest,
    ManualClock, ResourceTier, TimelineLifecycleState, TimelineRoute, TransferReason, TsoConfig,
    TsoError, TsoUnixMsBoundary, CUSTOM_EPOCH_UNIX_MS, MAX_GENERATORS, MAX_PHYSICAL_MS,
    MAX_UNIX_MS, PHYSICAL_BITS, SEQUENCE_CAPACITY, TSO_CAPACITY_ENVELOPE,
};

#[test]
fn crate_root_api_stays_stable() {
    let config = TsoConfig::default();
    assert_eq!(config.default_resource_tier, ResourceTier::Shared);
    let custom_epoch_unix_ms = std::hint::black_box(CUSTOM_EPOCH_UNIX_MS);
    let physical_bits = std::hint::black_box(PHYSICAL_BITS);
    assert!(custom_epoch_unix_ms > 0);
    assert_eq!(physical_bits, 40);
    assert_eq!(TSO_CAPACITY_ENVELOPE.max_supported_unix_ms, MAX_UNIX_MS);
    assert_eq!(
        TSO_CAPACITY_ENVELOPE.cluster_per_ms_capacity_ceiling,
        16_777_216
    );
    assert_eq!(
        checked_physical_ms_from_unix_ms(MAX_UNIX_MS + 1),
        Err(TsoUnixMsBoundary::BeyondCapacityEnvelope)
    );

    let tso = encode_tso(1, 2, 3).unwrap();
    let decoded = decode_tso(tso);
    assert_eq!(decoded.generator_id, 2);
    assert!(matches!(
        encode_tso(MAX_PHYSICAL_MS + 1, 2, 3),
        Err(TsoError::TsoOverflow)
    ));
    assert!(matches!(
        encode_tso(1, MAX_GENERATORS, 3),
        Err(TsoError::GeneratorIdOutOfRange { .. })
    ));
    assert!(matches!(
        encode_tso(1, 2, SEQUENCE_CAPACITY),
        Err(TsoError::TsoOverflow)
    ));

    let _route = TimelineRoute {
        timeline_key: "t".into(),
        generator_id: 1,
        epoch: 1,
        route_version: 1,
        resource_tier: ResourceTier::Shared,
        owner_worker_endpoint: "worker".into(),
    };

    let _request = AllocateTimestampsRequest {
        timeline_key: "t".into(),
        count: 1,
        expected_epoch: 1,
        expected_route_version: 1,
        client_request_id: "req".into(),
    };

    assert_eq!(TimelineLifecycleState::Active.to_string(), "active");
    let clock = ManualClock::new(10);
    assert_eq!(chronos::Clock::now_ms(&clock), 10);
    let _ = TransferReason::Manual;
    let _ = TsoError::InvalidCount;
}
