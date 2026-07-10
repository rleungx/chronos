use tonic::{Request, Response, Status};

use crate::authz::PeerCertAuthorizer;
use crate::config::TsoConfigValidationError;
use crate::proto::v1::{
    timeline_control_service_server::TimelineControlService,
    timeline_status_service_server::TimelineStatusService, GetTimelineStatusRequest,
    GetTimelineStatusResponse, HealthResponse, ListTimelineStatusesRequest,
    ListTimelineStatusesResponse, TransferTimelineRequest, TransferTimelineResponse,
};
use crate::TsoControlPlane;

use super::{
    current_timestamp, status_mapping, timeline_status_query, transfer_adapter, HealthStatusHandle,
};

pub struct TsoControlService {
    control_plane: TsoControlPlane,
    health_status: HealthStatusHandle,
    authorizer: PeerCertAuthorizer,
}

impl TsoControlService {
    pub fn new(control_plane: TsoControlPlane) -> Self {
        let health_info = control_plane.health();
        Self {
            control_plane,
            health_status: HealthStatusHandle::serving(&health_info),
            authorizer: PeerCertAuthorizer::disabled("TimelineControlService"),
        }
    }

    pub fn with_health_status(
        control_plane: TsoControlPlane,
        health_status: HealthStatusHandle,
    ) -> Self {
        Self {
            control_plane,
            health_status,
            authorizer: PeerCertAuthorizer::disabled("TimelineControlService"),
        }
    }

    pub fn with_health_status_and_allowlist(
        control_plane: TsoControlPlane,
        health_status: HealthStatusHandle,
        allowlist: &[String],
    ) -> Result<Self, TsoConfigValidationError> {
        Ok(Self {
            control_plane,
            health_status,
            authorizer: PeerCertAuthorizer::from_allowlist("TimelineControlService", allowlist)
                .map_err(TsoConfigValidationError::Security)?,
        })
    }

    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        self.authorizer.authorize(request)
    }
}

#[tonic::async_trait]
impl TimelineControlService for TsoControlService {
    async fn transfer_timeline(
        &self,
        request: Request<TransferTimelineRequest>,
    ) -> Result<Response<TransferTimelineResponse>, Status> {
        self.authorize(&request)?;
        Ok(Response::new(
            transfer_adapter::transfer_timeline_response(&self.control_plane, request.into_inner())
                .await?,
        ))
    }

    async fn health(&self, request: Request<()>) -> Result<Response<HealthResponse>, Status> {
        self.authorize(&request)?;
        let worker_status = self.health_status.snapshot();
        Ok(Response::new(status_mapping::health_response(
            worker_status,
            current_timestamp(),
        )))
    }
}

pub struct TsoTimelineStatusService {
    control_plane: TsoControlPlane,
    authorizer: PeerCertAuthorizer,
}

impl TsoTimelineStatusService {
    pub fn new(control_plane: TsoControlPlane) -> Self {
        Self {
            control_plane,
            authorizer: PeerCertAuthorizer::disabled("TimelineStatusService"),
        }
    }

    pub fn with_allowlist(
        control_plane: TsoControlPlane,
        allowlist: &[String],
    ) -> Result<Self, TsoConfigValidationError> {
        Ok(Self {
            control_plane,
            authorizer: PeerCertAuthorizer::from_allowlist("TimelineStatusService", allowlist)
                .map_err(TsoConfigValidationError::Security)?,
        })
    }

    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        self.authorizer.authorize(request)
    }
}

#[tonic::async_trait]
impl TimelineStatusService for TsoTimelineStatusService {
    async fn get_timeline_status(
        &self,
        request: Request<GetTimelineStatusRequest>,
    ) -> Result<Response<GetTimelineStatusResponse>, Status> {
        self.authorize(&request)?;
        Ok(Response::new(
            timeline_status_query::get_timeline_status_response(
                &self.control_plane,
                request.into_inner(),
            )
            .await?,
        ))
    }

    async fn list_timeline_statuses(
        &self,
        request: Request<ListTimelineStatusesRequest>,
    ) -> Result<Response<ListTimelineStatusesResponse>, Status> {
        self.authorize(&request)?;
        Ok(Response::new(
            timeline_status_query::list_timeline_statuses_response(
                &self.control_plane,
                request.into_inner(),
            )
            .await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tonic::Code;

    use crate::metadata::{
        EtcdMetadataStore, GeneratorLeaseAuthority, GeneratorRecord, MemoryMetadataStore,
        TimelineAuthority, TimelineRecord,
    };
    use crate::proto::v1::TimelineFailoverReadiness as ProtoTimelineFailoverReadiness;
    use crate::proto::v1::TimelineState as ProtoTimelineState;
    use crate::proto::v1::{
        GetTimelineStatusRequest, ListTimelineStatusesRequest, ResourceTier as ProtoResourceTier,
    };
    use crate::{
        ManualClock, ResourceTier, TimelineLifecycleState, TimelineRoute, TsoConfig,
        TsoSecurityMode, TsoService,
    };

    fn required_test_config(config: TsoConfig) -> TsoConfig {
        crate::test_tls::required_grpc_tls_test_config(config, 100)
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
        let endpoints = test_etcd_endpoints();
        let config = TsoConfig {
            metadata_kind: "etcd".into(),
            etcd_endpoints: endpoints,
            security_mode: Some(TsoSecurityMode::DevInsecure),
            worker_id: "rpc-etcd-store".into(),
            advertise_endpoint: "127.0.0.1:50051".into(),
            safety_gap_ms: 500,
            max_clock_skew_ms: 500,
            ..TsoConfig::default()
        };
        Arc::new(
            EtcdMetadataStore::from_config(&config, unique_test_etcd_prefix(label))
                .await
                .expect("etcd store should start"),
        )
    }

    #[tokio::test]
    async fn get_timeline_status_returns_authoritative_snapshot_and_zero_cache_ttl() {
        let clock = Arc::new(ManualClock::new(200));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let timeline = TimelineRecord {
            schema_version: 1,
            route: TimelineRoute {
                timeline_key: "timeline.status".into(),
                generator_id: 7,
                owner_worker_endpoint: "worker-a:50051".into(),
                epoch: 4,
                route_version: 9,
                resource_tier: ResourceTier::Warm,
            },
            state: TimelineLifecycleState::Active,
            recovery_floor_tso: Some(42),
            issued_upper_bound: Some(88),
            last_graceful_issued: Some(77),
            lease_expire_at_ms: Some(66),
            updated_at_ms: 10,
        };
        let generator = GeneratorRecord {
            schema_version: 1,
            generator_id: 7,
            owner_worker_endpoint: "worker-a:50051".into(),
            owner_instance_id: "instance-a".into(),
            generator_lease_token: 1,
            lease_expire_at_ms: Some(100),
            last_issued_tso: Some(99),
            issued_upper_bound: Some(111),
            updated_at_ms: 12,
        };
        metadata
            .create_timeline(&timeline.route.timeline_key, &timeline)
            .await
            .unwrap();
        metadata.create_generator(7, &generator).await.unwrap();

        let rpc = TsoTimelineStatusService::new(service.control_plane());
        let response = rpc
            .get_timeline_status(Request::new(GetTimelineStatusRequest {
                timeline_key: timeline.route.timeline_key.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        let status = response.status.expect("status should be present");
        let route = status.route.expect("route should be present");

        assert_eq!(route.timeline_key, timeline.route.timeline_key);
        assert_eq!(route.generator_id, timeline.route.generator_id);
        assert_eq!(
            route.owner_worker_endpoint,
            timeline.route.owner_worker_endpoint
        );
        assert_eq!(route.resource_tier, ProtoResourceTier::Warm as i32);
        assert_eq!(status.state, ProtoTimelineState::Active as i32);
        assert_eq!(status.recovery_floor_tso, Some(42));
        assert_eq!(status.issued_upper_bound, Some(88));
        assert_eq!(status.last_graceful_issued, Some(77));
        assert_eq!(status.lease_expire_at_ms, Some(100));
        assert_eq!(status.owner_instance_id.as_deref(), Some("instance-a"));
        assert_eq!(status.updated_at_ms, 12);
        assert_eq!(
            status.failover_readiness,
            ProtoTimelineFailoverReadiness::Eligible as i32
        );
    }

    #[tokio::test]
    async fn get_timeline_status_keeps_success_when_generator_does_not_match_route_owner() {
        let clock = Arc::new(ManualClock::new(200));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let timeline = TimelineRecord {
            schema_version: 1,
            route: TimelineRoute {
                timeline_key: "timeline.mismatch".into(),
                generator_id: 7,
                owner_worker_endpoint: "worker-a:50051".into(),
                epoch: 1,
                route_version: 2,
                resource_tier: ResourceTier::Shared,
            },
            state: TimelineLifecycleState::Draining,
            recovery_floor_tso: Some(55),
            issued_upper_bound: None,
            last_graceful_issued: None,
            lease_expire_at_ms: Some(33),
            updated_at_ms: 10,
        };
        let generator = GeneratorRecord {
            schema_version: 1,
            generator_id: 7,
            owner_worker_endpoint: "worker-b:50051".into(),
            owner_instance_id: "instance-b".into(),
            generator_lease_token: 1,
            lease_expire_at_ms: Some(100),
            last_issued_tso: None,
            issued_upper_bound: None,
            updated_at_ms: 90,
        };
        metadata
            .create_timeline(&timeline.route.timeline_key, &timeline)
            .await
            .unwrap();
        metadata.create_generator(7, &generator).await.unwrap();

        let rpc = TsoTimelineStatusService::new(service.control_plane());
        let response = rpc
            .get_timeline_status(Request::new(GetTimelineStatusRequest {
                timeline_key: timeline.route.timeline_key.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        let status = response.status.expect("status should be present");

        assert_eq!(status.state, ProtoTimelineState::Draining as i32);
        assert_eq!(status.lease_expire_at_ms, None);
        assert_eq!(status.owner_instance_id, None);
        assert_eq!(status.updated_at_ms, timeline.updated_at_ms);
        assert_eq!(
            status.failover_readiness,
            ProtoTimelineFailoverReadiness::NotApplicable as i32
        );
    }

    #[tokio::test]
    async fn get_timeline_status_returns_not_found_for_missing_timeline() {
        let clock = Arc::new(ManualClock::new(10));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service =
            TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();
        let rpc = TsoTimelineStatusService::new(service.control_plane());

        let error = rpc
            .get_timeline_status(Request::new(GetTimelineStatusRequest {
                timeline_key: "missing.timeline".into(),
            }))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn list_timeline_statuses_returns_sorted_filtered_rows_with_pagination() {
        let clock = Arc::new(ManualClock::new(500));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();

        let timeline_a = TimelineRecord {
            schema_version: 1,
            route: TimelineRoute {
                timeline_key: "timeline-a".into(),
                generator_id: 1,
                owner_worker_endpoint: "worker-a:50051".into(),
                epoch: 1,
                route_version: 1,
                resource_tier: ResourceTier::Shared,
            },
            state: TimelineLifecycleState::Draining,
            recovery_floor_tso: Some(10),
            issued_upper_bound: Some(20),
            last_graceful_issued: Some(9),
            lease_expire_at_ms: Some(100),
            updated_at_ms: 11,
        };
        let timeline_b = TimelineRecord {
            schema_version: 1,
            route: TimelineRoute {
                timeline_key: "timeline-b".into(),
                generator_id: 2,
                owner_worker_endpoint: "worker-a:50051".into(),
                epoch: 2,
                route_version: 2,
                resource_tier: ResourceTier::Warm,
            },
            state: TimelineLifecycleState::Recovering,
            recovery_floor_tso: Some(30),
            issued_upper_bound: Some(40),
            last_graceful_issued: Some(29),
            lease_expire_at_ms: Some(110),
            updated_at_ms: 22,
        };
        let timeline_c = TimelineRecord {
            schema_version: 1,
            route: TimelineRoute {
                timeline_key: "timeline-c".into(),
                generator_id: 3,
                owner_worker_endpoint: "worker-b:50051".into(),
                epoch: 3,
                route_version: 3,
                resource_tier: ResourceTier::Dedicated,
            },
            state: TimelineLifecycleState::Active,
            recovery_floor_tso: Some(50),
            issued_upper_bound: Some(60),
            last_graceful_issued: Some(49),
            lease_expire_at_ms: Some(120),
            updated_at_ms: 33,
        };
        let generators = [
            GeneratorRecord {
                schema_version: 1,
                generator_id: 1,
                owner_worker_endpoint: "worker-a:50051".into(),
                owner_instance_id: "instance-a".into(),
                generator_lease_token: 1,
                lease_expire_at_ms: Some(150),
                last_issued_tso: Some(100),
                issued_upper_bound: Some(120),
                updated_at_ms: 14,
            },
            GeneratorRecord {
                schema_version: 1,
                generator_id: 2,
                owner_worker_endpoint: "worker-a:50051".into(),
                owner_instance_id: "instance-b".into(),
                generator_lease_token: 2,
                lease_expire_at_ms: Some(160),
                last_issued_tso: Some(130),
                issued_upper_bound: Some(140),
                updated_at_ms: 24,
            },
            GeneratorRecord {
                schema_version: 1,
                generator_id: 3,
                owner_worker_endpoint: "worker-b:50051".into(),
                owner_instance_id: "instance-c".into(),
                generator_lease_token: 3,
                lease_expire_at_ms: Some(170),
                last_issued_tso: Some(150),
                issued_upper_bound: Some(160),
                updated_at_ms: 34,
            },
        ];

        for timeline in [&timeline_c, &timeline_b, &timeline_a] {
            metadata
                .create_timeline(&timeline.route.timeline_key, timeline)
                .await
                .unwrap();
        }
        for generator in generators {
            metadata
                .create_generator(generator.generator_id, &generator)
                .await
                .unwrap();
        }

        let rpc = TsoTimelineStatusService::new(service.control_plane());
        let first_page = rpc
            .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
                states: vec![
                    ProtoTimelineState::Recovering as i32,
                    ProtoTimelineState::Draining as i32,
                ],
                owner_worker_endpoint: Some("worker-a:50051".into()),
                page_size: 1,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(first_page.statuses.len(), 1);
        assert_eq!(
            first_page.statuses[0]
                .route
                .as_ref()
                .expect("route should be present")
                .timeline_key,
            "timeline-a"
        );
        assert!(!first_page.next_page_token.is_empty());

        let second_page = rpc
            .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
                states: vec![
                    ProtoTimelineState::Draining as i32,
                    ProtoTimelineState::Recovering as i32,
                    ProtoTimelineState::Recovering as i32,
                ],
                owner_worker_endpoint: Some("worker-a:50051".into()),
                page_size: 1,
                page_token: first_page.next_page_token.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(second_page.statuses.len(), 1);
        let status = &second_page.statuses[0];
        assert_eq!(
            status
                .route
                .as_ref()
                .expect("route should be present")
                .timeline_key,
            "timeline-b"
        );
        assert_eq!(status.state, ProtoTimelineState::Recovering as i32);
        assert_eq!(status.owner_instance_id.as_deref(), Some("instance-b"));
        assert_eq!(
            status.failover_readiness,
            ProtoTimelineFailoverReadiness::Eligible as i32
        );
        assert!(second_page.next_page_token.is_empty());
    }

    #[tokio::test]
    async fn list_timeline_statuses_rejects_unspecified_state_filter() {
        let clock = Arc::new(ManualClock::new(10));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service =
            TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();
        let rpc = TsoTimelineStatusService::new(service.control_plane());

        let error = rpc
            .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
                states: vec![ProtoTimelineState::Unspecified as i32],
                owner_worker_endpoint: None,
                page_size: 0,
                page_token: String::new(),
            }))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn list_timeline_statuses_rejects_filter_mismatched_page_token() {
        let clock = Arc::new(ManualClock::new(10));
        let metadata = Arc::new(MemoryMetadataStore::new());
        let service =
            TsoService::new(required_test_config(TsoConfig::default()), clock, metadata).unwrap();
        let rpc = TsoTimelineStatusService::new(service.control_plane());

        let token = serde_json::to_string(&timeline_status_query::TimelineStatusPageToken {
            version: 1,
            states: vec![ProtoTimelineState::Active as i32],
            owner_worker_endpoint: Some("worker-a:50051".into()),
            last_timeline_key: "timeline-a".into(),
        })
        .unwrap();

        let error = rpc
            .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
                states: vec![ProtoTimelineState::Active as i32],
                owner_worker_endpoint: Some("worker-b:50051".into()),
                page_size: 1,
                page_token: token,
            }))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    #[ignore]
    async fn etcd_list_timeline_statuses_supports_authoritative_inventory_scan() {
        let clock = Arc::new(ManualClock::new(800));
        let metadata = real_etcd_store("list-timeline-statuses").await;
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            clock,
            metadata.clone(),
        )
        .unwrap();
        let timeline_a = TimelineRecord {
            schema_version: 1,
            route: TimelineRoute {
                timeline_key: "timeline-a".into(),
                generator_id: 7,
                owner_worker_endpoint: "worker-a:50051".into(),
                epoch: 1,
                route_version: 1,
                resource_tier: ResourceTier::Shared,
            },
            state: TimelineLifecycleState::Draining,
            recovery_floor_tso: Some(11),
            issued_upper_bound: Some(22),
            last_graceful_issued: Some(10),
            lease_expire_at_ms: Some(200),
            updated_at_ms: 11,
        };
        let timeline_b = TimelineRecord {
            schema_version: 1,
            route: TimelineRoute {
                timeline_key: "timeline-b".into(),
                generator_id: 8,
                owner_worker_endpoint: "worker-a:50051".into(),
                epoch: 2,
                route_version: 2,
                resource_tier: ResourceTier::Warm,
            },
            state: TimelineLifecycleState::Recovering,
            recovery_floor_tso: Some(33),
            issued_upper_bound: Some(44),
            last_graceful_issued: Some(32),
            lease_expire_at_ms: Some(220),
            updated_at_ms: 22,
        };

        metadata
            .create_timeline(&timeline_b.route.timeline_key, &timeline_b)
            .await
            .unwrap();
        metadata
            .create_timeline(&timeline_a.route.timeline_key, &timeline_a)
            .await
            .unwrap();
        for generator in [
            GeneratorRecord {
                schema_version: 1,
                generator_id: 7,
                owner_worker_endpoint: "worker-a:50051".into(),
                owner_instance_id: "instance-a".into(),
                generator_lease_token: 1,
                lease_expire_at_ms: Some(300),
                last_issued_tso: Some(99),
                issued_upper_bound: Some(111),
                updated_at_ms: 15,
            },
            GeneratorRecord {
                schema_version: 1,
                generator_id: 8,
                owner_worker_endpoint: "worker-a:50051".into(),
                owner_instance_id: "instance-b".into(),
                generator_lease_token: 1,
                lease_expire_at_ms: Some(320),
                last_issued_tso: Some(100),
                issued_upper_bound: Some(122),
                updated_at_ms: 25,
            },
        ] {
            metadata
                .create_generator(generator.generator_id, &generator)
                .await
                .unwrap();
        }

        let rpc = TsoTimelineStatusService::new(service.control_plane());
        let response = rpc
            .list_timeline_statuses(Request::new(ListTimelineStatusesRequest {
                states: vec![
                    ProtoTimelineState::Recovering as i32,
                    ProtoTimelineState::Draining as i32,
                ],
                owner_worker_endpoint: Some("worker-a:50051".into()),
                page_size: 10,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();

        let keys: Vec<_> = response
            .statuses
            .iter()
            .map(|status| {
                status
                    .route
                    .as_ref()
                    .expect("route should be present")
                    .timeline_key
                    .clone()
            })
            .collect();
        assert_eq!(
            keys,
            vec!["timeline-a".to_string(), "timeline-b".to_string()]
        );
        assert!(response.next_page_token.is_empty());
    }
}
