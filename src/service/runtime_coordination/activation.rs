use crate::lifecycle::{TimelineLifecycleContract, TimelineServingReadiness};
use crate::metadata::TimelineRecord;
use crate::planning::recovered_timeline_floor_tso;
use crate::{TimelineLifecycleState, TsoError};
use tokio::time::Instant;

use super::super::TsoService;

impl TsoService {
    pub(in crate::service) async fn activate_local_timeline_record(
        &self,
        timeline_key: &str,
        mut record: TimelineRecord,
        mut revision: u64,
    ) -> Result<(TimelineRecord, u64), TsoError> {
        let deadline = Instant::now() + self.metadata_contention_retry_budget();
        let mut contention_retries: u32 = 0;
        loop {
            if !self.is_local_endpoint(&record.route.owner_worker_endpoint) {
                return Ok((record, revision));
            }
            let lifecycle = TimelineLifecycleContract::classify(record.state);
            match lifecycle.serving_readiness() {
                TimelineServingReadiness::NotReady => return Ok((record, revision)),
                TimelineServingReadiness::Serving => {
                    self.ensure_generator_lease(record.route.generator_id)
                        .await?;
                    return Ok((record, revision));
                }
                TimelineServingReadiness::RequiresActivation => {
                    debug_assert!(lifecycle.should_activate_locally());
                    if let Some(recovery_floor_tso) = recovered_timeline_floor_tso(&record) {
                        let recovery_physical_ms =
                            crate::decode_tso(recovery_floor_tso).physical_ms;
                        let now_ms = self.clock.now_ms();
                        if recovery_physical_ms
                            > now_ms.saturating_add(self.config.recovery_catchup_budget_ms)
                        {
                            return Ok((record, revision));
                        }
                    }
                    self.ensure_generator_lease(record.route.generator_id)
                        .await?;
                }
            }

            let mut activated_record = record.clone();
            activated_record.state = TimelineLifecycleState::Active;
            activated_record.updated_at_ms = self.clock.now_ms();
            match self
                .metadata
                .compare_exchange_timeline(timeline_key, revision, &activated_record)
                .await
            {
                Ok(new_revision) => return Ok((activated_record, new_revision)),
                Err(TsoError::CasFailed) => {
                    contention_retries = contention_retries.saturating_add(1);
                    self.backoff_after_metadata_contention(contention_retries, deadline)
                        .await?;
                    let (latest, latest_revision) = self
                        .load_timeline_with_singleflight(timeline_key)
                        .await?
                        .ok_or_else(|| TsoError::TimelineNotFound {
                            timeline_key: timeline_key.to_owned(),
                        })?;
                    record = latest;
                    revision = latest_revision;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::sync::broadcast;
    use tokio::time::{timeout, Duration};

    use crate::metadata::{
        ControlPlaneStore, GeneratorBatchOp, GeneratorLeaseAuthority, GeneratorRecord,
        RouteUpdateSource, TimelineAuthority, TimelineBatchOp, TimelineRecord,
    };
    use crate::{ManualClock, ResourceTier, TimelineRoute, TsoConfig, TsoSecurityMode, TsoService};

    use super::*;

    fn required_test_config(config: TsoConfig) -> TsoConfig {
        TsoConfig {
            security_mode: Some(TsoSecurityMode::Required),
            grpc_tls_cert_file: Some("server.crt".into()),
            grpc_tls_key_file: Some("server.key".into()),
            grpc_client_ca_file: Some("ca.pem".into()),
            grpc_request_timeout_ms: Some(1),
            grpc_max_request_bytes: Some(1024),
            grpc_max_concurrent_requests: Some(16),
            advertise_endpoint: "127.0.0.1:50052".into(),
            instance_id: "activation-instance".into(),
            ..config
        }
    }

    #[derive(Clone)]
    struct AlwaysCasFailActivationStore {
        record: TimelineRecord,
    }

    #[async_trait]
    impl TimelineAuthority for AlwaysCasFailActivationStore {
        async fn load_timeline(
            &self,
            _timeline_key: &str,
        ) -> Result<Option<(TimelineRecord, u64)>, TsoError> {
            Ok(Some((self.record.clone(), 1)))
        }

        async fn list_timelines(&self) -> Result<Vec<TimelineRecord>, TsoError> {
            Ok(vec![self.record.clone()])
        }

        async fn create_timeline(
            &self,
            _timeline_key: &str,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_timeline(
            &self,
            _timeline_key: &str,
            _expected_revision: u64,
            _record: &TimelineRecord,
        ) -> Result<u64, TsoError> {
            Err(TsoError::CasFailed)
        }

        async fn compare_exchange_timelines(
            &self,
            _operations: &[TimelineBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            Err(TsoError::CasFailed)
        }
    }

    #[async_trait]
    impl GeneratorLeaseAuthority for AlwaysCasFailActivationStore {
        async fn load_generator(
            &self,
            generator_id: u32,
        ) -> Result<Option<(GeneratorRecord, u64)>, TsoError> {
            Ok(Some((
                GeneratorRecord {
                    schema_version: 1,
                    generator_id,
                    owner_worker_endpoint: "127.0.0.1:50052".into(),
                    owner_instance_id: "activation-instance".into(),
                    generator_lease_token: 1,
                    lease_expire_at_ms: Some(u64::MAX / 4),
                    last_issued_tso: None,
                    issued_upper_bound: None,
                    updated_at_ms: 0,
                },
                1,
            )))
        }

        async fn create_generator(
            &self,
            _generator_id: u32,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_generator(
            &self,
            _generator_id: u32,
            _expected_revision: u64,
            _record: &GeneratorRecord,
        ) -> Result<u64, TsoError> {
            Ok(1)
        }

        async fn compare_exchange_generators(
            &self,
            _operations: &[GeneratorBatchOp],
        ) -> Result<Vec<u64>, TsoError> {
            Ok(Vec::new())
        }
    }

    impl RouteUpdateSource for AlwaysCasFailActivationStore {
        fn subscribe_route_updates(
            &self,
        ) -> broadcast::Receiver<crate::metadata::RouteUpdateSignal> {
            let (_tx, rx) = broadcast::channel(1);
            rx
        }
    }

    #[async_trait]
    impl ControlPlaneStore for AlwaysCasFailActivationStore {}

    #[tokio::test]
    async fn activate_local_timeline_record_retries_are_bounded_under_cas_contention() {
        let route = TimelineRoute {
            timeline_key: "activation.retry.bound.timeline".into(),
            generator_id: 0,
            owner_worker_endpoint: "127.0.0.1:50052".into(),
            epoch: 1,
            route_version: 1,
            resource_tier: ResourceTier::Shared,
        };
        let record = TimelineRecord {
            schema_version: 1,
            route: route.clone(),
            state: TimelineLifecycleState::Recovering,
            recovery_floor_tso: None,
            issued_upper_bound: None,
            last_graceful_issued: None,
            lease_expire_at_ms: None,
            updated_at_ms: 0,
        };
        let service = TsoService::new(
            required_test_config(TsoConfig::default()),
            Arc::new(ManualClock::new(60_000)),
            Arc::new(AlwaysCasFailActivationStore {
                record: record.clone(),
            }),
        )
        .unwrap();

        service.background.begin_shutdown();
        service.background.drain_tasks().await;

        let result = timeout(
            Duration::from_millis(200),
            service.activate_local_timeline_record(&route.timeline_key, record, 1),
        )
        .await
        .expect("activation should stop retrying within the request budget")
        .expect_err("contention exhaustion should surface as CasFailed");
        assert!(matches!(result, TsoError::CasFailed));
    }
}
