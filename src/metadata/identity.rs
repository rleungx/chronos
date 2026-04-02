use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, PutOptions, Txn, TxnOp};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration};
use tracing::{info, warn};

use crate::TsoError;

pub struct InstanceIdentityLease {
    lost_rx: watch::Receiver<bool>,
    keep_alive_task: Option<JoinHandle<()>>,
    revoke_client: Option<Box<dyn IdentityLeaseRevoker>>,
    lease_id: i64,
    released: bool,
}

const IDENTITY_LEASE_REVOKE_TIMEOUT: Duration = Duration::from_secs(2);

impl InstanceIdentityLease {
    pub fn lost_receiver(&self) -> watch::Receiver<bool> {
        self.lost_rx.clone()
    }

    pub async fn shutdown(&mut self) {
        self.shutdown_with_timeout(IDENTITY_LEASE_REVOKE_TIMEOUT)
            .await;
    }

    pub(super) fn new(
        lost_rx: watch::Receiver<bool>,
        keep_alive_task: JoinHandle<()>,
        revoke_client: Client,
        lease_id: i64,
    ) -> Self {
        Self {
            lost_rx,
            keep_alive_task: Some(keep_alive_task),
            revoke_client: Some(Box::new(revoke_client)),
            lease_id,
            released: false,
        }
    }

    async fn shutdown_with_timeout(&mut self, revoke_timeout: Duration) {
        if self.released {
            return;
        }

        if let Some(keep_alive_task) = self.keep_alive_task.take() {
            keep_alive_task.abort();
            match timeout(revoke_timeout, keep_alive_task).await {
                Ok(Ok(())) | Ok(Err(_)) => {}
                Err(_) => {
                    warn!(
                        component = "shutdown",
                        event = "identity_revoke_result",
                        result = "degraded",
                        reason = "keepalive_abort_timeout",
                        lease_id = self.lease_id,
                        timeout_ms = revoke_timeout.as_millis()
                    );
                }
            }
        }

        if let Some(mut revoke_client) = self.revoke_client.take() {
            info!(
                component = "shutdown",
                event = "identity_revoke_result",
                result = "success",
                reason = "revoke_started",
                lease_id = self.lease_id
            );
            match tokio::time::timeout(revoke_timeout, revoke_client.revoke(self.lease_id)).await {
                Ok(Ok(())) => {
                    info!(
                        component = "shutdown",
                        event = "identity_revoke_result",
                        result = "success",
                        reason = "revoke_succeeded",
                        lease_id = self.lease_id
                    );
                }
                Ok(Err(error)) => {
                    warn!(
                        component = "shutdown",
                        event = "identity_revoke_result",
                        result = "degraded",
                        reason = "revoke_failed",
                        lease_id = self.lease_id,
                        error = %error
                    );
                }
                Err(_) => {
                    warn!(
                        component = "shutdown",
                        event = "identity_revoke_result",
                        result = "degraded",
                        reason = "revoke_timeout",
                        lease_id = self.lease_id,
                        timeout_ms = revoke_timeout.as_millis()
                    );
                }
            }
        }

        self.released = true;
    }
}

impl Drop for InstanceIdentityLease {
    fn drop(&mut self) {
        if let Some(keep_alive_task) = self.keep_alive_task.take() {
            keep_alive_task.abort();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct InstanceIdentityLeaseRecord {
    pub(super) instance_id: String,
    pub(super) worker_id: String,
    pub(super) advertise_endpoint: String,
}

#[async_trait]
pub(super) trait IdentityLeaseTxnRunner {
    async fn put_identity_if_absent_with_lease(
        &mut self,
        key: &[u8],
        value: Vec<u8>,
        lease_id: i64,
    ) -> Result<bool, TsoError>;

    async fn revoke_identity_lease(&mut self, lease_id: i64);
}

#[async_trait]
trait IdentityLeaseRevoker: Send + Sync {
    async fn revoke(&mut self, lease_id: i64) -> Result<(), TsoError>;
}

#[async_trait]
impl IdentityLeaseTxnRunner for Client {
    async fn put_identity_if_absent_with_lease(
        &mut self,
        key: &[u8],
        value: Vec<u8>,
        lease_id: i64,
    ) -> Result<bool, TsoError> {
        let txn = Txn::new()
            .when(vec![Compare::create_revision(key, CompareOp::Equal, 0)])
            .and_then(vec![TxnOp::put(
                key,
                value,
                Some(PutOptions::new().with_lease(lease_id)),
            )]);
        let response = self.txn(txn).await.map_err(|error| {
            TsoError::Internal(format!("Etcd identity lease txn failed: {}", error))
        })?;
        Ok(response.succeeded())
    }

    async fn revoke_identity_lease(&mut self, lease_id: i64) {
        let _ = self.lease_revoke(lease_id).await;
    }
}

#[async_trait]
impl IdentityLeaseRevoker for Client {
    async fn revoke(&mut self, lease_id: i64) -> Result<(), TsoError> {
        self.lease_revoke(lease_id)
            .await
            .map(|_| ())
            .map_err(|error| {
                TsoError::Internal(format!("Etcd identity lease revoke failed: {}", error))
            })
    }
}

pub(super) async fn claim_instance_identity<R: IdentityLeaseTxnRunner + ?Sized>(
    runner: &mut R,
    key: &[u8],
    value: Vec<u8>,
    lease_id: i64,
    instance_id: &str,
) -> Result<(), TsoError> {
    let succeeded = runner
        .put_identity_if_absent_with_lease(key, value, lease_id)
        .await?;

    if !succeeded {
        runner.revoke_identity_lease(lease_id).await;
        return Err(TsoError::InstanceIdentityInUse {
            instance_id: instance_id.to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        claim_instance_identity, IdentityLeaseRevoker, IdentityLeaseTxnRunner,
        InstanceIdentityLease,
    };
    use crate::TsoError;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::watch;
    use tokio::time::Duration;

    struct FakeIdentityLeaseTxnRunner {
        put_results: Vec<bool>,
        revoked_leases: Vec<i64>,
    }

    struct FakeIdentityLeaseRevoker {
        revoked_leases: Arc<StdMutex<Vec<i64>>>,
        sleep_for: Duration,
    }

    #[async_trait]
    impl IdentityLeaseTxnRunner for FakeIdentityLeaseTxnRunner {
        async fn put_identity_if_absent_with_lease(
            &mut self,
            _key: &[u8],
            _value: Vec<u8>,
            _lease_id: i64,
        ) -> Result<bool, TsoError> {
            Ok(self.put_results.remove(0))
        }

        async fn revoke_identity_lease(&mut self, lease_id: i64) {
            self.revoked_leases.push(lease_id);
        }
    }

    #[async_trait]
    impl IdentityLeaseRevoker for FakeIdentityLeaseRevoker {
        async fn revoke(&mut self, lease_id: i64) -> Result<(), TsoError> {
            tokio::time::sleep(self.sleep_for).await;
            self.revoked_leases.lock().unwrap().push(lease_id);
            Ok(())
        }
    }

    #[tokio::test]
    async fn claim_instance_identity_succeeds_when_key_is_absent() {
        let mut runner = FakeIdentityLeaseTxnRunner {
            put_results: vec![true],
            revoked_leases: Vec::new(),
        };

        claim_instance_identity(
            &mut runner,
            b"/chronos/identity/instance-a",
            br#"{"instance_id":"instance-a"}"#.to_vec(),
            42,
            "instance-a",
        )
        .await
        .expect("identity claim should succeed");

        assert!(runner.revoked_leases.is_empty());
    }

    #[tokio::test]
    async fn claim_instance_identity_rejects_duplicate_and_revokes_lease() {
        let mut runner = FakeIdentityLeaseTxnRunner {
            put_results: vec![false],
            revoked_leases: Vec::new(),
        };

        let error = claim_instance_identity(
            &mut runner,
            b"/chronos/identity/instance-a",
            br#"{"instance_id":"instance-a"}"#.to_vec(),
            7,
            "instance-a",
        )
        .await
        .expect_err("identity claim should fail when key already exists");

        assert!(matches!(
            error,
            TsoError::InstanceIdentityInUse { ref instance_id } if instance_id == "instance-a"
        ));
        assert_eq!(runner.revoked_leases, vec![7]);
    }

    #[tokio::test]
    async fn instance_identity_lease_shutdown_is_idempotent_without_revoke_client() {
        let (_lost_tx, lost_rx) = watch::channel(false);
        let keep_alive_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let mut lease = InstanceIdentityLease {
            lost_rx,
            keep_alive_task: Some(keep_alive_task),
            revoke_client: None,
            lease_id: 0,
            released: false,
        };

        lease.shutdown().await;
        lease.shutdown().await;
    }

    #[tokio::test]
    async fn instance_identity_lease_shutdown_times_out_slow_revoke_and_returns() {
        let (_lost_tx, lost_rx) = watch::channel(false);
        let revoked_leases = Arc::new(StdMutex::new(Vec::new()));
        let mut lease = InstanceIdentityLease {
            lost_rx,
            keep_alive_task: None,
            revoke_client: Some(Box::new(FakeIdentityLeaseRevoker {
                revoked_leases: revoked_leases.clone(),
                sleep_for: Duration::from_millis(50),
            })),
            lease_id: 11,
            released: false,
        };

        lease.shutdown_with_timeout(Duration::from_millis(5)).await;

        assert!(lease.released);
        assert!(revoked_leases.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn instance_identity_lease_shutdown_revokes_active_lease() {
        let (_lost_tx, lost_rx) = watch::channel(false);
        let revoked_leases = Arc::new(StdMutex::new(Vec::new()));
        let mut lease = InstanceIdentityLease {
            lost_rx,
            keep_alive_task: None,
            revoke_client: Some(Box::new(FakeIdentityLeaseRevoker {
                revoked_leases: revoked_leases.clone(),
                sleep_for: Duration::from_millis(0),
            })),
            lease_id: 17,
            released: false,
        };

        lease.shutdown_with_timeout(Duration::from_millis(20)).await;

        assert_eq!(revoked_leases.lock().unwrap().as_slice(), &[17]);
    }

    #[tokio::test]
    async fn instance_identity_lease_shutdown_does_not_hang_on_aborted_keepalive_task() {
        let (_lost_tx, lost_rx) = watch::channel(false);
        let keep_alive_task = tokio::task::spawn_blocking(|| {
            std::thread::sleep(std::time::Duration::from_millis(100));
        });
        let mut lease = InstanceIdentityLease {
            lost_rx,
            keep_alive_task: Some(keep_alive_task),
            revoke_client: None,
            lease_id: 23,
            released: false,
        };

        tokio::time::timeout(
            Duration::from_millis(20),
            lease.shutdown_with_timeout(Duration::from_millis(5)),
        )
        .await
        .expect("shutdown should return even if aborted keepalive task lingers");
        assert!(lease.released);
    }
}
