use etcd_client::{Client, Compare, CompareOp, PutOptions, Txn, TxnOp};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Duration};
use tracing::warn;

use crate::TsoError;

use super::EtcdMetadataStore;

pub(super) struct EtcdLeaseLock {
    key: String,
    token: Vec<u8>,
    lease_id: i64,
    keep_alive_task: Option<JoinHandle<()>>,
    lost_rx: watch::Receiver<bool>,
    revoke_client: Client,
}

impl EtcdLeaseLock {
    pub(super) fn fence_compare(&self) -> Compare {
        Compare::value(self.key.as_bytes(), CompareOp::Equal, self.token.clone())
    }

    pub(super) fn key(&self) -> &str {
        &self.key
    }

    pub(super) fn token(&self) -> &[u8] {
        &self.token
    }

    pub(super) fn lease_id(&self) -> i64 {
        self.lease_id
    }

    pub(super) fn is_known_lost(&self) -> bool {
        *self.lost_rx.borrow()
    }

    pub(super) async fn release(mut self) -> Result<(), etcd_client::Error> {
        if let Some(task) = self.keep_alive_task.take() {
            task.abort();
        }
        self.revoke_client.lease_revoke(self.lease_id).await?;
        Ok(())
    }
}

impl Drop for EtcdLeaseLock {
    fn drop(&mut self) {
        if let Some(task) = self.keep_alive_task.take() {
            task.abort();
        }
    }
}

impl EtcdMetadataStore {
    pub(super) async fn try_acquire_lease_lock(
        &self,
        key: String,
        ttl_seconds: i64,
        mut additional_conditions: Vec<Compare>,
        operation: &'static str,
    ) -> Result<Option<EtcdLeaseLock>, TsoError> {
        let ttl_seconds = ttl_seconds.max(3);
        let mut lease_client = self.client.clone();
        let lease_id = self
            .etcd_lease_grant_for("lease_lock_grant", ttl_seconds)
            .await
            .map_err(|error| {
                TsoError::Internal(format!("Etcd {operation} lock lease grant failed: {error}"))
            })?
            .id();
        let token = lease_id.to_string().into_bytes();
        additional_conditions.push(Compare::create_revision(
            key.as_bytes(),
            CompareOp::Equal,
            0,
        ));

        let response = self
            .etcd_txn(
                operation,
                Txn::new()
                    .when(additional_conditions)
                    .and_then(vec![TxnOp::put(
                        key.as_bytes(),
                        token.clone(),
                        Some(PutOptions::new().with_lease(lease_id)),
                    )]),
            )
            .await;
        let acquired = matches!(response, Ok(ref response) if response.succeeded());
        if !acquired {
            if let Err(error) = lease_client.lease_revoke(lease_id).await {
                warn!(
                    component = "metadata",
                    event = "lease_lock_revoke_failed",
                    result = "degraded",
                    reason = %error,
                    operation,
                    lease_id
                );
            }
            if let Err(error) = response {
                return Err(TsoError::Internal(format!(
                    "Etcd {operation} lock transaction failed: {error}"
                )));
            }
            return Ok(None);
        }

        let keepalive = self
            .etcd_lease_keep_alive_for("lease_lock_keepalive_open", lease_id)
            .await;
        let (mut keeper, mut stream) = match keepalive {
            Ok(keepalive) => keepalive,
            Err(error) => {
                let _ = lease_client.lease_revoke(lease_id).await;
                return Err(TsoError::Internal(format!(
                    "Etcd {operation} lock keepalive failed: {error}"
                )));
            }
        };

        let (lost_tx, lost_rx) = watch::channel(false);
        let heartbeat = Duration::from_secs((ttl_seconds / 3).max(1) as u64);
        let keep_alive_task = tokio::spawn(async move {
            loop {
                let send = timeout(heartbeat, keeper.keep_alive()).await;
                if !matches!(send, Ok(Ok(()))) {
                    break;
                }
                let response = timeout(heartbeat, stream.message()).await;
                match response {
                    Ok(Ok(Some(response))) if response.ttl() > 0 => sleep(heartbeat).await,
                    _ => break,
                }
            }
            let _ = lost_tx.send(true);
        });

        Ok(Some(EtcdLeaseLock {
            key,
            token,
            lease_id,
            keep_alive_task: Some(keep_alive_task),
            lost_rx,
            revoke_client: lease_client,
        }))
    }

    pub(super) async fn lease_lock_is_owned(&self, lock: &EtcdLeaseLock) -> Result<bool, TsoError> {
        if lock.is_known_lost() {
            return Ok(false);
        }
        let response = self
            .etcd_get("lease_lock_verify", lock.key().as_bytes().to_vec(), None)
            .await
            .map_err(|error| {
                TsoError::Internal(format!("Etcd lease lock verification failed: {error}"))
            })?;
        Ok(response
            .kvs()
            .first()
            .is_some_and(|kv| kv.value() == lock.token() && kv.lease() == lock.lease_id()))
    }

    pub(super) async fn release_lease_lock(&self, lock: EtcdLeaseLock, operation: &'static str) {
        let lease_id = lock.lease_id();
        if let Err(error) = lock.release().await {
            warn!(
                component = "metadata",
                event = "lease_lock_revoke_failed",
                result = "degraded",
                reason = %error,
                operation,
                lease_id
            );
        }
    }
}
