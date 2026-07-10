use super::*;

pub(super) fn active_identity_formats_are_compatible(
    identities: &[InstanceIdentityLeaseRecord],
) -> Result<(), TsoError> {
    let incompatible = identities
        .iter()
        .filter(|identity| identity.cluster_format_version != CURRENT_CLUSTER_FORMAT_VERSION)
        .map(|identity| {
            format!(
                "{}:{}=v{}",
                identity.worker_id, identity.instance_id, identity.cluster_format_version
            )
        })
        .collect::<Vec<_>>();
    if incompatible.is_empty() {
        return Ok(());
    }
    Err(TsoError::Internal(format!(
        "active workers use an incompatible cluster format; expected v{}, observed [{}]. Quiesce traffic, stop every old worker, wait for identity leases to expire, then start the new version",
        CURRENT_CLUSTER_FORMAT_VERSION,
        incompatible.join(", ")
    )))
}

impl EtcdMetadataStore {
    async fn ensure_cluster_format_marker(&self) -> Result<(), TsoError> {
        let key = self.cluster_format_key();
        let expected = CURRENT_CLUSTER_FORMAT_VERSION.to_string();
        for _ in 0..8 {
            let response = self
                .etcd_get("cluster_format_get", key.as_bytes().to_vec(), None)
                .await
                .map_err(|error| {
                    TsoError::Internal(format!("Etcd cluster format lookup failed: {error}"))
                })?;
            if let Some(kv) = response.kvs().first() {
                let actual = std::str::from_utf8(kv.value()).map_err(|error| {
                    TsoError::Internal(format!(
                        "Etcd cluster format marker is not valid UTF-8: {error}"
                    ))
                })?;
                if actual == expected {
                    return Ok(());
                }
                return Err(TsoError::Internal(format!(
                    "cluster format mismatch: binary requires v{}, metadata prefix contains v{}; downgrade and mixed-format startup are forbidden",
                    CURRENT_CLUSTER_FORMAT_VERSION, actual
                )));
            }

            let created = self
                .etcd_txn(
                    "cluster_format_create",
                    Txn::new()
                        .when(vec![Compare::mod_revision(
                            key.as_bytes(),
                            CompareOp::Equal,
                            0,
                        )])
                        .and_then(vec![TxnOp::put(key.as_bytes(), expected.as_bytes(), None)]),
                )
                .await
                .map_err(|error| {
                    TsoError::Internal(format!("Etcd cluster format create failed: {error}"))
                })?;
            if created.succeeded() {
                return Ok(());
            }
        }
        Err(TsoError::Internal(
            "cluster format marker remained contended after 8 attempts".into(),
        ))
    }

    pub async fn initialize_cluster_format_and_indexes(&self) -> Result<(), TsoError> {
        active_identity_formats_are_compatible(&self.active_identity_records().await?)?;
        self.ensure_cluster_format_marker().await?;
        // Recheck after publishing the marker so a legacy worker observed during startup cannot be
        // hidden by the marker itself. Deployment guards prevent old binaries from being launched
        // after this point; old binaries must never be used to downgrade an upgraded prefix.
        active_identity_formats_are_compatible(&self.active_identity_records().await?)?;
        self.rebuild_timeline_status_indexes().await
    }
}
