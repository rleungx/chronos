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
    async fn timestamp_data_exists(&self) -> Result<bool, TsoError> {
        for prefix in [
            self.route_prefix(),
            self.generator_prefix(),
            self.request_prefix(),
            self.legacy_request_prefix(),
        ] {
            let response = self
                .etcd_get(
                    "cluster_timestamp_data_probe",
                    prefix.into_bytes(),
                    Some(GetOptions::new().with_prefix().with_limit(1)),
                )
                .await
                .map_err(|error| {
                    TsoError::Internal(format!(
                        "Etcd timestamp data compatibility probe failed: {error}"
                    ))
                })?;
            if !response.kvs().is_empty() {
                return Ok(true);
            }
        }
        let status_marker_key = self.timeline_status_index_marker_key();
        let status_response = self
            .etcd_get(
                "cluster_timestamp_status_data_probe",
                self.timeline_status_index_prefix().into_bytes(),
                Some(GetOptions::new().with_prefix().with_limit(2)),
            )
            .await
            .map_err(|error| {
                TsoError::Internal(format!(
                    "Etcd timestamp status compatibility probe failed: {error}"
                ))
            })?;
        if status_response
            .kvs()
            .iter()
            .any(|kv| kv.key() != status_marker_key.as_bytes())
        {
            return Ok(true);
        }
        Ok(false)
    }

    async fn ensure_cluster_format_marker(
        &self,
        timestamp_layout: TimestampLayout,
    ) -> Result<(), TsoError> {
        timestamp_layout
            .validate()
            .map_err(|error| TsoError::Internal(format!("invalid timestamp layout: {error}")))?;
        let format_key = self.cluster_format_key();
        let layout_key = self.cluster_timestamp_layout_key();
        let expected = CURRENT_CLUSTER_FORMAT_VERSION.to_string();
        let expected_layout = serde_json::to_vec(&timestamp_layout).map_err(|error| {
            TsoError::Internal(format!("timestamp layout serialization failed: {error}"))
        })?;
        for _ in 0..8 {
            let cluster_response = self
                .etcd_get(
                    "cluster_format_get",
                    self.cluster_prefix().into_bytes(),
                    Some(GetOptions::new().with_prefix()),
                )
                .await
                .map_err(|error| {
                    TsoError::Internal(format!("Etcd cluster format lookup failed: {error}"))
                })?;
            let format_kv = cluster_response
                .kvs()
                .iter()
                .find(|kv| kv.key() == format_key.as_bytes());
            let layout_kv = cluster_response
                .kvs()
                .iter()
                .find(|kv| kv.key() == layout_key.as_bytes());

            if let Some(kv) = format_kv {
                let actual = std::str::from_utf8(kv.value()).map_err(|error| {
                    TsoError::Internal(format!(
                        "Etcd cluster format marker is not valid UTF-8: {error}"
                    ))
                })?;
                if actual == expected {
                    let stored_layout = layout_kv.ok_or_else(|| {
                        TsoError::Internal(
                            "cluster format v3 is missing cluster/timestamp_layout".into(),
                        )
                    })?;
                    let stored: TimestampLayout = serde_json::from_slice(stored_layout.value())
                        .map_err(|error| {
                            TsoError::Internal(format!("Etcd timestamp layout is invalid: {error}"))
                        })?;
                    stored.validate().map_err(|error| {
                        TsoError::Internal(format!("Etcd timestamp layout is invalid: {error}"))
                    })?;
                    if stored == timestamp_layout {
                        return Ok(());
                    }
                    return Err(TsoError::Internal(format!(
                        "timestamp layout mismatch: configured {:?}, metadata prefix contains {:?}",
                        timestamp_layout, stored
                    )));
                }
                if actual == "2"
                    && layout_kv.is_none()
                    && timestamp_layout == crate::DEFAULT_TIMESTAMP_LAYOUT
                {
                    let migrated = self
                        .etcd_txn(
                            "cluster_format_v2_migrate",
                            Txn::new()
                                .when(vec![
                                    Compare::value(
                                        format_key.as_bytes(),
                                        CompareOp::Equal,
                                        b"2".as_slice(),
                                    ),
                                    Compare::mod_revision(
                                        layout_key.as_bytes(),
                                        CompareOp::Equal,
                                        0,
                                    ),
                                ])
                                .and_then(vec![
                                    TxnOp::put(format_key.as_bytes(), expected.as_bytes(), None),
                                    TxnOp::put(
                                        layout_key.as_bytes(),
                                        expected_layout.clone(),
                                        None,
                                    ),
                                ]),
                        )
                        .await
                        .map_err(|error| {
                            TsoError::Internal(format!(
                                "Etcd cluster format migration failed: {error}"
                            ))
                        })?;
                    if migrated.succeeded() {
                        return Ok(());
                    }
                    continue;
                }
                if actual == "2" && timestamp_layout != crate::DEFAULT_TIMESTAMP_LAYOUT {
                    return Err(TsoError::Internal(
                        "custom timestamp layouts require a new, empty metadata prefix; an existing v2 prefix can only migrate to the default layout"
                            .into(),
                    ));
                }
                return Err(TsoError::Internal(format!(
                    "cluster format mismatch: binary requires v{}, metadata prefix contains v{}; downgrade and mixed-format startup are forbidden",
                    CURRENT_CLUSTER_FORMAT_VERSION, actual
                )));
            }

            if layout_kv.is_some() {
                return Err(TsoError::Internal(
                    "metadata prefix contains a timestamp layout without a cluster format marker"
                        .into(),
                ));
            }
            if timestamp_layout != crate::DEFAULT_TIMESTAMP_LAYOUT
                && self.timestamp_data_exists().await?
            {
                return Err(TsoError::Internal(
                    "custom timestamp layouts require a new, empty metadata prefix; existing timestamp-bearing metadata was found"
                        .into(),
                ));
            }

            let created = self
                .etcd_txn(
                    "cluster_format_create",
                    Txn::new()
                        .when(vec![
                            Compare::mod_revision(format_key.as_bytes(), CompareOp::Equal, 0),
                            Compare::mod_revision(layout_key.as_bytes(), CompareOp::Equal, 0),
                        ])
                        .and_then(vec![
                            TxnOp::put(format_key.as_bytes(), expected.as_bytes(), None),
                            TxnOp::put(layout_key.as_bytes(), expected_layout.clone(), None),
                        ]),
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
        self.initialize_cluster_format_and_indexes_with_layout(crate::DEFAULT_TIMESTAMP_LAYOUT)
            .await
    }

    pub async fn initialize_cluster_format_and_indexes_with_layout(
        &self,
        timestamp_layout: TimestampLayout,
    ) -> Result<(), TsoError> {
        active_identity_formats_are_compatible(&self.active_identity_records().await?)?;
        self.ensure_cluster_format_marker(timestamp_layout).await?;
        // Recheck after publishing the marker so a legacy worker observed during startup cannot be
        // hidden by the marker itself. Deployment guards prevent old binaries from being launched
        // after this point; old binaries must never be used to downgrade an upgraded prefix.
        active_identity_formats_are_compatible(&self.active_identity_records().await?)?;
        self.rebuild_timeline_status_indexes().await
    }
}
