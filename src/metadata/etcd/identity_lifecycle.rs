use super::*;

use std::future::Future;

use tokio::time::error::Elapsed;
use tokio::time::timeout_at;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct IdentityLeaseConfirmedWindow {
    pub(super) deadline: Instant,
    pub(super) heartbeat_interval: Duration,
}

pub(super) fn identity_lease_confirmed_window(
    request_started_at: Instant,
    response_observed_at: Instant,
    confirmed_ttl_seconds: i64,
) -> Result<IdentityLeaseConfirmedWindow, TsoError> {
    let confirmed_ttl_seconds = u64::try_from(confirmed_ttl_seconds)
        .ok()
        .filter(|ttl| *ttl > 0)
        .ok_or_else(|| {
            TsoError::Internal(format!(
                "Etcd identity lease response returned non-positive TTL: {}",
                confirmed_ttl_seconds
            ))
        })?;
    let confirmed_ttl = Duration::from_secs(confirmed_ttl_seconds);
    let deadline = request_started_at
        .checked_add(confirmed_ttl)
        .ok_or_else(|| {
            TsoError::Internal(format!(
                "Etcd identity lease response TTL {}s exceeds the local monotonic clock range",
                confirmed_ttl_seconds
            ))
        })?;
    if deadline <= response_observed_at {
        return Err(TsoError::Internal(format!(
            "Etcd identity lease response TTL {}s was already exhausted before confirmation",
            confirmed_ttl_seconds
        )));
    }
    let remaining_ttl = deadline.duration_since(response_observed_at);
    Ok(IdentityLeaseConfirmedWindow {
        deadline,
        heartbeat_interval: remaining_ttl / 3,
    })
}

pub(super) async fn validate_identity_lease_grant_or_revoke<F, Fut>(
    lease_id: i64,
    request_started_at: Instant,
    response_observed_at: Instant,
    granted_ttl_seconds: i64,
    revoke: F,
) -> Result<IdentityLeaseConfirmedWindow, TsoError>
where
    F: FnOnce(i64) -> Fut,
    Fut: Future<Output = ()>,
{
    match identity_lease_confirmed_window(
        request_started_at,
        response_observed_at,
        granted_ttl_seconds,
    ) {
        Ok(window) => Ok(window),
        Err(error) => {
            revoke(lease_id).await;
            Err(error)
        }
    }
}

pub(super) async fn await_identity_keepalive_step<T>(
    lease_alive_until: Instant,
    future: impl Future<Output = T>,
) -> Result<T, Elapsed> {
    timeout_at(lease_alive_until, future).await
}

pub(super) async fn await_identity_keepalive_reconnect<T>(
    lease_alive_until: Instant,
    backoff: Duration,
    reconnect: impl Future<Output = T>,
) -> Result<T, Elapsed> {
    timeout_at(lease_alive_until, async {
        sleep(backoff).await;
        reconnect.await
    })
    .await
}

impl EtcdMetadataStore {
    async fn claimed_instance_identity_matches(
        &self,
        key: &str,
        lease_id: i64,
        expected_record: &InstanceIdentityLeaseRecord,
    ) -> Result<bool, TsoError> {
        let response = self
            .etcd_get("identity_lease_claim_verify", key.to_owned(), None)
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["identity_lease_claim_verify"])
                    .inc();
                TsoError::Internal(format!(
                    "Etcd identity lease claim verification lookup failed: {}",
                    error
                ))
            })?;

        let Some(kv) = response.kvs().first() else {
            return Ok(false);
        };
        identity_claim_matches_record(lease_id, kv.lease(), kv.value(), expected_record)
    }

    async fn claim_instance_identity_with_lease(
        &self,
        key: &str,
        value: Vec<u8>,
        lease_id: i64,
        record: &InstanceIdentityLeaseRecord,
    ) -> Result<(), TsoError> {
        let txn = Txn::new()
            .when(vec![Compare::create_revision(
                key.as_bytes(),
                CompareOp::Equal,
                0,
            )])
            .and_then(vec![TxnOp::put(
                key.as_bytes(),
                value,
                Some(PutOptions::new().with_lease(lease_id)),
            )]);
        match self.etcd_txn("identity_lease_claim", txn).await {
            Ok(response) if response.succeeded() => return Ok(()),
            Ok(_) => {
                // A previous retry may have committed the claim but lost the response.
                // Treat only the exact lease/payload match as success; every other
                // record still means the instance identity is occupied.
                if self
                    .claimed_instance_identity_matches(key, lease_id, record)
                    .await?
                {
                    return Ok(());
                }
            }
            Err(error) => {
                // Fail closed on uncertain errors, but first preserve a successfully
                // committed identity claim if etcd already attached this lease.
                if self
                    .claimed_instance_identity_matches(key, lease_id, record)
                    .await?
                {
                    return Ok(());
                }

                let mut revoke_client = self.client.clone();
                let _ = revoke_client.lease_revoke(lease_id).await;
                return Err(TsoError::Internal(format!(
                    "Etcd identity lease txn failed: {}",
                    error
                )));
            }
        }

        let mut revoke_client = self.client.clone();
        let _ = revoke_client.lease_revoke(lease_id).await;
        Err(TsoError::InstanceIdentityInUse {
            instance_id: record.instance_id.clone(),
        })
    }

    pub(in crate::metadata::etcd) async fn active_identity_records(
        &self,
    ) -> Result<Vec<InstanceIdentityLeaseRecord>, TsoError> {
        let prefix = self.instance_identity_prefix();
        let response = self
            .etcd_get(
                "list_identity_leases",
                prefix,
                Some(GetOptions::new().with_prefix()),
            )
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["list_identity_leases"])
                    .inc();
                TsoError::Internal(format!("Etcd list_identity_leases failed: {}", error))
            })?;

        let mut records = Vec::new();
        for kv in response.kvs() {
            if kv.lease() == 0 {
                continue;
            }
            let record: InstanceIdentityLeaseRecord =
                serde_json::from_slice(kv.value()).map_err(|error| {
                    metrics::TSO_METADATA_ERRORS_TOTAL
                        .with_label_values(&["list_identity_leases"])
                        .inc();
                    TsoError::Internal(format!(
                        "Identity lease record deserialization failed: {}",
                        error
                    ))
                })?;
            records.push(record);
        }
        Ok(records)
    }

    pub async fn admit_ownership_plan_member(&self, config: &TsoConfig) -> Result<(), TsoError> {
        if config.generator_ownership_modulo <= 1 {
            return Ok(());
        }

        let expected_plan_id = config.ownership_plan_id.trim();
        let expected_modulo = config.generator_ownership_modulo;
        let members = config
            .effective_generator_ownership_remainders()
            .into_iter()
            .map(|remainder| OwnershipPlanMember {
                remainder,
                worker_id: config.worker_id.clone(),
                advertise_endpoint: config.advertise_endpoint.clone(),
            })
            .collect::<Vec<_>>();
        let member_key = (config.worker_id.clone(), config.advertise_endpoint.clone());
        let key = self.ownership_plan_key();

        for _attempt in 0..8 {
            let now_ms = Self::ownership_plan_timestamp_ms();
            let active_identities = self.active_identity_records().await?;
            let active_member_count = active_identities
                .iter()
                .filter(|identity| {
                    identity.worker_id == member_key.0
                        && identity.advertise_endpoint == member_key.1
                })
                .count();
            if active_member_count > 1 {
                return Err(TsoError::Internal(format!(
                    "ownership plan worker_id={} advertise_endpoint={} has multiple active identity leases",
                    config.worker_id, config.advertise_endpoint
                )));
            }
            let identity_declares_expected_plan = active_identities.iter().any(|identity| {
                identity.worker_id == member_key.0
                    && identity.advertise_endpoint == member_key.1
                    && identity.ownership_plan_id == expected_plan_id
                    && identity.ownership_modulo == expected_modulo
            });
            if !identity_declares_expected_plan {
                return Err(TsoError::Internal(format!(
                    "active identity for worker_id={} advertise_endpoint={} does not declare ownership plan_id={} modulo={}",
                    config.worker_id,
                    config.advertise_endpoint,
                    expected_plan_id,
                    expected_modulo
                )));
            }
            match self
                .get_json_record::<OwnershipPlanRecord>(
                    key.clone(),
                    "get_ownership_plan",
                    "Ownership plan record deserialization",
                )
                .await?
            {
                Some((mut record, revision)) => {
                    let active_members = active_identities
                        .iter()
                        .filter(|identity| {
                            identity_record_belongs_to_ownership_plan(
                                identity,
                                &record.plan_id,
                                record.modulo,
                            )
                        })
                        .map(|identity| {
                            (
                                identity.worker_id.clone(),
                                identity.advertise_endpoint.clone(),
                            )
                        })
                        .collect::<HashSet<_>>();
                    let pruned = record.prune_inactive_members(&active_members, now_ms)?;
                    if pruned > 0 {
                        info!(
                            component = "metadata",
                            event = "ownership_plan_pruned",
                            result = "success",
                            reason = "inactive_identity",
                            pruned_members = pruned,
                            ownership_plan_id = %expected_plan_id
                        );
                    }
                    let replaced =
                        record.replace_if_empty(expected_plan_id, expected_modulo, now_ms)?;
                    if replaced {
                        info!(
                            component = "metadata",
                            event = "ownership_plan_replaced",
                            result = "success",
                            reason = "all_previous_members_inactive",
                            ownership_plan_id = %expected_plan_id,
                            ownership_modulo = expected_modulo
                        );
                    }
                    let mut admitted = false;
                    for member in &members {
                        admitted |= record.admit_member(
                            expected_plan_id,
                            expected_modulo,
                            member.clone(),
                            now_ms,
                        )?;
                    }
                    if pruned == 0 && !replaced && !admitted {
                        return Ok(());
                    }
                    match self
                        .cas_json_record(
                            key.clone(),
                            revision,
                            &record,
                            JsonTxnContext {
                                op_label: "cas_ownership_plan",
                                serialize_context: "Ownership plan record serialization",
                                txn_context: "Etcd ownership plan CAS txn failed",
                                invalid_response_context: "invalid ownership plan CAS response",
                            },
                        )
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(TsoError::CasFailed) => continue,
                        Err(error) => return Err(error),
                    }
                }
                None => {
                    let Some((first_member, remaining_members)) = members.split_first() else {
                        return Ok(());
                    };
                    let mut record = OwnershipPlanRecord::new(
                        expected_plan_id.to_owned(),
                        expected_modulo,
                        first_member.clone(),
                        now_ms,
                    );
                    for member in remaining_members {
                        record.admit_member(
                            expected_plan_id,
                            expected_modulo,
                            member.clone(),
                            now_ms,
                        )?;
                    }
                    match self
                        .create_json_record(
                            key.clone(),
                            &record,
                            JsonTxnContext {
                                op_label: "create_ownership_plan",
                                serialize_context: "Ownership plan record serialization",
                                txn_context: "Etcd ownership plan create txn failed",
                                invalid_response_context: "invalid ownership plan create response",
                            },
                        )
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(TsoError::MetadataAlreadyExists) => continue,
                        Err(error) => return Err(error),
                    }
                }
            }
        }

        Err(TsoError::CasFailed)
    }

    pub(in crate::metadata::etcd) async fn acquire_identity_lease_internal(
        &self,
        instance_id: &str,
        worker_id: &str,
        advertise_endpoint: &str,
        ownership_plan_id: &str,
        ownership_modulo: u32,
        ttl: Duration,
    ) -> Result<InstanceIdentityLease, TsoError> {
        info!(
            component = "identity_lease",
            event = "acquire_started",
            result = "success",
            reason = "grant_requested",
            instance_id,
            worker_id,
            advertise_endpoint
        );
        let request_ttl_seconds = identity_lease_grant_request_ttl_seconds(ttl)?;
        let key = self.instance_identity_key(instance_id);
        let record = InstanceIdentityLeaseRecord {
            instance_id: instance_id.to_owned(),
            worker_id: worker_id.to_owned(),
            advertise_endpoint: advertise_endpoint.to_owned(),
            ownership_plan_id: ownership_plan_id.to_owned(),
            ownership_modulo,
            cluster_format_version: CURRENT_CLUSTER_FORMAT_VERSION,
        };

        let revoke_client = self.client.clone();
        let grant_request_started_at = Instant::now();
        let grant_response = self
            .etcd_lease_grant(request_ttl_seconds)
            .await
            .map_err(|error| TsoError::Internal(format!("Etcd lease grant failed: {}", error)))?;
        let lease_id = grant_response.id();
        let granted_ttl_seconds = grant_response.ttl();
        let grant_window = validate_identity_lease_grant_or_revoke(
            lease_id,
            grant_request_started_at,
            Instant::now(),
            granted_ttl_seconds,
            |lease_id| async move {
                let mut revoke_client = self.client.clone();
                let _ = revoke_client.lease_revoke(lease_id).await;
            },
        )
        .await?;

        let value = Self::serialize_record(
            &record,
            "identity_lease_acquire",
            "Identity lease serialization",
        )?;
        self.claim_instance_identity_with_lease(&key, value, lease_id, &record)
            .await?;

        let (mut keeper, mut stream) =
            self.etcd_lease_keep_alive(lease_id)
                .await
                .map_err(|error| {
                    TsoError::Internal(format!("Etcd identity lease keepalive failed: {}", error))
                })?;
        let keepalive_client_source = self.client.clone();
        let request_retry_budget = self.request_retry_budget;
        let (lost_tx, lost_rx) = watch::channel(false);
        let initial_lease_alive_until = grant_window.deadline;
        let initial_heartbeat_interval = grant_window.heartbeat_interval;
        let lease_instance_id = instance_id.to_owned();
        let lease_worker_id = worker_id.to_owned();
        let lease_advertise_endpoint = advertise_endpoint.to_owned();
        let keep_alive_task = tokio::spawn(async move {
            let mut lease_alive_until = initial_lease_alive_until;
            let mut heartbeat_interval = initial_heartbeat_interval;
            'keepalive: loop {
                let heartbeat_result =
                    await_identity_keepalive_step(lease_alive_until, sleep(heartbeat_interval))
                        .await;
                let failure_reason = match heartbeat_result {
                    Ok(()) => {
                        let keepalive_request_started_at = Instant::now();
                        let keepalive_result =
                            await_identity_keepalive_step(lease_alive_until, keeper.keep_alive())
                                .await;
                        match keepalive_result {
                            Ok(Ok(())) => {
                                match await_identity_keepalive_step(
                                    lease_alive_until,
                                    stream.message(),
                                )
                                .await
                                {
                                    Ok(Ok(Some(response))) => {
                                        match identity_lease_confirmed_window(
                                            keepalive_request_started_at,
                                            Instant::now(),
                                            response.ttl(),
                                        ) {
                                            Ok(window) => {
                                                lease_alive_until = window.deadline;
                                                heartbeat_interval = window.heartbeat_interval;
                                                continue;
                                            }
                                            Err(error) => {
                                                format!("keepalive_response_invalid: {error}")
                                            }
                                        }
                                    }
                                    Ok(Ok(None)) => "keepalive_stream_closed".to_owned(),
                                    Ok(Err(error)) => {
                                        format!("keepalive_stream_error: {error}")
                                    }
                                    Err(_) => "keepalive_response_deadline_elapsed".to_owned(),
                                }
                            }
                            Ok(Err(error)) => format!("keepalive_send_failed: {error}"),
                            Err(_) => "keepalive_send_deadline_elapsed".to_owned(),
                        }
                    }
                    Err(_) => "keepalive_heartbeat_deadline_elapsed".to_owned(),
                };

                let mut consecutive_reconnect_failures = 0u32;
                let mut reconnect_reason = failure_reason;
                loop {
                    let now = Instant::now();
                    if now >= lease_alive_until {
                        error!(
                            component = "identity_lease",
                            event = "keepalive_lost",
                            result = "failure",
                            reason = %reconnect_reason,
                            lease_id,
                            instance_id = lease_instance_id,
                            worker_id = lease_worker_id,
                            advertise_endpoint = lease_advertise_endpoint
                        );
                        let _ = lost_tx.send(true);
                        break 'keepalive;
                    }

                    let backoff = identity_keepalive_reconnect_backoff(
                        consecutive_reconnect_failures.saturating_add(1),
                    );
                    warn!(
                        component = "identity_lease",
                        event = "keepalive_reconnect",
                        result = "degraded",
                        reason = %reconnect_reason,
                        lease_id,
                        remaining_ttl_ms = lease_alive_until.duration_since(now).as_millis(),
                        backoff_ms = backoff.as_millis(),
                        instance_id = lease_instance_id,
                        worker_id = lease_worker_id,
                        advertise_endpoint = lease_advertise_endpoint
                    );
                    match await_identity_keepalive_reconnect(
                        lease_alive_until,
                        backoff,
                        retry_etcd_request(
                            "identity_lease_keepalive_open",
                            request_retry_budget,
                            || {
                                let mut client = keepalive_client_source.clone();
                                async move { client.lease_keep_alive(lease_id).await }
                            },
                        ),
                    )
                    .await
                    {
                        Ok(Ok((new_keeper, new_stream))) => {
                            keeper = new_keeper;
                            stream = new_stream;
                            continue 'keepalive;
                        }
                        Ok(Err(error)) => {
                            consecutive_reconnect_failures =
                                consecutive_reconnect_failures.saturating_add(1);
                            reconnect_reason = format!("keepalive_reconnect_failed: {error}");
                        }
                        Err(_) => {
                            reconnect_reason = "keepalive_reconnect_deadline_elapsed".to_owned();
                        }
                    }
                }
            }
        });

        info!(
            component = "identity_lease",
            event = "acquire_succeeded",
            result = "success",
            reason = "lease_acquired",
            lease_id,
            instance_id,
            worker_id,
            advertise_endpoint,
            configured_ttl_ms = %ttl.as_millis(),
            request_ttl_seconds,
            granted_ttl_seconds
        );

        Ok(InstanceIdentityLease::new(
            lost_rx,
            keep_alive_task,
            revoke_client,
            lease_id,
        ))
    }

    pub async fn verify_instance_identity_write_path(
        &self,
        expected_lease_id: i64,
        instance_id: &str,
        worker_id: &str,
        advertise_endpoint: &str,
        ownership_plan_id: &str,
        ownership_modulo: u32,
    ) -> Result<(), TsoError> {
        let key = self.instance_identity_key(instance_id);
        let response = self
            .etcd_get("identity_lease_startup_probe_get", key, None)
            .await
            .map_err(|error| {
                metrics::TSO_METADATA_ERRORS_TOTAL
                    .with_label_values(&["identity_lease_startup_probe_get"])
                    .inc();
                TsoError::Internal(format!(
                    "Etcd identity lease startup probe lookup failed: {}",
                    error
                ))
            })?;

        let Some(kv) = response.kvs().first() else {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&["identity_lease_startup_probe_missing"])
                .inc();
            return Err(TsoError::Internal(format!(
                "Etcd identity lease startup probe missing record for instance {}",
                instance_id
            )));
        };

        if kv.lease() == 0 {
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&["identity_lease_startup_probe_missing_lease"])
                .inc();
            return Err(TsoError::Internal(format!(
                "Etcd identity lease startup probe found record without an attached lease for instance {}",
                instance_id
            )));
        }

        let expected_record = InstanceIdentityLeaseRecord {
            instance_id: instance_id.to_owned(),
            worker_id: worker_id.to_owned(),
            advertise_endpoint: advertise_endpoint.to_owned(),
            ownership_plan_id: ownership_plan_id.to_owned(),
            ownership_modulo,
            cluster_format_version: CURRENT_CLUSTER_FORMAT_VERSION,
        };
        verify_instance_identity_lease_record(
            expected_lease_id,
            kv.lease(),
            kv.value(),
            &expected_record,
        )
        .inspect_err(|error| {
            let label = match error.to_string().contains("decode failed") {
                true => "identity_lease_startup_probe_decode",
                false if error.to_string().contains("mismatched identity record") => {
                    "identity_lease_startup_probe_mismatch"
                }
                false if error.to_string().contains("observed lease") => {
                    "identity_lease_startup_probe_wrong_lease"
                }
                false => "identity_lease_startup_probe_mismatch",
            };
            metrics::TSO_METADATA_ERRORS_TOTAL
                .with_label_values(&[label])
                .inc();
        })?;

        Ok(())
    }
}
