use super::*;

impl EtcdMetadataStore {
    pub(in crate::metadata::etcd) fn spawn_route_watch_loop(&self) {
        let mut watch_client = self.client.clone();
        let route_prefix = self.route_prefix();
        let route_updates = self.route_updates.clone();
        let mut shutdown_rx = self.route_watch_shutdown_tx.subscribe();

        let route_watch_task = tokio::spawn(async move {
            let mut next_watch_revision: Option<i64> = None;
            let mut consecutive_failures = 0u32;
            let mut reset_sent_for_outage = false;
            loop {
                let watch_start_revision = next_watch_revision;
                let watch_result = tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    result = watch_client.watch(
                        route_prefix.clone(),
                        Some(route_watch_options(watch_start_revision)),
                    ) => result,
                };

                let (_watcher, mut watch_stream) = match watch_result {
                    Ok(stream) => {
                        consecutive_failures = 0;
                        reset_sent_for_outage = false;
                        info!(
                            component = "route_watch",
                            event = "watch_started",
                            result = "success",
                            reason = "watch_connected",
                            start_revision = watch_start_revision.unwrap_or(0)
                        );
                        stream
                    }
                    Err(_) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        record_route_watch_resync("watch_connect_failed");
                        send_route_watch_reset_once(&route_updates, &mut reset_sent_for_outage);
                        let backoff = route_watch_reconnect_backoff(consecutive_failures);
                        warn!(
                            component = "route_watch",
                            event = "watch_restarted",
                            result = "degraded",
                            reason = "watch_connect_failed",
                            backoff_ms = backoff.as_millis() as u64,
                            start_revision = watch_start_revision.unwrap_or(0)
                        );
                        tokio::select! {
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            _ = sleep(backoff) => {}
                        }
                        continue;
                    }
                };

                let reconnect_after: Duration;
                'watch_stream: loop {
                    let watch_message = tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                return;
                            }
                            continue;
                        }
                        message = watch_stream.message() => message,
                    };

                    match watch_message {
                        Ok(Some(response)) => {
                            if let Some(header) = response.header() {
                                let revision = header.revision();
                                if revision > 0 {
                                    next_watch_revision = revision.checked_add(1);
                                }
                            }
                            if response.compact_revision() > 0 {
                                consecutive_failures = consecutive_failures.saturating_add(1);
                                record_route_watch_resync("watch_compacted");
                                next_watch_revision = None;
                                send_route_watch_reset_once(
                                    &route_updates,
                                    &mut reset_sent_for_outage,
                                );
                                let backoff = route_watch_reconnect_backoff(consecutive_failures);
                                reconnect_after = backoff;
                                warn!(
                                    component = "route_watch",
                                    event = "watch_restarted",
                                    result = "degraded",
                                    reason = "watch_compacted",
                                    compact_revision = response.compact_revision(),
                                    backoff_ms = backoff.as_millis() as u64
                                );
                                break;
                            }
                            if response.canceled() {
                                consecutive_failures = consecutive_failures.saturating_add(1);
                                record_route_watch_resync("watch_canceled");
                                next_watch_revision = None;
                                send_route_watch_reset_once(
                                    &route_updates,
                                    &mut reset_sent_for_outage,
                                );
                                let backoff = route_watch_reconnect_backoff(consecutive_failures);
                                reconnect_after = backoff;
                                warn!(
                                    component = "route_watch",
                                    event = "watch_restarted",
                                    result = "degraded",
                                    reason = "watch_canceled",
                                    cancel_reason = response.cancel_reason(),
                                    backoff_ms = backoff.as_millis() as u64
                                );
                                break;
                            }
                            for event in response.events() {
                                if event.event_type() != EventType::Put {
                                    continue;
                                }
                                let Some(key_value) = event.kv() else {
                                    consecutive_failures = consecutive_failures.saturating_add(1);
                                    metrics::TSO_METADATA_ERRORS_TOTAL
                                        .with_label_values(&["route_watch_event_missing_kv"])
                                        .inc();
                                    record_route_watch_resync("watch_event_missing_kv");
                                    next_watch_revision = None;
                                    send_route_watch_reset_once(
                                        &route_updates,
                                        &mut reset_sent_for_outage,
                                    );
                                    reconnect_after =
                                        route_watch_reconnect_backoff(consecutive_failures);
                                    warn!(
                                        component = "route_watch",
                                        event = "watch_restarted",
                                        result = "degraded",
                                        reason = "watch_event_missing_kv",
                                        backoff_ms = reconnect_after.as_millis() as u64
                                    );
                                    break 'watch_stream;
                                };
                                let Ok(record) = serde_json::from_slice::<RouteOnlyTimelineRecord>(
                                    key_value.value(),
                                ) else {
                                    consecutive_failures = consecutive_failures.saturating_add(1);
                                    metrics::TSO_METADATA_ERRORS_TOTAL
                                        .with_label_values(&["route_watch_event_decode"])
                                        .inc();
                                    record_route_watch_resync("watch_event_decode_failed");
                                    next_watch_revision = None;
                                    send_route_watch_reset_once(
                                        &route_updates,
                                        &mut reset_sent_for_outage,
                                    );
                                    reconnect_after =
                                        route_watch_reconnect_backoff(consecutive_failures);
                                    warn!(
                                        component = "route_watch",
                                        event = "watch_restarted",
                                        result = "degraded",
                                        reason = "watch_event_decode_failed",
                                        backoff_ms = reconnect_after.as_millis() as u64
                                    );
                                    break 'watch_stream;
                                };
                                if let Some(route_update) = route_update_for_watch_event(
                                    event.prev_kv().map(|prev_key_value| prev_key_value.value()),
                                    &record.route,
                                ) {
                                    debug!(
                                        component = "route_watch",
                                        event = "watch_event_applied",
                                        result = "success",
                                        reason = "route_changed",
                                        timeline_key = %record.route.timeline_key,
                                        generator_id = record.route.generator_id,
                                        epoch = record.route.epoch,
                                        route_version = record.route.route_version,
                                        owner_endpoint = %record.route.owner_worker_endpoint
                                    );
                                    let _ =
                                        route_updates.send(RouteUpdateSignal::Route(route_update));
                                }
                            }
                        }
                        Ok(None) => {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            record_route_watch_resync("watch_stream_closed");
                            send_route_watch_reset_once(&route_updates, &mut reset_sent_for_outage);
                            reconnect_after = route_watch_reconnect_backoff(consecutive_failures);
                            break;
                        }
                        Err(_) => {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            record_route_watch_resync("watch_stream_error");
                            send_route_watch_reset_once(&route_updates, &mut reset_sent_for_outage);
                            let backoff = route_watch_reconnect_backoff(consecutive_failures);
                            reconnect_after = backoff;
                            warn!(
                                component = "route_watch",
                                event = "watch_restarted",
                                result = "degraded",
                                reason = "watch_stream_error",
                                backoff_ms = backoff.as_millis() as u64,
                                next_start_revision = next_watch_revision.unwrap_or(0)
                            );
                            break;
                        }
                    }
                }

                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = sleep(reconnect_after) => {}
                }
            }
        });

        *self.route_watch_task_lock() = Some(route_watch_task);
    }

    pub(crate) fn request_route_watch_shutdown(&self) {
        info!(
            component = "shutdown",
            event = "background_stop_started",
            result = "success",
            reason = "route_watch_shutdown_requested"
        );
        let _ = self.route_watch_shutdown_tx.send(true);
    }

    pub(crate) async fn shutdown_route_watch(&self) {
        self.request_route_watch_shutdown();
        let route_watch_task = self.route_watch_task_lock().take();
        if let Some(route_watch_task) = route_watch_task {
            let _ = route_watch_task.await;
        }
        info!(
            component = "shutdown",
            event = "background_stop_completed",
            result = "success",
            reason = "route_watch_shutdown_complete"
        );
    }
}
