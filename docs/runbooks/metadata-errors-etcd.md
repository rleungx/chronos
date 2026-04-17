# Runbook: Metadata Errors / etcd Issues

## Signal

- Alert: `ChronosMetadataErrors`
- Related alerts: `ChronosWatchSendTimeouts`, `ChronosWatchKeepaliveDropped`, `ChronosRecoveryEvents`

## Immediate checks

1. Inspect `tso_metadata_errors_total` by operation label.
2. Inspect etcd health and recent latency.
3. Check `chronos.log` and `etcd.log` together.

## Actions

1. Verify etcd quorum and endpoint reachability.
2. If errors coincide with watch churn or recovery spikes, inspect:
   - `tso_watch_resync_total`
   - `tso_recovery_events_total`
3. If metadata is degraded but Chronos is still serving, monitor for escalation to readiness loss.
4. Re-run `make test-layer-4` or environment-equivalent etcd validation after mitigation.
