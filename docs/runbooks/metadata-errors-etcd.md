# Runbook: Metadata Errors / etcd Issues

## Signal

- Alert: `ChronosMetadataErrors`
- Related alerts: `ChronosWatchResyncs`, `ChronosRecoveryEvents`

## Immediate checks

1. Inspect `tso_metadata_errors_total` by operation label.
2. Inspect etcd health and recent latency.
3. Check `chronos.log` and `etcd.log` together.
4. Compare with `tso_metadata_conflicts_total`; CAS/create conflicts are expected under
   contention and should not trigger `ChronosMetadataErrors` unless they exhaust retries or appear
   with real etcd errors.

## Actions

1. Verify etcd quorum and endpoint reachability.
2. If errors coincide with watch churn or recovery spikes, inspect:
   - `tso_watch_resync_total` grouped by `event`
   - `tso_recovery_events_total`
3. If metadata is degraded but Chronos is still serving, monitor for escalation to readiness loss.
4. Re-run `make test-layer-4` or environment-equivalent etcd validation after mitigation.
