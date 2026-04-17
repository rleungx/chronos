# Runbook: Identity Lease Lost

## Signal

- Alert: `ChronosIdentityLeaseLost`
- Metric: `increase(tso_identity_lease_events_total{event="lost"}[5m]) > 0`

## Why it matters

Chronos intentionally degrades or exits when the identity lease is lost to avoid split-brain
ownership.

## Immediate checks

1. Inspect `chronos.log` for `identity_lease` events.
2. Check etcd health and latency.
3. Inspect `tso_shutdown_total` and `tso_worker_readiness_transitions_total`.

## Actions

1. Treat the instance as unsafe for serving until readiness is restored.
2. Confirm the identity key is released before restarting a replacement.
3. Run the chaos harness locally or in staging if the failure mode is unclear:

   ```bash
   make test-chaos
   ```
