# Runbook: Chronos Not Ready

## Signal

- Alert: `ChronosNotReady`
- Metric: `tso_startup_ready == 0`

## Immediate checks

1. Query the plain health listener used by the Kubernetes probe:

   ```bash
   curl http://<health-endpoint>/readyz
   ```

2. Query the production mTLS metrics listener with an authorized client certificate:

   ```bash
   curl --cacert <ca.pem> --cert <client.pem> --key <client.key> \
     https://<metrics-endpoint>/metrics \
     | grep '^tso_worker_readiness_transitions_total'
   ```

3. Inspect `chronos.log` for startup, lease-loss, or shutdown events.

## Common causes

- identity lease loss
- startup preflight failure
- startup bootstrap failure
- controlled shutdown in progress

## Actions

1. Confirm config with `chronos --check-config` and `--print-effective-config`.
2. If the instance is shutting down, drain traffic and replace it.
3. If readiness dropped after etcd issues, inspect `tso_metadata_errors_total` and etcd health.
4. If readiness does not recover, restart only after understanding the underlying reason from logs
   and metrics.
