# Runbook: Transfer / Failover Failures

## Signal

- Alert: `ChronosTransferFailures`
- Metric: `tso_transfer_outcomes_total{outcome=~"failed|blocked"}`

## Immediate checks

1. Inspect `tso_transfer_outcomes_total` broken down by `action`, `reason`, and `outcome`.
2. Inspect `chronos.log` for transfer/failover events and blockers.
3. Check whether lease expiry or recovery-floor prerequisites are missing.

## Actions

1. If failures are caused by lease-not-expired, wait for lease expiry instead of forcing ownership.
2. If failures are caused by missing recovery floor, persist recovery state before retrying.
3. Validate the environment with:

   ```bash
   make test-failover-bench
   make test-rebalance-bench
   ```
