# Chronos Rollback Procedure

Use this document only after a candidate has already failed validation or a deployed build has
degraded. For normal release preparation, start with `docs/release.md`.

## Rollback triggers

Rollback immediately when any of the following occurs:

1. `make release-gate` fails for the build under consideration.
2. Any benchmark gate fails:
   - `make test-soak`
   - `make test-chaos`
   - `make test-failover-bench`
   - `make test-rebalance-bench`
3. Alert rule validation fails:

   ```bash
   make observability-check
   ```

4. Production readiness, identity-lease, or route-watch resync alerts start firing unexpectedly after rollout.

## Rollback steps

1. Drain or remove traffic from the bad instance set.
2. Redeploy the last release candidate whose validation bundle is intact.
3. Confirm:

   ```bash
   curl http://<metrics-endpoint>/readyz
   curl http://<metrics-endpoint>/metrics | grep '^tso_startup_ready'
   ```

4. Review the retained artifacts from the failed candidate:
   - `summary.txt`
   - `chronos.log`
   - `etcd.log`
   - benchmark logs

## Do not skip

- Do not promote a new build until the failed candidate has a rooted explanation.
- Do not overwrite the validation artifacts for the last known good candidate.
