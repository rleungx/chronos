# Chronos Rollback Procedure

Use this procedure when a release candidate or freshly deployed build fails validation or degrades
runtime behavior.

## Rollback triggers

Rollback immediately when any of the following occurs:

1. `make release-check` fails for the build under consideration.
2. Any benchmark gate fails:
   - `make test-soak`
   - `make test-chaos`
   - `make test-failover-bench`
   - `make test-rebalance-bench`
3. Alert rule validation fails:

   ```bash
   make observability-check
   ```

4. Production readiness or identity-lease alerts start firing unexpectedly after rollout.

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
