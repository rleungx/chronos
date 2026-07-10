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
2. Read the prefix's persistent `cluster/format_version` marker. Compare it with the
   `cluster_format_version` printed by the rollback binary's `--print-effective-config` output.
3. If the versions match, stop every current worker, wait for identity leases to expire, and then
   deploy the last compatible release candidate whose validation bundle is intact. Do not run old
   and new writers concurrently.
4. If the rollback binary requires an older format, do not start it against the upgraded prefix.
   Restore a pre-upgrade etcd snapshot into a separate prefix and point the rollback deployment at
   that prefix.
5. Confirm readiness through the plain health listener and inspect metrics through mTLS:

   ```bash
   curl http://<health-endpoint>/readyz
   curl --cacert <ca.pem> --cert <client.pem> --key <client.key> \
     https://<metrics-endpoint>/metrics \
     | grep '^tso_startup_ready'
   ```

6. Review the retained artifacts from the failed candidate:
   - `summary.txt`
   - `chronos.log`
   - `etcd.log`
   - benchmark logs

## Do not skip

- Do not promote a new build until the failed candidate has a rooted explanation.
- Do not overwrite the validation artifacts for the last known good candidate.
- Do not delete or rewrite `cluster/format_version` to force an older binary to start.
