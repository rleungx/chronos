# Chronos Observability

Chronos already exposes `/healthz`, `/readyz`, and Prometheus metrics on the metrics listener.
This directory is the observability entrypoint: local Prometheus/Grafana startup, alert rule
validation, dashboard assets, and links to the runbooks that explain what to do when an alert fires.

## Local Prometheus

1. Start Chronos with a metrics listener, for example:

   ```bash
   export CHRONOS_SECURITY_MODE=dev-insecure
   export CHRONOS_BIND_ADDR=127.0.0.1:50051
   export CHRONOS_ADVERTISE_ENDPOINT=127.0.0.1:50051
   export CHRONOS_METRICS_BIND_ADDR=127.0.0.1:9898

   cargo run --release --bin chronos
   ```

2. Start Prometheus:

   ```bash
   make observability-up
   ```

3. Open Grafana at <http://localhost:3000>.

   - Default login: `admin` / `admin`
   - Preprovisioned dashboard: `Chronos / Chronos Overview`

5. Validate the alert rules:

   ```bash
   make observability-check
   ```

4. Open Prometheus at <http://localhost:9090>.

5. Stop Prometheus and Grafana when done:

   ```bash
   make observability-down
   ```

## Included alerts

- `ChronosDown`
- `ChronosNotReady`
- `ChronosIdentityLeaseLost`
- `ChronosClockBackwards`
- `ChronosMetadataErrors`
- `ChronosWatchResyncs`
- `ChronosProxySaturation`
- `ChronosStartupPreflightFailures`
- `ChronosStartupBootstrapFailures`

These alerts are the repository's default operational rule set. Tune thresholds and routing for your
environment, but treat this file as the source of truth for alert behavior shipped with Chronos.

Runbooks for the highest-value alerts now live under:

- `docs/runbooks/not-ready.md`
- `docs/runbooks/identity-lease-lost.md`
- `docs/runbooks/metadata-errors-etcd.md`
- `docs/runbooks/transfer-failures.md`

## Included dashboards

- `observability/grafana/dashboards/chronos-overview.json`

The overview dashboard is intended to answer four operator questions quickly:

1. Is the instance ready and shutting down?
2. Are allocation throughput and latency healthy?
3. Are metadata and control-plane dependencies degrading?
4. Are transfer/failover/recovery paths firing unexpectedly?

## Local validation harnesses

- `make test-soak` runs an etcd-backed release-mode soak/load sanity sequence using
  `chronos-bench` and `chronos-control-bench`.
- `make test-chaos` injects an etcd failure, verifies Chronos degrades or exits, restores etcd,
  restarts Chronos, and runs a recovery smoke benchmark.

Both harnesses honor `CHRONOS_ARTIFACT_DIR`. When it is set, they retain:

- `summary.txt`
- `artifact-index.txt`
- `chronos.log`
- bench/control-plane logs
- `readyz.txt`
- `metrics.txt`
- `docker-ps.txt`
- `etcd.log`

This is the mode used by the scheduled/manual CI soak and chaos jobs so failures leave behind
downloadable artifacts instead of only console output.

Useful overrides for local runs:

- `CHRONOS_ARTIFACT_DIR=/path/to/artifacts`
- `CHRONOS_SOAK_DURATION_SECS`, `CHRONOS_SOAK_WARMUP_SECS`
- `CHRONOS_SOAK_CONTROL_TIMELINES`, `CHRONOS_SOAK_CONCURRENCY`
- `CHRONOS_CHAOS_BENCH_DURATION_SECS`

## CI artifact handoff

The `soak-real-etcd`, `chaos-lease-loss`, `failover-real-etcd`, and `rebalance-real-etcd`
workflows upload `artifacts/soak/`, `artifacts/chaos/`, `artifacts/failover/`, and
`artifacts/rebalance/` respectively. Start with `summary.txt`, then inspect `chronos.log` and
`etcd.log` before diving into bench output. `artifact-index.txt` is the fastest way to confirm what
the job actually captured.

See `docs/production.md` for the recommended validation order and benchmark budget variables.
See `docs/release.md` for the release gate sequence.
