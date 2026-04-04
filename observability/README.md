# Chronos Observability

Chronos already exposes `/healthz`, `/readyz`, and Prometheus metrics on the metrics listener.
This directory adds a minimal local Prometheus setup and starter alert rules for the existing
metrics surface.

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

3. Open <http://localhost:9090>.

4. Stop Prometheus when done:

   ```bash
   make observability-down
   ```

## Included alerts

- `ChronosDown`
- `ChronosNotReady`
- `ChronosIdentityLeaseLost`
- `ChronosClockBackwards`
- `ChronosMetadataErrors`
- `ChronosWatchSendTimeouts`
- `ChronosProxySaturation`
- `ChronosStartupPreflightFailures`
- `ChronosStartupBootstrapFailures`

These alerts are intended as starter rules, not final production SLO policy. Tune thresholds and
alert routing for your environment.

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

The `soak-real-etcd` and `chaos-lease-loss` workflows upload `artifacts/soak/` and
`artifacts/chaos/` respectively. Start with `summary.txt`, then inspect `chronos.log` and
`etcd.log` before diving into bench output. `artifact-index.txt` is the fastest way to confirm what
the job actually captured.
