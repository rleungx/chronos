# Chronos Production Operations

Chronos already ships the core runtime guards needed for production-like runs: startup preflight,
readiness degradation, identity-lease loss shutdown, Prometheus metrics, and etcd-backed
validation harnesses. This guide turns those mechanics into an operator workflow.

## Before a production run

1. Validate the effective configuration:

   ```bash
   cargo run --bin chronos -- --check-config
   cargo run --bin chronos -- --print-effective-config
   ```

2. Prefer the production profile and auditable build identity:

   ```bash
   export CHRONOS_PROFILE=production
   export CHRONOS_BUILD_COMMIT="$(git rev-parse HEAD)"
   ```

3. Confirm etcd settings are explicit and loopback overrides are not leaking from local testing:

   ```bash
   export CHRONOS_METADATA=etcd
   export CHRONOS_ETCD_ENDPOINTS=host1:2379,host2:2379,host3:2379
   export CHRONOS_ETCD_PREFIX=/chronos-prod
   ```

4. Validate transport, metrics, and identity settings before rollout:

   ```bash
   export CHRONOS_BIND_ADDR=0.0.0.0:50051
   export CHRONOS_ADVERTISE_ENDPOINT=chronos-a.example.com:50051
   export CHRONOS_METRICS_BIND_ADDR=0.0.0.0:9898
   export CHRONOS_WORKER_ID=worker-a
   export CHRONOS_INSTANCE_ID=chronos-a.example.com:50051
   ```

## Health and readiness

- `/healthz` indicates process liveness.
- `/readyz` indicates whether Chronos is currently safe to serve.
- In etcd mode, readiness is only reached after Chronos has validated metadata reads/watch subscription and verified that the acquired instance identity lease record was actually written with the expected identity payload.
- `tso_startup_ready == 0` means the instance should not receive traffic.
- `tso_worker_readiness_transitions_total` and `tso_shutdown_total` provide the reason path for
  readiness and shutdown changes.

## Required validation before release

Run the full release gate in order:

```bash
make release-gate
```

`make release-check` covers clippy, layer-0/2/3 validation, observability checks, dependency
policy, release-shape validation, container delivery checks, and release builds. `make
release-gate` adds layer-4 plus the long-running etcd-backed soak, chaos, failover, and rebalance
validation bundle.

## Interpreting retained artifacts

When `CHRONOS_ARTIFACT_DIR` is set, the long-running harnesses retain:

- `summary.txt`
- `artifact-index.txt`
- `chronos.log`
- `etcd.log`
- benchmark logs
- `readyz.txt`
- `metrics.txt`

Recommended triage order:

1. `summary.txt`
2. `readyz.txt`
3. `metrics.txt`
4. `chronos.log`
5. `etcd.log`
6. benchmark logs

## Benchmark budgets

The shell harnesses now support explicit budget variables. Use environment variables to tune for
your hardware, but do not remove the gates.

- Soak: `CHRONOS_SOAK_REQ_PER_SEC_MIN`, `CHRONOS_SOAK_LATENCY_P95_US_MAX`,
  `CHRONOS_SOAK_LATENCY_P99_US_MAX`
- Chaos recovery: `CHRONOS_CHAOS_RECOVERY_REQ_PER_SEC_MIN`,
  `CHRONOS_CHAOS_RECOVERY_LATENCY_P95_US_MAX`
- Failover: `CHRONOS_FAILOVER_ALLOCATE_SUCCESS_PER_SEC_MIN`,
  `CHRONOS_FAILOVER_ALLOCATE_LATENCY_P95_US_MAX`,
  `CHRONOS_FAILOVER_FAILOVER_LATENCY_P95_US_MAX`,
  `CHRONOS_FAILOVER_FIRST_SUCCESS_AFTER_KILL_MS_MAX`
- Rebalance: `CHRONOS_REBALANCE_ALLOCATE_SUCCESS_PER_SEC_MIN`,
  `CHRONOS_REBALANCE_ALLOCATE_LATENCY_P95_US_MAX`,
  `CHRONOS_REBALANCE_ROUTE_REFRESH_P95_US_MAX`,
  `CHRONOS_REBALANCE_TRANSFER_LATENCY_P95_US_MAX`

## Runbooks

For alert-to-action guidance, use the runbook index in `observability/README.md`.

## Dependency policy

Chronos now ships a repo-local `deny.toml` and executable dependency gate:

```bash
make dependency-check
```

This enforces advisory, license, source, and wildcard dependency policy before release promotion.

## What this repo proves vs what still needs environment evidence

This repo proves the single-repo release gate, startup contracts, alert rule validity, and
single-node/local etcd validation harnesses.

You still need environment evidence for clustered etcd quorum behavior, backup/restore drills,
staged rollout safety, and production alert threshold tuning.
