# Chronos Production Operations

Chronos already ships the core runtime guards needed for production-like runs: startup preflight,
readiness degradation, identity-lease loss shutdown, Prometheus metrics, and etcd-backed
validation harnesses. This guide turns those mechanics into an operator workflow.

## Scaling contract

Chronos scales horizontally by routing independent `timeline_key` shards to generator owners. Each
timeline preserves monotonic allocation for that timeline. Adding workers only increases throughput
when the workload has enough independent timelines or uses larger allocation batches.

A single global strictly increasing sequence is a single-writer ordering problem and is not expected
to scale linearly by adding workers. If an application needs production-grade linear throughput,
partition the timestamp namespace with timeline keys and keep any required ordering local to the
partition, or use batch allocation so each request amortizes the fixed gRPC/runtime cost.

For high-throughput multi-node deployments, shard generator ownership explicitly. The default
`CHRONOS_GENERATOR_OWNERSHIP_MODULO=1` remains an unpartitioned HA/standby mode: multiple workers
can start and participate in route/failover control, but they do not provide linear allocation
scale because every shared generator belongs to the same ownership bucket. For a two-worker
partitioned pool:

```bash
export CHRONOS_OWNERSHIP_PLAN_ID=prod-2026-05-10-a

# worker-a
export CHRONOS_GENERATOR_OWNERSHIP_MODULO=2
export CHRONOS_GENERATOR_OWNERSHIP_REMAINDER=0

# worker-b
export CHRONOS_GENERATOR_OWNERSHIP_MODULO=2
export CHRONOS_GENERATOR_OWNERSHIP_REMAINDER=1
```

For `N` partitioned workers, set one shared `CHRONOS_OWNERSHIP_PLAN_ID` and
`CHRONOS_GENERATOR_OWNERSHIP_MODULO=N` on every worker, then assign each worker a unique
`CHRONOS_GENERATOR_OWNERSHIP_REMAINDER` in `[0, N)`. Startup records the plan in etcd and rejects
mixed plan IDs, mixed modulo values, duplicate remainders, duplicate worker IDs, or duplicate
advertise endpoints on the same etcd prefix. Clients and benchmarks must route allocation requests
to the `owner_worker_endpoint` returned by the timeline route service.

The Kubernetes manifest uses this static partitioning model directly: the StatefulSet runs three
replicas with `CHRONOS_GENERATOR_OWNERSHIP_MODULO=3`, and each pod derives
`CHRONOS_GENERATOR_OWNERSHIP_REMAINDER` from its StatefulSet ordinal. Do not attach a normal HPA to
this StatefulSet. Changing replica count changes the ownership modulo and must be handled as a
planned repartition with a new ownership plan ID, release evidence, and a rebalance/failover window.

Before changing worker count, generate an ownership movement plan:

```bash
CHRONOS_OWNERSHIP_OLD_WORKERS=2 CHRONOS_OWNERSHIP_NEW_WORKERS=4 make scale-ownership-plan
```

Treat a modulo change as a planned repartition, not an in-place toggle. Roll it with an explicit
rebalance/failover window, verify route-owner distribution, and keep old and new ownership plans
from running against the same etcd prefix unless the change is part of a controlled migration.
During rebalance, route transfer control calls to the target owner and let that owner choose the
target generator unless you have prevalidated the exact generator. This avoids unsafe shared
generator jump-ahead and allows the owner to fall back to a dedicated generator when catch-up would
otherwise be too large. Allocation clients should set a rebalance-window request timeout, currently
`CHRONOS_REBALANCE_ALLOCATE_REQUEST_TIMEOUT_MS=10000` in the local gate, so transient catch-up waits
are absorbed without exposing allocation failures.

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

3. Size timeline runtime capacity for the expected active timeline count:

   ```bash
   export CHRONOS_MAX_TIMELINE_PROXY_LANES=8192
   export CHRONOS_MAX_TIMELINE_RUNTIME_ENTRIES=8192
   export CHRONOS_MAX_CONCURRENT_TIMELINE_LOADS=128
   ```

   Keep `CHRONOS_MAX_TIMELINE_PROXY_LANES` and
   `CHRONOS_MAX_TIMELINE_RUNTIME_ENTRIES` above the steady-state active timeline count plus
   rollout headroom. When the runtime cache saturates, Chronos preserves correctness by serving the
   affected timeline through the uncached metadata path, but allocation latency and metadata load
   will increase.

4. Confirm etcd settings are explicit and loopback overrides are not leaking from local testing:

   ```bash
   export CHRONOS_METADATA=etcd
   export CHRONOS_ETCD_ENDPOINTS=host1:2379,host2:2379,host3:2379
   export CHRONOS_ETCD_PREFIX=/chronos-prod
   ```

5. Validate transport, metrics, and identity settings before rollout:

   ```bash
   export CHRONOS_BIND_ADDR=0.0.0.0:50051
   export CHRONOS_ADVERTISE_ENDPOINT=chronos-a.example.com:50051
   export CHRONOS_METRICS_BIND_ADDR=0.0.0.0:9898
   export CHRONOS_WORKER_ID=worker-a
   export CHRONOS_INSTANCE_ID=chronos-a.example.com:50051
   ```

   In production profile or `CHRONOS_SECURITY_MODE=required`, `CHRONOS_ADVERTISE_ENDPOINT` must be
   a client-routable service address. `localhost`, `*.localhost`, loopback IPs, and wildcard
   addresses are accepted only for explicit `CHRONOS_SECURITY_MODE=dev-insecure` local validation.

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
release-gate` adds release packaging, build metadata, SBOM/hash checks, container vulnerability scanning,
clustered layer-4 validation, and the long-running etcd-backed soak, chaos, failover, production
scale matrix, rebalance, and restore validation bundle. The local gate writes retained evidence to
`artifacts/release-gate` by default and verifies the evidence bundle before returning success.

## Interpreting retained artifacts

When `CHRONOS_ARTIFACT_DIR` is set, the long-running harnesses retain:

- `summary.txt`
- `artifact-index.txt`
- `chronos.log`
- `etcd.log`
- benchmark logs
- `readyz.txt`
- `metrics.txt`
- `profile-summary.txt` for scale runs, derived from benchmark output and per-worker metrics
- `host-info.txt`, `host-load-before.txt`, and `host-load-after.txt` for scale runs

Recommended triage order:

1. `summary.txt`
2. `readyz.txt`
3. `metrics.txt`
4. `profile-summary.txt`
5. `chronos.log`
6. `etcd.log`
7. benchmark logs

## Benchmark budgets

The shell harnesses now support explicit budget variables. Use environment variables to tune for
your hardware, but do not remove the gates.

- Soak: `CHRONOS_SOAK_REQ_PER_SEC_MIN`, `CHRONOS_SOAK_LATENCY_P95_US_MAX`,
  `CHRONOS_SOAK_LATENCY_P99_US_MAX`, `CHRONOS_SOAK_LATENCY_P999_US_MAX`,
  `CHRONOS_SOAK_CONTROL_RPC_P999_US_MAX`, `CHRONOS_SOAK_CONTROL_SCAN_P999_US_MAX`,
  `CHRONOS_SOAK_FILTERED_RPC_P999_US_MAX`, `CHRONOS_SOAK_FILTERED_SCAN_P999_US_MAX`
- Allocation bench: `CHRONOS_BENCH_IDEMPOTENCY` defaults to `false` for hot-path performance, and
  `CHRONOS_BENCH_REQUEST_TIMEOUT_MS` defaults to `1000`. Use
  `CHRONOS_BENCH_CLIENT_TIMEOUT_MS` to set the benchmark client's protection timeout separately
  from the timeout field sent to Chronos. Set `CHRONOS_BENCH_CONTROL_ENDPOINTS` to a comma-separated
  list when seeding timelines through multiple workers, and set `CHRONOS_BENCH_ROUTE_TO_OWNERS=true`
  to send allocations to each route owner. Set `CHRONOS_BENCH_ALLOCATION_CONNECTION_POOL_SIZE` to
  spread high-concurrency allocation load over multiple gRPC connections per owner endpoint.
  `CHRONOS_BENCH_CONNECT_TIMEOUT_MS` defaults to at least `15000`, and
  `CHRONOS_BENCH_CONNECT_RETRY_INTERVAL_MS` defaults to `100`, so benchmark startup tolerates
  transient listener and DNS readiness without masking allocation failures.
  `CHRONOS_BENCH_OWNER_AFFINITY` defaults to `true` when owner routing is enabled, keeping each
  benchmark worker on one owner endpoint to avoid measuring artificial client-side endpoint
  switching. Set `CHRONOS_BENCH_COLD_PROBE=true` to measure first allocation after route creation
  separately from the hot-path window.
  `CHRONOS_CONTROL_BENCH_IDEMPOTENCY` and `CHRONOS_REBALANCE_CONTROL_BENCH_IDEMPOTENCY` default to
  `false` so rebalance gates measure the normal allocation hot path; set them to `true` only when
  explicitly validating request-record idempotency cost.
- Chaos recovery: `CHRONOS_CHAOS_RECOVERY_REQ_PER_SEC_MIN`,
  `CHRONOS_CHAOS_RECOVERY_LATENCY_P95_US_MAX`,
  `CHRONOS_CHAOS_RECOVERY_LATENCY_P999_US_MAX`
- Failover: `CHRONOS_FAILOVER_ALLOCATE_SUCCESS_PER_SEC_MIN`,
  `CHRONOS_FAILOVER_ALLOCATE_LATENCY_P95_US_MAX`,
  `CHRONOS_FAILOVER_ALLOCATE_LATENCY_P999_US_MAX`,
  `CHRONOS_FAILOVER_ROUTE_REFRESH_P999_US_MAX`,
  `CHRONOS_FAILOVER_LATENCY_P95_US_MAX`,
  `CHRONOS_FAILOVER_LATENCY_P999_US_MAX`,
  `CHRONOS_FAILOVER_FIRST_SUCCESS_AFTER_KILL_MS_MAX`
- Scale: `CHRONOS_SCALE_WORKERS`, `CHRONOS_SCALE_REQ_PER_SEC_MIN`,
  `CHRONOS_SCALE_LATENCY_P95_US_MAX`, `CHRONOS_SCALE_LATENCY_P99_US_MAX`,
  `CHRONOS_SCALE_LATENCY_P999_US_MAX`,
  `CHRONOS_SCALE_ROUTE_OWNER_ENDPOINTS_MIN`, `CHRONOS_SCALE_ALLOCATION_CONNECTION_POOL_SIZE`,
  `CHRONOS_SCALE_CONNECT_TIMEOUT_MS`, `CHRONOS_SCALE_CONNECT_RETRY_INTERVAL_MS`,
  `CHRONOS_SCALE_BENCH_CLIENT_PROCESSES`, `CHRONOS_SCALE_COLD_PROBE_LATENCY_P95_US_MAX`,
  `CHRONOS_SCALE_COLD_PROBE_LATENCY_P999_US_MAX`
  The local scale harness defaults allocation and cold-probe request timeouts to `10000` ms with a
  `15000` ms client protection timeout so warmup/cold-route jitter does not trip server-side
  timeout counters.
- Scale matrix: `CHRONOS_SCALE_MATRIX_WORKERS` defaults to `2,3`; use values such as `2,3,5,8`
  for broader production evidence. The matrix scales load with worker count through
  `CHRONOS_SCALE_MATRIX_CONCURRENCY_PER_WORKER` and
  `CHRONOS_SCALE_MATRIX_TIMELINES_PER_WORKER`, defaulting to `8` and `32`.
  `CHRONOS_SCALE_MATRIX_BENCH_CLIENT_PROCESSES_PER_WORKER` defaults to `1`, so each worker added
  to the matrix also gets an additional benchmark process unless `CHRONOS_SCALE_BENCH_CLIENT_PROCESSES`
  is set explicitly. This prevents a single benchmark process from becoming the default linearity
  bottleneck.
  `CHRONOS_SCALE_MATRIX_LINEAR_EFFICIENCY_MIN` controls the minimum relative throughput
  efficiency versus the first matrix entry and defaults to `0.55` for the local single-host gate.
  Use a stricter value, such as `0.80`, when benchmark clients and Chronos workers run on separate
  production-like hosts. Use a higher `CHRONOS_SCALE_MATRIX_CONCURRENCY_PER_WORKER` in those
  environments for saturation testing.
  `make test-scale-matrix-production` runs the same matrix with `2,3,5,8` workers and a default
  `0.80` linear-efficiency floor. Use it on production-like hosts with isolated benchmark clients
  before claiming linear scale-out capacity.
- Rebalance: `CHRONOS_REBALANCE_ALLOCATE_SUCCESS_PER_SEC_MIN`,
  `CHRONOS_REBALANCE_ALLOCATE_LATENCY_P95_US_MAX`,
  `CHRONOS_REBALANCE_ALLOCATE_LATENCY_P999_US_MAX`,
  `CHRONOS_REBALANCE_ROUTE_REFRESH_P95_US_MAX`,
  `CHRONOS_REBALANCE_ROUTE_REFRESH_P999_US_MAX`,
  `CHRONOS_REBALANCE_TRANSFER_LATENCY_P95_US_MAX`,
  `CHRONOS_REBALANCE_TRANSFER_LATENCY_P999_US_MAX`

## Runbooks

For alert-to-action guidance, use the runbook index in `observability/README.md`.

## Dependency policy

Chronos now ships a repo-local `deny.toml` and executable dependency gate:

```bash
make dependency-check
```

This enforces advisory, license, source, and wildcard dependency policy before release promotion.

## What this repo proves vs what still needs environment evidence

This repo proves the single-repo release gate, startup contracts, alert rule validity,
single-node/local etcd validation harnesses, and route-aware scale smoke with disjoint generator
ownership. `chronos-bench` maintains a configurable gRPC connection pool per owner endpoint and
reuses those channels across worker tasks. The scale harness can also run multiple benchmark client
processes and aggregate their output with `CHRONOS_SCALE_BENCH_CLIENT_PROCESSES`; scale matrix runs
default to one benchmark process per Chronos worker, while single-size `make test-scale-bench`
keeps one process unless configured explicitly. Local client processes still compete with service
workers, so production capacity claims should come from isolated benchmark clients. `make test-scale-bench`
defaults to two workers; use `CHRONOS_SCALE_WORKERS=N` to run the same harness for one size, or
`make test-scale-matrix` with `CHRONOS_SCALE_MATRIX_WORKERS=2,3,5,8` for a multi-size local matrix.
Use `make test-scale-matrix-production` for the stricter production-style linearity gate once the
benchmark clients are isolated from Chronos workers. Keep the generated `scale-matrix/summary.txt`
with release evidence; it includes per-worker linear efficiency and the minimum expected
throughput at the configured efficiency floor. Evidence verification also rechecks that each matrix
entry contains throughput, linear-efficiency, zero-allocation-failure metrics, and profile p95/p99
latency evidence, so incomplete or stale scale artifacts cannot pass the release evidence gate.
Scale `profile-summary.txt` also contains derived per-worker average and p95/p99/p999 allocation,
cached admission-wait, cached serve, proxy-wait latencies, plus per-worker allocation share, so a
regression can be triaged from retained artifacts before collecting deeper host profiles.

For Kubernetes scale changes, generate a planned ownership transition first:

```bash
make kubernetes-scale-plan CHRONOS_OWNERSHIP_OLD_WORKERS=3 CHRONOS_OWNERSHIP_NEW_WORKERS=5
```

Apply the generated ownership plan ID, StatefulSet replica count, ConfigMap modulo, and PDB
`minAvailable=replicas-1` together. The manifest validator rejects mismatched replicas/modulo/PDB
because a generic or partial scale change can create overlapping generator ownership.

You still need environment evidence for clustered etcd quorum behavior, backup/restore drills,
larger worker counts, staged rollout safety, and production alert threshold tuning.
