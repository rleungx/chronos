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

For `N` partitioned workers, set one shared `CHRONOS_OWNERSHIP_PLAN_ID` and a stable virtual-shard
count in `CHRONOS_GENERATOR_OWNERSHIP_MODULO` on every worker. Assign every virtual shard to one
worker with `CHRONOS_GENERATOR_OWNERSHIP_REMAINDERS`; the supplied Kubernetes startup helper does
this with rendezvous hashing over the StatefulSet ordinals. Startup records the plan in etcd and rejects
mixed plan IDs, mixed modulo values, duplicate remainders, duplicate worker IDs, or duplicate
advertise endpoints on the same etcd prefix. Clients and benchmarks must route allocation requests
to the `owner_worker_endpoint` returned by the timeline route service.

The Kubernetes manifest uses 256 stable virtual shards and three workers. Each pod derives its list
of owned remainders from its StatefulSet ordinal, worker count, and assignment seed. Do not attach a
normal HPA to this StatefulSet: changing worker count changes the rendezvous mapping and must use a
new ownership plan ID and the quiesced migration below.

Before changing worker count, generate an ownership movement plan:

```bash
CHRONOS_OWNERSHIP_OLD_WORKERS=2 CHRONOS_OWNERSHIP_NEW_WORKERS=4 make scale-ownership-plan
```

Treat any worker-count, virtual-shard-count, assignment-seed, ownership-plan, or cluster-format
change as a planned migration, not an in-place toggle. Old and new writers must never run
concurrently against the same etcd prefix. Use this safe sequence:

1. Quiesce allocation and control ingress and confirm clients have stopped creating work.
2. Scale the StatefulSet to zero and wait at least the plan's
   `minimum_identity_lease_wait_ms`. This is only the ceil-to-seconds grant request minimum plus
   `CHRONOS_SAFETY_GAP_MS`; etcd may have selected a longer grant TTL, and live workers update
   their last-confirmed deadline from every keepalive response. After the minimum wait, do not
   activate the new plan until an authoritative prefix query confirms that
   `${CHRONOS_ETCD_PREFIX}/identity/instances/` has no keys:

   ```bash
   check_chronos_identity_prefix_empty() {
     : "${CHRONOS_ETCD_ENDPOINTS:?must match the Chronos production configuration}"
     : "${CHRONOS_ETCD_PREFIX:?must match the Chronos production configuration}"
     local -a etcdctl_args=(--endpoints="${CHRONOS_ETCD_ENDPOINTS}")
     case "${CHRONOS_ETCD_CA_FILE:-}:${CHRONOS_ETCD_CERT_FILE:-}:${CHRONOS_ETCD_KEY_FILE:-}" in
       ::) ;;
       ?*:?*:?*)
         etcdctl_args+=(
           --cacert="${CHRONOS_ETCD_CA_FILE}"
           --cert="${CHRONOS_ETCD_CERT_FILE}"
           --key="${CHRONOS_ETCD_KEY_FILE}"
         )
         ;;
       *)
         echo "CHRONOS_ETCD_CA_FILE/CERT_FILE/KEY_FILE must be all set or all unset" >&2
         return 1
         ;;
     esac
     local active_identity_keys
     active_identity_keys="$(
       ETCDCTL_API=3 etcdctl "${etcdctl_args[@]}" \
         get "${CHRONOS_ETCD_PREFIX}/identity/instances/" --prefix --keys-only
     )" || return
     if [[ -n "${active_identity_keys}" ]]; then
       printf 'active Chronos identity keys remain:\n%s\n' "${active_identity_keys}" >&2
       return 1
     fi
   }
   check_chronos_identity_prefix_empty
   ```

   Keep replicas at zero and repeat the query until it is empty.
3. While replicas remain zero, apply the new binary, cluster format, plan ID, worker count, shard
   count, seed, and PDB.
4. Scale to the desired replica count.
5. Wait for every worker to become ready, then restore ingress and verify route-owner distribution.

For Helm, the drain release uses `replicaCount=0` and an explicit nonzero
`ownership.workerCount` because a zero-replica release cannot derive it. The chart permits topology
or format updates only in that zero-replica release. The activation release sets the new
`replicaCount` and returns `ownership.workerCount` to `0` (derive from replicas). The chart refuses
all online plan/worker/shard/seed and cluster-format changes; there is no unsafe bypass. ConfigMap
changes roll pods automatically; increment
`security.tlsRevision` or `security.allowlistRevision` when rotating same-name external Secrets.

Every identity lease declares `CURRENT_CLUSTER_FORMAT_VERSION`, and etcd stores the persistent
`cluster/format_version` marker. Startup rejects legacy active identities or a different marker.
Once a prefix is upgraded, never start an older binary against it; rollback requires a compatible
binary or restoring a pre-upgrade etcd snapshot to a separate prefix.

The `requests/` and `request_cleanup/` readers are temporary v1 idempotency-key compatibility,
not permanent metadata APIs. Keep them until every worker uses v2, more time than the largest
deployed `CHRONOS_REQUEST_RECORD_RETENTION_MS` plus one cleanup interval has elapsed, and etcd
shows both legacy prefixes are empty. Remove that compatibility only in a later cluster-format
version together with its migration tests.

After activation, verify route-owner distribution and retain the migration plan with release
evidence.
During rebalance, route transfer control calls to the target owner and let that owner choose the
target generator unless you have prevalidated the exact generator. This avoids unsafe shared
generator jump-ahead and allows the owner to fall back to a dedicated generator when catch-up would
otherwise be too large. Allocation clients should set a rebalance-window request timeout, currently
`CHRONOS_REBALANCE_ALLOCATE_REQUEST_TIMEOUT_MS=10000` in the local gate, so transient catch-up waits
are absorbed without exposing allocation failures.
The default `CHRONOS_PRE_BORROW_MS=100` keeps the normal graceful-transfer catch-up window below the
one-second tail-latency budget. Raising it reduces generator metadata renewal frequency but directly
increases the worst-case pause when ownership moves to another worker.

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
   export CHRONOS_MAX_TIMELINE_RECORDS=100000
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

6. Certify and monitor the worker clock bound before enabling traffic:

   ```bash
   export CHRONOS_MAX_CLOCK_SKEW_MS=500
   export CHRONOS_SAFETY_GAP_MS=500
   ```

   `CHRONOS_MAX_CLOCK_SKEW_MS` is the maximum pairwise wall-clock error your NTP/PTP monitoring
   guarantees across workers. Chronos refuses etcd-backed startup when the safety gap is below that
   bound. Alert externally before observed offset approaches the certified value; increasing the
   value delays failover but preserves the lease handoff invariant. The chart defaults both values
   to 500ms instead of assuming a generic Kubernetes cluster can hold 1ms skew.

`CHRONOS_GRPC_MAX_CONCURRENT_REQUESTS` is enforced across the whole process and also per
connection. Requests above the active limit are rejected with `RESOURCE_EXHAUSTED` instead of being
queued indefinitely. `CHRONOS_GRPC_MAX_CONNECTIONS` bounds accepted gRPC connections, so opening
additional HTTP/2 connections cannot create unbounded server tasks. The health and metrics
listeners separately cap active connections and bound TLS handshake/HTTP connection lifetimes.

Graceful shutdown flushes runtime floors and transfers with bounded concurrency for at most 90
seconds. Keep Kubernetes `terminationGracePeriodSeconds` at 120 or higher so background-task drain
and metadata shutdown retain a final 30-second margin; the Helm schema enforces this floor.

The current wire encoding has a 40-bit millisecond physical field starting at
`2026-01-01T00:00:00Z`; its last encodable instant is `2060-11-03T19:53:47.775Z`. Monitor
`tso_capacity_remaining_seconds`. The bundled alert fires with five years remaining so a versioned
encoding and mixed-version migration can be designed, load-tested, and deployed well before the
horizon; Chronos fails closed with `TSO overflow` after the boundary.

## Health and readiness

- `/healthz` indicates process liveness.
- `/readyz` indicates whether Chronos is currently safe to serve.
- In etcd mode, readiness is only reached after Chronos has validated metadata reads/watch subscription and verified that the acquired instance identity lease record was actually written with the expected identity payload.
- `tso_startup_ready == 0` means the instance should not receive traffic.
- `tso_worker_readiness_transitions_total` and `tso_shutdown_total` provide the reason path for
  readiness and shutdown changes.

## Required validation before release

Run the full release gate:

```bash
make release-gate
```

The authoritative command composition, promotion criteria, and retained-evidence requirements are
documented in [`docs/release.md`](release.md). Keep the operational artifact and benchmark guidance
below with the production deployment; do not duplicate the gate command list here.

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
  `CHRONOS_SOAK_CONTROL_DURATION_SECS`, `CHRONOS_SOAK_FILTERED_DURATION_SECS`,
  `CHRONOS_SOAK_CONTROL_RPC_P999_US_MAX`, `CHRONOS_SOAK_CONTROL_SCAN_P999_US_MAX`,
  `CHRONOS_SOAK_FILTERED_RPC_P999_US_MAX`, `CHRONOS_SOAK_FILTERED_SCAN_P999_US_MAX`. The allocation
  phase defaults to 3600 seconds; the two control-plane phases default to 300 seconds each. When
  `CHRONOS_SOAK_DURATION_SECS` is explicitly set for a quick run, both control phases inherit it
  unless their dedicated duration variables are also set.
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
  `CHRONOS_CHAOS_RECOVERY_LATENCY_P999_US_MAX`,
  `CHRONOS_CHAOS_RECOVERY_PROBE_ATTEMPTS`. Chaos recovery requires a successful allocation probe
  after `/readyz` and before the measured recovery window.
- Restore DR: `make test-restore-dr` records an allocation before the snapshot, another acknowledged
  allocation after the snapshot, the recovery floor read from the restored snapshot, and the first
  fresh allocation after restore. The evidence gate requires
  `before_high_water <= persisted_recovery_floor`,
  `before_high_water < post_snapshot_high_water`, and
  `after_first_tso > max(persisted_recovery_floor, post_snapshot_high_water)`. It also verifies that
  replaying the snapshotted request ID returns its original range.
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
  efficiency versus the first matrix entry and defaults to `0.55` for the smoke profile.
  `make test-scale-matrix-production` runs the same matrix with `2,3,5,8` workers and a default
  `0.80` linear-efficiency floor. Both current targets emit co-located stress evidence, not
  production scale evidence.
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
Use `make test-scale-matrix-production` for the stricter 2/3/5/8, 80%-efficiency workload profile;
it still emits `evidence_class=co_located_stress`. Stress verification rechecks every matrix entry,
but production release verification rejects this class as `UNVERIFIED`; a profile name or passing
budget cannot authorize production evidence.
Scale `profile-summary.txt` also contains derived per-worker average and p95/p99/p999 allocation,
cached admission-wait, cached serve, proxy-wait latencies, plus per-worker allocation share, so a
regression can be triaged from retained artifacts before collecting deeper host profiles.

For Kubernetes scale changes, generate a planned ownership transition first:

```bash
make kubernetes-scale-plan CHRONOS_OWNERSHIP_OLD_WORKERS=3 CHRONOS_OWNERSHIP_NEW_WORKERS=5
```

Follow the generated drain and activation phases exactly. Do not apply a new worker count while old
pods are alive. The activation phase applies the plan ID, StatefulSet replica count, ConfigMap shard
mapping, and PDB `minAvailable=replicas-1` together. The manifest validator rejects mismatched
replicas/worker-count/PDB values, and Helm blocks an in-place worker-count change.

`make test-cluster-leader-loss` runs a continuous single-timeline allocation stream against a
three-member etcd cluster, stops the actual etcd leader, observes a different leader and higher
Raft term while the stream is still running, and verifies non-overlapping TSO ranges before,
during, and after the election. It covers quorum-preserving leader loss.

`make test-owner-etcd-partition` keeps all three etcd members healthy while pausing three
owner-specific TCP forwarding paths. A continuous single-timeline stream must span the identity
lease-loss shutdown, observe client-visible failures after Chronos fails closed, and recover only
after the forwarding paths are resumed, the old identity expires, and Chronos is restarted. The
replacement uses a fresh process identity at the same advertised owner endpoint; the first
post-recovery TSO must be strictly above the last acknowledged pre-shutdown TSO. This is a
single-owner control-plane partition gate; separate environment evidence is still required for
quorum loss, multi-owner network isolation, stable-identity restart behavior, larger worker counts,
and production alert threshold tuning.

`make test-rolling-upgrade` builds the exact historical commit pinned in
`hack/upgrade/baseline.env` plus the current exact worktree and first rejects any package-version,
metadata-schema, or cluster-format difference. It then starts three historical workers and keeps one
single-timeline allocation client alive while each worker is replaced in order. The client moves
the same timeline onto every old and replacement instance, records health-reported instance/build
identity, and requires non-empty strictly increasing ranges in old-only, every replacement window,
both mixed-binary windows, and new-only. A bounded maximum success gap prevents readiness-only
evidence from hiding a prolonged allocation outage. This proves only that pinned historical
commit-to-current-head forward rollout; it is not a SemVer compatibility promise and does not cover
N+1-to-N rollback or any rollout that changes `CURRENT_CLUSTER_FORMAT_VERSION`.
