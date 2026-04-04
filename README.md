# Chronos

Chronos is a correctness-first Rust timestamp service for many independent timelines.

It issues `uint64` TSOs, keeps the hot allocation path local to the legal owner, and treats
metadata as the authority for routing, ownership, and recovery boundaries.

## What Chronos is optimized for

Chronos is built around one idea: **correctness is scoped to a single `timeline_key`**.

For successful allocations on one timeline, Chronos is designed to provide:

- globally unique successful TSOs across the cluster
- strict monotonicity and linearizability for that timeline
- rejection of stale route / epoch / ownership views
- no rollback across graceful transfer or failover once the recovery boundary is established in an
  authoritative metadata store

Different timelines are intentionally independent. Chronos does **not** try to provide a total
order across the whole cluster.

## Core model

- one TSO is one `uint64`
- the current wire format is:

  ```text
  physical_ms(40) + generator_id(13) + sequence(11)
  ```

- `timeline_key` is the unit of correctness
- a timeline maps to a route: owner endpoint, generator, epoch, and route version
- only one legal owner may issue for one timeline at a time
- watch is a convergence hint; authoritative route reads remain the correctness source

## Service shape

Chronos currently provides:

- a Rust library crate and a `chronos` binary
- gRPC APIs for timeline creation, routing, allocation, transfer, and watch flows
- timeline status read APIs
- health, readiness, and Prometheus metrics endpoints
- in-memory metadata for local runs and etcd-backed metadata for authoritative routing

The binary is currently environment-driven rather than a stable flag-driven CLI.

## Verification posture

This repository is not relying on a single smoke test. It uses layered verification:

- **Layer 0**: format, lint, and static checks
- **Layer 1**: library and public API smoke tests
- **Layer 2**: binary startup and transport seam tests
- **Layer 3**: semantic regression tests for routing, proxying, rebalance, and transfer behavior
- **Layer 4**: real-etcd validation

The CI workflow runs Layers 0-4 on normal CI flows, with a dedicated real-etcd job for Layer 4.

Recent kernel hardening also added explicit regression coverage for:

- stale cached route serving after missed updates
- stale cached lifecycle state serving
- lease expiry across slow timeline loads
- lease expiry across slow generator loads
- process-local monotonic wall-clock behavior

## Fast local validation

```bash
cargo check --all-targets
cargo test --lib
cargo test --test lifecycle_semantics
cargo test --test rpc_semantics
```

For the full repository validation flow:

```bash
make test-layer-0
make test-layer-1
make test-layer-2
make test-layer-3
make test-layer-4
```

For longer-running production-readiness harnesses:

```bash
make test-soak
make test-chaos
```

`test-soak` exercises an etcd-backed release-mode service with `chronos-bench` and
`chronos-control-bench`. `test-chaos` injects an etcd outage, verifies Chronos degrades or exits,
restores etcd, restarts Chronos, and runs a recovery smoke benchmark.

When `CHRONOS_ARTIFACT_DIR` is set, both harnesses retain logs, summaries, metrics snapshots, and
etcd diagnostics under that directory instead of deleting them on success. Each harness also writes
an `artifact-index.txt` alongside `summary.txt` so CI and operators can quickly see what was
captured. The CI soak/chaos jobs use this to upload artifacts for post-mortem debugging.

## Minimal local run

If `CHRONOS_METADATA` is unset, Chronos runs with in-memory metadata.

```bash
export CHRONOS_SECURITY_MODE=dev-insecure
export CHRONOS_BIND_ADDR=127.0.0.1:50051
export CHRONOS_ADVERTISE_ENDPOINT=127.0.0.1:50051

cargo run --bin chronos
```

## Minimal etcd-backed run

```bash
make etcd-up
make etcd-health

export CHRONOS_SECURITY_MODE=dev-insecure
export CHRONOS_METADATA=etcd
export CHRONOS_BIND_ADDR=127.0.0.1:50051
export CHRONOS_ETCD_ENDPOINTS=127.0.0.1:2379
export CHRONOS_ETCD_PREFIX=/chronos-local
export CHRONOS_WORKER_ID=worker-a
export CHRONOS_ADVERTISE_ENDPOINT=127.0.0.1:50051

cargo run --bin chronos
```

Layer 4 validation requires Docker / Docker Compose because it boots a local etcd for the test
run.

## Client flow

The smallest useful remote sequence is:

1. `Health`
2. `EnsureTimeline`
3. `GetTimelineRoute`
4. `AllocateTimestamps`

`AllocateTimestamps` must use the `epoch` and `route_version` from the route it is acting on.
If either becomes stale, the client must refresh the route and retry as a new request.

## Current scope and limits

Chronos is already strong on kernel correctness and verification, but this repository is still
primarily the service/kernel itself.

What is already present:

- explicit startup preflight validation
- enforced production-profile build identity checks
- security-mode and TLS validation
- health / readiness / metrics surfaces
- layered semantic and real-etcd verification

What is not yet a first-class part of this repository:

- a stable operator CLI surface
- official deployment artifacts such as Dockerfile / Helm / Kubernetes manifests
- a fully productized release and deployment layer

## Repository layout

- `src/`: service, runtime, metadata, routing, RPC, startup, and kernel logic
- `tests/`: semantic and integration regression suites
- `hack/`: validation helpers and local etcd workflow
- `.github/workflows/`: layered CI and real-etcd verification
- `tso.proto`: public protobuf service definition
