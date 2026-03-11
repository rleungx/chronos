# Chronos TSO (Rust)

Chronos is a multi-timeline TSO example implementation designed to provide:

- a single `uint64 tso`
- global uniqueness
- strict monotonicity and linearizability within a single `timeline_key`
- migration, failover, lease/fencing, and recovery-floor protection

See `design.md` for the design document and `tso.proto` for the protocol definition.

## Architecture Highlights

- Timestamp encoding: `40 bits physical_ms + 13 bits generator_id + 11 bits sequence`
- `timeline_key` is not encoded into `tso`; routing is maintained by the control plane and client-side routing layer
- At any time, a single timeline can have only one legal owner issuing timestamps
- Requests for the same timeline must pass through a single serial ingress point before they enter the allocation path
- Generator-level lease, owner identity, lease token, and `issued_upper_bound` provide fencing
- Migration and failover rely on persisted `recovery_floor_tso` so timestamps do not go backwards even if the new owner restarts before its first post-cutover allocation
- watch is only a convergence accelerator, not a correctness source

## Implemented APIs

- `EnsureTimeline`
- `GetTimelineRoute`
- `WatchTimelineRoutes`
- `AllocateTimestamps`
- `TransferTimeline`
- `Health`

## Quick Start

Run the demo with in-memory metadata:

```bash
cargo run --bin chronos-tso-demo
```

Default listeners:

- gRPC: `CHRONOS_TSO_BIND_ADDR=[::1]:50051`
- Metrics: `CHRONOS_TSO_METRICS_BIND_ADDR=127.0.0.1:9898`

## Using Etcd as Metadata

```bash
export CHRONOS_TSO_METADATA=etcd
export CHRONOS_TSO_ETCD_ENDPOINTS=127.0.0.1:2379
export CHRONOS_TSO_ETCD_PREFIX=/chronos-tso
cargo run --bin chronos-tso-demo
```

In the current implementation, hot Etcd writes are limited to:

- generator-level lease / ownership hot state
- low-frequency timeline routing and migration state updates

## Run Tests

```bash
cargo test
```

Benchmark:

```bash
cargo run --release --bin chronos-bench
```

## Configuration

Base configuration:

- `CHRONOS_TSO_BIND_ADDR`
- `CHRONOS_TSO_METADATA`: `memory` / `etcd`
- `CHRONOS_TSO_ETCD_ENDPOINTS`
- `CHRONOS_TSO_ETCD_PREFIX`
- `CHRONOS_TSO_METRICS_BIND_ADDR`

Core TSO configuration:

- `CHRONOS_TSO_WORKER_ID`
- `CHRONOS_TSO_INSTANCE_ID`
- `CHRONOS_TSO_ADVERTISE_ENDPOINT`
- `CHRONOS_TSO_SHARED_GENERATORS`
- `CHRONOS_TSO_WARM_GENERATORS`
- `CHRONOS_TSO_DEFAULT_RESOURCE_TIER`
- `CHRONOS_TSO_MAX_BATCH_PER_REQUEST`
- `CHRONOS_TSO_MAX_FUTURE_BORROW_MS`
- `CHRONOS_TSO_MAX_CLOCK_REWIND_MS`
- `CHRONOS_TSO_LEASE_TTL_MS`
- `CHRONOS_TSO_GENERATOR_LEASE_TTL_MS`
- `CHRONOS_TSO_GENERATOR_MAINTENANCE_INTERVAL_MS`
- `CHRONOS_TSO_SHARED_JUMP_AHEAD_THRESHOLD_MS`
- `CHRONOS_TSO_SAFETY_GAP_MS`
- `CHRONOS_TSO_ROUTE_CACHE_TTL_MS`
- `CHRONOS_TSO_MAX_TIMELINE_PROXY_LANES`
- `CHRONOS_TSO_MAX_TIMELINE_RUNTIME_ENTRIES`
- `CHRONOS_TSO_GENERATOR_OWNERSHIP_MODULO`
- `CHRONOS_TSO_GENERATOR_OWNERSHIP_REMAINDER`

## Current Implementation Boundaries

- Guarantees strict monotonicity and linearizability within a single timeline
- Does not guarantee a strict total order across timelines
- Allows gaps
- If multiple clients share one timeline and require “earlier requester gets smaller TSO”, they should use a timeline-scoped serial proxy instead of independent local prefetch pools
