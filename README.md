# Chronos

Chronos is a gRPC timestamp service for applications that need monotonic, timeline-scoped timestamp
allocation without carrying route, ownership, or failover logic in application code.

> Project status: pre-release. The correctness and operations gates are actively exercised, but no
> stable version has been published. Production adoption should pin a reviewed commit and run the
> full release gate in the target environment.

The application path is intentionally small:

1. Pick a `timeline_key` for the workload shard
2. Create one Chronos client bound to that timeline
3. Call `allocate_timestamps(count)`

The client handles timeline creation, route lookup, owner reconnects, stale-route refresh, and
failover recovery.

The `TimestampService` wire API also provides the bidirectional `AllocateTimestampsStream` RPC for
pipelined allocation traffic. Stream responses preserve request order and use the same routing,
timeout, idempotency, authorization, and structured-error semantics as the unary RPC. The
application-facing clients continue to use the unary API so their route-recovery contract remains
unchanged.

In production, construct clients with a stable control-plane address (for example, a Kubernetes
Service). Advertised owner endpoints may change or disappear during failover; the control-plane
address must remain reachable so clients can refresh their cached route.

## When to use Chronos

- You need monotonic timestamp ranges for each logical workload or partition
- You want clients to keep working through route changes and owner failover
- You can scale throughput by using multiple `timeline_key` shards or larger allocation batches
- You do not require one global strictly increasing sequence across all traffic

## Quick Start

Run a local memory-backed server:

```bash
export CHRONOS_SECURITY_MODE=dev-insecure
export CHRONOS_BIND_ADDR=127.0.0.1:50051
export CHRONOS_ADVERTISE_ENDPOINT=127.0.0.1:50051
cargo run --bin chronos
```

Then run a client example:

```bash
cargo run --example client_example
```

More examples:

- Rust: `examples/rust/client_example.rs`
- Go: `examples/go/main.go`
- Java: `examples/java/ClientExample.java`
- C++: `examples/cpp/client_example.cc`

## Client Integration

Start with `clients/README.md`.

Supported application-facing clients:

| Language | Status |
|---|---|
| Rust | Primary |
| Go | Primary |
| Java | Repository-local |
| C++ | Repository-local |

Applications should use the client libraries instead of calling route-management RPCs directly.
Route RPCs remain part of the wire contract, but normal allocation traffic only needs the client
allocation API.

## Production

Production deployments should use etcd metadata, routable advertise endpoints, explicit security
configuration, and capacity sized for active timelines. For horizontal allocation scale, partition
traffic across timeline keys and configure multi-node generator ownership.
The Kubernetes manifest is intentionally a static partitioned StatefulSet; scale it through a
planned ownership-plan change rather than a generic HPA. Use `make kubernetes-scale-plan` before
changing replicas, ownership modulo, or PDB settings.
Ownership-plan and worker-count changes currently require a quiesced, scale-to-zero migration; they
are not online elastic scaling operations.

Operator docs:

- Production guide: `docs/production.md`
- Release gate: `docs/release.md`
- Rollback: `docs/rollback.md`
- Observability: `observability/README.md`
