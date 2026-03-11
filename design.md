# Multi-Timeline TSO Design

## 1. Goals

This document defines the core Chronos TSO design. The system is intended to provide:

- a single externally visible `uint64 tso`
- global uniqueness
- strict monotonicity and linearizability within one `timeline_key`
- a decentralized allocation hot path
- hotspot isolation, online migration, failover, and recovery

This document keeps only the architectural constraints, state model, and critical flows needed by the current system. It intentionally omits long derivations, operational runbooks, and future-looking expansion details.

---

## 2. Design Summary

1. `tso` does not encode `timeline_key`. It encodes only:

   ```text
   tso = physical_ms(40) + generator_id(13) + sequence(11)
   ```

2. The mapping `timeline_key -> generator_id + owner endpoint + epoch + route_version` is maintained by the control plane and routing layer, not embedded in the timestamp.
3. At any time, a single timeline can have only one legal owner issuing timestamps.
4. Requests for the same timeline must first pass through one serial ingress point before they reach the target generator.
5. Hot correctness state is concentrated on the generator: lease, fencing, and issued upper bound are all owned by `GeneratorRecord`.
6. watch only accelerates convergence. It is not a correctness source. The authority always remains metadata / `GetTimelineRoute`.

---

## 3. Consistency Contract

### 3.1 Guarantees

1. **Global uniqueness**  
   Any two successful `tso` values must be different.

2. **Strict monotonicity and linearizability within one timeline**  
   For the same `timeline_key`, all successful `AllocateTimestamps` calls can be interpreted as a single linearized sequence. The smallest `tso` returned by a later call must be greater than the largest `tso` returned by an earlier call.

3. **Earlier requester gets smaller TSO**  
   For the same `timeline_key`, if request A is accepted by the timeline’s unique serial ingress point before request B, then the largest `tso` returned to A must be smaller than the smallest `tso` returned to B.

4. **No rollback after migration or failover**  
   The first `tso` issued by the new owner must be strictly greater than the maximum safe boundary the old owner could have issued.

5. **Gaps are allowed**  
   Gaps are permitted due to batch allocation, migration, failover, and upper-bound reservation.

### 3.2 Non-Guarantees

- no strict total order across different timelines
- no exact equality with wall-clock time
- no gap-free property
- no guarantee that watch events are lossless or real-time

---

## 4. Core Entities

### 4.1 Timeline

- `timeline_key` is the scope of monotonicity and linearizability
- one timeline binds to only one `generator_id` at a time
- one timeline has only one legal owner at a time

### 4.2 Generator

- `generator_id` is a logical allocation channel ID, not a Worker ID
- one Worker may host multiple generators
- one generator may have only one legal owner instance at a time

### 4.3 Worker / Manager / SDK

- **Worker**: hosts generators and performs allocation
- **Manager**: creates timelines, performs migration, handles hotspot governance, and drives failover
- **SDK / Proxy**: maintains route cache, performs retries, and may batch requests; if the system requires “earlier requester gets smaller TSO”, then it must provide a single serial ingress point for that timeline

### 4.4 Deployment Preconditions

For this design to actually hold in production, the deployment must satisfy the following:

- `owner_instance_id` must be unique across the cluster and must not be reused across concurrently live instances
- `owner_worker_endpoint` is a routable address, not an identity token; fencing is based on instance identity and lease token
- if a Worker is allowed to own only part of the generator space, that ownership partition must be static, deterministic, and identical on every node
- if these assumptions do not hold, lease/fencing semantics only partially hold and incorrect takeover can happen after failover or restart

---

## 5. Architectural Principles

### 5.1 Decentralized hot path

- Metadata and Manager do not participate in every allocation
- allocation happens locally on the Worker
- the control plane handles routing, lease, and migration only

### 5.2 Single serial ingress per timeline

This is the most important constraint in the design.

- requests for the same timeline must pass through one unique serial ingress point
- that ingress point can be a single-writer lane inside the legal owner or a timeline-scoped proxy
- multiple uncoordinated client instances must not independently prefetch for the same timeline and hand out timestamps directly to application code

### 5.3 watch only accelerates convergence

- route watch can reduce fallback reads, but it is never authoritative
- on watch lag, disconnect, keepalive timeout, version conflict, or owner mismatch, clients must fall back to `GetTimelineRoute`

---

## 6. Timestamp Model

### 6.1 Encoding

```text
64 = 40 bits physical_ms + 13 bits generator_id + 11 bits sequence
```

- `physical_ms`: milliseconds from a chosen epoch
- `generator_id`: allocation channel ID
- `sequence`: per-channel sequence inside the current millisecond

### 6.2 Important consequences

- numeric ordering across different generators does not imply real global business ordering
- after moving a timeline to a new generator, `old_tso + 1` is not safe
- the system must use an explicit recovery-floor rule: `next_on_generator(floor, generator_id, now_ms)`

### 6.3 `next_on_generator`

Given a safe floor `floor`, choose the smallest legal value on target `generator_id = g` that is strictly greater than `floor`:

```text
next_on_generator(floor, g, now_ms):
  floor_physical = decode(floor).physical_ms
  p = max(now_ms, floor_physical)
  candidate = pack(p, g, 0)
  if candidate <= floor:
      candidate = pack(p + 1, g, 0)
  return candidate
```

---

## 7. Metadata Model

### 7.1 TimelineRecord

Each timeline keeps one low-frequency primary record:

- `timeline_key`
- `generator_id`
- `owner_worker_endpoint`
- `epoch`
- `route_version`
- `resource_tier`
- `state`
- `last_graceful_issued`
- `recovery_floor_tso`
- `updated_at_ms`

Notes:

- `last_graceful_issued` is used for graceful migration
- `recovery_floor_tso` persists the recovery floor computed during migration/failover so the system does not lose the safety boundary if the new owner restarts before its first post-cutover allocation

### 7.2 GeneratorRecord

Each generator keeps one hot-state record:

- `generator_id`
- `owner_worker_endpoint`
- `owner_instance_id`
- `generator_lease_token`
- `lease_expire_at_ms`
- `last_issued_tso`
- `issued_upper_bound`
- `updated_at_ms`

Notes:

- the generator is the real carrier of fencing semantics
- `issued_upper_bound` defines the safe takeover boundary during failover

### 7.3 Metadata invariants

- one timeline may have only one legal owner at a time
- one `generator_id` may be held by only one legal instance at a time
- `epoch` is monotonically increasing
- `route_version` is monotonically increasing
- `issued_upper_bound` is monotonically increasing
- only the instance that matches `generator_id + owner_instance_id + generator_lease_token` may renew lease and issue timestamps

Additional notes:

- `issued_upper_bound` is not only failover metadata; it may also serve as an allocation-time guard in the hot path
- `TimelineRecord` and `GeneratorRecord` must have separate responsibilities: the timeline record owns routing and recovery boundaries, while the generator record owns lease/fencing and hot-state progression

---

## 8. Normal Allocation

### 8.1 Local state

Each Worker maintains two layers of local state:

- **Generator-level**: `last_physical_ms`, `last_issued_tso`, `sequence`, current lease
- **Timeline-level**: `epoch`, `route_version`, `last_issued_tso`, lifecycle state

### 8.2 Allocation flow

When a timeline requests `count` timestamps:

1. validate `epoch / route_version`
2. validate that the current local instance still holds the legal generator lease
3. read the current timeline floor and the latest generator state
4. compute the allocation floor:

   ```text
   allocation_floor = max(timeline.last_issued_tso, generator.last_issued_tso)
   ```

5. compute the allocation start:

   ```text
   allocation_start = next_on_generator(allocation_floor, generator_id, now_ms)
   ```

6. allocate one or more `TimestampRange` values
7. update local generator state and local timeline state
8. return the result

Additional notes:

- the serial ingress point only guarantees ordering for requests within one timeline; it does not guarantee that the full allocation path is free from cache misses or lease refresh overhead
- in the implementation, `issued_upper_bound` may participate directly in allocation validation, not only in migration/failover recovery

### 8.3 Batch semantics

- the response is `TimestampRange[]`
- in the common case there is only one segment
- crossing millisecond boundaries may produce multiple segments

---

## 9. Routing and SDK / Proxy Constraints

### 9.1 Route model

SDK / Proxy must obtain:

```text
TimelineRoute {
  timeline_key,
  generator_id,
  owner_worker_endpoint,
  epoch,
  route_version,
  resource_tier,
}
```

### 9.2 Recommended routing strategy

- first miss: synchronously call `GetTimelineRoute`
- hot key: use watch or a long-lived subscription
- cold key: use TTL + on-demand fallback reads
- on `NotTimelineOwner / RouteVersionMismatch / EpochMismatch`: immediately fall back to the authority

### 9.3 Local prefetch constraints

- if a timeline is consumed by only one client instance, a local prefetch pool is acceptable
- if multiple client instances may access the same timeline concurrently and the business requires “earlier requester gets smaller TSO”, they must not independently prefetch and issue timestamps locally
- in that case, the system must use a timeline-scoped serial proxy, or send all requests directly into the same serial ingress point on the legal owner

---

## 10. Lifecycle and Migration

### 10.1 Timeline lifecycle

```text
creating -> active -> draining -> locked -> recovering -> active
```

- `creating`: timeline creation in progress, not externally allocatable
- `active`: normal allocation
- `draining`: old owner drains queued work and stops accepting new expansion traffic
- `locked`: failover barrier state, allocation is forbidden
- `recovering`: new owner is restoring local state from a recovery floor

Notes:

- this is the target state machine, not a requirement that every implementation version expose every state explicitly
- the current implementation’s essential guarantee is that the new owner must not allocate safely before the recovery boundary is established; whether that is modeled as an explicit `locked` state or a short-lived `recovering` window can vary by implementation
- therefore, the “must reject allocation” rule in the document should be read as a correctness requirement, not a requirement to implement one exact state-machine shape

### 10.2 Graceful migration

Goal: move a timeline from `G1@A` to `G2@B` without rollback.

Flow:

1. old owner enters `draining`
2. persist `last_graceful_issued`
3. compute the recovery floor
4. atomically update metadata for `generator_id / owner / epoch / route_version / state`
5. persist `recovery_floor_tso`
6. new owner initializes local state from that floor
7. state returns to `active`

For non-failover migration:

```text
safe_floor = max(last_graceful_issued, previous_generator_floor)
```

where `previous_generator_floor = max(last_issued_tso, issued_upper_bound)`.

### 10.3 Failover

Flow:

1. wait for the old lease to expire
2. wait an additional `safety_gap_ms`
3. compute `previous_generator_floor` from the old generator’s persisted metadata
4. if the old generator floor is unavailable, failover must fail explicitly rather than taking over with an unknown boundary
5. persist that floor as `recovery_floor_tso`
6. only after recovery from that floor may the new owner reopen allocation

The failover safety boundary must come from persisted generator state, not from the old owner’s local memory.

If the persisted recovery boundary for the old generator cannot be obtained, the system must reject failover explicitly rather than continuing with an unknown floor.

---

## 11. Resource Tiering

`ResourceTier` only describes which class of generator pool hosts the timeline. It does not change the correctness contract.

- `shared`: many cold timelines share generators
- `warm`: medium-traffic timelines use a more isolated shared pool
- `dedicated`: hot timelines get exclusive generators

Principles:

- correctness is guaranteed by `Lease + Epoch + Recovery Floor + Single Serial Ingress`
- `ResourceTier` only controls where those mechanisms run
- if migration to a shared generator would force an excessive jump-ahead, the system should prefer `dedicated`

---

## 12. Clock Strategy

- per-timeline monotonicity takes priority over exact wall-clock fidelity
- when clock rollback is detected, the system may hold `physical_ms` steady and continue through `sequence`
- when rollback or clock lead exceeds tolerated bounds, the instance must lose takeover eligibility or be removed from service

---

## 13. Capacity and Boundaries

### 13.1 Per-generator capacity

With the current bit layout:

- at most `2048` values per millisecond
- theoretical peak of roughly `2.048M QPS` per generator
- `1M QPS` for a single timeline should normally use a dedicated generator

### 13.2 Scaling focus

For `1000K+` timelines, the bottlenecks are not bit width but:

- route cache
- metadata transactions and watch pressure
- local timeline state cache
- the number of timeline-scoped serial ingress points

### 13.3 Explicit boundaries

Production implementations must set explicit upper bounds for:

- SDK route-cache capacity and TTL
- local range-pool capacity
- per-Worker local timeline-state capacity
- watch subscription count and fallback read rate

These objects must not grow without bound as the timeline count grows.

At minimum, the system must define:

- how hot keys are distinguished from cold keys
- whether evicted state can be rebuilt purely from authoritative metadata
- whether timeline-scoped serial ingress points may be cached long-term and when they are reclaimed
- what backpressure and degradation policy applies during watch lag or fallback storms

---

## 14. API Semantics

### 14.1 `GetTimelineRoute`

- returns the current unique legal route
- when watch fails, versions conflict, or owner mismatch is detected, this is the only authoritative resync path

### 14.2 `WatchTimelineRoutes`

- used only as a convergence accelerator
- clients must tolerate lag, disconnects, re-subscription, and replay
- watch must never be treated as the sole correctness source

### 14.3 `AllocateTimestamps`

- this is the core data-plane API
- all successful calls for the same `timeline_key` form one linearized order
- if the recovery boundary has not yet been established, or the implementation still treats the timeline as semantically `locked/recovering`, allocation must be rejected
- if owner identity, lease token, or route is stale, allocation must be rejected and the client must resync
- if the client does not receive a successful response, the result is unknown; retry is treated as a new request

Clients must consume and correctly handle the following error context:

- `redirect_endpoint`: fast redirect for `NotTimelineOwner`-style cases
- `current_epoch`: refresh local route view after `EpochMismatch`
- `current_route_version`: refresh local route view after `RouteVersionMismatch`

The document’s “immediate fallback / redirect” requirement depends on these fields, not only on the error code.

### 14.4 `TransferTimeline`

- this is a control-plane API for migration, hotspot tier changes, and recovery
- it must atomically advance `epoch / route_version / owner / generator / state`

---

## 15. Testing Focus

At minimum, the system should continuously verify:

- global uniqueness
- strict monotonicity and linearizability within one timeline
- ordering guarantees when multiple clients share one timeline
- graceful migration without rollback
- failover without duplication or split brain
- recovery correctness when a restart happens after cutover but before first allocation
- rejection and recovery behavior under stale routes, watch lag, expired leases, and clock anomalies

---

## 16. Summary

The goal of this design is not to build a globally totally ordered timestamp service. It is to:

- use a single `uint64` to represent TSO
- strictly constrain monotonicity and linearizability to one timeline at a time
- guarantee correctness through `Generator lease + epoch + route_version + recovery floor + single serial ingress`
- use routing, resource tiering, and a decentralized data plane to support hotspot throughput and large timeline cardinality

As long as those constraints remain intact, the remaining implementation can continue to evolve around performance, caching, and automatic scheduling.
