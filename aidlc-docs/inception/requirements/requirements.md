# Requirements

## Intent Analysis

- **User request**: `全部修复`
- **Type**: Brownfield bug fix and release-gate repair
- **Scope**: Multiple service, benchmark, and CI components
- **Complexity**: High because lease safety and monotonic timestamp allocation are affected

## Functional Requirements

1. A CAS conflict on one generator must not force every generator renewal through a sequential fallback path.
2. Background renewal must retain retryability and preserve the existing expired-lease safety contract.
3. Proxy timeouts must identify whether time was spent waiting for serialization or executing allocation.
4. Repeated lease-expiration responses must not create an unbounded warning stream.
5. Benchmark measurement must include operations completing during the measured interval.
6. Recovery validation must wait for a successful data-plane probe, not only an HTTP readiness response.
7. Soak and release-candidate job timeouts must cover their configured workloads.
8. Workflow lint must use a Go version compatible with the pinned actionlint version.
9. Benchmark teardown must stop Chronos before etcd.

## Non-Functional Requirements

- Preserve monotonicity, fencing, protobuf compatibility, and the non-retry policy for deadline-exceeded client operations.
- Bound metadata amplification during CAS contention.
- Keep metric label values finite.
- Keep release thresholds strict; do not mask runtime failures by relaxing budgets.
- Add deterministic unit or static regression checks for every configuration repair.

## Security Compliance

SECURITY-03, SECURITY-10, SECURITY-13, and SECURITY-15 apply to logging, pinned CI dependencies, build integrity, and fail-safe error handling. The remaining baseline rules are not changed by this internal repair.
