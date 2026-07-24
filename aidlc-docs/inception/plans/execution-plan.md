# Execution Plan

## Scope and Risk

- **Transformation**: Coordinated repair within existing runtime and CI boundaries.
- **Public API/data model changes**: None.
- **NFR impact**: Availability, tail latency, observability, and release reliability.
- **Risk**: High; generator lease fencing is safety-critical.
- **Rollback**: One repository commit can revert the complete change.

## Component Relationships

- Lease background maintenance and foreground allocation share generator metadata records.
- Timeline proxy deadlines wrap the allocation path.
- Benchmarks validate those runtime paths and feed CI/release gates.

## Execution Sequence

1. Add deterministic runtime and benchmark regression tests.
2. Isolate batch CAS conflicts and add timeout/logging telemetry.
3. Repair recovery probes, soak budgets, lint toolchain, and teardown ordering.
4. Run formatting, unit, static, integration, and available quick release gates.

## Phase Selection

- User stories, application redesign, and infrastructure redesign are skipped because this is an internal bug fix with no new interface.
- Functional/NFR design, code generation, and build/test are executed.

## Success Criteria

- New regression tests pass.
- Existing lease-expiry and monotonicity safety tests remain unchanged and pass.
- Shell/workflow static checks pass.
- Available release quick gates pass; environment-only blockers are reported separately.
- The final diff contains no unrelated generated artifacts.
