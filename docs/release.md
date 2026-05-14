# Chronos Release Checklist

This document is the release gate companion to `docs/production.md`. Use it when you are about to
promote a build, not as the general operations guide.

## Required validation before cutting a release

Run the full repo-local release gate from a clean working tree:

```bash
make release-gate
```

`make release-gate` retains long-running validation evidence under `artifacts/release-gate` by
default and verifies it before the gate passes.

This is equivalent to the following validation bundle:

```bash
make release-check
make release-security-check
make release-gate-layer-4-clustered
make test-soak
make test-chaos
make test-failover-bench
make test-auto-failover-bench
make test-scale-matrix-production
make test-rebalance-bench
make test-restore-dr
bash hack/verify-evidence.sh artifacts/release-gate
```

For production scale evidence, also run `make test-scale-matrix-production` on production-like
hosts with benchmark clients isolated from Chronos workers. Archive the generated
`scale-matrix/summary.txt`; it records per-worker throughput, linear efficiency, and the minimum
expected throughput for the configured efficiency floor. The scale harness also retains
`host-info.txt`, `host-load-before.txt`, `host-load-after.txt`, and profile summaries with p95/p99
stage latency upper bounds, per-worker allocation share, and per-worker route ownership. `make
release-evidence-check` verifies that retained scale-matrix evidence includes every worker size, a
passing linear-efficiency value, zero allocation failures, per-node logs/metrics/readiness snapshots,
host snapshots, and derived profile summaries for latency triage.

## Required config checks

Validate production-facing config behavior from the repo itself:

```bash
cargo run --bin chronos -- --check-config
cargo run --bin chronos -- --print-effective-config
```

For production-profile builds, provide an auditable build identity:

```bash
export CHRONOS_PROFILE=production
export CHRONOS_BUILD_COMMIT="$(git rev-parse HEAD)"
```

## Artifact expectations

Before publishing or deploying, ensure you have:

1. the exact git commit
2. the exact `CHRONOS_BUILD_COMMIT` used at runtime
3. retained soak/chaos/failover/scale/rebalance/restore artifacts for the validation run
4. alert rules validated with `make observability-check`
5. dependency policy validated with `make dependency-check`
6. cross-language client behavior validated with `make client-conformance-check`
7. Kubernetes manifests validated for static partitioned ownership via `make kubernetes-manifest-check`
8. release-shape and container delivery checks validated via `make release-check`
9. `BUILD_INFO`, SBOM/hash artifacts, and container vulnerability scanning validated via `make release-security-check`

## Rollback expectation

If any benchmark budget or observability validation fails after a release candidate build, do not
promote the artifact. Revert to the last candidate whose validation bundle is intact.

For the full rollback procedure, see `docs/rollback.md`.
