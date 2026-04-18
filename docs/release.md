# Chronos Release Checklist

This document is the release gate companion to `docs/production.md`. Use it when you are about to
promote a build, not as the general operations guide.

## Required validation before cutting a release

Run all of the following from a clean working tree:

```bash
cargo clippy --locked --all-targets -- -D warnings
make test-layer-0
make test-layer-2
make test-layer-3
make test-layer-4
make observability-check
make dependency-check
make test-soak
make test-chaos
make test-failover-bench
make test-rebalance-bench
```

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
3. retained soak/chaos/failover/rebalance artifacts for the validation run
4. alert rules validated with `make observability-check`
5. dependency policy validated with `make dependency-check`

## Rollback expectation

If any benchmark budget or observability validation fails after a release candidate build, do not
promote the artifact. Revert to the last candidate whose validation bundle is intact.

For the full rollback procedure, see `docs/rollback.md`.
