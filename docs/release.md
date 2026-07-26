# Chronos Release Checklist

This document is the release gate companion to `docs/production.md`. Use it when you are about to
promote a build, not as the general operations guide.

## Required validation before cutting a release

Run the full repo-local release gate from a clean working tree:

```bash
make release-gate
```

`make release-gate` retains long-running validation evidence under `artifacts/release-gate` by
default and verifies it before the gate passes. The gate copies `BUILD_INFO` into that directory
and rejects evidence whose git/build commit differs from the current release commit.

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
make test-rolling-upgrade
bash hack/verify-evidence.sh artifacts/release-gate
```

The production gate runs a one-hour allocation soak followed by two five-minute control-plane
status passes, at least 30 seconds of post-chaos traffic, 60-second failover windows, and the fixed
2/3/5/8 scale matrix with an 80% linear-efficiency floor. Scheduled and release-candidate soak jobs
reserve 90 minutes for that workload plus setup and teardown. The gate does not accept the
single-host plateau escape hatch. For local iteration use `make test-soak-quick`,
`make test-chaos-quick`, and `make test-failover-bench-quick`; quick evidence is intentionally not
eligible for release promotion.

`make test-scale-matrix-production` applies the 2/3/5/8 production workload budget, but the current
harness co-locates clients, Chronos workers, and etcd and emits only
`evidence_class=co_located_stress`. Its artifacts remain useful for regression triage, but even a
stress PASS is not production scale evidence. Release verification fails as `UNVERIFIED` until a
separate L2 observed-topology producer and verifier exist.

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
4. retained same-format rolling-upgrade evidence from the pinned historical commit to the exact release commit
5. alert rules validated with `make observability-check`
6. dependency policy validated with `make dependency-check`
7. cross-language client behavior validated with `make client-conformance-check`
8. Kubernetes manifests validated for static partitioned ownership via `make kubernetes-manifest-check`
9. release-shape and container delivery checks validated via `make release-check`
10. `BUILD_INFO`, SBOM/hash artifacts, and container vulnerability scanning validated via `make release-security-check`

After the gate passes on the exact release commit, create an annotated root tag such as `v0.1.0`.
The tag-triggered Release Candidate workflow reruns the authoritative gates, attests the artifacts,
and publishes the GitHub release only after every required job succeeds. The release checks reject
a root tag that is not `v` plus stable SemVer (prerelease and build metadata are not
supported) or does not exactly match the root Cargo package, lockfile, Helm chart/app/image versions,
and Docker OCI version label. Normal source checks require an `[Unreleased]` changelog section; a
tagged release additionally requires a heading for that exact version. Run `make
release-version-check` locally while preparing the version surfaces, and add the versioned changelog
heading before creating the tag. Publish the Go module with the matching subdirectory tag, for
example `clients/go/v0.1.0`.

```bash
CHRONOS_RELEASE_TAG=v0.1.0 make release-version-check
```

Go, Java, and C++ clients have independent release tags and distributions. Follow
`clients/compatibility.md`; the `Client Release` workflow validates, packages, and publishes exactly
one client for each `clients/<language>/vX.Y.Z` tag.

## Rollback expectation

If any benchmark budget or observability validation fails after a release candidate build, do not
promote the artifact. Revert to the last candidate whose validation bundle is intact. A release
that advances `CURRENT_CLUSTER_FORMAT_VERSION` requires a quiesced all-worker upgrade; after its
etcd marker is written, an older binary must not be restarted on that prefix. Restore a pre-upgrade
snapshot to a separate prefix when a format-level rollback is required.

`make test-rolling-upgrade` is narrower than a version-support promise: it builds the exact
historical SHA pinned in `hack/upgrade/baseline.env` and the current exact SHA, verifies that package
version, metadata schema, and cluster format are unchanged, then exercises forward replacement
`0,1,2` and rollback replacement `2,1,0` without restarting etcd or the continuous allocator. It
also requires a current-created request to replay with the complete same protobuf response under
the historical binary before a fresh historical request and the allocator advance again. This is
evidence only for the two pinned same-format commits—even when both package versions are `0.1.0`.
It is not a SemVer downgrade promise and does not cover compatibility across a format change.

For the full rollback procedure, see `docs/rollback.md`.
