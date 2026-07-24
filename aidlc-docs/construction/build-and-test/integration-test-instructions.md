# Integration and Static Test Instructions

## Verified Commands

```bash
CARGO_TARGET_DIR=/tmp/chronos-target.uiqK2v make test-release-core
CARGO_TARGET_DIR=/tmp/chronos-target.uiqK2v cargo clippy --locked --all-targets -- -D warnings
cargo fmt --all -- --check
bash -n hack/*.sh hack/*/*.sh
actionlint -no-color .github/workflows/ci.yml .github/workflows/release-candidate.yml
cargo deny check
make client-conformance-check
make proto-generated-check
git diff --check
```

All commands completed successfully. Tests that explicitly require a reachable etcd remained ignored by the project test harness.
