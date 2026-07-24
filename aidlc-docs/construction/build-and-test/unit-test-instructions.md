# Unit Test Instructions

## Verified Commands

```bash
CARGO_TARGET_DIR=/tmp/chronos-target.uiqK2v cargo test --locked --lib --quiet
CARGO_TARGET_DIR=/tmp/chronos-target.uiqK2v make test-release-core
```

The complete library suite passed with 359 tests passed and 5 real-etcd tests ignored. The standard release-core gate also passed all selected library, binary, and integration targets.

The first library run inside the restricted sandbox reported seven local-socket `Operation not permitted` failures. The same suite passed outside that restriction, confirming an environment limitation rather than a product regression.
