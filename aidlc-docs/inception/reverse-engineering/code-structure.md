# Code Structure

- `src/service/lease.rs`: generator lease acquisition and renewal.
- `src/service/background.rs`: periodic maintenance.
- `src/timeline_proxy.rs`: per-timeline serialization and request deadlines.
- `src/rpc/translation.rs`: public error translation and protection logging.
- `src/metrics.rs`: Prometheus metrics.
- `src/bin/chronos-bench.rs`: allocation load generator.
- `hack/soak/`, `hack/chaos/`, `hack/scale/`, `hack/rebalance/`: release benchmarks.
- `.github/workflows/`: push, scheduled, and release-candidate gates.
- `tests/` and module-local tests: safety, API, integration, and recovery coverage.

The critical anti-pattern is an all-or-nothing lease CAS followed by sequential fallback across every generator, which amplifies one hot-generator conflict into broad lease-renewal pressure.
