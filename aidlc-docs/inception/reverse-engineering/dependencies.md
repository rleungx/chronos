# Dependencies

The timestamp RPC layer depends on `TimelineScopedAllocator`, which delegates to `TsoService`. `TsoService` depends on a metadata authority and maintains local generator/timeline runtime caches. Background lease maintenance and foreground upper-bound refreshes both update the same generator records and therefore must tolerate CAS contention without starving unrelated leases.

Benchmark scripts depend on release binaries and local etcd containers. CI jobs depend on the release-artifact job and must provide enough wall-clock budget for their configured workloads.
