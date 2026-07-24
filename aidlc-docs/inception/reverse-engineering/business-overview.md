# Business Overview

Chronos is an etcd-backed timestamp allocation service. It owns timeline routes, leases generator shards, allocates monotonic timestamp ranges, supports ownership transfer and failover, and exposes Rust, Go, Java, and C++ clients.

The affected transactions are steady-state allocation, generator lease renewal, restart recovery, scale-out ownership routing, and ownership rebalance.
