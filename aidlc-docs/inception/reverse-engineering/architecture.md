# System Architecture

The Rust service exposes gRPC route, control, status, and timestamp APIs. `TsoService` coordinates timeline runtime state and generator leases through a `ControlPlaneStore`; production uses etcd. `TimelineScopedAllocator` serializes allocations per timeline. Background maintenance persists generator progress and renews leases. Shell benchmarks exercise soak, chaos, scale, failover, rebalance, and restore workflows. GitHub Actions packages one release build and reuses it across those gates.

The repair keeps these boundaries intact: runtime lease coordination is fixed inside the service, timeout attribution inside the proxy, and release-gate defects inside scripts and workflows.
