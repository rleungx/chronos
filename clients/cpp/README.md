# Chronos C++ Client

`Client` is the C++ application-facing Chronos client implementation.

Support level: Release-ready implementation with versioned CMake package archives.

## Install

C++ releases use tags such as `clients/cpp/v0.1.0` and attach a platform-specific `.tar.gz` plus
portable SHA-256 checksums and build provenance. After extracting the archive under a chosen prefix:

```cmake
find_package(chronos CONFIG REQUIRED)
target_link_libraries(your_target PRIVATE chronos::client)
```

The published binary package targets Ubuntu 24.04 x86-64 and uses that distribution's gRPC and
protobuf development libraries. Build from source on other platforms; no cross-distribution C++ ABI
compatibility is claimed.

See `../compatibility.md` for the compatibility commitment.

## Public shape

- `Client client(addr, timeline_key)`
- `Client::Config config;`
- `Client client(addr, timeline_key, config)`
- `client.AllocateTimestamps(count)`

`Client::Config` covers desired resource tier, request timeout, route-recovery retry attempts/backoff,
idempotency, and transport. The transport config supports TLS, mTLS, server-name override, and
explicit plaintext for local development.

## Build and test

```bash
cmake -S clients/cpp -B clients/cpp/build
cmake --build clients/cpp/build
ctest --test-dir clients/cpp/build --output-on-failure
```

## Behavior

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled with a configurable refresh-and-retry budget
- Route refresh reconnects allocation traffic to the current owner endpoint
- Owner transport failures refresh through the stable construction endpoint; use a Service or load
  balancer rather than an owner-pod address in production
- Allocation request-record idempotency is disabled by default; set
  `config.idempotency_enabled = true` when callers need replay protection
- Other RPC failures are returned to the caller

## Example

```bash
cmake -S clients/cpp -B clients/cpp/build
cmake --build clients/cpp/build --target client_example
```
