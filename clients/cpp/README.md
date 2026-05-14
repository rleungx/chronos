# Chronos C++ Client

`Client` is the C++ application-facing Chronos client implementation.

Support level: Repository-local implementation with CMake install/export metadata.

## Public shape

- `Client client(addr, timeline_key)`
- `Client::Config config;`
- `Client client(addr, timeline_key, config)`
- `client.AllocateTimestamps(count)`

`Client::Config` covers desired resource tier, request timeout, stale-route retry attempts/backoff,
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
- Allocation request-record idempotency is disabled by default; set
  `config.idempotency_enabled = true` when callers need replay protection
- Other RPC failures are returned to the caller

## Example

```bash
cmake -S clients/cpp -B clients/cpp/build
cmake --build clients/cpp/build --target client_example
```
