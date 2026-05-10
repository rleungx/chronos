# Chronos C++ Client

`Client` is the C++ application-facing Chronos client implementation.

Support level: Repository-local implementation.

## Public shape

- `Client client(addr, timeline_key)`
- `Client client(addr, timeline_key, transport_config, idempotency_enabled)`
- `client.AllocateTimestamps(count)`

## Build and test

```bash
cmake -S clients/cpp -B clients/cpp/build
cmake --build clients/cpp/build
ctest --test-dir clients/cpp/build --output-on-failure
```

## Behavior

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled with one refresh-and-retry cycle
- Route refresh reconnects allocation traffic to the current owner endpoint
- Allocation request-record idempotency is disabled by default; use the constructor overload with
  `idempotency_enabled=true` when callers need replay protection
- Other RPC failures are returned to the caller

## Files

- Implementation: `clients/cpp/client.h`, `clients/cpp/client.cc`
- Build skeleton: `clients/cpp/CMakeLists.txt`

## Minimal usage

```cpp
Client::TransportConfig transport;
transport.insecure = true;
Client client("127.0.0.1:50051", "orders.primary", transport);
auto ranges = client.AllocateTimestamps(1);
std::cout << "tso=" << ranges.front().start_tso() << std::endl;
```
