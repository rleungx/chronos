# Chronos C++ Client

`Client` is the C++ application-facing Chronos client implementation.

## Public shape

- `Client client(addr, timeline_key)`
- `client.AllocateTimestamps(count)`

## Behavior

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled with one refresh-and-retry cycle
- Route refresh reconnects allocation traffic to the current owner endpoint
- Other RPC failures are returned to the caller

## Files

- Implementation: `clients/cpp/client.h`, `clients/cpp/client.cc`
- Build skeleton: `clients/cpp/CMakeLists.txt`

## Minimal usage

```cpp
Client client("127.0.0.1:50051", "orders.primary");
auto ranges = client.AllocateTimestamps(1);
std::cout << "tso=" << ranges.front().start_tso() << std::endl;
```
