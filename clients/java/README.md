# Chronos Java Client

`Client` is the Java application-facing Chronos client implementation.

## Public shape

- `client = new Client(addr, timelineKey)`
- `client.allocateTimestamps(count)`
- `client.close()`

## Behavior

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled with one refresh-and-retry cycle
- Route refresh reconnects allocation traffic to the current owner endpoint
- Other RPC failures are returned to the caller

## Files

- Implementation: `clients/java/src/main/java/chronos/client/Client.java`
- Build skeleton: `clients/java/build.gradle.kts`

## Minimal usage

```java
try (Client client = new Client("127.0.0.1:50051", "orders.primary")) {
  var ranges = client.allocateTimestamps(1);
  System.out.println("tso=" + ranges.get(0).getStartTso());
}
```
