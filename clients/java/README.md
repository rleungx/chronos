# Chronos Java Client

`Client` is the Java application-facing Chronos client implementation.

Support level: Repository-local implementation with Maven publication metadata.

## Public shape

- `client = new Client(addr, timelineKey)`
- `config = Client.Config.defaults()...`
- `client = new Client(addr, timelineKey, config)`
- `client.allocateTimestamps(count)`
- `client.close()`

`Client.Config` covers desired resource tier, request timeout, stale-route retry attempts/backoff,
idempotency, and transport. The transport config supports TLS, mTLS, authority override, and
explicit plaintext for local development.

## Build and test

```bash
cd clients/java
gradle test
```

To validate the example without adding it to the library artifact:

```bash
cd clients/java
gradle compileExampleJava
```

## Behavior

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled with a configurable refresh-and-retry budget
- Route refresh reconnects allocation traffic to the current owner endpoint
- Allocation request-record idempotency is disabled by default; use
  `Client.Config.defaults().withIdempotency(true)` when callers need replay protection
- Normal allocation calls are safe to run concurrently on one client instance
- Other RPC failures are returned to the caller

## Example

```bash
cd clients/java
gradle runExample
```
