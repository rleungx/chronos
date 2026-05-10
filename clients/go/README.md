# chronos

`chronos` is the Go application-facing Chronos client.

Support level: Primary.

## Public shape

- `client, err := chronos.New(ctx, addr, timelineKey)`
- `defer client.Close()`
- `client.AllocateTimestamps(ctx, count)`

Advanced configuration should use `NewWithOptions(...)` and package options such as
`WithDesiredResourceTier(...)`, `WithRequestTimeoutMs(...)`,
`WithStaleRouteRetryAttempts(...)`, `WithStaleRouteRetryBackoffMs(...)`,
`WithIdempotency(...)`, and transport options.

## Build and test

```bash
cd clients/go
go test ./...
```

## Example

```bash
cd examples/go
go run .
```

## Behavior

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled with a configurable refresh-and-retry budget
- Route refresh reconnects allocation traffic to the current owner endpoint
- Allocation request-record idempotency is disabled by default; use `WithIdempotency(true)`
  when callers need replay protection for ambiguous retries
- Other RPC failures are returned to the caller
