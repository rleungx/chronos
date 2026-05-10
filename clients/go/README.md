# chronos

`chronos` is the Go application-facing Chronos client.

Support level: Primary.

## Public shape

- `client, err := chronos.New(ctx, addr, timelineKey)`
- `defer client.Close()`
- `client.AllocateTimestamps(ctx, count)`

Advanced configuration should use `NewWithOptions(...)` and package options such as
`WithDesiredResourceTier(...)`, `WithRequestTimeoutMs(...)`, and `WithIdempotency(...)`.

## Build and test

```bash
cd clients/go
go test ./...
```

## Minimal example

```go
package main

import (
    "context"
    "log"

    chronos "github.com/rleungx/chronos"
)

func main() {
    ctx := context.Background()

    client, err := chronos.NewWithOptions(ctx, "127.0.0.1:50051", "orders.primary", chronos.WithInsecureTransport())
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ranges, err := client.AllocateTimestamps(ctx, 1)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("tso=%d", ranges[0].StartTso)
}
```

## Behavior

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled with one refresh-and-retry cycle
- Route refresh reconnects allocation traffic to the current owner endpoint
- Allocation request-record idempotency is disabled by default; use `WithIdempotency(true)`
  when callers need replay protection for ambiguous retries
- Other RPC failures are returned to the caller
