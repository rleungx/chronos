# chronos

`chronos` is the Go application-facing Chronos client.

## Public shape

- `client, err := chronos.New(ctx, addr, timelineKey)`
- `defer client.Close()`
- `client.AllocateTimestamps(ctx, count)`

Advanced configuration should use `NewWithOptions(...)` and package options such as
`WithDesiredResourceTier(...)` and `WithRequestTimeoutMs(...)`.

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

    client, err := chronos.New(ctx, "127.0.0.1:50051", "orders.primary")
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
- Other RPC failures are returned to the caller
