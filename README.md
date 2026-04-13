# Chronos

The intended application-facing model is:

1. Create one client bound to one timeline
2. Call one allocation API

Everything else is internal client logic.

## Public Client Contract

Go:

- `client, err := chronos.New(ctx, addr, timelineKey)`
- `defer client.Close()`
- `client.AllocateTimestamps(ctx, count)`

Rust:

- `let client = Client::connect(addr, timeline_key).await?`
- `client.allocate_timestamps(count).await?`

Java:

- `client = new Client(addr, timelineKey)`
- `client.allocateTimestamps(count)`
- `client.close()`

C++:

- `Client client(addr, timeline_key)`
- `client.AllocateTimestamps(count)`

Advanced Rust configuration should use `ClientConfig` builder methods, not public fields.

## Notes

- Your application should not call internal routing RPCs directly.
- `timeline_key` is chosen once when the client is created.
- Allocation is the only operation the application should need during normal use.
- Route ensure, route refresh, and stale-route retry are internal client behavior.
- Route refresh reconnects allocation traffic to the current owner endpoint.

## Clients

- Client index: `clients/README.md`
