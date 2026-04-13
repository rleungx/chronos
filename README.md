# Chronos

Chronos is a gRPC timestamp service.

The intended application-facing model is:

1. Create one client bound to one timeline
2. Call one allocation API

Everything else is internal client logic.

## Notes

- Your application should not call internal routing RPCs directly.
- `timeline_key` is chosen once when the client is created.
- Allocation is the only operation the application should need during normal use.
- Route ensure, route refresh, and stale-route retry are internal client behavior.
- Route refresh reconnects allocation traffic to the current owner endpoint.
- Direct protobuf route-management RPC use is considered internal or advanced usage.

## Clients

- Client index: `clients/README.md`
