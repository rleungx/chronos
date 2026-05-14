# Chronos Client Contract

This is the repo-local contract for application-facing clients. It is checked by
`make client-conformance-check`.

Every supported language client must expose the same application model:

1. construct one client for one Chronos endpoint and one `timeline_key`
2. allocate timestamps with one method that takes only `count`
3. close or destroy the client when the application is done

Every client must support the same behavior:

- ensure the bound timeline on first connect
- install the route returned by `EnsureTimeline` without an extra initial `GetTimelineRoute`
- cache the current route and allocation owner endpoint internally
- allocate against the `owner_worker_endpoint` returned by the route service
- refresh the route and retry only for Chronos stale-route error details:
  `NOT_TIMELINE_OWNER`, `ROUTE_VERSION_MISMATCH`, and `EPOCH_MISMATCH`
- reuse the same logical client request id across stale-route retries when idempotency is enabled
- reject missing routes and routes with an empty owner endpoint
- return non-route RPC failures directly to the caller

Every client must expose these configuration capabilities:

- desired resource tier for timeline ensure
- per-allocation Chronos request timeout
- stale-route retry attempts
- stale-route retry backoff
- optional allocation request-record idempotency
- TLS roots
- mTLS client identity
- server-name or authority override
- explicit plaintext transport for local development

Examples must stay aligned across Rust, Go, Java, and C++: local plaintext endpoint,
`orders.primary` timeline, and a single allocation call.
