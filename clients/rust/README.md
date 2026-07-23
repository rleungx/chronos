# Chronos Rust Client

`Client` is the Rust application-facing Chronos client implementation.

Support level: Primary.

## Public shape

- `let client = Client::connect(addr, timeline_key).await?`
- `client.allocate_timestamps(count).await?`

Advanced configuration should use `ClientConfig` builder methods for desired resource tier,
request timeout, route-recovery retry attempts/backoff, idempotency, and transport.

## Build and test

```bash
cargo clippy --all-targets -- -D warnings
cargo test client::tests --lib
```

## Example

```bash
cargo run --example client_example
```

## Behavior

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled with a configurable refresh-and-retry budget
- Route refresh reconnects allocation traffic to the current owner endpoint
- Owner transport failures refresh through the stable construction endpoint; use a Service or load
  balancer rather than an owner-pod address in production
- Allocation request-record idempotency is disabled by default; use
  `ClientConfig::with_idempotency_enabled(true)` when callers need replay protection
- Other RPC failures are returned to the caller
