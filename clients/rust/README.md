# Chronos Rust Client

`Client` is the Rust application-facing Chronos client implementation.

Support level: Primary.

## Public shape

- `let client = Client::connect(addr, timeline_key).await?`
- `client.allocate_timestamps(count).await?`

Advanced configuration should use `ClientConfig` builder methods.

## Build and test

```bash
cargo clippy --all-targets -- -D warnings
cargo test client::tests --lib
```

## Minimal usage

```rust
use chronos::{Client, ClientConfig, ClientTransportConfig};

type AppResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> AppResult<()> {
    let config = ClientConfig::new("orders.primary")
        .with_transport(ClientTransportConfig::default().with_insecure(true));
    let client = Client::connect_with_config("127.0.0.1:50051", config).await?;
    let ranges = client.allocate_timestamps(1).await?;
    println!("tso={}", ranges[0].start_tso);
    Ok(())
}
```

## Behavior

- The client ensures the bound timeline on first connect
- The client fetches and caches the current route internally
- Stale-route errors are handled with a configurable refresh-and-retry budget
- Route refresh reconnects allocation traffic to the current owner endpoint
- Allocation request-record idempotency is disabled by default; use
  `ClientConfig::with_idempotency_enabled(true)` when callers need replay protection
- Other RPC failures are returned to the caller

## Files

- Implementation: `src/client.rs`
- README: `clients/rust/README.md`
